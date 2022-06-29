#include <iostream>
#include "S3Source.h"
#include "SourceFactory.h"
#include "Deserializer.h"
#include "UnrolledDeserializer.h"
#include "FunctorTask.h"

using namespace cce::tf;

void DelayedProductStripeRetriever::fetch(TaskHolder&& callback) const {
  auto this_state{State::unretrieved};
  if ( state_.compare_exchange_strong(this_state, State::retrieving) ) {
    conn_->get(name_, [this, callback=std::move(callback)](S3Request* req) mutable {
        if ( req->status == S3Request::Status::ok ) {
          if ( not data_.ParseFromString(req->buffer) ) {
            throw std::runtime_error("Could not deserialize ProductStripe for key " + name_);
          }
          state_ = State::retrieved;
          callback.doneWaiting();
          for(auto& w : waiters_) w.doneWaiting();
        }
        else { throw std::runtime_error("Could not retrieve ProductStripe for key " + name_); }
      });
  } else if (this_state == State::retrieved ) {
    return;
  } else {
    // TODO: check again if not State::retrieved?
    waiters_.emplace_back(std::move(callback));
  }
}

std::string_view DelayedProductStripeRetriever::bufferAt(size_t globalEventIndex) const {
  assert(state_ == State::retrieved);
  assert(globalOffset_ == data_.globaloffset());
  assert(globalOffset_ <= globalEventIndex);
  size_t offset = globalEventIndex - globalOffset_;
  assert(offset < data_.offsets_size());
  size_t bstart = (offset == 0) ? 0 : data_.offsets(offset-1);
  size_t bstop = data_.offsets(offset);
  return {&data_.content()[bstart], bstop - bstart};
}

S3DelayedRetriever::S3DelayedRetriever(objstripe::ObjectStripeIndex const& index, DeserializeStrategy strategy):
  deserializers_{std::move(strategy)}
{
  dataProducts_.reserve(index.products_size());
  deserializers_.reserve(index.products_size());
  dataBuffers_.resize(index.products_size(), nullptr);
  stripes_.resize(index.products_size());
  size_t i{0};
  for(auto const& pi : index.products()) {
    TClass* cls = TClass::GetClass(pi.producttype().c_str());
    if ( cls == nullptr ) {
      throw std::runtime_error("No TClass reflection available for " + pi.productname());
    }
    dataBuffers_[i] = cls->New();
    dataProducts_.emplace_back(i, &dataBuffers_[i], pi.productname(), cls, this);
    deserializers_.emplace_back(cls);
    ++i;
  }
}

S3DelayedRetriever::~S3DelayedRetriever() {
  auto it = dataProducts_.begin();
  for(void * b: dataBuffers_) {
    it->classType()->Destructor(b);
    ++it;
  }
}

void S3DelayedRetriever::getAsync(DataProductRetriever& product, int index, TaskHolder callback) {
  assert(&product == &dataProducts_[index]);
  assert(product.address() == &dataBuffers_[index]);
  assert(stripes_[index]);
  TaskHolder fetchCallback(*callback.group(), make_functor_task(
      [this, index, callback=std::move(callback)]() mutable {
        auto start = std::chrono::high_resolution_clock::now();
        auto buf = stripes_[index]->bufferAt(globalEventIndex_);
        auto readSize = deserializers_[index].deserialize(buf.data(), buf.size(), *dataProducts_[index].address());
        dataProducts_[index].setSize(readSize);
        deserializeTime_ += std::chrono::duration_cast<decltype(deserializeTime_)>(std::chrono::high_resolution_clock::now() - start);
      }
    ));
  stripes_[index]->fetch(std::move(fetchCallback));
}

S3Source::S3Source(unsigned int iNLanes, std::string iObjPrefix, int iVerbose, unsigned long long iNEvents, S3ConnectionRef conn):
  SharedSourceBase(iNEvents),
  objPrefix_(std::move(iObjPrefix)),
  verbose_(iVerbose),
  conn_(conn),
  readTime_{std::chrono::microseconds::zero()}
{
  auto start = std::chrono::high_resolution_clock::now();

  conn->get(objPrefix_ + "index", [this](S3Request* req) mutable {
      if ( req->status == S3Request::Status::ok ) {
        if ( not index_.ParseFromString(req->buffer) ) {
          throw std::runtime_error("Could not deserialize index in S3Source construction");
        }
      }
      else { throw std::runtime_error("Could not retrieve index in S3Source construction"); }
    });
  if ( verbose_ >= 3 ) {
    std::cout << index_.DebugString() << "\n";
  }

  if ( index_.totalevents() < iNEvents ) {
    std::cout << "WARNING: less events in source than requested: "
      << index_.totalevents() << " vs. " << iNEvents << ". Will read all available events instead.\n";
  }

  currentProductStripes_.resize(index_.products_size());
 
  laneRetrievers_.reserve(iNLanes);
  for(unsigned int i = 0; i< iNLanes; ++i) {
    DeserializeStrategy strategy;
    switch(index_.serializestrategy()) {
      case objstripe::SerializeStrategy::kRoot:
        strategy = DeserializeStrategy::make<DeserializeProxy<Deserializer>>();
        break;
      case objstripe::SerializeStrategy::kRootUnrolled:
        strategy = DeserializeStrategy::make<DeserializeProxy<UnrolledDeserializer>>();
        break;
    }
    laneRetrievers_.emplace_back(index_, std::move(strategy));
  }

  readTime_ += std::chrono::duration_cast<decltype(readTime_)>(std::chrono::high_resolution_clock::now() - start);
}

size_t S3Source::numberOfDataProducts() const {
  return index_.products_size();
}

std::vector<DataProductRetriever>& S3Source::dataProducts(unsigned int iLane, long iEventIndex) {
  return laneRetrievers_[iLane].dataProducts();
}

EventIdentifier S3Source::eventIdentifier(unsigned int iLane, long iEventIndex) {
  return laneRetrievers_[iLane].event();
}

void S3Source::readEventAsync(unsigned int iLane, long iEventIndex, OptionalTaskHolder iTask) {
  queue_.push(*iTask.group(), [iLane, optTask = std::move(iTask), this]() mutable {
      auto start = std::chrono::high_resolution_clock::now();
      if(
          (nextEventStripe_ < index_.packedeventstripes_size())
          or (nextEventInStripe_ < currentEventStripe_.events_size())
        )
      {
        // default-constructed currentEventStripe_ will have size zero, so 0, 0 will load first stripe
        if(nextEventInStripe_ == currentEventStripe_.events_size()) {
          // need to read ahead
          // TODO: compression
          currentEventStripe_.ParseFromString(index_.packedeventstripes(nextEventStripe_++));
          nextEventInStripe_ = 0;
        }
        const auto event = currentEventStripe_.events(nextEventInStripe_);
        if ( verbose_ >= 2 ) std::cout << event.DebugString() << "\n";
        auto& retriever = laneRetrievers_[iLane];
        size_t globalEventIndex = event.offset();

        auto productinfo = std::begin(index_.products());
        size_t i{0};
        for (auto& ps : currentProductStripes_) {
          const auto& productinfo = index_.products(i);
          if ( nextEventInStripe_ % productinfo.flushsize() == 0 ) {
            auto new_ps = std::make_shared<const DelayedProductStripeRetriever>(
                conn_,
                objPrefix_ + productinfo.productname() + std::to_string(globalEventIndex),
                globalEventIndex
                );
            if ( verbose_ >= 2 ) {
              std::cout << "setting lane " << iLane << "to read stripe " <<
                objPrefix_ + productinfo.productname() + std::to_string(globalEventIndex) << "\n";
            }
            std::swap(ps, new_ps);
          }
          retriever.setStripe(i, ps);
          i++;
        }

        retriever.setEvent(globalEventIndex, {event.run(), event.lumi(), event.event()});
        optTask.releaseToTaskHolder();
        ++nextEventInStripe_;
      }
      readTime_ +=std::chrono::duration_cast<decltype(readTime_)>(std::chrono::high_resolution_clock::now() - start);
    });
}

void S3Source::printSummary() const {
  std::cout <<"\nSource:\n"
    "  serial read time: "<<serialReadTime().count()<<"us\n"
    "  decompress time: "<<decompressTime().count()<<"us\n"
    "  deserialize time: "<<deserializeTime().count()<<"us\n"
    "  total blocking time in S3Connection: "<<conn_->blockingTime().count()<<"us\n";
};

std::chrono::microseconds S3Source::serialReadTime() const {
  return readTime_;
}

std::chrono::microseconds S3Source::decompressTime() const {
  auto time = std::chrono::microseconds::zero();
  for(auto const& l : laneRetrievers_) {
    time += l.decompressTime();
  }
  return time;
}

std::chrono::microseconds S3Source::deserializeTime() const {
  auto time = std::chrono::microseconds::zero();
  for(auto const& l : laneRetrievers_) {
    time += l.deserializeTime();
  }
  return time;
}


namespace {
class Maker : public SourceMakerBase {
  public:
    Maker(): SourceMakerBase("S3Source") {}
    std::unique_ptr<SharedSourceBase> create(unsigned int iNLanes, unsigned long long iNEvents, ConfigurationParameters const& params) const final {
      auto verbose = params.get<int>("verbose", 0);
      auto objPrefix = params.get<std::string>("prefix");
      if(not objPrefix) {
        std::cerr << "no object prefix given for S3Outputer\n";
        return {};
      }
      auto connfile = params.get<std::string>("conn");
      if(not connfile) {
        std::cerr <<"no connection configuration file name given for S3Source\n";
        return {};
      }
      auto conn = S3Connection::from_config(connfile.value()); 
      if(not conn) {
        return {};
      }

      return std::make_unique<S3Source>(iNLanes, objPrefix.value(), verbose, iNEvents, conn);
    }
  };

Maker s_maker;
}
