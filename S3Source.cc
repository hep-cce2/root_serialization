#include <iostream>
#include "zstd.h"
#include "lzma.h"
#include "S3Source.h"
#include "SourceFactory.h"
#include "Deserializer.h"
#include "UnrolledDeserializer.h"
#include "FunctorTask.h"

using namespace cce::tf;

namespace {
struct ZSTD_ContextHolder {
  ZSTD_ContextHolder() { ctx = ZSTD_createDCtx(); }
  ~ZSTD_ContextHolder() { ZSTD_freeDCtx(ctx); }
  ZSTD_DCtx* ctx;
};

size_t zstd_perthread_decompress(void* dst, size_t dstCapacity, const void* src, size_t compressedSize) {
  static thread_local ZSTD_ContextHolder holder{};
  return ZSTD_decompressDCtx(holder.ctx, dst, dstCapacity, src, compressedSize);
}

void zstd_decompress(const std::string& blob, std::string& out, size_t dSize) {
  out.resize(dSize);
  size_t status = ZSTD_decompress(out.data(), out.size(), blob.data(), blob.size());
  // size_t status = zstd_perthread_decompress(out.data(), out.size(), blob.data(), blob.size());
  if ( ZSTD_isError(status) ) {
    std::cerr <<"ERROR in decompression " << ZSTD_getErrorName(status) << std::endl;
  }
  if (status < dSize) {
    std::cerr <<"ERROR in decompression, expected " << dSize << " bytes but only got " << status << std::endl;
  }
}

// /cvmfs/cms.cern.ch/slc7_amd64_gcc10/external/xz/5.2.5-d6fed2038c4e8d6e04531d1adba59f37
void lzma_decompress(const std::string& blob, std::string& out, size_t dSize) {
  lzma_stream strm = LZMA_STREAM_INIT;
  lzma_ret ret = lzma_stream_decoder(&strm, UINT64_MAX, 0);
  if (ret != LZMA_OK) { throw std::runtime_error("Could not initialize LZMA encoder"); }

  out.resize(dSize);
  strm.next_in = (const uint8_t*) blob.data();
  strm.avail_in = blob.size();
  strm.next_out = (uint8_t*) out.data();
  strm.avail_out = out.size();
  while ( strm.avail_in > 0 ) {
    ret = lzma_code(&strm, LZMA_RUN);
    if ( ret == LZMA_STREAM_END ) break;
    else if (ret != LZMA_OK) {
      std::cerr << "ERROR in lzma compression " << ret << std::endl;
      break;
    }
  }
  if ( strm.avail_out > 0 ) {
    std::cerr <<"ERROR in decompression, expected " << dSize << " bytes but only got " << dSize - strm.avail_out << std::endl;
  }
}

void decompress_stripe(const objstripe::Compression& setting, const std::string& blob, std::string& out, size_t dSize) {
  switch ( setting.type() ) {
    case objstripe::CompressionType::kNone:
      out = blob;
      break;
    case objstripe::CompressionType::kZSTD:
      ::zstd_decompress(blob, out, dSize);
      break;
    case objstripe::CompressionType::kLZMA:
      ::lzma_decompress(blob, out, dSize);
      break;
  }
}
}

void DelayedProductStripeRetriever::fetch(TaskHolder&& callback) const {
  auto this_state{State::unretrieved};
  if ( state_.compare_exchange_strong(this_state, State::retrieving) ) {
    auto req = std::make_shared<S3Request>(S3Request::Type::get, name_);
    auto group = callback.group();
    waiters_.add(std::move(callback));
    auto getDoneTask = TaskHolder(*group, make_functor_task([this, req]() {
        if ( req->status == S3Request::Status::ok ) {
          auto start = std::chrono::high_resolution_clock::now();
          if ( not data_.ParseFromString(req->buffer) ) {
            throw std::runtime_error("Could not deserialize ProductStripe for key " + name_);
          }
          offsets_.reserve(data_.counts_size() + 1);
          size_t nbytes{0};
          offsets_.push_back(nbytes);
          for (const auto& c : data_.counts()) {
            nbytes += c;
            offsets_.push_back(nbytes);
          }
          assert(offsets_.size() == data_.counts_size() + 1);
          ::decompress_stripe(data_.compression(), data_.content(), content_, nbytes);
          assert(nbytes == content_.size());
          data_.clear_content();
          decompressTime_ = std::chrono::duration_cast<decltype(decompressTime_)>(std::chrono::high_resolution_clock::now() - start);
          state_ = State::retrieved;
          waiters_.doneWaiting();
        } else { throw std::runtime_error("Could not retrieve ProductStripe for key " + name_); }
      }));
    conn_->submit(std::move(req), std::move(getDoneTask));
  } else if (this_state == State::retrieved ) {
    return;
  } else {
    waiters_.add(std::move(callback));
  }
}

std::string_view DelayedProductStripeRetriever::bufferAt(size_t globalEventIndex) const {
  assert(state_ == State::retrieved);
  assert(globalOffset_ == data_.globaloffset());
  assert(globalOffset_ <= globalEventIndex);
  size_t iOffset = globalEventIndex - globalOffset_;
  assert(iOffset < data_.counts_size());
  size_t bstart = offsets_[iOffset];
  size_t bstop = offsets_[iOffset+1];
  return {&content_[bstart], bstop - bstart};
}

ProductStripeGenerator::ProductStripeGenerator(const S3ConnectionRef& conn, const std::string& prefix, unsigned int flushSize, size_t globalIndexStart, size_t globalIndexEnd) :
  conn_(conn), prefix_(prefix), flushSize_(flushSize), globalIndexStart_(globalIndexStart), globalIndexEnd_(globalIndexEnd)
{
  auto indexThis = globalIndexStart - (globalIndexStart % flushSize_);
  auto indexNext = indexThis + flushSize_;
  currentStripe_ = std::make_shared<const DelayedProductStripeRetriever>(conn_, prefix_ + "/" + std::to_string(indexThis), indexThis);
  nextStripe_ = std::make_shared<const DelayedProductStripeRetriever>(conn_, prefix_ + "/" + std::to_string(indexNext), indexNext);
  prefetch_group_ = std::make_unique<tbb::task_group>();
}

std::shared_ptr<const DelayedProductStripeRetriever>
ProductStripeGenerator::stripeFor(size_t globalEventIndex) {
  assert(globalEventIndex >= globalIndexStart_ and globalEventIndex < globalIndexEnd_);
  if ( globalEventIndex == nextStripe_->globalOffset() ) {
    auto indexNext = globalEventIndex + flushSize_;
    auto new_ps = std::make_shared<const DelayedProductStripeRetriever>(conn_, prefix_ + "/" + std::to_string(indexNext), indexNext);
    // record decompress time of old stripe
    decompressTime_ += currentStripe_->decompressTime();
    // shuffle new_ps -> nextStripe_ -> currentStripe_
    std::swap(nextStripe_, currentStripe_);
    std::swap(new_ps, nextStripe_);
  }
  if ( 
      currentStripe_->wasFetched()
      and ~nextStripe_->wasFetched()
      and (globalEventIndex % flushSize_ >= flushSize_ / 2)
      and (globalEventIndex + flushSize_ < globalIndexEnd_)
      )
  {
    // somewhere in the middle of current stripe, prefetch next
    nextStripe_->fetch(TaskHolder(*prefetch_group_, make_functor_task([](){})));
  }
  return currentStripe_;
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

S3Source::S3Source(unsigned int iNLanes, std::string iObjPrefix, int iVerbose, unsigned long long iNEvents, const S3ConnectionRef& conn):
  SharedSourceBase(iNEvents),
  objPrefix_(std::move(iObjPrefix)),
  verbose_(iVerbose),
  conn_(conn),
  readTime_{std::chrono::microseconds::zero()}
{
  auto start = std::chrono::high_resolution_clock::now();

  {
    tbb::task_group group;
    auto req = std::make_shared<S3Request>(S3Request::Type::get, "index/" + objPrefix_);
    auto getDoneTask = TaskHolder(group, make_functor_task([this, req]() {
        if ( req->status == S3Request::Status::ok ) {
          if ( not index_.ParseFromString(req->buffer) ) {
            throw std::runtime_error("Could not deserialize index in S3Source construction");
          }
        }
        else { throw std::runtime_error("Could not retrieve index in S3Source construction"); }
      }));
    conn_->submit(std::move(req), std::move(getDoneTask));
    group.wait();
  }

  if ( verbose_ >= 3 ) {
    std::cout << index_.DebugString() << "\n";
  }

  if ( index_.totalevents() < iNEvents ) {
    std::cout << "WARNING: less events in source than requested: "
      << index_.totalevents() << " vs. " << iNEvents << ". Will read all available events instead.\n";
  }

  productRetrievers_.reserve(index_.products_size());
  for(const auto& productInfo : index_.products()) {
    productRetrievers_.emplace_back(conn_, objPrefix_ + "/" + productInfo.productname(), productInfo.flushsize(), 0ul, index_.totalevents());
  }
 
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
          // Need to read ahead
          const auto& stripeData = index_.packedeventstripes(nextEventStripe_);
          if ( index_.has_eventstripecompression() ) {
            auto dsize = index_.eventstripesizes(nextEventStripe_);
            std::string decompressedStripe;
            decompressedStripe.resize(dsize);
            ::decompress_stripe(index_.eventstripecompression(), stripeData, decompressedStripe, dsize);
            currentEventStripe_.ParseFromString(decompressedStripe);
          } else {
            currentEventStripe_.ParseFromString(stripeData);
          }
          nextEventStripe_++;
          nextEventInStripe_ = 0;
        }
        const auto event = currentEventStripe_.events(nextEventInStripe_);
        if ( verbose_ >= 1 ) std::cout << event.DebugString() << "\n";
        auto& retriever = laneRetrievers_[iLane];
        size_t globalEventIndex = event.offset();

        for (size_t i=0; i < productRetrievers_.size(); ++i) {
          retriever.setStripe(i, productRetrievers_[i].stripeFor(globalEventIndex));
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
  for(auto const& p : productRetrievers_) {
    time += p.decompressTime();
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
