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

void parse_productstripe(objstripe::ProductStripe& stripe, std::vector<size_t>& offsets, std::string& content) {
  offsets.reserve(stripe.counts_size() + 1);
  size_t nbytes{0};
  offsets.push_back(nbytes);
  for (const auto& c : stripe.counts()) {
    nbytes += c;
    offsets.push_back(nbytes);
  }
  assert(offsets.size() == stripe.counts_size() + 1);
  decompress_stripe(stripe.compression(), stripe.content(), content, nbytes);
  assert(nbytes == content.size());
  stripe.clear_content();
}
}

WaitableFetch::~WaitableFetch() {}

void WaitableFetch::fetch(TaskHolder&& callback) {
  auto this_state{State::unretrieved};
  if ( state_.compare_exchange_strong(this_state, State::retrieving) ) {
    auto req = std::make_shared<S3Request>(S3Request::Type::get, name_);
    auto group = callback.group();
    waiters_.add(std::move(callback));
    auto getDoneTask = TaskHolder(*group, make_functor_task([this, req]() {
        if ( req->status == S3Request::Status::ok ) {
          auto start = std::chrono::high_resolution_clock::now();
          parse(req->buffer);
          parseTime_ = std::chrono::duration_cast<decltype(parseTime_)>(std::chrono::high_resolution_clock::now() - start);
          state_ = State::retrieved;
          waiters_.doneWaiting();
        } else {
          // TODO: possible that prefetch is for a stripe that is in a group in the next event batch
          // throw std::runtime_error("Could not retrieve key " + name_);
          std::cerr << "Could not retrieve key "  + name_ + "\n";
        }
      }));
    conn_->submit(std::move(req), std::move(getDoneTask));
  } else if (this_state == State::retrieved ) {
    return;
  } else {
    waiters_.add(std::move(callback));
  }
}

std::string_view WaitableFetchProductStripe::bufferAt(size_t groupIdx, size_t iOffset) const {
  assert(state_ == State::retrieved);
  assert(groupIdx == 0);
  if ( iOffset >= offsets_.size() - 1 ) {
    std::cerr << name_ << " at " << groupIdx << ", " << iOffset << std::endl;
  }
  assert(iOffset < offsets_.size() - 1);
  size_t bstart = offsets_[iOffset];
  size_t bstop = offsets_[iOffset+1];
  assert(bstop > bstart);
  assert(bstop <= content_.size());
  return {&content_[bstart], bstop - bstart};
}

void WaitableFetchProductStripe::parse(const std::string& buffer) {
  if ( not data_.ParseFromString(buffer) ) {
    throw std::runtime_error("Could not deserialize key " + name_);
  }
  ::parse_productstripe(data_, offsets_, content_);
}

std::string_view WaitableFetchProductGroupStripe::bufferAt(size_t groupIdx, size_t iOffset) const {
  assert(state_ == State::retrieved);
  assert(groupIdx < offsets_.size());
  assert(iOffset < offsets_[groupIdx].size() - 1);
  size_t bstart = offsets_[groupIdx][iOffset];
  size_t bstop = offsets_[groupIdx][iOffset+1];
  assert(bstop > bstart);
  assert(bstop <= content_[groupIdx].size());
  return {&content_[groupIdx][bstart], bstop - bstart};
}

void WaitableFetchProductGroupStripe::parse(const std::string& buffer) {
  if ( not data_.ParseFromString(buffer) ) {
    throw std::runtime_error("Could not deserialize key " + name_);
  }
  offsets_.resize(data_.products_size());
  content_.resize(data_.products_size());
  for(size_t i=0; i< data_.products_size(); ++i) {
    ::parse_productstripe(*data_.mutable_products(i), offsets_[i], content_[i]);
  }
}

void DelayedProductStripeRetriever::fetch(TaskHolder&& callback) const {
  fetcher_->fetch(std::move(callback));
}

std::string_view DelayedProductStripeRetriever::bufferAt(size_t globalEventIndex) const {
  assert(globalOffset_ <= globalEventIndex);
  size_t iOffset = globalEventIndex - globalOffset_;
  assert(iOffset < flushSize_);
  return fetcher_->bufferAt(groupIdx_, iOffset);
}

ProductStripeGenerator::ProductStripeGenerator(const S3ConnectionRef& conn, const std::string& prefix, unsigned int flushSize, size_t globalIndexStart, size_t globalIndexEnd) :
  conn_(conn), prefix_(prefix), flushSize_(flushSize), globalIndexStart_(globalIndexStart), globalIndexEnd_(globalIndexEnd)
{
  auto indexThis = globalIndexStart - (globalIndexStart % flushSize_);
  auto indexNext = indexThis + flushSize_;
  currentStripe_ = std::make_shared<const DelayedProductStripeRetriever>(conn_, prefix_ + "/" + std::to_string(indexThis), indexThis, flushSize_);
  nextStripe_ = std::make_shared<const DelayedProductStripeRetriever>(conn_, prefix_ + "/" + std::to_string(indexNext), indexNext, flushSize_);
  prefetch_group_ = std::make_unique<tbb::task_group>();
}

std::shared_ptr<const DelayedProductStripeRetriever>
ProductStripeGenerator::stripeFor(size_t globalEventIndex) {
  assert(globalEventIndex >= globalIndexStart_ and globalEventIndex < globalIndexEnd_);
  assert(globalEventIndex >= currentStripe_->globalOffset());
  assert(globalEventIndex <= nextStripe_->globalOffset());
  if ( globalEventIndex == nextStripe_->globalOffset() ) {
    auto indexNext = globalEventIndex + flushSize_;
    auto new_ps = std::make_shared<const DelayedProductStripeRetriever>(conn_, prefix_ + "/" + std::to_string(indexNext), indexNext, flushSize_);
    // record decompress time of old stripe
    decompressTime_ += currentStripe_->decompressTime();
    // shuffle new_ps -> nextStripe_ -> currentStripe_
    currentStripe_ = nextStripe_;
    nextStripe_ = new_ps;
  }
  assert(globalEventIndex >= currentStripe_->globalOffset());
  assert(globalEventIndex < nextStripe_->globalOffset());
  if ( 
      currentStripe_->wasFetched()
      and ~nextStripe_->wasFetched()
      and (globalEventIndex % flushSize_ >= flushSize_ / 2)
      and (globalEventIndex + flushSize_ < globalIndexEnd_)
      and false
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
        if ( readSize != buf.size() ) {
          throw std::runtime_error(
              "Read fail for event " + std::to_string(globalEventIndex_) + " product " + std::to_string(index) + " (" + dataProducts_[index].name() + ")"
              );
        }
        assert(readSize == buf.size());
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

          productGroupMap_.clear();
          size_t eventStripeStart = currentEventStripe_.events(0).offset();
          assert(eventStripeStart == (nextEventStripe_-1)* index_.eventstripesize());
          for (size_t iGroup=0; iGroup < currentEventStripe_.groups_size(); ++iGroup) {
            const auto& group = currentEventStripe_.groups(iGroup);
            auto fetcher = std::make_shared<WaitableFetchProductGroupStripe>(
                conn_, objPrefix_ + "/group" + std::to_string(iGroup) + "/" + std::to_string(eventStripeStart)
              );
            if ( verbose_ >= 2 ) std::cout << "creating fetcher for group " + objPrefix_ + "/group" + std::to_string(iGroup) + "/" + std::to_string(eventStripeStart) + "\n";
            for (size_t groupIdx=0; groupIdx < group.names_size(); groupIdx++) {
              productGroupMap_[group.names(groupIdx)] = std::make_shared<const DelayedProductStripeRetriever>(fetcher, groupIdx, eventStripeStart, index_.eventstripesize());
            }
          }
        }
        size_t eventStripeStart = currentEventStripe_.events(0).offset();
        const auto event = currentEventStripe_.events(nextEventInStripe_);
        if ( verbose_ >= 1 ) std::cout << event.DebugString() << "\n";
        auto& retriever = laneRetrievers_[iLane];
        size_t globalEventIndex = event.offset();

        for (size_t i=0; i < productRetrievers_.size(); ++i) {
          auto itgroup = productGroupMap_.find(objPrefix_ + "/" + index_.products(i).productname() + "/" + std::to_string(eventStripeStart));
          auto pstripe = productRetrievers_[i].stripeFor(globalEventIndex);
          if ( itgroup != productGroupMap_.end() ) {
            if ( verbose_ >= 2) std::cout << "using group for product " + index_.products(i).productname() + "/" + std::to_string(eventStripeStart) + "\n";
            retriever.setStripe(i, itgroup->second);
          } else {
            if (verbose_ >= 2) std::cout << "using individual stripe for product " + index_.products(i).productname() + "\n";
            retriever.setStripe(i, std::move(pstripe));
          }
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
