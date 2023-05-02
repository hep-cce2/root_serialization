#if !defined(S3Outputer_h)
#define S3Outputer_h

#include <vector>
#include <string>
#include <iostream>
#include <cassert>

#include "zstd.h"
#include "lzma.h"

#define TBB_PREVIEW_TASK_GROUP_EXTENSIONS 1 // for task_group::defer
#include "tbb/task_group.h"

#include "OutputerBase.h"
#include "EventIdentifier.h"
#include "SerializeStrategy.h"
#include "DataProductRetriever.h"
#include "summarize_serializers.h"
#include "SerialTaskQueue.h"
#include "S3Common.h"
#include "objectstripe.pb.h"

namespace cce::tf {

class StreamCompressor {
  public:
    StreamCompressor() {};
    StreamCompressor(const objstripe::Compression& setting);
    const objstripe::Compression& getCompression() const { return setting_; }
    size_t write(const std::string_view blob, std::string& out);
    void flush(std::string& out);

  private:
    objstripe::Compression setting_;

    struct ZSTDDeleter { void operator()(ZSTD_CStream* s) const {ZSTD_freeCStream(s);} };
    std::unique_ptr<ZSTD_CStream, ZSTDDeleter> zstd_;
    struct LZMADeleter { void operator()(lzma_stream* s) const {lzma_end(s); free(s);} };
    std::unique_ptr<lzma_stream, LZMADeleter> lzma_;
};

class S3Outputer : public OutputerBase {
 public:
  S3Outputer(unsigned int iNLanes, std::string objPrefix, int iVerbose, size_t iProductBufferFlush, size_t iEventFlushSize, S3ConnectionRef conn, objstripe::CompressionType cType, uint32_t cLevel):
    serializers_(iNLanes),
    objPrefix_(objPrefix),
    verbose_(iVerbose),
    productBufferFlushMinBytes_(iProductBufferFlush),
    eventFlushSize_(iEventFlushSize),
    conn_(std::move(conn)),
    collateTime_{std::chrono::microseconds::zero()},
    flushTime_{std::chrono::microseconds::zero()},
    parallelTime_{0}
  {
    index_.set_eventstripesize(eventFlushSize_);
    currentEventStripe_.mutable_events()->Reserve(eventFlushSize_);

    // TODO: make configurable
    index_.set_serializestrategy(objstripe::SerializeStrategy::kRoot);
    defaultCompression_.set_type(cType);
    defaultCompression_.set_level(cLevel);
    index_.set_allocated_eventstripecompression(new objstripe::Compression(defaultCompression_));
    eventStripeCompressor_ = StreamCompressor(index_.eventstripecompression());
  }

  void setupForLane(unsigned int iLaneIndex, std::vector<DataProductRetriever> const& iDPs) final;
  bool usesProductReadyAsync() const final {return true;}
  void productReadyAsync(unsigned int iLaneIndex, DataProductRetriever const& iDataProduct, TaskHolder iCallback) const final;
  void outputAsync(unsigned int iLaneIndex, EventIdentifier const& iEventID, TaskHolder iCallback) const final;
  void printSummary() const final;

private:
  struct ProductOutputBuffer {
    ProductOutputBuffer(const std::string& prefix, objstripe::ProductInfo* info, const objstripe::Compression& comp) :
      prefix_{prefix}, info_{info}, compressor_{comp} {
        stripe_.set_content("");
        stripe_.set_allocated_compression(new objstripe::Compression(compressor_.getCompression()));
      };

    const std::string prefix_;
    objstripe::ProductInfo* info_; // owned by index_
    StreamCompressor compressor_;
    objstripe::ProductStripe stripe_{};
    SerialTaskQueue appendQueue_{};
    std::chrono::microseconds appendTime_{0};
  };

  // Plan:
  // productReadyAsync() is threadsafe because serializers_ is one per lane
  // outputAsync puts collateProducts() in collateQueue_
  // collateProducts() appends a new objstripe::Event to currentEventStripe_ and if time to flush
  // it creates a TaskHolder that appends flushEventStripe() to flushQueue_
  // then collate() calls appendProductBuffer() with the above TaskHolder as callback (or original callback)
  // printSummary() takes care of the tails by setting last=true in the calls
  void collateProducts(EventIdentifier const& iEventID, SerializeStrategy const& iSerializers, TaskHolder iCallback) const;
  void appendProductBuffer(ProductOutputBuffer& buf, const std::string_view blob, TaskHolder iCallback, bool last=false) const;
  void flushEventStripe(const objstripe::EventStripe& stripe, TaskHolder iCallback, bool last=false) const;

  // configuration options
  const int verbose_;
  const std::string objPrefix_;
  const size_t productBufferFlushMinBytes_;
  const size_t eventFlushSize_;
  S3ConnectionRef conn_;
  objstripe::Compression defaultCompression_{};

  // only modified by productReadyAsync()
  mutable std::vector<SerializeStrategy> serializers_;

  // only modified in collateProducts()
  mutable SerialTaskQueue collateQueue_;
  mutable size_t eventGlobalOffset_{0};
  mutable objstripe::EventStripe currentEventStripe_{};
  mutable std::chrono::microseconds collateTime_;
  constexpr static unsigned int maxFireAndForgetCollates_{4};
  mutable std::atomic<unsigned int> numFireAndForgetCollates_{0};

  // only modified in appendProductBuffer()
  mutable std::vector<ProductOutputBuffer> buffers_;

  // only modified in flushEventStripe()
  // (for index_'s ProductInfos, appendProductBuffer() has finished before we access)
  mutable SerialTaskQueue flushQueue_;
  mutable objstripe::ObjectStripeIndex index_;
  mutable StreamCompressor eventStripeCompressor_;
  mutable std::chrono::microseconds flushTime_;

  mutable std::atomic<std::chrono::microseconds::rep> parallelTime_;
};
}
#endif
