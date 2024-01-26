#if !defined(S3Source_h)
#define S3Source_h

#include <string>
#include <memory>
#include <chrono>
#include <iostream>
#include <utility>

#include "S3Common.h"
#include "SharedSourceBase.h"
#include "DataProductRetriever.h"
#include "DelayedProductRetriever.h"
#include "SerialTaskQueue.h"
#include "WaitingTaskList.h"
#include "DeserializeStrategy.h"
#include "objectstripe.pb.h"


namespace cce::tf {

class WaitableFetch {
  public:
    WaitableFetch(const S3ConnectionRef& conn, const std::string& name):
      conn_(conn), name_(name), state_{State::unretrieved} {};
    virtual ~WaitableFetch();
    void fetch(TaskHolder&& callback);
    bool wasFetched() const { return state_ != State::unretrieved; };
    virtual std::string_view bufferAt(size_t groupIdx, size_t iOffset) const = 0;
    std::chrono::microseconds parseTime() const { return parseTime_; };

  protected:
    const std::string name_;
    enum class State {unretrieved, retrieving, retrieved};
    std::atomic<State> state_;

  private:
    virtual void parse(const std::string& buffer) = 0;
    WaitingTaskList waiters_{};
    const S3ConnectionRef conn_;
    std::chrono::microseconds parseTime_{0};
};

class WaitableFetchProductStripe : public WaitableFetch {
  public:
    using WaitableFetch::WaitableFetch;
    std::string_view bufferAt(size_t groupIdx, size_t iOffset) const override;
  private:
    void parse(const std::string& buffer) override;
    objstripe::ProductStripe data_;
    std::vector<size_t> offsets_{};
    std::string content_{};
};

class WaitableFetchProductGroupStripe : public WaitableFetch {
  public:
    using WaitableFetch::WaitableFetch;
    std::string_view bufferAt(size_t groupIdx, size_t iOffset) const override;
  private:
    void parse(const std::string& buffer) override;
    objstripe::ProductGroupStripe data_;
    std::vector<std::vector<size_t>> offsets_{};
    std::vector<std::string> content_{};
};

class DelayedProductStripeRetriever {
  public:
    // Note: for ProductStripes not in a ProductGroupStripe, groupIdx is ignored
    DelayedProductStripeRetriever(const std::shared_ptr<WaitableFetch>& fetcher, size_t groupIdx, size_t globalOffset, size_t flushSize):
      fetcher_(fetcher), groupIdx_(groupIdx), globalOffset_(globalOffset), flushSize_(flushSize) {};
    DelayedProductStripeRetriever(const S3ConnectionRef& conn, const std::string& name, size_t globalOffset, size_t flushSize):
      fetcher_(std::make_shared<WaitableFetchProductStripe>(conn, name)), groupIdx_(0), globalOffset_(globalOffset), flushSize_(flushSize) {};
    void fetch(TaskHolder&& callback) const;
    std::string_view bufferAt(size_t globalEventIndex) const;
    size_t globalOffset() const { return globalOffset_; };
    bool wasFetched() const { return fetcher_->wasFetched(); };
    std::chrono::microseconds decompressTime() const { return fetcher_->parseTime(); }

  private:
    const size_t groupIdx_;
    const size_t globalOffset_;
    const size_t flushSize_;
    std::shared_ptr<WaitableFetch> fetcher_;
};

class ProductStripeGenerator {
  public:
    ProductStripeGenerator(const S3ConnectionRef& conn, const std::string& prefix, unsigned int flushSize, size_t globalIndexStart, size_t globalIndexEnd);
    std::shared_ptr<const DelayedProductStripeRetriever> stripeFor(size_t globalEventIndex);
    std::chrono::microseconds decompressTime() const { return decompressTime_; };

  private:
    const S3ConnectionRef conn_;
    const std::string prefix_;
    const unsigned int flushSize_;
    const size_t globalIndexStart_;
    const size_t globalIndexEnd_;
    std::shared_ptr<const DelayedProductStripeRetriever> currentStripe_;
    std::shared_ptr<const DelayedProductStripeRetriever> nextStripe_;
    std::chrono::microseconds decompressTime_{0};
    std::unique_ptr<tbb::task_group> prefetch_group_;
};

class S3DelayedRetriever : public DelayedProductRetriever {
  public:
    S3DelayedRetriever(objstripe::ObjectStripeIndex const&, DeserializeStrategy);
    ~S3DelayedRetriever();

    S3DelayedRetriever(S3DelayedRetriever&&) = default;
    S3DelayedRetriever(S3DelayedRetriever const&) = delete;
    S3DelayedRetriever& operator=(S3DelayedRetriever&&) = default;
    S3DelayedRetriever& operator=(S3DelayedRetriever const&) = delete;

    EventIdentifier event() const { return eventID_; }
    void setEvent(size_t globalEventIndex, EventIdentifier&& ev) { globalEventIndex_ = globalEventIndex; eventID_ = ev; }

    std::chrono::microseconds deserializeTime() const { return deserializeTime_; }

    void setStripe(size_t index, const std::shared_ptr<const DelayedProductStripeRetriever>& ptr) { stripes_[index] = ptr; }

    std::vector<DataProductRetriever>& dataProducts() { return dataProducts_; }

    void getAsync(DataProductRetriever& product, int index, TaskHolder callback) final;

  private:
    size_t globalEventIndex_;
    EventIdentifier eventID_;
    std::vector<DataProductRetriever> dataProducts_;
    std::vector<void*> dataBuffers_;
    DeserializeStrategy deserializers_;
    std::vector<std::shared_ptr<const DelayedProductStripeRetriever>> stripes_;
    std::chrono::microseconds deserializeTime_{0};
};

class S3Source : public SharedSourceBase {
  public:
    S3Source(unsigned int iNLanes, std::string iObjPrefix, int iVerbose, unsigned long long iNEvents, const S3ConnectionRef& conn);
    S3Source(S3Source&&) = delete;
    S3Source(S3Source const&) = delete;
    ~S3Source() = default;

    size_t numberOfDataProducts() const final;
    std::vector<DataProductRetriever>& dataProducts(unsigned int iLane, long iEventIndex) final;
    EventIdentifier eventIdentifier(unsigned int iLane, long iEventIndex) final;

    void printSummary() const final;

  private:
    void readEventAsync(unsigned int iLane, long iEventIndex, OptionalTaskHolder) final;

    std::chrono::microseconds serialReadTime() const;
    std::chrono::microseconds parallelReadTime() const;
    std::chrono::microseconds decompressTime() const;
    std::chrono::microseconds deserializeTime() const;

    const int verbose_;
    const std::string objPrefix_;
    const S3ConnectionRef conn_;
    SerialTaskQueue queue_;

    objstripe::ObjectStripeIndex index_;

    // mutated only by methods called in queue_
    size_t nextEventStripe_ = 0;
    size_t nextEventInStripe_ = 0;
    objstripe::EventStripe currentEventStripe_;
    std::map<std::string, std::shared_ptr<const DelayedProductStripeRetriever>> productGroupMap_;
    std::vector<ProductStripeGenerator> productRetrievers_;
    std::vector<S3DelayedRetriever> laneRetrievers_;
    std::chrono::microseconds readTime_;
};
}

#endif
