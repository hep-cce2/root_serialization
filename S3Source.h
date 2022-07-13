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
class DelayedProductStripeRetriever {
  public:
    DelayedProductStripeRetriever(S3ConnectionRef conn, std::string name, size_t globalOffset):
      conn_(conn), name_(name), globalOffset_(globalOffset), state_{State::unretrieved} {};
    void fetch(TaskHolder&& callback) const;
    std::string_view bufferAt(size_t globalEventIndex) const;
    ~DelayedProductStripeRetriever() {};
    std::chrono::microseconds decompressTime() const { return decompressTime_; }

  private:
    S3ConnectionRef conn_;
    std::string name_;
    size_t globalOffset_;

    enum class State {unretrieved, retrieving, retrieved};
    mutable std::atomic<State> state_;
    mutable WaitingTaskList waiters_{};
    mutable objstripe::ProductStripe data_{};
    mutable std::vector<size_t> offsets_{};
    mutable std::string content_{};
    mutable std::chrono::microseconds decompressTime_{0};
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
    S3Source(unsigned int iNLanes, std::string iObjPrefix, int iVerbose, unsigned long long iNEvents, S3ConnectionRef conn);
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
    std::vector<std::shared_ptr<const DelayedProductStripeRetriever>> currentProductStripes_;
    std::vector<S3DelayedRetriever> laneRetrievers_;
    std::chrono::microseconds readTime_;
    std::chrono::microseconds decompressTime_{0};
};
}

#endif
