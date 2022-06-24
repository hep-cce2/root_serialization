#if !defined(S3Source_h)
#define S3Source_h

#include <string>
#include <memory>
#include <chrono>
#include <iostream>
#include <utility>

#include "SharedSourceBase.h"
#include "DataProductRetriever.h"
#include "DelayedProductRetriever.h"
#include "SerialTaskQueue.h"
#include "DeserializeStrategy.h"
#include "S3Common.h"
#include "objectstripe.pb.h"


namespace cce::tf {
class S3DelayedRetriever : public DelayedProductRetriever {
  void getAsync(DataProductRetriever&, int index, TaskHolder) final {}
};

class DelayedProductStripeRetriever {
  public:
    DelayedProductStripeRetriever(S3ConnectionRef conn, std::string name, size_t globalOffset):
      conn_(conn), name_(name), globalOffset_(globalOffset) {};
    std::pair<const char*, size_t> bufferAt(size_t globalEventIndex) const;
    ~DelayedProductStripeRetriever() {};

  private:
    S3ConnectionRef conn_;
    std::string name_;
    size_t globalOffset_;

    mutable std::once_flag flag_;
    mutable objstripe::ProductStripe data_;
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

    int verbose_;
    std::string objPrefix_;
    S3ConnectionRef conn_;
    SerialTaskQueue queue_;

    objstripe::ObjectStripeIndex index_;

    struct LaneInfo {
      LaneInfo(objstripe::ObjectStripeIndex const&, DeserializeStrategy);

      LaneInfo(LaneInfo&&) = default;
      LaneInfo(LaneInfo const&) = delete;

      LaneInfo& operator=(LaneInfo&&) = default;
      LaneInfo& operator=(LaneInfo const&) = delete;

      EventIdentifier eventID_;
      std::vector<DataProductRetriever> dataProducts_;
      std::vector<void*> dataBuffers_;
      DeserializeStrategy deserializers_;
      S3DelayedRetriever delayedRetriever_;
      std::chrono::microseconds readTime_{0};
      std::chrono::microseconds decompressTime_{0};
      std::chrono::microseconds deserializeTime_{0};
      ~LaneInfo();
    };

    size_t nextEventStripe_ = 0;
    size_t nextEventInStripe_ = 0;
    objstripe::EventStripe currentEventStripe_;
    std::vector<std::shared_ptr<const DelayedProductStripeRetriever>> currentProductStripes_;

    std::vector<LaneInfo> laneInfos_;
    std::chrono::microseconds readTime_;
};
}

#endif
