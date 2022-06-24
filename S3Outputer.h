#if !defined(S3Outputer_h)
#define S3Outputer_h

#include <vector>
#include <string>
#include <iostream>
#include <cassert>

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
class S3Outputer : public OutputerBase {
 public:
  S3Outputer(unsigned int iNLanes, std::string objPrefix, int iVerbose, size_t iProductBufferFlush, size_t iEventFlushSize, S3ConnectionRef conn):
    serializers_(iNLanes),
    objPrefix_(objPrefix),
    verbose_(iVerbose),
    productBufferFlushMinSize_(iProductBufferFlush),
    eventFlushSize_(iEventFlushSize),
    conn_(conn),
    serialTime_{std::chrono::microseconds::zero()},
    parallelTime_{0}
  {
    index_.set_eventstripesize(eventFlushSize_);
    // TODO: make configurable
    index_.set_serializestrategy(objstripe::SerializeStrategy::kRoot);
    currentEventStripe_.mutable_events()->Reserve(eventFlushSize_);
  }

  void setupForLane(unsigned int iLaneIndex, std::vector<DataProductRetriever> const& iDPs) final;

  void productReadyAsync(unsigned int iLaneIndex, DataProductRetriever const& iDataProduct, TaskHolder iCallback) const final {
    assert(iLaneIndex < serializers_.size());
    auto& laneSerializers = serializers_[iLaneIndex];
    auto group = iCallback.group();
    assert(iDataProduct.index() < laneSerializers.size() );
    laneSerializers[iDataProduct.index()].doWorkAsync(*group, iDataProduct.address(), std::move(iCallback));
  }

  bool usesProductReadyAsync() const final {return true; }

  void outputAsync(unsigned int iLaneIndex, EventIdentifier const& iEventID, TaskHolder iCallback) const final {
    auto start = std::chrono::high_resolution_clock::now();
    // all products
    queue_.push(*iCallback.group(), [this, iEventID, iLaneIndex, callback=std::move(iCallback)]() mutable {
        auto start = std::chrono::high_resolution_clock::now();
        output(iEventID, serializers_[iLaneIndex], std::move(callback));
        serialTime_ += std::chrono::duration_cast<decltype(serialTime_)>(std::chrono::high_resolution_clock::now() - start);
      });
    auto time = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start);
    parallelTime_ += time.count();
  }

  void printSummary() const final;

private:
  void output(EventIdentifier const& iEventID, SerializeStrategy const& iSerializers, TaskHolder iCallback) const;
  void flushProductStripes(TaskHolder iCallback, bool last=false) const;
  void flushEventStripe(TaskHolder iCallback, bool last=false) const;

  mutable std::vector<SerializeStrategy> serializers_;
  mutable SerialTaskQueue queue_;

  // configuration options
  int verbose_;
  std::string objPrefix_;
  size_t productBufferFlushMinSize_;
  size_t eventFlushSize_;
  S3ConnectionRef conn_;

  // mutated only by methods called in queue_
  mutable objstripe::ObjectStripeIndex index_;
  mutable objstripe::EventStripe currentEventStripe_;
  mutable std::vector<objstripe::ProductStripe> currentProductStripes_;
  mutable size_t eventGlobalOffset_{0};

  mutable std::chrono::microseconds serialTime_;
  mutable std::atomic<std::chrono::microseconds::rep> parallelTime_;
};
}
#endif
