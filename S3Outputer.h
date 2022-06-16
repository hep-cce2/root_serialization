#if !defined(S3Outputer_h)
#define S3Outputer_h

#include <vector>
#include <string>
#include <iostream>
#include <cassert>

#include "tbb/task_group.h"

#include "OutputerBase.h"
#include "EventIdentifier.h"
#include "SerializerWrapper.h"
#include "DataProductRetriever.h"
#include "summarize_serializers.h"
#include "SerialTaskQueue.h"
#include "S3Common.h"
#include "FunctorTask.h"
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
    currentEventStripe_.mutable_events()->Reserve(eventFlushSize_);
  }

  void setupForLane(unsigned int iLaneIndex, std::vector<DataProductRetriever> const& iDPs) final {
    auto& s = serializers_[iLaneIndex];
    s.reserve(iDPs.size());
    for(auto const& dp: iDPs) {
      s.emplace_back(dp.name(), dp.classType());
    }
    if (currentProductStripes_.size() == 0) {
      currentProductStripes_.resize(iDPs.size());
      index_.mutable_products()->Reserve(iDPs.size());
      for(auto const& dp: iDPs) {
        auto prod = index_.add_products();
        prod->set_productname(dp.name());
        prod->set_flushsize(0);
      }
    }
    // all lanes see same products? if not we'll need a map
    assert(currentProductStripes_.size() == iDPs.size());
  }

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

  void printSummary() const final {
    {
      tbb::task_group group;
      {
        auto start = std::chrono::high_resolution_clock::now();
        TaskHolder th(group, make_functor_task([](){}));
        flushProductStripes(th, true);
        flushEventStripe(th, true);
        serialTime_ += std::chrono::duration_cast<decltype(serialTime_)>(std::chrono::high_resolution_clock::now() - start);
      }
      group.wait();
    }

    if(verbose_ >= 2) {
      summarize_serializers(serializers_);
    }
    std::chrono::microseconds serializerTime = std::chrono::microseconds::zero();
    for(const auto& lane : serializers_) {
      for(const auto& s : lane) {
        serializerTime += s.accumulatedTime();
      }
    }
    std::cout <<"S3Outputer\n  total serial time at end event: "<<serialTime_.count()<<"us\n"
      "  total non-serializer parallel time at end event: "<<parallelTime_.load()<<"us\n"
      "  total serializer parallel time at end event: "<<serializerTime.count()<<"us\n";
  }

private:
  void output(EventIdentifier const& iEventID, std::vector<SerializerWrapper> const& iSerializers, TaskHolder iCallback) const;
  void flushProductStripes(TaskHolder iCallback, bool last=false) const;
  void flushEventStripe(TaskHolder iCallback, bool last=false) const;

  mutable std::vector<std::vector<SerializerWrapper>> serializers_;
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
