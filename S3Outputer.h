#if !defined(S3Outputer_h)
#define S3Outputer_h

#include <vector>
#include <string>
#include <iostream>
#include <cassert>

#include "OutputerBase.h"
#include "EventIdentifier.h"
#include "SerializerWrapper.h"
#include "DataProductRetriever.h"
#include "summarize_serializers.h"
#include "SerialTaskQueue.h"

#include "S3Common.h"

namespace cce::tf {
class S3Outputer : public OutputerBase {
 public:
  S3Outputer(unsigned int iNLanes, int iVerbose, size_t iProductBufferFlush, size_t iEventFlushSize, S3ConnectionRef conn):
    serializers_(iNLanes),
    verbose_(iVerbose),
    productBufferFlushMinSize_(iProductBufferFlush),
    eventFlushSize_(iEventFlushSize),
    conn_(conn),
    serialTime_{std::chrono::microseconds::zero()},
    parallelTime_{0}
  {
    eventIDs_.reserve(eventFlushSize_);
  }

  void setupForLane(unsigned int iLaneIndex, std::vector<DataProductRetriever> const& iDPs) final {
    auto& s = serializers_[iLaneIndex];
    s.reserve(iDPs.size());
    for(auto const& dp: iDPs) {
      s.emplace_back(dp.name(), dp.classType());
    }
    if (outputProductBuffer_.size() == 0) {
      outputProductBuffer_.resize(iDPs.size());
    }
    // all lanes see same products? if not we'll need a map
    assert(outputProductBuffer_.size() == iDPs.size());
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
        output(iEventID, serializers_[iLaneIndex]);
        serialTime_ += std::chrono::duration_cast<decltype(serialTime_)>(std::chrono::high_resolution_clock::now() - start);
        callback.doneWaiting();
      });
    auto time = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start);
    parallelTime_ += time.count();
  }

  void printSummary() const final {
    if(verbose_ >= 2) {
      summarize_serializers(serializers_);
    }
    std::cout <<"S3Outputer\n  total serial time at end event: "<<serialTime_.count()<<"us\n"
      "  total parallel time at end event: "<<parallelTime_.load()<<"us\n";
  }

 private:
  void output(EventIdentifier const& iEventID, std::vector<SerializerWrapper> const& iSerializers) const {
    using namespace std::string_literals;
    if(verbose_ >= 2) {
      std::cout <<"   run:"s+std::to_string(iEventID.run)+" lumi:"s+std::to_string(iEventID.lumi)+" event:"s+std::to_string(iEventID.event)+"\n"<<std::flush;
    }
    eventIDs_.push_back(iEventID);
    
    auto s = std::begin(iSerializers);
    auto p = std::begin(outputProductBuffer_);
    for(; s != std::end(iSerializers); ++s, ++p) {
      auto& [global_offset, last_flush, offsets, buffer] = *p;
      size_t offset = buffer.size();
      offsets.push_back(offset);
      buffer.resize(offset + s->blob().size());
      std::copy(s->blob().begin(), s->blob().end(), buffer.begin()+offset);
      size_t bufferNevents = offsets.size();

      // first flush when we exceed min size and have an even divisor of eventFlushSize_
      // subsequent flush when we reach last_flush
      // always flush when we reach eventFlushSize_ (for buffers that never get big enough)
      if (
          ((last_flush == 0) && (buffer.size() > productBufferFlushMinSize_) && (eventFlushSize_ % bufferNevents == 0))
          || (bufferNevents == last_flush)
          || (bufferNevents == eventFlushSize_)
         )
      {
        assert(eventIDs_.size() - global_offset == bufferNevents);
        if(verbose_ >= 2) {
          std::cout << "product buffer for "s + std::string(s->name()) + " is full ("s + std::to_string(buffer.size())
            + " bytes, "s + std::to_string(bufferNevents) + " events), flushing\n" << std::flush;
        }
        std::vector<uint32_t> offsetsOut;
        std::vector<char> bufferOut;
        // use current size as hint
        offsetsOut.reserve(offsets.size());
        bufferOut.reserve(buffer.size());

        global_offset += bufferNevents;
        last_flush = bufferNevents;
        std::swap(offsets, offsetsOut);
        std::swap(buffer, bufferOut);
        // writeAsync(offsetsOut, bufferOut);
      }
    }

    if ( eventIDs_.size() == eventFlushSize_ ) {
      if(verbose_ >= 2) {
        std::cout << "reached event flush size "s + std::to_string(eventFlushSize_) + ", flushing\n" << std::flush;
      }
      // any buffers with global_offset > 0 should be empty
      // because the sizes all evenly divide eventFlushSize_
      for(auto& p : outputProductBuffer_) {
        auto& [global_offset, last_flush, offsets, buffer] = p;
        assert(bufferNevents == 0);
        assert(global_offset == eventFlushSize_);
        global_offset = 0;
      }
      std::vector<EventIdentifier> eventIDsOut;
      eventIDsOut.reserve(eventFlushSize_);
      std::swap(eventIDs_, eventIDsOut);
      // writeAsync(eventIDsOut);
    }
  }

private:
  mutable std::vector<std::vector<SerializerWrapper>> serializers_;
  mutable SerialTaskQueue queue_;

  // configuration options
  int verbose_;
  size_t productBufferFlushMinSize_;
  size_t eventFlushSize_;
  S3ConnectionRef conn_;

  // 0: starting event index (into eventIDs_)
  // 1: last buffer flush size
  // 2: byte offset for each event
  // 3: contiguous serialized product data
  using ProductInfo = std::tuple<size_t, size_t, std::vector<uint32_t>, std::vector<char>>;
  // data product order matches serializers_ inner vector
  mutable std::vector<ProductInfo> outputProductBuffer_;
  mutable std::vector<EventIdentifier> eventIDs_;

  mutable std::chrono::microseconds serialTime_;
  mutable std::atomic<std::chrono::microseconds::rep> parallelTime_;
};
}
#endif