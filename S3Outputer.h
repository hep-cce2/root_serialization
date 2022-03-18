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

#include "libs3.h"

namespace cce::tf {
class S3Outputer :public OutputerBase {
 public:
  S3Outputer(unsigned int iNLanes, bool iVerbose): serializers_(iNLanes), verbose_(iVerbose) {}

  void setupForLane(unsigned int iLaneIndex, std::vector<DataProductRetriever> const& iDPs) final {
    auto& s = serializers_[iLaneIndex];
    s.reserve(iDPs.size());
    for(auto const& dp: iDPs) {
      s.emplace_back(dp.name(), dp.classType());
    }
    if (outputProductBuffer_.size() == 0) {
      outputProductBuffer_.resize(iDPs.size());
      for (auto& p : outputProductBuffer_) {
        // initialize offsets
        std::get<1>(p).push_back(0);
      }
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
    summarize_serializers(serializers_);
    std::cout <<"S3Outputer\n  total serial time at end event: "<<serialTime_.count()<<"us\n"
      "  total parallel time at end event: "<<parallelTime_.load()<<"us\n";
  }

 private:
  void output(EventIdentifier const& iEventID, std::vector<SerializerWrapper> const& iSerializers) const {
    using namespace std::string_literals;
    if(verbose_) {
      std::cout <<"   run:"s+std::to_string(iEventID.run)+" lumi:"s+std::to_string(iEventID.lumi)+" event:"s+std::to_string(iEventID.event)+"\n"<<std::flush;
    }
    eventIDs_.push_back(iEventID);
    
    auto s = std::begin(iSerializers);
    auto p = std::begin(outputProductBuffer_);
    for(; s != std::end(iSerializers); ++s, ++p) {
      auto& [global_offset, offsets, buffer] = *p;
      size_t offset = buffer.size();
      offsets.push_back(offset);
      buffer.resize(offset + s->blob().size());
      std::copy(s->blob().begin(), s->blob().end(), buffer.begin()+offset);

      if ( buffer.size() > productBufferFlushMinSize_ ) {
        size_t bufferNevents = offsets.size() - 1;
        assert(eventIDs_.size() - global_offset == bufferNevents);
        if(verbose_) {
          std::cout << "product buffer for "s + std::string(s->name()) + " is full ("s + std::to_string(buffer.size())
            + " bytes, "s + std::to_string(bufferNevents) + " events), flushing\n" << std::flush;
        }
        // if ( goodDivisor(bufferNevents) ) ...
        // must remember chosen divisor?
        std::vector<uint32_t> offsetsOut {0};
        std::vector<char> bufferOut;
        // use current size as hint
        offsetsOut.reserve(offsets.size());
        bufferOut.reserve(buffer.size());

        global_offset += bufferNevents;
        std::swap(offsets, offsetsOut);
        std::swap(buffer, bufferOut);
        // writeAsync(offsetsOut, bufferOut);
      }
    }

    // if ( eventIDs_.size() > eventFlushSize_ )
    // any buffers with global_offset > 0 should be empty
    // because the sizes all evenly divide eventFlushSize_
    // the rest never got big enough, write them out now
    // merge some together to respect productBufferFlushMinSize_?
  }
private:
  mutable std::vector<std::vector<SerializerWrapper>> serializers_;
  mutable SerialTaskQueue queue_;

  // configuration options
  bool verbose_;
  size_t productBufferFlushMinSize_{1024*512};
  size_t eventFlushSize_{24};

  // starting event index (into eventIDs_), byte offset for each event, contiguous serialized product data
  using ProductInfo = std::tuple<size_t, std::vector<uint32_t>, std::vector<char>>;
  // data product order matches serializers_ inner vector
  mutable std::vector<ProductInfo> outputProductBuffer_;
  mutable std::vector<EventIdentifier> eventIDs_;

  mutable std::chrono::microseconds serialTime_;
  mutable std::atomic<std::chrono::microseconds::rep> parallelTime_;
};
}
#endif
