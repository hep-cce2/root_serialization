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
        output(iEventID, serializers_[iLaneIndex], std::move(callback));
        serialTime_ += std::chrono::duration_cast<decltype(serialTime_)>(std::chrono::high_resolution_clock::now() - start);
      });
    auto time = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start);
    parallelTime_ += time.count();
  }

  void printSummary() const final {
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
  void output(EventIdentifier const& iEventID, std::vector<SerializerWrapper> const& iSerializers, TaskHolder iCallback) const {
    using namespace std::string_literals;
    if(verbose_ >= 2) {
      std::cout <<"   run:"s+std::to_string(iEventID.run)+" lumi:"s+std::to_string(iEventID.lumi)+" event:"s+std::to_string(iEventID.event)+"\n"<<std::flush;
    }
    eventIDs_.push_back(iEventID);
    
    auto s = std::begin(iSerializers);
    auto p = std::begin(outputProductBuffer_);
    for(; s != std::end(iSerializers); ++s, ++p) {
      auto& [productGlobalOffset, productFlushSize, offsets, buffer] = *p;
      size_t offset = buffer.size();
      offsets.push_back(offset);
      buffer.resize(offset + s->blob().size());
      std::copy(s->blob().begin(), s->blob().end(), buffer.begin()+offset);
      size_t bufferNevents = offsets.size();

      // first flush when we exceed min size and have an even divisor of eventFlushSize_
      // subsequent flush when we reach productFlushSize
      // always flush when we reach eventFlushSize_ (for buffers that never get big enough)
      if (
          ((productFlushSize == 0) && (buffer.size() > productBufferFlushMinSize_) && (eventFlushSize_ % bufferNevents == 0))
          || (bufferNevents == productFlushSize)
          || (bufferNevents == eventFlushSize_)
         )
      {
        assert(eventGlobalOffset_ + eventIDs_.size() - productGlobalOffset == bufferNevents);
        if(verbose_ >= 2) {
          std::cout << "product buffer for "s + std::string(s->name()) + " is full ("s + std::to_string(buffer.size())
            + " bytes, "s + std::to_string(bufferNevents) + " events), flushing\n" << std::flush;
        }
        std::vector<uint32_t> offsetsOut;
        std::vector<char> bufferOut;
        // use current size as hint
        offsetsOut.reserve(offsets.size());
        bufferOut.reserve(buffer.size());

        std::swap(offsets, offsetsOut);
        std::swap(buffer, bufferOut);
        iCallback.group()->run(
            [this, productName=s->name(), productGlobalOffset, offsets=std::move(offsetsOut), buffer=std::move(bufferOut), callback=iCallback]() {
              auto start = std::chrono::high_resolution_clock::now();
              // concatenate offsets and buffer
              std::vector<char> finalbuf(
                  sizeof(decltype(productGlobalOffset))
                  + offsets.size()*sizeof(decltype(offsets)::value_type)
                  + buffer.size()
                  );
              auto it = std::begin(finalbuf);
              it = std::copy_n(reinterpret_cast<const char*>(&productGlobalOffset), sizeof(decltype(productGlobalOffset)), it);
              it = std::copy(std::begin(offsets), std::end(offsets), it);
              it = std::copy(std::begin(buffer), std::end(buffer), it);
              // TODO can we clear offsets and buffer yet?
              assert(it == std::end(finalbuf));
              std::string name(productName);
              name += std::to_string(productGlobalOffset);
              conn_->put(name, std::move(finalbuf), [name, callback=std::move(callback)](S3Request* req) {
                  if ( req->status != S3Request::Status::ok ) {
                    std::cerr << "failed to write product buffer " << name << std::endl;
                  }
                });
              auto time = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start);
              parallelTime_ += time.count();
            }
        );
        productGlobalOffset += bufferNevents;
        productFlushSize = bufferNevents;
      }
    }

    if ( eventIDs_.size() == eventFlushSize_ ) {
      if(verbose_ >= 2) {
        std::cout << "reached event flush size "s + std::to_string(eventFlushSize_) + ", flushing\n" << std::flush;
      }
      // all buffers should be empty because the sizes all evenly divide eventFlushSize_
      for(auto& p : outputProductBuffer_) {
        auto& [productGlobalOffset, productFlushSize, offsets, buffer] = p;
        assert(bufferNevents == 0);
        assert(productGlobalOffset == eventGlobalOffset_ + eventFlushSize_);
      }
      std::vector<EventIdentifier> eventIDsOut;
      eventIDsOut.reserve(eventFlushSize_);
      std::swap(eventIDs_, eventIDsOut);
      iCallback.group()->run(
          [this, events=std::move(eventIDsOut), callback=iCallback]() {
            auto start = std::chrono::high_resolution_clock::now();

            // serialize EventIdentifier
            constexpr unsigned int headerBufferSizeInWords = 5;
            std::vector<char> finalbuf(headerBufferSizeInWords*4*events.size());
            auto it = begin(finalbuf);
            size_t iev = eventGlobalOffset_;
            std::array<uint32_t,headerBufferSizeInWords> buffer;
            for(const auto& ev : events) {
              buffer[0] = iev++;
              buffer[1] = ev.run;
              buffer[2] = ev.lumi;
              buffer[3] = (ev.event >> 32) & 0xFFFFFFFF;
              buffer[4] = ev.event & 0xFFFFFFFF;
              it = std::copy(begin(buffer), end(buffer), it);
            }
            assert(it == std::end(finalbuf));
            // TODO: can we clear events?
            std::string name = std::to_string(eventGlobalOffset_);
            conn_->put(name, std::move(finalbuf), [name, callback=std::move(callback)](S3Request* req) {
                if ( req->status != S3Request::Status::ok ) {
                  std::cerr << "failed to write event index buffer " << name << std::endl;
                }
              });
            auto time = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::high_resolution_clock::now() - start);
            parallelTime_ += time.count();
          }
      );
      eventGlobalOffset_ += eventFlushSize_;
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
  mutable size_t eventGlobalOffset_;

  mutable std::chrono::microseconds serialTime_;
  mutable std::atomic<std::chrono::microseconds::rep> parallelTime_;
};
}
#endif
