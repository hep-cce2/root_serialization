#if !defined(SerialRNTupleSource_h)
#define SerialRNTupleSource_h

#include <string>
#include <memory>
#include <optional>
#include <vector>
#include <atomic>

#include "DataProductRetriever.h"
#include "DelayedProductRetriever.h"

#include "SharedSourceBase.h"
#include "SerialTaskQueue.h"
#include "ROOT/RNTupleReader.hxx"

namespace cce::tf {
  class SerialRNTuplePromptRetriever : public DelayedProductRetriever {
  public:
    SerialRNTuplePromptRetriever():
      accumulatedTime_{std::chrono::microseconds::zero().count()}{}
    SerialRNTuplePromptRetriever(SerialRNTuplePromptRetriever&&) = default;
    SerialRNTuplePromptRetriever(SerialRNTuplePromptRetriever const& iOther):
      accumulatedTime_(iOther.accumulatedTime_.load()) {}
    void getAsync(DataProductRetriever&, int index, TaskHolder) final;
    std::chrono::microseconds accumulatedTime() const { return std::chrono::microseconds{accumulatedTime_.load()};}

  private:
    std::atomic<std::chrono::microseconds::rep> accumulatedTime_;
  };

  class SerialRNTupleDelayedRetriever : public DelayedProductRetriever {
  public:
    SerialRNTupleDelayedRetriever(SerialTaskQueue* iQueue,
                                  ROOT::Experimental::RNTupleReader& iReader,
                                  std::vector<std::string> const& iFieldIDs,
                                  std::vector<void*>* iProductPtrs):
      queue_(iQueue), addresses_(iProductPtrs), accumulatedTime_{std::chrono::microseconds::zero()}{
      fillViews(iReader, iFieldIDs);
    }

    void setEventIndex(std::uint64_t iIndex) { eventIndex_ = iIndex;}
    void getAsync(DataProductRetriever&, int index, TaskHolder) final;
    std::chrono::microseconds accumulatedTime() const { return accumulatedTime_;}

  private:
    void fillViews(ROOT::Experimental::RNTupleReader& iReader, std::vector<std::string> const& iFieldIDs);
    SerialTaskQueue* queue_;
    std::vector<ROOT::Experimental::RNTupleView<void, false>> views_;
    std::vector<void*>* addresses_;
    std::chrono::microseconds accumulatedTime_;
    std::uint64_t eventIndex_ = 0;
  };

  class SerialRNTupleSource : public SharedSourceBase {
  public:
    SerialRNTupleSource(unsigned iNLanes, unsigned long long iNEvents, std::string const& iName, bool iDelayReading);
    size_t numberOfDataProducts() const final {return dataProductsPerLane_[0].size();}

    std::vector<DataProductRetriever>& dataProducts(unsigned int iLane, long iEventIndex) final {
      return dataProductsPerLane_[iLane];
    }
    EventIdentifier eventIdentifier(unsigned int iLane, long iEventIndex) final {
      return identifiers_[iLane];
    }
    
    void printSummary() const final;
    std::chrono::microseconds accumulatedTime() const;
  private:
    void readEventAsync(unsigned int iLane, long iEventIndex,  OptionalTaskHolder) final;

    
    SerialTaskQueue queue_;
    std::unique_ptr<ROOT::Experimental::RNTupleReader> events_;
    long nEvents_;
    std::chrono::microseconds accumulatedTime_;

    //per lane items
    std::vector<std::unique_ptr<ROOT::Experimental::REntry>> entries_;
    std::vector<SerialRNTuplePromptRetriever> promptReaders_;
    std::vector<SerialRNTupleDelayedRetriever> delayedReaders_;
    std::vector<EventIdentifier> identifiers_;
    std::vector<std::vector<void*>> ptrToDataProducts_;
    std::vector<std::vector<DataProductRetriever>> dataProductsPerLane_;

    bool delayReading_;
  };
}

#endif
