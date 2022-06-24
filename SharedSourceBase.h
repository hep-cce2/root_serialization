#if !defined(SharedSourceBase_h)
#define SharedSourceBase_h

#include "DataProductRetriever.h"
#include "EventIdentifier.h"
#include "OptionalTaskHolder.h"

#include <vector>
#include <chrono>

namespace cce::tf {
class SharedSourceBase {

 public:
  explicit SharedSourceBase(unsigned long long iNEvents): 
  maxNEvents_{iNEvents} {}
  virtual ~SharedSourceBase() {}

  virtual size_t numberOfDataProducts() const = 0;
  virtual std::vector<DataProductRetriever>& dataProducts(unsigned int iLane, long iEventIndex) = 0;
  virtual EventIdentifier eventIdentifier(unsigned int iLane, long iEventIndex) = 0;

  bool mayBeAbleToGoToEvent(long int iEventIndex) const;

  //returns false if can immediately tell that can not continue processing
  void gotoEventAsync(unsigned int iLane, long iEventIndex, OptionalTaskHolder);

  virtual void printSummary() const = 0;

 private:
  //NOTE: fully reentrant sources can do their work during this call without needing to create a new Task. 
  // If can not process the event, do not convert the OptionalTaskHolder to a TaskHolder
  virtual void readEventAsync(unsigned int iLane, long iEventIndex,  OptionalTaskHolder) =0;

  const unsigned long long maxNEvents_;
};

 inline bool SharedSourceBase::mayBeAbleToGoToEvent(long iEventIndex) const {
   return iEventIndex < maxNEvents_;
 }

 inline void SharedSourceBase::gotoEventAsync(unsigned int iLane, long iEventIndex, OptionalTaskHolder iTask) {
   return readEventAsync(iLane, iEventIndex, std::move(iTask));
 }
}
#endif
