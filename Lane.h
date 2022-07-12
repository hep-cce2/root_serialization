#if !defined(Lane_h)
#define Lane_h

#include <vector>
#include <atomic>
#include <memory>

#include "tbb/task_group.h"

#include "SharedSourceBase.h"
#include "OutputerBase.h"
#include "WaiterBase.h"

namespace cce::tf {
class Lane {
public:
  Lane(unsigned int iIndex, SharedSourceBase* iSource, WaiterBase const* iWaiter);

  void processEventsAsync(std::atomic<long>& index, tbb::task_group& group, const OutputerBase& outputer, TaskHolder finalTask);

  void setVerbose(bool iSet) { verbose_ = iSet; }

  std::vector<DataProductRetriever> const& dataProducts() const { return source_->dataProducts(index_, presentEventIndex_); }

  long presentEventIndex() const { return presentEventIndex_;}
private:

  std::vector<DataProductRetriever>& mutableDataProducts() { return source_->dataProducts(index_, presentEventIndex_); }
  TaskHolder makeWaiterTask(tbb::task_group& group, size_t index, TaskHolder holder) ;

  TaskHolder makeTaskForDataProduct(tbb::task_group& group, size_t index, DataProductRetriever& iDP, OutputerBase const& outputer, TaskHolder holder) ;

  void processEventAsync(tbb::task_group& group, TaskHolder iCallback, const OutputerBase& outputer);

  void doNextEvent(std::atomic<long>& index, tbb::task_group& group,  const OutputerBase& outputer, 
		   TaskHolder finalTask);

  SharedSourceBase* source_;
  WaiterBase const* waiter_;
  long presentEventIndex_ = -1;
  unsigned int index_;
  bool verbose_ = false;
};
}
#endif
