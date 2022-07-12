#include <utility>
#include <iostream>
#include <string>

#include "Lane.h"
#include "FunctorTask.h"

using namespace cce::tf;

Lane::Lane(unsigned int iIndex, SharedSourceBase* iSource, WaiterBase const* iWaiter): source_(iSource), waiter_(iWaiter), index_{iIndex} {
}

void Lane::processEventsAsync(std::atomic<long>& index, tbb::task_group& group, const OutputerBase& outputer, 
			      TaskHolder finalTask) {
  doNextEvent(index, group,  outputer, std::move(finalTask));
}


TaskHolder Lane::makeWaiterTask(tbb::task_group& group, size_t index, TaskHolder holder) {
  if(not waiter_) {
    return holder;
  } else {  
    return TaskHolder(group,
                      make_functor_task([index,  holder, this]() {
                          waiter_->waitAsync(index_, 
                                             source_->eventIdentifier(index_, presentEventIndex_),
                                             presentEventIndex_,
                                             dataProducts(),index, std::move(holder));
                        }) );
  }
}

TaskHolder Lane::makeTaskForDataProduct(tbb::task_group& group, size_t index, DataProductRetriever& iDP, OutputerBase const& outputer, TaskHolder holder) {
  if(outputer.usesProductReadyAsync()) {
    auto laneIndex = this->index_;
    return makeWaiterTask(group, index,TaskHolder(group, 
                                                  make_functor_task([holder, laneIndex, &iDP, &outputer]() {
                                                      outputer.productReadyAsync(laneIndex, iDP, std::move(holder));
                                                    })));
  } else {
    return makeWaiterTask(group, index, holder);
  }
}

void Lane::processEventAsync(tbb::task_group& group, TaskHolder iCallback, const OutputerBase& outputer) { 
  //Process order: retrieve data product, do wait, serialize, do output, call iCallback 
  
  //std::cout <<"make process event task"<<std::endl;
  TaskHolder holder(group, 
                    make_functor_task([&outputer, this, callback=std::move(iCallback)]() {
                        outputer.outputAsync(this->index_, source_->eventIdentifier(index_, presentEventIndex_),
                                             std::move(callback));
                      }));
  
  //NOTE: I once replaced with with a tbb::parallel_for but that made the code slower and did not
  // scale as well as the number of threads were increased.
  size_t index=0;
  for(auto& d: mutableDataProducts()) {
    d.getAsync(makeTaskForDataProduct(group, index,d, outputer, holder));
    ++index;
  }
}

void Lane::doNextEvent(std::atomic<long>& index, tbb::task_group& group,  const OutputerBase& outputer, TaskHolder finalTask) {
  using namespace std::string_literals;
  presentEventIndex_ = index++;
  if(source_->mayBeAbleToGoToEvent(presentEventIndex_)) {
    if(verbose_) {
      std::cout <<"event "+std::to_string(presentEventIndex_)+"\n"<<std::flush;
    }
    
    OptionalTaskHolder processEventTask(group, make_functor_task([this,&index, &group, &outputer, finalTask]() {
          TaskHolder recursiveTask(group, make_functor_task([this, &index, &group, &outputer, finalTask]() {
                doNextEvent(index, group, outputer, std::move(finalTask));
              }));
          processEventAsync(group, std::move(recursiveTask), outputer);
        }) );
    source_->gotoEventAsync(this->index_, presentEventIndex_, std::move(processEventTask));
  }
}
