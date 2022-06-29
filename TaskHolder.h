#if !defined(TaskHolder_h)
#define TaskHolder_h

#include <memory>
#include "tbb/task_group.h"
#include "TaskBase.h"

namespace cce::tf {
class TaskHolder {
public:
  friend class WaitingTaskList;

  TaskHolder(): group_{nullptr}, task_{nullptr} {}
  TaskHolder(tbb::task_group& iGroup, std::unique_ptr<TaskBase> iTask): 
    group_{&iGroup}, task_{iTask.release()} {
      //std::cout <<"new task "<<task_<<std::endl;
      task_->increment_ref_count();
    }

  ~TaskHolder() {
    if(task_) {
      doneWaiting();
    }
  }

  TaskHolder( const TaskHolder& iOther):
    group_{iOther.group_},
    task_{iOther.task_} {
      //std::cout <<"copy holder with task "<<task_<<std::endl;
      if(task_) { task_->increment_ref_count(); }
  }
  TaskHolder(TaskHolder&& iOther):
    group_{iOther.group_},
    task_{iOther.task_} {
      //std::cout <<"move holder with task "<<task_<<std::endl;
      iOther.task_ = nullptr;
  }

  /*
  TaskHolder& operator=(TaskHolder const& iOther) {
    TaskHolder o(iOther);
    std::swap(group_, o.group_);
    std::swap(task_,o.task_);
    return *this;
  }

  TaskHolder& operator=(TaskHolder&& iOther) {
    TaskHolder o(std::move(iOther));
    std::swap(group_, o.group_);
    std::swap(task_, o.task_);
    return *this;
  }
  */
  TaskHolder& operator=(TaskHolder const&) = delete;
  TaskHolder& operator=(TaskHolder&&) = delete;

  tbb::task_group* group() { return group_;}
  void doneWaiting() {
    auto t = task_;
    task_ = nullptr;
    if(t->decrement_ref_count()) {
      //std::cout <<"Task "<<t<<std::endl;
      group_->run([t]() {
	  t->execute();
	  //std::cout <<"delete "<<t<<std::endl;
	  delete t;
	});
    }
  }
private:
  TaskBase* release_no_decrement() {
    auto t = task_;
    task_ = nullptr;
    return t;
  }

  tbb::task_group* group_;
  TaskBase* task_;
};
}
#endif
