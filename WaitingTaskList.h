#if !defined(WaitingTaskList_h)
#define WaitingTaskList_h
//
// Original Author:  Chris Jones
//         Created:  Thu Feb 21 13:46:31 CST 2013
// $Id$
//

// system include files
#include <atomic>

// user include files
#include "TaskBase.h"
#include "TaskHolder.h"

// forward declarations

namespace cce::tf {
  class WaitingTaskList {
  public:
    explicit WaitingTaskList(unsigned int iInitialSize = 2);
    WaitingTaskList(const WaitingTaskList&) = delete;                   // stop default
    const WaitingTaskList& operator=(const WaitingTaskList&) = delete;  // stop default
    ~WaitingTaskList() = default;

    void add(oneapi::tbb::task_group*, TaskBase*);
    void add(TaskHolder);

    ///Signals that the resource is now available and tasks should be spawned
    /**The owner of the resource calls this function to allow the waiting tasks to
       * start accessing it.
       * If the task fails, a non 'null' std::exception_ptr should be used.
       * To have tasks wait again one must call reset().
       * Calls to add() and doneWaiting() can safely be done concurrently.
       */
    void doneWaiting();

    ///Resets access to the resource so that added tasks will wait.
    /**The owner of the resouce calls reset() to make tasks wait.
       * Calling reset() is NOT thread safe. The system must guarantee that no tasks are
       * using the resource when reset() is called and neither add() nor doneWaiting() can
       * be called concurrently with reset().
       */
    void reset();

  private:
    /**Handles running the tasks,
       * safe to call from multiple threads
       */
    void announce();

    struct WaitNode {
      TaskBase* m_task;
      oneapi::tbb::task_group* m_group;
      std::atomic<WaitNode*> m_next;
      bool m_fromCache;

      void setNextNode(WaitNode* iNext) { m_next = iNext; }

      WaitNode* nextNode() const { return m_next; }
    };

    WaitNode* createNode(oneapi::tbb::task_group* iGroup, TaskBase* iTask);

    // ---------- member data --------------------------------
    std::atomic<WaitNode*> m_head;
    std::unique_ptr<WaitNode[]> m_nodeCache;
    unsigned int m_nodeCacheSize;
    std::atomic<unsigned int> m_lastAssignedCacheIndex;
    std::atomic<bool> m_waiting;
  };
}  // namespace cce::tf

#endif
