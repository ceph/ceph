// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef LIBRBD_TASK_FINISHER_H
#define LIBRBD_TASK_FINISHER_H

#include "include/Context.h"
#include "common/Finisher.h"
#include "common/Mutex.h"
#include "common/Timer.h"
#include <map>
#include <utility>

class CephContext;

namespace librbd {

struct TaskFinisherSingleton {
  Mutex m_lock;
  SafeTimer *m_safe_timer;
  Finisher *m_finisher;

  explicit TaskFinisherSingleton(CephContext *cct)
    : m_lock("librbd::TaskFinisher::m_lock") {
    m_safe_timer = new SafeTimer(cct, m_lock, false);
    m_safe_timer->init();
    m_finisher = new Finisher(cct, "librbd::TaskFinisher::m_finisher", "taskfin_librbd");
    m_finisher->start();
  }
  virtual ~TaskFinisherSingleton() {
    {
      Mutex::Locker l(m_lock);
      m_safe_timer->shutdown();
      delete m_safe_timer;
    }
    m_finisher->wait_for_empty();
    m_finisher->stop();
    delete m_finisher;
  }
};


template <typename Task>
class TaskFinisher {
public:
  TaskFinisher(CephContext &cct) : m_cct(cct) {
    TaskFinisherSingleton *singleton;
    cct.lookup_or_create_singleton_object<TaskFinisherSingleton>(
      singleton, "librbd::TaskFinisher::m_safe_timer");
    m_lock = &singleton->m_lock;
    m_safe_timer = singleton->m_safe_timer;
    m_finisher = singleton->m_finisher;
  }

  void cancel(const Task& task) {
    Mutex::Locker l(*m_lock);
    typename TaskContexts::iterator it = m_task_contexts.find(task);
    if (it != m_task_contexts.end()) {
      delete it->second.first;
      m_safe_timer->cancel_event(it->second.second);
      m_task_contexts.erase(it);
    }
  }

  void cancel_all(Context *comp) {
    {
      Mutex::Locker l(*m_lock);
      for (typename TaskContexts::iterator it = m_task_contexts.begin();
           it != m_task_contexts.end(); ++it) {
        delete it->second.first;
        m_safe_timer->cancel_event(it->second.second);
      }
      m_task_contexts.clear();
    }
    m_finisher->queue(comp);
  }

  bool add_event_after(const Task& task, double seconds, Context *ctx) {
    Mutex::Locker l(*m_lock);
    if (m_task_contexts.count(task) != 0) {
      // task already scheduled on finisher or timer
      delete ctx;
      return false;
    }
    C_Task *timer_ctx = new C_Task(this, task);
    m_task_contexts[task] = std::make_pair(ctx, timer_ctx);

    m_safe_timer->add_event_after(seconds, timer_ctx);
    return true;
  }

  void queue(Context *ctx) {
    m_finisher->queue(ctx);
  }

  bool queue(const Task& task, Context *ctx) {
    Mutex::Locker l(*m_lock);
    typename TaskContexts::iterator it = m_task_contexts.find(task);
    if (it != m_task_contexts.end()) {
      if (it->second.second != NULL) {
        assert(m_safe_timer->cancel_event(it->second.second));
        delete it->second.first;
      } else {
        // task already scheduled on the finisher
        delete ctx;
        return false;
      }
    }
    m_task_contexts[task] = std::make_pair(ctx, reinterpret_cast<Context *>(0));

    m_finisher->queue(new C_Task(this, task));
    return true;
  }

private:
  class C_Task : public Context {
  public:
    C_Task(TaskFinisher *task_finisher, const Task& task)
      : m_task_finisher(task_finisher), m_task(task)
    {
    }
  protected:
    virtual void finish(int r) {
      m_task_finisher->complete(m_task);
    }
  private:
    TaskFinisher *m_task_finisher;
    Task m_task;
  };

  CephContext &m_cct;

  Mutex *m_lock;
  Finisher *m_finisher;
  SafeTimer *m_safe_timer;

  typedef std::map<Task, std::pair<Context *, Context *> > TaskContexts;
  TaskContexts m_task_contexts;

  void complete(const Task& task) {
    Context *ctx = NULL;
    {
      Mutex::Locker l(*m_lock);
      typename TaskContexts::iterator it = m_task_contexts.find(task);
      if (it != m_task_contexts.end()) {
        ctx = it->second.first;
        m_task_contexts.erase(it);
      }
    }

    if (ctx != NULL) {
      ctx->complete(0);
    }
  }
};

} // namespace librbd

#endif // LIBRBD_TASK_FINISHER
