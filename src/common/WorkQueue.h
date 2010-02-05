// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef __CEPH_WORKQUEUE
#define __CEPH_WORKQUEUE

#include "Mutex.h"
#include "Cond.h"
#include "Thread.h"

class ThreadPool {
  string name;
  Mutex _lock;
  Cond _cond;
  bool _stop, _pause, _draining;
  Cond _wait_cond;

  struct _WorkQueue {
    string name;
    _WorkQueue(string n) : name(n) {}
    virtual ~_WorkQueue() {}
    virtual void _clear() = 0;
    virtual void *_void_dequeue() = 0;
    virtual void _void_process(void *) = 0;
    virtual void _void_process_finish(void *) = 0;
  };  

public:
  template<class T>
  class WorkQueue : public _WorkQueue {
    ThreadPool *pool;
    
    virtual bool _enqueue(T *) = 0;
    virtual void _dequeue(T *) = 0;
    virtual T *_dequeue() = 0;
    virtual void _process(T *) = 0;
    virtual void _process_finish(T *) {}
    virtual void _clear() = 0;
    
    void *_void_dequeue() {
      return (void *)_dequeue();
    }
    void _void_process(void *p) {
      _process((T *)p);
    }
    void _void_process_finish(void *p) {
      _process_finish((T *)p);
    }

  public:
    WorkQueue(string n, ThreadPool *p) : _WorkQueue(n), pool(p) {
      pool->add_work_queue(this);
    }
    ~WorkQueue() {
      pool->remove_work_queue(this);
    }
    
    bool queue(T *item) {
      pool->_lock.Lock();
      bool r = _enqueue(item);
      pool->_cond.SignalOne();
      pool->_lock.Unlock();
      return r;
    }
    void dequeue(T *item) {
      pool->_lock.Lock();
      _dequeue(item);
      pool->_lock.Unlock();
    }
    void clear() {
      pool->_lock.Lock();
      _clear();
      pool->_lock.Unlock();
    }

    void lock() {
      pool->lock();
    }
    void unlock() {
      pool->unlock();
    }
    void _kick() {
      pool->_kick();
    }

  };

private:
  vector<_WorkQueue*> work_queues;
  int last_work_queue;
 

  // threads
  struct WorkThread : public Thread {
    ThreadPool *pool;
    WorkThread(ThreadPool *p) : pool(p) {}
    void *entry() {
      pool->worker();
      return 0;
    }
  };
  
  set<WorkThread*> _threads;
  int processing;

  void worker();

public:
  ThreadPool(string nm, int n=1) :
    name(nm),
    _lock((new string(name + "::lock"))->c_str()),  // deliberately leak this
    _stop(false),
    _pause(false), _draining(false),
    last_work_queue(0),
    processing(0) {
    set_num_threads(n);
  }
  ~ThreadPool() {
    for (set<WorkThread*>::iterator p = _threads.begin();
	 p != _threads.end();
	 p++)
      delete *p;
  }
  
  void add_work_queue(_WorkQueue *wq) {
    work_queues.push_back(wq);
  }
  void remove_work_queue(_WorkQueue *wq) {
    unsigned i = 0;
    while (work_queues[i] != wq)
      i++;
    for (i++; i < work_queues.size(); i++) 
      work_queues[i-1] = work_queues[i];
    assert(i == work_queues.size());
    work_queues.resize(i-1);
  }

  void set_num_threads(unsigned n) {
    while (_threads.size() < n) {
      WorkThread *t = new WorkThread(this);
      _threads.insert(t);
    }
  }

  void kick() {
    _lock.Lock();
    _cond.Signal();
    _lock.Unlock();
  }
  void _kick() {
    assert(_lock.is_locked());
    _cond.Signal();
  }
  void lock() {
    _lock.Lock();
  }
  void unlock() {
    _lock.Unlock();
  }
  void wait(Cond &c) {
    c.Wait(_lock);
  }

  void start();
  void stop(bool clear_after=true);
  void pause();
  void pause_new();
  void unpause();
  void drain();
};



#endif
