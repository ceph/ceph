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

template<class T>
class WorkQueue {
  
  Mutex _lock;
  Cond cond;
  bool _stop, _pause;
  int processing;
  Cond wait_cond;

  void entry() {
    _lock.Lock();
    while (!_stop) {
      if (!_pause) {
	T *item = _dequeue();
	if (item) {
	  processing++;
	  _lock.Unlock();
	  _process(item);
	  _lock.Lock();
	  processing--;
	  if (_pause)
	    wait_cond.Signal();
	  continue;
	}
      }
      cond.Wait(_lock);
    }
    _lock.Unlock();
  }

  struct WorkThread : public Thread {
    WorkQueue *wq;
    WorkThread(WorkQueue *q) : wq(q) {}
    void *entry() {
      wq->entry();
      return 0;
    }
  } thread;

public:
  WorkQueue(string name) :
    _lock((new string(name + "::lock"))->c_str()),  // deliberately leak this
    _stop(false), _pause(false),
    processing(0),
    thread(this) {}

  virtual bool _enqueue(T *) = 0;
  virtual void _dequeue(T *) = 0;
  virtual T *_dequeue() = 0;
  virtual void _process(T *) = 0;
  virtual void _clear() = 0;

  void start() {
    thread.create();
  }
  void stop(bool clear_after=true) {
    _lock.Lock();
    _stop = true;
    cond.Signal();
    _lock.Unlock();
    thread.join();
    if (clear_after)
      clear();      
  }
  void kick() {
    _lock.Lock();
    cond.Signal();
    _lock.Unlock();
  }
  void _kick() {
    assert(_lock.is_locked());
    cond.Signal();
  }
  void lock() {
    _lock.Lock();
  }
  void unlock() {
    _lock.Unlock();
  }

  void pause() {
    _lock.Lock();
    assert(!_pause);
    _pause = true;
    while (processing)
      wait_cond.Wait(_lock);
    _lock.Unlock();
  }
  void pause_new() {
    _lock.Lock();
    assert(!_pause);
    _pause = true;
    _lock.Unlock();
  }

  void unpause() {
    _lock.Lock();
    assert(_pause);
    _pause = false;
    cond.Signal();
    _lock.Unlock();
  }

  bool queue(T *item) {
    _lock.Lock();
    bool r = _enqueue(item);
    cond.Signal();
    _lock.Unlock();
    return r;
  }
  void dequeue(T *item) {
    _lock.Lock();
    _dequeue(item);
    _lock.Unlock();
  }
  void clear() {
    _lock.Lock();
    _clear();
    _lock.Unlock();
  }

};


#endif
