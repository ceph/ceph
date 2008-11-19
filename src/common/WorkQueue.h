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
  
  Mutex lock;
  Cond cond;
  Mutex queue_lock;
  bool _stop, _pause;
  int processing;
  Cond wait_cond;

  void entry() {
    lock.Lock();
    while (!_stop) {
      if (!_pause) {
	queue_lock.Lock();
	T *item = _dequeue();
	queue_lock.Unlock();
	if (item) {
	  processing++;
	  lock.Unlock();
	  _process(item);
	  lock.Lock();
	  processing--;
	  if (_pause)
	    wait_cond.Signal();
	  continue;
	}
      }
      cond.Wait(lock);
    }
    lock.Unlock();
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
    lock(string(name + "::lock").c_str()),
    queue_lock(string(name + "::queue_lock").c_str()),
    _stop(false), _pause(false),
    processing(0),
    thread(this) {}

  virtual void _enqueue(T *) = 0;
  virtual void _dequeue(T *) = 0;
  virtual T *_dequeue() = 0;
  virtual void _process(T *) = 0;

  void start() {
    thread.create();
  }
  void stop() {
    lock.Lock();
    _stop = true;
    cond.Signal();
    lock.Unlock();
    thread.join();
  }
  void kick() {
    lock.Lock();
    cond.Signal();
    lock.Unlock();
  }

  void pause() {
    lock.Lock();
    assert(!_pause);
    _pause = true;
    while (processing)
      wait_cond.Wait(lock);
    lock.Unlock();
  }

  void unpause() {
    lock.Lock();
    assert(_pause);
    _pause = false;
    cond.Signal();
    lock.Unlock();
  }

  void queue(T *item) {
    queue_lock.Lock();
    _enqueue(item);
    cond.Signal();
    queue_lock.Unlock();
  }
  void dequeue(T *item) {
    queue_lock.Lock();
    _dequeue(item);
    queue_lock.Unlock();
  }

};


#endif
