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

#ifndef __CEPH_FINISHER_H
#define __CEPH_FINISHER_H

#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"

class Finisher {
  Mutex          finisher_lock;
  Cond           finisher_cond;
  bool           finisher_stop;
  list<Context*> finisher_queue;
  
  void *finisher_thread_entry();

  struct FinisherThread : public Thread {
    Finisher *fin;    
    FinisherThread(Finisher *f) : fin(f) {}
    void* entry() { return (void*)fin->finisher_thread_entry(); }
  } finisher_thread;

 public:
  void queue(Context *c) {
    finisher_lock.Lock();
    finisher_queue.push_back(c);
    finisher_cond.Signal();
    finisher_lock.Unlock();
  }
  void queue(list<Context*>& ls) {
    finisher_lock.Lock();
    finisher_queue.splice(finisher_queue.end(), ls);
    finisher_cond.Signal();
    finisher_lock.Unlock();
  }
  
  void start();
  void stop();

  Finisher() : finisher_stop(false), finisher_thread(this) {}
};

#endif
