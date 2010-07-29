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

#ifndef CEPH_FINISHER_H
#define CEPH_FINISHER_H

#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"

class Finisher {
  Mutex          finisher_lock;
  Cond           finisher_cond, finisher_empty_cond;
  bool           finisher_stop, finisher_running;
  vector<Context*> finisher_queue;
  list<pair<Context*,int> > finisher_queue_rval;
  
  void *finisher_thread_entry();

  struct FinisherThread : public Thread {
    Finisher *fin;    
    FinisherThread(Finisher *f) : fin(f) {}
    void* entry() { return (void*)fin->finisher_thread_entry(); }
  } finisher_thread;

 public:
  void queue(Context *c, int r = 0) {
    finisher_lock.Lock();
    if (r) {
      finisher_queue_rval.push_back(pair<Context*, int>(c, r));
      finisher_queue.push_back(NULL);
    } else
      finisher_queue.push_back(c);
    finisher_cond.Signal();
    finisher_lock.Unlock();
  }
  void queue(vector<Context*>& ls) {
    finisher_lock.Lock();
    finisher_queue.insert(finisher_queue.end(), ls.begin(), ls.end());
    finisher_cond.Signal();
    finisher_lock.Unlock();
    ls.clear();
  }
  void queue(deque<Context*>& ls) {
    finisher_lock.Lock();
    finisher_queue.insert(finisher_queue.end(), ls.begin(), ls.end());
    finisher_cond.Signal();
    finisher_lock.Unlock();
    ls.clear();
  }
  
  void start();
  void stop();

  void wait_for_empty();

  Finisher() : finisher_lock("Finisher::finisher_lock"),
	       finisher_stop(false), finisher_running(false), finisher_thread(this) {}
};

class C_OnFinisher : public Context {
  Context *con;
  Finisher *fin;
public:
  C_OnFinisher(Context *c, Finisher *f) : con(c), fin(f) {}
  void finish(int r) {
    fin->queue(con, r);
  }
};

#endif
