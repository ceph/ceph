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

#include "include/atomic.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/Thread.h"
#include "common/perf_counters.h"

class CephContext;

enum {
  l_finisher_first = 997082,
  l_finisher_queue_len,
  l_finisher_last
};

class Finisher {
  CephContext *cct;
  Mutex          finisher_lock;
  Cond           finisher_cond, finisher_empty_cond;
  bool           finisher_stop, finisher_running;
  vector<Context*> finisher_queue;
  list<pair<Context*,int> > finisher_queue_rval;
  PerfCounters *logger;
  
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
    if (logger)
      logger->inc(l_finisher_queue_len);
  }
  void queue(vector<Context*>& ls) {
    finisher_lock.Lock();
    finisher_queue.insert(finisher_queue.end(), ls.begin(), ls.end());
    finisher_cond.Signal();
    finisher_lock.Unlock();
    ls.clear();
    if (logger)
      logger->inc(l_finisher_queue_len);
  }
  void queue(deque<Context*>& ls) {
    finisher_lock.Lock();
    finisher_queue.insert(finisher_queue.end(), ls.begin(), ls.end());
    finisher_cond.Signal();
    finisher_lock.Unlock();
    ls.clear();
    if (logger)
      logger->inc(l_finisher_queue_len);
  }
  
  void start();
  void stop();

  void wait_for_empty();

  Finisher(CephContext *cct_) :
    cct(cct_), finisher_lock("Finisher::finisher_lock"),
    finisher_stop(false), finisher_running(false),
    logger(0),
    finisher_thread(this) {}
  Finisher(CephContext *cct_, string name) :
    cct(cct_), finisher_lock("Finisher::finisher_lock"),
    finisher_stop(false), finisher_running(false),
    logger(0),
    finisher_thread(this) {
    PerfCountersBuilder b(cct, string("finisher-") + name,
			  l_finisher_first, l_finisher_last);
    b.add_fl_avg(l_finisher_queue_len, "queue_len");
    logger = b.create_perf_counters();
    cct->get_perfcounters_collection()->add(logger);
    logger->set(l_finisher_queue_len, 0);
  }

  ~Finisher() {
    if (logger && cct) {
      cct->get_perfcounters_collection()->remove(logger);
      delete logger;
    }
  }
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
