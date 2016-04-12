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

/// Finisher queue length performance counter ID.
enum {
  l_finisher_first = 997082,
  l_finisher_queue_len,
  l_finisher_complete_lat,
  l_finisher_last
};

/** @brief Asynchronous cleanup class.
 * Finisher asynchronously completes Contexts, which are simple classes
 * representing callbacks, in a dedicated worker thread. Enqueuing
 * contexts to complete is thread-safe.
 */
class Finisher {
  CephContext *cct;
  Mutex        finisher_lock; ///< Protects access to queues and finisher_running.
  Cond         finisher_cond; ///< Signaled when there is something to process.
  Cond         finisher_empty_cond; ///< Signaled when the finisher has nothing more to process.
  bool         finisher_stop; ///< Set when the finisher should stop.
  bool         finisher_running; ///< True when the finisher is currently executing contexts.
  /// Queue for contexts for which complete(0) will be called.
  /// NULLs in this queue indicate that an item from finisher_queue_rval
  /// should be completed in that place instead.
  vector<Context*> finisher_queue;

  string thread_name;

  /// Queue for contexts for which the complete function will be called
  /// with a parameter other than 0.
  list<pair<Context*,int> > finisher_queue_rval;

  /// Performance counter for the finisher's queue length.
  /// Only active for named finishers.
  PerfCounters *logger;
  
  void *finisher_thread_entry();

  struct FinisherThread : public Thread {
    Finisher *fin;    
    explicit FinisherThread(Finisher *f) : fin(f) {}
    void* entry() { return (void*)fin->finisher_thread_entry(); }
  } finisher_thread;

 public:
  /// Add a context to complete, optionally specifying a parameter for the complete function.
  void queue(Context *c, int r = 0) {
    finisher_lock.Lock();
    if (finisher_queue.empty()) {
      finisher_cond.Signal();
    }
    if (r) {
      finisher_queue_rval.push_back(pair<Context*, int>(c, r));
      finisher_queue.push_back(NULL);
    } else
      finisher_queue.push_back(c);
    if (logger)
      logger->inc(l_finisher_queue_len);
    finisher_lock.Unlock();
  }
  void queue(vector<Context*>& ls) {
    finisher_lock.Lock();
    if (finisher_queue.empty()) {
      finisher_cond.Signal();
    }
    finisher_queue.insert(finisher_queue.end(), ls.begin(), ls.end());
    if (logger)
      logger->inc(l_finisher_queue_len, ls.size());
    finisher_lock.Unlock();
    ls.clear();
  }
  void queue(deque<Context*>& ls) {
    finisher_lock.Lock();
    if (finisher_queue.empty()) {
      finisher_cond.Signal();
    }
    finisher_queue.insert(finisher_queue.end(), ls.begin(), ls.end());
    if (logger)
      logger->inc(l_finisher_queue_len, ls.size());
    finisher_lock.Unlock();
    ls.clear();
  }
  void queue(list<Context*>& ls) {
    finisher_lock.Lock();
    if (finisher_queue.empty()) {
      finisher_cond.Signal();
    }
    finisher_queue.insert(finisher_queue.end(), ls.begin(), ls.end());
    if (logger)
      logger->inc(l_finisher_queue_len, ls.size());
    finisher_lock.Unlock();
    ls.clear();
  }

  /// Start the worker thread.
  void start();

  /** @brief Stop the worker thread.
   *
   * Does not wait until all outstanding contexts are completed.
   * To ensure that everything finishes, you should first shut down
   * all sources that can add contexts to this finisher and call
   * wait_for_empty() before calling stop(). */
  void stop();

  /** @brief Blocks until the finisher has nothing left to process.
   * This function will also return when a concurrent call to stop()
   * finishes, but this class should never be used in this way. */
  void wait_for_empty();

  /// Construct an anonymous Finisher.
  /// Anonymous finishers do not log their queue length.
  explicit Finisher(CephContext *cct_) :
    cct(cct_), finisher_lock("Finisher::finisher_lock"),
    finisher_stop(false), finisher_running(false),
    thread_name("fn_anonymous"), logger(0),
    finisher_thread(this) {}

  /// Construct a named Finisher that logs its queue length.
  Finisher(CephContext *cct_, string name, string tn) :
    cct(cct_), finisher_lock("Finisher::finisher_lock"),
    finisher_stop(false), finisher_running(false),
    thread_name(tn), logger(0),
    finisher_thread(this) {
    PerfCountersBuilder b(cct, string("finisher-") + name,
			  l_finisher_first, l_finisher_last);
    b.add_u64(l_finisher_queue_len, "queue_len");
    b.add_time_avg(l_finisher_complete_lat, "complete_latency");
    logger = b.create_perf_counters();
    cct->get_perfcounters_collection()->add(logger);
    logger->set(l_finisher_queue_len, 0);
    logger->set(l_finisher_complete_lat, 0);
  }

  ~Finisher() {
    if (logger && cct) {
      cct->get_perfcounters_collection()->remove(logger);
      delete logger;
    }
  }
};

/// Context that is completed asynchronously on the supplied finisher.
class C_OnFinisher : public Context {
  Context *con;
  Finisher *fin;
public:
  C_OnFinisher(Context *c, Finisher *f) : con(c), fin(f) {
    assert(fin != NULL);
    assert(con != NULL);
  }
  void finish(int r) {
    fin->queue(con, r);
  }
};

#endif
