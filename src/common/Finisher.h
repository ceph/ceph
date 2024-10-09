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

#include "include/Context.h"
#include "include/common_fwd.h"
#include "common/Thread.h"
#include "common/ceph_mutex.h"
#include "common/Cond.h"

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
  CephContext *const cct;
  ceph::mutex finisher_lock; ///< Protects access to queues and finisher_running.
  ceph::condition_variable finisher_cond; ///< Signaled when there is something to process.
  ceph::condition_variable finisher_empty_cond; ///< Signaled when the finisher has nothing more to process.
  bool         finisher_stop = false; ///< Set when the finisher should stop.
  bool         finisher_running = false; ///< True when the finisher is currently executing contexts.
  bool	       finisher_empty_wait = false; ///< True mean someone wait finisher empty.

  /// Queue for contexts for which complete(0) will be called.
  std::vector<std::pair<Context*,int>> finisher_queue;
  std::vector<std::pair<Context*,int>> in_progress_queue;

  const std::string thread_name;

  /// Performance counter for the finisher's queue length.
  /// Only active for named finishers.
  PerfCounters *logger = nullptr;

  void *finisher_thread_entry();

  struct FinisherThread : public Thread {
    Finisher *fin;
    explicit FinisherThread(Finisher *f) : fin(f) {}
    void* entry() override { return fin->finisher_thread_entry(); }
  } finisher_thread;

 public:
  /// Add a context to complete, optionally specifying a parameter for the complete function.
  void queue(Context *c, int r = 0) {
    {
      const std::lock_guard l{finisher_lock};
      const bool should_notify = finisher_queue.empty() && !finisher_running;
      finisher_queue.push_back(std::make_pair(c, r));
      if (should_notify) {
	finisher_cond.notify_one();
      }
    }

    if (logger)
      logger->inc(l_finisher_queue_len);
  }

  // TODO use C++20 concept checks instead of SFINAE
  template<typename T>
  auto queue(T &ls) -> decltype(std::distance(ls.begin(), ls.end()), void()) {
    {
      const std::lock_guard l{finisher_lock};
      const bool should_notify = finisher_queue.empty() && !finisher_running;
      for (Context *i : ls) {
	finisher_queue.push_back(std::make_pair(i, 0));
      }
      if (should_notify) {
	finisher_cond.notify_one();
      }
    }
    if (logger)
      logger->inc(l_finisher_queue_len, ls.size());
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

  bool is_empty();

  std::string_view get_thread_name() const noexcept {
    return thread_name;
  }

  /// Construct an anonymous Finisher.
  /// Anonymous finishers do not log their queue length.
  explicit Finisher(CephContext *cct_);

  /// Construct a named Finisher that logs its queue length.
  Finisher(CephContext *cct_, std::string_view name, std::string &&tn);
  ~Finisher();
};

/// Context that is completed asynchronously on the supplied finisher.
class C_OnFinisher : public Context {
  Context *con;
  Finisher *fin;
public:
  C_OnFinisher(Context *c, Finisher *f) : con(c), fin(f) {
    ceph_assert(fin != NULL);
    ceph_assert(con != NULL);
  }

  ~C_OnFinisher() override {
    if (con != nullptr) {
      delete con;
      con = nullptr;
    }
  }

  void finish(int r) override {
    fin->queue(con, r);
    con = nullptr;
  }
};

class ContextQueue {
  std::list<Context *> q;
  std::mutex q_mutex;
  ceph::mutex& mutex;
  ceph::condition_variable& cond;
  std::atomic_bool q_empty = true;
public:
  ContextQueue(ceph::mutex& mut,
	       ceph::condition_variable& con)
    : mutex(mut), cond(con) {}

  void queue(std::list<Context *>& ls) {
    bool was_empty = false;
    {
      std::scoped_lock l(q_mutex);
      if (q.empty()) {
	q.swap(ls);
	was_empty = true;
      } else {
	q.insert(q.end(), ls.begin(), ls.end());
      }
      q_empty = q.empty();
    }

    if (was_empty) {
      std::scoped_lock l{mutex};
      cond.notify_all();
    }

    ls.clear();
  }

  void move_to(std::list<Context *>& ls) {
    ls.clear();
    std::scoped_lock l(q_mutex);
    if (!q.empty()) {
      q.swap(ls);
    }
    q_empty = true;
  }

  bool empty() {
    return q_empty;
  }
};

#endif
