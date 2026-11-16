// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright contributors to the Ceph project
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>

#include "common/async/yield_context.h"

namespace rgw { namespace sal { namespace nsfs {

/* The intent queue of `automatic` adoption.
 *
 * An intent is one bucket's whole adoption:  its record imported, its
 * markers on the directory, its tree converted.  A request on a bucket
 * not yet adopted promotes the bucket's intent and waits for it up to a
 * quantum;  startup records an intent for every bucket, behind the
 * promoted ones.  Intents for one bucket coalesce.
 *
 * Workers are coroutines on the gateway's io_context.  Each hands its
 * bucket's adoption to a thread of its own and yields until it returns,
 * so a worker has one hop outstanding and the threads number at most
 * the workers, and only while there is adopting to do.
 */
class AdoptionQueue {
public:
  /* Adopt `bucket`.  Runs on a thread of its own, never an executor
   * thread.  `stopping` is set at shutdown;  a walk that sees it stops
   * where it is, and its marker stands for a later run. */
  using adopt_fn = std::function<int(const std::string& bucket,
                                     const std::atomic<bool>& stopping)>;
  /* Run on a thread of its own when the queue empties after work. */
  using drained_fn = std::function<void()>;

  AdoptionQueue(boost::asio::io_context& ioc, size_t workers,
                adopt_fn adopt, drained_fn drained);
  ~AdoptionQueue();

  AdoptionQueue(const AdoptionQueue&) = delete;
  AdoptionQueue& operator=(const AdoptionQueue&) = delete;

  void start();
  /* Stops the workers without waiting for them:  each leaves at its
   * next turn.  From any thread, the drain's included. */
  void stop();
  /* Stops the workers and waits for them.  Not from an executor thread:
   * it waits for hops that complete through the executor. */
  void shutdown();

  /* Record a backfill intent. */
  void enqueue(const std::string& bucket);

  /* Record a promoted intent and, given a coroutine, wait for it up to
   * `quantum`.  Returns 0 when the bucket's adoption finished in that
   * time, its error when it failed, and -ETIMEDOUT when it has not
   * finished -- at once with null_yield or a zero quantum.  A bucket
   * whose adoption failed earlier returns that error at once and is not
   * tried again:  a failure stays until an operator acts. */
  int promote(const std::string& bucket, std::chrono::milliseconds quantum,
              optional_yield y);

  /* Run `drained` once nothing is queued or running, though nothing
   * was adopted:  an abandonment can complete adoption by itself. */
  void recheck();

  struct Stats {
    uint64_t queued{0};
    uint64_t running{0};
    uint64_t adopted{0};
    uint64_t failed{0};
    uint64_t promotions{0};
    uint64_t waits_timed_out{0};
    /* workers not yet exited */
    uint64_t workers{0};
  };
  Stats stats() const;

private:
  struct Intent {
    std::string bucket;
    bool promoted{false};
    bool taken{false};
    bool done{false};
    int result{0};
    std::vector<std::shared_ptr<boost::asio::steady_timer>> waiters;
  };

  boost::asio::io_context& ioc;
  const size_t nworkers;
  adopt_fn adopt;
  drained_fn drained;

  mutable std::mutex mtx;
  std::condition_variable exited;
  /* queued or running, by bucket */
  std::map<std::string, std::shared_ptr<Intent>> intents;
  /* each failed bucket's error, which neither a request nor the
   * backfill retries */
  std::map<std::string, int> failures;
  std::deque<std::shared_ptr<Intent>> promoted_q;
  std::deque<std::shared_ptr<Intent>> backfill_q;
  /* idle workers, each suspended on one of these */
  std::vector<std::shared_ptr<boost::asio::steady_timer>> parked;
  size_t live{0};
  size_t running{0};
  /* a drain is due:  an intent finished since the last one, or
   * recheck() asked */
  bool worked{false};
  bool started{false};
  std::atomic<bool> stopping{false};
  uint64_t adopted{0};
  uint64_t failed{0};
  uint64_t promotions{0};
  uint64_t waits_timed_out{0};

  std::shared_ptr<Intent> record(const std::string& bucket, bool& created);
  std::shared_ptr<Intent> next();
  void wake_one();
  void worker(boost::asio::yield_context yield);
  int hop(boost::asio::yield_context yield, std::function<int()> fn);
  void finish(const std::shared_ptr<Intent>& in, int result);
};

} } } // namespace rgw::sal::nsfs
