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

#include "adoption_queue.h"

#include <algorithm>
#include <thread>

#include <boost/asio/post.hpp>
#include <boost/asio/spawn.hpp>
#include <boost/asio/strand.hpp>
#include <boost/context/protected_fixedsize_stack.hpp>

#include "common/Thread.h"
#include "common/async/yield_waiter.h"

namespace rgw { namespace sal { namespace nsfs {

namespace {

/* A timer is cancelled on its own executor.  Every timer here was made
 * on a strand -- a worker's, or a request's connection -- so the cancel
 * cannot run before the wait it ends has begun:  the coroutine that
 * registered the timer holds the strand until it suspends in that wait. */
void cancel_on(std::shared_ptr<boost::asio::steady_timer> t)
{
  boost::asio::post(t->get_executor(), [t] { t->cancel(); });
}

} // anonymous namespace

AdoptionQueue::AdoptionQueue(boost::asio::io_context& ioc, size_t workers,
                             adopt_fn adopt, drained_fn drained)
  : ioc(ioc), nworkers(std::max<size_t>(workers, 1)),
    adopt(std::move(adopt)), drained(std::move(drained))
{ }

AdoptionQueue::~AdoptionQueue()
{
  shutdown();
}

void AdoptionQueue::start()
{
  {
    std::lock_guard l{mtx};
    if (started || stopping) {
      return;
    }
    started = true;
    live = nworkers;
  }
  for (size_t i = 0; i < nworkers; ++i) {
    boost::asio::spawn(
      boost::asio::make_strand(ioc), std::allocator_arg,
      boost::context::protected_fixedsize_stack{128*1024},
      [this] (boost::asio::yield_context yield) { worker(yield); },
      [] (std::exception_ptr eptr) {
        if (eptr) {
          std::rethrow_exception(eptr);
        }
      });
  }
}

void AdoptionQueue::stop()
{
  std::vector<std::shared_ptr<boost::asio::steady_timer>> timers;
  {
    std::lock_guard l{mtx};
    stopping = true;
    timers.swap(parked);
    for (auto& [_, in] : intents) {
      timers.insert(timers.end(), in->waiters.begin(), in->waiters.end());
      in->waiters.clear();
    }
  }
  for (auto& t : timers) {
    cancel_on(t);
  }
}

void AdoptionQueue::shutdown()
{
  stop();
  std::unique_lock l{mtx};
  exited.wait(l, [this] { return live == 0; });
}

std::shared_ptr<AdoptionQueue::Intent>
AdoptionQueue::record(const std::string& bucket, bool& created)
{
  auto [it, inserted] = intents.try_emplace(bucket);
  if (inserted) {
    it->second = std::make_shared<Intent>();
    it->second->bucket = bucket;
  }
  created = inserted;
  return it->second;
}

void AdoptionQueue::enqueue(const std::string& bucket)
{
  std::lock_guard l{mtx};
  if (stopping || failures.contains(bucket)) {
    return;
  }
  bool created;
  auto in = record(bucket, created);
  if (created) {
    backfill_q.push_back(in);
    wake_one();
  }
}

int AdoptionQueue::promote(const std::string& bucket,
                           std::chrono::milliseconds quantum,
                           optional_yield y)
{
  std::shared_ptr<Intent> in;
  std::shared_ptr<boost::asio::steady_timer> t;
  {
    std::lock_guard l{mtx};
    if (stopping) {
      return -ESHUTDOWN;
    }
    if (auto f = failures.find(bucket); f != failures.end()) {
      return f->second;
    }
    bool created;
    in = record(bucket, created);
    if (!in->promoted) {
      in->promoted = true;
      ++promotions;
      /* one still in the backfill queue is skipped there once taken */
      if (!in->taken) {
        promoted_q.push_back(in);
        wake_one();
      }
    }
    if (!y || (quantum.count() <= 0)) {
      return -ETIMEDOUT;
    }
    t = std::make_shared<boost::asio::steady_timer>(
      y.get_yield_context().get_executor(), quantum);
    in->waiters.push_back(t);
  }

  boost::system::error_code ec;
  t->async_wait(y.get_yield_context()[ec]);

  std::lock_guard l{mtx};
  std::erase(in->waiters, t);
  if (in->done) {
    return in->result;
  }
  ++waits_timed_out;
  return -ETIMEDOUT;
}

void AdoptionQueue::recheck()
{
  std::lock_guard l{mtx};
  if (stopping) {
    return;
  }
  worked = true;
  wake_one();
}

std::shared_ptr<AdoptionQueue::Intent> AdoptionQueue::next()
{
  for (auto* q : { &promoted_q, &backfill_q }) {
    while (!q->empty()) {
      auto in = std::move(q->front());
      q->pop_front();
      if (!in->taken) {
        return in;
      }
    }
  }
  return nullptr;
}

void AdoptionQueue::wake_one()
{
  if (!parked.empty()) {
    cancel_on(std::move(parked.back()));
    parked.pop_back();
  }
}

int AdoptionQueue::hop(boost::asio::yield_context yield,
                       std::function<int()> fn)
{
  struct Hop {
    std::mutex m;
    bool done{false};
    int result{0};
    ceph::async::yield_waiter<int> waiter;
  };
  auto h = std::make_shared<Hop>();
  /* Complete under the lock and touch nothing after it:  complete()
   * posts the handler, which takes the lock again before the coroutine
   * resumes, so it cannot see `done` and leave before this releases. */
  std::thread t = make_named_thread("nsfs_adopt", [h, fn = std::move(fn)] {
    const int r = fn();
    std::lock_guard l{h->m};
    h->result = r;
    h->done = true;
    if (h->waiter) {
      h->waiter.complete(boost::system::error_code{}, r);
    }
  });
  int r;
  {
    std::unique_lock l{h->m};
    r = h->done ? h->result : h->waiter.async_wait(l, yield);
  }
  t.join();
  return r;
}

void AdoptionQueue::finish(const std::shared_ptr<Intent>& in, int result)
{
  std::vector<std::shared_ptr<boost::asio::steady_timer>> timers;
  {
    std::lock_guard l{mtx};
    in->done = true;
    in->result = result;
    --running;
    worked = true;
    (result == 0) ? ++adopted : ++failed;
    /* a walk stopped by shutdown has not failed;  its marker stands for
     * the next start */
    if ((result < 0) && (result != -ECANCELED)) {
      failures[in->bucket] = result;
    }
    if (auto it = intents.find(in->bucket);
        (it != intents.end()) && (it->second == in)) {
      intents.erase(it);
    }
    timers.swap(in->waiters);
  }
  for (auto& t : timers) {
    cancel_on(t);
  }
}

void AdoptionQueue::worker(boost::asio::yield_context yield)
{
  for (;;) {
    std::shared_ptr<Intent> in;
    std::shared_ptr<boost::asio::steady_timer> park;
    bool drain = false;
    {
      std::lock_guard l{mtx};
      if (stopping) {
        break;
      }
      in = next();
      if (in) {
        in->taken = true;
        ++running;
      } else if (worked && (running == 0)) {
        worked = false;
        drain = true;
      } else {
        park = std::make_shared<boost::asio::steady_timer>(
          yield.get_executor(), boost::asio::steady_timer::time_point::max());
        parked.push_back(park);
      }
    }
    if (park) {
      boost::system::error_code ec;
      park->async_wait(yield[ec]);
      continue;
    }
    if (drain) {
      hop(yield, [this] { drained(); return 0; });
      continue;
    }
    finish(in, hop(yield, [this, in] { return adopt(in->bucket, stopping); }));
  }

  {
    std::lock_guard l{mtx};
    --live;
  }
  exited.notify_all();
}

AdoptionQueue::Stats AdoptionQueue::stats() const
{
  std::lock_guard l{mtx};
  Stats s;
  s.queued = intents.size() - running;
  s.running = running;
  s.adopted = adopted;
  s.failed = failed;
  s.promotions = promotions;
  s.waits_timed_out = waits_timed_out;
  s.workers = live;
  return s;
}

} } } // namespace rgw::sal::nsfs
