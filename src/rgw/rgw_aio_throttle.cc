// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_aio_throttle.h"

namespace rgw {

bool Throttle::waiter_ready() const
{
  switch (waiter) {
  case Wait::Available: return is_available();
  case Wait::Completion: return has_completion();
  case Wait::Drained: return is_drained();
  default: return false;
  }
}

AioResultList BlockingAioThrottle::get(rgw_raw_obj obj,
                                       OpFunc&& f,
                                       uint64_t cost, uint64_t id)
{
  auto p = std::make_unique<Pending>();
  p->obj = std::move(obj);
  p->id = id;
  p->cost = cost;

  std::unique_lock lock{mutex};
  if (cost > window) {
    p->result = -EDEADLK; // would never succeed
    completed.push_back(*p);
  } else {
    // wait for the write size to become available
    pending_size += p->cost;
    if (!is_available()) {
      ceph_assert(waiter == Wait::None);
      waiter = Wait::Available;
      cond.wait(lock, [this] { return is_available(); });
      waiter = Wait::None;
    }

    // register the pending write and attach a completion
    p->parent = this;
    pending.push_back(*p);
    lock.unlock();
    std::move(f)(this, *static_cast<AioResult*>(p.get()));
    lock.lock();
  }
  // coverity[leaked_storage:SUPPRESS]
  p.release();
  return std::move(completed);
}

void BlockingAioThrottle::put(AioResult& r)
{
  auto& p = static_cast<Pending&>(r);
  std::scoped_lock lock{mutex};

  // move from pending to completed
  pending.erase(pending.iterator_to(p));
  completed.push_back(p);

  pending_size -= p.cost;

  if (waiter_ready()) {
    cond.notify_one();
  }
}

AioResultList BlockingAioThrottle::poll()
{
  std::unique_lock lock{mutex};
  return std::move(completed);
}

AioResultList BlockingAioThrottle::wait()
{
  std::unique_lock lock{mutex};
  if (completed.empty() && !pending.empty()) {
    ceph_assert(waiter == Wait::None);
    waiter = Wait::Completion;
    cond.wait(lock, [this] { return has_completion(); });
    waiter = Wait::None;
  }
  return std::move(completed);
}

AioResultList BlockingAioThrottle::drain()
{
  std::unique_lock lock{mutex};
  if (!pending.empty()) {
    ceph_assert(waiter == Wait::None);
    waiter = Wait::Drained;
    cond.wait(lock, [this] { return is_drained(); });
    waiter = Wait::None;
  }
  return std::move(completed);
}

template <typename CompletionToken>
auto YieldingAioThrottle::async_wait(CompletionToken&& token)
{
  using boost::asio::async_completion;
  using Signature = void(boost::system::error_code);
  async_completion<CompletionToken, Signature> init(token);
  completion = Completion::create(context.get_executor(),
                                  std::move(init.completion_handler));
  return init.result.get();
}

AioResultList YieldingAioThrottle::get(rgw_raw_obj obj,
                                       OpFunc&& f,
                                       uint64_t cost, uint64_t id)
{
  auto p = std::make_unique<Pending>();
  p->obj = std::move(obj);
  p->id = id;
  p->cost = cost;

  if (cost > window) {
    p->result = -EDEADLK; // would never succeed
    completed.push_back(*p);
  } else {
    // wait for the write size to become available
    pending_size += p->cost;
    if (!is_available()) {
      ceph_assert(waiter == Wait::None);
      ceph_assert(!completion);

      boost::system::error_code ec;
      waiter = Wait::Available;
      async_wait(yield[ec]);
    }

    // register the pending write and initiate the operation
    pending.push_back(*p);
    std::move(f)(this, *static_cast<AioResult*>(p.get()));
  }
  // coverity[leaked_storage:SUPPRESS]
  p.release();
  return std::move(completed);
}

void YieldingAioThrottle::put(AioResult& r)
{
  auto& p = static_cast<Pending&>(r);

  // move from pending to completed
  pending.erase(pending.iterator_to(p));
  completed.push_back(p);

  pending_size -= p.cost;

  if (waiter_ready()) {
    ceph_assert(completion);
    ceph::async::post(std::move(completion), boost::system::error_code{});
    waiter = Wait::None;
  }
}

AioResultList YieldingAioThrottle::poll()
{
  return std::move(completed);
}

AioResultList YieldingAioThrottle::wait()
{
  if (!has_completion() && !pending.empty()) {
    ceph_assert(waiter == Wait::None);
    ceph_assert(!completion);

    boost::system::error_code ec;
    waiter = Wait::Completion;
    async_wait(yield[ec]);
  }
  return std::move(completed);
}

AioResultList YieldingAioThrottle::drain()
{
  if (!is_drained()) {
    ceph_assert(waiter == Wait::None);
    ceph_assert(!completion);

    boost::system::error_code ec;
    waiter = Wait::Drained;
    async_wait(yield[ec]);
  }
  return std::move(completed);
}
} // namespace rgw
