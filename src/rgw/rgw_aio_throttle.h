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

#pragma once

#include <memory>
#include "common/ceph_mutex.h"
#include "common/async/completion.h"
#include "common/async/yield_context.h"
#include "rgw_aio.h"

namespace rgw {

class Throttle {
 protected:
  const uint64_t window;
  uint64_t pending_size = 0;

  AioResultList pending;
  AioResultList completed;

  bool is_available() const { return pending_size <= window; }
  bool has_completion() const { return !completed.empty(); }
  bool is_drained() const { return pending.empty(); }

  enum class Wait { None, Available, Completion, Drained };
  Wait waiter = Wait::None;

  bool waiter_ready() const;

 public:
  Throttle(uint64_t window) : window(window) {}

  virtual ~Throttle() {
    // must drain before destructing
    ceph_assert(pending.empty());
    ceph_assert(completed.empty());
  }
};

// a throttle for aio operations. all public functions must be called from
// the same thread
class BlockingAioThrottle final : public Aio, private Throttle {
  ceph::mutex mutex = ceph::make_mutex("AioThrottle");
  ceph::condition_variable cond;

  struct Pending : AioResultEntry {
    BlockingAioThrottle *parent = nullptr;
    uint64_t cost = 0;
  };
 public:
  BlockingAioThrottle(uint64_t window) : Throttle(window) {}

  virtual ~BlockingAioThrottle() override {};

  AioResultList get(rgw_raw_obj obj, OpFunc&& f,
                    uint64_t cost, uint64_t id) override final;

  void put(AioResult& r) override final;

  AioResultList poll() override final;

  AioResultList wait() override final;

  AioResultList drain() override final;
};

// a throttle that yields the coroutine instead of blocking. all public
// functions must be called within the coroutine strand
class YieldingAioThrottle final : public Aio, private Throttle {
  boost::asio::io_context& context;
  spawn::yield_context yield;
  struct Handler;

  // completion callback associated with the waiter
  using Completion = ceph::async::Completion<void(boost::system::error_code)>;
  std::unique_ptr<Completion> completion;

  template <typename CompletionToken>
  auto async_wait(CompletionToken&& token);

  struct Pending : AioResultEntry { uint64_t cost = 0; };

 public:
  YieldingAioThrottle(uint64_t window, boost::asio::io_context& context,
                      spawn::yield_context yield)
    : Throttle(window), context(context), yield(yield)
  {}

  virtual ~YieldingAioThrottle() override {};

  AioResultList get(rgw_raw_obj obj, OpFunc&& f,
                    uint64_t cost, uint64_t id) override final;

  void put(AioResult& r) override final;

  AioResultList poll() override final;

  AioResultList wait() override final;

  AioResultList drain() override final;
};

// return a smart pointer to Aio
inline auto make_throttle(uint64_t window_size, optional_yield y)
{
  std::unique_ptr<Aio> aio;
  if (y) {
    aio = std::make_unique<YieldingAioThrottle>(window_size,
                                                y.get_io_context(),
                                                y.get_yield_context());
  } else {
    aio = std::make_unique<BlockingAioThrottle>(window_size);
  }
  return aio;
}

} // namespace rgw
