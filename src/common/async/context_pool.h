// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat <contact@redhat.com>
 * Author: Adam C. Emerson <aemerson@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_COMMON_ASYNC_CONTEXT_POOL_H
#define CEPH_COMMON_ASYNC_CONTEXT_POOL_H

#include <concepts>
#include <cstdint>
#include <mutex>
#include <optional>
#include <thread>
#include <vector>

#include <boost/asio/io_context.hpp>
#include <boost/asio/executor_work_guard.hpp>

#include "common/ceph_mutex.h"
#include "common/Thread.h"

namespace ceph::async {
class io_context_pool {
  std::vector<std::thread> threadvec;
  std::vector<std::unique_ptr<boost::asio::io_context>> ioctxs;
  std::vector<std::optional<boost::asio::executor_work_guard<
			  boost::asio::io_context::executor_type>>> guards;
  std::atomic<uint32_t> next_ctx{0};
  ceph::mutex m = make_mutex("ceph::io_context_pool::m");

  void cleanup() noexcept {
    for (auto& g : guards) g = std::nullopt;
    for (auto& th : threadvec) {
      th.join();
    }
    threadvec.clear();
  }
public:
  io_context_pool() noexcept {}

  io_context_pool(std::int64_t threadcnt, std::int16_t ncontexts = 1) noexcept {
    start(threadcnt, ncontexts);
  }
  template<std::invocable<> Init>
  io_context_pool(std::int64_t threadcnt, Init&& init,
		  std::int16_t ncontexts = 1) noexcept {
    start(threadcnt, std::forward<Init>(init), ncontexts);
  }
  ~io_context_pool() {
    stop();
  }
  void start(std::int16_t threadcnt, std::int16_t ncontexts = 1) noexcept {
    auto l = std::scoped_lock(m);
    if (threadvec.empty()) {
      if (ncontexts <= 0) ncontexts = 1;
      if (ncontexts > threadcnt) ncontexts = threadcnt;
      ioctxs.resize(ncontexts);
      guards.resize(ncontexts);
      for (std::int16_t i = 0; i < ncontexts; ++i) {
	ioctxs[i] = std::make_unique<boost::asio::io_context>();
	guards[i].emplace(boost::asio::make_work_guard(*ioctxs[i]));
      }
      for (std::int16_t i = 0; i < threadcnt; ++i) {
	int ctx_ix = i % ncontexts;
	threadvec.emplace_back(make_named_thread("io_context_pool",
						 [this, ctx_ix] {
						   ioctxs[ctx_ix]->run();
						 }));
      }
    }
  }
  template<std::invocable<> Init>
  void start(std::int16_t threadcnt, Init&& init,
	     std::int16_t ncontexts = 1) noexcept {
    auto l = std::scoped_lock(m);
    if (threadvec.empty()) {
      if (ncontexts <= 0) ncontexts = 1;
      if (ncontexts > threadcnt) ncontexts = threadcnt;
      ioctxs.resize(ncontexts);
      guards.resize(ncontexts);
      for (std::int16_t i = 0; i < ncontexts; ++i) {
	ioctxs[i] = std::make_unique<boost::asio::io_context>();
	guards[i].emplace(boost::asio::make_work_guard(*ioctxs[i]));
      }
      for (std::int16_t i = 0; i < threadcnt; ++i) {
	int ctx_ix = i % ncontexts;
	threadvec.emplace_back(make_named_thread("io_context_pool",
						 [this, ctx_ix, init] {
						   init();
						   ioctxs[ctx_ix]->run();
						 }));
      }
    }
  }
  void finish() noexcept {
    auto l = std::scoped_lock(m);
    if (!threadvec.empty()) {
      cleanup();
    }
  }
  void stop() noexcept {
    auto l = std::scoped_lock(m);
    if (!threadvec.empty()) {
      for (auto& ctx : ioctxs) {
	ctx->stop();
      }
      cleanup();
    }
  }

  /* round-robin context selection for new connections */
  boost::asio::io_context& get_io_context() {
    return *ioctxs[next_ctx.fetch_add(1, std::memory_order_relaxed)
		   % ioctxs.size()];
  }

  /* primary context — for timers, admin, backward compat */
  boost::asio::io_context& get_primary_context() {
    return *ioctxs[0];
  }

  operator boost::asio::io_context&() {
    return get_primary_context();
  }
  using executor_type = boost::asio::io_context::executor_type;
  boost::asio::io_context::executor_type get_executor() {
    return get_primary_context().get_executor();
  }
};
}

#endif // CEPH_COMMON_ASYNC_CONTEXT_POOL_H
