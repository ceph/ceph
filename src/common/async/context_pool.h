// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
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
#include <cstddef>
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
  boost::asio::io_context ioctx;
  std::optional<boost::asio::executor_work_guard<
		  boost::asio::io_context::executor_type>> guard;
  ceph::mutex m = make_mutex("ceph::io_context_pool::m");

  void cleanup() noexcept {
    guard = std::nullopt;
    for (auto& th : threadvec) {
      th.join();
    }
    threadvec.clear();
  }
public:
  io_context_pool() noexcept {}

  io_context_pool(std::int64_t threadcnt) noexcept {
    start(threadcnt);
  }
  template<std::invocable<> Init>
  io_context_pool(std::int64_t threadcnt, Init&& init) noexcept {
    start(threadcnt, std::move(init));
  }
  ~io_context_pool() {
    stop();
  }
  void start(std::int16_t threadcnt) noexcept {
    auto l = std::scoped_lock(m);
    if (threadvec.empty()) {
      guard.emplace(boost::asio::make_work_guard(ioctx));
      ioctx.restart();
      for (std::int16_t i = 0; i < threadcnt; ++i) {
	threadvec.emplace_back(make_named_thread("io_context_pool",
						 [this] {
						   ioctx.run();
						 }));
      }
    }
  }
  template<std::invocable<> Init>
  void start(std::int16_t threadcnt, Init&& init) noexcept {
    auto l = std::scoped_lock(m);
    if (threadvec.empty()) {
      guard.emplace(boost::asio::make_work_guard(ioctx));
      ioctx.restart();
      for (std::int16_t i = 0; i < threadcnt; ++i) {
	threadvec.emplace_back(make_named_thread("io_context_pool",
						 [this, init=std::move(init)] {
						   std::move(init)();
						   ioctx.run();
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
      ioctx.stop();
      cleanup();
    }
  }

  boost::asio::io_context& get_io_context() {
    return ioctx;
  }
  operator boost::asio::io_context&() {
    return ioctx;
  }
  boost::asio::io_context::executor_type get_executor() {
    return ioctx.get_executor();
  }
};
}

#endif // CEPH_COMMON_ASYNC_CONTEXT_POOL_H
