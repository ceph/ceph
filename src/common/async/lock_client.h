// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <boost/asio/any_completion_handler.hpp>
#include <boost/asio/async_result.hpp>
#include <boost/system.hpp>
#include "common/ceph_time.h"

namespace ceph::async {

/// \brief Client interface for a specific timed exclusive lock.
/// \see with_lease
class LockClient {
 protected:
  using Signature = void(boost::system::error_code);
  using Handler = boost::asio::any_completion_handler<Signature>;

  virtual void acquire(ceph::timespan duration, Handler handler) = 0;
  virtual void renew(ceph::timespan duration, Handler handler) = 0;
  virtual void release(Handler handler) = 0;

 public:
  virtual ~LockClient() {}

  /// Acquire a timed lock for the given duration.
  template <boost::asio::completion_token_for<Signature> CompletionToken>
  auto async_acquire(ceph::timespan duration, CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, Signature>(
        [this, duration] (auto h) { acquire(duration, std::move(h)); }, token);
  }
  /// Renew an acquired lock for the given duration.
  template <boost::asio::completion_token_for<Signature> CompletionToken>
  auto async_renew(ceph::timespan duration, CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, Signature>(
        [this, duration] (auto h) { renew(duration, std::move(h)); }, token);
  }
  /// Release an acquired lock.
  template <boost::asio::completion_token_for<Signature> CompletionToken>
  auto async_release(CompletionToken&& token) {
    return boost::asio::async_initiate<CompletionToken, Signature>(
        [this] (auto h) { release(std::move(h)); }, token);
  }
};

} // namespace ceph::async
