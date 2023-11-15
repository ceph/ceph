// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#pragma once

#include <coroutine>
#include <memory>

#include <boost/asio/awaitable.hpp>
#include <boost/asio/any_io_executor.hpp>

#include "rgw_obj_types.h"
#include "rgw_sal_rados.h"

/// watch/notify protocol to coordinate the sharing of sync locks
///
/// each gateway generates a set of random bids, and broadcasts them regularly
/// to other active gateways. in response, the peer gateways send their own set
/// of bids
///
/// sync will only lock and process log shards where it holds the highest bid
namespace rgw::sync_fairness {
namespace asio = boost::asio;

class BidManagerAsio {
 public:
  virtual ~BidManagerAsio() = default;

  /// Returns true if we're the highest bidder on the given shard index
  virtual asio::awaitable<bool> is_highest_bidder(std::size_t index) = 0;

  /// Broadcast our current bids and record the bids from other peers
  /// that respond
  virtual asio::awaitable<void> notify() = 0;

  /// Shut down the bid manager
  virtual asio::awaitable<void> stop() = 0;
};

// NeoRADOS BidManager factory
asio::awaitable<std::unique_ptr<BidManagerAsio>> create_neorados_bid_manager(
  const DoutPrefixProvider* dpp, sal::RadosStore* store,
  asio::any_io_executor executor, std::string_view obj, const rgw_pool& pool,
  std::size_t num_shards);
} // namespace rgw::sync_fairness
