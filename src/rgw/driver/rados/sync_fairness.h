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

#include <memory>

namespace rgw::sal { class RadosStore; }
struct rgw_raw_obj;
class RGWCoroutine;

/// watch/notify protocol to coordinate the sharing of sync locks
///
/// each gateway generates a set of random bids, and broadcasts them regularly
/// to other active gateways. in response, the peer gateways send their own set
/// of bids
///
/// sync will only lock and process log shards where it holds the highest bid
namespace rgw::sync_fairness {

class BidManager {
 public:
  virtual ~BidManager() {}

  /// establish a watch, creating the control object if necessary
  virtual int start() = 0;

  /// returns true if we're the highest bidder on the given shard index
  virtual bool is_highest_bidder(std::size_t index) = 0;

  /// return a coroutine that broadcasts our current bids and records the
  /// bids from other peers that respond
  virtual RGWCoroutine* notify_cr() = 0;
};

// rados BidManager factory
auto create_rados_bid_manager(sal::RadosStore* store,
                              const rgw_raw_obj& watch_obj,
                              std::size_t num_shards)
  -> std::unique_ptr<BidManager>;

} // namespace rgw::sync_fairness
