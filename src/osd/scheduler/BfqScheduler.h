// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <deque>
#include <limits>
#include <list>
#include <map>
#include <optional>
#include <ostream>
#include <unordered_map>
#include <vector>

#include "common/ceph_context.h"
#include "common/ceph_time.h"
#include "common/config.h"
#include "osd/scheduler/OpScheduler.h"
#include "osd/scheduler/OpSchedulerItem.h"

class OSDMap;

namespace ceph::osd::scheduler {

/**
 * bfq_stream_t
 *
 * The leaf queues of the bfq hierarchy.  Client ops are subdivided by
 * the workload type of their pool (derived from the pool application
 * metadata); background ops by their scheduler class.
 */
enum class bfq_stream_t : uint8_t {
  client_block = 0,        ///< pools tagged with the rbd application
  client_object,           ///< rgw pools (traffic-class data or unmarked)
  client_object_meta,      ///< rgw pools marked traffic-class metadata
  client_file,             ///< cephfs data pools
  client_file_meta,        ///< cephfs metadata pools
  client_other,            ///< untagged or ambiguously tagged pools
  background_recovery,     ///< SchedulerClass::background_recovery
  background_best_effort,  ///< SchedulerClass::background_best_effort
  stream_count
};

enum class bfq_group_t : uint8_t {
  client = 0,
  background,
  group_count
};

inline constexpr size_t bfq_num_streams =
  static_cast<size_t>(bfq_stream_t::stream_count);
inline constexpr size_t bfq_num_groups =
  static_cast<size_t>(bfq_group_t::group_count);
inline constexpr size_t bfq_num_client_streams = 6;
inline constexpr size_t bfq_num_background_streams = 2;

constexpr bfq_group_t bfq_group_of(bfq_stream_t s) {
  return s < bfq_stream_t::background_recovery ?
    bfq_group_t::client : bfq_group_t::background;
}

/// index of a stream's entity within its group's service tree
constexpr unsigned bfq_index_in_group(bfq_stream_t s) {
  return bfq_group_of(s) == bfq_group_t::client ?
    static_cast<unsigned>(s) :
    static_cast<unsigned>(s) -
      static_cast<unsigned>(bfq_stream_t::background_recovery);
}

constexpr bfq_stream_t bfq_stream_at(unsigned group, unsigned index_in_group) {
  return static_cast<bfq_stream_t>(
    group == static_cast<unsigned>(bfq_group_t::client) ?
      index_in_group :
      static_cast<unsigned>(bfq_stream_t::background_recovery) +
        index_in_group);
}

std::string_view bfq_stream_name(bfq_stream_t s);
std::string_view bfq_group_name(bfq_group_t g);

namespace bfq_detail {

using vtime_t = double;

struct BfqEntity {
  uint32_t weight = 1;    ///< share while active, fixed at (re)activation
  vtime_t start = 0.0;    ///< S: virtual start of the current allotment
  vtime_t finish = 0.0;   ///< F while active; finish memory while idle
  uint64_t allotted = 0;  ///< service allotted at activation (scaled bytes)
  bool active = false;
};

/**
 * BfqServiceTree
 *
 * WF2Q+ scheduling over a small fixed set of entities addressed by
 * index.  The kernel maintains augmented rb-trees because a cgroup
 * hierarchy may hold thousands of entities; with at most a handful
 * per tree we scan linearly instead.
 *
 * Not thread safe: callers rely on the OSD shard lock.
 */
class BfqServiceTree {
public:
  explicit BfqServiceTree(unsigned num_entities) : entities(num_entities) {}

  /**
   * Make an idle entity backlogged.  S = max(V, previous F) -- the
   * finish memory prevents a queue from banking service while idle --
   * and F = S + allotted/weight.
   */
  void activate(unsigned idx, uint64_t allotted, uint32_t weight);

  /// account served (scaled) bytes to the tree's virtual time
  void charge(uint64_t served);

  /**
   * Close out an entity's allotment: back-shift F to the service it
   * actually received (so unused budget carries no penalty), then
   * either re-tag it for another allotment or remove it from the tree.
   */
  void expire(unsigned idx, uint64_t served, uint64_t next_allotted,
	      uint32_t next_weight, bool still_backlogged);

  /**
   * WF2Q+ selection: the eligible (S <= V) active entity with the
   * minimum F.  If active entities exist but none is eligible, V
   * jumps forward to the earliest pending S first.
   */
  std::optional<unsigned> select();

  bool has_active() const {
    return total_weight > 0;
  }
  const BfqEntity &entity(unsigned idx) const {
    return entities[idx];
  }
  vtime_t get_vtime() const {
    return vtime;
  }
  void dump(ceph::Formatter &f) const;

private:
  std::vector<BfqEntity> entities;
  vtime_t vtime = 0.0;
  uint64_t total_weight = 0;  ///< sum of active entity weights
};

} // namespace bfq_detail

/**
 * BfqScheduler
 *
 * OpScheduler implementation modeled on the Linux BFQ (Budget Fair
 * Queueing) I/O scheduler as exposed through the cgroups v2 io
 * controller.  Service (scaled bytes) is distributed by a two-level
 * B-WF2Q+ hierarchy of weighted groups:
 *
 *   root
 *   |-- client group                  (osd_bfq_client_group_weight)
 *   |   |-- block       (rbd pools)   (osd_bfq_client_block_weight)
 *   |   |-- object      (rgw data)    (osd_bfq_client_object_weight)
 *   |   |-- object_meta (rgw omap)    (osd_bfq_client_object_meta_weight)
 *   |   |-- file        (cephfs data) (osd_bfq_client_file_weight)
 *   |   |-- file_meta   (cephfs md)   (osd_bfq_client_file_meta_weight)
 *   |   `-- other                     (osd_bfq_client_other_weight)
 *   `-- background group              (osd_bfq_background_group_weight)
 *       |-- recovery                  (osd_bfq_background_recovery_weight)
 *       `-- best_effort               (osd_bfq_background_best_effort_weight)
 *
 * Unlike mclock, bfq is purely proportional-share: it needs no
 * estimate of the device's absolute capacity (IOPS or bandwidth).
 * Each backlogged leaf is granted a byte budget and served
 * exclusively until the budget is exhausted, its timeout elapses, or
 * it empties; virtual finish times computed over the assigned budget
 * are back-shifted to the consumed service on expiration, and the
 * next budget adapts to observed demand.
 *
 * Deliberate scope exclusions relative to the kernel: no weight
 * raising (low-latency heuristics), no device idling/anticipation.
 * Service is charged at dequeue (dispatch to a shard worker), not at
 * device completion, so fairness is over dispatched scaled bytes and
 * budget exclusivity is selection-exclusivity; this mirrors the
 * accounting model mclock already uses.
 *
 * Like mClockScheduler, items of SchedulerClass::immediate and items
 * at or above the priority cutoff bypass the fair hierarchy through a
 * strict high-priority queue, and enqueue_front lands requeued items
 * in that queue at priority 0.
 */
class BfqScheduler final : public OpScheduler, public md_config_obs_t {
public:
  BfqScheduler(CephContext *cct, int whoami, uint32_t num_shards,
	       int shard_id, bool is_rotational, unsigned cutoff_priority);
  ~BfqScheduler() final;

  void enqueue(OpSchedulerItem &&item) final;
  void enqueue_front(OpSchedulerItem &&item) final;

  bool empty() const final {
    return fair_queued == 0 && high_priority.empty();
  }

  // Never returns the future-time (double) alternative: if empty() is
  // false an item is always returned.
  WorkItem dequeue() final;

  void dump(ceph::Formatter &f) const final;

  void print(std::ostream &out) const final {
    out << get_op_queue_type_name(get_type())
	<< ", cutoff=" << cutoff_priority;
  }

  op_queue_type_t get_type() const final {
    return op_queue_type_t::BfqScheduler;
  }

  /// rebuild the pool -> client stream map from pool application tags
  void update_from_osdmap(const OSDMap &map) final;

  // md_config_obs_t
  std::vector<std::string> get_tracked_keys() const noexcept final;
  void handle_conf_change(const ConfigProxy &conf,
			  const std::set<std::string> &changed) final;

  // exposed for unit tests
  uint64_t calc_scaled_cost(int item_cost) const;
  bfq_stream_t classify(const OpSchedulerItem &item) const;
  void set_pool_streams(std::unordered_map<int64_t, bfq_stream_t> &&map);
  uint64_t get_max_budget() const {
    return max_budget;
  }

private:
  enum class expire_reason { emptied, exhausted, timed_out };

  static constexpr size_t idx(bfq_stream_t s) {
    return static_cast<size_t>(s);
  }
  static constexpr size_t idx(bfq_group_t g) {
    return static_cast<size_t>(g);
  }

  void refresh_config();
  void maybe_refresh_config() {
    if (config_dirty.exchange(false)) {
      refresh_config();
    }
  }
  void enqueue_high(unsigned prio, OpSchedulerItem &&item, bool front = false);
  void activate_stream(bfq_stream_t s);
  void begin_round(bfq_stream_t s);
  void end_round(expire_reason reason);
  OpSchedulerItem dequeue_fair();

  CephContext *cct;
  const unsigned cutoff_priority;
  const int shard_id;
  const bool is_rotational;

  // config cache; refreshed under the shard lock when config_dirty
  uint64_t max_budget = 0;
  uint64_t min_cost = 0;
  uint64_t cost_per_op = 0;
  uint64_t cost_per_io = 0;
  uint64_t min_budget = 0;
  std::chrono::milliseconds budget_timeout{125};
  std::array<uint32_t, bfq_num_streams> stream_weights;
  std::array<uint32_t, bfq_num_groups> group_weights;
  std::atomic<bool> config_dirty = true;

  struct Stream {
    std::deque<OpSchedulerItem> items;
    uint64_t next_budget = 0;       ///< adapted by budget feedback
    uint64_t served_round = 0;      ///< scaled bytes served this round
    int64_t budget_remaining = 0;   ///< may go negative for oversized items
  };
  std::array<Stream, bfq_num_streams> streams;
  std::array<uint64_t, bfq_num_groups> group_served = {0, 0};
  size_t fair_queued = 0;

  bfq_detail::BfqServiceTree root_tree;
  std::array<bfq_detail::BfqServiceTree, bfq_num_groups> group_trees;

  std::optional<bfq_stream_t> in_service;
  ceph::coarse_mono_clock::time_point round_start;

  /// pool id -> client stream, rebuilt on every OSDMap the shard consumes
  std::unordered_map<int64_t, bfq_stream_t> pool_streams;

  using priority_t = unsigned;
  using SubQueue = std::map<priority_t,
	std::list<OpSchedulerItem>,
	std::greater<priority_t>>;
  /**
   * high_priority
   *
   * Holds entries to be dequeued in strict order ahead of the fair
   * hierarchy.  Invariant: entries are never empty.
   */
  SubQueue high_priority;
  static constexpr priority_t immediate_class_priority =
    std::numeric_limits<priority_t>::max();
};

}
