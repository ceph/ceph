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

#include <algorithm>

#include "osd/scheduler/BfqScheduler.h"

#include "common/debug.h"
#include "osd/OSDMap.h"

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "BfqScheduler(" << shard_id << "): "

namespace ceph::osd::scheduler {

std::string_view bfq_stream_name(bfq_stream_t s)
{
  switch (s) {
  case bfq_stream_t::client_block:
    return "client_block";
  case bfq_stream_t::client_object:
    return "client_object";
  case bfq_stream_t::client_object_meta:
    return "client_object_meta";
  case bfq_stream_t::client_file:
    return "client_file";
  case bfq_stream_t::client_file_meta:
    return "client_file_meta";
  case bfq_stream_t::client_other:
    return "client_other";
  case bfq_stream_t::background_recovery:
    return "background_recovery";
  case bfq_stream_t::background_best_effort:
    return "background_best_effort";
  default:
    return "unknown";
  }
}

std::string_view bfq_group_name(bfq_group_t g)
{
  switch (g) {
  case bfq_group_t::client:
    return "client";
  case bfq_group_t::background:
    return "background";
  default:
    return "unknown";
  }
}

namespace bfq_detail {

void BfqServiceTree::activate(unsigned idx, uint64_t allotted,
			      uint32_t weight)
{
  BfqEntity &e = entities[idx];
  ceph_assert(!e.active);
  ceph_assert(weight > 0);
  e.weight = weight;
  e.start = std::max(vtime, e.finish);
  e.finish = e.start + static_cast<vtime_t>(allotted) / e.weight;
  e.allotted = allotted;
  e.active = true;
  total_weight += e.weight;
}

void BfqServiceTree::charge(uint64_t served)
{
  if (total_weight > 0) {
    vtime += static_cast<vtime_t>(served) / total_weight;
  }
}

void BfqServiceTree::expire(unsigned idx, uint64_t served,
			    uint64_t next_allotted, uint32_t next_weight,
			    bool still_backlogged)
{
  BfqEntity &e = entities[idx];
  ceph_assert(e.active);
  ceph_assert(next_weight > 0);
  // back-shift: charge the entity for the service it actually
  // received, not the estimate it was tagged with at activation
  e.finish = e.start + static_cast<vtime_t>(served) / e.weight;
  if (still_backlogged) {
    e.start = e.finish;
    total_weight -= e.weight;
    e.weight = next_weight;
    total_weight += e.weight;
    e.finish = e.start + static_cast<vtime_t>(next_allotted) / e.weight;
    e.allotted = next_allotted;
  } else {
    e.active = false;
    total_weight -= e.weight;
    if (total_weight == 0) {
      // fully idle: the ideal fluid server has no backlog, so relative
      // finish memories no longer matter; renormalize to keep the
      // virtual time magnitude (and double precision) bounded
      vtime = 0.0;
      for (auto &ent : entities) {
	ent.start = 0.0;
	ent.finish = 0.0;
      }
    }
  }
}

std::optional<unsigned> BfqServiceTree::select()
{
  int best = -1;
  bool any_active = false;
  vtime_t min_start = 0.0;
  for (unsigned i = 0; i < entities.size(); ++i) {
    const BfqEntity &e = entities[i];
    if (!e.active) {
      continue;
    }
    if (!any_active || e.start < min_start) {
      min_start = e.start;
      any_active = true;
    }
    if (e.start <= vtime &&
	(best < 0 || e.finish < entities[best].finish)) {
      best = static_cast<int>(i);
    }
  }
  if (best < 0 && any_active) {
    // active entities exist but none is eligible: the ideal server
    // idled while V lagged; jump V forward to the earliest pending
    // start (WF2Q+ virtual time rule) and pick again
    vtime = std::max(vtime, min_start);
    for (unsigned i = 0; i < entities.size(); ++i) {
      const BfqEntity &e = entities[i];
      if (e.active && e.start <= vtime &&
	  (best < 0 || e.finish < entities[best].finish)) {
	best = static_cast<int>(i);
      }
    }
  }
  if (best < 0) {
    return std::nullopt;
  }
  return static_cast<unsigned>(best);
}

void BfqServiceTree::dump(ceph::Formatter &f) const
{
  f.dump_float("vtime", vtime);
  f.dump_unsigned("total_active_weight", total_weight);
  f.open_array_section("entities");
  for (const auto &e : entities) {
    f.open_object_section("entity");
    f.dump_bool("active", e.active);
    f.dump_unsigned("weight", e.weight);
    f.dump_float("vstart", e.start);
    f.dump_float("vfinish", e.finish);
    f.dump_unsigned("allotted", e.allotted);
    f.close_section();
  }
  f.close_section();
}

} // namespace bfq_detail

BfqScheduler::BfqScheduler(CephContext *cct, int whoami, uint32_t num_shards,
			   int shard_id, bool is_rotational,
			   unsigned cutoff_priority)
  : cct(cct),
    cutoff_priority(cutoff_priority),
    shard_id(shard_id),
    is_rotational(is_rotational),
    root_tree(bfq_num_groups),
    group_trees{bfq_detail::BfqServiceTree(bfq_num_client_streams),
		bfq_detail::BfqServiceTree(bfq_num_background_streams)}
{
  ceph_assert(num_shards > 0);
  refresh_config();
  for (auto &s : streams) {
    s.next_budget = std::max(min_budget, max_budget / 2);
  }
  cct->_conf.add_observer(this);
}

BfqScheduler::~BfqScheduler()
{
  cct->_conf.remove_observer(this);
}

void BfqScheduler::refresh_config()
{
  const auto &conf = cct->_conf;
  max_budget = conf.get_val<Option::size_t>("osd_bfq_max_budget");
  if (max_budget == 0) {
    // auto: rotation between backlogged streams stalls the others for
    // up to ~max_budget / device_bandwidth, so flash gets a smaller
    // default budget than a rotational device of far lower bandwidth
    max_budget = is_rotational ? 8 * 1024 * 1024 : 1024 * 1024;
  }
  min_cost = conf.get_val<Option::size_t>("osd_bfq_min_cost");
  cost_per_op = conf.get_val<Option::size_t>("osd_bfq_cost_per_op");
  cost_per_io = conf.get_val<Option::size_t>("osd_bfq_cost_per_io");
  // floor budgets so a round always covers at least one scaled item
  // and rounds cannot degenerate into per-item switching
  const uint64_t fixed_cost = cost_per_op + cost_per_io;
  min_budget = std::max<uint64_t>(
    {1, fixed_cost > 0 ? fixed_cost : min_cost, max_budget / 32});
  max_budget = std::max(max_budget, min_budget);
  budget_timeout =
    conf.get_val<std::chrono::milliseconds>("osd_bfq_budget_timeout");

  group_weights[idx(bfq_group_t::client)] =
    conf.get_val<uint64_t>("osd_bfq_client_group_weight");
  group_weights[idx(bfq_group_t::background)] =
    conf.get_val<uint64_t>("osd_bfq_background_group_weight");
  stream_weights[idx(bfq_stream_t::client_block)] =
    conf.get_val<uint64_t>("osd_bfq_client_block_weight");
  stream_weights[idx(bfq_stream_t::client_object)] =
    conf.get_val<uint64_t>("osd_bfq_client_object_weight");
  stream_weights[idx(bfq_stream_t::client_object_meta)] =
    conf.get_val<uint64_t>("osd_bfq_client_object_meta_weight");
  stream_weights[idx(bfq_stream_t::client_file)] =
    conf.get_val<uint64_t>("osd_bfq_client_file_weight");
  stream_weights[idx(bfq_stream_t::client_file_meta)] =
    conf.get_val<uint64_t>("osd_bfq_client_file_meta_weight");
  stream_weights[idx(bfq_stream_t::client_other)] =
    conf.get_val<uint64_t>("osd_bfq_client_other_weight");
  stream_weights[idx(bfq_stream_t::background_recovery)] =
    conf.get_val<uint64_t>("osd_bfq_background_recovery_weight");
  stream_weights[idx(bfq_stream_t::background_best_effort)] =
    conf.get_val<uint64_t>("osd_bfq_background_best_effort_weight");

  dout(10) << __func__ << " max_budget " << max_budget
	   << " min_budget " << min_budget
	   << " min_cost " << min_cost
	   << " cost_per_op " << cost_per_op
	   << " cost_per_io " << cost_per_io
	   << " budget_timeout " << budget_timeout.count() << "ms"
	   << dendl;
}

std::vector<std::string> BfqScheduler::get_tracked_keys() const noexcept
{
  using namespace std::literals;
  return {
    "osd_bfq_client_group_weight"s,
    "osd_bfq_background_group_weight"s,
    "osd_bfq_client_block_weight"s,
    "osd_bfq_client_object_weight"s,
    "osd_bfq_client_object_meta_weight"s,
    "osd_bfq_client_file_weight"s,
    "osd_bfq_client_file_meta_weight"s,
    "osd_bfq_client_other_weight"s,
    "osd_bfq_background_recovery_weight"s,
    "osd_bfq_background_best_effort_weight"s,
    "osd_bfq_max_budget"s,
    "osd_bfq_min_cost"s,
    "osd_bfq_cost_per_op"s,
    "osd_bfq_cost_per_io"s,
    "osd_bfq_budget_timeout"s
  };
}

void BfqScheduler::handle_conf_change(const ConfigProxy &conf,
				      const std::set<std::string> &changed)
{
  // runs on the config thread; fold the change in under the shard
  // lock on the next enqueue/dequeue.  Weights of currently active
  // entities take effect at their next (re)activation.
  config_dirty = true;
}

uint64_t BfqScheduler::calc_scaled_cost(int item_cost) const
{
  const uint64_t bytes = static_cast<uint64_t>(std::max(1, item_cost));
  const uint64_t fixed_cost = cost_per_op + cost_per_io;
  if (fixed_cost > 0) {
    // additive model: fixed OSD pipeline overhead (cost_per_op) plus
    // seek-time equivalent (cost_per_io) plus transfer bytes.  When
    // either constant is configured it replaces the min_cost floor.
    return fixed_cost + bytes;
  }
  // a pure config floor covering per-IO overhead; unlike mclock none
  // of these values is derived from a device capacity estimate
  return std::max(bytes, min_cost);
}

bfq_stream_t BfqScheduler::classify(const OpSchedulerItem &item) const
{
  switch (item.get_scheduler_class()) {
  case SchedulerClass::background_recovery:
    return bfq_stream_t::background_recovery;
  case SchedulerClass::background_best_effort:
    return bfq_stream_t::background_best_effort;
  default:
    // immediate never reaches the fair hierarchy (handled in enqueue)
    ceph_assert(item.get_scheduler_class() == SchedulerClass::client);
    [[fallthrough]];
  case SchedulerClass::client:
    if (auto p = pool_streams.find(item.get_ordering_token().pool());
	p != pool_streams.end()) {
      return p->second;
    }
    return bfq_stream_t::client_other;
  }
}

void BfqScheduler::update_from_osdmap(const OSDMap &map)
{
  std::unordered_map<int64_t, bfq_stream_t> next;
  for (const auto &[id, pool] : map.get_pools()) {
    std::optional<bfq_stream_t> stream;
    const std::map<std::string, std::string> *app_md_p = nullptr;
    bool ambiguous = false;
    for (const auto &[app, app_md] : pool.application_metadata) {
      std::optional<bfq_stream_t> tagged;
      if (app == pg_pool_t::APPLICATION_NAME_RBD) {
	tagged = bfq_stream_t::client_block;
      } else if (app == pg_pool_t::APPLICATION_NAME_RGW) {
	tagged = bfq_stream_t::client_object;
      } else if (app == pg_pool_t::APPLICATION_NAME_CEPHFS) {
	tagged = bfq_stream_t::client_file;
      }
      if (tagged) {
	if (stream && *stream != *tagged) {
	  ambiguous = true;
	}
	stream = tagged;
	app_md_p = &app_md;
      }
    }
    if (stream && !ambiguous) {
      // refine rgw and cephfs pools into data vs metadata streams from
      // EXPLICIT application metadata only: a "traffic-class" key under
      // the application tag ("metadata" selects the metadata stream,
      // anything else the data stream), falling back for cephfs to the
      // "metadata" key the mon already stamps at fs new /
      // add_data_pool time (pre-created pools included).  Autoscaler
      // hints (the bulk flag, pg_autoscale_bias) are deliberately not
      // consulted: they are absent on pools created before the
      // application starts and inert or rejected under ratio-driven
      // autoscaling, and pool sizing hints are not QoS policy.
      const bool meta_class = [&] {
	if (auto tc = app_md_p->find("traffic-class");
	    tc != app_md_p->end()) {
	  return tc->second == "metadata";
	}
	return *stream == bfq_stream_t::client_file &&
	  app_md_p->count("metadata") > 0;
      }();
      if (meta_class && *stream == bfq_stream_t::client_object) {
	stream = bfq_stream_t::client_object_meta;
      } else if (meta_class && *stream == bfq_stream_t::client_file) {
	stream = bfq_stream_t::client_file_meta;
      }
      // no metadata stream exists for block: the tag is ignored there
      next[id] = *stream;
    }
    // untagged and ambiguously tagged pools fall to client_other via
    // lookup miss
  }
  pool_streams = std::move(next);
  dout(20) << __func__ << " classified " << pool_streams.size()
	   << " of " << map.get_pools().size() << " pools" << dendl;
}

void BfqScheduler::set_pool_streams(
  std::unordered_map<int64_t, bfq_stream_t> &&map)
{
  pool_streams = std::move(map);
}

void BfqScheduler::activate_stream(bfq_stream_t s)
{
  const auto g = bfq_group_of(s);
  auto &group_tree = group_trees[idx(g)];
  const bool group_was_idle = !group_tree.has_active();
  auto &stream = streams[idx(s)];
  stream.next_budget = std::clamp(stream.next_budget, min_budget, max_budget);
  group_tree.activate(bfq_index_in_group(s), stream.next_budget,
		      stream_weights[idx(s)]);
  if (group_was_idle) {
    // the group's expected service is one full budget; the estimate
    // is corrected to actual service when the round expires
    root_tree.activate(idx(g), max_budget, group_weights[idx(g)]);
  }
}

void BfqScheduler::begin_round(bfq_stream_t s)
{
  auto &stream = streams[idx(s)];
  in_service = s;
  stream.budget_remaining = static_cast<int64_t>(stream.next_budget);
  stream.served_round = 0;
  // group_served needs no reset here: it only accumulates while a
  // round is in service, and end_round() consumed and zeroed it when
  // the previous round expired
  round_start = ceph::coarse_mono_clock::now();
  dout(20) << __func__ << " " << bfq_stream_name(s)
	   << " budget " << stream.next_budget << dendl;
}

void BfqScheduler::end_round(expire_reason reason)
{
  const bfq_stream_t s = *in_service;
  auto &stream = streams[idx(s)];
  const auto g = bfq_group_of(s);
  auto &group_tree = group_trees[idx(g)];
  const uint64_t served = stream.served_round;

  // budget feedback (simplified from the kernel): grow when the
  // stream exhausted its budget while still backlogged, adapt down to
  // observed demand when it emptied, pin to actual progress on
  // timeout
  switch (reason) {
  case expire_reason::exhausted:
    stream.next_budget = std::min(max_budget, stream.next_budget * 2);
    break;
  case expire_reason::emptied:
  case expire_reason::timed_out:
    stream.next_budget = std::clamp(served, min_budget, max_budget);
    break;
  }

  const bool backlogged = !stream.items.empty();
  group_tree.expire(bfq_index_in_group(s), served, stream.next_budget,
		    stream_weights[idx(s)], backlogged);

  root_tree.expire(idx(g), group_served[idx(g)], max_budget,
		   group_weights[idx(g)], group_tree.has_active());
  // the group tally was just consumed by the root expire; zero it here
  // so the next begin_round() starts from a clean slate without having
  // to reset it itself
  group_served[idx(g)] = 0;
  in_service.reset();

  dout(20) << __func__ << " " << bfq_stream_name(s)
	   << " reason " << static_cast<int>(reason)
	   << " served " << served
	   << " next_budget " << stream.next_budget
	   << dendl;
}

void BfqScheduler::enqueue_high(unsigned priority, OpSchedulerItem &&item,
				bool front)
{
  // each band drains from the BACK of its list (dequeue() pops
  // .back()), so the head of the band is the back: front insertion is
  // push_back, normal FIFO insertion is push_front (as in mclock)
  if (front) {
    high_priority[priority].push_back(std::move(item));
  } else {
    high_priority[priority].push_front(std::move(item));
  }
}

void BfqScheduler::enqueue(OpSchedulerItem &&item)
{
  maybe_refresh_config();
  const unsigned priority = item.get_priority();

  if (SchedulerClass::immediate == item.get_scheduler_class()) {
    enqueue_high(immediate_class_priority, std::move(item));
  } else if (priority >= cutoff_priority) {
    enqueue_high(priority, std::move(item));
  } else {
    const bfq_stream_t s = classify(item);
    const uint64_t scaled = calc_scaled_cost(item.get_cost());
    item.set_qos_cost(static_cast<uint32_t>(
      std::min<uint64_t>(scaled, std::numeric_limits<uint32_t>::max())));
    dout(20) << __func__ << " " << bfq_stream_name(s)
	     << " cost " << item.get_cost()
	     << " scaled_cost " << scaled
	     << dendl;
    auto &stream = streams[idx(s)];
    const bool was_idle = stream.items.empty();
    stream.items.push_back(std::move(item));
    ++fair_queued;
    if (was_idle && in_service != s) {
      // an empty stream that is not in service is never on its tree;
      // an in-service stream keeps its round until it expires
      activate_stream(s);
    }
  }
}

void BfqScheduler::enqueue_front(OpSchedulerItem &&item)
{
  maybe_refresh_config();
  const unsigned priority = item.get_priority();
  if (SchedulerClass::immediate == item.get_scheduler_class()) {
    enqueue_high(immediate_class_priority, std::move(item), true);
  } else if (priority >= cutoff_priority) {
    enqueue_high(priority, std::move(item), true);
  } else {
    // like mclock: the fair hierarchy cannot insert at the front, so
    // requeued items bypass it via the strict queue at priority 0
    enqueue_high(0, std::move(item), true);
  }
}

WorkItem BfqScheduler::dequeue()
{
  maybe_refresh_config();
  if (!high_priority.empty()) {
    auto iter = high_priority.begin();
    // invariant: high_priority entries are never empty
    ceph_assert(!iter->second.empty());
    WorkItem ret{std::move(iter->second.back())};
    iter->second.pop_back();
    if (iter->second.empty()) {
      high_priority.erase(iter);
    }
    return ret;
  }
  ceph_assert(fair_queued > 0 &&
	      "Impossible, must have checked empty() first");
  return dequeue_fair();
}

OpSchedulerItem BfqScheduler::dequeue_fair()
{
  if (in_service) {
    auto &stream = streams[idx(*in_service)];
    if (stream.items.empty()) {
      end_round(expire_reason::emptied);
    } else if (stream.budget_remaining <= 0) {
      end_round(expire_reason::exhausted);
    } else if (ceph::coarse_mono_clock::now() - round_start >
	       budget_timeout) {
      end_round(expire_reason::timed_out);
    }
  }
  if (!in_service) {
    // two-level B-WF2Q+ selection: group, then leaf within the group.
    // Invariant: every non-empty stream is on its group tree (or in
    // service) and every group with active leaves is on the root
    // tree, so fair_queued > 0 guarantees both selections succeed.
    auto gsel = root_tree.select();
    ceph_assert(gsel);
    auto lsel = group_trees[*gsel].select();
    ceph_assert(lsel);
    begin_round(bfq_stream_at(*gsel, *lsel));
  }

  const bfq_stream_t s = *in_service;
  auto &stream = streams[idx(s)];
  ceph_assert(!stream.items.empty());
  OpSchedulerItem item = std::move(stream.items.front());
  stream.items.pop_front();
  --fair_queued;

  const uint64_t served = calc_scaled_cost(item.get_cost());
  stream.budget_remaining -= static_cast<int64_t>(served);
  stream.served_round += served;
  group_served[idx(bfq_group_of(s))] += served;
  group_trees[idx(bfq_group_of(s))].charge(served);
  root_tree.charge(served);
  return item;
}

void BfqScheduler::dump(ceph::Formatter &f) const
{
  f.open_object_section("queue_sizes");
  f.dump_unsigned("high_priority_queue", high_priority.size());
  f.dump_unsigned("fair_queued", fair_queued);
  f.close_section();

  f.open_object_section("bfq");
  f.dump_string("in_service",
		in_service ? std::string(bfq_stream_name(*in_service))
			   : "none");
  f.open_object_section("root_tree");
  root_tree.dump(f);
  f.close_section();
  f.open_array_section("groups");
  for (size_t g = 0; g < bfq_num_groups; ++g) {
    f.open_object_section("group");
    f.dump_string("name", std::string(
      bfq_group_name(static_cast<bfq_group_t>(g))));
    f.dump_unsigned("weight", group_weights[g]);
    f.open_object_section("tree");
    group_trees[g].dump(f);
    f.close_section();
    f.close_section();
  }
  f.close_section();
  f.open_array_section("streams");
  for (size_t s = 0; s < bfq_num_streams; ++s) {
    f.open_object_section("stream");
    f.dump_string("name", std::string(
      bfq_stream_name(static_cast<bfq_stream_t>(s))));
    f.dump_unsigned("weight", stream_weights[s]);
    f.dump_unsigned("queue_size", streams[s].items.size());
    f.dump_unsigned("next_budget", streams[s].next_budget);
    f.close_section();
  }
  f.close_section();
  f.close_section();

  f.open_object_section("HighPriorityQueue");
  for (auto it = high_priority.begin(); it != high_priority.end(); it++) {
    f.dump_int("priority", it->first);
    f.dump_int("queue_size", it->second.size());
  }
  f.close_section();
}

}
