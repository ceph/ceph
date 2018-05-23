// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 * Copyright 2013 Inktank
 */

#include "TrackedOp.h"

#define dout_context cct
#define dout_subsys ceph_subsys_optracker
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "-- op tracker -- ";
}

void OpHistoryServiceThread::break_thread() {
  ceph::spin::trying_guard<std::mutex> sl(queue_lock);
  _break_thread = true;
}

void* OpHistoryServiceThread::entry() {
  int sleep_time = 1000;
  queue_t internal_queue;
  while (1) {
    {
      ceph::spin::trying_guard<std::mutex> sl(queue_lock);
      if (_break_thread) {
        break;
      }
      internal_queue.swap(_external_queue);
    }
    if (internal_queue.empty()) {
      usleep(sleep_time);
      if (sleep_time < 128000) {
        sleep_time <<= 2;
      }
    } else {
      sleep_time = 1000;
    }

    while (!internal_queue.empty()) {
      queue_item_t& item = internal_queue.front();
      _ophistory->_insert_delayed(item.time, item.op);
      internal_queue.pop_front();
      delete &item;
    }
  }
  return nullptr;
}

OpHistoryServiceThread::~OpHistoryServiceThread() {
  if (!_external_queue.empty()) {
    assert(_break_thread);
  }

  while (!_external_queue.empty()) {
    delete &_external_queue.back();
    _external_queue.pop_back();
  }
}


void OpHistory::on_shutdown()
{
  opsvc.break_thread();
  opsvc.join();
  Mutex::Locker history_lock(ops_history_lock);
  arrived.clear();
  duration.clear();
  slow_op.clear();
  shutdown = true;
}

void OpHistory::_insert_delayed(const utime_t& now, TrackedOpRef op)
{
  Mutex::Locker history_lock(ops_history_lock);
  if (shutdown)
    return;
  double opduration = op->get_duration();
  duration.insert(make_pair(opduration, op));
  arrived.insert(make_pair(op->get_initiated(), op));
  if (opduration >= history_slow_op_threshold)
    slow_op.insert(make_pair(op->get_initiated(), op));
  cleanup(now);
}

void OpHistory::cleanup(utime_t now)
{
  while (arrived.size() &&
	 (now - arrived.begin()->first >
	  (double)(history_duration))) {
    duration.erase(make_pair(
	arrived.begin()->second->get_duration(),
	arrived.begin()->second));
    arrived.erase(arrived.begin());
  }

  while (duration.size() > history_size) {
    arrived.erase(make_pair(
	duration.begin()->second->get_initiated(),
	duration.begin()->second));
    duration.erase(duration.begin());
  }

  while (slow_op.size() > history_slow_op_size) {
    slow_op.erase(make_pair(
	slow_op.begin()->second->get_initiated(),
	slow_op.begin()->second));
  }
}

void OpHistory::dump_ops(utime_t now, Formatter *f, set<string> filters, bool by_duration)
{
  Mutex::Locker history_lock(ops_history_lock);
  cleanup(now);
  f->open_object_section("op_history");
  f->dump_int("size", history_size);
  f->dump_int("duration", history_duration);
  {
    f->open_array_section("ops");
    auto dump_fn = [&f, &now, &filters](auto begin_iter, auto end_iter) {
      for (auto i=begin_iter; i!=end_iter; ++i) {
	if (!i->second->filter_out(filters))
	  continue;
	f->open_object_section("op");
	i->second->dump(now, f);
	f->close_section();
      }
    };

    if (by_duration) {
      dump_fn(duration.rbegin(), duration.rend());
    } else {
      dump_fn(arrived.begin(), arrived.end());
    }
    f->close_section();
  }
  f->close_section();
}

OpTracker::OpTracker(CephContext *cct_, bool tracking, uint32_t num_shards):
  seq(0),
  sharded_in_flight_list(num_shards, [](const size_t i, auto emplacer) {
    char lock_name[32];
    snprintf(lock_name, sizeof(lock_name), "%s:%zd",
	     "OpTracker::ShardedLock", i);
    emplacer.emplace(lock_name);
  }),
  complaint_time(0),
  log_threshold(0),
  tracking_enabled(tracking),
  cct(cct_) {
}

OpTracker::~OpTracker() {
  for (const auto& shard : sharded_in_flight_list) {
    assert(shard.ops_in_flight_sharded.empty());
  }
}

bool OpTracker::dump_historic_ops(Formatter *f, bool by_duration, set<string> filters)
{
  if (!tracking_enabled)
    return false;

  utime_t now = ceph_clock_now();
  history.dump_ops(now, f, filters, by_duration);
  return true;
}

void OpHistory::dump_slow_ops(utime_t now, Formatter *f, set<string> filters)
{
  Mutex::Locker history_lock(ops_history_lock);
  cleanup(now);
  f->open_object_section("OpHistory slow ops");
  f->dump_int("num to keep", history_slow_op_size);
  f->dump_int("threshold to keep", history_slow_op_threshold);
  {
    f->open_array_section("Ops");
    for (set<pair<utime_t, TrackedOpRef> >::const_iterator i =
	   slow_op.begin();
	 i != slow_op.end();
	 ++i) {
      if (!i->second->filter_out(filters))
        continue;
      f->open_object_section("Op");
      i->second->dump(now, f);
      f->close_section();
    }
    f->close_section();
  }
  f->close_section();
}

bool OpTracker::dump_historic_slow_ops(Formatter *f, set<string> filters)
{
  if (!tracking_enabled)
    return false;

  utime_t now = ceph_clock_now();
  history.dump_slow_ops(now, f, filters);
  return true;
}

bool OpTracker::dump_ops_in_flight(Formatter *f, bool print_only_blocked, set<string> filters)
{
  if (!tracking_enabled)
    return false;

  f->open_object_section("ops_in_flight"); // overall dump
  uint64_t total_ops_in_flight = 0;
  f->open_array_section("ops"); // list of TrackedOps
  utime_t now = ceph_clock_now();
  for (auto& sdata : sharded_in_flight_list) {
    Mutex::Locker locker(sdata.ops_in_flight_lock_sharded);
    for (auto& op : sdata.ops_in_flight_sharded) {
      if (print_only_blocked && (now - op.get_initiated() <= complaint_time))
        break;
      if (!op.filter_out(filters))
        continue;
      f->open_object_section("op");
      op.dump(now, f);
      f->close_section(); // this TrackedOp
      total_ops_in_flight++;
    }
  }
  f->close_section(); // list of TrackedOps
  if (print_only_blocked) {
    f->dump_float("complaint_time", complaint_time);
    f->dump_int("num_blocked_ops", total_ops_in_flight);
  } else
    f->dump_int("num_ops", total_ops_in_flight);
  f->close_section(); // overall dump
  return true;
}

void OpTracker::register_inflight_op(TrackedOp *i,
				     const std::uint32_t shard_index)
{
  if (!tracking_enabled)
    return;

  ShardedTrackingData& sdata = sharded_in_flight_list.get_shard(shard_index);
  {
    Mutex::Locker locker(sdata.ops_in_flight_lock_sharded);
    sdata.ops_in_flight_sharded.push_back(*i);
    i->seq = shard_index;
    // default state is STATE_UNTRACKED. Altough TrackedOp::state
    // is std::atomic, we're setting it inside critical section.
    // This means the cost of MFENCE on x86 should be relatively
    // small as LOCKed ADD/SUB already fence the memory.
    i->state = TrackedOp::STATE_LIVE;
  }
}

void OpTracker::unregister_inflight_op(TrackedOp* const i)
{
  // caller checks;
  assert(i->state);

  ShardedTrackingData& sdata = sharded_in_flight_list.get_shard(i->seq);
  {
    Mutex::Locker locker(sdata.ops_in_flight_lock_sharded);
    auto p = sdata.ops_in_flight_sharded.iterator_to(*i);
    sdata.ops_in_flight_sharded.erase(p);
  }
}

bool OpTracker::visit_ops_in_flight(utime_t* oldest_secs,
				    std::function<bool(TrackedOp&)>&& visit)
{
  if (!tracking_enabled)
    return false;

  const utime_t now = ceph_clock_now();
  utime_t oldest_op = now;
  uint64_t total_ops_in_flight = 0;

  for (const auto& sdata : sharded_in_flight_list) {
    Mutex::Locker locker(sdata.ops_in_flight_lock_sharded);
    if (!sdata.ops_in_flight_sharded.empty()) {
      utime_t oldest_op_tmp =
	sdata.ops_in_flight_sharded.front().get_initiated();
      if (oldest_op_tmp < oldest_op) {
        oldest_op = oldest_op_tmp;
      }
    }
    total_ops_in_flight += sdata.ops_in_flight_sharded.size();
  }
  if (!total_ops_in_flight)
    return false;
  *oldest_secs = now - oldest_op;
  dout(10) << "ops_in_flight.size: " << total_ops_in_flight
           << "; oldest is " << *oldest_secs
           << " seconds old" << dendl;

  if (*oldest_secs < complaint_time)
    return false;

  for (auto& sdata : sharded_in_flight_list) {
    Mutex::Locker locker(sdata.ops_in_flight_lock_sharded);
    for (auto& op : sdata.ops_in_flight_sharded) {
      if (!visit(op))
	break;
    }
  }
  return true;
}

bool OpTracker::with_slow_ops_in_flight(utime_t* oldest_secs,
					int* num_slow_ops,
					int* num_warned_ops,
					std::function<void(TrackedOp&)>&& on_warn)
{
  const utime_t now = ceph_clock_now();
  auto too_old = now;
  too_old -= complaint_time;
  int slow = 0;
  int warned = 0;
  auto check = [&](TrackedOp& op) {
    if (op.get_initiated() >= too_old) {
      // no more slow ops in flight
      return false;
    }
    if (!op.warn_interval_multiplier)
      return true;
    slow++;
    if (warned >= log_threshold) {
      // enough samples of slow ops
      return true;
    }
    auto time_to_complain = (op.get_initiated() +
			     complaint_time * op.warn_interval_multiplier);
    if (time_to_complain >= now) {
      // complain later if the op is still in flight
      return true;
    }
    // will warn, increase counter
    warned++;
    on_warn(op);
    return true;
  };
  if (visit_ops_in_flight(oldest_secs, check)) {
    if (num_slow_ops) {
      *num_slow_ops = slow;
      *num_warned_ops = warned;
    }
    return true;
  } else {
    return false;
  }
}

bool OpTracker::check_ops_in_flight(std::string* summary,
				    std::vector<string> &warnings,
				    int *num_slow_ops)
{
  const utime_t now = ceph_clock_now();
  auto too_old = now;
  too_old -= complaint_time;
  int warned = 0;
  utime_t oldest_secs;
  auto warn_on_slow_op = [&](TrackedOp& op) {
    stringstream ss;
    utime_t age = now - op.get_initiated();
    ss << "slow request " << age << " seconds old, received at "
       << op.get_initiated() << ": " << op.get_desc()
       << " currently "
        << (op.current ? op.current : op.state_string());
    warnings.push_back(ss.str());
    // only those that have been shown will backoff
    op.warn_interval_multiplier *= 2;
  };
  int slow = 0;
  if (with_slow_ops_in_flight(&oldest_secs, &slow, &warned, warn_on_slow_op) &&
      slow > 0) {
    stringstream ss;
    ss << slow << " slow requests, "
       << warned << " included below; oldest blocked for > "
       << oldest_secs << " secs";
    *summary = ss.str();
    if (num_slow_ops) {
      *num_slow_ops = slow;
    }
    return true;
  } else {
    return false;
  }
}

void OpTracker::get_age_ms_histogram(pow2_hist_t *h)
{
  h->clear();
  utime_t now = ceph_clock_now();

  for (const auto& sdata : sharded_in_flight_list) {
    Mutex::Locker locker(sdata.ops_in_flight_lock_sharded);

    for (auto& i : sdata.ops_in_flight_sharded) {
      utime_t age = now - i.get_initiated();
      uint32_t ms = (long)(age * 1000.0);
      h->add(ms);
    }
  }
}


#undef dout_context
#define dout_context tracker->cct

void TrackedOp::mark_event_string(const string &event, utime_t stamp)
{
  if (!state)
    return;

  {
    std::unique_lock l(lock);
    events.emplace_back(stamp, event);
    current = events.back().c_str();
  }
  dout(6) << " seq: " << seq
	  << ", time: " << stamp
	  << ", event: " << event
	  << ", op: " << get_desc()
	  << dendl;
  _event_marked();
}

void TrackedOp::mark_event(const char *event, utime_t stamp)
{
  if (!state)
    return;

  {
    std::unique_lock l(lock);
    events.emplace_back(stamp, event);
    current = event;
  }
  dout(6) << " seq: " << seq
	  << ", time: " << stamp
	  << ", event: " << event
	  << ", op: " << get_desc()
	  << dendl;
  _event_marked();
}

void TrackedOp::dump(utime_t now, Formatter *f) const
{
  // Ignore if still in the constructor
  if (!state)
    return;
  f->dump_string("description", get_desc());
  f->dump_stream("initiated_at") << get_initiated();
  f->dump_float("age", now - get_initiated());
  f->dump_float("duration", get_duration());
  {
    f->open_object_section("type_data");
    _dump(f);
    f->close_section();
  }
}
