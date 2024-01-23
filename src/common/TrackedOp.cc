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

using std::list;
using std::make_pair;
using std::ostream;
using std::pair;
using std::set;
using std::string;
using std::stringstream;

using ceph::Formatter;

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "-- op tracker -- ";
}

void OpHistoryServiceThread::break_thread() {
  queue_spinlock.lock();
  _external_queue.clear();
  _break_thread = true;
  queue_spinlock.unlock();
}

void* OpHistoryServiceThread::entry() {
  int sleep_time = 1000;
  list<pair<utime_t, TrackedOpRef>> internal_queue;
  while (1) {
    queue_spinlock.lock();
    if (_break_thread) {
      queue_spinlock.unlock();
      break;
    }
    internal_queue.swap(_external_queue);
    queue_spinlock.unlock();
    if (internal_queue.empty()) {
      usleep(sleep_time);
      if (sleep_time < 128000) {
        sleep_time <<= 2;
      }
    } else {
      sleep_time = 1000;
    }

    while (!internal_queue.empty()) {
      pair<utime_t, TrackedOpRef> op = internal_queue.front();
      _ophistory->_insert_delayed(op.first, op.second);
      internal_queue.pop_front();
    }
  }
  return nullptr;
}


void OpHistory::on_shutdown()
{
  opsvc.break_thread();
  opsvc.join();
  std::lock_guard history_lock(ops_history_lock);
  arrived.clear();
  duration.clear();
  slow_op.clear();
  shutdown = true;
}

void OpHistory::_insert_delayed(const utime_t& now, TrackedOpRef op)
{
  std::lock_guard history_lock(ops_history_lock);
  if (shutdown)
    return;
  double opduration = op->get_duration();
  duration.insert(make_pair(opduration, op));
  arrived.insert(make_pair(op->get_initiated(), op));
  if (opduration >= history_slow_op_threshold.load())
    slow_op.insert(make_pair(op->get_initiated(), op));
  cleanup(now);
}

void OpHistory::cleanup(utime_t now)
{
  while (arrived.size() &&
	 (now - arrived.begin()->first >
	  (double)(history_duration.load()))) {
    duration.erase(make_pair(
	arrived.begin()->second->get_duration(),
	arrived.begin()->second));
    arrived.erase(arrived.begin());
  }

  while (duration.size() > history_size.load()) {
    arrived.erase(make_pair(
	duration.begin()->second->get_initiated(),
	duration.begin()->second));
    duration.erase(duration.begin());
  }

  while (slow_op.size() > history_slow_op_size.load()) {
    slow_op.erase(make_pair(
	slow_op.begin()->second->get_initiated(),
	slow_op.begin()->second));
  }
}

void OpHistory::dump_ops(utime_t now, Formatter *f, set<string> filters, bool by_duration)
{
  std::lock_guard history_lock(ops_history_lock);
  cleanup(now);
  f->open_object_section("op_history");
  f->dump_int("size", history_size.load());
  f->dump_int("duration", history_duration.load());
  {
    f->open_array_section("ops");
    auto dump_fn = [&f, &now, &filters](auto begin_iter, auto end_iter) {
      for (auto i=begin_iter; i!=end_iter; ++i) {
	if (!i->second->filter_out(filters))
	  continue;
	f->open_object_section("op");
	i->second->dump(now, f, OpTracker::default_dumper);
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

struct ShardedTrackingData {
  ceph::mutex ops_in_flight_lock_sharded;
  TrackedOp::tracked_op_list_t ops_in_flight_sharded;
  explicit ShardedTrackingData(string lock_name)
    : ops_in_flight_lock_sharded(ceph::make_mutex(lock_name)) {}
};

OpTracker::OpTracker(CephContext *cct_, bool tracking, uint32_t num_shards):
  seq(0),
  num_optracker_shards(num_shards),
  complaint_time(0), log_threshold(0),
  tracking_enabled(tracking),
  cct(cct_) {
    for (uint32_t i = 0; i < num_optracker_shards; i++) {
      char lock_name[32] = {0};
      snprintf(lock_name, sizeof(lock_name), "%s:%" PRIu32, "OpTracker::ShardedLock", i);
      ShardedTrackingData* one_shard = new ShardedTrackingData(lock_name);
      sharded_in_flight_list.push_back(one_shard);
    }
}

OpTracker::~OpTracker() {
  while (!sharded_in_flight_list.empty()) {
    ShardedTrackingData* sdata = sharded_in_flight_list.back();
    ceph_assert(NULL != sdata);
    while (!sdata->ops_in_flight_sharded.empty()) {
      {
        std::lock_guard locker(sdata->ops_in_flight_lock_sharded);
        sdata->ops_in_flight_sharded.pop_back();
      }
    }
    ceph_assert((sharded_in_flight_list.back())->ops_in_flight_sharded.empty());
    delete sharded_in_flight_list.back();
    sharded_in_flight_list.pop_back();
  }
}

bool OpTracker::dump_historic_ops(Formatter *f, bool by_duration, set<string> filters)
{
  if (!tracking_enabled)
    return false;

  std::shared_lock l{lock};
  utime_t now = ceph_clock_now();
  history.dump_ops(now, f, filters, by_duration);
  return true;
}

void OpHistory::dump_slow_ops(utime_t now, Formatter *f, set<string> filters)
{
  std::lock_guard history_lock(ops_history_lock);
  cleanup(now);
  f->open_object_section("OpHistory slow ops");
  f->dump_int("num to keep", history_slow_op_size.load());
  f->dump_int("threshold to keep", history_slow_op_threshold.load());
  {
    f->open_array_section("Ops");
    for (set<pair<utime_t, TrackedOpRef> >::const_iterator i =
	   slow_op.begin();
	 i != slow_op.end();
	 ++i) {
      if (!i->second->filter_out(filters))
        continue;
      f->open_object_section("Op");
      i->second->dump(now, f, OpTracker::default_dumper);
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

  std::shared_lock l{lock};
  utime_t now = ceph_clock_now();
  history.dump_slow_ops(now, f, filters);
  return true;
}

bool OpTracker::dump_ops_in_flight(Formatter *f, bool print_only_blocked, set<string> filters, dumper lambda)
{
  if (!tracking_enabled)
    return false;

  std::shared_lock l{lock};
  f->open_object_section("ops_in_flight"); // overall dump
  uint64_t total_ops_in_flight = 0;
  f->open_array_section("ops"); // list of TrackedOps
  utime_t now = ceph_clock_now();
  for (uint32_t i = 0; i < num_optracker_shards; i++) {
    ShardedTrackingData* sdata = sharded_in_flight_list[i];
    ceph_assert(NULL != sdata); 
    std::lock_guard locker(sdata->ops_in_flight_lock_sharded);
    for (auto& op : sdata->ops_in_flight_sharded) {
      if (print_only_blocked && (now - op.get_initiated() <= complaint_time))
        break;
      if (!op.filter_out(filters))
        continue;
      f->open_object_section("op");
      op.dump(now, f, lambda);
      f->close_section(); // this TrackedOp
      total_ops_in_flight++;
    }
  }
  f->close_section(); // list of TrackedOps
  if (print_only_blocked) {
    f->dump_float("complaint_time", complaint_time);
    f->dump_int("num_blocked_ops", total_ops_in_flight);
  } else {
    f->dump_int("num_ops", total_ops_in_flight);
  }
  f->close_section(); // overall dump
  return true;
}

bool OpTracker::register_inflight_op(TrackedOp *i)
{
  if (!tracking_enabled)
    return false;

  std::shared_lock l{lock};
  uint64_t current_seq = ++seq;
  uint32_t shard_index = current_seq % num_optracker_shards;
  ShardedTrackingData* sdata = sharded_in_flight_list[shard_index];
  ceph_assert(NULL != sdata);
  {
    std::lock_guard locker(sdata->ops_in_flight_lock_sharded);
    sdata->ops_in_flight_sharded.push_back(*i);
    i->seq = current_seq;
  }
  return true;
}

void OpTracker::unregister_inflight_op(TrackedOp* const i)
{
  // caller checks;
  ceph_assert(i->state);

  uint32_t shard_index = i->seq % num_optracker_shards;
  ShardedTrackingData* sdata = sharded_in_flight_list[shard_index];
  ceph_assert(NULL != sdata);
  {
    std::lock_guard locker(sdata->ops_in_flight_lock_sharded);
    auto p = sdata->ops_in_flight_sharded.iterator_to(*i);
    sdata->ops_in_flight_sharded.erase(p);
  }
}

void OpTracker::record_history_op(TrackedOpRef&& i)
{
  std::shared_lock l{lock};
  history.insert(ceph_clock_now(), std::move(i));
}

bool OpTracker::visit_ops_in_flight(utime_t* oldest_secs,
				    std::function<bool(TrackedOp&)>&& visit)
{
  if (!tracking_enabled)
    return false;

  const utime_t now = ceph_clock_now();
  utime_t oldest_op = now;
  // single representation of all inflight operations reunified
  // from OpTracker's shards. TrackedOpRef extends the lifetime
  // to carry the ops outside of the critical section, and thus
  // allows to call the visitor without any lock being held.
  // This simplifies the contract on API at the price of plenty
  // additional moves and atomic ref-counting. This seems OK as
  // `visit_ops_in_flight()` is definitely not intended for any
  // hot path.
  std::vector<TrackedOpRef> ops_in_flight;

  std::shared_lock l{lock};
  for (const auto sdata : sharded_in_flight_list) {
    ceph_assert(sdata);
    std::lock_guard locker(sdata->ops_in_flight_lock_sharded);
    if (!sdata->ops_in_flight_sharded.empty()) {
      utime_t oldest_op_tmp =
	sdata->ops_in_flight_sharded.front().get_initiated();
      if (oldest_op_tmp < oldest_op) {
        oldest_op = oldest_op_tmp;
      }
    }
    std::transform(std::begin(sdata->ops_in_flight_sharded),
                   std::end(sdata->ops_in_flight_sharded),
                   std::back_inserter(ops_in_flight),
                   [] (TrackedOp& op) { return TrackedOpRef(&op); });
  }
  if (ops_in_flight.empty())
    return false;
  *oldest_secs = now - oldest_op;
  dout(10) << "ops_in_flight.size: " << ops_in_flight.size()
           << "; oldest is " << *oldest_secs
           << " seconds old" << dendl;

  if (*oldest_secs < complaint_time)
    return false;

  l.unlock();
  for (auto& op : ops_in_flight) {
    // `lock` neither `ops_in_flight_lock_sharded` should be held when
    // calling the visitor. Otherwise `OSD::get_health_metrics()` can
    // dead-lock due to the `~TrackedOp()` calling `record_history_op()`
    // or `unregister_inflight_op()`.
    if (!visit(*op))
      break;
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
       << op.state_string();
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

  for (uint32_t iter = 0; iter < num_optracker_shards; iter++) {
    ShardedTrackingData* sdata = sharded_in_flight_list[iter];
    ceph_assert(NULL != sdata);
    std::lock_guard locker(sdata->ops_in_flight_lock_sharded);

    for (auto& i : sdata->ops_in_flight_sharded) {
      utime_t age = now - i.get_initiated();
      uint32_t ms = (long)(age * 1000.0);
      h->add(ms);
    }
  }
}


#undef dout_context
#define dout_context tracker->cct

void TrackedOp::mark_event(std::string_view event, utime_t stamp)
{
  if (!state)
    return;

  {
    std::lock_guard l(lock);
    events.emplace_back(stamp, event);
  }
  dout(6) << " seq: " << seq
	  << ", time: " << stamp
	  << ", event: " << event
	  << ", op: " << get_desc()
	  << dendl;
  _event_marked();
}

void TrackedOp::dump(utime_t now, Formatter *f, OpTracker::dumper lambda) const
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
    lambda(*this, f);
    f->close_section();
  }
}
