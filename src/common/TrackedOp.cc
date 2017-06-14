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

void OpHistory::on_shutdown()
{
  Mutex::Locker history_lock(ops_history_lock);
  arrived.clear();
  duration.clear();
  slow_op.clear();
  shutdown = true;
}

void OpHistory::insert(utime_t now, TrackedOpRef op)
{
  Mutex::Locker history_lock(ops_history_lock);
  if (shutdown)
    return;
  duration.insert(make_pair(op->get_duration(), op));
  arrived.insert(make_pair(op->get_initiated(), op));
  if (op->get_duration() >= history_slow_op_threshold)
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

void OpHistory::dump_ops(utime_t now, Formatter *f)
{
  Mutex::Locker history_lock(ops_history_lock);
  cleanup(now);
  f->open_object_section("op_history");
  f->dump_int("size", history_size);
  f->dump_int("duration", history_duration);
  {
    f->open_array_section("ops");
    for (set<pair<utime_t, TrackedOpRef> >::const_iterator i =
	   arrived.begin();
	 i != arrived.end();
	 ++i) {
      f->open_object_section("op");
      i->second->dump(now, f);
      f->close_section();
    }
    f->close_section();
  }
  f->close_section();
}

void OpHistory::dump_ops_by_duration(utime_t now, Formatter *f)
{
  Mutex::Locker history_lock(ops_history_lock);
  cleanup(now);
  f->open_object_section("op_history");
  f->dump_int("size", history_size);
  f->dump_int("duration", history_duration);
  {
    f->open_array_section("ops");
    if (arrived.size()) {
      vector<pair<double, TrackedOpRef> > durationvec;
      durationvec.reserve(arrived.size());

      for (set<pair<utime_t, TrackedOpRef> >::const_iterator i =
	     arrived.begin();
	   i != arrived.end();
	   ++i) {
	durationvec.push_back(pair<double, TrackedOpRef>(i->second->get_duration(), i->second));
      }

      sort(durationvec.begin(), durationvec.end());

      for (auto i = durationvec.rbegin(); i != durationvec.rend(); ++i) {
	f->open_object_section("op");
	i->second->dump(now, f);
	f->close_section();
      }
    }
    f->close_section();
  }
  f->close_section();
}

struct ShardedTrackingData {
  Mutex ops_in_flight_lock_sharded;
  TrackedOp::tracked_op_list_t ops_in_flight_sharded;
  explicit ShardedTrackingData(string lock_name):
      ops_in_flight_lock_sharded(lock_name.c_str()) {}
};

OpTracker::OpTracker(CephContext *cct_, bool tracking, uint32_t num_shards):
  seq(0),
  num_optracker_shards(num_shards),
  complaint_time(0), log_threshold(0),
  tracking_enabled(tracking),
  lock("OpTracker::lock"), cct(cct_) {
    for (uint32_t i = 0; i < num_optracker_shards; i++) {
      char lock_name[32] = {0};
      snprintf(lock_name, sizeof(lock_name), "%s:%d", "OpTracker::ShardedLock", i);
      ShardedTrackingData* one_shard = new ShardedTrackingData(lock_name);
      sharded_in_flight_list.push_back(one_shard);
    }
}

OpTracker::~OpTracker() {
  while (!sharded_in_flight_list.empty()) {
    assert((sharded_in_flight_list.back())->ops_in_flight_sharded.empty());
    delete sharded_in_flight_list.back();
    sharded_in_flight_list.pop_back();
  }
}

bool OpTracker::dump_historic_ops(Formatter *f, bool by_duration)
{
  RWLock::RLocker l(lock);
  if (!tracking_enabled)
    return false;

  utime_t now = ceph_clock_now();
  if (by_duration) {
    history.dump_ops_by_duration(now, f);
  } else {
    history.dump_ops(now, f);
  }
  return true;
}

void OpHistory::dump_slow_ops(utime_t now, Formatter *f)
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
      f->open_object_section("Op");
      i->second->dump(now, f);
      f->close_section();
    }
    f->close_section();
  }
  f->close_section();
}

bool OpTracker::dump_historic_slow_ops(Formatter *f)
{
  RWLock::RLocker l(lock);
  if (!tracking_enabled)
    return false;

  utime_t now = ceph_clock_now();
  history.dump_slow_ops(now, f);
  return true;
}

bool OpTracker::dump_ops_in_flight(Formatter *f, bool print_only_blocked)
{
  RWLock::RLocker l(lock);
  if (!tracking_enabled)
    return false;

  f->open_object_section("ops_in_flight"); // overall dump
  uint64_t total_ops_in_flight = 0;
  f->open_array_section("ops"); // list of TrackedOps
  utime_t now = ceph_clock_now();
  for (uint32_t i = 0; i < num_optracker_shards; i++) {
    ShardedTrackingData* sdata = sharded_in_flight_list[i];
    assert(NULL != sdata); 
    Mutex::Locker locker(sdata->ops_in_flight_lock_sharded);
    for (auto& op : sdata->ops_in_flight_sharded) {
      if (print_only_blocked && (now - op.get_initiated() <= complaint_time))
	break;
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

bool OpTracker::register_inflight_op(TrackedOp *i)
{
  RWLock::RLocker l(lock);
  if (!tracking_enabled)
    return false;

  uint64_t current_seq = ++seq;
  uint32_t shard_index = current_seq % num_optracker_shards;
  ShardedTrackingData* sdata = sharded_in_flight_list[shard_index];
  assert(NULL != sdata);
  {
    Mutex::Locker locker(sdata->ops_in_flight_lock_sharded);
    sdata->ops_in_flight_sharded.push_back(*i);
    i->seq = current_seq;
  }
  return true;
}

void OpTracker::unregister_inflight_op(TrackedOp *i)
{
  // caller checks;
  assert(i->state);

  uint32_t shard_index = i->seq % num_optracker_shards;
  ShardedTrackingData* sdata = sharded_in_flight_list[shard_index];
  assert(NULL != sdata);
  {
    Mutex::Locker locker(sdata->ops_in_flight_lock_sharded);
    auto p = sdata->ops_in_flight_sharded.iterator_to(*i);
    sdata->ops_in_flight_sharded.erase(p);
  }
  i->_unregistered();

  RWLock::RLocker l(lock);
  if (!tracking_enabled)
    delete i;
  else {
    i->state = TrackedOp::STATE_HISTORY;
    utime_t now = ceph_clock_now();
    history.insert(now, TrackedOpRef(i));
  }
}

bool OpTracker::check_ops_in_flight(std::vector<string> &warning_vector, int *slow)
{
  RWLock::RLocker l(lock);
  if (!tracking_enabled)
    return false;

  utime_t now = ceph_clock_now();
  utime_t too_old = now;
  too_old -= complaint_time;
  utime_t oldest_op = now;
  uint64_t total_ops_in_flight = 0;

  for (uint32_t i = 0; i < num_optracker_shards; i++) {
    ShardedTrackingData* sdata = sharded_in_flight_list[i];
    assert(NULL != sdata);
    Mutex::Locker locker(sdata->ops_in_flight_lock_sharded);
    if (!sdata->ops_in_flight_sharded.empty()) {
      utime_t oldest_op_tmp =
	sdata->ops_in_flight_sharded.front().get_initiated();
      if (oldest_op_tmp < oldest_op) {
        oldest_op = oldest_op_tmp;
      }
    }
    total_ops_in_flight += sdata->ops_in_flight_sharded.size();
  }

  if (0 == total_ops_in_flight)
    return false;

  utime_t oldest_secs = now - oldest_op;

  dout(10) << "ops_in_flight.size: " << total_ops_in_flight
           << "; oldest is " << oldest_secs
           << " seconds old" << dendl;

  if (oldest_secs < complaint_time)
    return false;

  warning_vector.reserve(log_threshold + 1);
  //store summary message
  warning_vector.push_back("");

  int _slow = 0;    // total slow
  if (!slow)
    slow = &_slow; 
  else
    *slow = _slow;  // start from 0 anyway
  int warned = 0;   // total logged
  for (uint32_t iter = 0; iter < num_optracker_shards; iter++) {
    ShardedTrackingData* sdata = sharded_in_flight_list[iter];
    assert(NULL != sdata);
    Mutex::Locker locker(sdata->ops_in_flight_lock_sharded);
    if (sdata->ops_in_flight_sharded.empty())
      continue;
    auto i = sdata->ops_in_flight_sharded.begin();
    while (i != sdata->ops_in_flight_sharded.end() &&
	   i->get_initiated() < too_old) {
      (*slow)++;

      // exponential backoff of warning intervals
      if (warned < log_threshold &&
	  (i->get_initiated() + (complaint_time *
				 i->warn_interval_multiplier)) < now) {
        // will warn, increase counter
        warned++;

        utime_t age = now - i->get_initiated();
        stringstream ss;
        ss << "slow request " << age << " seconds old, received at "
           << i->get_initiated() << ": " << i->get_desc()
	   << " currently "
	   << (i->current ? i->current : i->state_string());
        warning_vector.push_back(ss.str());

        // only those that have been shown will backoff
        i->warn_interval_multiplier *= 2;
      }
      ++i;
    }
  }

  // only summarize if we warn about any.  if everything has backed
  // off, we will stay silent.
  if (warned > 0) {
    stringstream ss;
    ss << *slow << " slow requests, " << warned << " included below; oldest blocked for > "
       << oldest_secs << " secs";
    warning_vector[0] = ss.str();
  }

  return warned > 0;
}

void OpTracker::get_age_ms_histogram(pow2_hist_t *h)
{
  h->clear();
  utime_t now = ceph_clock_now();

  for (uint32_t iter = 0; iter < num_optracker_shards; iter++) {
    ShardedTrackingData* sdata = sharded_in_flight_list[iter];
    assert(NULL != sdata);
    Mutex::Locker locker(sdata->ops_in_flight_lock_sharded);

    for (auto& i : sdata->ops_in_flight_sharded) {
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
    Mutex::Locker l(lock);
    events.push_back(Event(stamp, event));
    current = events.back().c_str();
  }
  dout(6) <<  "seq: " << seq
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
    Mutex::Locker l(lock);
    events.push_back(Event(stamp, event));
    current = event;
  }
  dout(6) <<  "seq: " << seq
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
    f->open_array_section("type_data");
    _dump(f);
    f->close_section();
  }
}
