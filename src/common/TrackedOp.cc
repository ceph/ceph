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
#include "common/Formatter.h"
#include <iostream>
#include <vector>
#include "common/debug.h"
#include "common/config.h"
#include "msg/Message.h"
#include "include/assert.h"

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
  shutdown = true;
}

void OpHistory::insert(utime_t now, TrackedOpRef op)
{
  Mutex::Locker history_lock(ops_history_lock);
  if (shutdown)
    return;
  duration.insert(make_pair(op->get_duration(), op));
  arrived.insert(make_pair(op->get_initiated(), op));
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
}

void OpHistory::dump_ops(utime_t now, Formatter *f)
{
  Mutex::Locker history_lock(ops_history_lock);
  cleanup(now);
  f->open_object_section("OpHistory");
  f->dump_int("num to keep", history_size);
  f->dump_int("duration to keep", history_duration);
  {
    f->open_array_section("Ops");
    for (set<pair<utime_t, TrackedOpRef> >::const_iterator i =
	   arrived.begin();
	 i != arrived.end();
	 ++i) {
      f->open_object_section("Op");
      i->second->dump(now, f);
      f->close_section();
    }
    f->close_section();
  }
  f->close_section();
}

bool OpTracker::dump_historic_ops(Formatter *f)
{
  RWLock::RLocker l(lock);
  if (!tracking_enabled)
    return false;

  utime_t now = ceph_clock_now(cct);
  history.dump_ops(now, f);
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
  utime_t now = ceph_clock_now(cct);
  for (uint32_t i = 0; i < num_optracker_shards; i++) {
    ShardedTrackingData* sdata = sharded_in_flight_list[i];
    assert(NULL != sdata); 
    Mutex::Locker locker(sdata->ops_in_flight_lock_sharded);
    for (xlist<TrackedOp*>::iterator p = sdata->ops_in_flight_sharded.begin(); !p.end(); ++p) {
      if (print_only_blocked && (now - (*p)->get_initiated() <= complaint_time))
	break;
      f->open_object_section("op");
      (*p)->dump(now, f);
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

void OpTracker::register_inflight_op(xlist<TrackedOp*>::item *i)
{
  // caller checks;
  assert(tracking_enabled);

  uint64_t current_seq = seq.inc();
  uint32_t shard_index = current_seq % num_optracker_shards;
  ShardedTrackingData* sdata = sharded_in_flight_list[shard_index];
  assert(NULL != sdata);
  {
    Mutex::Locker locker(sdata->ops_in_flight_lock_sharded);
    sdata->ops_in_flight_sharded.push_back(i);
    sdata->ops_in_flight_sharded.back()->seq = current_seq;
  }
}

void OpTracker::unregister_inflight_op(TrackedOp *i)
{
  // caller checks;
  assert(i->is_tracked);

  uint32_t shard_index = i->seq % num_optracker_shards;
  ShardedTrackingData* sdata = sharded_in_flight_list[shard_index];
  assert(NULL != sdata);
  {
    Mutex::Locker locker(sdata->ops_in_flight_lock_sharded);
    assert(i->xitem.get_list() == &sdata->ops_in_flight_sharded);
    i->xitem.remove_myself();
  }
  i->_unregistered();

  if (!tracking_enabled)
    delete i;
  else {
    utime_t now = ceph_clock_now(cct);
    history.insert(now, TrackedOpRef(i));
  }
}

bool OpTracker::check_ops_in_flight(std::vector<string> &warning_vector)
{
  RWLock::RLocker l(lock);
  if (!tracking_enabled)
    return false;

  utime_t now = ceph_clock_now(cct);
  utime_t too_old = now;
  too_old -= complaint_time;
  utime_t oldest_op = now;
  uint64_t total_ops_in_flight = 0;

  for (uint32_t i = 0; i < num_optracker_shards; i++) {
    ShardedTrackingData* sdata = sharded_in_flight_list[i];
    assert(NULL != sdata);
    Mutex::Locker locker(sdata->ops_in_flight_lock_sharded);
    if (!sdata->ops_in_flight_sharded.empty()) {
      utime_t oldest_op_tmp = sdata->ops_in_flight_sharded.front()->get_initiated();
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

  int slow = 0;     // total slow
  int warned = 0;   // total logged
  for (uint32_t iter = 0; iter < num_optracker_shards; iter++) {
    ShardedTrackingData* sdata = sharded_in_flight_list[iter];
    assert(NULL != sdata);
    Mutex::Locker locker(sdata->ops_in_flight_lock_sharded);
    if (sdata->ops_in_flight_sharded.empty())
      continue;
    xlist<TrackedOp*>::iterator i = sdata->ops_in_flight_sharded.begin();    
    while (!i.end() && (*i)->get_initiated() < too_old) {
      slow++;

      // exponential backoff of warning intervals
      if (warned < log_threshold &&
         ((*i)->get_initiated() + (complaint_time * (*i)->warn_interval_multiplier)) < now) {
        // will warn, increase counter
        warned++;

        utime_t age = now - (*i)->get_initiated();
        stringstream ss;
        ss << "slow request " << age << " seconds old, received at "
           << (*i)->get_initiated() << ": ";
        (*i)->_dump_op_descriptor_unlocked(ss);
        ss << " currently "
	   << ((*i)->current.size() ? (*i)->current : (*i)->state_string());
        warning_vector.push_back(ss.str());

        // only those that have been shown will backoff
        (*i)->warn_interval_multiplier *= 2;
      }
      ++i;
    }
  }

  // only summarize if we warn about any.  if everything has backed
  // off, we will stay silent.
  if (warned > 0) {
    stringstream ss;
    ss << slow << " slow requests, " << warned << " included below; oldest blocked for > "
       << oldest_secs << " secs";
    warning_vector[0] = ss.str();
  }

  return warned > 0;
}

void OpTracker::get_age_ms_histogram(pow2_hist_t *h)
{
  h->clear();
  utime_t now = ceph_clock_now(NULL);

  for (uint32_t iter = 0; iter < num_optracker_shards; iter++) {
    ShardedTrackingData* sdata = sharded_in_flight_list[iter];
    assert(NULL != sdata);
    Mutex::Locker locker(sdata->ops_in_flight_lock_sharded);

    for (xlist<TrackedOp*>::iterator i = sdata->ops_in_flight_sharded.begin();
                                                               !i.end(); ++i) {
      utime_t age = now - (*i)->get_initiated();
      uint32_t ms = (long)(age * 1000.0);
      h->add(ms);
    }
  }
}

void OpTracker::mark_event(TrackedOp *op, const string &dest, utime_t time)
{
  if (!op->is_tracked)
    return;
  return _mark_event(op, dest, time);
}

void OpTracker::_mark_event(TrackedOp *op, const string &evt,
			    utime_t time)
{
  dout(5);
  *_dout  <<  "seq: " << op->seq
	  << ", time: " << time << ", event: " << evt
	  << ", op: ";
  op->_dump_op_descriptor_unlocked(*_dout);
  *_dout << dendl;
     
}

void OpTracker::RemoveOnDelete::operator()(TrackedOp *op) {
  if (!op->is_tracked) {
    op->_unregistered();
    delete op;
    return;
  }
  op->mark_event("done");
  tracker->unregister_inflight_op(op);
  // Do not delete op, unregister_inflight_op took control
}

void TrackedOp::mark_event(const string &event)
{
  if (!is_tracked)
    return;

  utime_t now = ceph_clock_now(g_ceph_context);
  {
    Mutex::Locker l(lock);
    events.push_back(make_pair(now, event));
  }
  tracker->mark_event(this, event);
  _event_marked();
}

void TrackedOp::dump(utime_t now, Formatter *f) const
{
  // Ignore if still in the constructor
  if (!is_tracked)
    return;
  stringstream name;
  _dump_op_descriptor_unlocked(name);
  f->dump_string("description", name.str().c_str()); // this TrackedOp
  f->dump_stream("initiated_at") << get_initiated();
  f->dump_float("age", now - get_initiated());
  f->dump_float("duration", get_duration());
  {
    f->open_array_section("type_data");
    _dump(now, f);
    f->close_section();
  }
}
