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
  arrived.clear();
  duration.clear();
  shutdown = true;
}

void OpHistory::insert(utime_t now, TrackedOpRef op)
{
  if (shutdown)
    return;
  duration.insert(make_pair(op->get_duration(), op));
  arrived.insert(make_pair(op->get_arrived(), op));
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
	duration.begin()->second->get_arrived(),
	duration.begin()->second));
    duration.erase(duration.begin());
  }
}

void OpHistory::dump_ops(utime_t now, Formatter *f)
{
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

void OpTracker::dump_historic_ops(Formatter *f)
{
  Mutex::Locker locker(ops_in_flight_lock);
  utime_t now = ceph_clock_now(cct);
  history.dump_ops(now, f);
}

void OpTracker::dump_ops_in_flight(Formatter *f)
{
  Mutex::Locker locker(ops_in_flight_lock);
  f->open_object_section("ops_in_flight"); // overall dump
  f->dump_int("num_ops", ops_in_flight.size());
  f->open_array_section("ops"); // list of TrackedOps
  utime_t now = ceph_clock_now(cct);
  for (xlist<TrackedOp*>::iterator p = ops_in_flight.begin(); !p.end(); ++p) {
    f->open_object_section("op");
    (*p)->dump(now, f);
    f->close_section(); // this TrackedOp
  }
  f->close_section(); // list of TrackedOps
  f->close_section(); // overall dump
}

void OpTracker::register_inflight_op(xlist<TrackedOp*>::item *i)
{
  Mutex::Locker locker(ops_in_flight_lock);
  ops_in_flight.push_back(i);
  ops_in_flight.back()->seq = seq++;
}

void OpTracker::unregister_inflight_op(TrackedOp *i)
{
  Mutex::Locker locker(ops_in_flight_lock);
  assert(i->xitem.get_list() == &ops_in_flight);
  utime_t now = ceph_clock_now(cct);
  i->xitem.remove_myself();
  i->request->clear_data();
  history.insert(now, TrackedOpRef(i));
}

bool OpTracker::check_ops_in_flight(std::vector<string> &warning_vector)
{
  Mutex::Locker locker(ops_in_flight_lock);
  if (!ops_in_flight.size())
    return false;

  utime_t now = ceph_clock_now(cct);
  utime_t too_old = now;
  too_old -= complaint_time;

  utime_t oldest_secs = now - ops_in_flight.front()->get_arrived();

  dout(10) << "ops_in_flight.size: " << ops_in_flight.size()
           << "; oldest is " << oldest_secs
           << " seconds old" << dendl;

  if (oldest_secs < complaint_time)
    return false;

  xlist<TrackedOp*>::iterator i = ops_in_flight.begin();
  warning_vector.reserve(log_threshold + 1);

  int slow = 0;     // total slow
  int warned = 0;   // total logged
  while (!i.end() && (*i)->get_arrived() < too_old) {
    slow++;

    // exponential backoff of warning intervals
    if (((*i)->get_arrived() +
	 (complaint_time * (*i)->warn_interval_multiplier)) < now) {
      // will warn
      if (warning_vector.empty())
	warning_vector.push_back("");
      warned++;
      if (warned > log_threshold)
        break;

      utime_t age = now - (*i)->get_arrived();
      stringstream ss;
      ss << "slow request " << age << " seconds old, received at " << (*i)->get_arrived()
	 << ": " << *((*i)->request) << " currently "
	 << ((*i)->current.size() ? (*i)->current : (*i)->state_string());
      warning_vector.push_back(ss.str());

      // only those that have been shown will backoff
      (*i)->warn_interval_multiplier *= 2;
    }
    ++i;
  }

  // only summarize if we warn about any.  if everything has backed
  // off, we will stay silent.
  if (warned > 0) {
    stringstream ss;
    ss << slow << " slow requests, " << warned << " included below; oldest blocked for > "
       << oldest_secs << " secs";
    warning_vector[0] = ss.str();
  }

  return warning_vector.size();
}

void OpTracker::get_age_ms_histogram(pow2_hist_t *h)
{
  Mutex::Locker locker(ops_in_flight_lock);

  h->clear();

  utime_t now = ceph_clock_now(NULL);
  unsigned bin = 30;
  uint32_t lb = 1 << (bin-1);  // lower bound for this bin
  int count = 0;
  for (xlist<TrackedOp*>::iterator i = ops_in_flight.begin(); !i.end(); ++i) {
    utime_t age = now - (*i)->get_arrived();
    uint32_t ms = (long)(age * 1000.0);
    if (ms >= lb) {
      count++;
      continue;
    }
    if (count)
      h->set(bin, count);
    while (lb > ms) {
      bin--;
      lb >>= 1;
    }
    count = 1;
  }
  if (count)
    h->set(bin, count);
}

void OpTracker::mark_event(TrackedOp *op, const string &dest)
{
  utime_t now = ceph_clock_now(cct);
  return _mark_event(op, dest, now);
}

void OpTracker::_mark_event(TrackedOp *op, const string &evt,
			    utime_t time)
{
  Mutex::Locker locker(ops_in_flight_lock);
  dout(5) << //"reqid: " << op->get_reqid() <<
	     ", seq: " << op->seq
	  << ", time: " << time << ", event: " << evt
	  << ", request: " << *op->request << dendl;
}

void OpTracker::RemoveOnDelete::operator()(TrackedOp *op) {
  op->mark_event("done");
  tracker->unregister_inflight_op(op);
  // Do not delete op, unregister_inflight_op took control
}

void TrackedOp::mark_event(const string &event)
{
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
  Message *m = request;
  stringstream name;
  m->print(name);
  f->dump_string("description", name.str().c_str()); // this TrackedOp
  f->dump_stream("received_at") << get_arrived();
  f->dump_float("age", now - get_arrived());
  f->dump_float("duration", get_duration());
  {
    f->open_array_section("type_data");
    _dump(now, f);
    f->close_section();
  }
}
