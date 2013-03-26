// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 

#include "OpRequest.h"
#include "common/Formatter.h"
#include <iostream>
#include <vector>
#include "common/debug.h"
#include "common/config.h"
#include "msg/Message.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDSubOp.h"
#include "include/assert.h"

#define dout_subsys ceph_subsys_optracker
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "--OSD::tracker-- ";
}

OpRequest::OpRequest(Message *req, OpTracker *tracker) :
  TrackedOp(req),
  rmw_flags(0),
  tracker(tracker),
  hit_flag_points(0), latest_flag_point(0) {
  received_time = request->get_recv_stamp();
  tracker->register_inflight_op(&xitem);
  if (req->get_priority() < tracker->cct->_conf->osd_client_op_priority) {
    // don't warn as quickly for low priority ops
    warn_interval_multiplier = tracker->cct->_conf->osd_recovery_op_warn_multiple;
  }
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
	  (double)(tracker->cct->_conf->osd_op_history_duration))) {
    duration.erase(make_pair(
	arrived.begin()->second->get_duration(),
	arrived.begin()->second));
    arrived.erase(arrived.begin());
  }

  while (duration.size() > tracker->cct->_conf->osd_op_history_size) {
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
  f->dump_int("num to keep", tracker->cct->_conf->osd_op_history_size);
  f->dump_int("duration to keep", tracker->cct->_conf->osd_op_history_duration);
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
  too_old -= cct->_conf->osd_op_complaint_time;

  utime_t oldest_secs = now - ops_in_flight.front()->received_time;

  dout(10) << "ops_in_flight.size: " << ops_in_flight.size()
           << "; oldest is " << oldest_secs
           << " seconds old" << dendl;

  if (oldest_secs < cct->_conf->osd_op_complaint_time)
    return false;

  xlist<TrackedOp*>::iterator i = ops_in_flight.begin();
  warning_vector.reserve(cct->_conf->osd_op_log_threshold + 1);

  int slow = 0;     // total slow
  int warned = 0;   // total logged
  while (!i.end() && (*i)->received_time < too_old) {
    slow++;

    // exponential backoff of warning intervals
    if (((*i)->received_time +
	 (cct->_conf->osd_op_complaint_time *
	  (*i)->warn_interval_multiplier)) < now) {
      // will warn
      if (warning_vector.empty())
	warning_vector.push_back("");
      warned++;
      if (warned > cct->_conf->osd_op_log_threshold)
        break;

      utime_t age = now - (*i)->received_time;
      stringstream ss;
      ss << "slow request " << age << " seconds old, received at " << (*i)->received_time
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
    utime_t age = now - (*i)->received_time;
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

void OpRequest::dump(utime_t now, Formatter *f) const
{
  Message *m = request;
  stringstream name;
  m->print(name);
  f->dump_string("description", name.str().c_str()); // this OpRequest
  f->dump_unsigned("rmw_flags", rmw_flags);
  f->dump_stream("received_at") << received_time;
  f->dump_float("age", now - received_time);
  f->dump_float("duration", get_duration());
  f->dump_string("flag_point", state_string());
  if (m->get_orig_source().is_client()) {
    f->open_object_section("client_info");
    stringstream client_name;
    client_name << m->get_orig_source();
    f->dump_string("client", client_name.str());
    f->dump_int("tid", m->get_tid());
    f->close_section(); // client_info
  }
  {
    f->open_array_section("events");
    for (list<pair<utime_t, string> >::const_iterator i = events.begin();
	 i != events.end();
	 ++i) {
      f->open_object_section("event");
      f->dump_stream("time") << i->first;
      f->dump_string("event", i->second);
      f->close_section();
    }
    f->close_section();
  }
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

void OpRequest::mark_event(const string &event)
{
  utime_t now = ceph_clock_now(tracker->cct);
  {
    Mutex::Locker l(lock);
    events.push_back(make_pair(now, event));
  }
  tracker->mark_event(this, event);
}

void OpRequest::init_from_message()
{
  if (request->get_type() == CEPH_MSG_OSD_OP) {
    reqid = static_cast<MOSDOp*>(request)->get_reqid();
  } else if (request->get_type() == MSG_OSD_SUBOP) {
    reqid = static_cast<MOSDSubOp*>(request)->reqid;
  }
}
