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

void OpHistory::insert(utime_t now, OpRequest *op)
{
  duration.insert(make_pair(op->get_duration(), op));
  arrived.insert(make_pair(op->get_arrived(), op));
  cleanup(now);
}

void OpHistory::cleanup(utime_t now)
{
  while (arrived.size() &&
	 (now - arrived.begin()->first >
	  (double)(g_conf->osd_op_history_duration))) {
    duration.erase(make_pair(
	arrived.begin()->second->get_duration(),
	arrived.begin()->second));
    arrived.erase(arrived.begin());
  }

  while (duration.size() > g_conf->osd_op_history_size) {
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
  f->dump_int("num to keep", g_conf->osd_op_history_size);
  f->dump_int("duration to keep", g_conf->osd_op_history_duration);
  {
    f->open_array_section("Ops");
    for (set<pair<utime_t, OpRequestRef> >::const_iterator i =
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

void OpTracker::dump_historic_ops(ostream &ss)
{
  JSONFormatter jf(true);
  Mutex::Locker locker(ops_in_flight_lock);
  utime_t now = ceph_clock_now(g_ceph_context);
  history.dump_ops(now, &jf);
  jf.flush(ss);
}

void OpTracker::dump_ops_in_flight(ostream &ss)
{
  JSONFormatter jf(true);
  Mutex::Locker locker(ops_in_flight_lock);
  jf.open_object_section("ops_in_flight"); // overall dump
  jf.dump_int("num_ops", ops_in_flight.size());
  jf.open_array_section("ops"); // list of OpRequests
  utime_t now = ceph_clock_now(g_ceph_context);
  for (xlist<OpRequest*>::iterator p = ops_in_flight.begin(); !p.end(); ++p) {
    jf.open_object_section("op");
    (*p)->dump(now, &jf);
    jf.close_section(); // this OpRequest
  }
  jf.close_section(); // list of OpRequests
  jf.close_section(); // overall dump
  jf.flush(ss);
}

void OpTracker::register_inflight_op(xlist<OpRequest*>::item *i)
{
  Mutex::Locker locker(ops_in_flight_lock);
  ops_in_flight.push_back(i);
  ops_in_flight.back()->seq = seq++;
}

void OpTracker::unregister_inflight_op(OpRequest *i)
{
  Mutex::Locker locker(ops_in_flight_lock);
  assert(i->xitem.get_list() == &ops_in_flight);
  utime_t now = ceph_clock_now(g_ceph_context);
  i->xitem.remove_myself();
  i->request->clear_data();
  history.insert(now, i);
}

bool OpTracker::check_ops_in_flight(std::vector<string> &warning_vector)
{
  Mutex::Locker locker(ops_in_flight_lock);
  if (!ops_in_flight.size())
    return false;

  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t too_old = now;
  too_old -= g_conf->osd_op_complaint_time;

  utime_t oldest_secs = now - ops_in_flight.front()->received_time;

  dout(10) << "ops_in_flight.size: " << ops_in_flight.size()
           << "; oldest is " << oldest_secs
           << " seconds old" << dendl;

  if (oldest_secs < g_conf->osd_op_complaint_time)
    return false;

  xlist<OpRequest*>::iterator i = ops_in_flight.begin();
  warning_vector.reserve(g_conf->osd_op_log_threshold + 1);

  int slow = 0;     // total slow
  int warned = 0;   // total logged
  while (!i.end() && (*i)->received_time < too_old) {
    slow++;

    // exponential backoff of warning intervals
    if (((*i)->received_time +
	 (g_conf->osd_op_complaint_time *
	  (*i)->warn_interval_multiplier)) < now) {
      // will warn
      if (warning_vector.empty())
	warning_vector.push_back("");
      warned++;
      if (warned > g_conf->osd_op_log_threshold)
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

void OpTracker::mark_event(OpRequest *op, const string &dest)
{
  utime_t now = ceph_clock_now(g_ceph_context);
  return _mark_event(op, dest, now);
}

void OpTracker::_mark_event(OpRequest *op, const string &evt,
			    utime_t time)
{
  Mutex::Locker locker(ops_in_flight_lock);
  dout(5) << "reqid: " << op->get_reqid() << ", seq: " << op->seq
	  << ", time: " << time << ", event: " << evt
	  << ", request: " << *op->request << dendl;
}

void OpTracker::RemoveOnDelete::operator()(OpRequest *op) {
  op->mark_event("done");
  tracker->unregister_inflight_op(op);
  // Do not delete op, unregister_inflight_op took control
}

OpRequestRef OpTracker::create_request(Message *ref)
{
  OpRequestRef retval(new OpRequest(ref, this),
		      RemoveOnDelete(this));

  if (ref->get_type() == CEPH_MSG_OSD_OP) {
    retval->reqid = static_cast<MOSDOp*>(ref)->get_reqid();
  } else if (ref->get_type() == MSG_OSD_SUBOP) {
    retval->reqid = static_cast<MOSDSubOp*>(ref)->reqid;
  }
  _mark_event(retval.get(), "header_read", ref->get_recv_stamp());
  _mark_event(retval.get(), "throttled", ref->get_throttle_stamp());
  _mark_event(retval.get(), "all_read", ref->get_recv_complete_stamp());
  _mark_event(retval.get(), "dispatched", ref->get_dispatch_stamp());
  return retval;
}

void OpRequest::mark_event(const string &event)
{
  utime_t now = ceph_clock_now(g_ceph_context);
  {
    Mutex::Locker l(lock);
    events.push_back(make_pair(now, event));
  }
  tracker->mark_event(this, event);
}
