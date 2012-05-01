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

#define dout_subsys ceph_subsys_optracker
#undef dout_prefix
#define dout_prefix _prefix(_dout)

static ostream& _prefix(std::ostream* _dout)
{
  return *_dout << "--OSD::tracker-- ";
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
    stringstream name;
    Message *m = (*p)->request;
    m->print(name);
    jf.open_object_section("op");
    jf.dump_string("description", name.str().c_str()); // this OpRequest
    jf.dump_stream("received_at") << (*p)->received_time;
    jf.dump_float("age", now - (*p)->received_time);
    jf.dump_string("flag_point", (*p)->state_string());
    if (m->get_orig_source().is_client()) {
      jf.open_object_section("client_info");
      stringstream client_name;
      client_name << m->get_orig_source();
      jf.dump_string("client", client_name.str());
      jf.dump_int("tid", m->get_tid());
      jf.close_section(); // client_info
    }
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

void OpTracker::unregister_inflight_op(xlist<OpRequest*>::item *i)
{
  Mutex::Locker locker(ops_in_flight_lock);
  assert(i->get_list() == &ops_in_flight);
  i->remove_myself();
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

  xlist<OpRequest*>::iterator i = ops_in_flight.begin();
  warning_vector.reserve(g_conf->osd_op_log_threshold + 1);
  warning_vector.push_back("");

  int total = 0, skipped = 0;

  while (!i.end() && (*i)->received_time < too_old) {
    // exponential backoff of warning intervals
    if ( ( (*i)->received_time +
           (g_conf->osd_op_complaint_time *
            (*i)->warn_interval_multiplier) )< now) {

      if (total++ >= g_conf->osd_op_log_threshold)
        break;

      stringstream ss;
      ss << "received at " << (*i)->received_time
          << ": " << *((*i)->request) << " currently " << (*i)->state_string();
      warning_vector.push_back(ss.str());
      // only those that have been shown will backoff
      (*i)->warn_interval_multiplier *= 2;
    } else {
      skipped++;
    }
    ++i;
  }

  stringstream ss;
  ss << "there are " << ops_in_flight.size()
      << " ops in flight; the oldest is blocked for >"
      << oldest_secs << " secs";

  if (total) {
    ss << "; these are ";
    if (!skipped)
      ss << "the " << warning_vector.size()-1 << " oldest:";
    else {
      ss << warning_vector.size()-1 << " amongst the oldest ("
          << skipped << " precedes them):";
    }
  }
  warning_vector[0] = ss.str();

  return warning_vector.size();
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
  tracker->unregister_inflight_op(&(op->xitem));
  delete op;
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
  tracker->mark_event(this, event);
}
