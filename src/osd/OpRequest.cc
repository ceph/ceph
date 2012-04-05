// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 

#include "OpRequest.h"
#include "common/Formatter.h"
#include <iostream>
#include "common/debug.h"
#include "common/config.h"

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

bool OpTracker::check_ops_in_flight(ostream &out)
{
  Mutex::Locker locker(ops_in_flight_lock);
  if (!ops_in_flight.size())
    return false;

  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t too_old = now;
  too_old -= g_conf->osd_op_complaint_time;
  
  dout(10) << "ops_in_flight.size: " << ops_in_flight.size()
	   << "; oldest is " << now - ops_in_flight.front()->received_time
	   << " seconds old" << dendl;
  xlist<OpRequest*>::iterator i = ops_in_flight.begin();
  while (!i.end() && (*i)->received_time < too_old) {
    // exponential backoff of warning intervals
    if ( ( (*i)->received_time +
	   (g_conf->osd_op_complaint_time *
	    (*i)->warn_interval_multiplier) )< now) {
      out << "old request " << *((*i)->request) << " received at "
	  << (*i)->received_time << " currently " << (*i)->state_string();
      (*i)->warn_interval_multiplier *= 2;
    }
    ++i;
  }
  return !i.end();
}

void OpTracker::mark_event(OpRequest *op, const string &dest)
{
  Mutex::Locker locker(ops_in_flight_lock);
  utime_t now = ceph_clock_now(g_ceph_context);
  dout(1) << "seq: " << op->seq << ", time: " << now << ", event: " << dest
	  << " " << *op->request << dendl;
}

void OpTracker::RemoveOnDelete::operator()(OpRequest *op) {
  tracker->unregister_inflight_op(&(op->xitem));
  delete op;
}

OpRequestRef OpTracker::create_request(Message *ref)
{
  return OpRequestRef(new OpRequest(ref, this),
		      RemoveOnDelete(this));
}

void OpRequest::mark_event(const string &event)
{
  tracker->mark_event(this, event);
}
