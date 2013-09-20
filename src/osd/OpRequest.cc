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

void OpRequest::init_from_message()
{
  if (request->get_type() == CEPH_MSG_OSD_OP) {
    reqid = static_cast<MOSDOp*>(request)->get_reqid();
  } else if (request->get_type() == MSG_OSD_SUBOP) {
    reqid = static_cast<MOSDSubOp*>(request)->reqid;
  }
}
