// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 

#include "OpRequest.h"
#include "common/Formatter.h"
#include <iostream>
#include <vector>
#include "common/debug.h"
#include "common/config.h"
#include "msg/Message.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDRepOp.h"
#include "messages/MOSDRepOpReply.h"
#include "include/ceph_assert.h"
#include "osd/osd_types.h"

#ifdef WITH_LTTNG
#define TRACEPOINT_DEFINE
#define TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#include "tracing/oprequest.h"
#undef TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#undef TRACEPOINT_DEFINE
#else
#define tracepoint(...)
#endif

using std::ostream;
using std::set;
using std::string;
using std::stringstream;

using ceph::Formatter;

OpRequest::OpRequest(Message* req, OpTracker* tracker)
    : TrackedOp(tracker, req->get_throttle_stamp()),
      request(req),
      hit_flag_points(0),
      latest_flag_point(0),
      hitset_inserted(false) {
  if (req->get_priority() < tracker->cct->_conf->osd_client_op_priority) {
    // don't warn as quickly for low priority ops
    warn_interval_multiplier = tracker->cct->_conf->osd_recovery_op_warn_multiple;
  }
  if (req->get_type() == CEPH_MSG_OSD_OP) {
    reqid = static_cast<MOSDOp*>(req)->get_reqid();
  } else if (req->get_type() == MSG_OSD_REPOP) {
    reqid = static_cast<MOSDRepOp*>(req)->reqid;
  } else if (req->get_type() == MSG_OSD_REPOPREPLY) {
    reqid = static_cast<MOSDRepOpReply*>(req)->reqid;
  }
  req_src_inst = req->get_source_inst();
}

void OpRequest::_dump(Formatter *f) const
{
  Message *m = request;
  f->dump_string("flag_point", state_string());
  if (m->get_orig_source().is_client()) {
    f->open_object_section("client_info");
    stringstream client_name, client_addr;
    client_name << req_src_inst.name;
    client_addr << req_src_inst.addr;
    f->dump_string("client", client_name.str());
    f->dump_string("client_addr", client_addr.str());
    f->dump_unsigned("tid", m->get_tid());
    f->close_section(); // client_info
  }

  {
    f->open_array_section("events");
    std::lock_guard l(lock);

    for (auto i = events.begin(); i != events.end(); ++i) {
      f->open_object_section("event");
      f->dump_string("event", i->str);
      f->dump_stream("time") << i->stamp;

      double duration = 0;

      if (i != events.begin()) {
        auto i_prev = i - 1;
        duration = i->stamp - i_prev->stamp;
      }

      f->dump_float("duration", duration);
      f->close_section();
    }
    f->close_section();
  }
}

void OpRequest::_dump_op_descriptor(ostream& stream) const
{
  get_req()->print(stream);
}

void OpRequest::_unregistered() {
  request->clear_data();
  request->clear_payload();
  request->release_message_throttle();
  request->set_connection(nullptr);
}

int OpRequest::maybe_init_op_info(const OSDMap &osdmap) {
  if (op_info.get_flags())
    return 0;

  auto m = get_req<MOSDOp>();

#ifdef WITH_LTTNG
  auto old_rmw_flags = op_info.get_flags();
#endif
  auto ret = op_info.set_from_op(m, osdmap);
  tracepoint(oprequest, set_rmw_flags, reqid.name._type,
	     reqid.name._num, reqid.tid, reqid.inc,
	     op_info.get_flags(), old_rmw_flags, op_info.get_flags());
  return ret;
}

void OpRequest::mark_flag_point(uint8_t flag, const char *s) {
#ifdef WITH_LTTNG
  uint8_t old_flags = hit_flag_points;
#endif
  mark_event(s);
  last_event_detail = s;
  hit_flag_points |= flag;
  latest_flag_point = flag;
  tracepoint(oprequest, mark_flag_point, reqid.name._type,
	     reqid.name._num, reqid.tid, reqid.inc, op_info.get_flags(),
	     flag, s, old_flags, hit_flag_points);
}

void OpRequest::mark_flag_point_string(uint8_t flag, const string& s) {
#ifdef WITH_LTTNG
  uint8_t old_flags = hit_flag_points;
#endif
  mark_event(s);
  hit_flag_points |= flag;
  latest_flag_point = flag;
  tracepoint(oprequest, mark_flag_point, reqid.name._type,
	     reqid.name._num, reqid.tid, reqid.inc, op_info.get_flags(),
	     flag, s.c_str(), old_flags, hit_flag_points);
}

bool OpRequest::filter_out(const set<string>& filters)
{
  set<entity_addr_t> addrs;
  for (auto it = filters.begin(); it != filters.end(); it++) {
    entity_addr_t addr;
    if (addr.parse((*it).c_str())) {
      addrs.insert(addr);
    }
  }
  if (addrs.empty())
    return true;

  entity_addr_t cmp_addr = req_src_inst.addr;
  if (addrs.count(cmp_addr)) {
    return true;
  }
  cmp_addr.set_nonce(0);
  if (addrs.count(cmp_addr)) {
    return true;
  }
  cmp_addr.set_port(0);
  if (addrs.count(cmp_addr)) {
    return true;
  }

  return false;
}

