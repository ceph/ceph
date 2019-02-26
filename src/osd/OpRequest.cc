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

OpRequest::OpRequest(Message *req, OpTracker *tracker) :
  TrackedOp(tracker, req->get_recv_stamp()),
  rmw_flags(0), request(req),
  hit_flag_points(0), latest_flag_point(0),
  hitset_inserted(false)
{
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
    for (auto& i : events) {
      f->dump_object("event", i);
    }
    f->close_section();
  }
}

void OpRequest::_dump_op_descriptor_unlocked(ostream& stream) const
{
  get_req()->print(stream);
}

void OpRequest::_unregistered() {
  request->clear_data();
  request->clear_payload();
  request->release_message_throttle();
  request->set_connection(nullptr);
}

bool OpRequest::check_rmw(int flag) const {
  ceph_assert(rmw_flags != 0);
  return rmw_flags & flag;
}
bool OpRequest::may_read() const {
  return need_read_cap() || check_rmw(CEPH_OSD_RMW_FLAG_CLASS_READ);
}
bool OpRequest::may_write() const {
  return need_write_cap() || check_rmw(CEPH_OSD_RMW_FLAG_CLASS_WRITE);
}
bool OpRequest::may_cache() const { return check_rmw(CEPH_OSD_RMW_FLAG_CACHE); }
bool OpRequest::rwordered_forced() const {
  return check_rmw(CEPH_OSD_RMW_FLAG_RWORDERED);
}
bool OpRequest::rwordered() const {
  return may_write() || may_cache() || rwordered_forced();
}

bool OpRequest::includes_pg_op() { return check_rmw(CEPH_OSD_RMW_FLAG_PGOP); }
bool OpRequest::need_read_cap() const {
  return check_rmw(CEPH_OSD_RMW_FLAG_READ);
}
bool OpRequest::need_write_cap() const {
  return check_rmw(CEPH_OSD_RMW_FLAG_WRITE);
}
bool OpRequest::need_promote() {
  return check_rmw(CEPH_OSD_RMW_FLAG_FORCE_PROMOTE);
}
bool OpRequest::need_skip_handle_cache() {
  return check_rmw(CEPH_OSD_RMW_FLAG_SKIP_HANDLE_CACHE);
}
bool OpRequest::need_skip_promote() {
  return check_rmw(CEPH_OSD_RMW_FLAG_SKIP_PROMOTE);
}

void OpRequest::set_rmw_flags(int flags) {
#ifdef WITH_LTTNG
  int old_rmw_flags = rmw_flags;
#endif
  rmw_flags |= flags;
  tracepoint(oprequest, set_rmw_flags, reqid.name._type,
	     reqid.name._num, reqid.tid, reqid.inc,
	     flags, old_rmw_flags, rmw_flags);
}

void OpRequest::set_read() { set_rmw_flags(CEPH_OSD_RMW_FLAG_READ); }
void OpRequest::set_write() { set_rmw_flags(CEPH_OSD_RMW_FLAG_WRITE); }
void OpRequest::set_class_read() { set_rmw_flags(CEPH_OSD_RMW_FLAG_CLASS_READ); }
void OpRequest::set_class_write() { set_rmw_flags(CEPH_OSD_RMW_FLAG_CLASS_WRITE); }
void OpRequest::set_pg_op() { set_rmw_flags(CEPH_OSD_RMW_FLAG_PGOP); }
void OpRequest::set_cache() { set_rmw_flags(CEPH_OSD_RMW_FLAG_CACHE); }
void OpRequest::set_promote() { set_rmw_flags(CEPH_OSD_RMW_FLAG_FORCE_PROMOTE); }
void OpRequest::set_skip_handle_cache() { set_rmw_flags(CEPH_OSD_RMW_FLAG_SKIP_HANDLE_CACHE); }
void OpRequest::set_skip_promote() { set_rmw_flags(CEPH_OSD_RMW_FLAG_SKIP_PROMOTE); }
void OpRequest::set_force_rwordered() { set_rmw_flags(CEPH_OSD_RMW_FLAG_RWORDERED); }

void OpRequest::mark_flag_point(uint8_t flag, const char *s) {
#ifdef WITH_LTTNG
  uint8_t old_flags = hit_flag_points;
#endif
  mark_event(s);
  hit_flag_points |= flag;
  latest_flag_point = flag;
  tracepoint(oprequest, mark_flag_point, reqid.name._type,
	     reqid.name._num, reqid.tid, reqid.inc, rmw_flags,
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
	     reqid.name._num, reqid.tid, reqid.inc, rmw_flags,
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

ostream& operator<<(ostream& out, const OpRequest::ClassInfo& i)
{
  out << "class " << i.class_name << " method " << i.method_name
      << " rd " << i.read << " wr " << i.write << " wl " << i.whitelisted;
  return out;
}
