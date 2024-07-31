// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 

#include "MgrOpRequest.h"
#include <iostream>
#include <vector>
#include "common/debug.h"
#include "common/config.h"
#include "common/Formatter.h"
#include "include/ceph_assert.h"
#include "msg/Message.h"

#ifdef WITH_LTTNG
#define TRACEPOINT_DEFINE
#define TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#include "tracing/mgroprequest.h"
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

MgrOpRequest::MgrOpRequest(MessageRef req, OpTracker* tracker)
    : TrackedOp(tracker, req->get_recv_stamp()),
      request(req) {
  req_src_inst = req->get_source_inst();
}

void MgrOpRequest::_dump(Formatter *f) const
{
  MessageRef m = request;
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

void MgrOpRequest::_dump_op_descriptor(ostream& stream) const
{
  get_req()->print(stream);
}

void MgrOpRequest::_unregistered() {
  request->clear_data();
  request->clear_payload();
  request->release_message_throttle();
  request->set_connection(nullptr);
}

void MgrOpRequest::mark_flag_point(uint8_t flag, const char *s) {
  [[maybe_unused]] uint8_t old_flags = hit_flag_points;
  mark_event(s);
  last_event_detail = s;
  hit_flag_points |= flag;
  latest_flag_point = flag;

  tracepoint(mgroprequest, mark_flag_point,
             flag, s, old_flags, hit_flag_points);
}

void MgrOpRequest::mark_flag_point_string(uint8_t flag, const string& s) {
  [[maybe_unused]] uint8_t old_flags = hit_flag_points;
  mark_event(s);
  hit_flag_points |= flag;
  latest_flag_point = flag;

  tracepoint(mgroprequest, mark_flag_point,
             flag, s.c_str(), old_flags, hit_flag_points);
}

bool MgrOpRequest::filter_out(const set<string>& filters)
{
  set<entity_addr_t> addrs;
  for (const auto& filter : filters) {
    entity_addr_t addr;
    if (addr.parse(filter.c_str())) {
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
