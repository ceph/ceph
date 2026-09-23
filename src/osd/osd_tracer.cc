// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "osd_tracer.h"

#include "common/TrackedOp.h"
#include "include/stringify.h"
#include "osd/OpRequest.h"

namespace tracing {
namespace osd {

tracing::Tracer tracer;

std::string trace_slow_op(TrackedOp& tracked, int whoami)
{
  // the OSD's op tracker only tracks OpRequests
  auto& op = static_cast<OpRequest&>(tracked);
  const Message* m = op.get_req();

  OpTimeline t;
  t.name = m->get_type_name();
  t.start = op.get_initiated();
  t.events = op.get_events();
  if (t.events.empty()) {
    return {};
  }
  t.end = t.events.back().first;
  t.attributes = {
    {"description", op.get_desc()},
    {"osd", stringify(whoami)},
    {"source", stringify(m->get_source())},
  };
  if (op.get_reqid() != osd_reqid_t()) {
    t.attributes.emplace_back("reqid", stringify(op.get_reqid()));
  }
  // attach to the client's trace when it sent one
  return tracer.record_op(t, m->otel_trace);
}

} // namespace osd
} // namespace tracing
