// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "osd_tracer.h"

#include <cstring>

#include "common/TrackedOp.h"
#include "include/stringify.h"
#include "messages/MOSDECSubOpWrite.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDRepOp.h"
#include "osd/OSDMap.h"
#include "osd/OpRequest.h"

namespace tracing {
namespace osd {

tracing::Tracer tracer;

namespace {

// the request id of an op, including EC sub-writes, whose OpRequest does not
// record one
osd_reqid_t op_reqid(const OpRequest& op)
{
  if (op.get_reqid() != osd_reqid_t()) {
    return op.get_reqid();
  }
  if (auto m = op.get_req(); m->get_type() == MSG_OSD_EC_WRITE) {
    return static_cast<const MOSDECSubOpWrite*>(m)->op.reqid;
  }
  return {};
}

// which part this OSD played in the request
const char* op_role(const Message* m, int whoami, const OSDMap* osdmap)
{
  switch (m->get_type()) {
  case CEPH_MSG_OSD_OP: {
    // only balanced and localized reads go to a replica; the map can have
    // changed since, but that is the best there is after the fact
    auto op = static_cast<const MOSDOp*>(m);
    if (osdmap &&
        op->has_flag(CEPH_OSD_FLAG_BALANCE_READS | CEPH_OSD_FLAG_LOCALIZE_READS)) {
      int primary = -1;
      osdmap->pg_to_acting_osds(op->get_spg().pgid, nullptr, &primary);
      return primary == whoami ? "primary" : "replica";
    }
    return "primary";
  }
  case MSG_OSD_REPOP:
  case MSG_OSD_EC_WRITE:
  case MSG_OSD_EC_READ:
    return "replica";
  case MSG_OSD_REPOPREPLY:
  case MSG_OSD_EC_WRITE_REPLY:
  case MSG_OSD_EC_READ_REPLY:
    return "primary";
  default:
    return nullptr;
  }
}

// what a client op does, from the flags do_op computed; "" if it never got
// that far
const char* op_type(const OpRequest& op)
{
  if (op.op_info_needs_init()) {
    return "";
  }
  if (op.may_write()) {
    return op.may_read() ? "read-write" : "write";
  }
  return op.may_read() ? "read" : "";
}

// searchable attributes of a client op: the object, what was done to it and
// how many bytes that involved
void add_client_op_attributes(const OpRequest& op, const MOSDOp* m, OpTimeline& t)
{
  // the op may have been dropped before do_op finished decoding it, in which
  // case the object name and ops are still empty
  const hobject_t& hobj = m->get_hobj();
  if (!hobj.oid.name.empty()) {
    t.attributes.emplace_back("object", hobj.oid.name);
  }
  if (!hobj.nspace.empty()) {
    t.attributes.emplace_back("namespace", hobj.nspace);
  }
  if (const char* type = op_type(op); *type) {
    t.attributes.emplace_back("op_type", type);
  }
  if (m->ops.empty()) {
    return;
  }
  std::string names;
  int64_t bytes = 0;
  for (const auto& osd_op : m->ops) {
    const int code = osd_op.op.op;
    if (!names.empty()) {
      names += ',';
    }
    names += ceph_osd_op_name(code);
    // by now the reply took the read data with it, and the transaction the
    // written data, so count the extent of data ops
    if (ceph_osd_op_type_data(code)) {
      bytes += osd_op.op.extent.length;
    } else {
      bytes += osd_op.indata.length();
    }
  }
  t.attributes.emplace_back("ops", std::move(names));
  t.int_attributes.emplace_back("bytes", bytes);
}

// Where an op's span goes. The trace is the client's, when it sent a context
// (RGW does; the primary forwards it to the replicas), or else the request's
// own, derived from its reqid. In the client's trace the primary's op hangs off
// the client's span; in the request's own, off its never-exported root. The
// primary's op span takes the id derived from the reqid, which every OSD can
// compute, so the replicas' sub-ops and the replies hang off it without it
// being sent anywhere.
void place_op(const Message* m, const std::optional<RequestTrace>& request, OpTimeline& t)
{
  trace_id_t ctx_trace;
  span_id_t ctx_span;
  const bool has_ctx = context_ids(m->otel_trace, &ctx_trace, &ctx_span);
  if (has_ctx) {
    t.trace_id = ctx_trace;
  } else if (request) {
    t.trace_id = request->trace_id;
  } else {
    return;  // a new trace of its own
  }
  switch (m->get_type()) {
  case CEPH_MSG_OSD_OP:
    t.parent_span_id = has_ctx ? ctx_span : request->root_span_id;
    if (request) {
      t.span_id = request->primary_span_id;
    }
    break;
  case MSG_OSD_REPOP:
  case MSG_OSD_REPOPREPLY:
  case MSG_OSD_EC_WRITE:
  case MSG_OSD_EC_WRITE_REPLY:
  case MSG_OSD_EC_READ:
  case MSG_OSD_EC_READ_REPLY:
    if (request) {
      t.parent_span_id = request->primary_span_id;
    } else {
      t.parent_span_id = ctx_span;
    }
    break;
  default:
    if (has_ctx) {
      t.parent_span_id = ctx_span;
    } else {
      t.parent_span_id = request->root_span_id;
    }
  }
}

} // anonymous namespace

std::string trace_slow_op(TrackedOp& tracked, int whoami, const OSDMap* osdmap,
                          bool in_flight, const trace_admit_t& admit)
{
  // the OSD's op tracker only tracks OpRequests
  auto& op = static_cast<OpRequest&>(tracked);
  const Message* m = op.get_req();
  switch (m->get_type()) {
  case MSG_OSD_REPOPREPLY:
  case MSG_OSD_EC_WRITE_REPLY:
  case MSG_OSD_EC_READ_REPLY:
    // the primary's op already has a "replica osd.N" phase for each reply
    return {};
  }
  if (!m->otel_trace.IsValid() &&
      g_conf().get_val<bool>("osd_op_trace_slow_require_context")) {
    return {};
  }

  OpTimeline t;
  t.name = m->get_type_name();
  t.start = op.get_initiated();
  t.events = op.get_events();
  if (t.events.empty()) {
    return {};
  }
  if (in_flight) {
    t.complete = false;
    t.end = ceph_clock_now();
  } else {
    t.end = t.events.back().first;
  }
  t.attributes = {
    {"description", op.get_desc()},
    {"osd", stringify(whoami)},
    {"source", stringify(m->get_source())},
  };
  const auto& header = m->get_header();
  t.int_attributes.emplace_back(
    "msg_bytes",
    int64_t(header.front_len) + header.middle_len + header.data_len);
  if (const char* role = op_role(m, whoami, osdmap)) {
    t.attributes.emplace_back("role", role);
  }

  if (auto pg_op = dynamic_cast<const MOSDFastDispatchOp*>(m)) {
    const spg_t pgid = pg_op->get_spg();
    t.attributes.emplace_back("pg", stringify(pgid));
    t.int_attributes.emplace_back("pool", pgid.pool());
    if (osdmap && osdmap->have_pg_pool(pgid.pool())) {
      t.attributes.emplace_back("pool_name", osdmap->get_pool_name(pgid.pool()));
    }
    t.int_attributes.emplace_back("osdmap_epoch", pg_op->get_map_epoch());
  }
  if (m->get_type() == CEPH_MSG_OSD_OP) {
    // until do_op finished decoding it, an op in flight is still being
    // written to by the thread that runs it
    if (!in_flight) {
      add_client_op_attributes(op, static_cast<const MOSDOp*>(m), t);
    }
  } else if (m->get_type() == MSG_OSD_REPOP) {
    t.attributes.emplace_back(
      "object", static_cast<const MOSDRepOp*>(m)->poid.oid.name);
  } else if (m->get_type() == MSG_OSD_EC_WRITE) {
    t.attributes.emplace_back(
      "object", static_cast<const MOSDECSubOpWrite*>(m)->op.soid.oid.name);
  }

  std::optional<RequestTrace> request;
  if (const osd_reqid_t reqid = op_reqid(op); reqid != osd_reqid_t()) {
    t.attributes.emplace_back("reqid", stringify(reqid));
    // the fsid keeps clusters that share a tracing backend apart
    if (osdmap) {
      const char* fsid = osdmap->get_fsid().bytes();
      uint64_t lo, hi;
      memcpy(&lo, fsid, sizeof(lo));
      memcpy(&hi, fsid + sizeof(lo), sizeof(hi));
      request = request_trace(lo ^ hi, reqid.name.type(), reqid.name.num(),
                              reqid.inc, reqid.tid);
    }
  }
  place_op(m, request, t);
  // an op without a trace to join (no client context, no reqid) is sampled
  // by a hash of what it is
  if (!admit(t.trace_id ? TraceSampler::key(*t.trace_id) :
             std::hash<std::string>{}(op.get_desc()))) {
    return {};
  }
  if (in_flight) {
    // the op's own id stays for the span of the completed op, which the
    // replicas' spans hang off; the snapshot sits next to it
    t.span_id.reset();
  }
  return tracer.record_op(t);
}

} // namespace osd
} // namespace tracing
