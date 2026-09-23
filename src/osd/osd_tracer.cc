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
    // the reply took the read data with it, so count what was asked for
    if (ceph_osd_op_type_data(code) && ceph_osd_op_mode_read(code)) {
      bytes += osd_op.op.extent.length;
    } else {
      bytes += osd_op.indata.length();
    }
  }
  t.attributes.emplace_back("ops", std::move(names));
  t.int_attributes.emplace_back("bytes", bytes);
}

} // anonymous namespace

std::string trace_slow_op(TrackedOp& tracked, int whoami, const OSDMap* osdmap)
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
    add_client_op_attributes(op, static_cast<const MOSDOp*>(m), t);
  } else if (m->get_type() == MSG_OSD_REPOP) {
    t.attributes.emplace_back(
      "object", static_cast<const MOSDRepOp*>(m)->poid.oid.name);
  } else if (m->get_type() == MSG_OSD_EC_WRITE) {
    t.attributes.emplace_back(
      "object", static_cast<const MOSDECSubOpWrite*>(m)->op.soid.oid.name);
  }

  if (const osd_reqid_t reqid = op_reqid(op); reqid != osd_reqid_t()) {
    t.attributes.emplace_back("reqid", stringify(reqid));
    // the fsid keeps clusters that share a tracing backend apart
    if (osdmap) {
      const char* fsid = osdmap->get_fsid().bytes();
      uint64_t lo, hi;
      memcpy(&lo, fsid, sizeof(lo));
      memcpy(&hi, fsid + sizeof(lo), sizeof(hi));
      t.request = request_trace(lo ^ hi, reqid.name.type(), reqid.name.num(),
                                reqid.inc, reqid.tid);
    }
  }
  // attach to the client's trace when it sent one
  return tracer.record_op(t, m->otel_trace);
}

} // namespace osd
} // namespace tracing
