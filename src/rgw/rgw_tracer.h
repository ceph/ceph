// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once
#include "common/tracer.h"

#include "rgw_common.h"

class RGWOp;

namespace tracing {
namespace rgw {


const auto BUCKET_NAME = "bucket_name";
const auto USER_ID = "user_id";
const auto OBJECT_NAME = "object_name";
const auto OP_RESULT = "op_result";
const auto UPLOAD_ID = "upload_id";
const auto MULTIPART = "multipart_upload ";
const auto TRANS_ID = "trans_id";
const auto HOST_ID = "host_id";

extern tracing::Tracer tracer;

// starts the tracer, and follows changes to where it exports to
void init(CephContext* cct);
void shutdown(CephContext* cct);

// whether requests that are not traced live should carry a context_span(), so
// that slow requests can be traced after the fact and the OSDs can place their
// slow ops under them
bool trace_slow_requests(CephContext* cct);

// exports a request that carried a context_span() as a span with that span's
// ids, if it took rgw_trace_slow_threshold or longer; call when it is done
void trace_slow_request(const req_state* s, const ::RGWOp* op, ::rgw::sal::Driver* driver);

} // namespace rgw
} // namespace tracing

static inline void extract_span_context(const rgw::sal::Attrs& attr, jspan_context& span_ctx) {
  auto trace_iter = attr.find(RGW_ATTR_TRACE);
  if (trace_iter != attr.end()) {
    try {
      auto trace_bl_iter = trace_iter->second.cbegin();
      tracing::decode(span_ctx, trace_bl_iter);
    } catch (buffer::error& err) {}
  }
}
