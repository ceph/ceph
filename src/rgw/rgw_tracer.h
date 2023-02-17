// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once
#include "common/tracer.h"

#include "rgw_common.h"

namespace tracing {
namespace rgw {

const auto OP = "op";
const auto BUCKET_NAME = "bucket_name";
const auto USER_ID = "user_id";
const auto OBJECT_NAME = "object_name";
const auto RETURN = "return";
const auto UPLOAD_ID = "upload_id";
const auto TYPE = "type";
const auto REQUEST = "request";
const auto MULTIPART = "multipart_upload ";

extern tracing::Tracer tracer;

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
