// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once
#include "common/tracer.h"

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

#ifdef HAVE_JAEGER
extern thread_local tracing::Tracer tracer;
#else
extern tracing::Tracer tracer;
#endif

} // namespace rgw
} // namespace tracing

