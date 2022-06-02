// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <string>
#include "rgw_tracer.h"

namespace tracing {
namespace rgw {

#ifdef HAVE_JAEGER
thread_local tracing::Tracer tracer("rgw");
#else // !HAVE_JAEGER
tracing::Tracer tracer;
#endif

} // namespace rgw
} // namespace tracing
