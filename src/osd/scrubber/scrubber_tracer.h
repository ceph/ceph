// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "common/tracer.h"

namespace tracing {
namespace scrubber {

inline tracing::Tracer tracer;

// span attribute keys
inline constexpr const char* PG_ID = "pg.id";
inline constexpr const char* PG_ROLE = "pg.role";

// values for the PG_ROLE attribute
inline constexpr const char* ROLE_PRIMARY = "primary";
inline constexpr const char* ROLE_REPLICA = "replica";

} // namespace scrubber
} // namespace tracing
