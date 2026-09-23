// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "common/tracer.h"

class TrackedOp;

namespace tracing {
namespace osd {

extern tracing::Tracer tracer;

// exports a completed OpRequest as a trace; returns its trace id or ""
std::string trace_slow_op(TrackedOp& op, int whoami);

} // namespace osd
} // namespace tracing
