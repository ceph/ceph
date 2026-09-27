// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include "common/tracer.h"

class OSDMap;
class TrackedOp;

namespace tracing {
namespace osd {

extern tracing::Tracer tracer;

// exports an OpRequest as a trace; returns its trace id or "". With
// in_flight, the op has not completed: the trace shows what it did until now
// and ends with the phase it is still in. `osdmap`, the OSD's current map, may
// be null; without it the trace has no pool name and is not joined with the
// request's other ops.
std::string trace_slow_op(TrackedOp& op, int whoami, const OSDMap* osdmap,
                          bool in_flight = false);

} // namespace osd
} // namespace tracing
