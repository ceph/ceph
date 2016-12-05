// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) Intel Corporation.
 * All rights reserved.
 *
 * Author: Anjaneya Chagam <anjaneya.chagam@intel.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/OIDTrace.h"
#include "common/dout.h"
#include "common/TracepointProvider.h"
#include <iostream>

#ifdef WITH_LTTNG
#define TRACEPOINT_DEFINE
#define TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#include "tracing/oidtrace.h"
#undef TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#undef TRACEPOINT_DEFINE
#else
#define tracepoint(...)
#endif

TracepointProvider::Traits oid_tracepoint_traits("liboidtrace_tp.so", "oid_tracing");
bool OIDTrace::tpinit = false;

void OIDTrace::inittp(void)
{
  if (unlikely(!tpinit)) {
    TracepointProvider::initialize<oid_tracepoint_traits>(g_ceph_context);
    tpinit = true;
  }
}

void OIDTrace::trace_oid_event(const char *oid, const char *tag, const char *file, const char *func, int line)
{
  if (unlikely(!g_ceph_context)) 
    return;
  inittp();
  tracepoint(oidtrace, oid_event, oid, tag, file, func, line);
}

void OIDTrace::trace_oid_elapsed(const char *oid, double elapsed, const char *tag, const char *file, 
                               const char *func, int line)
{
  if (unlikely(!g_ceph_context)) 
    return;
  inittp();
  tracepoint(oidtrace, oid_elapsed, oid, elapsed, tag, file, func, line);
}
