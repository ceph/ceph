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

#include "common/FuncTrace.h"
#include "common/dout.h"
#include "common/TracepointProvider.h"
#include <iostream>

#ifdef WITH_LTTNG
#define TRACEPOINT_DEFINE
#define TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#include "tracing/functrace.h"
#undef TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#undef TRACEPOINT_DEFINE
#else
#define tracepoint(...)
#endif

TracepointProvider::Traits func_tracepoint_traits("libfunctrace_tp.so", "function_tracing");
bool FuncTrace::tpinit = false;

void FuncTrace::inittp(void)
{
  if (unlikely(!tpinit)) {
    TracepointProvider::initialize<func_tracepoint_traits>(g_ceph_context);
    tpinit = true;
  }
}

FuncTrace::FuncTrace(CephContext *_ctx, const char *_file, const char *_func, int line) :
               file(_file),
               func(_func),
               ctx(_ctx)
{
  if (unlikely(!ctx)) 
    return;
  inittp();
  lsubdout(ctx, functrace, LOG_LEVEL) << "ENTRY (" <<  func << ") " << file << ":" << line << " " << dendl;
  tracepoint(functrace, func_enter, file, func, line);
}

FuncTrace::~FuncTrace()
{
  if (unlikely(!ctx)) 
    return;
  lsubdout(ctx, functrace, LOG_LEVEL) << "EXIT (" << func << ") " << file << dendl;
  tracepoint(functrace, func_exit, file, func);
}
