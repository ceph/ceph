// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 Intel Corporation.
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

#ifndef _OIDTrace_h_
#define _OIDTrace_h_

#include "common/ceph_context.h"

#ifdef WITH_LTTNG
#define OID_EVENT_TRACE(oid, tag) OIDTrace::trace_oid_event(oid, tag, __FILE__, __func__, __LINE__);
#define OID_ELAPSED(oid, elapsed, tag) OIDTrace::trace_oid_elapsed(oid, elapsed, tag, __FILE__, __func__, __LINE__);
#else
#define OID_EVENT_TRACE(oid, tag)
#define OID_ELAPSED(oid, tag, elapsed)
#endif

class OIDTrace {
private:
  static bool tpinit;
  static void inittp(void);

public:
  static void trace_oid_event(const char *oid, const char *tag, const char *file, 
                              const char *func, int line);
  static void trace_oid_elapsed(const char *oid, double elapsed, const char *tag, 
                                const char *file, const char *func, int line);
};
#endif
