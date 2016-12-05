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

#ifndef _FuncTrace_h_
#define _FuncTrace_h_

#include "common/ceph_context.h"

#define FUNCTRACE() FuncTrace ___t(g_ceph_context, __FILE__, __func__, __LINE__)
#define FUNCTRACE1(ctx) FuncTrace ___t(ctx, __FILE__, __func__, __LINE__)
#define LOG_LEVEL 1

class FuncTrace {
private:
  const char *file;
  const char *func;
  CephContext *ctx;

  static bool tpinit;
  static void inittp(void);

public:
  FuncTrace(CephContext *_ctx, const char *_file, const char *_func, int line);
  ~FuncTrace();
};
#endif
