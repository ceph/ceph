// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/ceph_context.h"
#include "global/global_context.h"

/*
 * Global variables for use from process context.
 */
CephContext *g_ceph_context = NULL;
ConfigProxy& g_conf() {
#ifdef WITH_SEASTAR
  return ceph::common::local_conf();
#else
  return g_ceph_context->_conf;
#endif
}

const char *g_assert_file = 0;
int g_assert_line = 0;
const char *g_assert_func = 0;
const char *g_assert_condition = 0;
unsigned long long g_assert_thread = 0;
char g_assert_thread_name[4096];
char g_assert_msg[8096];
char g_process_name[NAME_MAX + 1];
