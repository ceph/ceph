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

#ifndef CEPH_GLOBAL_CONTEXT_H
#define CEPH_GLOBAL_CONTEXT_H

#include <limits.h>

#include "common/config_fwd.h"

class CephContext;

extern CephContext *g_ceph_context;
ConfigProxy& g_conf();

extern const char *g_assert_file;
extern int g_assert_line;
extern const char *g_assert_func;
extern const char *g_assert_condition;
extern unsigned long long g_assert_thread;
extern char g_assert_thread_name[4096];
extern char g_assert_msg[8096];
extern char g_process_name[NAME_MAX + 1];

#endif
