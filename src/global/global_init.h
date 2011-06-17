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

#ifndef CEPH_COMMON_GLOBAL_INIT_H
#define CEPH_COMMON_GLOBAL_INIT_H

#include <deque>
#include <stdint.h>
#include <string>
#include <vector>

#include "common/code_environment.h"
#include "common/common_init.h"

class CephContext;

void global_init(std::vector < const char* >& args,
	       uint32_t module_type, code_environment_t code_env, int flags);
int global_init_shutdown_stderr(CephContext *cct);
void global_init_daemonize(CephContext *cct, int flags);
void global_init_chdir(const CephContext *cct);

#endif
