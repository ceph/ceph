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

#include "common/ceph_context.h"

#include <stdint.h>

struct md_config_t;

extern CephContext *g_ceph_context;
extern md_config_t *g_conf;

#endif
