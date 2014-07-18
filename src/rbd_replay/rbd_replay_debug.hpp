// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Adam Crume <adamcrume@gmail.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef _INCLUDED_RBD_REPLAY_DEBUG_H
#define _INCLUDED_RBD_REPLAY_DEBUG_H

#include "common/debug.h"
#include "include/assert.h"

namespace rbd_replay {

static const int ACTION_LEVEL = 11;
static const int DEPGRAPH_LEVEL = 12;
static const int SLEEP_LEVEL = 13;
static const int THREAD_LEVEL = 10;

}

#define dout_subsys ceph_subsys_rbd_replay
#undef dout_prefix
#define dout_prefix *_dout << "rbd_replay: "

#endif
