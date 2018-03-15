// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2018 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#ifndef RGW_DMCLOCK_H
#define RGW_DMCLOCK_H

#include "dmclock/src/dmclock_server.h"

namespace rgw::dmclock {

/// dmclock client classes
enum class client_id {
  admin, //< /admin apis
  auth, //< swift auth, sts
  data, //< PutObj, GetObj
  metadata, //< bucket operations, object metadata

  count
};

using crimson::dmclock::Cost;
using crimson::dmclock::ReqParams;
using crimson::dmclock::PhaseType;
using crimson::dmclock::ClientInfo;

using crimson::dmclock::Time;
using crimson::dmclock::get_time;

class PriorityQueue;

} // namespace rgw::dmclock

#endif // RGW_DMCLOCK_H
