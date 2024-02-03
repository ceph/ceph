// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat, Inc.
 * Copyright (C) 2019 SUSE LLC
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include "dmclock/src/dmclock_server.h"

namespace rgw::dmclock {
// TODO: implement read vs write
enum class client_id {
                      admin, //< /admin apis
                      auth, //< swift auth, sts
                      data, //< PutObj, GetObj
                      metadata, //< bucket operations, object metadata
                      count
};

// TODO move these to dmclock/types or so in submodule
using crimson::dmclock::Cost;
using crimson::dmclock::ClientInfo;

enum class scheduler_t {
                        none,
                        throttler,
                        dmclock
};

inline scheduler_t get_scheduler_t(CephContext* const cct)
{
  const auto scheduler_type = cct->_conf.get_val<std::string>("rgw_scheduler_type");
  if (scheduler_type == "dmclock")
    return scheduler_t::dmclock;
  else if (scheduler_type == "throttler")
    return scheduler_t::throttler;
  else
    return scheduler_t::none;
}

} // namespace rgw::dmclock
