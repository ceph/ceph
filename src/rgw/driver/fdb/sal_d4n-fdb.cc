// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "rgw_perf_counters.h"

#include "sal_d4n-fdb.h"

#define dout_subsys ceph_subsys_rgw
#include "common/dout.h"
#include "common/dout_fmt.h"

#include <boost/redis/config.hpp>

#include <memory>

namespace rgw { namespace sal {

} } // namespace rgw::sal

extern "C" {

rgw::sal::Driver* newFDB_D4NFilter(CephContext *cct, rgw::sal::Driver* next, boost::asio::io_context& /* io_context */, bool /* admin */) 
{
  ldout_fmt(cct, 0, "JFW: newFDB_D4NFilter()");
  return new rgw::sal::D4NFilterDriver_FDB(cct, next);
}

}
