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

#include "common/HeartbeatMap.h"
#include "common/ceph_context.h"
#include "common/config.h"
#include "global/global_context.h"
#include "gtest/gtest.h"

using namespace ceph;

TEST(HeartbeatMap, Healthy) {
  HeartbeatMap hm(g_ceph_context);
  heartbeat_handle_d *h = hm.add_worker("one", pthread_self());

  hm.reset_timeout(h, ceph::make_timespan(9), ceph::make_timespan(18));
  bool healthy = hm.is_healthy();
  ASSERT_TRUE(healthy);

  hm.remove_worker(h);
}

TEST(HeartbeatMap, Unhealth) {
  HeartbeatMap hm(g_ceph_context);
  heartbeat_handle_d *h = hm.add_worker("one", pthread_self());

  hm.reset_timeout(h, ceph::make_timespan(1), ceph::make_timespan(3));
  sleep(2);
  bool healthy = hm.is_healthy();
  ASSERT_FALSE(healthy);

  hm.remove_worker(h);
}
