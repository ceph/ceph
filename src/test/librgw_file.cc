// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <stdint.h>

#include "include/rados/librgw.h"
#include "include/rados/rgw_file.h"

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "common/debug.h"
#include "global/global_init.h"

namespace {
  librgw_t rgw = nullptr;
}

TEST(LibRGW, INIT) {
  int ret = librgw_create(&rgw, NULL);
  ASSERT_EQ(ret, 0);
  ASSERT_NE(rgw, nullptr);
}

TEST(LibRGW, SHUTDOWN) {
  librgw_shutdown(rgw);
}

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
