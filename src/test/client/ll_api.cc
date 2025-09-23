// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <errno.h>

#include <iostream>
#include <string>

#include <fmt/format.h>

#include "test/client/TestClient.h"

TEST_F(TestClient, LL_Walk) {
  auto dir = fmt::format("/{}_{}", ::testing::UnitTest::GetInstance()->current_test_info()->name(), getpid());
  ASSERT_EQ(0, client->mkdir(dir.c_str(), 0777, myperm));

  ASSERT_EQ(0, client->chdir(dir.c_str(), myperm));
  std::string cwd;
  ASSERT_EQ(0, client->getcwd(cwd, myperm));
  ASSERT_STREQ(cwd.c_str(), dir.c_str());

  Inode* in = nullptr;
  struct ceph_statx xbuf;
  ASSERT_EQ(0, client->ll_walk(dir.c_str(), &in, &xbuf, 0, 0, myperm));

  ASSERT_EQ(0, client->rmdir(dir.c_str(), myperm));
}
