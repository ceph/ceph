// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "gtest/gtest-spi.h"
#include "gmock/gmock-matchers.h"
#include "gmock/gmock-more-matchers.h"
#include "include/compat.h"
#include "include/cephfs/libcephfs.h"
#include "include/fs_types.h"
#include "mds/mdstypes.h"
#include "include/stat.h"
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

#ifdef __linux__
#include <limits.h>
#include <sys/xattr.h>
#endif

#include <fmt/format.h>
#include <map>
#include <vector>
#include <thread>
#include <regex>
#include <string>

using ::testing::AnyOf;
using ::testing::Gt;
using ::testing::Eq;
using namespace std;

/*
 * Test this with different ceph versions
 */

TEST(LibCephFS, NewOPs)
{
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  const char *test_path = "test_newops_dir";

  ASSERT_EQ(0, ceph_mkdirs(cmount, test_path, 0777));

  {
    char value[1024] = "";
    int r = ceph_getxattr(cmount, test_path, "ceph.dir.pin.random", (void*)value, sizeof(value));
    // Clients will return -CEPHFS_ENODATA if new getvxattr op not support yet.
    EXPECT_THAT(r, AnyOf(Gt(0), Eq(-CEPHFS_ENODATA)));
  }

  {
    double val = (double)1.0/(double)128.0;
    std::stringstream ss;
    ss << val;
    int r = ceph_setxattr(cmount, test_path, "ceph.dir.pin.random", (void*)ss.str().c_str(), strlen(ss.str().c_str()), XATTR_CREATE);
    // Old cephs will return -CEPHFS_EINVAL if not support "ceph.dir.pin.random" yet.
    EXPECT_THAT(r, AnyOf(Eq(0), Eq(-CEPHFS_EINVAL)));

    char value[1024] = "";
    r = ceph_getxattr(cmount, test_path, "ceph.dir.pin.random", (void*)value, sizeof(value));
    // Clients will return -CEPHFS_ENODATA if new getvxattr op not support yet.
    EXPECT_THAT(r, AnyOf(Gt(0), Eq(-CEPHFS_ENODATA)));
  }

  ASSERT_EQ(0, ceph_rmdir(cmount, test_path));

  ceph_shutdown(cmount);
}
