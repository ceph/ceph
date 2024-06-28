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

TEST(LibCephFS, RequestForwardAuth)
{
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  char test_path[1024];
  sprintf(test_path, "test_forwardauth_dir_%d", getpid());

  ASSERT_EQ(0, ceph_mkdirs(cmount, test_path, 0777));

  {
    char value[1024] = "";
    int r = -1;

    ASSERT_EQ(0, ceph_setxattr(cmount, test_path, "ceph.dir.pin", (void*)"1", 1, XATTR_CREATE));

    r = ceph_getxattr(cmount, test_path, "ceph.dir.pin", (void*)value, sizeof(value));
    ASSERT_GT(r, 0);
    ASSERT_LT(r, sizeof value);
    ASSERT_STREQ("1", value);
  }

  ASSERT_EQ(ceph_conf_set(cmount, "client_debug_force_send_request_to_rank0", "true"), 0);

  char file_path[1024];
  sprintf(file_path, "/%s/forward_auth_file_%d", test_path, getpid());

  // create the file first and then close it
  int fd = ceph_open(cmount, file_path, O_CREAT, 0644);
  ASSERT_LT(0, fd);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  // open it and it will forward this client request to mds.1
  fd = ceph_open(cmount, file_path, O_CREAT|O_TRUNC|O_WRONLY, 0644);
  ASSERT_LT(0, fd);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  ASSERT_EQ(0, ceph_unlink(cmount, file_path));
  ASSERT_EQ(0, ceph_rmdir(cmount, test_path));

  ceph_shutdown(cmount);
}
