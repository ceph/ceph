// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "include/compat.h"
#include "gtest/gtest.h"
#include "include/cephfs/libcephfs.h"
#include "mds/mdstypes.h"
#include "include/stat.h"
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/resource.h>

#ifdef __linux__
#include <limits.h>
#include <sys/xattr.h>
#endif

TEST(LibCephFS, SnapQuota) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char test_snap_dir_quota_xattr[256];
  char test_snap_subdir_quota_xattr[256];
  char test_snap_subdir_noquota_xattr[256];
  char xattrk[128];
  char xattrv[128];
  char c_temp[PATH_MAX];
  char gxattrv[128];
  int xbuflen = sizeof(gxattrv);
  pid_t mypid = getpid();

  // create dir and set quota
  sprintf(test_snap_dir_quota_xattr, "test_snap_dir_quota_xattr_%d", mypid);
  ASSERT_EQ(0, ceph_mkdir(cmount, test_snap_dir_quota_xattr, 0777));

  sprintf(xattrk, "ceph.quota.max_bytes");
  sprintf(xattrv, "65536");
  ASSERT_EQ(0, ceph_setxattr(cmount, test_snap_dir_quota_xattr, xattrk, (void *)xattrv, 5, XATTR_CREATE));

  // create subdir and set quota
  sprintf(test_snap_subdir_quota_xattr, "test_snap_dir_quota_xattr_%d/subdir_quota", mypid);
  ASSERT_EQ(0, ceph_mkdirs(cmount, test_snap_subdir_quota_xattr, 0777));

  sprintf(xattrk, "ceph.quota.max_bytes");
  sprintf(xattrv, "32768");
  ASSERT_EQ(0, ceph_setxattr(cmount, test_snap_subdir_quota_xattr, xattrk, (void *)xattrv, 5, XATTR_CREATE));

  // create subdir with no quota
  sprintf(test_snap_subdir_noquota_xattr, "test_snap_dir_quota_xattr_%d/subdir_noquota", mypid);
  ASSERT_EQ(0, ceph_mkdirs(cmount, test_snap_subdir_noquota_xattr, 0777));

  // snapshot dir
  sprintf(c_temp, "/.snap/test_snap_dir_quota_xattr_snap_%d", mypid);
  ASSERT_EQ(0, ceph_mkdirs(cmount, c_temp, 0777));

  // check dir quota under snap
  sprintf(c_temp, "/.snap/test_snap_dir_quota_xattr_snap_%d/test_snap_dir_quota_xattr_%d", mypid, mypid);
  int alen = ceph_getxattr(cmount, c_temp, "ceph.quota.max_bytes", (void *)gxattrv, xbuflen);
  ASSERT_LT(0, alen);
  ASSERT_LT(alen, xbuflen);
  gxattrv[alen] = '\0';
  ASSERT_STREQ(gxattrv, "65536");

  // check subdir quota under snap
  sprintf(c_temp, "/.snap/test_snap_dir_quota_xattr_snap_%d/test_snap_dir_quota_xattr_%d/subdir_quota", mypid, mypid);
  alen = ceph_getxattr(cmount, c_temp, "ceph.quota.max_bytes", (void *)gxattrv, xbuflen);
  ASSERT_LT(0, alen);
  ASSERT_LT(alen, xbuflen);
  gxattrv[alen] = '\0';
  ASSERT_STREQ(gxattrv, "32768");

  // ensure subdir noquota xattr under snap
  sprintf(c_temp, "/.snap/test_snap_dir_quota_xattr_snap_%d/test_snap_dir_quota_xattr_%d/subdir_noquota", mypid, mypid);
  EXPECT_EQ(-ENODATA, ceph_getxattr(cmount, c_temp, "ceph.quota.max_bytes", (void *)gxattrv, xbuflen));

  // listxattr() shouldn't return ceph.quota.max_bytes vxattr
  sprintf(c_temp, "/.snap/test_snap_dir_quota_xattr_snap_%d/test_snap_dir_quota_xattr_%d", mypid, mypid);
  char xattrlist[512];
  int len = ceph_listxattr(cmount, c_temp, xattrlist, sizeof(xattrlist));
  ASSERT_GE(sizeof(xattrlist), (size_t)len);
  char *p = xattrlist;
  int found = 0;
  while (len > 0) {
    if (strcmp(p, "ceph.quota.max_bytes") == 0)
      found++;
    len -= strlen(p) + 1;
    p += strlen(p) + 1;
  }
  ASSERT_EQ(found, 0);

  ceph_shutdown(cmount);
}
