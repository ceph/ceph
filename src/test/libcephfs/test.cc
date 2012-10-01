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

#include "gtest/gtest.h"
#include "include/cephfs/libcephfs.h"
#include <errno.h>
#include <sys/fcntl.h>
#include <unistd.h>
#include <sys/types.h>

TEST(LibCephFS, Open_empty_component) {

  pid_t mypid = getpid();
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  char c_dir[1024];
  sprintf(c_dir, "/open_test_%d", mypid);
  struct ceph_dir_result *dirp;

  ASSERT_EQ(0, ceph_mkdirs(cmount, c_dir, 0777));

  ASSERT_EQ(0, ceph_opendir(cmount, c_dir, &dirp));

  char c_path[1024];
  sprintf(c_path, "/open_test_%d//created_file_%d", mypid, mypid);
  int fd = ceph_open(cmount, c_path, O_RDONLY|O_CREAT, 0666);
  ASSERT_LT(0, fd);

  ASSERT_EQ(0, ceph_close(cmount, fd));
  ASSERT_EQ(0, ceph_closedir(cmount, dirp));
  ceph_shutdown(cmount);

  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));

  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  fd = ceph_open(cmount, c_path, O_RDONLY, 0666);
  ASSERT_LT(0, fd);

  ASSERT_EQ(0, ceph_close(cmount, fd));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, Mount_non_exist) {

  struct ceph_mount_info *cmount;

  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_NE(0, ceph_mount(cmount, "/non-exist"));
}
