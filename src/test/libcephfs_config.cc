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

#include <sstream>
#include <string>
#include <string.h>

using std::string;

TEST(LibCephConfig, SimpleSet) {
  struct ceph_mount_info *cmount;
  int ret = ceph_create(&cmount, NULL);
  ASSERT_EQ(ret, 0);

  ret = ceph_conf_set(cmount, "max_open_files", "21");
  ASSERT_EQ(ret, 0);

  char buf[128];
  memset(buf, 0, sizeof(buf));
  ret = ceph_conf_get(cmount, "max_open_files", buf, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("21"), string(buf));

  ceph_shutdown(cmount);
}

TEST(LibCephConfig, ArgV) {
  struct ceph_mount_info *cmount;
  int ret = ceph_create(&cmount, NULL);
  ASSERT_EQ(ret, 0);

  const char *argv[] = { "foo", "--max-open-files", "2",
			 "--keyfile", "/tmp/my-keyfile", NULL };
  size_t argc = (sizeof(argv) / sizeof(argv[0])) - 1;
  ceph_conf_parse_argv(cmount, argc, argv);

  char buf[128];
  memset(buf, 0, sizeof(buf));
  ret = ceph_conf_get(cmount, "keyfile", buf, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("/tmp/my-keyfile"), string(buf));

  memset(buf, 0, sizeof(buf));
  ret = ceph_conf_get(cmount, "max_open_files", buf, sizeof(buf));
  ASSERT_EQ(ret, 0);
  ASSERT_EQ(string("2"), string(buf));

  ceph_shutdown(cmount);
}
