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
#include <sys/stat.h>
#include <dirent.h>
#include <sys/xattr.h>

TEST(LibCephFS, MulticlientSimple) {
  struct ceph_mount_info *ca, *cb;
  ASSERT_EQ(ceph_create(&ca, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(ca, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(ca, NULL));
  ASSERT_EQ(ceph_mount(ca, NULL), 0);

  ASSERT_EQ(ceph_create(&cb, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cb, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cb, NULL));
  ASSERT_EQ(ceph_mount(cb, NULL), 0);

  char name[20];
  snprintf(name, sizeof(name), "foo.%d", getpid());
  int fda = ceph_open(ca, name, O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fda);
  int fdb = ceph_open(cb, name, O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fdb);

  char bufa[4] = "foo";
  char bufb[4];

  for (int i=0; i<10; i++) {
    strcpy(bufa, "foo");
    ASSERT_EQ((int)sizeof(bufa), ceph_write(ca, fda, bufa, sizeof(bufa), i*6));
    ASSERT_EQ((int)sizeof(bufa), ceph_read(cb, fdb, bufb, sizeof(bufa), i*6));
    ASSERT_EQ(0, memcmp(bufa, bufb, sizeof(bufa)));
    strcpy(bufb, "bar");
    ASSERT_EQ((int)sizeof(bufb), ceph_write(cb, fdb, bufb, sizeof(bufb), i*6+3));
    ASSERT_EQ((int)sizeof(bufb), ceph_read(ca, fda, bufa, sizeof(bufb), i*6+3));
    ASSERT_EQ(0, memcmp(bufa, bufb, sizeof(bufa)));
  }

  ceph_close(ca, fda);
  ceph_close(cb, fdb);

  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

TEST(LibCephFS, MulticlientHoleEOF) {
  struct ceph_mount_info *ca, *cb;
  ASSERT_EQ(ceph_create(&ca, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(ca, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(ca, NULL));
  ASSERT_EQ(ceph_mount(ca, NULL), 0);

  ASSERT_EQ(ceph_create(&cb, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cb, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cb, NULL));
  ASSERT_EQ(ceph_mount(cb, NULL), 0);

  char name[20];
  snprintf(name, sizeof(name), "foo.%d", getpid());
  int fda = ceph_open(ca, name, O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fda);
  int fdb = ceph_open(cb, name, O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fdb);

  ASSERT_EQ(3, ceph_write(ca, fda, "foo", 3, 0));
  ASSERT_EQ(0, ceph_ftruncate(ca, fda, 1000000));

  char buf[4];
  ASSERT_EQ(2, ceph_read(cb, fdb, buf, sizeof(buf), 1000000-2));
  ASSERT_EQ(0, buf[0]);
  ASSERT_EQ(0, buf[1]);

  ceph_close(ca, fda);
  ceph_close(cb, fdb);

  ceph_shutdown(ca);
  ceph_shutdown(cb);
}
