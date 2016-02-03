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
#include "include/int_types.h"

#include "gtest/gtest.h"
#include "include/ceph_fs.h"
#include "include/cephfs/libcephfs.h"
#include <errno.h>
#include <sys/fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <signal.h>

#if defined(__FreeBSD__)
#include <sys/extattr.h>
#define XATTR_CREATE    0x1
#define XATTR_REPLACE   0x2
#else
#include <sys/xattr.h>
#endif

TEST(Caps, ReadZero) {

  int mypid = getpid();
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  int i = 0;
  for(; i < 30; ++i) {

    char c_path[1024];
    sprintf(c_path, "/caps_rzfile_%d_%d", mypid, i);
    int fd = ceph_open(cmount, c_path, O_CREAT|O_TRUNC|O_WRONLY, 0644);
    ASSERT_LT(0, fd);

    int expect = CEPH_CAP_FILE_EXCL | CEPH_CAP_FILE_WR | CEPH_CAP_FILE_BUFFER;
    int caps = ceph_debug_get_fd_caps(cmount, fd);

    ASSERT_EQ(expect, caps & expect);
    ASSERT_EQ(0, ceph_close(cmount, fd));

    caps = ceph_debug_get_file_caps(cmount, c_path);
    ASSERT_EQ(expect, caps & expect);

    char cw_path[1024];
    sprintf(cw_path, "/caps_wzfile_%d_%d", mypid, i);
    int wfd = ceph_open(cmount, cw_path, O_CREAT|O_TRUNC|O_WRONLY, 0644);
    ASSERT_LT(0, wfd);

    char wbuf[4096];
    ASSERT_EQ(4096, ceph_write(cmount, wfd, wbuf, 4096, 0));

    ASSERT_EQ(0, ceph_close(cmount, wfd));

    struct stat st;
    ASSERT_EQ(0, ceph_stat(cmount, c_path, &st));

    caps = ceph_debug_get_file_caps(cmount, c_path);
    ASSERT_EQ(expect, caps & expect);
  }

  ASSERT_EQ(0, ceph_conf_set(cmount, "client_debug_inject_tick_delay", "20"));

  for(i = 0; i < 30; ++i) {

    char c_path[1024];
    sprintf(c_path, "/caps_rzfile_%d_%d", mypid, i);

    int fd = ceph_open(cmount, c_path, O_RDONLY, 0);
    ASSERT_LT(0, fd);
    char buf[256];

    int expect = CEPH_CAP_FILE_RD | CEPH_STAT_CAP_SIZE | CEPH_CAP_FILE_CACHE;
    int caps = ceph_debug_get_fd_caps(cmount, fd);
    ASSERT_EQ(expect, caps & expect);
    ASSERT_EQ(0, ceph_read(cmount, fd, buf, 256, 0));

    caps = ceph_debug_get_fd_caps(cmount, fd);
    ASSERT_EQ(expect, caps & expect);
    ASSERT_EQ(0, ceph_close(cmount, fd));

  }
  ceph_shutdown(cmount);
}
