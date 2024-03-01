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
#include "include/compat.h"
#include "include/cephfs/libcephfs.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <thread>
#ifdef __linux__
#include <sys/xattr.h>
#endif

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

static void write_func(bool *stop)
{
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char name[20];
  snprintf(name, sizeof(name), "foo.%d", getpid());
  int fd = ceph_open(cmount, name, O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fd);

  int buf_size = 4096;
  char *buf = (char *)malloc(buf_size);
  if (!buf) {
    *stop = true;
    printf("write_func failed to allocate buffer!");
    return;
  }
  memset(buf, 1, buf_size);

  while (!(*stop)) {
    int i;

    // truncate the file size to 4096 will set the max_size to 4MB.
    ASSERT_EQ(0, ceph_ftruncate(cmount, fd, 4096));

    // write 4MB + extra 64KB data will make client to trigger to
    // call check_cap() to report new size. And if MDS is revoking
    // the Fsxrw caps and we are still holding the Fw caps and will
    // trigger tracker#57244.
    for (i = 0; i < 1040; i++) {
      ASSERT_EQ(ceph_write(cmount, fd, buf, buf_size, 0), buf_size);
    }
  }

  ceph_shutdown(cmount);
}

static void setattr_func(bool *stop)
{
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char name[20];
  snprintf(name, sizeof(name), "foo.%d", getpid());
  int fd = ceph_open(cmount, name, O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fd);

  while (!(*stop)) {
    // setattr will make the MDS to acquire xlock for the filelock and
    // force to revoke caps from clients
    struct ceph_statx stx = {.stx_size = 0};
    ASSERT_EQ(ceph_fsetattrx(cmount, fd, &stx, CEPH_SETATTR_SIZE), 0);
  }

  ceph_shutdown(cmount);
}

TEST(LibCephFS, MulticlientRevokeCaps) {
  std::thread thread1, thread2;
  bool stop = false;
  int wait = 60; // in second

  thread1 = std::thread(write_func, &stop);
  thread2 = std::thread(setattr_func, &stop);

  printf(" Will run test for %d seconds!\n", wait);
  sleep(wait);
  stop = true;

  thread1.join();
  thread2.join();
}
