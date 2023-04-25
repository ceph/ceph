// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat Ltd
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
#include "include/rados/librados.h"
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#if defined(__linux__)
#include <sys/xattr.h>
#endif

rados_t cluster;

TEST(LibCephFS, LazyIOOneWriterMulipleReaders) {
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

  int fdb = ceph_open(cb, name, O_RDONLY, 0644);
  ASSERT_LE(0, fdb);

  ASSERT_EQ(0, ceph_lazyio(ca, fda, 1));
  ASSERT_EQ(0, ceph_lazyio(cb, fdb, 1));
 
  char out_buf[] = "fooooooooo";

  /* Client a issues a write and propagates/flushes the buffer */
  ASSERT_EQ((int)sizeof(out_buf), ceph_write(ca, fda, out_buf, sizeof(out_buf), 0));
  ASSERT_EQ(0, ceph_lazyio_propagate(ca, fda, 0, 0));

  /* Client a issues a write and propagates/flushes the buffer */
  ASSERT_EQ((int)sizeof(out_buf), ceph_write(ca, fda, out_buf, sizeof(out_buf), 10));
  ASSERT_EQ(0, ceph_lazyio_propagate(ca, fda, 0, 0));

  char in_buf[40];
  /* Calling ceph_lazyio_synchronize here will invalidate client b's cache and hence enable client a to fetch the propagated write of client a in the subsequent read */
  ASSERT_EQ(0, ceph_lazyio_synchronize(cb, fdb, 0, 0));
  ASSERT_EQ(ceph_read(cb, fdb, in_buf, sizeof(in_buf), 0), 2*strlen(out_buf)+1);
  ASSERT_STREQ(in_buf, "fooooooooofooooooooo");

  /* Client a does not need to call ceph_lazyio_synchronize here because it is the latest writer and fda holds the updated inode*/
  ASSERT_EQ(ceph_read(ca, fda, in_buf, sizeof(in_buf), 0), 2*strlen(out_buf)+1);
  ASSERT_STREQ(in_buf, "fooooooooofooooooooo");

  ceph_close(ca, fda);
  ceph_close(cb, fdb);

  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

TEST(LibCephFS, LazyIOMultipleWritersMulipleReaders) {
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
  snprintf(name, sizeof(name), "foo2.%d", getpid());

  int fda = ceph_open(ca, name, O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fda);

  int fdb = ceph_open(cb, name, O_RDWR, 0644);
  ASSERT_LE(0, fdb);

  ASSERT_EQ(0, ceph_lazyio(ca, fda, 1));
  ASSERT_EQ(0, ceph_lazyio(cb, fdb, 1));

  char out_buf[] = "fooooooooo";
  /* Client a issues a write and propagates/flushes the buffer */
  ASSERT_EQ((int)sizeof(out_buf), ceph_write(ca, fda, out_buf, sizeof(out_buf), 0));
  ASSERT_EQ(0, ceph_lazyio_propagate(ca, fda, 0, 0));
  
  /* Client b issues a write and propagates/flushes the buffer*/
  ASSERT_EQ((int)sizeof(out_buf), ceph_write(cb, fdb, out_buf, sizeof(out_buf), 10));
  ASSERT_EQ(0, ceph_lazyio_propagate(cb, fdb, 0, 0));

  char in_buf[40];
  /* Calling ceph_lazyio_synchronize here will invalidate client a's cache and hence enable client a to fetch the propagated writes of client b in the subsequent read */
  ASSERT_EQ(0, ceph_lazyio_synchronize(ca, fda, 0, 0));
  ASSERT_EQ(ceph_read(ca, fda, in_buf, sizeof(in_buf), 0), 2*strlen(out_buf)+1);
  ASSERT_STREQ(in_buf, "fooooooooofooooooooo");
  
  /* Client b does not need to call ceph_lazyio_synchronize here because it is the latest writer and the writes before it have already been propagated*/
  ASSERT_EQ(ceph_read(cb, fdb, in_buf, sizeof(in_buf), 0), 2*strlen(out_buf)+1);
  ASSERT_STREQ(in_buf, "fooooooooofooooooooo");

  /* Client a issues a write */
  char wait_out_buf[] = "foobarbars";
  ASSERT_EQ((int)sizeof(wait_out_buf), ceph_write(ca, fda, wait_out_buf, sizeof(wait_out_buf), 20));
  ASSERT_EQ(0, ceph_lazyio_propagate(ca, fda, 0, 0));

  /* Client a does not need to call ceph_lazyio_synchronize here because it is the latest writer and the writes before it have already been propagated*/
  ASSERT_EQ(ceph_read(ca, fda, in_buf, sizeof(in_buf), 0), (2*(strlen(out_buf)))+strlen(wait_out_buf)+1);
  ASSERT_STREQ(in_buf, "fooooooooofooooooooofoobarbars");

  /* Calling ceph_lazyio_synchronize here will invalidate client b's cache and hence enable client a to fetch the propagated write of client a in the subsequent read */
  ASSERT_EQ(0, ceph_lazyio_synchronize(cb, fdb, 0, 0));
  ASSERT_EQ(ceph_read(cb, fdb, in_buf, sizeof(in_buf), 0), (2*(strlen(out_buf)))+strlen(wait_out_buf)+1);
  ASSERT_STREQ(in_buf, "fooooooooofooooooooofoobarbars");

  ceph_close(ca, fda);
  ceph_close(cb, fdb);

  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

TEST(LibCephFS, LazyIOMultipleWritersOneReader) {
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
  snprintf(name, sizeof(name), "foo3.%d", getpid());

  int fda = ceph_open(ca, name, O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fda);

  int fdb = ceph_open(cb, name, O_RDWR, 0644);
  ASSERT_LE(0, fdb);
 
  ASSERT_EQ(0, ceph_lazyio(ca, fda, 1));
  ASSERT_EQ(0, ceph_lazyio(cb, fdb, 1));

  char out_buf[] = "fooooooooo";
  /* Client a issues a write and propagates/flushes the buffer */
  ASSERT_EQ((int)sizeof(out_buf), ceph_write(ca, fda, out_buf, sizeof(out_buf), 0));
  ASSERT_EQ(0, ceph_lazyio_propagate(ca, fda, 0, 0));
  
  /* Client b issues a write and propagates/flushes the buffer*/
  ASSERT_EQ((int)sizeof(out_buf), ceph_write(cb, fdb, out_buf, sizeof(out_buf), 10));
  ASSERT_EQ(0, ceph_lazyio_propagate(cb, fdb, 0, 0));

  char in_buf[40];
  /* Client a reads the file and verifies that it only reads it's propagated writes and not Client b's*/
  ASSERT_EQ(ceph_read(ca, fda, in_buf, sizeof(in_buf), 0), strlen(out_buf)+1);
  ASSERT_STREQ(in_buf, "fooooooooo");
  
  /* Client a reads the file again, this time with a lazyio_synchronize to check if the cache gets invalidated and data is refetched i.e all the propagated writes are being read*/
  ASSERT_EQ(0, ceph_lazyio_synchronize(ca, fda, 0, 0));
  ASSERT_EQ(ceph_read(ca, fda, in_buf, sizeof(in_buf), 0), 2*strlen(out_buf)+1);
  ASSERT_STREQ(in_buf, "fooooooooofooooooooo");

  ceph_close(ca, fda);
  ceph_close(cb, fdb);

  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

TEST(LibCephFS, LazyIOSynchronizeFlush) {
  /* Test to make sure lazyio_synchronize flushes dirty buffers */
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
  snprintf(name, sizeof(name), "foo4.%d", getpid());

  int fda = ceph_open(ca, name, O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fda);

  int fdb = ceph_open(cb, name, O_RDWR, 0644);
  ASSERT_LE(0, fdb);

  ASSERT_EQ(0, ceph_lazyio(ca, fda, 1));
  ASSERT_EQ(0, ceph_lazyio(cb, fdb, 1));

  char out_buf[] = "fooooooooo";

  /* Client a issues a write and propagates it*/
  ASSERT_EQ((int)sizeof(out_buf), ceph_write(ca, fda, out_buf, sizeof(out_buf), 0));
  ASSERT_EQ(0, ceph_lazyio_propagate(ca, fda, 0, 0));

  /* Client b issues writes and without lazyio_propagate*/
  ASSERT_EQ((int)sizeof(out_buf), ceph_write(cb, fdb, out_buf, sizeof(out_buf), 10));
  ASSERT_EQ((int)sizeof(out_buf), ceph_write(cb, fdb, out_buf, sizeof(out_buf), 20));
  
  char in_buf[40];
  /* Calling ceph_lazyio_synchronize here will first flush the possibly pending buffered write of client b and invalidate client b's cache and hence enable client b to fetch all the propagated writes */
  ASSERT_EQ(0, ceph_lazyio_synchronize(cb, fdb, 0, 0));
  ASSERT_EQ(ceph_read(cb, fdb, in_buf, sizeof(in_buf), 0), 3*strlen(out_buf)+1);
  ASSERT_STREQ(in_buf, "fooooooooofooooooooofooooooooo");

  /* Required to call ceph_lazyio_synchronize here since client b is the latest writer and client a is out of sync with updated file*/ 
  ASSERT_EQ(0, ceph_lazyio_synchronize(ca, fda, 0, 0));
  ASSERT_EQ(ceph_read(ca, fda, in_buf, sizeof(in_buf), 0), 3*strlen(out_buf)+1);
  ASSERT_STREQ(in_buf, "fooooooooofooooooooofooooooooo");

  ceph_close(ca, fda);
  ceph_close(cb, fdb);

  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

TEST(LibCephFS, WithoutandWithLazyIO) {
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
  snprintf(name, sizeof(name), "foo5.%d", getpid());

  int fda = ceph_open(ca, name, O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fda);

  int fdb = ceph_open(cb, name, O_RDWR, 0644);
  ASSERT_LE(0, fdb);

  char out_buf_w[] = "1234567890";
  /* Doing some non lazyio writes and read*/
  ASSERT_EQ((int)sizeof(out_buf_w), ceph_write(ca, fda, out_buf_w, sizeof(out_buf_w), 0));

  ASSERT_EQ((int)sizeof(out_buf_w), ceph_write(cb, fdb, out_buf_w, sizeof(out_buf_w), 10));

  char in_buf_w[30];
  ASSERT_EQ(ceph_read(ca, fda, in_buf_w, sizeof(in_buf_w), 0), 2*strlen(out_buf_w)+1);

  /* Enable lazyio*/
  ASSERT_EQ(0, ceph_lazyio(ca, fda, 1));
  ASSERT_EQ(0, ceph_lazyio(cb, fdb, 1));

  char out_buf[] = "fooooooooo";

  /* Client a issues a write and propagates/flushes the buffer*/
  ASSERT_EQ((int)sizeof(out_buf), ceph_write(ca, fda, out_buf, sizeof(out_buf), 20));
  ASSERT_EQ(0, ceph_lazyio_propagate(ca, fda, 0, 0));
  
  /* Client b issues a write and propagates/flushes the buffer*/
  ASSERT_EQ((int)sizeof(out_buf), ceph_write(cb, fdb, out_buf, sizeof(out_buf), 30));
  ASSERT_EQ(0, ceph_lazyio_propagate(cb, fdb, 0, 0));

  char in_buf[50];
  /* Calling ceph_lazyio_synchronize here will invalidate client a's cache and hence enable client a to fetch the propagated writes of client b in the subsequent read */
  ASSERT_EQ(0, ceph_lazyio_synchronize(ca, fda, 0, 0));
  ASSERT_EQ(ceph_read(ca, fda, in_buf, sizeof(in_buf), 0), (2*(strlen(out_buf)))+(2*(strlen(out_buf_w)))+1);
  ASSERT_STREQ(in_buf, "12345678901234567890fooooooooofooooooooo");

  /* Client b does not need to call ceph_lazyio_synchronize here because it is the latest writer and the writes before it have already been propagated*/
  ASSERT_EQ(ceph_read(cb, fdb, in_buf, sizeof(in_buf), 0), (2*(strlen(out_buf)))+(2*(strlen(out_buf_w)))+1);
  ASSERT_STREQ(in_buf, "12345678901234567890fooooooooofooooooooo");

  ceph_close(ca, fda);
  ceph_close(cb, fdb);

  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

static int update_root_mode()
{
  struct ceph_mount_info *admin;
  int r = ceph_create(&admin, NULL);
  if (r < 0)
    return r;
  ceph_conf_read_file(admin, NULL);
  ceph_conf_parse_env(admin, NULL);
  ceph_conf_set(admin, "client_permissions", "false");
  r = ceph_mount(admin, "/");
  if (r < 0)
    goto out;
  r = ceph_chmod(admin, "/", 0777);
out:
  ceph_shutdown(admin);
  return r;
}

int main(int argc, char **argv)
{
  int r = update_root_mode();
  if (r < 0)
    exit(1);

  ::testing::InitGoogleTest(&argc, argv);

  srand(getpid());

  r = rados_create(&cluster, NULL);
  if (r < 0)
    exit(1);

  r = rados_conf_read_file(cluster, NULL);
  if (r < 0)
    exit(1);

  rados_conf_parse_env(cluster, NULL);
  r = rados_connect(cluster);
  if (r < 0)
    exit(1);

  r = RUN_ALL_TESTS();

  rados_shutdown(cluster);

  return r;
}
