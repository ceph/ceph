// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
#include "include/ceph_fs.h"
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
#include <chrono>
#include <string>
#include <thread>
#include <vector>

rados_t cluster;

static struct ceph_mount_info *lazy_mount(bool force_lazyio = false)
{
  struct ceph_mount_info *cmount;
  if (ceph_create(&cmount, NULL) != 0)
    return NULL;
  ceph_conf_read_file(cmount, NULL);
  ceph_conf_parse_env(cmount, NULL);
  if (force_lazyio)
    ceph_conf_set(cmount, "client_force_lazyio", "true");
  if (ceph_mount(cmount, NULL) != 0) {
    ceph_release(cmount);
    return NULL;
  }
  return cmount;
}

static std::string lazy_name(const char *prefix)
{
  return std::string(prefix) + "." + std::to_string(getpid());
}

/* Wait up to 30 seconds for the caps of fd to have (or lack) cap */
static bool wait_for_cap(struct ceph_mount_info *cmount, int fd, int cap,
                         bool has)
{
  for (int i = 0; i < 300; i++) {
    int caps = ceph_debug_get_fd_caps(cmount, fd);
    if (caps >= 0 && !!(caps & cap) == has)
      return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return false;
}

/* The file as a client that has not cached any of it reads it */
static std::string read_fresh(const std::string &name)
{
  struct ceph_mount_info *cmount = lazy_mount();
  if (!cmount)
    return "<mount failed>";
  std::string out;
  int fd = ceph_open(cmount, name.c_str(), O_RDONLY, 0);
  if (fd >= 0) {
    char buf[65536];
    int64_t off = 0;
    int r;
    while ((r = ceph_read(cmount, fd, buf, sizeof(buf), off)) > 0) {
      out.append(buf, r);
      off += r;
    }
    ceph_close(cmount, fd);
  }
  ceph_shutdown(cmount);
  return out;
}

/* Where a and b first differ, for a readable failure message */
static std::string first_diff(const std::string &a, const std::string &b)
{
  size_t n = std::min(a.size(), b.size()), i = 0;
  while (i < n && a[i] == b[i])
    i++;
  return "sizes " + std::to_string(a.size()) + "/" + std::to_string(b.size()) +
         ", first difference at " + std::to_string(i);
}

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

TEST(LibCephFS, LazyIOToggleAndBadFd) {
  struct ceph_mount_info *ca = lazy_mount();
  ASSERT_NE(nullptr, ca);
  std::string name = lazy_name("lazy_toggle");

  int fd = ceph_open(ca, name.c_str(), O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fd);
  /* enabling or disabling twice is no error */
  ASSERT_EQ(0, ceph_lazyio(ca, fd, 1));
  ASSERT_EQ(0, ceph_lazyio(ca, fd, 1));
  ASSERT_EQ(0, ceph_lazyio(ca, fd, 0));
  ASSERT_EQ(0, ceph_lazyio(ca, fd, 0));
  ASSERT_EQ(0, ceph_lazyio(ca, fd, 1));
  /* a range, not only the whole file */
  ASSERT_EQ(0, ceph_lazyio_propagate(ca, fd, 4096, 100));
  ASSERT_EQ(0, ceph_lazyio_synchronize(ca, fd, 4096, 100));
  ceph_close(ca, fd);

  ASSERT_EQ(-EBADF, ceph_lazyio(ca, fd, 1));
  ASSERT_EQ(-EBADF, ceph_lazyio_propagate(ca, fd, 0, 0));
  ASSERT_EQ(-EBADF, ceph_lazyio_synchronize(ca, fd, 0, 0));

  ceph_unlink(ca, name.c_str());
  ceph_shutdown(ca);
}

/*
 * Two clients with a file open for writing put its filelock in MIX, which
 * allows neither CACHE nor BUFFER: a lazy opener gets LAZYIO instead.
 */
TEST(LibCephFS, LazyIOCapWithTwoWriters) {
  struct ceph_mount_info *ca = lazy_mount(), *cb = lazy_mount();
  ASSERT_NE(nullptr, ca);
  ASSERT_NE(nullptr, cb);
  std::string name = lazy_name("lazy_cap");

  int fda = ceph_open(ca, name.c_str(), O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fda);
  int fdb = ceph_open(cb, name.c_str(), O_RDWR, 0644);
  ASSERT_LE(0, fdb);
  ASSERT_EQ(1, ceph_write(ca, fda, "a", 1, 0));
  ASSERT_EQ(1, ceph_write(cb, fdb, "b", 1, 1));

  /* not lazy: no LAZYIO */
  ASSERT_TRUE(wait_for_cap(ca, fda, CEPH_CAP_FILE_BUFFER, false));
  ASSERT_FALSE(ceph_debug_get_fd_caps(ca, fda) & CEPH_CAP_FILE_LAZYIO);
  ASSERT_FALSE(ceph_debug_get_fd_caps(cb, fdb) & CEPH_CAP_FILE_LAZYIO);

  ASSERT_EQ(0, ceph_lazyio(ca, fda, 1));
  ASSERT_EQ(0, ceph_lazyio(cb, fdb, 1));
  ASSERT_TRUE(wait_for_cap(ca, fda, CEPH_CAP_FILE_LAZYIO, true));
  ASSERT_TRUE(wait_for_cap(cb, fdb, CEPH_CAP_FILE_LAZYIO, true));

  ceph_close(ca, fda);
  ceph_close(cb, fdb);
  ceph_unlink(ca, name.c_str());
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

/* client_force_lazyio opens every file for lazy I/O */
TEST(LibCephFS, LazyIOForceLazyio) {
  struct ceph_mount_info *ca = lazy_mount(true), *cb = lazy_mount(true);
  ASSERT_NE(nullptr, ca);
  ASSERT_NE(nullptr, cb);
  std::string name = lazy_name("lazy_force");

  int fda = ceph_open(ca, name.c_str(), O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fda);
  int fdb = ceph_open(cb, name.c_str(), O_RDWR, 0644);
  ASSERT_LE(0, fdb);
  ASSERT_EQ(1, ceph_write(ca, fda, "a", 1, 0));
  ASSERT_EQ(1, ceph_write(cb, fdb, "b", 1, 1));
  ASSERT_TRUE(wait_for_cap(ca, fda, CEPH_CAP_FILE_LAZYIO, true));
  ASSERT_TRUE(wait_for_cap(cb, fdb, CEPH_CAP_FILE_LAZYIO, true));

  ceph_close(ca, fda);
  ceph_close(cb, fdb);
  ceph_unlink(ca, name.c_str());
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

/* ceph_ll_lazyio() on a low-level file handle */
TEST(LibCephFS, LazyIOLowLevel) {
  struct ceph_mount_info *ca = lazy_mount(), *cb = lazy_mount();
  ASSERT_NE(nullptr, ca);
  ASSERT_NE(nullptr, cb);
  std::string name = lazy_name("lazy_ll");
  UserPerm *perms = ceph_mount_perms(ca);

  int fdb = ceph_open(cb, name.c_str(), O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fdb);
  ASSERT_EQ(0, ceph_lazyio(cb, fdb, 1));

  Inode *root, *in;
  struct ceph_statx stx;
  Fh *fh;
  ASSERT_EQ(0, ceph_ll_lookup_root(ca, &root));
  ASSERT_EQ(0, ceph_ll_lookup(ca, root, name.c_str(), &in, &stx, 0, 0, perms));
  ASSERT_EQ(0, ceph_ll_open(ca, in, O_RDWR, &fh, perms));
  ASSERT_EQ(0, ceph_ll_lazyio(ca, fh, 1));

  ASSERT_EQ(3, ceph_ll_write(ca, fh, 0, 3, "aaa"));
  ASSERT_EQ(3, ceph_write(cb, fdb, "bbb", 3, 3));
  ASSERT_TRUE(wait_for_cap(cb, fdb, CEPH_CAP_FILE_LAZYIO, true));
  ASSERT_TRUE(ceph_debug_get_file_caps(ca, name.c_str()) &
              CEPH_CAP_FILE_LAZYIO);

  ASSERT_EQ(0, ceph_ll_fsync(ca, fh, 0));
  ASSERT_EQ(0, ceph_lazyio_propagate(cb, fdb, 0, 0));
  ASSERT_EQ("aaabbb", read_fresh(name));

  ASSERT_EQ(0, ceph_ll_close(ca, fh));
  ceph_ll_put(ca, in);
  ceph_ll_put(ca, root);
  ceph_close(cb, fdb);
  ceph_unlink(ca, name.c_str());
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

/*
 * Two clients write interleaved records that are not page multiples (io500
 * ior-hard's 47008 bytes), so that most pages hold parts of both clients'
 * records, and propagate them.
 */
TEST(LibCephFS, LazyIOInterleavedRecords) {
  struct ceph_mount_info *c[2] = { lazy_mount(), lazy_mount() };
  ASSERT_NE(nullptr, c[0]);
  ASSERT_NE(nullptr, c[1]);
  std::string name = lazy_name("lazy_records");
  const int rec = 47008, nrec = 32;
  int fd[2];

  fd[0] = ceph_open(c[0], name.c_str(), O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fd[0]);
  fd[1] = ceph_open(c[1], name.c_str(), O_RDWR, 0644);
  ASSERT_LE(0, fd[1]);
  for (int i = 0; i < 2; i++)
    ASSERT_EQ(0, ceph_lazyio(c[i], fd[i], 1));

  std::string expect;
  for (int i = 0; i < nrec; i++)
    expect.append(rec, 'A' + i % 26);
  for (int me = 0; me < 2; me++)
    for (int i = me; i < nrec; i += 2)
      ASSERT_EQ(rec, ceph_write(c[me], fd[me], expect.data() + i * rec, rec,
                                (int64_t)i * rec));
  for (int i = 0; i < 2; i++)
    ASSERT_EQ(0, ceph_lazyio_propagate(c[i], fd[i], 0, 0));

  std::string got = read_fresh(name);
  ASSERT_TRUE(got == expect) << first_diff(got, expect);

  for (int i = 0; i < 2; i++) {
    ceph_close(c[i], fd[i]);
    ceph_shutdown(c[i]);
  }
}

/* Two clients write alternate 512-byte pieces of the same pages. */
TEST(LibCephFS, LazyIOSamePageDisjointBytes) {
  struct ceph_mount_info *c[2] = { lazy_mount(), lazy_mount() };
  ASSERT_NE(nullptr, c[0]);
  ASSERT_NE(nullptr, c[1]);
  std::string name = lazy_name("lazy_samepage");
  const int piece = 512, n = 4 * 4096 / piece;
  int fd[2];

  fd[0] = ceph_open(c[0], name.c_str(), O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fd[0]);
  fd[1] = ceph_open(c[1], name.c_str(), O_RDWR, 0644);
  ASSERT_LE(0, fd[1]);
  for (int i = 0; i < 2; i++)
    ASSERT_EQ(0, ceph_lazyio(c[i], fd[i], 1));

  std::string expect;
  for (int i = 0; i < n; i++)
    expect.append(piece, 'A' + i % 2);
  for (int me = 0; me < 2; me++)
    for (int i = me; i < n; i += 2)
      ASSERT_EQ(piece, ceph_write(c[me], fd[me], expect.data() + i * piece,
                                  piece, (int64_t)i * piece));
  for (int i = 0; i < 2; i++)
    ASSERT_EQ(0, ceph_lazyio_propagate(c[i], fd[i], 0, 0));

  std::string got = read_fresh(name);
  ASSERT_TRUE(got == expect) << first_diff(got, expect);

  for (int i = 0; i < 2; i++) {
    ceph_close(c[i], fd[i]);
    ceph_shutdown(c[i]);
  }
}

/*
 * Blocks across object boundaries (4 MiB objects), shifted off any page or
 * object alignment, alternately from two clients.
 */
TEST(LibCephFS, LazyIOAcrossObjects) {
  struct ceph_mount_info *c[2] = { lazy_mount(), lazy_mount() };
  ASSERT_NE(nullptr, c[0]);
  ASSERT_NE(nullptr, c[1]);
  std::string name = lazy_name("lazy_objects");
  const int block = 1 << 20, nblock = 10, shift = 1000;
  int fd[2];

  fd[0] = ceph_open(c[0], name.c_str(), O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fd[0]);
  fd[1] = ceph_open(c[1], name.c_str(), O_RDWR, 0644);
  ASSERT_LE(0, fd[1]);
  for (int i = 0; i < 2; i++)
    ASSERT_EQ(0, ceph_lazyio(c[i], fd[i], 1));

  std::string expect(shift, '\0');
  for (int i = 0; i < nblock; i++)
    expect.append(block, 'a' + i);
  for (int me = 0; me < 2; me++)
    for (int i = me; i < nblock; i += 2)
      ASSERT_EQ(block, ceph_write(c[me], fd[me],
                                  expect.data() + shift + (int64_t)i * block,
                                  block, shift + (int64_t)i * block));
  for (int i = 0; i < 2; i++)
    ASSERT_EQ(0, ceph_lazyio_propagate(c[i], fd[i], 0, 0));

  std::string got = read_fresh(name);
  ASSERT_TRUE(got == expect) << first_diff(got, expect);

  for (int i = 0; i < 2; i++) {
    ceph_close(c[i], fd[i]);
    ceph_shutdown(c[i]);
  }
}

/*
 * a caches a page, b rewrites it, a writes 32 bytes at its end with the
 * rest stale in its cache, and b truncates the file below them: a must not
 * write back the stale bytes.
 */
TEST(LibCephFS, LazyIOTruncateBelowWrite) {
  struct ceph_mount_info *ca = lazy_mount(), *cb = lazy_mount();
  ASSERT_NE(nullptr, ca);
  ASSERT_NE(nullptr, cb);
  std::string name = lazy_name("lazy_trunc");
  const int size = 4 * 4096, tail = 4096 + 3000, cut = 4096 + 1000;
  std::string p(size, 'P'), q(size, 'Q');
  char buf[size];

  int fda = ceph_open(ca, name.c_str(), O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fda);
  ASSERT_EQ(size, ceph_write(ca, fda, p.data(), size, 0));
  ASSERT_EQ(0, ceph_fsync(ca, fda, 0));
  int fdb = ceph_open(cb, name.c_str(), O_RDWR, 0644);
  ASSERT_LE(0, fdb);
  ASSERT_EQ(0, ceph_lazyio(ca, fda, 1));
  ASSERT_EQ(0, ceph_lazyio(cb, fdb, 1));

  ASSERT_EQ(size, ceph_read(ca, fda, buf, size, 0));
  ASSERT_EQ(size, ceph_write(cb, fdb, q.data(), size, 0));
  ASSERT_EQ(0, ceph_lazyio_propagate(cb, fdb, 0, 0));
  ASSERT_EQ(32, ceph_write(ca, fda, std::string(32, 'A').data(), 32, tail));
  ASSERT_EQ(0, ceph_ftruncate(cb, fdb, cut));
  ASSERT_EQ(0, ceph_lazyio_propagate(ca, fda, 0, 0));

  std::string got = read_fresh(name), expect(cut, 'Q');
  ASSERT_TRUE(got == expect) << first_diff(got, expect);

  ceph_close(ca, fda);
  ceph_close(cb, fdb);
  ceph_unlink(ca, name.c_str());
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

/* Closing a file writes back what was not propagated. */
TEST(LibCephFS, LazyIOCloseFlushes) {
  struct ceph_mount_info *ca = lazy_mount(), *cb = lazy_mount();
  ASSERT_NE(nullptr, ca);
  ASSERT_NE(nullptr, cb);
  std::string name = lazy_name("lazy_close");

  int fda = ceph_open(ca, name.c_str(), O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fda);
  int fdb = ceph_open(cb, name.c_str(), O_RDWR, 0644);
  ASSERT_LE(0, fdb);
  ASSERT_EQ(0, ceph_lazyio(ca, fda, 1));
  ASSERT_EQ(0, ceph_lazyio(cb, fdb, 1));

  ASSERT_EQ(5, ceph_write(ca, fda, "hello", 5, 0));
  ASSERT_EQ(0, ceph_close(ca, fda));

  char buf[16] = {0};
  ASSERT_EQ(0, ceph_lazyio_synchronize(cb, fdb, 0, 0));
  ASSERT_EQ(5, ceph_read(cb, fdb, buf, sizeof(buf), 0));
  ASSERT_STREQ("hello", buf);

  ceph_close(cb, fdb);
  ceph_unlink(cb, name.c_str());
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

/*
 * With lazy I/O turned off again, the clients are coherent: a reads what b
 * wrote without synchronizing.  (They may still hold LAZYIO, but no longer
 * use it.)
 */
TEST(LibCephFS, LazyIODisableRestoresCoherency) {
  struct ceph_mount_info *ca = lazy_mount(), *cb = lazy_mount();
  ASSERT_NE(nullptr, ca);
  ASSERT_NE(nullptr, cb);
  std::string name = lazy_name("lazy_disable");
  char buf[16] = {0};

  int fda = ceph_open(ca, name.c_str(), O_CREAT|O_RDWR, 0644);
  ASSERT_LE(0, fda);
  int fdb = ceph_open(cb, name.c_str(), O_RDWR, 0644);
  ASSERT_LE(0, fdb);
  ASSERT_EQ(0, ceph_lazyio(ca, fda, 1));
  ASSERT_EQ(0, ceph_lazyio(cb, fdb, 1));
  ASSERT_EQ(5, ceph_write(ca, fda, "aaaaa", 5, 0));
  ASSERT_EQ(5, ceph_read(ca, fda, buf, 5, 0));
  ASSERT_EQ(0, ceph_lazyio_propagate(ca, fda, 0, 0));

  ASSERT_EQ(0, ceph_lazyio(ca, fda, 0));
  ASSERT_EQ(0, ceph_lazyio(cb, fdb, 0));

  ASSERT_EQ(5, ceph_write(cb, fdb, "bbbbb", 5, 0));
  ASSERT_EQ(5, ceph_read(ca, fda, buf, 5, 0));
  ASSERT_STREQ("bbbbb", buf);

  ceph_close(ca, fda);
  ceph_close(cb, fdb);
  ceph_unlink(ca, name.c_str());
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
