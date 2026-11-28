// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Clyso Technologies Inc.
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
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>

static const char *contents_a = "aaaaaaaaaa";
static const char *contents_b = "bbbbbbbbbb";

/* the length of the above, including the terminating NUL */
static const int contents_len = 11;

static void mount_cto(struct ceph_mount_info **cmount, const char *timeout)
{
  ASSERT_EQ(0, ceph_create(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(*cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(*cmount, NULL));
  ASSERT_EQ(0, ceph_conf_set(*cmount, "client_close_to_open", "true"));
  ASSERT_EQ(0, ceph_conf_set(*cmount, "client_close_to_open_timeout",
			     timeout));
  ASSERT_EQ(0, ceph_mount(*cmount, NULL));
}

static void write_and_close(struct ceph_mount_info *cmount, const char *name,
			    int flags, const char *what)
{
  int fd = ceph_open(cmount, name, flags, 0644);
  ASSERT_LE(0, fd);
  ASSERT_EQ(contents_len, ceph_write(cmount, fd, what, contents_len, 0));
  ASSERT_EQ(0, ceph_close(cmount, fd));
}

static void open_read_close(struct ceph_mount_info *cmount, const char *name,
			    const char *expected)
{
  char buf[64];
  int fd = ceph_open(cmount, name, O_RDONLY, 0);
  ASSERT_LE(0, fd);
  ASSERT_EQ(contents_len, ceph_read(cmount, fd, buf, sizeof(buf), 0));
  ASSERT_STREQ(expected, buf);
  ASSERT_EQ(0, ceph_close(cmount, fd));
}

/*
 * Whatever was written before close() has to be on its way to the cluster by
 * the time close() returns, not whenever writeback gets around to it.  Client
 * a keeps a second descriptor open in the same mode, so that the close is not
 * the last of that mode - which flushes anyway - and client b has the file open
 * throughout, so that a's writes are buffered on Fl alone.
 */
TEST(LibCephFS, CloseToOpenFlushOnClose) {
  struct ceph_mount_info *ca, *cb;
  mount_cto(&ca, "60");
  mount_cto(&cb, "60");

  char name[32];
  snprintf(name, sizeof(name), "cto1.%d", getpid());

  write_and_close(ca, name, O_CREAT|O_RDWR, contents_a);

  int fdb = ceph_open(cb, name, O_RDONLY, 0);
  ASSERT_LE(0, fdb);
  int fda2 = ceph_open(ca, name, O_RDWR, 0);
  ASSERT_LE(0, fda2);

  write_and_close(ca, name, O_RDWR, contents_b);

  ASSERT_EQ(0, ceph_close(cb, fdb));
  open_read_close(cb, name, contents_b);

  ASSERT_EQ(0, ceph_close(ca, fda2));
  ASSERT_EQ(0, ceph_unlink(ca, name));
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

/*
 * A reader that cached the file before it was rewritten by somebody else has
 * to pick up the new contents when it opens the file again.
 */
TEST(LibCephFS, CloseToOpenRevalidateOnOpen) {
  struct ceph_mount_info *ca, *cb;
  mount_cto(&ca, "60");
  mount_cto(&cb, "60");

  char name[32];
  snprintf(name, sizeof(name), "cto2.%d", getpid());

  write_and_close(ca, name, O_CREAT|O_RDWR, contents_a);

  /* client b caches the file */
  open_read_close(cb, name, contents_a);

  write_and_close(ca, name, O_RDWR, contents_b);

  /* ... and has to notice that it went away */
  open_read_close(cb, name, contents_b);

  ASSERT_EQ(0, ceph_unlink(ca, name));
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

/*
 * This is what close-to-open gives up: while the file stays open, a write by
 * another client does not invalidate the cache, and reads keep coming out of
 * it.  Reopening the file is what makes the reader catch up.
 */
TEST(LibCephFS, CloseToOpenStaleWhileOpen) {
  struct ceph_mount_info *ca, *cb;
  mount_cto(&ca, "60");
  mount_cto(&cb, "60");

  char name[32];
  snprintf(name, sizeof(name), "cto3.%d", getpid());

  write_and_close(ca, name, O_CREAT|O_RDWR, contents_a);

  char buf[64];
  int fdb = ceph_open(cb, name, O_RDONLY, 0);
  ASSERT_LE(0, fdb);
  ASSERT_EQ(contents_len, ceph_read(cb, fdb, buf, sizeof(buf), 0));
  ASSERT_STREQ(contents_a, buf);

  /* opening the file for writing on client a moves it out of Fc on client b */
  int fda = ceph_open(ca, name, O_RDWR, 0);
  ASSERT_LE(0, fda);
  ASSERT_EQ(contents_len, ceph_write(ca, fda, contents_b, contents_len, 0));
  ASSERT_EQ(0, ceph_fsync(ca, fda, 0));

  /*
   * client b is allowed to keep reading what it cached; this is not racy:
   * while client a has the file open for writing the MDS cannot give Fc back
   * to client b
   */
  ASSERT_EQ(contents_len, ceph_read(cb, fdb, buf, sizeof(buf), 0));
  ASSERT_STREQ(contents_a, buf);

  /* but not across an open */
  ASSERT_EQ(0, ceph_close(cb, fdb));
  open_read_close(cb, name, contents_b);

  ASSERT_EQ(0, ceph_close(ca, fda));
  ASSERT_EQ(0, ceph_unlink(ca, name));
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

/*
 * Applications that share a file usually coordinate with locks rather than
 * with close() and open(), so taking a lock revalidates as well.
 */
TEST(LibCephFS, CloseToOpenRevalidateOnLock) {
  struct ceph_mount_info *ca, *cb;
  mount_cto(&ca, "60");
  mount_cto(&cb, "60");

  char name[32];
  snprintf(name, sizeof(name), "cto4.%d", getpid());

  write_and_close(ca, name, O_CREAT|O_RDWR, contents_a);

  char buf[64];
  int fdb = ceph_open(cb, name, O_RDONLY, 0);
  ASSERT_LE(0, fdb);
  ASSERT_EQ(contents_len, ceph_read(cb, fdb, buf, sizeof(buf), 0));
  ASSERT_STREQ(contents_a, buf);

  int fda = ceph_open(ca, name, O_RDWR, 0);
  ASSERT_LE(0, fda);
  ASSERT_EQ(contents_len, ceph_write(ca, fda, contents_b, contents_len, 0));
  ASSERT_EQ(0, ceph_fsync(ca, fda, 0));

  /* taking the lock drops what client b cached without holding Fc */
  ASSERT_EQ(0, ceph_flock(cb, fdb, LOCK_EX, 42));
  ASSERT_EQ(contents_len, ceph_read(cb, fdb, buf, sizeof(buf), 0));
  ASSERT_STREQ(contents_b, buf);
  ASSERT_EQ(0, ceph_flock(cb, fdb, LOCK_UN, 42));

  ASSERT_EQ(0, ceph_close(cb, fdb));
  ASSERT_EQ(0, ceph_close(ca, fda));
  ASSERT_EQ(0, ceph_unlink(ca, name));
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

/*
 * A descriptor that stays open forever still has to catch up eventually.
 */
TEST(LibCephFS, CloseToOpenStaleCacheExpires) {
  struct ceph_mount_info *ca, *cb;
  mount_cto(&ca, "60");
  mount_cto(&cb, "1");

  char name[32];
  snprintf(name, sizeof(name), "cto5.%d", getpid());

  write_and_close(ca, name, O_CREAT|O_RDWR, contents_a);

  char buf[64];
  int fdb = ceph_open(cb, name, O_RDONLY, 0);
  ASSERT_LE(0, fdb);
  ASSERT_EQ(contents_len, ceph_read(cb, fdb, buf, sizeof(buf), 0));
  ASSERT_STREQ(contents_a, buf);

  int fda = ceph_open(ca, name, O_RDWR, 0);
  ASSERT_LE(0, fda);
  ASSERT_EQ(contents_len, ceph_write(ca, fda, contents_b, contents_len, 0));
  ASSERT_EQ(0, ceph_fsync(ca, fda, 0));

  sleep(3);

  ASSERT_EQ(contents_len, ceph_read(cb, fdb, buf, sizeof(buf), 0));
  ASSERT_STREQ(contents_b, buf);

  ASSERT_EQ(0, ceph_close(cb, fdb));
  ASSERT_EQ(0, ceph_close(ca, fda));
  ASSERT_EQ(0, ceph_unlink(ca, name));
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

/*
 * A cache that went stale while only Fl kept it must not become valid again
 * when the MDS hands Fc back: client b caches the file, client a rewrites it,
 * and a read through a descriptor that does not revalidate (O_APPEND opens
 * keep strict semantics) has to see the new contents.
 */
TEST(LibCephFS, CloseToOpenFcBackDropsStaleCache) {
  struct ceph_mount_info *ca, *cb;
  mount_cto(&ca, "60");
  mount_cto(&cb, "60");

  char name[32];
  snprintf(name, sizeof(name), "cto6.%d", getpid());

  write_and_close(ca, name, O_CREAT|O_RDWR, contents_a);

  char buf[64];
  int fdb = ceph_open(cb, name, O_RDONLY, 0);
  ASSERT_LE(0, fdb);
  ASSERT_EQ(contents_len, ceph_read(cb, fdb, buf, sizeof(buf), 0));
  ASSERT_STREQ(contents_a, buf);

  write_and_close(ca, name, O_RDWR, contents_b);

  int fdb2 = ceph_open(cb, name, O_RDONLY|O_APPEND, 0);
  ASSERT_LE(0, fdb2);
  ASSERT_EQ(contents_len, ceph_read(cb, fdb2, buf, sizeof(buf), 0));
  ASSERT_STREQ(contents_b, buf);

  ASSERT_EQ(0, ceph_close(cb, fdb2));
  ASSERT_EQ(0, ceph_close(cb, fdb));
  ASSERT_EQ(0, ceph_unlink(ca, name));
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}
