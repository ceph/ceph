// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Tests for Ceph delegation handling
 *
 * (c) 2017, Jeff Layton <jlayton@redhat.com>
 */

#include "gtest/gtest.h"
#include "include/cephfs/libcephfs.h"
#include "include/stat.h"
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <sys/xattr.h>
#include <sys/uio.h>

#ifdef __linux__
#include <limits.h>
#endif

#include <map>
#include <vector>
#include <thread>
#include <atomic>

static void dummy_deleg_cb(Fh *fh, void *priv)
{
  std::atomic_bool *recalled = (std::atomic_bool *)priv;
  recalled->store(true);
}

static void breaker_func(struct ceph_mount_info *cmount, const char *filename, int flags)
{
  bool do_shutdown = false;

  if (!cmount) {
    ASSERT_EQ(ceph_create(&cmount, NULL), 0);
    ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
    ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
    ASSERT_EQ(ceph_mount(cmount, "/"), 0);
    do_shutdown = true;
  }

  Inode *root, *file;
  ASSERT_EQ(ceph_ll_lookup_root(cmount, &root), 0);

  Fh *fh;
  struct ceph_statx stx;
  UserPerm *perms = ceph_mount_perms(cmount);

  ASSERT_EQ(ceph_ll_lookup(cmount, root, filename, &file, &stx, 0, 0, perms), 0);
  ASSERT_EQ(ceph_ll_open(cmount, file, flags, &fh, perms), 0);
  ASSERT_EQ(ceph_ll_close(cmount, fh), 0);

  if (do_shutdown)
    ceph_shutdown(cmount);
}

static void simple_deleg_test(struct ceph_mount_info *cmount, struct ceph_mount_info *tcmount)
{
  Inode *root, *file;

  ASSERT_EQ(ceph_ll_lookup_root(cmount, &root), 0);

  char filename[32];
  sprintf(filename, "delegation%x", getpid());

  Fh *fh;
  struct ceph_statx stx;
  UserPerm *perms = ceph_mount_perms(cmount);

  // ensure r/w open breaks a r/w delegation
  ASSERT_EQ(ceph_ll_create(cmount, root, filename, 0666,
		    O_RDWR|O_CREAT|O_EXCL, &file, &fh, &stx, 0, 0, perms), 0);

  std::atomic_bool recalled(false);
  ASSERT_EQ(ceph_ll_delegation(cmount, fh, CEPH_DELEGATION_WR, dummy_deleg_cb, &recalled), 0);
  std::thread breaker1(breaker_func, tcmount, filename, O_RDWR);
  while (!recalled.load())
    usleep(1000);
  ASSERT_EQ(ceph_ll_delegation(cmount, fh, CEPH_DELEGATION_NONE, dummy_deleg_cb, &recalled), 0);
  breaker1.join();

  // ensure r/o open breaks a r/w delegation
  recalled.store(false);
  ASSERT_EQ(ceph_ll_delegation(cmount, fh, CEPH_DELEGATION_WR, dummy_deleg_cb, &recalled), 0);
  std::thread breaker2(breaker_func, tcmount, filename, O_RDONLY);
  while (!recalled.load())
    usleep(1000);
  ASSERT_EQ(ceph_ll_delegation(cmount, fh, CEPH_DELEGATION_NONE, dummy_deleg_cb, &recalled), 0);
  breaker2.join();

  // close file, reopen r/o
  ASSERT_EQ(ceph_ll_close(cmount, fh), 0);
  ASSERT_EQ(ceph_ll_open(cmount, file, O_RDONLY, &fh, perms), 0);

  // ensure r/o open does not break a r/o delegation
  recalled.store(false);
  ASSERT_EQ(ceph_ll_delegation(cmount, fh, CEPH_DELEGATION_RD, dummy_deleg_cb, &recalled), 0);
  std::thread breaker3(breaker_func, tcmount, filename, O_RDONLY);
  breaker3.join();
  ASSERT_EQ(recalled.load(), false);

  // ensure that r/w open breaks r/o delegation
  std::thread breaker4(breaker_func, tcmount, filename, O_WRONLY);
  while (!recalled.load())
    usleep(1000);
  ASSERT_EQ(ceph_ll_delegation(cmount, fh, CEPH_DELEGATION_NONE, dummy_deleg_cb, &recalled), 0);
  breaker4.join();

  ASSERT_EQ(ceph_ll_close(cmount, fh), 0);
  ASSERT_EQ(ceph_ll_unlink(cmount, root, filename, perms), 0);
}

TEST(LibCephFS, DelegMultiClient) {
  struct ceph_mount_info *cmount;

  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  simple_deleg_test(cmount, nullptr);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, DelegSingleClient) {
  struct ceph_mount_info *cmount;

  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  simple_deleg_test(cmount, cmount);

  ceph_shutdown(cmount);
}
