// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Tests for Ceph delegation handling
 *
 * (c) 2017, Jeff Layton <jlayton@redhat.com>
 */

#include "gtest/gtest.h"
#include "include/compat.h"
#include "include/cephfs/libcephfs.h"
#include "include/fs_types.h"
#include "include/stat.h"
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <sys/uio.h>

#ifdef __linux__
#include <limits.h>
#include <sys/xattr.h>
#endif

#include <map>
#include <vector>
#include <thread>
#include <atomic>

#include "include/ceph_assert.h"

/* in ms -- 1 minute */
#define MAX_WAIT	(60 * 1000)

static void wait_for_atomic_bool(std::atomic_bool &recalled)
{
  int i = 0;

  while (!recalled.load()) {
    ASSERT_LT(i++, MAX_WAIT);
    usleep(1000);
  }
}

static int ceph_ll_delegation_wait(struct ceph_mount_info *cmount, Fh *fh,
				   unsigned cmd, ceph_deleg_cb_t cb, void *priv)
{
  int ret, retry = 0;

  /* Wait 10s at most */
  do {
    ret = ceph_ll_delegation(cmount, fh, cmd, cb, priv);
    usleep(10000);
  } while (ret == -CEPHFS_EAGAIN && retry++ < 1000);

  return ret;
}

static int set_default_deleg_timeout(struct ceph_mount_info *cmount)
{
  uint32_t session_timeout = ceph_get_cap_return_timeout(cmount);
  return ceph_set_deleg_timeout(cmount, session_timeout - 1);
}

static void dummy_deleg_cb(Fh *fh, void *priv)
{
  std::atomic_bool *recalled = (std::atomic_bool *)priv;
  recalled->store(true);
}

static void open_breaker_func(struct ceph_mount_info *cmount, const char *filename, int flags, std::atomic_bool *opened)
{
  bool do_shutdown = false;

  if (!cmount) {
    ASSERT_EQ(ceph_create(&cmount, NULL), 0);
    ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
    ASSERT_EQ(ceph_conf_parse_env(cmount, NULL), 0);
    ASSERT_EQ(ceph_mount(cmount, "/"), 0);
    ASSERT_EQ(set_default_deleg_timeout(cmount), 0);
    do_shutdown = true;
  }

  Inode *root, *file;
  ASSERT_EQ(ceph_ll_lookup_root(cmount, &root), 0);

  Fh *fh;
  struct ceph_statx stx;
  UserPerm *perms = ceph_mount_perms(cmount);

  ASSERT_EQ(ceph_ll_lookup(cmount, root, filename, &file, &stx, CEPH_STATX_ALL_STATS, 0, perms), 0);
  int ret, i = 0;
  for (;;) {
    ASSERT_EQ(ceph_ll_getattr(cmount, file, &stx, CEPH_STATX_ALL_STATS, 0, perms), 0);
    ret = ceph_ll_open(cmount, file, flags, &fh, perms);
    if (ret != -CEPHFS_EAGAIN)
      break;
    ASSERT_LT(i++, MAX_WAIT);
    usleep(1000);
  }
  ASSERT_EQ(ret, 0);
  opened->store(true);
  ASSERT_EQ(ceph_ll_close(cmount, fh), 0);

  if (do_shutdown)
    ceph_shutdown(cmount);
}

enum {
  DelegTestLink,
  DelegTestRename,
  DelegTestUnlink
};

static void namespace_breaker_func(struct ceph_mount_info *cmount, int cmd, const char *oldname, const char *newname)
{
  bool do_shutdown = false;

  if (!cmount) {
    ASSERT_EQ(ceph_create(&cmount, NULL), 0);
    ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
    ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
    ASSERT_EQ(ceph_mount(cmount, "/"), 0);
    ASSERT_EQ(set_default_deleg_timeout(cmount), 0);
    do_shutdown = true;
  }

  Inode *root, *file = nullptr;
  ASSERT_EQ(ceph_ll_lookup_root(cmount, &root), 0);

  struct ceph_statx stx;
  UserPerm *perms = ceph_mount_perms(cmount);

  int ret, i = 0;
  for (;;) {
    switch (cmd) {
    case DelegTestRename:
      ret = ceph_ll_rename(cmount, root, oldname, root, newname, perms);
      break;
    case DelegTestLink:
      if (!file) {
	ASSERT_EQ(ceph_ll_lookup(cmount, root, oldname, &file, &stx, 0, 0, perms), 0);
      }
      ret = ceph_ll_link(cmount, file, root, newname, perms);
      break;
    case DelegTestUnlink:
      ret = ceph_ll_unlink(cmount, root, oldname, perms);
      break;
    default:
      // Bad command
      ceph_abort();
    }
    if (ret != -CEPHFS_EAGAIN)
      break;
    ASSERT_LT(i++, MAX_WAIT);
    usleep(1000);
  }
  ASSERT_EQ(ret, 0);

  if (do_shutdown)
    ceph_shutdown(cmount);
}

static void simple_deleg_test(struct ceph_mount_info *cmount, struct ceph_mount_info *tcmount)
{
  Inode *root, *file;

  ASSERT_EQ(ceph_ll_lookup_root(cmount, &root), 0);

  char filename[32];

  Fh *fh;
  struct ceph_statx stx;
  UserPerm *perms = ceph_mount_perms(cmount);

  std::atomic_bool recalled(false);
  std::atomic_bool opened(false);

  // ensure r/w open breaks a r/w delegation
  sprintf(filename, "deleg.rwrw.%x", getpid());
  ASSERT_EQ(ceph_ll_create(cmount, root, filename, 0666,
		    O_RDWR|O_CREAT|O_EXCL, &file, &fh, &stx, 0, 0, perms), 0);
  ASSERT_EQ(ceph_ll_delegation_wait(cmount, fh, CEPH_DELEGATION_WR, dummy_deleg_cb, &recalled), 0);
  std::thread breaker1(open_breaker_func, tcmount, filename, O_RDWR, &opened);

  wait_for_atomic_bool(recalled);
  ASSERT_EQ(ceph_ll_delegation(cmount, fh, CEPH_DELEGATION_NONE, dummy_deleg_cb, &recalled), 0);
  breaker1.join();
  ASSERT_EQ(opened.load(), true);
  ASSERT_EQ(ceph_ll_close(cmount, fh), 0);
  ASSERT_EQ(ceph_ll_unlink(cmount, root, filename, perms), 0);

  // ensure r/o open breaks a r/w delegation
  recalled.store(false);
  opened.store(false);
  sprintf(filename, "deleg.rorw.%x", getpid());
  ASSERT_EQ(ceph_ll_create(cmount, root, filename, 0666,
		    O_RDWR|O_CREAT|O_EXCL, &file, &fh, &stx, 0, 0, perms), 0);
  ASSERT_EQ(ceph_ll_delegation_wait(cmount, fh, CEPH_DELEGATION_WR, dummy_deleg_cb, &recalled), 0);
  std::thread breaker2(open_breaker_func, tcmount, filename, O_RDONLY, &opened);
  wait_for_atomic_bool(recalled);
  ASSERT_EQ(ceph_ll_delegation(cmount, fh, CEPH_DELEGATION_NONE, dummy_deleg_cb, &recalled), 0);
  breaker2.join();
  ASSERT_EQ(opened.load(), true);
  ASSERT_EQ(ceph_ll_close(cmount, fh), 0);
  ASSERT_EQ(ceph_ll_unlink(cmount, root, filename, perms), 0);

  // ensure r/o open does not break a r/o delegation
  sprintf(filename, "deleg.rwro.%x", getpid());
  ASSERT_EQ(ceph_ll_create(cmount, root, filename, 0666,
		    O_RDONLY|O_CREAT|O_EXCL, &file, &fh, &stx, 0, 0, perms), 0);
  recalled.store(false);
  ASSERT_EQ(ceph_ll_delegation_wait(cmount, fh, CEPH_DELEGATION_RD, dummy_deleg_cb, &recalled), 0);
  std::thread breaker3(open_breaker_func, tcmount, filename, O_RDONLY, &opened);
  breaker3.join();
  ASSERT_EQ(recalled.load(), false);

  // ensure that r/w open breaks r/o delegation
  opened.store(false);
  std::thread breaker4(open_breaker_func, tcmount, filename, O_WRONLY, &opened);
  wait_for_atomic_bool(recalled);
  usleep(1000);
  ASSERT_EQ(ceph_ll_delegation(cmount, fh, CEPH_DELEGATION_NONE, dummy_deleg_cb, &recalled), 0);
  breaker4.join();
  ASSERT_EQ(opened.load(), true);
  ASSERT_EQ(ceph_ll_close(cmount, fh), 0);
  ASSERT_EQ(ceph_ll_unlink(cmount, root, filename, perms), 0);

  // ensure hardlinking breaks a r/w delegation
  recalled.store(false);
  char newname[32];
  sprintf(filename, "deleg.old.%x", getpid());
  sprintf(newname, "deleg.new.%x", getpid());
  ASSERT_EQ(ceph_ll_create(cmount, root, filename, 0666,
		    O_RDWR|O_CREAT|O_EXCL, &file, &fh, &stx, 0, 0, perms), 0);
  ASSERT_EQ(ceph_ll_delegation_wait(cmount, fh, CEPH_DELEGATION_WR, dummy_deleg_cb, &recalled), 0);
  std::thread breaker5(namespace_breaker_func, tcmount, DelegTestLink, filename, newname);
  wait_for_atomic_bool(recalled);
  ASSERT_EQ(ceph_ll_delegation(cmount, fh, CEPH_DELEGATION_NONE, dummy_deleg_cb, &recalled), 0);
  breaker5.join();
  ASSERT_EQ(ceph_ll_close(cmount, fh), 0);
  ASSERT_EQ(ceph_ll_unlink(cmount, root, filename, perms), 0);
  ASSERT_EQ(ceph_ll_unlink(cmount, root, newname, perms), 0);

  // ensure renaming breaks a r/w delegation
  recalled.store(false);
  ASSERT_EQ(ceph_ll_create(cmount, root, filename, 0666,
		    O_RDWR|O_CREAT|O_EXCL, &file, &fh, &stx, 0, 0, perms), 0);
  ASSERT_EQ(ceph_ll_delegation_wait(cmount, fh, CEPH_DELEGATION_WR, dummy_deleg_cb, &recalled), 0);
  std::thread breaker6(namespace_breaker_func, tcmount, DelegTestRename, filename, newname);
  wait_for_atomic_bool(recalled);
  ASSERT_EQ(ceph_ll_delegation(cmount, fh, CEPH_DELEGATION_NONE, dummy_deleg_cb, &recalled), 0);
  breaker6.join();
  ASSERT_EQ(ceph_ll_close(cmount, fh), 0);
  ASSERT_EQ(ceph_ll_unlink(cmount, root, newname, perms), 0);

  // ensure unlinking breaks a r/w delegation
  recalled.store(false);
  ASSERT_EQ(ceph_ll_create(cmount, root, filename, 0666,
		    O_RDWR|O_CREAT|O_EXCL, &file, &fh, &stx, 0, 0, perms), 0);
  ASSERT_EQ(ceph_ll_delegation_wait(cmount, fh, CEPH_DELEGATION_WR, dummy_deleg_cb, &recalled), 0);
  std::thread breaker7(namespace_breaker_func, tcmount, DelegTestUnlink, filename, nullptr);
  wait_for_atomic_bool(recalled);
  ASSERT_EQ(ceph_ll_delegation(cmount, fh, CEPH_DELEGATION_NONE, dummy_deleg_cb, &recalled), 0);
  breaker7.join();
  ASSERT_EQ(ceph_ll_close(cmount, fh), 0);
}

TEST(LibCephFS, DelegMultiClient) {
  struct ceph_mount_info *cmount;

  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);
  ASSERT_EQ(set_default_deleg_timeout(cmount), 0);

  simple_deleg_test(cmount, nullptr);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, DelegSingleClient) {
  struct ceph_mount_info *cmount;

  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);
  ASSERT_EQ(set_default_deleg_timeout(cmount), 0);

  simple_deleg_test(cmount, cmount);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, DelegTimeout) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);
  // tweak timeout to run quickly, since we don't plan to return it anyway
  ASSERT_EQ(ceph_set_deleg_timeout(cmount, 2), 0);

  Inode *root, *file;
  ASSERT_EQ(ceph_ll_lookup_root(cmount, &root), 0);

  char filename[32];
  sprintf(filename, "delegtimeo%x", getpid());

  Fh *fh;
  struct ceph_statx stx;
  UserPerm *perms = ceph_mount_perms(cmount);

  ASSERT_EQ(ceph_ll_create(cmount, root, filename, 0666,
		    O_RDWR|O_CREAT|O_EXCL, &file, &fh, &stx, 0, 0, perms), 0);

  /* Reopen read-only */
  ASSERT_EQ(ceph_ll_close(cmount, fh), 0);
  ASSERT_EQ(ceph_ll_open(cmount, file, O_RDONLY, &fh, perms), 0);

  std::atomic_bool recalled(false);
  ASSERT_EQ(ceph_ll_delegation_wait(cmount, fh, CEPH_DELEGATION_RD, dummy_deleg_cb, &recalled), 0);
  std::atomic_bool opened(false);
  std::thread breaker1(open_breaker_func, nullptr, filename, O_RDWR, &opened);
  breaker1.join();
  ASSERT_EQ(recalled.load(), true);
  /*
   * Wait for the delegation to be timedout.
   *
   * MDSs will allow multiple clients to do r/w at the same time and the filelock
   * will be in LOCK_MIX state. So the open() in a second mounter in the
   * open_breaker_func() thread won't guarantee that after it exiting the delegation
   * in the mounter in main thread has already timedout.
   */
  sleep(3);
  ASSERT_EQ(ceph_ll_getattr(cmount, root, &stx, 0, 0, perms), -CEPHFS_ENOTCONN);
  ceph_release(cmount);
}

TEST(LibCephFS, RecalledGetattr) {
  struct ceph_mount_info *cmount1;
  ASSERT_EQ(ceph_create(&cmount1, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount1, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount1, NULL));
  ASSERT_EQ(ceph_mount(cmount1, "/"), 0);
  ASSERT_EQ(set_default_deleg_timeout(cmount1), 0);

  Inode *root, *file;
  ASSERT_EQ(ceph_ll_lookup_root(cmount1, &root), 0);

  char filename[32];
  sprintf(filename, "recalledgetattr%x", getpid());

  Fh *fh;
  struct ceph_statx stx;
  UserPerm *perms = ceph_mount_perms(cmount1);

  ASSERT_EQ(ceph_ll_create(cmount1, root, filename, 0666,
		    O_RDWR|O_CREAT|O_EXCL, &file, &fh, &stx, 0, 0, perms), 0);
  ASSERT_EQ(ceph_ll_write(cmount1, fh, 0, sizeof(filename), filename),
	    static_cast<int>(sizeof(filename)));
  ASSERT_EQ(ceph_ll_close(cmount1, fh), 0);

  /* New mount for read delegation */
  struct ceph_mount_info *cmount2;
  ASSERT_EQ(ceph_create(&cmount2, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount2, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount2, NULL));
  ASSERT_EQ(ceph_mount(cmount2, "/"), 0);
  ASSERT_EQ(set_default_deleg_timeout(cmount2), 0);

  ASSERT_EQ(ceph_ll_lookup_root(cmount2, &root), 0);
  perms = ceph_mount_perms(cmount2);
  ASSERT_EQ(ceph_ll_lookup(cmount2, root, filename, &file, &stx, 0, 0, perms), 0);

  ASSERT_EQ(ceph_ll_open(cmount2, file, O_WRONLY, &fh, perms), 0);
  ASSERT_EQ(ceph_ll_write(cmount2, fh, 0, sizeof(filename), filename),
	    static_cast<int>(sizeof(filename)));
  ASSERT_EQ(ceph_ll_close(cmount2, fh), 0);

  ASSERT_EQ(ceph_ll_open(cmount2, file, O_RDONLY, &fh, perms), 0);

  /* Break delegation */
  std::atomic_bool recalled(false);
  ASSERT_EQ(ceph_ll_delegation_wait(cmount2, fh, CEPH_DELEGATION_RD, dummy_deleg_cb, &recalled), 0);
  ASSERT_EQ(ceph_ll_read(cmount2, fh, 0, sizeof(filename), filename),
	    static_cast<int>(sizeof(filename)));
  ASSERT_EQ(ceph_ll_getattr(cmount2, file, &stx, CEPH_STATX_ALL_STATS, 0, perms), 0);
  std::atomic_bool opened(false);
  std::thread breaker1(open_breaker_func, cmount1, filename, O_WRONLY, &opened);
  int i = 0;
  do {
    ASSERT_EQ(ceph_ll_getattr(cmount2, file, &stx, CEPH_STATX_ALL_STATS, 0, perms), 0);
    ASSERT_LT(i++, MAX_WAIT);
    usleep(1000);
  } while (!recalled.load());
  ASSERT_EQ(ceph_ll_getattr(cmount2, file, &stx, CEPH_STATX_ALL_STATS, 0, perms), 0);
  ASSERT_EQ(ceph_ll_delegation(cmount2, fh, CEPH_DELEGATION_NONE, dummy_deleg_cb, nullptr), 0);
  breaker1.join();
  ASSERT_EQ(opened.load(), true);
  ASSERT_EQ(ceph_ll_close(cmount2, fh), 0);
  ceph_unmount(cmount2);
  ceph_release(cmount2);
  ceph_unmount(cmount1);
  ceph_release(cmount1);
}
