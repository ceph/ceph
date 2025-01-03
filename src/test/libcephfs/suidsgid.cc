// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2023 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "gtest/gtest.h"
#include "common/ceph_argparse.h"
#include "include/buffer.h"
#include "include/fs_types.h"
#include "include/stringify.h"
#include "include/cephfs/libcephfs.h"
#include "include/rados/librados.h"
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <sys/uio.h>
#include <iostream>
#include <vector>
#include "json_spirit/json_spirit.h"

#ifdef __linux__
#include <limits.h>
#include <sys/xattr.h>
#endif

using namespace std;
struct ceph_mount_info *admin;
struct ceph_mount_info *cmount;
char filename[128];

void run_fallocate_test_case(int mode, int result, bool with_admin=false)
{
  struct ceph_statx stx;
  int flags = FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE;

  ASSERT_EQ(0, ceph_chmod(admin, filename, mode));

  struct ceph_mount_info *_cmount = cmount;
  if (with_admin) {
    _cmount = admin;
  }
  int fd = ceph_open(_cmount, filename, O_RDWR, 0);
  ASSERT_LE(0, fd);
  ASSERT_EQ(0, ceph_fallocate(_cmount, fd, flags, 1024, 40960));
  ASSERT_EQ(ceph_statx(_cmount, filename, &stx, CEPH_STATX_MODE, 0), 0);
  std::cout << "After ceph_fallocate, mode: 0" << oct << mode << " -> 0"
            << (stx.stx_mode & 07777) << dec << std::endl;
  ASSERT_EQ(stx.stx_mode & (S_ISUID|S_ISGID), result);
  ceph_close(_cmount, fd);
}

rados_t cluster;

int do_mon_command(string s, string *key)
{
  char *outs, *outbuf;
  size_t outs_len, outbuf_len;
  const char *ss = s.c_str();
  int r = rados_mon_command(cluster, (const char **)&ss, 1,
			    0, 0,
			    &outbuf, &outbuf_len,
			    &outs, &outs_len);
  if (outbuf_len) {
    string s(outbuf, outbuf_len);
    std::cout << "out: " << s << std::endl;

    // parse out the key
    json_spirit::mValue v, k;
    json_spirit::read_or_throw(s, v);
    k = v.get_array()[0].get_obj().find("key")->second;
    *key = k.get_str();
    std::cout << "key: " << *key << std::endl;
    free(outbuf);
  } else {
    return -CEPHFS_EINVAL;
  }
  if (outs_len) {
    string s(outs, outs_len);
    std::cout << "outs: " << s << std::endl;
    free(outs);
  }
  return r;
}

void run_write_test_case(int mode, int result, bool with_admin=false)
{
  struct ceph_statx stx;

  ASSERT_EQ(0, ceph_chmod(admin, filename, mode));

  struct ceph_mount_info *_cmount = cmount;
  if (with_admin) {
    _cmount = admin;
  }
  int fd = ceph_open(_cmount, filename, O_RDWR, 0);
  ASSERT_LE(0, fd);
  ASSERT_EQ(ceph_write(_cmount, fd, "foo", 3, 0), 3);
  ASSERT_EQ(ceph_statx(_cmount, filename, &stx, CEPH_STATX_MODE, 0), 0);
  std::cout << "After ceph_write, mode: 0" << oct << mode << " -> 0"
            << (stx.stx_mode & 07777) << dec << std::endl;
  ASSERT_EQ(stx.stx_mode & (S_ISUID|S_ISGID), result);
  ceph_close(_cmount, fd);
}

void run_truncate_test_case(int mode, int result, size_t size, bool with_admin=false)
{
  struct ceph_statx stx;

  ASSERT_EQ(0, ceph_chmod(admin, filename, mode));

  struct ceph_mount_info *_cmount = cmount;
  if (with_admin) {
    _cmount = admin;
  }
  int fd = ceph_open(_cmount, filename, O_RDWR, 0);
  ASSERT_LE(0, fd);
  ASSERT_GE(ceph_ftruncate(_cmount, fd, size), 0);
  ASSERT_EQ(ceph_statx(_cmount, filename, &stx, CEPH_STATX_MODE, 0), 0);
  std::cout << "After ceph_truncate size " << size << " mode: 0" << oct
            << mode << " -> 0" << (stx.stx_mode & 07777) << dec << std::endl;
  ASSERT_EQ(stx.stx_mode & (S_ISUID|S_ISGID), result);
  ceph_close(_cmount, fd);
}

TEST(SuidsgidTest, WriteClearSetuid) {
  ASSERT_EQ(0, ceph_create(&admin, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(admin, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(admin, NULL));
  ASSERT_EQ(0, ceph_mount(admin, "/"));

  sprintf(filename, "/clear_suidsgid_file_%d", getpid());
  int fd = ceph_open(admin, filename, O_CREAT|O_RDWR, 0766);
  ASSERT_GE(ceph_ftruncate(admin, fd, 10000000), 0);
  ceph_close(admin, fd);

  string user = "clear_suidsgid_" + stringify(rand());
  // create access key
  string key;
  ASSERT_EQ(0, do_mon_command(
      "{\"prefix\": \"auth get-or-create\", \"entity\": \"client." + user + "\", "
      "\"caps\": [\"mon\", \"allow *\", \"osd\", \"allow *\", \"mgr\", \"allow *\", "
      "\"mds\", \"allow *\"], \"format\": \"json\"}", &key));

  ASSERT_EQ(0, ceph_create(&cmount, user.c_str()));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_set(cmount, "key", key.c_str()));
  ASSERT_EQ(ceph_init(cmount), 0);
  UserPerm *perms = ceph_userperm_new(123, 456, 0, NULL);
  ASSERT_NE(nullptr, perms);
  ASSERT_EQ(0, ceph_mount_perms_set(cmount, perms));
  ceph_userperm_destroy(perms);
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  // 1, Commit to a non-exec file by an unprivileged user clears suid and sgid.
  run_fallocate_test_case(06666, 0); // a+rws

  // 2, Commit to a group-exec file by an unprivileged user clears suid and sgid.
  run_fallocate_test_case(06676, 0); // g+x,a+rws

  // 3, Commit to a user-exec file by an unprivileged user clears suid and sgid.
  run_fallocate_test_case(06766, 0); // u+x,a+rws,g-x

  // 4, Commit to a all-exec file by an unprivileged user clears suid and sgid.
  run_fallocate_test_case(06777, 0); // a+rwxs

  // 5, Commit to a non-exec file by root leaves suid and sgid.
  run_fallocate_test_case(06666, S_ISUID|S_ISGID, true); // a+rws

  // 6, Commit to a group-exec file by root leaves suid and sgid.
  run_fallocate_test_case(06676, S_ISUID|S_ISGID, true); // g+x,a+rws

  // 7, Commit to a user-exec file by root leaves suid and sgid.
  run_fallocate_test_case(06766, S_ISUID|S_ISGID, true); // u+x,a+rws,g-x

  // 8, Commit to a all-exec file by root leaves suid and sgid.
  run_fallocate_test_case(06777, S_ISUID|S_ISGID, true); // a+rwxs

  // 9, Commit to a group-exec file by an unprivileged user clears sgid
  run_fallocate_test_case(02676, 0); // a+rw,g+rwxs

  // 10, Commit to a all-exec file by an unprivileged user clears sgid.
  run_fallocate_test_case(02777, 0); // a+rwx,g+rwxs

  // 11, Write by privileged user leaves the suid and sgid
  run_write_test_case(06766, S_ISUID | S_ISGID, true);

  // 12, Write by unprivileged user clears the suid and sgid
  run_write_test_case(06766, 0);

  // 13, Truncate by privileged user leaves the suid and sgid
  run_truncate_test_case(06766, S_ISUID | S_ISGID, 10000, true);

  // 14, Truncate by unprivileged user clears the suid and sgid
  run_truncate_test_case(06766, 0, 100);

  // clean up
  ceph_shutdown(cmount);
  ceph_shutdown(admin);
}

TEST(LibCephFS, ChownClearSetuid) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  Inode *root;
  ASSERT_EQ(ceph_ll_lookup_root(cmount, &root), 0);

  char filename[32];
  sprintf(filename, "clearsetuid%x", getpid());

  Fh *fh;
  Inode *in;
  struct ceph_statx stx;
  const mode_t after_mode = S_IRWXU;
  const mode_t before_mode = S_IRWXU | S_ISUID | S_ISGID;
  const unsigned want = CEPH_STATX_UID|CEPH_STATX_GID|CEPH_STATX_MODE;
  UserPerm *usercred = ceph_mount_perms(cmount);

  ceph_ll_unlink(cmount, root, filename, usercred);
  ASSERT_EQ(ceph_ll_create(cmount, root, filename, before_mode,
                          O_RDWR|O_CREAT|O_EXCL, &in, &fh, &stx, want, 0,
                          usercred), 0);

  ASSERT_EQ(stx.stx_mode & (mode_t)ALLPERMS, before_mode);

  // chown  -- for this we need to be "root"
  UserPerm *rootcred = ceph_userperm_new(0, 0, 0, NULL);
  ASSERT_TRUE(rootcred);
  stx.stx_uid++;
  stx.stx_gid++;
  ASSERT_EQ(ceph_ll_setattr(cmount, in, &stx, CEPH_SETATTR_UID|CEPH_SETATTR_GID, rootcred), 0);
  ASSERT_EQ(ceph_ll_getattr(cmount, in, &stx, CEPH_STATX_MODE, 0, usercred), 0);
  ASSERT_TRUE(stx.stx_mask & CEPH_STATX_MODE);
  ASSERT_EQ(stx.stx_mode & (mode_t)ALLPERMS, after_mode);

  /* test chown with supplementary groups, and chown with/without exe bit */
  uid_t u = 65534;
  gid_t g = 65534;
  gid_t gids[] = {65533,65532};
  UserPerm *altcred = ceph_userperm_new(u, g, sizeof gids / sizeof gids[0], gids);
  stx.stx_uid = u;
  stx.stx_gid = g;
  mode_t m = S_ISGID|S_ISUID|S_IRUSR|S_IWUSR;
  stx.stx_mode = m;
  ASSERT_EQ(ceph_ll_setattr(cmount, in, &stx, CEPH_SETATTR_MODE|CEPH_SETATTR_UID|CEPH_SETATTR_GID, rootcred), 0);
  ASSERT_EQ(ceph_ll_getattr(cmount, in, &stx, CEPH_STATX_MODE, 0, altcred), 0);
  ASSERT_EQ(stx.stx_mode&(mode_t)ALLPERMS, m);
  /* not dropped without exe bit */
  stx.stx_gid = gids[0];
  ASSERT_EQ(ceph_ll_setattr(cmount, in, &stx, CEPH_SETATTR_GID, altcred), 0);
  ASSERT_EQ(ceph_ll_getattr(cmount, in, &stx, CEPH_STATX_MODE, 0, altcred), 0);
  ASSERT_EQ(stx.stx_mode&(mode_t)ALLPERMS, m);
  /* now check dropped with exe bit */
  m = S_ISGID|S_ISUID|S_IRWXU;
  stx.stx_mode = m;
  ASSERT_EQ(ceph_ll_setattr(cmount, in, &stx, CEPH_STATX_MODE, altcred), 0);
  ASSERT_EQ(ceph_ll_getattr(cmount, in, &stx, CEPH_STATX_MODE, 0, altcred), 0);
  ASSERT_EQ(stx.stx_mode&(mode_t)ALLPERMS, m);
  stx.stx_gid = gids[1];
  ASSERT_EQ(ceph_ll_setattr(cmount, in, &stx, CEPH_SETATTR_GID, altcred), 0);
  ASSERT_EQ(ceph_ll_getattr(cmount, in, &stx, CEPH_STATX_MODE, 0, altcred), 0);
  ASSERT_EQ(stx.stx_mode&(mode_t)ALLPERMS, m&(S_IRWXU|S_IRWXG|S_IRWXO));
  ceph_userperm_destroy(altcred);

  ASSERT_EQ(ceph_ll_close(cmount, fh), 0);
  ceph_shutdown(cmount);
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
