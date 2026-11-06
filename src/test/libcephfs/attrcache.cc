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
#include <stdint.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string>

/*
 * The two clients below always end up on opposite sides of a capability
 * revocation: the MDS does not let the writer proceed until the reader has
 * given up the shared capability that protects what it is about to change,
 * and it does not hand that capability straight back while the writer is
 * still holding the file open.  What the reader answers in the meantime is
 * what these tests are about.
 */
static void mount_ac(struct ceph_mount_info **cmount, const char *attr,
		     const char *dentry)
{
  ASSERT_EQ(0, ceph_create(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(*cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(*cmount, NULL));
  ASSERT_EQ(0, ceph_conf_set(*cmount, "client_attr_cache_timeout", attr));
  ASSERT_EQ(0, ceph_conf_set(*cmount, "client_dentry_cache_timeout", dentry));
  ASSERT_EQ(0, ceph_mount(*cmount, NULL));
}

static void create_file(struct ceph_mount_info *cmount, const char *name)
{
  int fd = ceph_open(cmount, name, O_CREAT|O_WRONLY, 0644);
  ASSERT_LE(0, fd);
  ASSERT_EQ(0, ceph_close(cmount, fd));
}

static void assert_size(struct ceph_mount_info *cmount, const char *name,
			uint64_t expected)
{
  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_statx(cmount, name, &stx, CEPH_STATX_SIZE, 0));
  ASSERT_EQ(expected, stx.stx_size);
}

/* leaves the file open for writing on ca, so that cb does not get Fs back */
static int grow_file(struct ceph_mount_info *ca, const char *name)
{
  int fd = ceph_open(ca, name, O_WRONLY, 0644);
  if (fd < 0)
    return fd;
  if (ceph_write(ca, fd, "bbbbbbbb", 8, 0) != 8)
    return -EIO;
  if (int r = ceph_fsync(ca, fd, 0); r < 0)
    return r;
  return fd;
}

/*
 * A client that was told to give up the capability protecting the size of a
 * file goes on answering out of what it last heard, the way an NFS client
 * does within acregmin.
 */
TEST(LibCephFS, AttrCacheServesStaleSize) {
  struct ceph_mount_info *ca, *cb;
  mount_ac(&ca, "60", "0");
  mount_ac(&cb, "60", "0");

  char name[32];
  snprintf(name, sizeof(name), "ac1.%d", getpid());

  int fda = ceph_open(ca, name, O_CREAT|O_WRONLY, 0644);
  ASSERT_LE(0, fda);
  ASSERT_EQ(4, ceph_write(ca, fda, "aaaa", 4, 0));
  ASSERT_EQ(0, ceph_close(ca, fda));

  /* client b picks up the size, and the capability that protects it */
  assert_size(cb, name, 4);

  /* which the MDS has to take away again to let this through */
  fda = grow_file(ca, name);
  ASSERT_LE(0, fda);

  /* client b is allowed to keep serving what it had */
  assert_size(cb, name, 4);

  ASSERT_EQ(0, ceph_close(ca, fda));
  ASSERT_EQ(0, ceph_unlink(ca, name));
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

/*
 * Without the option the client asks the MDS as soon as it no longer holds
 * the capability, which is what CephFS has always done.
 */
TEST(LibCephFS, AttrCacheDisabled) {
  struct ceph_mount_info *ca, *cb;
  mount_ac(&ca, "0", "0");
  mount_ac(&cb, "0", "0");

  char name[32];
  snprintf(name, sizeof(name), "ac2.%d", getpid());

  int fda = ceph_open(ca, name, O_CREAT|O_WRONLY, 0644);
  ASSERT_LE(0, fda);
  ASSERT_EQ(4, ceph_write(ca, fda, "aaaa", 4, 0));
  ASSERT_EQ(0, ceph_close(ca, fda));

  assert_size(cb, name, 4);

  fda = grow_file(ca, name);
  ASSERT_LE(0, fda);

  assert_size(cb, name, 8);

  ASSERT_EQ(0, ceph_close(ca, fda));
  ASSERT_EQ(0, ceph_unlink(ca, name));
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

/*
 * A client that is never handed the capability back still has to catch up
 * once the interval has passed.
 */
TEST(LibCephFS, AttrCacheExpires) {
  struct ceph_mount_info *ca, *cb;
  mount_ac(&ca, "60", "0");
  mount_ac(&cb, "1", "0");

  char name[32];
  snprintf(name, sizeof(name), "ac3.%d", getpid());

  int fda = ceph_open(ca, name, O_CREAT|O_WRONLY, 0644);
  ASSERT_LE(0, fda);
  ASSERT_EQ(4, ceph_write(ca, fda, "aaaa", 4, 0));
  ASSERT_EQ(0, ceph_close(ca, fda));

  assert_size(cb, name, 4);

  fda = grow_file(ca, name);
  ASSERT_LE(0, fda);

  sleep(3);

  assert_size(cb, name, 8);

  ASSERT_EQ(0, ceph_close(ca, fda));
  ASSERT_EQ(0, ceph_unlink(ca, name));
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

static void assert_xattr(struct ceph_mount_info *cmount, const char *path,
			 const char *name, const char *expected)
{
  char buf[32];
  int len = strlen(expected);
  ASSERT_EQ(len, ceph_getxattr(cmount, path, name, buf, sizeof(buf)));
  ASSERT_EQ(0, memcmp(buf, expected, len));
}

/*
 * The reply to a setxattr refreshes the attributes without carrying the
 * xattrs, so it must not make the ones we already hold look fresh.
 */
TEST(LibCephFS, AttrCacheSeesOwnXattr) {
  struct ceph_mount_info *ca, *cb;
  mount_ac(&ca, "60", "60");
  // cb reads uncached, which keeps taking Xs away from ca
  mount_ac(&cb, "0", "0");
  char name[32];
  snprintf(name, sizeof(name), "ac_own_xattr.%d", getpid());
  create_file(ca, name);

  for (auto value : {"one", "two", "three"}) {
    ASSERT_EQ(0, ceph_setxattr(ca, name, "user.k", value, strlen(value), 0));
    assert_xattr(ca, name, "user.k", value);
    assert_xattr(cb, name, "user.k", value);
  }
  ASSERT_EQ(0, ceph_removexattr(ca, name, "user.k"));
  char buf[32];
  ASSERT_EQ(-ENODATA, ceph_getxattr(ca, name, "user.k", buf, sizeof(buf)));

  ASSERT_EQ(0, ceph_unlink(ca, name));
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

/* stats that reach the MDS must not keep xattrs in the cache past the timeout */
TEST(LibCephFS, AttrCacheXattrsExpire) {
  struct ceph_mount_info *ca, *cb;
  mount_ac(&ca, "60", "0");
  mount_ac(&cb, "2", "0");
  char name[32];
  snprintf(name, sizeof(name), "ac_xattr.%d", getpid());
  create_file(ca, name);

  ASSERT_EQ(0, ceph_setxattr(ca, name, "user.k", "one", 3, 0));
  assert_xattr(cb, name, "user.k", "one");
  ASSERT_EQ(0, ceph_setxattr(ca, name, "user.k", "two", 3, 0));

  // with ca writing, cb has no Fs, so its stats go to the MDS once they expire
  int fda = grow_file(ca, name);
  ASSERT_LE(0, fda);
  for (int i = 0; i < 9; ++i) {
    struct ceph_statx stx;
    ASSERT_EQ(0, ceph_statx(cb, name, &stx, CEPH_STATX_SIZE, 0));
    usleep(500 * 1000);
  }
  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_statx(cb, name, &stx, CEPH_STATX_SIZE, 0));
  assert_xattr(cb, name, "user.k", "two");

  ASSERT_EQ(0, ceph_close(ca, fda));
  ASSERT_EQ(0, ceph_unlink(ca, name));
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

static void mount_ac_acl(struct ceph_mount_info **cmount)
{
  ASSERT_EQ(0, ceph_create(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(*cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(*cmount, NULL));
  ASSERT_EQ(0, ceph_conf_set(*cmount, "client_attr_cache_timeout", "60"));
  ASSERT_EQ(0, ceph_conf_set(*cmount, "client_acl_type", "posix_acl"));
  ASSERT_EQ(0, ceph_mount(*cmount, NULL));
}

/* user:<uid>:r--, with the usual owner, group, mask and other entries */
static std::string acl_for_user(uint32_t uid)
{
  struct entry { uint16_t tag, perm; uint32_t id; };
  const entry entries[] = {
    {0x01, 6, 0xffffffff}, {0x02, 4, uid}, {0x04, 4, 0xffffffff},
    {0x10, 6, 0xffffffff}, {0x20, 4, 0xffffffff},
  };
  const uint32_t version = 2;
  std::string acl((const char *)&version, sizeof(version));
  acl.append((const char *)entries, sizeof(entries));
  return acl;
}

/* chmod rewrites the ACL, so it must not bring back a cached one */
TEST(LibCephFS, AttrCacheChmodKeepsNewAcl) {
  struct ceph_mount_info *ca, *cb;
  mount_ac_acl(&ca);
  mount_ac_acl(&cb);
  char name[32];
  snprintf(name, sizeof(name), "ac_acl.%d", getpid());
  create_file(ca, name);

  const char *key = "system.posix_acl_access";
  std::string first = acl_for_user(1000), second = acl_for_user(1001);
  ASSERT_EQ(0, ceph_setxattr(ca, name, key, first.data(), first.size(), 0));
  char buf[128];
  ASSERT_EQ((int)first.size(), ceph_getxattr(cb, name, key, buf, sizeof(buf)));
  ASSERT_EQ(0, ceph_setxattr(ca, name, key, second.data(), second.size(), 0));

  ASSERT_EQ(0, ceph_chmod(cb, name, 0640));

  ASSERT_EQ(0, ceph_conf_set(ca, "client_attr_cache_timeout", "0"));
  ASSERT_EQ((int)second.size(), ceph_getxattr(ca, name, key, buf, sizeof(buf)));
  // the named user entry is the second one, after the version
  uint32_t uid;
  memcpy(&uid, buf + 4 + 8 + 4, sizeof(uid));
  ASSERT_EQ(1001u, uid);

  ASSERT_EQ(0, ceph_unlink(ca, name));
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

static int count_entries(struct ceph_mount_info *cmount, const char *path)
{
  struct ceph_dir_result *dirp;
  if (int r = ceph_opendir(cmount, path, &dirp); r < 0)
    return r;

  int n = 0;
  while (struct dirent *de = ceph_readdir(cmount, dirp)) {
    if (strcmp(de->d_name, ".") && strcmp(de->d_name, ".."))
      n++;
  }
  ceph_closedir(cmount, dirp);
  return n;
}

/*
 * Dentry caching is the other half: a client that read a directory keeps
 * answering readdir out of that listing after the MDS stopped telling it
 * about changes, so a name another client created is not listed yet.  This
 * is the acdirmin behaviour of NFS.  Looking the name up still finds it:
 * the client never answers ENOENT out of a listing it cannot vouch for.
 */
TEST(LibCephFS, DentryCacheHidesNewName) {
  struct ceph_mount_info *ca, *cb;
  mount_ac(&ca, "60", "60");
  mount_ac(&cb, "60", "60");

  char dir[32], path[64];
  snprintf(dir, sizeof(dir), "acdir1.%d", getpid());
  snprintf(path, sizeof(path), "%s/new", dir);

  ASSERT_EQ(0, ceph_mkdir(ca, dir, 0755));

  /* client b learns the whole directory ... */
  ASSERT_EQ(0, count_entries(cb, dir));

  /* ... and the MDS takes Fs away from it to let this through */
  create_file(ca, path);

  /* client b may go on listing the directory as empty ... */
  ASSERT_EQ(0, count_entries(cb, dir));

  /* ... but not deny that the name exists */
  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_statx(cb, path, &stx, CEPH_STATX_MODE, 0));

  ASSERT_EQ(0, ceph_unlink(ca, path));
  ASSERT_EQ(0, ceph_rmdir(ca, dir));
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

/*
 * Without the option the client goes back to the MDS as soon as it stops
 * holding Fs on the directory.
 */
TEST(LibCephFS, DentryCacheDisabled) {
  struct ceph_mount_info *ca, *cb;
  mount_ac(&ca, "0", "0");
  mount_ac(&cb, "0", "0");

  char dir[32], path[64];
  snprintf(dir, sizeof(dir), "acdir2.%d", getpid());
  snprintf(path, sizeof(path), "%s/new", dir);

  ASSERT_EQ(0, ceph_mkdir(ca, dir, 0755));
  ASSERT_EQ(0, count_entries(cb, dir));

  create_file(ca, path);

  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_statx(cb, path, &stx, CEPH_STATX_MODE, 0));
  ASSERT_EQ(1, count_entries(cb, dir));

  ASSERT_EQ(0, ceph_unlink(ca, path));
  ASSERT_EQ(0, ceph_rmdir(ca, dir));
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

/*
 * And a listing the MDS never took an interest in again is dropped once the
 * interval has passed.
 */
TEST(LibCephFS, DentryCacheExpires) {
  struct ceph_mount_info *ca, *cb;
  mount_ac(&ca, "60", "60");
  mount_ac(&cb, "60", "1");

  char dir[32], path[64];
  snprintf(dir, sizeof(dir), "acdir3.%d", getpid());
  snprintf(path, sizeof(path), "%s/new", dir);

  ASSERT_EQ(0, ceph_mkdir(ca, dir, 0755));
  ASSERT_EQ(0, count_entries(cb, dir));

  create_file(ca, path);

  sleep(3);

  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_statx(cb, path, &stx, CEPH_STATX_MODE, 0));
  ASSERT_EQ(1, count_entries(cb, dir));

  ASSERT_EQ(0, ceph_unlink(ca, path));
  ASSERT_EQ(0, ceph_rmdir(ca, dir));
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

/*
 * Client b resolves dir/old by name, without ever listing dir, and client a
 * then renames it away.  Returns what b's next stat() of dir/old says after
 * waiting `wait` seconds, or a value below -1000 if the setup failed.
 */
static int stat_renamed_name(const char *battr, const char *bdentry, int wait)
{
  struct ceph_mount_info *ca, *cb;
  mount_ac(&ca, "60", "60");
  mount_ac(&cb, battr, bdentry);

  char dir[64], oldp[96], newp[96];
  snprintf(dir, sizeof(dir), "acdir4.%d.%s.%d", getpid(), bdentry, wait);
  snprintf(oldp, sizeof(oldp), "%s/old", dir);
  snprintf(newp, sizeof(newp), "%s/new", dir);

  int r = ceph_mkdir(ca, dir, 0755);
  if (r < 0) {
    r = -1000 + r;
  } else {
    create_file(ca, oldp);

    /* client b learns the name by looking it up, not by reading dir */
    struct ceph_statx stx;
    r = ceph_statx(cb, oldp, &stx, CEPH_STATX_MODE, 0);
    if (r < 0) {
      r = -1000 + r;
    } else if ((r = ceph_rename(ca, oldp, newp)) < 0) {
      /* the MDS revoked b's lease on the name to let that through */
      r = -1000 + r;
    } else {
      if (wait)
	sleep(wait);
      r = ceph_statx(cb, oldp, &stx, CEPH_STATX_MODE, 0);
    }
    ceph_unlink(ca, newp);
    ceph_unlink(ca, oldp);
    ceph_rmdir(ca, dir);
  }
  ceph_shutdown(ca);
  ceph_shutdown(cb);
  return r;
}

/*
 * A name looked up on its own is cached like one read in a listing: the
 * client goes on resolving it after another client renamed it away, the way
 * an NFS client does within acdirmin.
 */
TEST(LibCephFS, DentryCacheServesLookedUpName) {
  ASSERT_EQ(0, stat_renamed_name("60", "60", 0));
}

/* Without the option the client asks the MDS, which no longer has the name. */
TEST(LibCephFS, DentryCacheLookedUpNameDisabled) {
  ASSERT_EQ(-ENOENT, stat_renamed_name("0", "0", 0));
}

/* And once the interval has passed the client asks again. */
TEST(LibCephFS, DentryCacheLookedUpNameExpires) {
  ASSERT_EQ(-ENOENT, stat_renamed_name("60", "1", 3));
}

/*
 * An exclusive create must not fail with EEXIST because of a name that is
 * only still in the cache: client b learned dir/x, client a removed it, and
 * b creating dir/x exclusively has to succeed.
 */
TEST(LibCephFS, DentryCacheExclusiveCreate) {
  struct ceph_mount_info *ca, *cb;
  mount_ac(&ca, "60", "60");
  mount_ac(&cb, "60", "60");

  char dir[32], path[64];
  snprintf(dir, sizeof(dir), "acdir5.%d", getpid());
  snprintf(path, sizeof(path), "%s/x", dir);

  ASSERT_EQ(0, ceph_mkdir(ca, dir, 0755));
  create_file(ca, path);

  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_statx(cb, path, &stx, CEPH_STATX_MODE, 0));
  ASSERT_EQ(0, ceph_unlink(ca, path));

  /* a stat may still find it ... */
  ASSERT_EQ(0, ceph_statx(cb, path, &stx, CEPH_STATX_MODE, 0));

  /* ... but creating it exclusively asks the MDS */
  int fd = ceph_open(cb, path, O_CREAT|O_EXCL|O_WRONLY, 0644);
  ASSERT_LE(0, fd);
  ASSERT_EQ(0, ceph_close(cb, fd));

  /* and a second exclusive create of a name that really exists still fails */
  ASSERT_EQ(-EEXIST, ceph_open(ca, path, O_CREAT|O_EXCL|O_WRONLY, 0644));

  ASSERT_EQ(0, ceph_unlink(ca, path));
  ASSERT_EQ(0, ceph_rmdir(ca, dir));
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

/*
 * One client makes a directory, another uses it straight away.  Client b
 * has seen the directory empty - which marks it complete - and must still
 * find what client a then creates in it, both by name and to create a file
 * underneath.  IO500's mdtest-easy does exactly this across nodes.
 */
TEST(LibCephFS, DentryCacheNewSubdirIsFound) {
  struct ceph_mount_info *ca, *cb;
  mount_ac(&ca, "60", "60");
  mount_ac(&cb, "60", "60");

  char dir[32], sub[64], file[96];
  snprintf(dir, sizeof(dir), "acdir6.%d", getpid());
  snprintf(sub, sizeof(sub), "%s/sub", dir);
  snprintf(file, sizeof(file), "%s/sub/f", dir);

  ASSERT_EQ(0, ceph_mkdir(ca, dir, 0755));

  /* client b sees dir while it is empty */
  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_statx(cb, dir, &stx, CEPH_STATX_MODE, 0));
  ASSERT_EQ(0, count_entries(cb, dir));

  /* the MDS takes Fs away from b to let this through */
  ASSERT_EQ(0, ceph_mkdir(ca, sub, 0755));

  ASSERT_EQ(0, ceph_statx(cb, sub, &stx, CEPH_STATX_MODE, 0));
  int fd = ceph_open(cb, file, O_CREAT|O_WRONLY, 0644);
  ASSERT_LE(0, fd);
  ASSERT_EQ(0, ceph_close(cb, fd));

  ASSERT_EQ(0, ceph_unlink(ca, file));
  ASSERT_EQ(0, ceph_rmdir(ca, sub));
  ASSERT_EQ(0, ceph_rmdir(ca, dir));
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

/*
 * Opening through a cached name must not open the inode behind it after
 * another client removed it: writes to that orphan would be lost.  By inode
 * (how ceph-fuse opens) the open fails with ESTALE, on which the kernel looks
 * the name up again; by path the name is resolved at the MDS.
 */
TEST(LibCephFS, DentryCacheOpenOfRemovedName) {
  struct ceph_mount_info *ca, *cb;
  mount_ac(&ca, "60", "60");
  mount_ac(&cb, "60", "60");

  char dir[32], path[64];
  snprintf(dir, sizeof(dir), "acdir7.%d", getpid());
  snprintf(path, sizeof(path), "%s/f", dir);

  ASSERT_EQ(0, ceph_mkdir(ca, dir, 0755));
  create_file(ca, path);

  UserPerm *perms = ceph_mount_perms(cb);
  Inode *root, *diri, *in;
  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_ll_lookup_root(cb, &root));
  ASSERT_EQ(0, ceph_ll_lookup(cb, root, dir, &diri, &stx, 0, 0, perms));
  ASSERT_EQ(0, ceph_ll_lookup(cb, diri, "f", &in, &stx, CEPH_STATX_MODE, 0, perms));

  ASSERT_EQ(0, ceph_unlink(ca, path));

  /* whatever the cache still says about the name, opening it must not work */
  struct Fh *fh = nullptr;
  ASSERT_EQ(-ESTALE, ceph_ll_open(cb, in, O_RDONLY, &fh, perms));
  ASSERT_EQ(-ENOENT, ceph_open(cb, path, O_RDONLY, 0));

  /* and a create makes a new file that client a can see */
  int fd = ceph_open(cb, path, O_CREAT|O_WRONLY, 0644);
  ASSERT_LE(0, fd);
  ASSERT_EQ(0, ceph_close(cb, fd));
  ASSERT_EQ(0, ceph_statx(ca, path, &stx, CEPH_STATX_MODE, 0));

  ceph_ll_put(cb, in);
  ceph_ll_put(cb, diri);
  ceph_ll_put(cb, root);
  ASSERT_EQ(0, ceph_unlink(ca, path));
  ASSERT_EQ(0, ceph_rmdir(ca, dir));
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

/*
 * The same for the other ways of creating a name: a mkdir-based lock taken
 * on one client must work right after another client dropped it.
 */
TEST(LibCephFS, DentryCacheMkdirOfRemovedName) {
  struct ceph_mount_info *ca, *cb;
  mount_ac(&ca, "60", "60");
  mount_ac(&cb, "60", "60");

  char dir[32], lock[64];
  snprintf(dir, sizeof(dir), "acdir8.%d", getpid());
  snprintf(lock, sizeof(lock), "%s/lock", dir);

  ASSERT_EQ(0, ceph_mkdir(ca, dir, 0755));
  ASSERT_EQ(0, ceph_mkdir(ca, lock, 0755));

  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_statx(cb, lock, &stx, CEPH_STATX_MODE, 0));
  ASSERT_EQ(0, ceph_rmdir(ca, lock));

  ASSERT_EQ(0, ceph_mkdir(cb, lock, 0755));
  ASSERT_EQ(0, ceph_rmdir(cb, lock));
  ASSERT_EQ(0, ceph_symlink(cb, "target", lock));
  ASSERT_EQ(0, ceph_unlink(cb, lock));

  ASSERT_EQ(0, ceph_rmdir(ca, dir));
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}

/*
 * The attribute cache must not hide this client's own changes: unlink(),
 * link() and rename() over a name give up the capability protecting the link
 * count, and the MDS does not always send the inode back.
 */
static void assert_nlink(struct ceph_mount_info *cmount, const char *name,
			 uint32_t expected)
{
  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_statx(cmount, name, &stx, CEPH_STATX_NLINK, 0));
  ASSERT_EQ(expected, stx.stx_nlink);
}

TEST(LibCephFS, AttrCacheSeesOwnLinkCount) {
  struct ceph_mount_info *cmount;
  mount_ac(&cmount, "60", "60");
  char a[64], b[64], c[64];
  sprintf(a, "own_nlink_a_%d", getpid());
  sprintf(b, "own_nlink_b_%d", getpid());
  sprintf(c, "own_nlink_c_%d", getpid());
  create_file(cmount, a);
  create_file(cmount, c);
  assert_nlink(cmount, a, 1);

  ASSERT_EQ(0, ceph_link(cmount, a, b));
  assert_nlink(cmount, a, 2);
  ASSERT_EQ(0, ceph_unlink(cmount, b));
  assert_nlink(cmount, a, 1);

  // rename c over a link of a: a loses that link
  ASSERT_EQ(0, ceph_link(cmount, a, b));
  assert_nlink(cmount, a, 2);
  ASSERT_EQ(0, ceph_rename(cmount, c, b));
  assert_nlink(cmount, a, 1);

  ASSERT_EQ(0, ceph_unlink(cmount, a));
  ASSERT_EQ(0, ceph_unlink(cmount, b));
  ceph_shutdown(cmount);
}

/*
 * What a FUSE file system on libcephfs hands the kernel: with the caches on,
 * names and attributes the client may serve get a timeout up to the cache
 * timeout; with them off, or for a name that does not exist, nothing may be
 * cached.
 */
static void check_ttls(const char *timeout, bool cached)
{
  struct ceph_mount_info *cmount;
  mount_ac(&cmount, timeout, timeout);
  char name[64];
  sprintf(name, "ttl_api_%d_%s", getpid(), timeout);
  create_file(cmount, name);

  UserPerm *perms = ceph_mount_perms(cmount);
  Inode *root, *in;
  ASSERT_EQ(0, ceph_ll_lookup_root(cmount, &root));
  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_ll_lookup(cmount, root, name, &in, &stx,
			      CEPH_STATX_BASIC_STATS, 0, perms));

  double attr = ceph_ll_attr_ttl(cmount, in);
  double dentry = ceph_ll_dentry_ttl(cmount, root, name);
  if (cached) {
    ASSERT_GT(attr, 0.0);
    ASSERT_LE(attr, 60.0);
    ASSERT_GT(dentry, 0.0);
    ASSERT_LE(dentry, 60.0);
  } else {
    ASSERT_EQ(0.0, attr);
    ASSERT_EQ(0.0, dentry);
  }
  ASSERT_EQ(0.0, ceph_ll_dentry_ttl(cmount, root, "no_such_name_here"));

  // without close-to-open an open keeps the page cache and the attributes
  Fh *fh;
  ASSERT_EQ(0, ceph_ll_open(cmount, in, O_RDONLY, &fh, perms));
  ASSERT_EQ(1, ceph_ll_keep_page_cache(cmount, fh));
  ASSERT_EQ(0, ceph_ll_open_invalidates_attrs(cmount, fh));
  ASSERT_EQ(0, ceph_ll_flush(cmount, fh));
  ASSERT_EQ(0, ceph_ll_close(cmount, fh));

  ceph_ll_put(cmount, in);
  ceph_ll_put(cmount, root);
  ASSERT_EQ(0, ceph_unlink(cmount, name));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, CacheTtlApi) {
  check_ttls("60", true);
}

TEST(LibCephFS, CacheTtlApiDisabled) {
  check_ttls("0", false);
}

/*
 * A FUSE file system hands the kernel ceph_ll_dentry_ttl() for every name it
 * returns, and the kernel asks again about such a name before that runs out
 * only when it needs the name to be current: for an exclusive create, a
 * mkdir() and the like.  That lookup must not be answered out of the cache.
 * An ordinary lookup of a cached name still is.
 */
TEST(LibCephFS, DentryCacheKernelRevalidation) {
  struct ceph_mount_info *ca, *cb;
  mount_ac(&ca, "60", "60");
  mount_ac(&cb, "60", "60");

  char dir[32], f[64], g[64];
  snprintf(dir, sizeof(dir), "acdir9.%d", getpid());
  snprintf(f, sizeof(f), "%s/f", dir);
  snprintf(g, sizeof(g), "%s/g", dir);
  ASSERT_EQ(0, ceph_mkdir(ca, dir, 0755));
  create_file(ca, f);
  create_file(ca, g);

  UserPerm *perms = ceph_mount_perms(cb);
  Inode *root, *diri, *fi, *gi, *in;
  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_ll_lookup_root(cb, &root));
  ASSERT_EQ(0, ceph_ll_lookup(cb, root, dir, &diri, &stx, 0, 0, perms));
  ASSERT_EQ(0, ceph_ll_lookup(cb, diri, "f", &fi, &stx, CEPH_STATX_MODE, 0, perms));
  ASSERT_EQ(0, ceph_ll_lookup(cb, diri, "g", &gi, &stx, CEPH_STATX_MODE, 0, perms));

  /* Names no dentry can hold: an empty one used to abort the client. */
  struct Inode *same;
  for (const char *name : {"", ".", ".."}) {
    ASSERT_EQ(0, ceph_ll_lookup(cb, diri, name, &same, &stx, CEPH_STATX_MODE, 0, perms));
    ceph_ll_put(cb, same);
  }
  // the kernel may cache f, not g
  ASSERT_GT(ceph_ll_dentry_ttl(cb, diri, "f"), 0.0);

  ASSERT_EQ(0, ceph_unlink(ca, f));
  ASSERT_EQ(0, ceph_unlink(ca, g));

  // the kernel revalidating f is told the truth
  ASSERT_EQ(-ENOENT, ceph_ll_lookup(cb, diri, "f", &in, &stx, CEPH_STATX_MODE, 0, perms));
  // a lookup of g is served from the cache, as before
  ASSERT_EQ(0, ceph_ll_lookup(cb, diri, "g", &in, &stx, CEPH_STATX_MODE, 0, perms));
  ceph_ll_put(cb, in);

  ceph_ll_put(cb, gi);
  ceph_ll_put(cb, fi);
  ceph_ll_put(cb, diri);
  ceph_ll_put(cb, root);
  ASSERT_EQ(0, ceph_rmdir(ca, dir));
  ceph_shutdown(ca);
  ceph_shutdown(cb);
}
