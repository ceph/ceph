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

#include "include/compat.h"
#include "gtest/gtest.h"
#include "include/cephfs/libcephfs.h"
#include "mds/mdstypes.h"
#include "include/stat.h"
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <sys/uio.h>
#include <sys/time.h>

#ifndef _WIN32
#include <sys/resource.h>
#endif

#include "common/Clock.h"

#ifdef __linux__
#include <limits.h>
#include <sys/xattr.h>
#endif

#include <fmt/format.h>
#include <map>
#include <vector>
#include <thread>
#include <regex>

using namespace std;

TEST(LibCephFS, OpenEmptyComponent) {

  pid_t mypid = getpid();
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  char c_dir[1024];
  sprintf(c_dir, "/open_test_%d", mypid);
  struct ceph_dir_result *dirp;

  ASSERT_EQ(0, ceph_mkdirs(cmount, c_dir, 0777));

  ASSERT_EQ(0, ceph_opendir(cmount, c_dir, &dirp));

  char c_path[1024];
  sprintf(c_path, "/open_test_%d//created_file_%d", mypid, mypid);
  int fd = ceph_open(cmount, c_path, O_RDONLY|O_CREAT, 0666);
  ASSERT_LT(0, fd);

  ASSERT_EQ(0, ceph_close(cmount, fd));
  ASSERT_EQ(0, ceph_closedir(cmount, dirp));
  ceph_shutdown(cmount);

  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));

  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  fd = ceph_open(cmount, c_path, O_RDONLY, 0666);
  ASSERT_LT(0, fd);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  // cleanup
  ASSERT_EQ(0, ceph_unlink(cmount, c_path));
  ASSERT_EQ(0, ceph_rmdir(cmount, c_dir));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, OpenReadTruncate) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  auto path = fmt::format("test_open_rdt_{}", getpid());
  int fd = ceph_open(cmount, path.c_str(), O_WRONLY|O_CREAT, 0666);
  ASSERT_LE(0, fd);

  auto data = std::string("hello world");
  ASSERT_EQ(ceph_write(cmount, fd, data.c_str(), data.size(), 0), (int)data.size());
  ASSERT_EQ(0, ceph_close(cmount, fd));

  fd = ceph_open(cmount, path.c_str(), O_RDONLY, 0);
  ASSERT_LE(0, fd);
  ASSERT_EQ(ceph_ftruncate(cmount, fd, 0), -CEPHFS_EBADF);
  ASSERT_EQ(ceph_ftruncate(cmount, fd, 1), -CEPHFS_EBADF);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, OpenReadWrite) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  char c_path[1024];
  sprintf(c_path, "test_open_rdwr_%d", getpid());
  int fd = ceph_open(cmount, c_path, O_WRONLY|O_CREAT, 0666);
  ASSERT_LT(0, fd);

  const char *out_buf = "hello world";
  size_t size = strlen(out_buf);
  char in_buf[100];
  ASSERT_EQ(ceph_write(cmount, fd, out_buf, size, 0), (int)size);
  ASSERT_EQ(ceph_read(cmount, fd, in_buf, sizeof(in_buf), 0), -CEPHFS_EBADF);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  fd = ceph_open(cmount, c_path, O_RDONLY, 0);
  ASSERT_LT(0, fd);
  ASSERT_EQ(ceph_write(cmount, fd, out_buf, size, 0), -CEPHFS_EBADF);
  ASSERT_EQ(ceph_read(cmount, fd, in_buf, sizeof(in_buf), 0), (int)size);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  fd = ceph_open(cmount, c_path, O_RDWR, 0);
  ASSERT_LT(0, fd);
  ASSERT_EQ(ceph_write(cmount, fd, out_buf, size, 0), (int)size);
  ASSERT_EQ(ceph_read(cmount, fd, in_buf, sizeof(in_buf), 0), (int)size);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, MountNonExist) {

  struct ceph_mount_info *cmount;

  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_NE(0, ceph_mount(cmount, "/non-exist"));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, MountDouble) {

  struct ceph_mount_info *cmount;

  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));
  ASSERT_EQ(-CEPHFS_EISCONN, ceph_mount(cmount, "/"));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, MountRemount) {

  struct ceph_mount_info *cmount;

  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));

  CephContext *cct = ceph_get_mount_context(cmount);
  ASSERT_EQ(0, ceph_mount(cmount, "/"));
  ASSERT_EQ(0, ceph_unmount(cmount));

  ASSERT_EQ(0, ceph_mount(cmount, "/"));
  ASSERT_EQ(cct, ceph_get_mount_context(cmount));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, UnmountUnmounted) {

  struct ceph_mount_info *cmount;

  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(-CEPHFS_ENOTCONN, ceph_unmount(cmount));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, ReleaseUnmounted) {

  struct ceph_mount_info *cmount;

  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_release(cmount));
}

TEST(LibCephFS, ReleaseMounted) {

  struct ceph_mount_info *cmount;

  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));
  ASSERT_EQ(-CEPHFS_EISCONN, ceph_release(cmount));
  ASSERT_EQ(0, ceph_unmount(cmount));
  ASSERT_EQ(0, ceph_release(cmount));
}

TEST(LibCephFS, UnmountRelease) {

  struct ceph_mount_info *cmount;

  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));
  ASSERT_EQ(0, ceph_unmount(cmount));
  ASSERT_EQ(0, ceph_release(cmount));
}

TEST(LibCephFS, Mount) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);
  ceph_shutdown(cmount);

  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);
  ceph_shutdown(cmount);
}

TEST(LibCephFS, OpenLayout) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  /* valid layout */
  char test_layout_file[256];
  sprintf(test_layout_file, "test_layout_%d_b", getpid());
  int fd = ceph_open_layout(cmount, test_layout_file, O_CREAT|O_WRONLY, 0666, (1<<20), 7, (1<<20), NULL);
  ASSERT_GT(fd, 0);
  char poolname[80];
  ASSERT_LT(0, ceph_get_file_pool_name(cmount, fd, poolname, sizeof(poolname)));
  ASSERT_LT(0, ceph_get_file_pool_name(cmount, fd, poolname, 0));

  /* on already-written file (CEPHFS_ENOTEMPTY) */
  ceph_write(cmount, fd, "hello world", 11, 0);
  ceph_close(cmount, fd);

  char xattrk[128];
  char xattrv[128];
  sprintf(xattrk, "ceph.file.layout.stripe_unit");
  sprintf(xattrv, "65536");
  ASSERT_EQ(-CEPHFS_ENOTEMPTY, ceph_setxattr(cmount, test_layout_file, xattrk, (void *)xattrv, 5, 0));

  /* invalid layout */
  sprintf(test_layout_file, "test_layout_%d_c", getpid());
  fd = ceph_open_layout(cmount, test_layout_file, O_CREAT, 0666, (1<<20), 1, 19, NULL);
  ASSERT_EQ(fd, -CEPHFS_EINVAL);

  /* with data pool */
  sprintf(test_layout_file, "test_layout_%d_d", getpid());
  fd = ceph_open_layout(cmount, test_layout_file, O_CREAT, 0666, (1<<20), 7, (1<<20), poolname);
  ASSERT_GT(fd, 0);
  ceph_close(cmount, fd);

  /* with metadata pool (invalid) */
  sprintf(test_layout_file, "test_layout_%d_e", getpid());
  fd = ceph_open_layout(cmount, test_layout_file, O_CREAT, 0666, (1<<20), 7, (1<<20), "metadata");
  ASSERT_EQ(fd, -CEPHFS_EINVAL);

  /* with metadata pool (does not exist) */
  sprintf(test_layout_file, "test_layout_%d_f", getpid());
  fd = ceph_open_layout(cmount, test_layout_file, O_CREAT, 0666, (1<<20), 7, (1<<20), "asdfjasdfjasdf");
  ASSERT_EQ(fd, -CEPHFS_EINVAL);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, DirLs) {

  pid_t mypid = getpid();

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  struct ceph_dir_result *ls_dir = NULL;
  char foostr[256];
  sprintf(foostr, "dir_ls%d", mypid);
  ASSERT_EQ(ceph_opendir(cmount, foostr, &ls_dir), -CEPHFS_ENOENT);

  ASSERT_EQ(ceph_mkdir(cmount, foostr, 0777), 0);
  struct ceph_statx stx;
  ASSERT_EQ(ceph_statx(cmount, foostr, &stx, 0, 0), 0);
  ASSERT_NE(S_ISDIR(stx.stx_mode), 0);

  char barstr[256];
  sprintf(barstr, "dir_ls2%d", mypid);
  ASSERT_EQ(ceph_statx(cmount, barstr, &stx, 0, AT_SYMLINK_NOFOLLOW), -CEPHFS_ENOENT);

  // insert files into directory and test open
  char bazstr[256];
  int i = 0, r = rand() % 4096;
  if (getenv("LIBCEPHFS_RAND")) {
    r = atoi(getenv("LIBCEPHFS_RAND"));
  }
  printf("rand: %d\n", r);
  for(; i < r; ++i) {

    sprintf(bazstr, "dir_ls%d/dirf%d", mypid, i);
    int fd  = ceph_open(cmount, bazstr, O_CREAT|O_RDONLY, 0666);
    ASSERT_GT(fd, 0);
    ASSERT_EQ(ceph_close(cmount, fd), 0);

    // set file sizes for readdirplus
    ceph_truncate(cmount, bazstr, i);
  }

  ASSERT_EQ(ceph_opendir(cmount, foostr, &ls_dir), 0);

  // not guaranteed to get . and .. first, but its a safe assumption in this case
  struct dirent *result = ceph_readdir(cmount, ls_dir);
  ASSERT_TRUE(result != NULL);
  ASSERT_STREQ(result->d_name, ".");
  result = ceph_readdir(cmount, ls_dir);
  ASSERT_TRUE(result != NULL);
  ASSERT_STREQ(result->d_name, "..");

  std::vector<std::string> entries;
  std::map<std::string, int64_t> offset_map;
  int64_t offset = ceph_telldir(cmount, ls_dir);
  for(i = 0; i < r; ++i) {
    result = ceph_readdir(cmount, ls_dir);
    ASSERT_TRUE(result != NULL);
    entries.push_back(result->d_name);
    offset_map[result->d_name] = offset;
    offset = ceph_telldir(cmount, ls_dir);
  }

  ASSERT_TRUE(ceph_readdir(cmount, ls_dir) == NULL);
  offset = ceph_telldir(cmount, ls_dir);

  ASSERT_EQ(offset_map.size(), entries.size());
  for(i = 0; i < r; ++i) {
    sprintf(bazstr, "dirf%d", i);
    ASSERT_TRUE(offset_map.count(bazstr) == 1);
  }

  // test seekdir
  ceph_seekdir(cmount, ls_dir, offset);
  ASSERT_TRUE(ceph_readdir(cmount, ls_dir) == NULL);

  for (auto p = offset_map.begin(); p != offset_map.end(); ++p) {
    ceph_seekdir(cmount, ls_dir, p->second);
    result = ceph_readdir(cmount, ls_dir);
    ASSERT_TRUE(result != NULL);
    std::string d_name(result->d_name);
    ASSERT_EQ(p->first, d_name);
  }

  // test rewinddir
  ceph_rewinddir(cmount, ls_dir);

  result = ceph_readdir(cmount, ls_dir);
  ASSERT_TRUE(result != NULL);
  ASSERT_STREQ(result->d_name, ".");
  result = ceph_readdir(cmount, ls_dir);
  ASSERT_TRUE(result != NULL);
  ASSERT_STREQ(result->d_name, "..");

  ceph_rewinddir(cmount, ls_dir);

  int t = ceph_telldir(cmount, ls_dir);
  ASSERT_GT(t, -1);

  ASSERT_TRUE(ceph_readdir(cmount, ls_dir) != NULL);

  // test seekdir - move back to the beginning
  ceph_seekdir(cmount, ls_dir, t);

  // test getdents
  struct dirent *getdents_entries;
  size_t getdents_entries_len = (r + 2) * sizeof(*getdents_entries);
  getdents_entries = (struct dirent *)malloc(getdents_entries_len);

  int count = 0;
  std::vector<std::string> found;
  while (true) {
    int len = ceph_getdents(cmount, ls_dir, (char *)getdents_entries, getdents_entries_len);
    if (len == 0)
      break;
    ASSERT_GT(len, 0);
    ASSERT_TRUE((len % sizeof(*getdents_entries)) == 0);
    int n = len / sizeof(*getdents_entries);
    int j;
    if (count == 0) {
      ASSERT_STREQ(getdents_entries[0].d_name, ".");
      ASSERT_STREQ(getdents_entries[1].d_name, "..");
      j = 2;
    } else {
      j = 0;
    }
    count += n;
    for(; j < n; ++i, ++j) {
      const char *name = getdents_entries[j].d_name;
      found.push_back(name);
    }
  }
  ASSERT_EQ(found, entries);
  free(getdents_entries);

  // test readdir_r
  ceph_rewinddir(cmount, ls_dir);

  result = ceph_readdir(cmount, ls_dir);
  ASSERT_TRUE(result != NULL);
  ASSERT_STREQ(result->d_name, ".");
  result = ceph_readdir(cmount, ls_dir);
  ASSERT_TRUE(result != NULL);
  ASSERT_STREQ(result->d_name, "..");

  found.clear();
  while (true) {
    struct dirent rdent;
    int len = ceph_readdir_r(cmount, ls_dir, &rdent);
    if (len == 0)
      break;
    ASSERT_EQ(len, 1);
    found.push_back(rdent.d_name);
  }
  ASSERT_EQ(found, entries);

  // test readdirplus
  ceph_rewinddir(cmount, ls_dir);

  result = ceph_readdir(cmount, ls_dir);
  ASSERT_TRUE(result != NULL);
  ASSERT_STREQ(result->d_name, ".");
  result = ceph_readdir(cmount, ls_dir);
  ASSERT_TRUE(result != NULL);
  ASSERT_STREQ(result->d_name, "..");

  found.clear();
  while (true) {
    struct dirent rdent;
    struct ceph_statx stx;
    int len = ceph_readdirplus_r(cmount, ls_dir, &rdent, &stx,
				 CEPH_STATX_SIZE, AT_STATX_DONT_SYNC, NULL);
    if (len == 0)
      break;
    ASSERT_EQ(len, 1);
    const char *name = rdent.d_name;
    found.push_back(name);
    int size;
    sscanf(name, "dirf%d", &size);
    ASSERT_TRUE(stx.stx_mask & CEPH_STATX_SIZE);
    ASSERT_EQ(stx.stx_size, (size_t)size);
    // On Windows, dirent uses long (4B) inodes, which get trimmed
    // and can't be used.
    // TODO: consider defining ceph_dirent.
    #ifndef _WIN32
    ASSERT_EQ(stx.stx_ino, rdent.d_ino);
    #endif
    //ASSERT_EQ(st.st_mode, (mode_t)0666);
  }
  ASSERT_EQ(found, entries);

  ASSERT_EQ(ceph_closedir(cmount, ls_dir), 0);

  // cleanup
  for(i = 0; i < r; ++i) {
    sprintf(bazstr, "dir_ls%d/dirf%d", mypid, i);
    ASSERT_EQ(0, ceph_unlink(cmount, bazstr));
  }
  ASSERT_EQ(0, ceph_rmdir(cmount, foostr));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, ManyNestedDirs) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  const char *many_path = "a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a";
  ASSERT_EQ(ceph_mkdirs(cmount, many_path, 0755), 0);

  int i = 0;

  for(; i < 39; ++i) {
    ASSERT_EQ(ceph_chdir(cmount, "a"), 0);

    struct ceph_dir_result *dirp;
    ASSERT_EQ(ceph_opendir(cmount, "a", &dirp), 0);
    struct dirent *dent = ceph_readdir(cmount, dirp);
    ASSERT_TRUE(dent != NULL);
    ASSERT_STREQ(dent->d_name, ".");
    dent = ceph_readdir(cmount, dirp);
    ASSERT_TRUE(dent != NULL);
    ASSERT_STREQ(dent->d_name, "..");
    dent = ceph_readdir(cmount, dirp);
    ASSERT_TRUE(dent != NULL);
    ASSERT_STREQ(dent->d_name, "a");
    ASSERT_EQ(ceph_closedir(cmount, dirp), 0);
  }

  ASSERT_STREQ(ceph_getcwd(cmount), "/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a/a");

  ASSERT_EQ(ceph_chdir(cmount, "a/a/a"), 0);

  for(i = 0; i < 39; ++i) {
    ASSERT_EQ(ceph_chdir(cmount, ".."), 0);
    ASSERT_EQ(ceph_rmdir(cmount, "a"), 0);
  }

  ASSERT_EQ(ceph_chdir(cmount, "/"), 0);

  ASSERT_EQ(ceph_rmdir(cmount, "a/a/a"), 0);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, Xattrs) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char test_xattr_file[256];
  sprintf(test_xattr_file, "test_xattr_%d", getpid());
  int fd = ceph_open(cmount, test_xattr_file, O_CREAT, 0666);
  ASSERT_GT(fd, 0);

  // test removing non-existent xattr
  ASSERT_EQ(-CEPHFS_ENODATA, ceph_removexattr(cmount, test_xattr_file, "user.nosuchxattr"));

  char i = 'a';
  char xattrk[128];
  char xattrv[128];
  for(; i < 'a'+26; ++i) {
    sprintf(xattrk, "user.test_xattr_%c", i);
    int len = sprintf(xattrv, "testxattr%c", i);
    ASSERT_EQ(ceph_setxattr(cmount, test_xattr_file, xattrk, (void *) xattrv, len, XATTR_CREATE), 0);
  }

  // zero size should return required buffer length
  int len_needed = ceph_listxattr(cmount, test_xattr_file, NULL, 0);
  ASSERT_GT(len_needed, 0);

  // buffer size smaller than needed should fail
  char xattrlist[128*26];
  ASSERT_GT(sizeof(xattrlist), (size_t)len_needed);
  int len = ceph_listxattr(cmount, test_xattr_file, xattrlist, len_needed - 1);
  ASSERT_EQ(-CEPHFS_ERANGE, len);

  len = ceph_listxattr(cmount, test_xattr_file, xattrlist, sizeof(xattrlist));
  ASSERT_EQ(len, len_needed);
  char *p = xattrlist;
  char *n;
  i = 'a';
  while (len > 0) {
    // ceph.* xattrs should not be listed
    ASSERT_NE(strncmp(p, "ceph.", 5), 0);

    sprintf(xattrk, "user.test_xattr_%c", i);
    ASSERT_STREQ(p, xattrk);

    char gxattrv[128];
    std::cout << "getting attr " << p << std::endl;
    int alen = ceph_getxattr(cmount, test_xattr_file, p, (void *) gxattrv, 128);
    ASSERT_GT(alen, 0);
    sprintf(xattrv, "testxattr%c", i);
    ASSERT_TRUE(!strncmp(xattrv, gxattrv, alen));

    n = strchr(p, '\0');
    n++;
    len -= (n - p);
    p = n;
    ++i;
  }

  i = 'a';
  for(i = 'a'; i < 'a'+26; ++i) {
    sprintf(xattrk, "user.test_xattr_%c", i);
    ASSERT_EQ(ceph_removexattr(cmount, test_xattr_file, xattrk), 0);
  }

  ceph_close(cmount, fd);
  ceph_shutdown(cmount);

}

TEST(LibCephFS, Xattrs_ll) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char test_xattr_file[256];
  sprintf(test_xattr_file, "test_xattr_%d", getpid());
  int fd = ceph_open(cmount, test_xattr_file, O_CREAT, 0666);
  ASSERT_GT(fd, 0);
  ceph_close(cmount, fd);

  Inode *root = NULL;
  Inode *existent_file_handle = NULL;

  int res = ceph_ll_lookup_root(cmount, &root);
  ASSERT_EQ(res, 0);

  UserPerm *perms = ceph_mount_perms(cmount);
  struct ceph_statx stx;

  res = ceph_ll_lookup(cmount, root, test_xattr_file, &existent_file_handle,
		       &stx, 0, 0, perms);
  ASSERT_EQ(res, 0);

  const char *valid_name = "user.attrname";
  const char *value = "attrvalue";
  char value_buf[256] = { 0 };

  res = ceph_ll_setxattr(cmount, existent_file_handle, valid_name, value, strlen(value), 0, perms);
  ASSERT_EQ(res, 0);

  res = ceph_ll_getxattr(cmount, existent_file_handle, valid_name, value_buf, 256, perms);
  ASSERT_EQ(res, (int)strlen(value));

  value_buf[res] = '\0';
  ASSERT_STREQ(value_buf, value);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, LstatSlashdot) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  struct ceph_statx stx;
  ASSERT_EQ(ceph_statx(cmount, "/.", &stx, 0, AT_SYMLINK_NOFOLLOW), 0);
  ASSERT_EQ(ceph_statx(cmount, ".", &stx, 0, AT_SYMLINK_NOFOLLOW), 0);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, StatDirNlink) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char test_dir1[256];
  sprintf(test_dir1, "dir1_symlinks_%d", getpid());
  ASSERT_EQ(ceph_mkdir(cmount, test_dir1, 0700), 0);

  int fd = ceph_open(cmount, test_dir1, O_DIRECTORY|O_RDONLY, 0);
  ASSERT_GT(fd, 0);
  struct ceph_statx stx;
  ASSERT_EQ(ceph_fstatx(cmount, fd, &stx, CEPH_STATX_NLINK, AT_SYMLINK_NOFOLLOW), 0);
  ASSERT_EQ(stx.stx_nlink, 2u);

  {
    char test_dir2[296];
    sprintf(test_dir2, "%s/.", test_dir1);
    ASSERT_EQ(ceph_statx(cmount, test_dir2, &stx, CEPH_STATX_NLINK, AT_SYMLINK_NOFOLLOW), 0);
    ASSERT_EQ(stx.stx_nlink, 2u);
  }

  {
    char test_dir2[296];
    sprintf(test_dir2, "%s/1", test_dir1);
    ASSERT_EQ(ceph_mkdir(cmount, test_dir2, 0700), 0);
    ASSERT_EQ(ceph_statx(cmount, test_dir2, &stx, CEPH_STATX_NLINK, AT_SYMLINK_NOFOLLOW), 0);
    ASSERT_EQ(stx.stx_nlink, 2u);
      ASSERT_EQ(ceph_statx(cmount, test_dir1, &stx, CEPH_STATX_NLINK, AT_SYMLINK_NOFOLLOW), 0);
      ASSERT_EQ(stx.stx_nlink, 3u);
    sprintf(test_dir2, "%s/2", test_dir1);
    ASSERT_EQ(ceph_mkdir(cmount, test_dir2, 0700), 0);
      ASSERT_EQ(ceph_statx(cmount, test_dir1, &stx, CEPH_STATX_NLINK, AT_SYMLINK_NOFOLLOW), 0);
      ASSERT_EQ(stx.stx_nlink, 4u);
    sprintf(test_dir2, "%s/1/1", test_dir1);
    ASSERT_EQ(ceph_mkdir(cmount, test_dir2, 0700), 0);
      ASSERT_EQ(ceph_statx(cmount, test_dir1, &stx, CEPH_STATX_NLINK, AT_SYMLINK_NOFOLLOW), 0);
      ASSERT_EQ(stx.stx_nlink, 4u);
    ASSERT_EQ(ceph_rmdir(cmount, test_dir2), 0);
      ASSERT_EQ(ceph_statx(cmount, test_dir1, &stx, CEPH_STATX_NLINK, AT_SYMLINK_NOFOLLOW), 0);
      ASSERT_EQ(stx.stx_nlink, 4u);
    sprintf(test_dir2, "%s/1", test_dir1);
    ASSERT_EQ(ceph_rmdir(cmount, test_dir2), 0);
      ASSERT_EQ(ceph_statx(cmount, test_dir1, &stx, CEPH_STATX_NLINK, AT_SYMLINK_NOFOLLOW), 0);
      ASSERT_EQ(stx.stx_nlink, 3u);
    sprintf(test_dir2, "%s/2", test_dir1);
    ASSERT_EQ(ceph_rmdir(cmount, test_dir2), 0);
      ASSERT_EQ(ceph_statx(cmount, test_dir1, &stx, CEPH_STATX_NLINK, AT_SYMLINK_NOFOLLOW), 0);
      ASSERT_EQ(stx.stx_nlink, 2u);
  }

  ASSERT_EQ(ceph_rmdir(cmount, test_dir1), 0);
  ASSERT_EQ(ceph_fstatx(cmount, fd, &stx, CEPH_STATX_NLINK, AT_SYMLINK_NOFOLLOW), 0);
  ASSERT_EQ(stx.stx_nlink, 0u);

  ceph_close(cmount, fd);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, DoubleChmod) {

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char test_file[256];
  sprintf(test_file, "test_perms_%d", getpid());

  int fd = ceph_open(cmount, test_file, O_CREAT|O_RDWR, 0666);
  ASSERT_GT(fd, 0);

  // write some stuff
  const char *bytes = "foobarbaz";
  ASSERT_EQ(ceph_write(cmount, fd, bytes, strlen(bytes), 0), (int)strlen(bytes));

  ceph_close(cmount, fd);

  // set perms to read but can't write
  ASSERT_EQ(ceph_chmod(cmount, test_file, 0400), 0);

  fd = ceph_open(cmount, test_file, O_RDWR, 0);
  ASSERT_EQ(fd, -CEPHFS_EACCES);

  fd = ceph_open(cmount, test_file, O_RDONLY, 0);
  ASSERT_GT(fd, -1);

  char buf[100];
  int ret = ceph_read(cmount, fd, buf, 100, 0);
  ASSERT_EQ(ret, (int)strlen(bytes));
  buf[ret] = '\0';
  ASSERT_STREQ(buf, bytes);

  ASSERT_EQ(ceph_write(cmount, fd, bytes, strlen(bytes), 0), -CEPHFS_EBADF);

  ceph_close(cmount, fd);

  // reset back to writeable
  ASSERT_EQ(ceph_chmod(cmount, test_file, 0600), 0);

  // ensure perms are correct
  struct ceph_statx stx;
  ASSERT_EQ(ceph_statx(cmount, test_file, &stx, CEPH_STATX_MODE, AT_SYMLINK_NOFOLLOW), 0);
  ASSERT_EQ(stx.stx_mode, 0100600U);

  fd = ceph_open(cmount, test_file, O_RDWR, 0);
  ASSERT_GT(fd, 0);

  ASSERT_EQ(ceph_write(cmount, fd, bytes, strlen(bytes), 0), (int)strlen(bytes));
  ceph_close(cmount, fd);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, Fchmod) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char test_file[256];
  sprintf(test_file, "test_perms_%d", getpid());

  int fd = ceph_open(cmount, test_file, O_CREAT|O_RDWR, 0666);
  ASSERT_GT(fd, 0);

  // write some stuff
  const char *bytes = "foobarbaz";
  ASSERT_EQ(ceph_write(cmount, fd, bytes, strlen(bytes), 0), (int)strlen(bytes));

  // set perms to read but can't write
  ASSERT_EQ(ceph_fchmod(cmount, fd, 0400), 0);

  char buf[100];
  int ret = ceph_read(cmount, fd, buf, 100, 0);
  ASSERT_EQ(ret, (int)strlen(bytes));
  buf[ret] = '\0';
  ASSERT_STREQ(buf, bytes);

  ASSERT_EQ(ceph_write(cmount, fd, bytes, strlen(bytes), 0), (int)strlen(bytes));

  ceph_close(cmount, fd);

  ASSERT_EQ(ceph_open(cmount, test_file, O_RDWR, 0), -CEPHFS_EACCES);

  // reset back to writeable
  ASSERT_EQ(ceph_chmod(cmount, test_file, 0600), 0);

  fd = ceph_open(cmount, test_file, O_RDWR, 0);
  ASSERT_GT(fd, 0);

  ASSERT_EQ(ceph_write(cmount, fd, bytes, strlen(bytes), 0), (int)strlen(bytes));
  ceph_close(cmount, fd);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, Lchmod) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char test_file[256];
  sprintf(test_file, "test_perms_lchmod_%d", getpid());

  int fd = ceph_open(cmount, test_file, O_CREAT|O_RDWR, 0666);
  ASSERT_GT(fd, 0);

  // write some stuff
  const char *bytes = "foobarbaz";
  ASSERT_EQ(ceph_write(cmount, fd, bytes, strlen(bytes), 0), (int)strlen(bytes));
  ceph_close(cmount, fd);

  // Create symlink
  char test_symlink[256];
  sprintf(test_symlink, "test_lchmod_sym_%d", getpid());
  ASSERT_EQ(ceph_symlink(cmount, test_file, test_symlink), 0);

  // get symlink stat - lstat
  struct ceph_statx stx_orig1;
  ASSERT_EQ(ceph_statx(cmount, test_symlink, &stx_orig1, CEPH_STATX_ALL_STATS, AT_SYMLINK_NOFOLLOW), 0);

  // Change mode on symlink file
  ASSERT_EQ(ceph_lchmod(cmount, test_symlink, 0400), 0);
  struct ceph_statx stx_orig2;
  ASSERT_EQ(ceph_statx(cmount, test_symlink, &stx_orig2, CEPH_STATX_ALL_STATS, AT_SYMLINK_NOFOLLOW), 0);

  // Compare modes
  ASSERT_NE(stx_orig1.stx_mode, stx_orig2.stx_mode);
  static const int permbits = S_IRWXU|S_IRWXG|S_IRWXO;
  ASSERT_EQ(permbits&stx_orig1.stx_mode, 0777);
  ASSERT_EQ(permbits&stx_orig2.stx_mode, 0400);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, Fchown) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char test_file[256];
  sprintf(test_file, "test_fchown_%d", getpid());

  int fd = ceph_open(cmount, test_file, O_CREAT|O_RDWR, 0666);
  ASSERT_GT(fd, 0);

  // set perms to readable and writeable only by owner
  ASSERT_EQ(ceph_fchmod(cmount, fd, 0600), 0);

  // change ownership to nobody -- we assume nobody exists and id is always 65534
  ASSERT_EQ(ceph_conf_set(cmount, "client_permissions", "0"), 0);
  ASSERT_EQ(ceph_fchown(cmount, fd, 65534, 65534), 0);
  ASSERT_EQ(ceph_conf_set(cmount, "client_permissions", "1"), 0);

  ceph_close(cmount, fd);

  // "nobody" will be ignored on Windows
  #ifndef _WIN32
  fd = ceph_open(cmount, test_file, O_RDWR, 0);
  ASSERT_EQ(fd, -CEPHFS_EACCES);
  #endif

  ceph_shutdown(cmount);
}

#if defined(__linux__) && defined(O_PATH)
TEST(LibCephFS, FlagO_PATH) {
  struct ceph_mount_info *cmount;

  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, NULL));

  char test_file[PATH_MAX];
  sprintf(test_file, "test_oflag_%d", getpid());

  int fd = ceph_open(cmount, test_file, O_CREAT|O_RDWR|O_PATH, 0666);
  ASSERT_EQ(-CEPHFS_ENOENT, fd);

  fd = ceph_open(cmount, test_file, O_CREAT|O_RDWR, 0666);
  ASSERT_GT(fd, 0);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  // ok, the file has been created. perform real checks now
  fd = ceph_open(cmount, test_file, O_CREAT|O_RDWR|O_PATH, 0666);
  ASSERT_GT(fd, 0);

  char buf[128];
  ASSERT_EQ(-CEPHFS_EBADF, ceph_read(cmount, fd, buf, sizeof(buf), 0));
  ASSERT_EQ(-CEPHFS_EBADF, ceph_write(cmount, fd, buf, sizeof(buf), 0));

  // set perms to readable and writeable only by owner
  ASSERT_EQ(-CEPHFS_EBADF, ceph_fchmod(cmount, fd, 0600));

  // change ownership to nobody -- we assume nobody exists and id is always 65534
  ASSERT_EQ(-CEPHFS_EBADF, ceph_fchown(cmount, fd, 65534, 65534));

  // try to sync
  ASSERT_EQ(-CEPHFS_EBADF, ceph_fsync(cmount, fd, false));

  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_fstatx(cmount, fd, &stx, 0, 0));

  ASSERT_EQ(0, ceph_close(cmount, fd));
  ceph_shutdown(cmount);
}
#endif /* __linux */

TEST(LibCephFS, Symlinks) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char test_file[256];
  sprintf(test_file, "test_symlinks_%d", getpid());

  int fd = ceph_open(cmount, test_file, O_CREAT|O_RDWR, 0666);
  ASSERT_GT(fd, 0);

  ceph_close(cmount, fd);

  char test_symlink[256];
  sprintf(test_symlink, "test_symlinks_sym_%d", getpid());

  ASSERT_EQ(ceph_symlink(cmount, test_file, test_symlink), 0);

  // test the O_NOFOLLOW case
  fd = ceph_open(cmount, test_symlink, O_NOFOLLOW, 0);
  ASSERT_EQ(fd, -CEPHFS_ELOOP);

  // stat the original file
  struct ceph_statx stx_orig;
  ASSERT_EQ(ceph_statx(cmount, test_file, &stx_orig, CEPH_STATX_ALL_STATS, 0), 0);
  // stat the symlink
  struct ceph_statx stx_symlink_orig;
  ASSERT_EQ(ceph_statx(cmount, test_symlink, &stx_symlink_orig, CEPH_STATX_ALL_STATS, 0), 0);
  // ensure the statx bufs are equal
  ASSERT_EQ(memcmp(&stx_orig, &stx_symlink_orig, sizeof(stx_orig)), 0);

  sprintf(test_file, "/test_symlinks_abs_%d", getpid());

  fd = ceph_open(cmount, test_file, O_CREAT|O_RDWR, 0666);
  ASSERT_GT(fd, 0);

  ceph_close(cmount, fd);

  sprintf(test_symlink, "/test_symlinks_abs_sym_%d", getpid());

  ASSERT_EQ(ceph_symlink(cmount, test_file, test_symlink), 0);

  // stat the original file
  ASSERT_EQ(ceph_statx(cmount, test_file, &stx_orig, CEPH_STATX_ALL_STATS, 0), 0);
  // stat the symlink
  ASSERT_EQ(ceph_statx(cmount, test_symlink, &stx_symlink_orig, CEPH_STATX_ALL_STATS, 0), 0);
  // ensure the statx bufs are equal
  ASSERT_TRUE(!memcmp(&stx_orig, &stx_symlink_orig, sizeof(stx_orig)));

  // test lstat
  ASSERT_EQ(ceph_statx(cmount, test_symlink, &stx_orig, CEPH_STATX_ALL_STATS, AT_SYMLINK_NOFOLLOW), 0);
  ASSERT_TRUE(S_ISLNK(stx_orig.stx_mode));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, DirSyms) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char test_dir1[256];
  sprintf(test_dir1, "dir1_symlinks_%d", getpid());

  ASSERT_EQ(ceph_mkdir(cmount, test_dir1, 0700), 0);

  char test_symdir[256];
  sprintf(test_symdir, "symdir_symlinks_%d", getpid());

  ASSERT_EQ(ceph_symlink(cmount, test_dir1, test_symdir), 0);

  char test_file[256];
  sprintf(test_file, "/symdir_symlinks_%d/test_symdir_file", getpid());
  int fd = ceph_open(cmount, test_file, O_CREAT|O_RDWR, 0600);
  ASSERT_GT(fd, 0);
  ceph_close(cmount, fd);

  struct ceph_statx stx;
  ASSERT_EQ(ceph_statx(cmount, test_file, &stx, 0, AT_SYMLINK_NOFOLLOW), 0);

  // ensure that its a file not a directory we get back
  ASSERT_TRUE(S_ISREG(stx.stx_mode));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, LoopSyms) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char test_dir1[256];
  sprintf(test_dir1, "dir1_loopsym_%d", getpid());

  ASSERT_EQ(ceph_mkdir(cmount, test_dir1, 0700), 0);

  char test_dir2[256];
  sprintf(test_dir2, "/dir1_loopsym_%d/loop_dir", getpid());

  ASSERT_EQ(ceph_mkdir(cmount, test_dir2, 0700), 0);

  // symlink it itself:  /path/to/mysym -> /path/to/mysym
  char test_symdir[256];
  sprintf(test_symdir, "/dir1_loopsym_%d/loop_dir/symdir", getpid());

  ASSERT_EQ(ceph_symlink(cmount, test_symdir, test_symdir), 0);

  char test_file[256];
  sprintf(test_file, "/dir1_loopsym_%d/loop_dir/symdir/test_loopsym_file", getpid());
  int fd = ceph_open(cmount, test_file, O_CREAT|O_RDWR, 0600);
  ASSERT_EQ(fd, -CEPHFS_ELOOP);

  // loop: /a -> /b, /b -> /c, /c -> /a
  char a[264], b[264], c[264];
  sprintf(a, "/%s/a", test_dir1);
  sprintf(b, "/%s/b", test_dir1);
  sprintf(c, "/%s/c", test_dir1);
  ASSERT_EQ(ceph_symlink(cmount, a, b), 0);
  ASSERT_EQ(ceph_symlink(cmount, b, c), 0);
  ASSERT_EQ(ceph_symlink(cmount, c, a), 0);
  ASSERT_EQ(ceph_open(cmount, a, O_RDWR, 0), -CEPHFS_ELOOP);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, HardlinkNoOriginal) {

  int mypid = getpid();

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char dir[256];
  sprintf(dir, "/test_rmdirfail%d", mypid);
  ASSERT_EQ(ceph_mkdir(cmount, dir, 0777), 0);

  ASSERT_EQ(ceph_chdir(cmount, dir), 0);

  int fd = ceph_open(cmount, "f1", O_CREAT, 0644);
  ASSERT_GT(fd, 0);

  ceph_close(cmount, fd);

  // create hard link
  ASSERT_EQ(ceph_link(cmount, "f1", "hardl1"), 0);

  // remove file link points to
  ASSERT_EQ(ceph_unlink(cmount, "f1"), 0);

  ceph_shutdown(cmount);

  // now cleanup
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);
  ASSERT_EQ(ceph_chdir(cmount, dir), 0);
  ASSERT_EQ(ceph_unlink(cmount, "hardl1"), 0);
  ASSERT_EQ(ceph_rmdir(cmount, dir), 0);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, BadArgument) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  int fd = ceph_open(cmount, "test_file", O_CREAT|O_RDWR, 0666);
  ASSERT_GT(fd, 0);
  char buf[100];
  ASSERT_EQ(ceph_write(cmount, fd, buf, sizeof(buf), 0), (int)sizeof(buf));
  ASSERT_EQ(ceph_read(cmount, fd, buf, 0, 5), 0);
  ceph_close(cmount, fd);
  ASSERT_EQ(ceph_unlink(cmount, "test_file"), 0);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, BadFileDesc) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  ASSERT_EQ(ceph_fchmod(cmount, -1, 0655), -CEPHFS_EBADF);
  ASSERT_EQ(ceph_close(cmount, -1), -CEPHFS_EBADF);
  ASSERT_EQ(ceph_lseek(cmount, -1, 0, SEEK_SET), -CEPHFS_EBADF);

  char buf[0];
  ASSERT_EQ(ceph_read(cmount, -1, buf, 0, 0), -CEPHFS_EBADF);
  ASSERT_EQ(ceph_write(cmount, -1, buf, 0, 0), -CEPHFS_EBADF);

  ASSERT_EQ(ceph_ftruncate(cmount, -1, 0), -CEPHFS_EBADF);
  ASSERT_EQ(ceph_fsync(cmount, -1, 0), -CEPHFS_EBADF);

  struct ceph_statx stx;
  ASSERT_EQ(ceph_fstatx(cmount, -1, &stx, 0, 0), -CEPHFS_EBADF);

  struct sockaddr_storage addr;
  ASSERT_EQ(ceph_get_file_stripe_address(cmount, -1, 0, &addr, 1), -CEPHFS_EBADF);

  ASSERT_EQ(ceph_get_file_stripe_unit(cmount, -1), -CEPHFS_EBADF);
  ASSERT_EQ(ceph_get_file_pool(cmount, -1), -CEPHFS_EBADF);
  char poolname[80];
  ASSERT_EQ(ceph_get_file_pool_name(cmount, -1, poolname, sizeof(poolname)), -CEPHFS_EBADF);
  ASSERT_EQ(ceph_get_file_replication(cmount, -1), -CEPHFS_EBADF);
  ASSERT_EQ(ceph_get_file_object_size(cmount, -1), -CEPHFS_EBADF);
  int stripe_unit, stripe_count, object_size, pg_pool;
  ASSERT_EQ(ceph_get_file_layout(cmount, -1, &stripe_unit, &stripe_count, &object_size, &pg_pool), -CEPHFS_EBADF);
  ASSERT_EQ(ceph_get_file_stripe_count(cmount, -1), -CEPHFS_EBADF);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, ReadEmptyFile) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  // test the read_sync path in the client for zero files
  ASSERT_EQ(ceph_conf_set(cmount, "client_debug_force_sync_read", "true"), 0);

  int mypid = getpid();
  char testf[256];

  sprintf(testf, "test_reademptyfile%d", mypid);
  int fd = ceph_open(cmount, testf, O_CREAT|O_TRUNC|O_WRONLY, 0644);
  ASSERT_GT(fd, 0);

  ceph_close(cmount, fd);

  fd = ceph_open(cmount, testf, O_RDONLY, 0);
  ASSERT_GT(fd, 0);

  char buf[4096];
  ASSERT_EQ(ceph_read(cmount, fd, buf, 4096, 0), 0);

  ceph_close(cmount, fd);
  ceph_shutdown(cmount);
}

TEST(LibCephFS, PreadvPwritev) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  int mypid = getpid();
  char testf[256];

  sprintf(testf, "test_preadvpwritevfile%d", mypid);
  int fd = ceph_open(cmount, testf, O_CREAT|O_RDWR, 0666);
  ASSERT_GT(fd, 0);

  char out0[] = "hello ";
  char out1[] = "world\n";
  struct iovec iov_out[2] = {
	{out0, sizeof(out0)},
	{out1, sizeof(out1)},
  };
  char in0[sizeof(out0)];
  char in1[sizeof(out1)];
  struct iovec iov_in[2] = {
	{in0, sizeof(in0)},
	{in1, sizeof(in1)},
  };
  ssize_t nwritten = iov_out[0].iov_len + iov_out[1].iov_len; 
  ssize_t nread = iov_in[0].iov_len + iov_in[1].iov_len; 

  ASSERT_EQ(ceph_pwritev(cmount, fd, iov_out, 2, 0), nwritten);
  ASSERT_EQ(ceph_preadv(cmount, fd, iov_in, 2, 0), nread);
  ASSERT_EQ(0, strncmp((const char*)iov_in[0].iov_base, (const char*)iov_out[0].iov_base, iov_out[0].iov_len));
  ASSERT_EQ(0, strncmp((const char*)iov_in[1].iov_base, (const char*)iov_out[1].iov_base, iov_out[1].iov_len));

  ceph_close(cmount, fd);
  ceph_shutdown(cmount);
}

TEST(LibCephFS, LlreadvLlwritev) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  int mypid = getpid();
  char filename[256];

  sprintf(filename, "test_llreadvllwritevfile%u", mypid);

  Inode *root, *file;
  ASSERT_EQ(ceph_ll_lookup_root(cmount, &root), 0);

  Fh *fh;
  struct ceph_statx stx;
  UserPerm *perms = ceph_mount_perms(cmount);

  ASSERT_EQ(ceph_ll_create(cmount, root, filename, 0666,
		    O_RDWR|O_CREAT|O_TRUNC, &file, &fh, &stx, 0, 0, perms), 0);

  /* Reopen read-only */
  char out0[] = "hello ";
  char out1[] = "world\n";
  struct iovec iov_out[2] = {
	{out0, sizeof(out0)},
	{out1, sizeof(out1)},
  };
  char in0[sizeof(out0)];
  char in1[sizeof(out1)];
  struct iovec iov_in[2] = {
	{in0, sizeof(in0)},
	{in1, sizeof(in1)},
  };
  ssize_t nwritten = iov_out[0].iov_len + iov_out[1].iov_len;
  ssize_t nread = iov_in[0].iov_len + iov_in[1].iov_len;

  ASSERT_EQ(ceph_ll_writev(cmount, fh, iov_out, 2, 0), nwritten);
  ASSERT_EQ(ceph_ll_readv(cmount, fh, iov_in, 2, 0), nread);
  ASSERT_EQ(0, strncmp((const char*)iov_in[0].iov_base, (const char*)iov_out[0].iov_base, iov_out[0].iov_len));
  ASSERT_EQ(0, strncmp((const char*)iov_in[1].iov_base, (const char*)iov_out[1].iov_base, iov_out[1].iov_len));

  ceph_ll_close(cmount, fh);
  ceph_shutdown(cmount);
}

TEST(LibCephFS, StripeUnitGran) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);
  ASSERT_GT(ceph_get_stripe_unit_granularity(cmount), 0);
  ceph_shutdown(cmount);
}

TEST(LibCephFS, Rename) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  int mypid = getpid();
  char path_src[256];
  char path_dst[256];

  /* make a source file */
  sprintf(path_src, "test_rename_src%d", mypid);
  int fd = ceph_open(cmount, path_src, O_CREAT|O_TRUNC|O_WRONLY, 0777);
  ASSERT_GT(fd, 0);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  /* rename to a new dest path */
  sprintf(path_dst, "test_rename_dst%d", mypid);
  ASSERT_EQ(0, ceph_rename(cmount, path_src, path_dst));

  /* test that dest path exists */
  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_statx(cmount, path_dst, &stx, 0, 0));

  /* test that src path doesn't exist */
  ASSERT_EQ(-CEPHFS_ENOENT, ceph_statx(cmount, path_src, &stx, 0, AT_SYMLINK_NOFOLLOW));

  /* rename with non-existent source path */
  ASSERT_EQ(-CEPHFS_ENOENT, ceph_rename(cmount, path_src, path_dst));

  ASSERT_EQ(0, ceph_unlink(cmount, path_dst));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, UseUnmounted) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));

  struct statvfs stvfs;
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_statfs(cmount, "/", &stvfs));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_get_local_osd(cmount));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_chdir(cmount, "/"));

  struct ceph_dir_result *dirp;
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_opendir(cmount, "/", &dirp));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_closedir(cmount, dirp));

  ceph_readdir(cmount, dirp);
  EXPECT_EQ(CEPHFS_ENOTCONN, errno);

  struct dirent rdent;
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_readdir_r(cmount, dirp, &rdent));

  struct ceph_statx stx;
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_readdirplus_r(cmount, dirp, &rdent, &stx, 0, 0, NULL));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_getdents(cmount, dirp, NULL, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_getdnames(cmount, dirp, NULL, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_telldir(cmount, dirp));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_link(cmount, "/", "/link"));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_unlink(cmount, "/path"));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_rename(cmount, "/path", "/path"));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_mkdir(cmount, "/", 0655));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_mkdirs(cmount, "/", 0655));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_rmdir(cmount, "/path"));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_readlink(cmount, "/path", NULL, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_symlink(cmount, "/path", "/path"));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_statx(cmount, "/path", &stx, 0, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_setattrx(cmount, "/path", &stx, 0, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_getxattr(cmount, "/path", "name", NULL, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_lgetxattr(cmount, "/path", "name", NULL, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_listxattr(cmount, "/path", NULL, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_llistxattr(cmount, "/path", NULL, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_removexattr(cmount, "/path", "name"));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_lremovexattr(cmount, "/path", "name"));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_setxattr(cmount, "/path", "name", NULL, 0, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_lsetxattr(cmount, "/path", "name", NULL, 0, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_fsetattrx(cmount, 0, &stx, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_chmod(cmount, "/path", 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_fchmod(cmount, 0, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_chown(cmount, "/path", 0, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_lchown(cmount, "/path", 0, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_fchown(cmount, 0, 0, 0));

  struct utimbuf utb;
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_utime(cmount, "/path", &utb));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_truncate(cmount, "/path", 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_mknod(cmount, "/path", 0, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_open(cmount, "/path", 0, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_open_layout(cmount, "/path", 0, 0, 0, 0, 0, "pool"));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_close(cmount, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_lseek(cmount, 0, 0, SEEK_SET));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_read(cmount, 0, NULL, 0, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_write(cmount, 0, NULL, 0, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_ftruncate(cmount, 0, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_fsync(cmount, 0, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_fstatx(cmount, 0, &stx, 0, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_sync_fs(cmount));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_get_file_stripe_unit(cmount, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_get_file_stripe_count(cmount, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_get_file_layout(cmount, 0, NULL, NULL ,NULL ,NULL));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_get_file_object_size(cmount, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_get_file_pool(cmount, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_get_file_pool_name(cmount, 0, NULL, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_get_file_replication(cmount, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_get_path_replication(cmount, "/path"));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_get_path_layout(cmount, "/path", NULL, NULL, NULL, NULL));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_get_path_object_size(cmount, "/path"));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_get_path_stripe_count(cmount, "/path"));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_get_path_stripe_unit(cmount, "/path"));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_get_path_pool(cmount, "/path"));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_get_path_pool_name(cmount, "/path", NULL, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_get_pool_name(cmount, 0, NULL, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_get_file_stripe_address(cmount, 0, 0, NULL, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_localize_reads(cmount, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_debug_get_fd_caps(cmount, 0));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_debug_get_file_caps(cmount, "/path"));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_get_stripe_unit_granularity(cmount));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_get_pool_id(cmount, "data"));
  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_get_pool_replication(cmount, 1));

  ceph_release(cmount);
}

TEST(LibCephFS, GetPoolId) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char name[80];
  memset(name, 0, sizeof(name));
  ASSERT_LE(0, ceph_get_path_pool_name(cmount, "/", name, sizeof(name)));
  ASSERT_GE(ceph_get_pool_id(cmount, name), 0);
  ASSERT_EQ(ceph_get_pool_id(cmount, "weflkjwelfjwlkejf"), -CEPHFS_ENOENT);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, GetPoolReplication) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  /* negative pools */
  ASSERT_EQ(ceph_get_pool_replication(cmount, -10), -CEPHFS_ENOENT);

  /* valid pool */
  int pool_id;
  int stripe_unit, stripe_count, object_size;
  ASSERT_EQ(0, ceph_get_path_layout(cmount, "/", &stripe_unit, &stripe_count,
				    &object_size, &pool_id));
  ASSERT_GE(pool_id, 0);
  ASSERT_GT(ceph_get_pool_replication(cmount, pool_id), 0);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, GetExtentOsds) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);

  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_get_file_extent_osds(cmount, 0, 0, NULL, NULL, 0));

  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  int stripe_unit = (1<<18);

  /* make a file! */
  char test_file[256];
  sprintf(test_file, "test_extent_osds_%d", getpid());
  int fd = ceph_open_layout(cmount, test_file, O_CREAT|O_RDWR, 0666,
      stripe_unit, 2, stripe_unit*2, NULL);
  ASSERT_GT(fd, 0);

  /* get back how many osds > 0 */
  int ret = ceph_get_file_extent_osds(cmount, fd, 0, NULL, NULL, 0);
  EXPECT_GT(ret, 0);

  int64_t len;
  int osds[ret];

  /* full stripe extent */
  EXPECT_EQ(ret, ceph_get_file_extent_osds(cmount, fd, 0, &len, osds, ret));
  EXPECT_EQ(len, (int64_t)stripe_unit);

  /* half stripe extent */
  EXPECT_EQ(ret, ceph_get_file_extent_osds(cmount, fd, stripe_unit/2, &len, osds, ret));
  EXPECT_EQ(len, (int64_t)stripe_unit/2);

  /* 1.5 stripe unit offset -1 byte */
  EXPECT_EQ(ret, ceph_get_file_extent_osds(cmount, fd, 3*stripe_unit/2-1, &len, osds, ret));
  EXPECT_EQ(len, (int64_t)stripe_unit/2+1);

  /* 1.5 stripe unit offset +1 byte */
  EXPECT_EQ(ret, ceph_get_file_extent_osds(cmount, fd, 3*stripe_unit/2+1, &len, osds, ret));
  EXPECT_EQ(len, (int64_t)stripe_unit/2-1);

  /* only when more than 1 osd */
  if (ret > 1) {
    EXPECT_EQ(-CEPHFS_ERANGE, ceph_get_file_extent_osds(cmount, fd, 0, NULL, osds, 1));
  }

  ceph_close(cmount, fd);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, GetOsdCrushLocation) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);

  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_get_osd_crush_location(cmount, 0, NULL, 0));

  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  ASSERT_EQ(ceph_get_osd_crush_location(cmount, 0, NULL, 1), -CEPHFS_EINVAL);

  char path[256];
  ASSERT_EQ(ceph_get_osd_crush_location(cmount, 9999999, path, 0), -CEPHFS_ENOENT);
  ASSERT_EQ(ceph_get_osd_crush_location(cmount, -1, path, 0), -CEPHFS_EINVAL);

  char test_file[256];
  sprintf(test_file, "test_osds_loc_%d", getpid());
  int fd = ceph_open(cmount, test_file, O_CREAT|O_RDWR, 0666);
  ASSERT_GT(fd, 0);

  /* get back how many osds > 0 */
  int ret = ceph_get_file_extent_osds(cmount, fd, 0, NULL, NULL, 0);
  EXPECT_GT(ret, 0);

  /* full stripe extent */
  int osds[ret];
  EXPECT_EQ(ret, ceph_get_file_extent_osds(cmount, fd, 0, NULL, osds, ret));

  ASSERT_GT(ceph_get_osd_crush_location(cmount, 0, path, 0), 0);
  ASSERT_EQ(ceph_get_osd_crush_location(cmount, 0, path, 1), -CEPHFS_ERANGE);

  for (int i = 0; i < ret; i++) {
    int len = ceph_get_osd_crush_location(cmount, osds[i], path, sizeof(path));
    ASSERT_GT(len, 0);
    int pos = 0;
    while (pos < len) {
      std::string type(path + pos);
      ASSERT_GT((int)type.size(), 0);
      pos += type.size() + 1;

      std::string name(path + pos);
      ASSERT_GT((int)name.size(), 0);
      pos += name.size() + 1;
    }
  }

  ceph_close(cmount, fd);
  ceph_shutdown(cmount);
}

TEST(LibCephFS, GetOsdAddr) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);

  EXPECT_EQ(-CEPHFS_ENOTCONN, ceph_get_osd_addr(cmount, 0, NULL));

  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  ASSERT_EQ(-CEPHFS_EINVAL, ceph_get_osd_addr(cmount, 0, NULL));

  struct sockaddr_storage addr;
  ASSERT_EQ(-CEPHFS_ENOENT, ceph_get_osd_addr(cmount, -1, &addr));
  ASSERT_EQ(-CEPHFS_ENOENT, ceph_get_osd_addr(cmount, 9999999, &addr));

  ASSERT_EQ(0, ceph_get_osd_addr(cmount, 0, &addr));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, OpenNoClose) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  pid_t mypid = getpid();
  char str_buf[256];
  sprintf(str_buf, "open_no_close_dir%d", mypid);
  ASSERT_EQ(0, ceph_mkdirs(cmount, str_buf, 0777));

  struct ceph_dir_result *ls_dir = NULL;
  ASSERT_EQ(ceph_opendir(cmount, str_buf, &ls_dir), 0);

  sprintf(str_buf, "open_no_close_file%d", mypid);
  int fd = ceph_open(cmount, str_buf, O_RDONLY|O_CREAT, 0666);
  ASSERT_LT(0, fd);

  // shutdown should force close opened file/dir
  ceph_shutdown(cmount);
}

TEST(LibCephFS, Nlink) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  Inode *root, *dir, *file;

  ASSERT_EQ(ceph_ll_lookup_root(cmount, &root), 0);

  char dirname[32], filename[32], linkname[32];
  sprintf(dirname, "nlinkdir%x", getpid());
  sprintf(filename, "nlinkorig%x", getpid());
  sprintf(linkname, "nlinklink%x", getpid());

  struct ceph_statx stx;
  Fh *fh;
  UserPerm *perms = ceph_mount_perms(cmount);

  ASSERT_EQ(ceph_ll_mkdir(cmount, root, dirname, 0755, &dir, &stx, 0, 0, perms), 0);
  ASSERT_EQ(ceph_ll_create(cmount, dir, filename, 0666, O_RDWR|O_CREAT|O_EXCL,
			   &file, &fh, &stx, CEPH_STATX_NLINK, 0, perms), 0);
  ASSERT_EQ(ceph_ll_close(cmount, fh), 0);
  ASSERT_EQ(stx.stx_nlink, (nlink_t)1);

  ASSERT_EQ(ceph_ll_link(cmount, file, dir, linkname, perms), 0);
  ASSERT_EQ(ceph_ll_getattr(cmount, file, &stx, CEPH_STATX_NLINK, 0, perms), 0);
  ASSERT_EQ(stx.stx_nlink, (nlink_t)2);

  ASSERT_EQ(ceph_ll_unlink(cmount, dir, linkname, perms), 0);
  ASSERT_EQ(ceph_ll_lookup(cmount, dir, filename, &file, &stx,
			   CEPH_STATX_NLINK, 0, perms), 0);
  ASSERT_EQ(stx.stx_nlink, (nlink_t)1);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, SlashDotDot) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  struct ceph_statx	stx;
  ASSERT_EQ(ceph_statx(cmount, "/.", &stx, CEPH_STATX_INO, 0), 0);

  ino_t ino = stx.stx_ino;
  ASSERT_EQ(ceph_statx(cmount, "/..", &stx, CEPH_STATX_INO, 0), 0);

  /* At root, "." and ".." should be the same inode */
  ASSERT_EQ(ino, stx.stx_ino);

  /* Test accessing the parent of an unlinked directory */
  char dir1[32], dir2[56];
  sprintf(dir1, "/sldotdot%x", getpid());
  sprintf(dir2, "%s/sub%x", dir1, getpid());

  ASSERT_EQ(ceph_mkdir(cmount, dir1, 0755), 0);
  ASSERT_EQ(ceph_mkdir(cmount, dir2, 0755), 0);

  ASSERT_EQ(ceph_chdir(cmount, dir2), 0);

  /* Test behavior when unlinking cwd */
  struct ceph_dir_result *rdir;
  ASSERT_EQ(ceph_opendir(cmount, ".", &rdir), 0);
  ASSERT_EQ(ceph_rmdir(cmount, dir2), 0);

  /* get "." entry */
  struct dirent *result = ceph_readdir(cmount, rdir);
  ino = result->d_ino;

  /* get ".." entry */
  result = ceph_readdir(cmount, rdir);
  ASSERT_EQ(ino, result->d_ino);
  ceph_closedir(cmount, rdir);

  /* Make sure it works same way when mounting subtree */
  ASSERT_EQ(ceph_unmount(cmount), 0);
  ASSERT_EQ(ceph_mount(cmount, dir1), 0);
  ASSERT_EQ(ceph_statx(cmount, "/..", &stx, CEPH_STATX_INO, 0), 0);

  /* Test readdir behavior */
  ASSERT_EQ(ceph_opendir(cmount, "/", &rdir), 0);
  result = ceph_readdir(cmount, rdir);
  ASSERT_TRUE(result != NULL);
  ASSERT_STREQ(result->d_name, ".");
  ino = result->d_ino;
  result = ceph_readdir(cmount, rdir);
  ASSERT_TRUE(result != NULL);
  ASSERT_STREQ(result->d_name, "..");
  ASSERT_EQ(ino, result->d_ino);

  ceph_shutdown(cmount);
}

static inline bool
timespec_eq(timespec const& lhs, timespec const& rhs)
{
  return lhs.tv_sec == rhs.tv_sec && lhs.tv_nsec == rhs.tv_nsec;
}

TEST(LibCephFS, Btime) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char filename[32];
  sprintf(filename, "/getattrx%x", getpid());

  ceph_unlink(cmount, filename);
  int fd = ceph_open(cmount, filename, O_RDWR|O_CREAT|O_EXCL, 0666);
  ASSERT_LT(0, fd);

  /* make sure fstatx works */
  struct ceph_statx	stx;

  ASSERT_EQ(ceph_fstatx(cmount, fd, &stx, CEPH_STATX_CTIME|CEPH_STATX_BTIME, 0), 0);
  ASSERT_TRUE(stx.stx_mask & (CEPH_STATX_CTIME|CEPH_STATX_BTIME));
  ASSERT_TRUE(timespec_eq(stx.stx_ctime, stx.stx_btime));
  ceph_close(cmount, fd);

  ASSERT_EQ(ceph_statx(cmount, filename, &stx, CEPH_STATX_CTIME|CEPH_STATX_BTIME, 0), 0);
  ASSERT_TRUE(timespec_eq(stx.stx_ctime, stx.stx_btime));
  ASSERT_TRUE(stx.stx_mask & (CEPH_STATX_CTIME|CEPH_STATX_BTIME));

  struct timespec old_btime = stx.stx_btime;

  /* Now sleep, do a chmod and verify that the ctime changed, but btime didn't */
  sleep(1);
  ASSERT_EQ(ceph_chmod(cmount, filename, 0644), 0);
  ASSERT_EQ(ceph_statx(cmount, filename, &stx, CEPH_STATX_CTIME|CEPH_STATX_BTIME, 0), 0);
  ASSERT_TRUE(stx.stx_mask & CEPH_STATX_BTIME);
  ASSERT_TRUE(timespec_eq(stx.stx_btime, old_btime));
  ASSERT_FALSE(timespec_eq(stx.stx_ctime, stx.stx_btime));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, SetBtime) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char filename[32];
  sprintf(filename, "/setbtime%x", getpid());

  ceph_unlink(cmount, filename);
  int fd = ceph_open(cmount, filename, O_RDWR|O_CREAT|O_EXCL, 0666);
  ASSERT_LT(0, fd);
  ceph_close(cmount, fd);

  struct ceph_statx stx;
  struct timespec old_btime = { 1, 2 };

  stx.stx_btime = old_btime;

  ASSERT_EQ(ceph_setattrx(cmount, filename, &stx, CEPH_SETATTR_BTIME, 0), 0);

  ASSERT_EQ(ceph_statx(cmount, filename, &stx, CEPH_STATX_BTIME, 0), 0);
  ASSERT_TRUE(stx.stx_mask & CEPH_STATX_BTIME);
  ASSERT_TRUE(timespec_eq(stx.stx_btime, old_btime));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, LazyStatx) {
  struct ceph_mount_info *cmount1, *cmount2;
  ASSERT_EQ(ceph_create(&cmount1, NULL), 0);
  ASSERT_EQ(ceph_create(&cmount2, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount1, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount2, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount1, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount2, NULL));
  ASSERT_EQ(ceph_mount(cmount1, "/"), 0);
  ASSERT_EQ(ceph_mount(cmount2, "/"), 0);

  char filename[32];
  sprintf(filename, "lazystatx%x", getpid());

  Inode *root1, *file1, *root2, *file2;
  struct ceph_statx stx;
  Fh *fh;
  UserPerm *perms1 = ceph_mount_perms(cmount1);
  UserPerm *perms2 = ceph_mount_perms(cmount2);

  ASSERT_EQ(ceph_ll_lookup_root(cmount1, &root1), 0);
  ceph_ll_unlink(cmount1, root1, filename, perms1);
  ASSERT_EQ(ceph_ll_create(cmount1, root1, filename, 0666, O_RDWR|O_CREAT|O_EXCL,
			   &file1, &fh, &stx, 0, 0, perms1), 0);
  ASSERT_EQ(ceph_ll_close(cmount1, fh), 0);

  ASSERT_EQ(ceph_ll_lookup_root(cmount2, &root2), 0);

  ASSERT_EQ(ceph_ll_lookup(cmount2, root2, filename, &file2, &stx, CEPH_STATX_CTIME, 0, perms2), 0);

  struct timespec old_ctime = stx.stx_ctime;

  /*
   * Now sleep, do a chmod on the first client and the see whether we get a
   * different ctime with a statx that uses AT_STATX_DONT_SYNC
   */
  sleep(1);
  stx.stx_mode = 0644;
  ASSERT_EQ(ceph_ll_setattr(cmount1, file1, &stx, CEPH_SETATTR_MODE, perms1), 0);

  ASSERT_EQ(ceph_ll_getattr(cmount2, file2, &stx, CEPH_STATX_CTIME, AT_STATX_DONT_SYNC, perms2), 0);
  ASSERT_TRUE(stx.stx_mask & CEPH_STATX_CTIME);
  ASSERT_TRUE(stx.stx_ctime.tv_sec == old_ctime.tv_sec &&
	      stx.stx_ctime.tv_nsec == old_ctime.tv_nsec);

  ceph_shutdown(cmount1);
  ceph_shutdown(cmount2);
}

TEST(LibCephFS, ChangeAttr) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char filename[32];
  sprintf(filename, "/changeattr%x", getpid());

  ceph_unlink(cmount, filename);
  int fd = ceph_open(cmount, filename, O_RDWR|O_CREAT|O_EXCL, 0666);
  ASSERT_LT(0, fd);

  struct ceph_statx	stx;
  ASSERT_EQ(ceph_statx(cmount, filename, &stx, CEPH_STATX_VERSION, 0), 0);
  ASSERT_TRUE(stx.stx_mask & CEPH_STATX_VERSION);

  uint64_t old_change_attr = stx.stx_version;

  /* do chmod, and check whether change_attr changed */
  ASSERT_EQ(ceph_chmod(cmount, filename, 0644), 0);
  ASSERT_EQ(ceph_statx(cmount, filename, &stx, CEPH_STATX_VERSION, 0), 0);
  ASSERT_TRUE(stx.stx_mask & CEPH_STATX_VERSION);
  ASSERT_NE(stx.stx_version, old_change_attr);
  old_change_attr = stx.stx_version;

  /* now do a write and see if it changed again */
  ASSERT_EQ(3, ceph_write(cmount, fd, "foo", 3, 0));
  ASSERT_EQ(ceph_statx(cmount, filename, &stx, CEPH_STATX_VERSION, 0), 0);
  ASSERT_TRUE(stx.stx_mask & CEPH_STATX_VERSION);
  ASSERT_NE(stx.stx_version, old_change_attr);
  old_change_attr = stx.stx_version;

  /* Now truncate and check again */
  ASSERT_EQ(0, ceph_ftruncate(cmount, fd, 0));
  ASSERT_EQ(ceph_statx(cmount, filename, &stx, CEPH_STATX_VERSION, 0), 0);
  ASSERT_TRUE(stx.stx_mask & CEPH_STATX_VERSION);
  ASSERT_NE(stx.stx_version, old_change_attr);

  ceph_close(cmount, fd);
  ceph_shutdown(cmount);
}

TEST(LibCephFS, DirChangeAttrCreateFile) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char dirpath[32], filepath[56];
  sprintf(dirpath, "/dirchange%x", getpid());
  sprintf(filepath, "%s/foo", dirpath);

  ASSERT_EQ(ceph_mkdir(cmount, dirpath, 0755), 0);

  struct ceph_statx	stx;
  ASSERT_EQ(ceph_statx(cmount, dirpath, &stx, CEPH_STATX_VERSION, 0), 0);
  ASSERT_TRUE(stx.stx_mask & CEPH_STATX_VERSION);

  uint64_t old_change_attr = stx.stx_version;

  /* Important: Follow an operation that changes the directory's ctime (setxattr)
   * with one that changes the directory's mtime and ctime (create).
   * Check that directory's change_attr is updated everytime ctime changes.
   */

  /* set xattr on dir, and check whether dir's change_attr is incremented */
  ASSERT_EQ(ceph_setxattr(cmount, dirpath, "user.name", (void*)"bob", 3, XATTR_CREATE), 0);
  ASSERT_EQ(ceph_statx(cmount, dirpath, &stx, CEPH_STATX_VERSION, 0), 0);
  ASSERT_TRUE(stx.stx_mask & CEPH_STATX_VERSION);
  ASSERT_GT(stx.stx_version, old_change_attr);
  old_change_attr = stx.stx_version;

  /* create a file within dir, and check whether dir's change_attr is incremented */
  int fd = ceph_open(cmount, filepath, O_RDWR|O_CREAT|O_EXCL, 0666);
  ASSERT_LT(0, fd);
  ceph_close(cmount, fd);
  ASSERT_EQ(ceph_statx(cmount, dirpath, &stx, CEPH_STATX_VERSION, 0), 0);
  ASSERT_TRUE(stx.stx_mask & CEPH_STATX_VERSION);
  ASSERT_GT(stx.stx_version, old_change_attr);

  ASSERT_EQ(0, ceph_unlink(cmount, filepath));
  ASSERT_EQ(0, ceph_rmdir(cmount, dirpath));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, DirChangeAttrRenameFile) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char dirpath[32], filepath[56], newfilepath[56];
  sprintf(dirpath, "/dirchange%x", getpid());
  sprintf(filepath, "%s/foo", dirpath);

  ASSERT_EQ(ceph_mkdir(cmount, dirpath, 0755), 0);

  int fd = ceph_open(cmount, filepath, O_RDWR|O_CREAT|O_EXCL, 0666);
  ASSERT_LT(0, fd);
  ceph_close(cmount, fd);

  /* Important: Follow an operation that changes the directory's ctime (setattr)
   * with one that changes the directory's mtime and ctime (rename).
   * Check that directory's change_attr is updated everytime ctime changes.
   */
  struct ceph_statx	stx;
  ASSERT_EQ(ceph_statx(cmount, dirpath, &stx, CEPH_STATX_VERSION, 0), 0);
  ASSERT_TRUE(stx.stx_mask & CEPH_STATX_VERSION);

  uint64_t old_change_attr = stx.stx_version;

  /* chmod dir, and check whether dir's change_attr is incremented */
  ASSERT_EQ(ceph_chmod(cmount, dirpath, 0777), 0);
  ASSERT_EQ(ceph_statx(cmount, dirpath, &stx, CEPH_STATX_VERSION, 0), 0);
  ASSERT_TRUE(stx.stx_mask & CEPH_STATX_VERSION);
  ASSERT_GT(stx.stx_version, old_change_attr);
  old_change_attr = stx.stx_version;

  /* rename a file within dir, and check whether dir's change_attr is incremented */
  sprintf(newfilepath, "%s/bar", dirpath);
  ASSERT_EQ(ceph_rename(cmount, filepath, newfilepath), 0);
  ASSERT_EQ(ceph_statx(cmount, dirpath, &stx, CEPH_STATX_VERSION, 0), 0);
  ASSERT_TRUE(stx.stx_mask & CEPH_STATX_VERSION);
  ASSERT_GT(stx.stx_version, old_change_attr);

  ASSERT_EQ(0, ceph_unlink(cmount, newfilepath));
  ASSERT_EQ(0, ceph_rmdir(cmount, dirpath));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, DirChangeAttrRemoveFile) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char dirpath[32], filepath[56];
  sprintf(dirpath, "/dirchange%x", getpid());
  sprintf(filepath, "%s/foo", dirpath);

  ASSERT_EQ(ceph_mkdir(cmount, dirpath, 0755), 0);

  ASSERT_EQ(ceph_setxattr(cmount, dirpath, "user.name", (void*)"bob", 3, XATTR_CREATE), 0);

  int fd = ceph_open(cmount, filepath, O_RDWR|O_CREAT|O_EXCL, 0666);
  ASSERT_LT(0, fd);
  ceph_close(cmount, fd);

  /* Important: Follow an operation that changes the directory's ctime (removexattr)
   * with one that changes the directory's mtime and ctime (remove a file).
   * Check that directory's change_attr is updated everytime ctime changes.
   */
  struct ceph_statx	stx;
  ASSERT_EQ(ceph_statx(cmount, dirpath, &stx, CEPH_STATX_VERSION, 0), 0);
  ASSERT_TRUE(stx.stx_mask & CEPH_STATX_VERSION);

  uint64_t old_change_attr = stx.stx_version;

  /* remove xattr, and check whether dir's change_attr is incremented */
  ASSERT_EQ(ceph_removexattr(cmount, dirpath, "user.name"), 0);
  ASSERT_EQ(ceph_statx(cmount, dirpath, &stx, CEPH_STATX_VERSION, 0), 0);
  ASSERT_TRUE(stx.stx_mask & CEPH_STATX_VERSION);
  ASSERT_GT(stx.stx_version, old_change_attr);
  old_change_attr = stx.stx_version;

  /* remove a file within dir, and check whether dir's change_attr is incremented */
  ASSERT_EQ(0, ceph_unlink(cmount, filepath));
  ASSERT_EQ(ceph_statx(cmount, dirpath, &stx, CEPH_STATX_VERSION, 0), 0);
  ASSERT_TRUE(stx.stx_mask & CEPH_STATX_VERSION);
  ASSERT_GT(stx.stx_version, old_change_attr);

  ASSERT_EQ(0, ceph_rmdir(cmount, dirpath));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, SetSize) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char filename[32];
  sprintf(filename, "/setsize%x", getpid());

  ceph_unlink(cmount, filename);
  int fd = ceph_open(cmount, filename, O_RDWR|O_CREAT|O_EXCL, 0666);
  ASSERT_LT(0, fd);

  struct ceph_statx stx;
  uint64_t size = 8388608;
  stx.stx_size = size;
  ASSERT_EQ(ceph_fsetattrx(cmount, fd, &stx, CEPH_SETATTR_SIZE), 0);
  ASSERT_EQ(ceph_fstatx(cmount, fd, &stx, CEPH_STATX_SIZE, 0), 0);
  ASSERT_EQ(stx.stx_size, size);

  ceph_close(cmount, fd);
  ceph_shutdown(cmount);
}

TEST(LibCephFS, OperationsOnRoot)
{
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char dirname[32];
  sprintf(dirname, "/somedir%x", getpid());

  ASSERT_EQ(ceph_mkdir(cmount, dirname, 0755), 0);

  ASSERT_EQ(ceph_rmdir(cmount, "/"), -CEPHFS_EBUSY);

  ASSERT_EQ(ceph_link(cmount, "/", "/"), -CEPHFS_EEXIST);
  ASSERT_EQ(ceph_link(cmount, dirname, "/"), -CEPHFS_EEXIST);
  ASSERT_EQ(ceph_link(cmount, "nonExisitingDir", "/"), -CEPHFS_ENOENT);

  ASSERT_EQ(ceph_unlink(cmount, "/"), -CEPHFS_EISDIR);

  ASSERT_EQ(ceph_rename(cmount, "/", "/"), -CEPHFS_EBUSY);
  ASSERT_EQ(ceph_rename(cmount, dirname, "/"), -CEPHFS_EBUSY);
  ASSERT_EQ(ceph_rename(cmount, "nonExistingDir", "/"), -CEPHFS_EBUSY);
  ASSERT_EQ(ceph_rename(cmount, "/", dirname), -CEPHFS_EBUSY);
  ASSERT_EQ(ceph_rename(cmount, "/", "nonExistingDir"), -CEPHFS_EBUSY);

  ASSERT_EQ(ceph_mkdir(cmount, "/", 0777), -CEPHFS_EEXIST);

  ASSERT_EQ(ceph_mknod(cmount, "/", 0, 0), -CEPHFS_EEXIST);

  ASSERT_EQ(ceph_symlink(cmount, "/", "/"), -CEPHFS_EEXIST);
  ASSERT_EQ(ceph_symlink(cmount, dirname, "/"), -CEPHFS_EEXIST);
  ASSERT_EQ(ceph_symlink(cmount, "nonExistingDir", "/"), -CEPHFS_EEXIST);

  ceph_shutdown(cmount);
}

// no rlimits on Windows
#ifndef _WIN32
static void shutdown_racer_func()
{
  const int niter = 32;
  struct ceph_mount_info *cmount;
  int i;

  for (i = 0; i < niter; ++i) {
    ASSERT_EQ(ceph_create(&cmount, NULL), 0);
    ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
    ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
    ASSERT_EQ(ceph_mount(cmount, "/"), 0);
    ceph_shutdown(cmount);
  }
}

// See tracker #20988
TEST(LibCephFS, ShutdownRace)
{
  const int nthreads = 32;
  std::thread threads[nthreads];

  // Need a bunch of fd's for this test
  struct rlimit rold, rnew;
  ASSERT_EQ(getrlimit(RLIMIT_NOFILE, &rold), 0);
  rnew = rold;
  rnew.rlim_cur = rnew.rlim_max;

  cout << "Setting RLIMIT_NOFILE from " << rold.rlim_cur <<
	  " to " << rnew.rlim_cur << std::endl;

  ASSERT_EQ(setrlimit(RLIMIT_NOFILE, &rnew), 0);

  for (int i = 0; i < nthreads; ++i)
    threads[i] = std::thread(shutdown_racer_func);

  for (int i = 0; i < nthreads; ++i)
    threads[i].join();
  /*
   * Let's just ignore restoring the open files limit,
   * the kernel will defer releasing the file descriptors
   * and then the process will be possibly reachthe open
   * files limit. More detail, please see tracer#43039
   */
//  ASSERT_EQ(setrlimit(RLIMIT_NOFILE, &rold), 0);
}
#endif

static void get_current_time_utimbuf(struct utimbuf *utb)
{
  utime_t t = ceph_clock_now();
  utb->actime = t.sec();
  utb->modtime = t.sec();
}

static void get_current_time_timeval(struct timeval tv[2])
{
  utime_t t = ceph_clock_now();
  t.copy_to_timeval(&tv[0]);
  t.copy_to_timeval(&tv[1]);
}

static void get_current_time_timespec(struct timespec ts[2])
{
  utime_t t = ceph_clock_now();
  t.to_timespec(&ts[0]);
  t.to_timespec(&ts[1]);
}

TEST(LibCephFS, TestUtime) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char test_file[256];
  sprintf(test_file, "test_utime_file_%d", getpid());
  int fd = ceph_open(cmount, test_file, O_CREAT, 0666);
  ASSERT_GT(fd, 0);

  struct utimbuf utb;
  struct ceph_statx stx;

  get_current_time_utimbuf(&utb);

  // ceph_utime()
  EXPECT_EQ(0, ceph_utime(cmount, test_file, &utb));
  ASSERT_EQ(ceph_statx(cmount, test_file, &stx,
                       CEPH_STATX_MTIME|CEPH_STATX_ATIME, 0), 0);
  ASSERT_EQ(utime_t(stx.stx_atime), utime_t(utb.actime, 0));
  ASSERT_EQ(utime_t(stx.stx_mtime), utime_t(utb.modtime, 0));

  get_current_time_utimbuf(&utb);

  // ceph_futime()
  EXPECT_EQ(0, ceph_futime(cmount, fd, &utb));
  ASSERT_EQ(ceph_statx(cmount, test_file, &stx,
                       CEPH_STATX_MTIME|CEPH_STATX_ATIME, 0), 0);
  ASSERT_EQ(utime_t(stx.stx_atime), utime_t(utb.actime, 0));
  ASSERT_EQ(utime_t(stx.stx_mtime), utime_t(utb.modtime, 0));

  ceph_close(cmount, fd);
  ceph_shutdown(cmount);
}

TEST(LibCephFS, TestUtimes) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char test_file[256];
  char test_symlink[256];

  sprintf(test_file, "test_utimes_file_%d", getpid());
  sprintf(test_symlink, "test_utimes_symlink_%d", getpid());
  int fd = ceph_open(cmount, test_file, O_CREAT, 0666);
  ASSERT_GT(fd, 0);

  ASSERT_EQ(ceph_symlink(cmount, test_file, test_symlink), 0);

  struct timeval times[2];
  struct ceph_statx stx;

  get_current_time_timeval(times);

  // ceph_utimes() on symlink, validate target file time
  EXPECT_EQ(0, ceph_utimes(cmount, test_symlink, times));
  ASSERT_EQ(ceph_statx(cmount, test_symlink, &stx,
                       CEPH_STATX_MTIME|CEPH_STATX_ATIME, 0), 0);
  ASSERT_EQ(utime_t(stx.stx_atime), utime_t(times[0]));
  ASSERT_EQ(utime_t(stx.stx_mtime), utime_t(times[1]));

  get_current_time_timeval(times);

  // ceph_lutimes() on symlink, validate symlink time
  EXPECT_EQ(0, ceph_lutimes(cmount, test_symlink, times));
  ASSERT_EQ(ceph_statx(cmount, test_symlink, &stx,
                       CEPH_STATX_MTIME|CEPH_STATX_ATIME, AT_SYMLINK_NOFOLLOW), 0);
  ASSERT_EQ(utime_t(stx.stx_atime), utime_t(times[0]));
  ASSERT_EQ(utime_t(stx.stx_mtime), utime_t(times[1]));

  get_current_time_timeval(times);

  // ceph_futimes()
  EXPECT_EQ(0, ceph_futimes(cmount, fd, times));
  ASSERT_EQ(ceph_statx(cmount, test_file, &stx,
                       CEPH_STATX_MTIME|CEPH_STATX_ATIME, 0), 0);
  ASSERT_EQ(utime_t(stx.stx_atime), utime_t(times[0]));
  ASSERT_EQ(utime_t(stx.stx_mtime), utime_t(times[1]));

  ceph_close(cmount, fd);
  ceph_shutdown(cmount);
}

TEST(LibCephFS, TestFutimens) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char test_file[256];

  sprintf(test_file, "test_futimens_file_%d", getpid());
  int fd = ceph_open(cmount, test_file, O_CREAT, 0666);
  ASSERT_GT(fd, 0);

  struct timespec times[2];
  struct ceph_statx stx;

  get_current_time_timespec(times);

  // ceph_futimens()
  EXPECT_EQ(0, ceph_futimens(cmount, fd, times));
  ASSERT_EQ(ceph_statx(cmount, test_file, &stx,
                       CEPH_STATX_MTIME|CEPH_STATX_ATIME, 0), 0);
  ASSERT_EQ(utime_t(stx.stx_atime), utime_t(times[0]));
  ASSERT_EQ(utime_t(stx.stx_mtime), utime_t(times[1]));

  ceph_close(cmount, fd);
  ceph_shutdown(cmount);
}

TEST(LibCephFS, OperationsOnDotDot) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char c_dir[512], c_dir_dot[1024], c_dir_dotdot[1024];
  char c_non_existent_dir[1024], c_non_existent_dirs[1024];
  char c_temp[1024];

  pid_t mypid = getpid();
  sprintf(c_dir, "/oodd_dir_%d", mypid);
  sprintf(c_dir_dot, "/%s/.", c_dir);
  sprintf(c_dir_dotdot, "/%s/..", c_dir);
  sprintf(c_non_existent_dir, "/%s/../oodd_nonexistent/..", c_dir);
  sprintf(c_non_existent_dirs,
          "/%s/../ood_nonexistent1_%d/oodd_nonexistent2_%d", c_dir, mypid, mypid);
  sprintf(c_temp, "/oodd_temp_%d", mypid);

  ASSERT_EQ(0, ceph_mkdir(cmount, c_dir, 0777));
  ASSERT_EQ(-CEPHFS_EEXIST, ceph_mkdir(cmount, c_dir_dot, 0777));
  ASSERT_EQ(-CEPHFS_EEXIST, ceph_mkdir(cmount, c_dir_dotdot, 0777));
  ASSERT_EQ(0, ceph_mkdirs(cmount, c_non_existent_dirs, 0777));

  ASSERT_EQ(-CEPHFS_ENOTEMPTY, ceph_rmdir(cmount, c_dir_dot));
  ASSERT_EQ(-CEPHFS_ENOTEMPTY, ceph_rmdir(cmount, c_dir_dotdot));
  // non existent directory should return -CEPHFS_ENOENT
  ASSERT_EQ(-CEPHFS_ENOENT, ceph_rmdir(cmount, c_non_existent_dir));

  ASSERT_EQ(-CEPHFS_EBUSY, ceph_rename(cmount, c_dir_dot, c_temp));
  ASSERT_EQ(0, ceph_chdir(cmount, c_dir));
  ASSERT_EQ(0, ceph_mkdir(cmount, c_temp, 0777));
  ASSERT_EQ(-CEPHFS_EBUSY, ceph_rename(cmount, c_temp, ".."));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, Caps_vxattr) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char test_caps_vxattr_file[256];
  char gxattrv[128];
  int xbuflen = sizeof(gxattrv);
  pid_t mypid = getpid();

  sprintf(test_caps_vxattr_file, "test_caps_vxattr_%d", mypid);
  int fd = ceph_open(cmount, test_caps_vxattr_file, O_CREAT, 0666);
  ASSERT_GT(fd, 0);
  ceph_close(cmount, fd);

  int alen = ceph_getxattr(cmount, test_caps_vxattr_file, "ceph.caps", (void *)gxattrv, xbuflen);
  ASSERT_GT(alen, 0);
  gxattrv[alen] = '\0';

  char caps_regex[] = "pA[sx]*L[sx]*X[sx]*F[sxcrwbal]*/0x[0-9a-fA-f]+";
  ASSERT_TRUE(regex_match(gxattrv, regex(caps_regex)) == 1);
  ceph_shutdown(cmount);
}

TEST(LibCephFS, SnapXattrs) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char test_snap_xattr_file[256];
  char c_temp[PATH_MAX];
  char gxattrv[128];
  char gxattrv2[128];
  int xbuflen = sizeof(gxattrv);
  pid_t mypid = getpid();

  sprintf(test_snap_xattr_file, "test_snap_xattr_%d", mypid);
  int fd = ceph_open(cmount, test_snap_xattr_file, O_CREAT, 0666);
  ASSERT_GT(fd, 0);
  ceph_close(cmount, fd);

  sprintf(c_temp, "/.snap/test_snap_xattr_snap_%d", mypid);
  ASSERT_EQ(0, ceph_mkdir(cmount, c_temp, 0777));

  int alen = ceph_getxattr(cmount, c_temp, "ceph.snap.btime", (void *)gxattrv, xbuflen);
  // xattr value is secs.nsecs (don't assume zero-term)
  ASSERT_LT(0, alen);
  ASSERT_LT(alen, xbuflen);
  gxattrv[alen] = '\0';
  char *s = strchr(gxattrv, '.');
  char *q = NULL;
  ASSERT_NE(q, s);
  ASSERT_LT(s, gxattrv + alen);
  ASSERT_EQ('.', *s);
  *s = '\0';
  utime_t btime = utime_t(strtoull(gxattrv, NULL, 10), strtoull(s + 1, NULL, 10));
  *s = '.';  // restore for later strcmp

  // file within the snapshot should carry the same btime
  sprintf(c_temp, "/.snap/test_snap_xattr_snap_%d/%s", mypid, test_snap_xattr_file);

  int alen2 = ceph_getxattr(cmount, c_temp, "ceph.snap.btime", (void *)gxattrv2, xbuflen);
  ASSERT_EQ(alen, alen2);
  ASSERT_EQ(0, strncmp(gxattrv, gxattrv2, alen));

  // non-snap file shouldn't carry the xattr
  alen = ceph_getxattr(cmount, test_snap_xattr_file, "ceph.snap.btime", (void *)gxattrv2, xbuflen);
  ASSERT_EQ(-CEPHFS_ENODATA, alen);

  // create a second snapshot
  sprintf(c_temp, "/.snap/test_snap_xattr_snap2_%d", mypid);
  ASSERT_EQ(0, ceph_mkdir(cmount, c_temp, 0777));

  // check that the btime for the newer snapshot is > older
  alen = ceph_getxattr(cmount, c_temp, "ceph.snap.btime", (void *)gxattrv2, xbuflen);
  ASSERT_LT(0, alen);
  ASSERT_LT(alen, xbuflen);
  gxattrv2[alen] = '\0';
  s = strchr(gxattrv2, '.');
  ASSERT_NE(q, s);
  ASSERT_LT(s, gxattrv2 + alen);
  ASSERT_EQ('.', *s);
  *s = '\0';
  utime_t new_btime = utime_t(strtoull(gxattrv2, NULL, 10), strtoull(s + 1, NULL, 10));
  #ifndef _WIN32
  // This assertion sometimes fails on Windows, possibly due to the clock precision.
  ASSERT_LT(btime, new_btime);
  #endif

  // listxattr() shouldn't return snap.btime vxattr
  char xattrlist[512];
  int len = ceph_listxattr(cmount, test_snap_xattr_file, xattrlist, sizeof(xattrlist));
  ASSERT_GE(sizeof(xattrlist), (size_t)len);
  char *p = xattrlist;
  int found = 0;
  while (len > 0) {
    if (strcmp(p, "ceph.snap.btime") == 0)
      found++;
    len -= strlen(p) + 1;
    p += strlen(p) + 1;
  }
  ASSERT_EQ(found, 0);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, Lseek) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  char c_path[1024];
  sprintf(c_path, "test_lseek_%d", getpid());
  int fd = ceph_open(cmount, c_path, O_RDWR|O_CREAT|O_TRUNC, 0666);
  ASSERT_LT(0, fd);

  const char *out_buf = "hello world";
  size_t size = strlen(out_buf);
  ASSERT_EQ(ceph_write(cmount, fd, out_buf, size, 0), (int)size);

  /* basic SEEK_SET/END/CUR tests */
  ASSERT_EQ(0, ceph_lseek(cmount, fd, 0, SEEK_SET));
  ASSERT_EQ(size, ceph_lseek(cmount, fd, 0, SEEK_END));
  ASSERT_EQ(0, ceph_lseek(cmount, fd, -size, SEEK_CUR));

  /* Test basic functionality and out of bounds conditions for SEEK_HOLE/DATA */
#ifdef SEEK_HOLE
  ASSERT_EQ(size, ceph_lseek(cmount, fd, 0, SEEK_HOLE));
  ASSERT_EQ(-CEPHFS_ENXIO, ceph_lseek(cmount, fd, -1, SEEK_HOLE));
  ASSERT_EQ(-CEPHFS_ENXIO, ceph_lseek(cmount, fd, size + 1, SEEK_HOLE));
#endif
#ifdef SEEK_DATA
  ASSERT_EQ(0, ceph_lseek(cmount, fd, 0, SEEK_DATA));
  ASSERT_EQ(-CEPHFS_ENXIO, ceph_lseek(cmount, fd, -1, SEEK_DATA));
  ASSERT_EQ(-CEPHFS_ENXIO, ceph_lseek(cmount, fd, size + 1, SEEK_DATA));
#endif

  ASSERT_EQ(0, ceph_close(cmount, fd));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, SnapInfoOnNonSnapshot) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  struct snap_info info;
  ASSERT_EQ(-CEPHFS_EINVAL, ceph_get_snap_info(cmount, "/", &info));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, EmptySnapInfo) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char dir_path[64];
  char snap_path[PATH_MAX];
  sprintf(dir_path, "/dir0_%d", getpid());
  sprintf(snap_path, "%s/.snap/snap0_%d", dir_path, getpid());

  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path, 0755));
  // snapshot without custom metadata
  ASSERT_EQ(0, ceph_mkdir(cmount, snap_path, 0755));

  struct snap_info info;
  ASSERT_EQ(0, ceph_get_snap_info(cmount, snap_path, &info));
  ASSERT_GT(info.id, 0);
  ASSERT_EQ(info.nr_snap_metadata, 0);

  ASSERT_EQ(0, ceph_rmdir(cmount, snap_path));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, SnapInfo) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char dir_path[64];
  char snap_name[64];
  char snap_path[PATH_MAX];
  sprintf(dir_path, "/dir0_%d", getpid());
  sprintf(snap_name, "snap0_%d", getpid());
  sprintf(snap_path, "%s/.snap/%s", dir_path, snap_name);

  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path, 0755));
  // snapshot with custom metadata
  struct snap_metadata snap_meta[] = {{"foo", "bar"},{"this", "that"},{"abcdefg", "12345"}};
  ASSERT_EQ(0, ceph_mksnap(cmount, dir_path, snap_name, 0755, snap_meta, std::size(snap_meta)));

  struct snap_info info;
  ASSERT_EQ(0, ceph_get_snap_info(cmount, snap_path, &info));
  ASSERT_GT(info.id, 0);
  ASSERT_EQ(info.nr_snap_metadata, std::size(snap_meta));
  for (size_t i = 0; i < info.nr_snap_metadata; ++i) {
    auto &k1 = info.snap_metadata[i].key;
    auto &v1 = info.snap_metadata[i].value;
    bool found = false;
    for (size_t j = 0; j < info.nr_snap_metadata; ++j) {
      auto &k2 = snap_meta[j].key;
      auto &v2 = snap_meta[j].value;
      if (strncmp(k1, k2, strlen(k1)) == 0 && strncmp(v1, v2, strlen(v1)) == 0) {
        found = true;
        break;
      }
    }
    ASSERT_TRUE(found);
  }
  ceph_free_snap_info_buffer(&info);

  ASSERT_EQ(0, ceph_rmsnap(cmount, dir_path, snap_name));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, LookupInoMDSDir) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  Inode *inode;
  auto ino = inodeno_t(0x100); /* rank 0 ~mdsdir */
  ASSERT_EQ(-CEPHFS_ESTALE, ceph_ll_lookup_inode(cmount, ino, &inode));
  ino = inodeno_t(0x600); /* rank 0 first stray dir */
  ASSERT_EQ(-CEPHFS_ESTALE, ceph_ll_lookup_inode(cmount, ino, &inode));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, LookupVino) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char dir_path[64];
  char snap_name[64];
  char snapdir_path[128];
  char snap_path[256];
  char file_path[PATH_MAX];
  char snap_file[PATH_MAX];
  sprintf(dir_path, "/dir0_%d", getpid());
  sprintf(snap_name, "snap0_%d", getpid());
  sprintf(file_path, "%s/file_%d", dir_path, getpid());
  sprintf(snapdir_path, "%s/.snap", dir_path);
  sprintf(snap_path, "%s/%s", snapdir_path, snap_name);
  sprintf(snap_file, "%s/file_%d", snap_path, getpid());

  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path, 0755));
  int fd = ceph_open(cmount, file_path, O_WRONLY|O_CREAT, 0666);
  ASSERT_LE(0, fd);
  ASSERT_EQ(0, ceph_close(cmount, fd));
  ASSERT_EQ(0, ceph_mksnap(cmount, dir_path, snap_name, 0755, nullptr, 0));

  // record vinos for all of them
  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_statx(cmount, dir_path, &stx, CEPH_STATX_INO, 0));
  vinodeno_t dir_vino(stx.stx_ino, stx.stx_dev);

  ASSERT_EQ(0, ceph_statx(cmount, file_path, &stx, CEPH_STATX_INO, 0));
  vinodeno_t file_vino(stx.stx_ino, stx.stx_dev);

  ASSERT_EQ(0, ceph_statx(cmount, snapdir_path, &stx, CEPH_STATX_INO, 0));
  vinodeno_t snapdir_vino(stx.stx_ino, stx.stx_dev);

  ASSERT_EQ(0, ceph_statx(cmount, snap_path, &stx, CEPH_STATX_INO, 0));
  vinodeno_t snap_vino(stx.stx_ino, stx.stx_dev);

  ASSERT_EQ(0, ceph_statx(cmount, snap_file, &stx, CEPH_STATX_INO, 0));
  vinodeno_t snap_file_vino(stx.stx_ino, stx.stx_dev);

  // Remount
  ASSERT_EQ(0, ceph_unmount(cmount));
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, NULL));

  // Find them all
  Inode *inode;
  ASSERT_EQ(0, ceph_ll_lookup_vino(cmount, dir_vino, &inode));
  ceph_ll_put(cmount, inode);
  ASSERT_EQ(0, ceph_ll_lookup_vino(cmount, file_vino, &inode));
  ceph_ll_put(cmount, inode);
  ASSERT_EQ(0, ceph_ll_lookup_vino(cmount, snapdir_vino, &inode));
  ceph_ll_put(cmount, inode);
  ASSERT_EQ(0, ceph_ll_lookup_vino(cmount, snap_vino, &inode));
  ceph_ll_put(cmount, inode);
  ASSERT_EQ(0, ceph_ll_lookup_vino(cmount, snap_file_vino, &inode));
  ceph_ll_put(cmount, inode);

  // cleanup
  ASSERT_EQ(0, ceph_rmsnap(cmount, dir_path, snap_name));
  ASSERT_EQ(0, ceph_unlink(cmount, file_path));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, Openat) {
  pid_t mypid = getpid();
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  char c_rel_dir[64];
  char c_dir[128];
  sprintf(c_rel_dir, "open_test_%d", mypid);
  sprintf(c_dir, "/%s", c_rel_dir);
  ASSERT_EQ(0, ceph_mkdir(cmount, c_dir, 0777));

  int root_fd = ceph_open(cmount, "/", O_DIRECTORY | O_RDONLY, 0);
  ASSERT_LE(0, root_fd);

  int dir_fd = ceph_openat(cmount, root_fd, c_rel_dir, O_DIRECTORY | O_RDONLY, 0);
  ASSERT_LE(0, dir_fd);

  struct ceph_statx stx;
  ASSERT_EQ(ceph_statxat(cmount, root_fd, c_rel_dir, &stx, 0, 0), 0);
  ASSERT_EQ(stx.stx_mode & S_IFMT, S_IFDIR);

  char c_rel_path[64];
  char c_path[256];
  sprintf(c_rel_path, "created_file_%d", mypid);
  sprintf(c_path, "%s/created_file_%d", c_dir, mypid);
  int file_fd = ceph_openat(cmount, dir_fd, c_rel_path, O_RDONLY | O_CREAT, 0666);
  ASSERT_LE(0, file_fd);

  ASSERT_EQ(ceph_statxat(cmount, dir_fd, c_rel_path, &stx, 0, 0), 0);
  ASSERT_EQ(stx.stx_mode & S_IFMT, S_IFREG);

  ASSERT_EQ(0, ceph_close(cmount, file_fd));
  ASSERT_EQ(0, ceph_close(cmount, dir_fd));
  ASSERT_EQ(0, ceph_close(cmount, root_fd));

  ASSERT_EQ(0, ceph_unlink(cmount, c_path));
  ASSERT_EQ(0, ceph_rmdir(cmount, c_dir));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, Statxat) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char dir_name[64];
  char rel_file_name_1[128];
  char rel_file_name_2[256];

  char dir_path[512];
  char file_path[1024];

  // relative paths for *at() calls
  sprintf(dir_name, "dir0_%d", getpid());
  sprintf(rel_file_name_1, "file_%d", getpid());
  sprintf(rel_file_name_2, "%s/%s", dir_name, rel_file_name_1);

  sprintf(dir_path, "/%s", dir_name);
  sprintf(file_path, "%s/%s", dir_path, rel_file_name_1);

  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path, 0755));
  int fd = ceph_open(cmount, file_path, O_WRONLY|O_CREAT, 0666);
  ASSERT_LE(0, fd);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  struct ceph_statx stx;

  // test relative to root
  fd = ceph_open(cmount, "/", O_DIRECTORY | O_RDONLY, 0);
  ASSERT_LE(0, fd);
  ASSERT_EQ(ceph_statxat(cmount, fd, dir_name, &stx, 0, 0), 0);
  ASSERT_EQ(stx.stx_mode & S_IFMT, S_IFDIR);
  ASSERT_EQ(ceph_statxat(cmount, fd, rel_file_name_2, &stx, 0, 0), 0);
  ASSERT_EQ(stx.stx_mode & S_IFMT, S_IFREG);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  // test relative to dir
  fd = ceph_open(cmount, dir_path, O_DIRECTORY | O_RDONLY, 0);
  ASSERT_LE(0, fd);
  ASSERT_EQ(ceph_statxat(cmount, fd, rel_file_name_1, &stx, 0, 0), 0);
  ASSERT_EQ(stx.stx_mode & S_IFMT, S_IFREG);

  // delete the dirtree, recreate and verify
  ASSERT_EQ(0, ceph_unlink(cmount, file_path));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path));
  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path, 0755));
  int fd1 = ceph_open(cmount, file_path, O_WRONLY|O_CREAT, 0666);
  ASSERT_LE(0, fd1);
  ASSERT_EQ(0, ceph_close(cmount, fd1));
  ASSERT_EQ(ceph_statxat(cmount, fd, rel_file_name_1, &stx, 0, 0), -CEPHFS_ENOENT);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  ASSERT_EQ(0, ceph_unlink(cmount, file_path));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, StatxatATFDCWD) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char dir_name[64];
  char rel_file_name_1[128];

  char dir_path[512];
  char file_path[1024];

  // relative paths for *at() calls
  sprintf(dir_name, "dir0_%d", getpid());
  sprintf(rel_file_name_1, "file_%d", getpid());

  sprintf(dir_path, "/%s", dir_name);
  sprintf(file_path, "%s/%s", dir_path, rel_file_name_1);

  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path, 0755));
  int fd = ceph_open(cmount, file_path, O_WRONLY|O_CREAT, 0666);
  ASSERT_LE(0, fd);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  struct ceph_statx stx;
  // chdir and test with CEPHFS_AT_FDCWD
  ASSERT_EQ(0, ceph_chdir(cmount, dir_path));
  ASSERT_EQ(ceph_statxat(cmount, CEPHFS_AT_FDCWD, rel_file_name_1, &stx, 0, 0), 0);
  ASSERT_EQ(stx.stx_mode & S_IFMT, S_IFREG);

  ASSERT_EQ(0, ceph_unlink(cmount, file_path));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, Fdopendir) {
  pid_t mypid = getpid();

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char foostr[256];
  sprintf(foostr, "/dir_ls%d", mypid);
  ASSERT_EQ(ceph_mkdir(cmount, foostr, 0777), 0);

  char bazstr[512];
  sprintf(bazstr, "%s/elif", foostr);
  int fd  = ceph_open(cmount, bazstr, O_CREAT|O_RDONLY, 0666);
  ASSERT_LE(0, fd);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  fd = ceph_open(cmount, foostr, O_DIRECTORY | O_RDONLY, 0);
  ASSERT_LE(0, fd);
  struct ceph_dir_result *ls_dir = NULL;
  ASSERT_EQ(ceph_fdopendir(cmount, fd, &ls_dir), 0);

  // not guaranteed to get . and .. first, but its a safe assumption in this case
  struct dirent *result = ceph_readdir(cmount, ls_dir);
  ASSERT_TRUE(result != NULL);
  ASSERT_STREQ(result->d_name, ".");
  result = ceph_readdir(cmount, ls_dir);
  ASSERT_TRUE(result != NULL);
  ASSERT_STREQ(result->d_name, "..");
  result = ceph_readdir(cmount, ls_dir);
  ASSERT_TRUE(result != NULL);
  ASSERT_STREQ(result->d_name, "elif");

  ASSERT_TRUE(ceph_readdir(cmount, ls_dir) == NULL);

  ASSERT_EQ(0, ceph_close(cmount, fd));
  ASSERT_EQ(0, ceph_closedir(cmount, ls_dir));
  ASSERT_EQ(0, ceph_unlink(cmount, bazstr));
  ASSERT_EQ(0, ceph_rmdir(cmount, foostr));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, FdopendirATFDCWD) {
  pid_t mypid = getpid();

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char foostr[256];
  sprintf(foostr, "/dir_ls%d", mypid);
  ASSERT_EQ(ceph_mkdir(cmount, foostr, 0777), 0);

  char bazstr[512];
  sprintf(bazstr, "%s/elif", foostr);
  int fd  = ceph_open(cmount, bazstr, O_CREAT|O_RDONLY, 0666);
  ASSERT_LE(0, fd);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  ASSERT_EQ(0, ceph_chdir(cmount, foostr));
  struct ceph_dir_result *ls_dir = NULL;
  ASSERT_EQ(ceph_fdopendir(cmount, CEPHFS_AT_FDCWD, &ls_dir), 0);

  // not guaranteed to get . and .. first, but its a safe assumption in this case
  struct dirent *result = ceph_readdir(cmount, ls_dir);
  ASSERT_TRUE(result != NULL);
  ASSERT_STREQ(result->d_name, ".");
  result = ceph_readdir(cmount, ls_dir);
  ASSERT_TRUE(result != NULL);
  ASSERT_STREQ(result->d_name, "..");
  result = ceph_readdir(cmount, ls_dir);
  ASSERT_TRUE(result != NULL);
  ASSERT_STREQ(result->d_name, "elif");

  ASSERT_TRUE(ceph_readdir(cmount, ls_dir) == NULL);

  ASSERT_EQ(0, ceph_closedir(cmount, ls_dir));
  ASSERT_EQ(0, ceph_unlink(cmount, bazstr));
  ASSERT_EQ(0, ceph_rmdir(cmount, foostr));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, FdopendirReaddirTestWithDelete) {
  pid_t mypid = getpid();

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char foostr[256];
  sprintf(foostr, "/dir_ls%d", mypid);
  ASSERT_EQ(ceph_mkdir(cmount, foostr, 0777), 0);

  char bazstr[512];
  sprintf(bazstr, "%s/elif", foostr);
  int fd  = ceph_open(cmount, bazstr, O_CREAT|O_RDONLY, 0666);
  ASSERT_LE(0, fd);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  fd = ceph_open(cmount, foostr, O_DIRECTORY | O_RDONLY, 0);
  ASSERT_LE(0, fd);
  struct ceph_dir_result *ls_dir = NULL;
  ASSERT_EQ(ceph_fdopendir(cmount, fd, &ls_dir), 0);

  ASSERT_EQ(0, ceph_unlink(cmount, bazstr));
  ASSERT_EQ(0, ceph_rmdir(cmount, foostr));

  // not guaranteed to get . and .. first, but its a safe assumption
  // in this case. also, note that we may or may not get other
  // entries.
  struct dirent *result = ceph_readdir(cmount, ls_dir);
  ASSERT_TRUE(result != NULL);
  ASSERT_STREQ(result->d_name, ".");
  result = ceph_readdir(cmount, ls_dir);
  ASSERT_TRUE(result != NULL);
  ASSERT_STREQ(result->d_name, "..");

  ASSERT_EQ(0, ceph_close(cmount, fd));
  ASSERT_EQ(0, ceph_closedir(cmount, ls_dir));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, FdopendirOnNonDir) {
  pid_t mypid = getpid();

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char foostr[256];
  sprintf(foostr, "/dir_ls%d", mypid);
  ASSERT_EQ(ceph_mkdir(cmount, foostr, 0777), 0);

  char bazstr[512];
  sprintf(bazstr, "%s/file", foostr);
  int fd  = ceph_open(cmount, bazstr, O_CREAT|O_RDONLY, 0666);
  ASSERT_LE(0, fd);

  struct ceph_dir_result *ls_dir = NULL;
  ASSERT_EQ(ceph_fdopendir(cmount, fd, &ls_dir), -CEPHFS_ENOTDIR);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  ASSERT_EQ(0, ceph_unlink(cmount, bazstr));
  ASSERT_EQ(0, ceph_rmdir(cmount, foostr));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, Mkdirat) {
  pid_t mypid = getpid();

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char dir_name[128];
  char dir_path1[256];
  sprintf(dir_name, "dir_%d", mypid);
  sprintf(dir_path1, "/%s", dir_name);

  char dir_path2[512];
  char rel_dir_path2[512];
  sprintf(dir_path2, "%s/dir_%d", dir_path1, mypid);
  sprintf(rel_dir_path2, "%s/dir_%d", dir_name, mypid);

  int fd = ceph_open(cmount, "/", O_DIRECTORY | O_RDONLY, 0);
  ASSERT_LE(0, fd);

  ASSERT_EQ(0, ceph_mkdirat(cmount, fd, dir_name, 0777));
  ASSERT_EQ(0, ceph_mkdirat(cmount, fd, rel_dir_path2, 0666));

  ASSERT_EQ(0, ceph_close(cmount, fd));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path2));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path1));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, MkdiratATFDCWD) {
  pid_t mypid = getpid();

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char dir_name[128];
  char dir_path1[256];
  sprintf(dir_name, "dir_%d", mypid);
  sprintf(dir_path1, "/%s", dir_name);

  char dir_path2[512];
  sprintf(dir_path2, "%s/dir_%d", dir_path1, mypid);

  ASSERT_EQ(0, ceph_mkdirat(cmount, CEPHFS_AT_FDCWD, dir_name, 0777));

  ASSERT_EQ(0, ceph_chdir(cmount, dir_path1));
  ASSERT_EQ(0, ceph_mkdirat(cmount, CEPHFS_AT_FDCWD, dir_name, 0666));

  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path2));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path1));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, Readlinkat) {
  pid_t mypid = getpid();

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char dir_name[128];
  char dir_path[256];
  sprintf(dir_name, "dir_%d", mypid);
  sprintf(dir_path, "/%s", dir_name);
  ASSERT_EQ(ceph_mkdir(cmount, dir_path, 0777), 0);

  char file_path[512];
  char rel_file_path[512];
  sprintf(rel_file_path, "%s/elif", dir_name);
  sprintf(file_path, "%s/elif", dir_path);
  int fd  = ceph_open(cmount, file_path, O_CREAT|O_RDONLY, 0666);
  ASSERT_LE(0, fd);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  char link_path[128];
  char rel_link_path[64];
  sprintf(rel_link_path, "linkfile_%d", mypid);
  sprintf(link_path, "/%s", rel_link_path);
  ASSERT_EQ(0, ceph_symlink(cmount, rel_file_path, link_path));

  fd = ceph_open(cmount, "/", O_DIRECTORY | O_RDONLY, 0);
  ASSERT_LE(0, fd);
  size_t target_len = strlen(rel_file_path);
  char target[target_len+1];
  ASSERT_EQ(target_len, ceph_readlinkat(cmount, fd, rel_link_path, target, target_len));
  target[target_len] = '\0';
  ASSERT_EQ(0, memcmp(target, rel_file_path, target_len));

  ASSERT_EQ(0, ceph_close(cmount, fd));
  ASSERT_EQ(0, ceph_unlink(cmount, link_path));
  ASSERT_EQ(0, ceph_unlink(cmount, file_path));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, ReadlinkatATFDCWD) {
  pid_t mypid = getpid();

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char dir_name[128];
  char dir_path[256];
  sprintf(dir_name, "dir_%d", mypid);
  sprintf(dir_path, "/%s", dir_name);
  ASSERT_EQ(ceph_mkdir(cmount, dir_path, 0777), 0);

  char file_path[512];
  char rel_file_path[512] = "./elif";
  sprintf(file_path, "%s/elif", dir_path);
  int fd  = ceph_open(cmount, file_path, O_CREAT|O_RDONLY, 0666);
  ASSERT_LE(0, fd);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  char link_path[PATH_MAX];
  char rel_link_path[1024];
  sprintf(rel_link_path, "./linkfile_%d", mypid);
  sprintf(link_path, "%s/%s", dir_path, rel_link_path);
  ASSERT_EQ(0, ceph_symlink(cmount, rel_file_path, link_path));

  ASSERT_EQ(0, ceph_chdir(cmount, dir_path));
  size_t target_len = strlen(rel_file_path);
  char target[target_len+1];
  ASSERT_EQ(target_len, ceph_readlinkat(cmount, CEPHFS_AT_FDCWD, rel_link_path, target, target_len));
  target[target_len] = '\0';
  ASSERT_EQ(0, memcmp(target, rel_file_path, target_len));

  ASSERT_EQ(0, ceph_unlink(cmount, link_path));
  ASSERT_EQ(0, ceph_unlink(cmount, file_path));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, Symlinkat) {
  pid_t mypid = getpid();

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char dir_name[128];
  char dir_path[256];
  sprintf(dir_name, "dir_%d", mypid);
  sprintf(dir_path, "/%s", dir_name);
  ASSERT_EQ(ceph_mkdir(cmount, dir_path, 0777), 0);

  char file_path[512];
  char rel_file_path[512];
  sprintf(rel_file_path, "%s/elif", dir_name);
  sprintf(file_path, "%s/elif", dir_path);

  int fd  = ceph_open(cmount, file_path, O_CREAT|O_RDONLY, 0666);
  ASSERT_LE(0, fd);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  char link_path[128];
  char rel_link_path[64];
  sprintf(rel_link_path, "linkfile_%d", mypid);
  sprintf(link_path, "/%s", rel_link_path);

  fd = ceph_open(cmount, "/", O_DIRECTORY | O_RDONLY, 0);
  ASSERT_LE(0, fd);
  ASSERT_EQ(0, ceph_symlinkat(cmount, rel_file_path, fd, rel_link_path));

  size_t target_len = strlen(rel_file_path);
  char target[target_len+1];
  ASSERT_EQ(target_len, ceph_readlinkat(cmount, fd, rel_link_path, target, target_len));
  target[target_len] = '\0';
  ASSERT_EQ(0, memcmp(target, rel_file_path, target_len));

  ASSERT_EQ(0, ceph_close(cmount, fd));
  ASSERT_EQ(0, ceph_unlink(cmount, link_path));
  ASSERT_EQ(0, ceph_unlink(cmount, file_path));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, SymlinkatATFDCWD) {
  pid_t mypid = getpid();

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char dir_name[128];
  char dir_path[256];
  sprintf(dir_name, "dir_%d", mypid);
  sprintf(dir_path, "/%s", dir_name);
  ASSERT_EQ(ceph_mkdir(cmount, dir_path, 0777), 0);

  char file_path[512];
  char rel_file_path[512] = "./elif";
  sprintf(file_path, "%s/elif", dir_path);
  int fd  = ceph_open(cmount, file_path, O_CREAT|O_RDONLY, 0666);
  ASSERT_LE(0, fd);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  char link_path[PATH_MAX];
  char rel_link_path[1024];
  sprintf(rel_link_path, "./linkfile_%d", mypid);
  sprintf(link_path, "%s/%s", dir_path, rel_link_path);
  ASSERT_EQ(0, ceph_chdir(cmount, dir_path));
  ASSERT_EQ(0, ceph_symlinkat(cmount, rel_file_path, CEPHFS_AT_FDCWD, rel_link_path));

  size_t target_len = strlen(rel_file_path);
  char target[target_len+1];
  ASSERT_EQ(target_len, ceph_readlinkat(cmount, CEPHFS_AT_FDCWD, rel_link_path, target, target_len));
  target[target_len] = '\0';
  ASSERT_EQ(0, memcmp(target, rel_file_path, target_len));

  ASSERT_EQ(0, ceph_unlink(cmount, link_path));
  ASSERT_EQ(0, ceph_unlink(cmount, file_path));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, Unlinkat) {
  pid_t mypid = getpid();

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char dir_name[128];
  char dir_path[256];
  sprintf(dir_name, "dir_%d", mypid);
  sprintf(dir_path, "/%s", dir_name);
  ASSERT_EQ(ceph_mkdir(cmount, dir_path, 0777), 0);

  char file_path[512];
  char rel_file_path[512] = "elif";
  sprintf(file_path, "%s/elif", dir_path);

  int fd  = ceph_open(cmount, file_path, O_CREAT|O_RDONLY, 0666);
  ASSERT_LE(0, fd);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  fd = ceph_open(cmount, dir_path, O_DIRECTORY | O_RDONLY, 0);
  ASSERT_LE(0, fd);
  ASSERT_EQ(-CEPHFS_ENOTDIR, ceph_unlinkat(cmount, fd, rel_file_path, AT_REMOVEDIR));
  ASSERT_EQ(0, ceph_unlinkat(cmount, fd, rel_file_path, 0));
  ASSERT_EQ(0, ceph_close(cmount, fd));

  fd = ceph_open(cmount, "/", O_DIRECTORY | O_RDONLY, 0);
  ASSERT_EQ(-CEPHFS_EISDIR, ceph_unlinkat(cmount, fd, dir_name, 0));
  ASSERT_EQ(0, ceph_unlinkat(cmount, fd, dir_name, AT_REMOVEDIR));
  ASSERT_LE(0, fd);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, UnlinkatATFDCWD) {
  pid_t mypid = getpid();

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char dir_name[128];
  char dir_path[256];
  sprintf(dir_name, "dir_%d", mypid);
  sprintf(dir_path, "/%s", dir_name);
  ASSERT_EQ(ceph_mkdir(cmount, dir_path, 0777), 0);

  char file_path[512];
  char rel_file_path[512] = "elif";
  sprintf(file_path, "%s/elif", dir_path);

  int fd  = ceph_open(cmount, file_path, O_CREAT|O_RDONLY, 0666);
  ASSERT_LE(0, fd);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  ASSERT_EQ(0, ceph_chdir(cmount, dir_path));
  ASSERT_EQ(-CEPHFS_ENOTDIR, ceph_unlinkat(cmount, CEPHFS_AT_FDCWD, rel_file_path, AT_REMOVEDIR));
  ASSERT_EQ(0, ceph_unlinkat(cmount, CEPHFS_AT_FDCWD, rel_file_path, 0));

  ASSERT_EQ(0, ceph_chdir(cmount, "/"));
  ASSERT_EQ(-CEPHFS_EISDIR, ceph_unlinkat(cmount, CEPHFS_AT_FDCWD, dir_name, 0));
  ASSERT_EQ(0, ceph_unlinkat(cmount, CEPHFS_AT_FDCWD, dir_name, AT_REMOVEDIR));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, Chownat) {
  pid_t mypid = getpid();

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char dir_name[128];
  char dir_path[256];
  sprintf(dir_name, "dir_%d", mypid);
  sprintf(dir_path, "/%s", dir_name);
  ASSERT_EQ(ceph_mkdir(cmount, dir_path, 0777), 0);

  char file_path[512];
  char rel_file_path[512] = "elif";
  sprintf(file_path, "%s/elif", dir_path);
  int fd = ceph_open(cmount, file_path, O_CREAT|O_RDWR, 0666);
  ASSERT_LE(0, fd);

  // set perms to readable and writeable only by owner
  ASSERT_EQ(ceph_fchmod(cmount, fd, 0600), 0);
  ceph_close(cmount, fd);

  fd = ceph_open(cmount, dir_path, O_DIRECTORY | O_RDONLY, 0);
  // change ownership to nobody -- we assume nobody exists and id is always 65534
  ASSERT_EQ(ceph_conf_set(cmount, "client_permissions", "0"), 0);
  ASSERT_EQ(ceph_chownat(cmount, fd, rel_file_path, 65534, 65534, 0), 0);
  ASSERT_EQ(ceph_conf_set(cmount, "client_permissions", "1"), 0);
  ceph_close(cmount, fd);

  // "nobody" will be ignored on Windows
  #ifndef _WIN32
  fd = ceph_open(cmount, file_path, O_RDWR, 0);
  ASSERT_EQ(fd, -CEPHFS_EACCES);
  #endif

  ASSERT_EQ(ceph_conf_set(cmount, "client_permissions", "0"), 0);
  ASSERT_EQ(0, ceph_unlink(cmount, file_path));
  ASSERT_EQ(ceph_conf_set(cmount, "client_permissions", "1"), 0);
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, ChownatATFDCWD) {
  pid_t mypid = getpid();

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char dir_name[128];
  char dir_path[256];
  sprintf(dir_name, "dir_%d", mypid);
  sprintf(dir_path, "/%s", dir_name);
  ASSERT_EQ(ceph_mkdir(cmount, dir_path, 0777), 0);

  char file_path[512];
  char rel_file_path[512] = "elif";
  sprintf(file_path, "%s/elif", dir_path);
  int fd = ceph_open(cmount, file_path, O_CREAT|O_RDWR, 0666);
  ASSERT_LE(0, fd);

  // set perms to readable and writeable only by owner
  ASSERT_EQ(ceph_fchmod(cmount, fd, 0600), 0);
  ceph_close(cmount, fd);

  ASSERT_EQ(0, ceph_chdir(cmount, dir_path));
  // change ownership to nobody -- we assume nobody exists and id is always 65534
  ASSERT_EQ(ceph_conf_set(cmount, "client_permissions", "0"), 0);
  ASSERT_EQ(ceph_chownat(cmount, CEPHFS_AT_FDCWD, rel_file_path, 65534, 65534, 0), 0);
  ASSERT_EQ(ceph_conf_set(cmount, "client_permissions", "1"), 0);

  // "nobody" will be ignored on Windows
  #ifndef _WIN32
  fd = ceph_open(cmount, file_path, O_RDWR, 0);
  ASSERT_EQ(fd, -CEPHFS_EACCES);
  #endif

  ASSERT_EQ(ceph_conf_set(cmount, "client_permissions", "0"), 0);
  ASSERT_EQ(0, ceph_unlink(cmount, file_path));
  ASSERT_EQ(ceph_conf_set(cmount, "client_permissions", "1"), 0);
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, Chmodat) {
  pid_t mypid = getpid();

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char dir_name[128];
  char dir_path[256];
  sprintf(dir_name, "dir_%d", mypid);
  sprintf(dir_path, "/%s", dir_name);
  ASSERT_EQ(ceph_mkdir(cmount, dir_path, 0777), 0);

  char file_path[512];
  char rel_file_path[512] = "elif";
  sprintf(file_path, "%s/elif", dir_path);
  int fd = ceph_open(cmount, file_path, O_CREAT|O_RDWR, 0666);
  ASSERT_LE(0, fd);
  const char *bytes = "foobarbaz";
  ASSERT_EQ(ceph_write(cmount, fd, bytes, strlen(bytes), 0), (int)strlen(bytes));
  ASSERT_EQ(0, ceph_close(cmount, fd));

  fd = ceph_open(cmount, dir_path, O_DIRECTORY | O_RDONLY, 0);

  // set perms to read but can't write
  ASSERT_EQ(ceph_chmodat(cmount, fd, rel_file_path, 0400, 0), 0);
  ASSERT_EQ(ceph_open(cmount, file_path, O_RDWR, 0), -CEPHFS_EACCES);

  // reset back to writeable
  ASSERT_EQ(ceph_chmodat(cmount, fd, rel_file_path, 0600, 0), 0);
  int fd2 = ceph_open(cmount, file_path, O_RDWR, 0);
  ASSERT_LE(0, fd2);

  ASSERT_EQ(0, ceph_close(cmount, fd2));
  ASSERT_EQ(0, ceph_close(cmount, fd));

  ASSERT_EQ(0, ceph_unlink(cmount, file_path));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, ChmodatATFDCWD) {
  pid_t mypid = getpid();

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, "/"), 0);

  char dir_name[128];
  char dir_path[256];
  sprintf(dir_name, "dir_%d", mypid);
  sprintf(dir_path, "/%s", dir_name);
  ASSERT_EQ(ceph_mkdir(cmount, dir_path, 0777), 0);

  char file_path[512];
  char rel_file_path[512] = "elif";
  sprintf(file_path, "%s/elif", dir_path);
  int fd = ceph_open(cmount, file_path, O_CREAT|O_RDWR, 0666);
  ASSERT_LE(0, fd);
  const char *bytes = "foobarbaz";
  ASSERT_EQ(ceph_write(cmount, fd, bytes, strlen(bytes), 0), (int)strlen(bytes));
  ASSERT_EQ(0, ceph_close(cmount, fd));

  // set perms to read but can't write
  ASSERT_EQ(0, ceph_chdir(cmount, dir_path));
  ASSERT_EQ(ceph_chmodat(cmount, CEPHFS_AT_FDCWD, rel_file_path, 0400, 0), 0);
  ASSERT_EQ(ceph_open(cmount, file_path, O_RDWR, 0), -CEPHFS_EACCES);

  // reset back to writeable
  ASSERT_EQ(ceph_chmodat(cmount, CEPHFS_AT_FDCWD, rel_file_path, 0600, 0), 0);
  int fd2 = ceph_open(cmount, file_path, O_RDWR, 0);
  ASSERT_LE(0, fd2);
  ASSERT_EQ(0, ceph_close(cmount, fd2));

  ASSERT_EQ(0, ceph_unlink(cmount, file_path));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, Utimensat) {
  pid_t mypid = getpid();

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char dir_name[128];
  char dir_path[256];
  sprintf(dir_name, "dir_%d", mypid);
  sprintf(dir_path, "/%s", dir_name);
  ASSERT_EQ(ceph_mkdir(cmount, dir_path, 0777), 0);

  char file_path[512];
  char rel_file_path[512] = "elif";
  sprintf(file_path, "%s/elif", dir_path);
  int fd = ceph_open(cmount, file_path, O_CREAT|O_RDWR, 0666);
  ASSERT_LE(0, fd);

  struct timespec times[2];
  get_current_time_timespec(times);

  fd = ceph_open(cmount, dir_path, O_DIRECTORY | O_RDONLY, 0);
  ASSERT_LE(0, fd);
  EXPECT_EQ(0, ceph_utimensat(cmount, fd, rel_file_path, times, 0));
  ceph_close(cmount, fd);

  struct ceph_statx stx;
  ASSERT_EQ(ceph_statx(cmount, file_path, &stx,
                       CEPH_STATX_MTIME|CEPH_STATX_ATIME, 0), 0);
  ASSERT_EQ(utime_t(stx.stx_atime), utime_t(times[0]));
  ASSERT_EQ(utime_t(stx.stx_mtime), utime_t(times[1]));

  ASSERT_EQ(0, ceph_unlink(cmount, file_path));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, UtimensatATFDCWD) {
  pid_t mypid = getpid();

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char dir_name[128];
  char dir_path[256];
  sprintf(dir_name, "dir_%d", mypid);
  sprintf(dir_path, "/%s", dir_name);
  ASSERT_EQ(ceph_mkdir(cmount, dir_path, 0777), 0);

  char file_path[512];
  char rel_file_path[512] = "elif";
  sprintf(file_path, "%s/elif", dir_path);
  int fd = ceph_open(cmount, file_path, O_CREAT|O_RDWR, 0666);
  ASSERT_LE(0, fd);

  struct timespec times[2];
  get_current_time_timespec(times);

  ASSERT_EQ(0, ceph_chdir(cmount, dir_path));
  EXPECT_EQ(0, ceph_utimensat(cmount, CEPHFS_AT_FDCWD, rel_file_path, times, 0));

  struct ceph_statx stx;
  ASSERT_EQ(ceph_statx(cmount, file_path, &stx,
                       CEPH_STATX_MTIME|CEPH_STATX_ATIME, 0), 0);
  ASSERT_EQ(utime_t(stx.stx_atime), utime_t(times[0]));
  ASSERT_EQ(utime_t(stx.stx_mtime), utime_t(times[1]));

  ASSERT_EQ(0, ceph_unlink(cmount, file_path));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, LookupMdsPrivateInos) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  Inode *inode;
  for (int ino = 0; ino < MDS_INO_SYSTEM_BASE; ino++) {
    if (MDS_IS_PRIVATE_INO(ino)) {
      ASSERT_EQ(-CEPHFS_ESTALE, ceph_ll_lookup_inode(cmount, ino, &inode));
    } else if (ino == CEPH_INO_ROOT || ino == CEPH_INO_GLOBAL_SNAPREALM) {
      ASSERT_EQ(0, ceph_ll_lookup_inode(cmount, ino, &inode));
      ceph_ll_put(cmount, inode);
    } else if (ino == CEPH_INO_LOST_AND_FOUND) {
      // the ino 3 will only exists after the recovery tool ran, so
      // it may return -CEPHFS_ESTALE with a fresh fs cluster
      int r = ceph_ll_lookup_inode(cmount, ino, &inode);
      if (r == 0) {
        ceph_ll_put(cmount, inode);
      } else {
        ASSERT_TRUE(r == -CEPHFS_ESTALE);
      }
    } else {
      // currently the ino 0 and 4~99 is not useded yet.
      ASSERT_EQ(-CEPHFS_ESTALE, ceph_ll_lookup_inode(cmount, ino, &inode));
    }
  }

  ceph_shutdown(cmount);
}

TEST(LibCephFS, SetMountTimeoutPostMount) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  ASSERT_EQ(-CEPHFS_EINVAL, ceph_set_mount_timeout(cmount, 5));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, SetMountTimeout) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_set_mount_timeout(cmount, 5));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);
  ceph_shutdown(cmount);
}

TEST(LibCephFS, SnapdirAttrs) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char dir_name[128];
  char dir_path[256];
  char snap_dir_path[512];

  pid_t mypid = getpid();
  sprintf(dir_name, "dir_%d", mypid);
  sprintf(dir_path, "/%s", dir_name);
  sprintf(snap_dir_path, "%s/.snap", dir_path);

  Inode *dir, *root;
  struct ceph_statx stx_dir;
  struct ceph_statx stx_snap_dir;
  struct ceph_statx stx_root_snap_dir;
  UserPerm *perms = ceph_mount_perms(cmount);

  ASSERT_EQ(ceph_ll_lookup_root(cmount, &root), 0);
  ASSERT_EQ(ceph_ll_mkdir(cmount, root, dir_name, 0755, &dir, &stx_dir, 0, 0, perms), 0);

  ASSERT_EQ(ceph_statx(cmount, dir_path, &stx_dir,
                       CEPH_STATX_MTIME|CEPH_STATX_ATIME|CEPH_STATX_MODE|CEPH_STATX_MODE|CEPH_STATX_GID|CEPH_STATX_VERSION, 0), 0);
  ASSERT_EQ(ceph_statx(cmount, snap_dir_path, &stx_snap_dir,
                       CEPH_STATX_MTIME|CEPH_STATX_ATIME|CEPH_STATX_MODE|CEPH_STATX_MODE|CEPH_STATX_GID|CEPH_STATX_VERSION, 0), 0);
  ASSERT_EQ(ceph_statx(cmount, "/.snap", &stx_root_snap_dir,
                       CEPH_STATX_MTIME|CEPH_STATX_ATIME|CEPH_STATX_MODE|CEPH_STATX_MODE|CEPH_STATX_GID|CEPH_STATX_VERSION, 0), 0);

  // these should match the parent directories attrs
  ASSERT_EQ(stx_dir.stx_mode, stx_snap_dir.stx_mode);
  ASSERT_EQ(stx_dir.stx_uid, stx_snap_dir.stx_uid);
  ASSERT_EQ(stx_dir.stx_gid, stx_snap_dir.stx_gid);
  ASSERT_EQ(utime_t(stx_dir.stx_atime), utime_t(stx_snap_dir.stx_atime));
  // these should match the closest snaprealm ancestor (root in this
  // case) attrs
  ASSERT_EQ(utime_t(stx_root_snap_dir.stx_mtime), utime_t(stx_snap_dir.stx_mtime));
  ASSERT_EQ(utime_t(stx_root_snap_dir.stx_ctime), utime_t(stx_snap_dir.stx_ctime));
  ASSERT_EQ(stx_root_snap_dir.stx_version, stx_snap_dir.stx_version);

  // chown  -- for this we need to be "root"
  UserPerm *rootcred = ceph_userperm_new(0, 0, 0, NULL);
  ASSERT_TRUE(rootcred);
  stx_dir.stx_uid++;
  stx_dir.stx_gid++;
  ASSERT_EQ(ceph_ll_setattr(cmount, dir, &stx_dir, CEPH_SETATTR_UID|CEPH_SETATTR_GID, rootcred), 0);

  memset(&stx_dir, 0, sizeof(stx_dir));
  memset(&stx_snap_dir, 0, sizeof(stx_snap_dir));

  ASSERT_EQ(ceph_statx(cmount, dir_path, &stx_dir,
                       CEPH_STATX_MTIME|CEPH_STATX_ATIME|CEPH_STATX_MODE|CEPH_STATX_MODE|CEPH_STATX_GID|CEPH_STATX_VERSION, 0), 0);
  ASSERT_EQ(ceph_statx(cmount, snap_dir_path, &stx_snap_dir,
                       CEPH_STATX_MTIME|CEPH_STATX_ATIME|CEPH_STATX_MODE|CEPH_STATX_MODE|CEPH_STATX_GID|CEPH_STATX_VERSION, 0), 0);

  ASSERT_EQ(stx_dir.stx_mode, stx_snap_dir.stx_mode);
  ASSERT_EQ(stx_dir.stx_uid, stx_snap_dir.stx_uid);
  ASSERT_EQ(stx_dir.stx_gid, stx_snap_dir.stx_gid);
  ASSERT_EQ(utime_t(stx_dir.stx_atime), utime_t(stx_snap_dir.stx_atime));
  ASSERT_EQ(utime_t(stx_root_snap_dir.stx_mtime), utime_t(stx_snap_dir.stx_mtime));
  ASSERT_EQ(utime_t(stx_root_snap_dir.stx_ctime), utime_t(stx_snap_dir.stx_ctime));
  ASSERT_EQ(stx_root_snap_dir.stx_version, stx_snap_dir.stx_version);

  ASSERT_EQ(ceph_ll_rmdir(cmount, root, dir_name, rootcred), 0);
  ASSERT_EQ(0, ceph_unmount(cmount));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, SnapdirAttrsOnSnapCreate) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char dir_name[128];
  char dir_path[256];
  char snap_dir_path[512];

  pid_t mypid = getpid();
  sprintf(dir_name, "dir_%d", mypid);
  sprintf(dir_path, "/%s", dir_name);
  sprintf(snap_dir_path, "%s/.snap", dir_path);

  ASSERT_EQ(ceph_mkdir(cmount, dir_path, 0777), 0);

  struct ceph_statx stx_dir;
  struct ceph_statx stx_snap_dir;
  struct ceph_statx stx_root_snap_dir;
  ASSERT_EQ(ceph_statx(cmount, dir_path, &stx_dir, CEPH_STATX_MTIME|CEPH_STATX_CTIME|CEPH_STATX_VERSION, 0), 0);
  ASSERT_EQ(ceph_statx(cmount, snap_dir_path, &stx_snap_dir, CEPH_STATX_MTIME|CEPH_STATX_CTIME|CEPH_STATX_VERSION, 0), 0);
  ASSERT_EQ(ceph_statx(cmount, "/.snap", &stx_root_snap_dir, CEPH_STATX_MTIME|CEPH_STATX_CTIME|CEPH_STATX_VERSION, 0), 0);
  ASSERT_EQ(utime_t(stx_root_snap_dir.stx_mtime), utime_t(stx_snap_dir.stx_mtime));
  ASSERT_EQ(utime_t(stx_root_snap_dir.stx_ctime), utime_t(stx_snap_dir.stx_ctime));
  ASSERT_EQ(stx_root_snap_dir.stx_version, stx_snap_dir.stx_version);

  char snap_path[1024];
  sprintf(snap_path, "%s/snap_a", snap_dir_path);
  ASSERT_EQ(ceph_mkdir(cmount, snap_path, 0777), 0);

  struct ceph_statx stx_snap_dir_1;
  ASSERT_EQ(ceph_statx(cmount, snap_dir_path, &stx_snap_dir_1, CEPH_STATX_MTIME|CEPH_STATX_CTIME|CEPH_STATX_VERSION, 0), 0);
  ASSERT_LT(utime_t(stx_root_snap_dir.stx_mtime), utime_t(stx_snap_dir_1.stx_mtime));
  ASSERT_LT(utime_t(stx_root_snap_dir.stx_ctime), utime_t(stx_snap_dir_1.stx_ctime));
  ASSERT_LT(stx_root_snap_dir.stx_version, stx_snap_dir_1.stx_version);

  ASSERT_EQ(0, ceph_rmdir(cmount, snap_path));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path));
  ASSERT_EQ(0, ceph_unmount(cmount));
  ceph_shutdown(cmount);
}


TEST(LibCephFS, SnapdirAttrsOnSnapDelete) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char dir_name[128];
  char dir_path[256];
  char snap_dir_path[512];

  pid_t mypid = getpid();
  sprintf(dir_name, "dir_%d", mypid);
  sprintf(dir_path, "/%s", dir_name);
  sprintf(snap_dir_path, "%s/.snap", dir_path);

  ASSERT_EQ(ceph_mkdir(cmount, dir_path, 0777), 0);

  struct ceph_statx stx_dir;
  struct ceph_statx stx_snap_dir;
  struct ceph_statx stx_root_snap_dir;
  ASSERT_EQ(ceph_statx(cmount, dir_path, &stx_dir, CEPH_STATX_MTIME|CEPH_STATX_CTIME|CEPH_STATX_VERSION, 0), 0);
  ASSERT_EQ(ceph_statx(cmount, snap_dir_path, &stx_snap_dir, CEPH_STATX_MTIME|CEPH_STATX_CTIME|CEPH_STATX_VERSION, 0), 0);
  ASSERT_EQ(ceph_statx(cmount, "/.snap", &stx_root_snap_dir, CEPH_STATX_MTIME|CEPH_STATX_CTIME|CEPH_STATX_VERSION, 0), 0);
  ASSERT_EQ(utime_t(stx_root_snap_dir.stx_mtime), utime_t(stx_snap_dir.stx_mtime));
  ASSERT_EQ(utime_t(stx_root_snap_dir.stx_ctime), utime_t(stx_snap_dir.stx_mtime));
  ASSERT_EQ(stx_root_snap_dir.stx_version, stx_snap_dir.stx_version);

  char snap_path[1024];
  sprintf(snap_path, "%s/snap_a", snap_dir_path);
  ASSERT_EQ(ceph_mkdir(cmount, snap_path, 0777), 0);

  struct ceph_statx stx_snap_dir_1;
  ASSERT_EQ(ceph_statx(cmount, snap_dir_path, &stx_snap_dir_1, CEPH_STATX_MTIME|CEPH_STATX_CTIME|CEPH_STATX_VERSION, 0), 0);
  ASSERT_LT(utime_t(stx_root_snap_dir.stx_mtime), utime_t(stx_snap_dir_1.stx_mtime));
  ASSERT_LT(utime_t(stx_root_snap_dir.stx_ctime), utime_t(stx_snap_dir_1.stx_ctime));
  ASSERT_LT(stx_root_snap_dir.stx_version, stx_snap_dir_1.stx_version);

  ASSERT_EQ(0, ceph_rmdir(cmount, snap_path));

  struct ceph_statx stx_snap_dir_2;
  ASSERT_EQ(ceph_statx(cmount, snap_dir_path, &stx_snap_dir_2, CEPH_STATX_MTIME|CEPH_STATX_CTIME|CEPH_STATX_VERSION, 0), 0);
  // Flaky assertion on Windows, potentially due to timestamp precision.
  #ifndef _WIN32
  ASSERT_LT(utime_t(stx_snap_dir_1.stx_mtime), utime_t(stx_snap_dir_2.stx_mtime));
  ASSERT_LT(utime_t(stx_snap_dir_1.stx_ctime), utime_t(stx_snap_dir_2.stx_ctime));
  #endif
  ASSERT_LT(stx_snap_dir_1.stx_version, stx_snap_dir_2.stx_version);

  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path));
  ASSERT_EQ(0, ceph_unmount(cmount));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, SnapdirAttrsOnSnapRename) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char dir_name[128];
  char dir_path[256];
  char snap_dir_path[512];

  pid_t mypid = getpid();
  sprintf(dir_name, "dir_%d", mypid);
  sprintf(dir_path, "/%s", dir_name);
  sprintf(snap_dir_path, "%s/.snap", dir_path);

  ASSERT_EQ(ceph_mkdir(cmount, dir_path, 0777), 0);

  struct ceph_statx stx_dir;
  struct ceph_statx stx_snap_dir;
  struct ceph_statx stx_root_snap_dir;
  ASSERT_EQ(ceph_statx(cmount, dir_path, &stx_dir, CEPH_STATX_MTIME|CEPH_STATX_CTIME|CEPH_STATX_VERSION, 0), 0);
  ASSERT_EQ(ceph_statx(cmount, snap_dir_path, &stx_snap_dir, CEPH_STATX_MTIME|CEPH_STATX_CTIME|CEPH_STATX_VERSION, 0), 0);
  ASSERT_EQ(ceph_statx(cmount, "/.snap", &stx_root_snap_dir, CEPH_STATX_MTIME|CEPH_STATX_CTIME|CEPH_STATX_VERSION, 0), 0);
  ASSERT_EQ(utime_t(stx_root_snap_dir.stx_mtime), utime_t(stx_snap_dir.stx_mtime));
  ASSERT_EQ(utime_t(stx_root_snap_dir.stx_ctime), utime_t(stx_snap_dir.stx_ctime));
  ASSERT_EQ(stx_root_snap_dir.stx_version, stx_snap_dir.stx_version);

  char snap_path[1024];
  sprintf(snap_path, "%s/snap_a", snap_dir_path);
  ASSERT_EQ(ceph_mkdir(cmount, snap_path, 0777), 0);

  struct ceph_statx stx_snap_dir_1;
  ASSERT_EQ(ceph_statx(cmount, snap_dir_path, &stx_snap_dir_1, CEPH_STATX_MTIME|CEPH_STATX_CTIME|CEPH_STATX_VERSION, 0), 0);
  ASSERT_LT(utime_t(stx_root_snap_dir.stx_mtime), utime_t(stx_snap_dir_1.stx_mtime));
  ASSERT_LT(utime_t(stx_root_snap_dir.stx_ctime), utime_t(stx_snap_dir_1.stx_ctime));
  ASSERT_LT(stx_root_snap_dir.stx_version, stx_snap_dir_1.stx_version);

  char snap_path_r[1024];
  sprintf(snap_path_r, "%s/snap_b", snap_dir_path);
  ASSERT_EQ(ceph_rename(cmount, snap_path, snap_path_r), 0);

  struct ceph_statx stx_snap_dir_2;
  ASSERT_EQ(ceph_statx(cmount, snap_dir_path, &stx_snap_dir_2, CEPH_STATX_MTIME|CEPH_STATX_CTIME|CEPH_STATX_VERSION, 0), 0);
  // Flaky assertion on Windows, potentially due to timestamp precision.
  #ifndef _WIN32
  ASSERT_LT(utime_t(stx_snap_dir_1.stx_mtime), utime_t(stx_snap_dir_2.stx_mtime));
  ASSERT_LT(utime_t(stx_snap_dir_1.stx_ctime), utime_t(stx_snap_dir_2.stx_ctime));
  #endif
  ASSERT_LT(stx_snap_dir_1.stx_version, stx_snap_dir_2.stx_version);

  ASSERT_EQ(0, ceph_rmdir(cmount, snap_path_r));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path));
  ASSERT_EQ(0, ceph_unmount(cmount));
  ceph_shutdown(cmount);
}
