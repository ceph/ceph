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
#include "include/cephfs/libcephfs.h"
#include <errno.h>
#include <sys/fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <sys/uio.h>
#if defined(__FreeBSD__)
#include <sys/extattr.h>
#define XATTR_CREATE    0x1
#define XATTR_REPLACE   0x2
#else
#include <sys/xattr.h>
#endif

#ifdef __linux__
#include <limits.h>
#endif

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
  ASSERT_EQ(ceph_read(cmount, fd, in_buf, sizeof(in_buf), 0), -EBADF);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  fd = ceph_open(cmount, c_path, O_RDONLY, 0);
  ASSERT_LT(0, fd);
  ASSERT_EQ(ceph_write(cmount, fd, out_buf, size, 0), -EBADF);
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
  ASSERT_EQ(-EISCONN, ceph_mount(cmount, "/"));
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
  ASSERT_EQ(-ENOTCONN, ceph_unmount(cmount));
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
  ASSERT_EQ(-EISCONN, ceph_release(cmount));
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

  /* on already-written file (ENOTEMPTY) */
  ceph_write(cmount, fd, "hello world", 11, 0);
  ceph_close(cmount, fd);

  char xattrk[128];
  char xattrv[128];
  sprintf(xattrk, "ceph.file.layout.stripe_unit");
  sprintf(xattrv, "65536");
  ASSERT_EQ(-ENOTEMPTY, ceph_setxattr(cmount, test_layout_file, xattrk, (void *)xattrv, 5, 0));

  /* invalid layout */
  sprintf(test_layout_file, "test_layout_%d_c", getpid());
  fd = ceph_open_layout(cmount, test_layout_file, O_CREAT, 0666, (1<<20), 1, 19, NULL);
  ASSERT_EQ(fd, -EINVAL);

  /* with data pool */
  sprintf(test_layout_file, "test_layout_%d_d", getpid());
  fd = ceph_open_layout(cmount, test_layout_file, O_CREAT, 0666, (1<<20), 7, (1<<20), poolname);
  ASSERT_GT(fd, 0);
  ceph_close(cmount, fd);

  /* with metadata pool (invalid) */
  sprintf(test_layout_file, "test_layout_%d_e", getpid());
  fd = ceph_open_layout(cmount, test_layout_file, O_CREAT, 0666, (1<<20), 7, (1<<20), "metadata");
  ASSERT_EQ(fd, -EINVAL);

  /* with metadata pool (does not exist) */
  sprintf(test_layout_file, "test_layout_%d_f", getpid());
  fd = ceph_open_layout(cmount, test_layout_file, O_CREAT, 0666, (1<<20), 7, (1<<20), "asdfjasdfjasdf");
  ASSERT_EQ(fd, -EINVAL);

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
  ASSERT_EQ(ceph_opendir(cmount, foostr, &ls_dir), -ENOENT);

  ASSERT_EQ(ceph_mkdir(cmount, foostr, 0777), 0);
  struct stat stbuf;
  ASSERT_EQ(ceph_stat(cmount, foostr, &stbuf), 0);
  ASSERT_NE(S_ISDIR(stbuf.st_mode), 0);

  char barstr[256];
  sprintf(barstr, "dir_ls2%d", mypid);
  ASSERT_EQ(ceph_lstat(cmount, barstr, &stbuf), -ENOENT);

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

  for(i = 0; i < r; ++i)
    ASSERT_TRUE(ceph_readdir(cmount, ls_dir) != NULL);

  ASSERT_TRUE(ceph_readdir(cmount, ls_dir) == NULL);

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
  getdents_entries = (struct dirent *)malloc(r * sizeof(*getdents_entries));

  int count = 0;
  std::set<std::string> found;
  while (true) {
    int len = ceph_getdents(cmount, ls_dir, (char *)getdents_entries, r * sizeof(*getdents_entries));
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
      found.insert(name);
    }
  }
  ASSERT_EQ(found.size(), (unsigned)r);
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
    found.insert(rdent.d_name);
  }
  ASSERT_EQ(found.size(), (unsigned)r);

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
    struct stat st;
    int stmask;
    int len = ceph_readdirplus_r(cmount, ls_dir, &rdent, &st, &stmask);
    if (len == 0)
      break;
    ASSERT_EQ(len, 1);
    const char *name = rdent.d_name;
    found.insert(name);
    int size;
    sscanf(name, "dirf%d", &size);
    ASSERT_EQ(st.st_size, size);
    ASSERT_EQ(st.st_ino, rdent.d_ino);
    //ASSERT_EQ(st.st_mode, (mode_t)0666);
  }
  ASSERT_EQ(found.size(), (unsigned)r);

  ASSERT_EQ(ceph_closedir(cmount, ls_dir), 0);

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

  char i = 'a';
  char xattrk[128];
  char xattrv[128];
  for(; i < 'a'+26; ++i) {
    sprintf(xattrk, "user.test_xattr_%c", i);
    int len = sprintf(xattrv, "testxattr%c", i);
    ASSERT_EQ(ceph_setxattr(cmount, test_xattr_file, xattrk, (void *) xattrv, len, XATTR_CREATE), 0);
  }

  char xattrlist[128*26];
  int len = ceph_listxattr(cmount, test_xattr_file, xattrlist, sizeof(xattrlist));
  char *p = xattrlist;
  char *n;
  i = 'a';
  while (len > 0) {
    // skip/ignore the dir layout
    if (strcmp(p, "ceph.dir.layout") == 0 ||
	strcmp(p, "ceph.file.layout") == 0) {
      len -= strlen(p) + 1;
      p += strlen(p) + 1;
      continue;
    }

    sprintf(xattrk, "user.test_xattr_%c", i);
    ASSERT_STREQ(p, xattrk);

    char gxattrv[128];
    std::cout << "getting attr " << p << std::endl;
    int alen = ceph_getxattr(cmount, test_xattr_file, p, (void *) gxattrv, 128);
    ASSERT_GT(alen, 0);
    sprintf(xattrv, "testxattr%c", i);
    ASSERT_TRUE(!strncmp(xattrv, gxattrv, alen));

    n = index(p, '\0');
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
  struct stat attr;

  int res = ceph_ll_lookup_root(cmount, &root);
  ASSERT_EQ(res, 0);
  res = ceph_ll_lookup(cmount, root, test_xattr_file, &attr, &existent_file_handle, 0, 0);
  ASSERT_EQ(res, 0);

  const char *valid_name = "user.attrname";
  const char *value = "attrvalue";
  char value_buf[256] = { 0 };

  res = ceph_ll_setxattr(cmount, existent_file_handle, valid_name, value, strlen(value), 0, 0, 0);
  ASSERT_EQ(res, 0);

  res = ceph_ll_getxattr(cmount, existent_file_handle, valid_name, value_buf, 256, 0, 0);
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

  struct stat stbuf;
  ASSERT_EQ(ceph_lstat(cmount, "/.", &stbuf), 0);
  ASSERT_EQ(ceph_lstat(cmount, ".", &stbuf), 0);

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
  ASSERT_EQ(fd, -EACCES);

  fd = ceph_open(cmount, test_file, O_RDONLY, 0);
  ASSERT_GT(fd, -1);

  char buf[100];
  int ret = ceph_read(cmount, fd, buf, 100, 0);
  ASSERT_EQ(ret, (int)strlen(bytes));
  buf[ret] = '\0';
  ASSERT_STREQ(buf, bytes);

  ASSERT_EQ(ceph_write(cmount, fd, bytes, strlen(bytes), 0), -EBADF);

  ceph_close(cmount, fd);

  // reset back to writeable
  ASSERT_EQ(ceph_chmod(cmount, test_file, 0600), 0);

  // ensure perms are correct
  struct stat stbuf;
  ASSERT_EQ(ceph_lstat(cmount, test_file, &stbuf), 0);
  ASSERT_EQ(stbuf.st_mode, 0100600U);

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

  ASSERT_EQ(ceph_open(cmount, test_file, O_RDWR, 0), -EACCES);

  // reset back to writeable
  ASSERT_EQ(ceph_chmod(cmount, test_file, 0600), 0);

  fd = ceph_open(cmount, test_file, O_RDWR, 0);
  ASSERT_GT(fd, 0);

  ASSERT_EQ(ceph_write(cmount, fd, bytes, strlen(bytes), 0), (int)strlen(bytes));
  ceph_close(cmount, fd);

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

  fd = ceph_open(cmount, test_file, O_RDWR, 0);
  ASSERT_EQ(fd, -EACCES);

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
  ASSERT_EQ(-ENOENT, fd);

  fd = ceph_open(cmount, test_file, O_CREAT|O_RDWR, 0666);
  ASSERT_GT(fd, 0);
  ASSERT_EQ(0, ceph_close(cmount, fd));

  // ok, the file has been created. perform real checks now
  fd = ceph_open(cmount, test_file, O_CREAT|O_RDWR|O_PATH, 0666);
  ASSERT_GT(fd, 0);

  char buf[128];
  ASSERT_EQ(-EBADF, ceph_read(cmount, fd, buf, sizeof(buf), 0));
  ASSERT_EQ(-EBADF, ceph_write(cmount, fd, buf, sizeof(buf), 0));

  // set perms to readable and writeable only by owner
  ASSERT_EQ(-EBADF, ceph_fchmod(cmount, fd, 0600));

  // change ownership to nobody -- we assume nobody exists and id is always 65534
  ASSERT_EQ(-EBADF, ceph_fchown(cmount, fd, 65534, 65534));

  // try to sync
  ASSERT_EQ(-EBADF, ceph_fsync(cmount, fd, false));

  struct stat sb;
  ASSERT_EQ(0, ceph_fstat(cmount, fd, &sb));

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
  ASSERT_EQ(fd, -ELOOP);

  // stat the original file
  struct stat stbuf_orig;
  ASSERT_EQ(ceph_stat(cmount, test_file, &stbuf_orig), 0);
  // stat the symlink
  struct stat stbuf_symlink_orig;
  ASSERT_EQ(ceph_stat(cmount, test_symlink, &stbuf_symlink_orig), 0);
  // ensure the stat bufs are equal
  ASSERT_TRUE(!memcmp(&stbuf_orig, &stbuf_symlink_orig, sizeof(stbuf_orig)));

  sprintf(test_file, "/test_symlinks_abs_%d", getpid());

  fd = ceph_open(cmount, test_file, O_CREAT|O_RDWR, 0666);
  ASSERT_GT(fd, 0);

  ceph_close(cmount, fd);

  sprintf(test_symlink, "/test_symlinks_abs_sym_%d", getpid());

  ASSERT_EQ(ceph_symlink(cmount, test_file, test_symlink), 0);

  // stat the original file
  ASSERT_EQ(ceph_stat(cmount, test_file, &stbuf_orig), 0);
  // stat the symlink
  ASSERT_EQ(ceph_stat(cmount, test_symlink, &stbuf_symlink_orig), 0);
  // ensure the stat bufs are equal
  ASSERT_TRUE(!memcmp(&stbuf_orig, &stbuf_symlink_orig, sizeof(stbuf_orig)));

  // test lstat
  struct stat stbuf_symlink;
  ASSERT_EQ(ceph_lstat(cmount, test_symlink, &stbuf_symlink), 0);
  ASSERT_TRUE(S_ISLNK(stbuf_symlink.st_mode));

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

  struct stat stbuf;
  ASSERT_EQ(ceph_lstat(cmount, test_file, &stbuf), 0);

  // ensure that its a file not a directory we get back
  ASSERT_TRUE(S_ISREG(stbuf.st_mode));

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
  ASSERT_EQ(fd, -ELOOP);

  // loop: /a -> /b, /b -> /c, /c -> /a
  char a[256], b[256], c[256];
  sprintf(a, "/%s/a", test_dir1);
  sprintf(b, "/%s/b", test_dir1);
  sprintf(c, "/%s/c", test_dir1);
  ASSERT_EQ(ceph_symlink(cmount, a, b), 0);
  ASSERT_EQ(ceph_symlink(cmount, b, c), 0);
  ASSERT_EQ(ceph_symlink(cmount, c, a), 0);
  ASSERT_EQ(ceph_open(cmount, a, O_RDWR, 0), -ELOOP);

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

  ASSERT_EQ(ceph_fchmod(cmount, -1, 0655), -EBADF);
  ASSERT_EQ(ceph_close(cmount, -1), -EBADF);
  ASSERT_EQ(ceph_lseek(cmount, -1, 0, SEEK_SET), -EBADF);

  char buf[0];
  ASSERT_EQ(ceph_read(cmount, -1, buf, 0, 0), -EBADF);
  ASSERT_EQ(ceph_write(cmount, -1, buf, 0, 0), -EBADF);

  ASSERT_EQ(ceph_ftruncate(cmount, -1, 0), -EBADF);
  ASSERT_EQ(ceph_fsync(cmount, -1, 0), -EBADF);

  struct stat stat;
  ASSERT_EQ(ceph_fstat(cmount, -1, &stat), -EBADF);

  struct sockaddr_storage addr;
  ASSERT_EQ(ceph_get_file_stripe_address(cmount, -1, 0, &addr, 1), -EBADF);

  ASSERT_EQ(ceph_get_file_stripe_unit(cmount, -1), -EBADF);
  ASSERT_EQ(ceph_get_file_pool(cmount, -1), -EBADF);
  ASSERT_EQ(ceph_get_file_replication(cmount, -1), -EBADF);

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
  struct stat st;
  ASSERT_EQ(0, ceph_lstat(cmount, path_dst, &st));

  /* test that src path doesn't exist */
  ASSERT_EQ(-ENOENT, ceph_lstat(cmount, path_src, &st));

  /* rename with non-existent source path */
  ASSERT_EQ(-ENOENT, ceph_rename(cmount, path_src, path_dst));

  ASSERT_EQ(0, ceph_unlink(cmount, path_dst));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, UseUnmounted) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));

  struct statvfs stvfs;
  EXPECT_EQ(-ENOTCONN, ceph_statfs(cmount, "/", &stvfs));
  EXPECT_EQ(-ENOTCONN, ceph_get_local_osd(cmount));
  EXPECT_EQ(-ENOTCONN, ceph_chdir(cmount, "/"));

  struct ceph_dir_result *dirp;
  EXPECT_EQ(-ENOTCONN, ceph_opendir(cmount, "/", &dirp));
  EXPECT_EQ(-ENOTCONN, ceph_closedir(cmount, dirp));

  ceph_readdir(cmount, dirp);
  EXPECT_EQ(-ENOTCONN, errno);

  struct dirent rdent;
  EXPECT_EQ(-ENOTCONN, ceph_readdir_r(cmount, dirp, &rdent));

  int stmask;
  struct stat st;
  EXPECT_EQ(-ENOTCONN, ceph_readdirplus_r(cmount, dirp, &rdent, &st, &stmask));
  EXPECT_EQ(-ENOTCONN, ceph_getdents(cmount, dirp, NULL, 0));
  EXPECT_EQ(-ENOTCONN, ceph_getdnames(cmount, dirp, NULL, 0));
  EXPECT_EQ(-ENOTCONN, ceph_telldir(cmount, dirp));
  EXPECT_EQ(-ENOTCONN, ceph_link(cmount, "/", "/link"));
  EXPECT_EQ(-ENOTCONN, ceph_unlink(cmount, "/path"));
  EXPECT_EQ(-ENOTCONN, ceph_rename(cmount, "/path", "/path"));
  EXPECT_EQ(-ENOTCONN, ceph_mkdir(cmount, "/", 0655));
  EXPECT_EQ(-ENOTCONN, ceph_mkdirs(cmount, "/", 0655));
  EXPECT_EQ(-ENOTCONN, ceph_rmdir(cmount, "/path"));
  EXPECT_EQ(-ENOTCONN, ceph_readlink(cmount, "/path", NULL, 0));
  EXPECT_EQ(-ENOTCONN, ceph_symlink(cmount, "/path", "/path"));
  EXPECT_EQ(-ENOTCONN, ceph_stat(cmount, "/path", &st));
  EXPECT_EQ(-ENOTCONN, ceph_lstat(cmount, "/path", &st));
  EXPECT_EQ(-ENOTCONN, ceph_setattr(cmount, "/path", &st, 0));
  EXPECT_EQ(-ENOTCONN, ceph_getxattr(cmount, "/path", "name", NULL, 0));
  EXPECT_EQ(-ENOTCONN, ceph_lgetxattr(cmount, "/path", "name", NULL, 0));
  EXPECT_EQ(-ENOTCONN, ceph_listxattr(cmount, "/path", NULL, 0));
  EXPECT_EQ(-ENOTCONN, ceph_llistxattr(cmount, "/path", NULL, 0));
  EXPECT_EQ(-ENOTCONN, ceph_removexattr(cmount, "/path", "name"));
  EXPECT_EQ(-ENOTCONN, ceph_lremovexattr(cmount, "/path", "name"));
  EXPECT_EQ(-ENOTCONN, ceph_setxattr(cmount, "/path", "name", NULL, 0, 0));
  EXPECT_EQ(-ENOTCONN, ceph_lsetxattr(cmount, "/path", "name", NULL, 0, 0));
  EXPECT_EQ(-ENOTCONN, ceph_chmod(cmount, "/path", 0));
  EXPECT_EQ(-ENOTCONN, ceph_fchmod(cmount, 0, 0));
  EXPECT_EQ(-ENOTCONN, ceph_chown(cmount, "/path", 0, 0));
  EXPECT_EQ(-ENOTCONN, ceph_lchown(cmount, "/path", 0, 0));
  EXPECT_EQ(-ENOTCONN, ceph_fchown(cmount, 0, 0, 0));

  struct utimbuf utb;
  EXPECT_EQ(-ENOTCONN, ceph_utime(cmount, "/path", &utb));
  EXPECT_EQ(-ENOTCONN, ceph_truncate(cmount, "/path", 0));
  EXPECT_EQ(-ENOTCONN, ceph_mknod(cmount, "/path", 0, 0));
  EXPECT_EQ(-ENOTCONN, ceph_open(cmount, "/path", 0, 0));
  EXPECT_EQ(-ENOTCONN, ceph_open_layout(cmount, "/path", 0, 0, 0, 0, 0, "pool"));
  EXPECT_EQ(-ENOTCONN, ceph_close(cmount, 0));
  EXPECT_EQ(-ENOTCONN, ceph_lseek(cmount, 0, 0, SEEK_SET));
  EXPECT_EQ(-ENOTCONN, ceph_read(cmount, 0, NULL, 0, 0));
  EXPECT_EQ(-ENOTCONN, ceph_write(cmount, 0, NULL, 0, 0));
  EXPECT_EQ(-ENOTCONN, ceph_ftruncate(cmount, 0, 0));
  EXPECT_EQ(-ENOTCONN, ceph_fsync(cmount, 0, 0));
  EXPECT_EQ(-ENOTCONN, ceph_fstat(cmount, 0, &st));
  EXPECT_EQ(-ENOTCONN, ceph_sync_fs(cmount));
  EXPECT_EQ(-ENOTCONN, ceph_get_file_stripe_unit(cmount, 0));
  EXPECT_EQ(-ENOTCONN, ceph_get_file_pool(cmount, 0));
  EXPECT_EQ(-ENOTCONN, ceph_get_file_pool_name(cmount, 0, NULL, 0));
  EXPECT_EQ(-ENOTCONN, ceph_get_file_replication(cmount, 0));
  EXPECT_EQ(-ENOTCONN, ceph_get_file_stripe_address(cmount, 0, 0, NULL, 0));
  EXPECT_EQ(-ENOTCONN, ceph_localize_reads(cmount, 0));
  EXPECT_EQ(-ENOTCONN, ceph_debug_get_fd_caps(cmount, 0));
  EXPECT_EQ(-ENOTCONN, ceph_debug_get_file_caps(cmount, "/path"));
  EXPECT_EQ(-ENOTCONN, ceph_get_stripe_unit_granularity(cmount));
  EXPECT_EQ(-ENOTCONN, ceph_get_pool_id(cmount, "data"));
  EXPECT_EQ(-ENOTCONN, ceph_get_pool_replication(cmount, 1));

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
  ASSERT_EQ(ceph_get_pool_id(cmount, "weflkjwelfjwlkejf"), -ENOENT);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, GetPoolReplication) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  /* negative pools */
  ASSERT_EQ(ceph_get_pool_replication(cmount, -10), -ENOENT);

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

  EXPECT_EQ(-ENOTCONN, ceph_get_file_extent_osds(cmount, 0, 0, NULL, NULL, 0));

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
  if (ret > 1)
    EXPECT_EQ(-ERANGE, ceph_get_file_extent_osds(cmount, fd, 0, NULL, osds, 1));

  ceph_close(cmount, fd);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, GetOsdCrushLocation) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);

  EXPECT_EQ(-ENOTCONN, ceph_get_osd_crush_location(cmount, 0, NULL, 0));

  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  ASSERT_EQ(ceph_get_osd_crush_location(cmount, 0, NULL, 1), -EINVAL);

  char path[256];
  ASSERT_EQ(ceph_get_osd_crush_location(cmount, 9999999, path, 0), -ENOENT);
  ASSERT_EQ(ceph_get_osd_crush_location(cmount, -1, path, 0), -EINVAL);

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
  ASSERT_EQ(ceph_get_osd_crush_location(cmount, 0, path, 1), -ERANGE);

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

  EXPECT_EQ(-ENOTCONN, ceph_get_osd_addr(cmount, 0, NULL));

  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  ASSERT_EQ(-EINVAL, ceph_get_osd_addr(cmount, 0, NULL));

  struct sockaddr_storage addr;
  ASSERT_EQ(-ENOENT, ceph_get_osd_addr(cmount, -1, &addr));
  ASSERT_EQ(-ENOENT, ceph_get_osd_addr(cmount, 9999999, &addr));

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
