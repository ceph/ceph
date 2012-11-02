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
#include <sys/xattr.h>

TEST(LibCephFS, Open_empty_component) {

  pid_t mypid = getpid();
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
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

  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  fd = ceph_open(cmount, c_path, O_RDONLY, 0666);
  ASSERT_LT(0, fd);

  ASSERT_EQ(0, ceph_close(cmount, fd));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, Mount_non_exist) {

  struct ceph_mount_info *cmount;

  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_NE(0, ceph_mount(cmount, "/non-exist"));
}

TEST(LibCephFS, Mount_double) {

  struct ceph_mount_info *cmount;

  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));
  ASSERT_EQ(-EISCONN, ceph_mount(cmount, "/"));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, Mount_remount) {

  struct ceph_mount_info *cmount;

  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));

  CephContext *cct = ceph_get_mount_context(cmount);
  ASSERT_EQ(0, ceph_mount(cmount, "/"));
  ASSERT_EQ(0, ceph_unmount(cmount));

  ASSERT_EQ(0, ceph_mount(cmount, "/"));
  ASSERT_EQ(cct, ceph_get_mount_context(cmount));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, Unmount_unmounted) {

  struct ceph_mount_info *cmount;

  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(-ENOTCONN, ceph_unmount(cmount));
}

TEST(LibCephFS, Release_unmounted) {

  struct ceph_mount_info *cmount;

  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_release(cmount));
}

TEST(LibCephFS, Release_mounted) {

  struct ceph_mount_info *cmount;

  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));
  ASSERT_EQ(-EISCONN, ceph_release(cmount));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, Unmount_release) {

  struct ceph_mount_info *cmount;

  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));
  ASSERT_EQ(0, ceph_unmount(cmount));
  ASSERT_EQ(0, ceph_release(cmount));
}

TEST(LibCephFS, Mount) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);
  ceph_shutdown(cmount);

  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);
  ceph_shutdown(cmount);
}

TEST(LibCephFS, Dir_ls) {

  pid_t mypid = getpid();

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
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

  std::vector<std::pair<char *, int> > entries;
  // check readdir and capture stream order for future tests
  for(i = 0; i < r; ++i) {

    result = ceph_readdir(cmount, ls_dir);
    ASSERT_TRUE(result != NULL);

    int size;
    sscanf(result->d_name, "dirf%d", &size);
    entries.push_back(std::pair<char*,int>(strdup(result->d_name), size));
  }

  ASSERT_TRUE(ceph_readdir(cmount, ls_dir) == NULL);

  // test rewinddir
  ceph_rewinddir(cmount, ls_dir);

  result = ceph_readdir(cmount, ls_dir);
  ASSERT_TRUE(result != NULL);
  ASSERT_STREQ(result->d_name, ".");
  result = ceph_readdir(cmount, ls_dir);
  ASSERT_TRUE(result != NULL);
  ASSERT_STREQ(result->d_name, "..");

  // check telldir
  for(i = 0; i < r-1; ++i) {
    int r = ceph_telldir(cmount, ls_dir);
    ASSERT_GT(r, -1);
    ceph_seekdir(cmount, ls_dir, r);
    result = ceph_readdir(cmount, ls_dir);
    ASSERT_TRUE(result != NULL);
    ASSERT_STREQ(result->d_name, entries[i].first);
  }

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
  while (count < r) {
    int len = ceph_getdents(cmount, ls_dir, (char *)getdents_entries, r * sizeof(*getdents_entries));
    ASSERT_GT(len, 0);
    ASSERT_TRUE((len % sizeof(*getdents_entries)) == 0);
    int n = len / sizeof(*getdents_entries);
    if (count == 0) {
      ASSERT_STREQ(getdents_entries[0].d_name, ".");
      ASSERT_STREQ(getdents_entries[1].d_name, "..");
    }
    int j;
    i = count;
    for(j = 2; j < n; ++i, ++j) {
      ASSERT_STREQ(getdents_entries[j].d_name, entries[i].first);
    }
    count += n;
  }

  free(getdents_entries);

  // test readdir_r
  ceph_rewinddir(cmount, ls_dir);

  result = ceph_readdir(cmount, ls_dir);
  ASSERT_TRUE(result != NULL);
  ASSERT_STREQ(result->d_name, ".");
  result = ceph_readdir(cmount, ls_dir);
  ASSERT_TRUE(result != NULL);
  ASSERT_STREQ(result->d_name, "..");

  for(i = 0; i < r; ++i) {
    struct dirent rdent;
    ASSERT_EQ(ceph_readdir_r(cmount, ls_dir, &rdent), 1);
    ASSERT_STREQ(rdent.d_name, entries[i].first);
  }

  // test readdirplus
  ceph_rewinddir(cmount, ls_dir);

  result = ceph_readdir(cmount, ls_dir);
  ASSERT_TRUE(result != NULL);
  ASSERT_STREQ(result->d_name, ".");
  result = ceph_readdir(cmount, ls_dir);
  ASSERT_TRUE(result != NULL);
  ASSERT_STREQ(result->d_name, "..");

  for(i = 0; i < r; ++i) {
    struct dirent rdent;
    struct stat st;
    int stmask;
    ASSERT_EQ(ceph_readdirplus_r(cmount, ls_dir, &rdent, &st, &stmask), 1);
    ASSERT_STREQ(rdent.d_name, entries[i].first);
    ASSERT_EQ(st.st_size, entries[i].second);
    ASSERT_EQ(st.st_ino, rdent.d_ino);
    //ASSERT_EQ(st.st_mode, (mode_t)0666);
  }

  ASSERT_EQ(ceph_closedir(cmount, ls_dir), 0);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, Many_nested_dirs) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
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
  while(len > 0) {
    sprintf(xattrk, "user.test_xattr_%c", i);
    ASSERT_STREQ(p, xattrk);

    char gxattrv[128];
    int alen = ceph_getxattr(cmount, test_xattr_file, p, (void *) gxattrv, 128);
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

TEST(LibCephFS, Lstat_slashdot) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  struct stat stbuf;
  ASSERT_EQ(ceph_lstat(cmount, "/.", &stbuf), 0);
  ASSERT_EQ(ceph_lstat(cmount, ".", &stbuf), 0);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, Double_chmod) {

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
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
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char test_file[256];
  sprintf(test_file, "test_fchown_%d", getpid());

  int fd = ceph_open(cmount, test_file, O_CREAT|O_RDWR, 0666);
  ASSERT_GT(fd, 0);

  // set perms to readable and writeable only by owner
  ASSERT_EQ(ceph_fchmod(cmount, fd, 0600), 0);

  // change ownership to nobody -- we assume nobody exists and id is always 65534
  ASSERT_EQ(ceph_fchown(cmount, fd, 65534, 65534), 0);

  ceph_close(cmount, fd);

  fd = ceph_open(cmount, test_file, O_RDWR, 0);
  ASSERT_EQ(fd, -EACCES);
  ceph_shutdown(cmount);
}

TEST(LibCephFS, Symlinks) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char test_file[256];
  sprintf(test_file, "test_symlinks_%d", getpid());

  int fd = ceph_open(cmount, test_file, O_CREAT|O_RDWR, 0666);
  ASSERT_GT(fd, 0);

  ceph_close(cmount, fd);

  char test_symlink[256];
  sprintf(test_symlink, "test_symlinks_sym_%d", getpid());

  ASSERT_EQ(ceph_symlink(cmount, test_file, test_symlink), 0);

  // stat the original file
  struct stat stbuf_orig;
  ASSERT_EQ(ceph_stat(cmount, test_file, &stbuf_orig), 0);
  // stat the symlink
  struct stat stbuf_symlink_orig;
  ASSERT_EQ(ceph_stat(cmount, test_symlink, &stbuf_symlink_orig), 0);
  // ensure the stat bufs are equal
  ASSERT_TRUE(!memcmp(&stbuf_orig, &stbuf_symlink_orig, sizeof(stbuf_orig)));

  // test lstat
  struct stat stbuf_symlink;
  ASSERT_EQ(ceph_lstat(cmount, test_symlink, &stbuf_symlink), 0);
  ASSERT_TRUE(S_ISLNK(stbuf_symlink.st_mode));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, Hardlink_no_original) {

  int mypid = getpid();

  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
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
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);
  ASSERT_EQ(ceph_chdir(cmount, dir), 0);
  ASSERT_EQ(ceph_unlink(cmount, "hardl1"), 0);
  ASSERT_EQ(ceph_rmdir(cmount, dir), 0);

  ceph_shutdown(cmount);
}

TEST(LibCephFS, BadFileDesc) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
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
}
