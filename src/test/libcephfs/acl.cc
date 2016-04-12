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
#include "include/types.h"
#include "gtest/gtest.h"
#include "include/cephfs/libcephfs.h"
#include "include/ceph_fs.h"
#include "client/posix_acl.h"
#include <errno.h>
#include <sys/fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/xattr.h>

static size_t acl_ea_size(int count)
{
  return sizeof(acl_ea_header) + count * sizeof(acl_ea_entry);
}

static int acl_ea_count(size_t size)
{
  if (size < sizeof(acl_ea_header))
    return -1;
  size -= sizeof(acl_ea_header);
  if (size % sizeof(acl_ea_entry))
    return -1;
  return size / sizeof(acl_ea_entry);
}

static int check_acl_and_mode(const void *buf, size_t size, mode_t mode)
{
  const acl_ea_entry *group_entry = NULL, *mask_entry = NULL;
  const acl_ea_header *header = reinterpret_cast<const acl_ea_header*>(buf);
  const acl_ea_entry *entry = header->a_entries;
  int count = (size - sizeof(*header)) / sizeof(*entry);
  for (int i = 0; i < count; ++i) {
    __u16 tag = entry->e_tag;
    __u16 perm = entry->e_perm;
    switch(tag) {
      case ACL_USER_OBJ:
	if (perm != ((mode >> 6) & 7))
	  return -EINVAL;
	break;
      case ACL_USER:
      case ACL_GROUP:
	break;
      case ACL_GROUP_OBJ:
	group_entry = entry;
	break;
      case ACL_OTHER:
	if (perm != (mode & 7))
	  return -EINVAL;
	break;
      case ACL_MASK:
	mask_entry = entry;
	break;
      default:
	return -EIO;
    }
    ++entry;
  }
  if (mask_entry) {
    __u16 perm = mask_entry->e_perm;
    if (perm != ((mode >> 3) & 7))
      return -EINVAL;
  } else {
    if (!group_entry)
      return -EIO;
    __u16 perm = group_entry->e_perm;
    if (perm != ((mode >> 3) & 7))
      return -EINVAL;
  }
  return 0;
}

static int generate_test_acl(void *buf, size_t size, mode_t mode)
{
  if (acl_ea_count(size) != 5)
    return -1;
  acl_ea_header *header = reinterpret_cast<acl_ea_header*>(buf);
  header->a_version = (__u32)ACL_EA_VERSION;
  acl_ea_entry *entry = header->a_entries;
  entry->e_tag = ACL_USER_OBJ;
  entry->e_perm = (mode >> 6) & 7;
  ++entry;
  entry->e_tag = ACL_USER;
  entry->e_perm = 7;
  entry->e_id = getuid();
  ++entry;
  entry->e_tag = ACL_GROUP_OBJ;
  entry->e_perm = (mode >> 3) & 7;
  ++entry;
  entry->e_tag = ACL_MASK;
  entry->e_perm = 7;
  ++entry;
  entry->e_tag = ACL_OTHER;
  entry->e_perm = mode & 7;
  return 0;
}

static int generate_empty_acl(void *buf, size_t size, mode_t mode)
{

 if (acl_ea_count(size) != 3)
    return -1;
  acl_ea_header *header = reinterpret_cast<acl_ea_header*>(buf);
  header->a_version = (__u32)ACL_EA_VERSION;
  acl_ea_entry *entry = header->a_entries;
  entry->e_tag = ACL_USER_OBJ;
  entry->e_perm = (mode >> 6) & 7;
  ++entry;
  entry->e_tag = ACL_GROUP_OBJ;
  entry->e_perm = (mode >> 3) & 7;
  ++entry;
  entry->e_tag = ACL_OTHER;
  entry->e_perm = mode & 7;
  return 0;
}

TEST(ACL, SetACL) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));
  ASSERT_EQ(0, ceph_conf_set(cmount, "client_acl_type", "posix_acl"));
  ASSERT_EQ(0, ceph_conf_set(cmount, "client_permissions", "0"));

  char test_file[256];
  sprintf(test_file, "file1_setacl_%d", getpid());

  int fd = ceph_open(cmount, test_file, O_CREAT|O_RDWR, 0600);
  ASSERT_GT(fd, 0);
  // change ownership to nobody -- we assume nobody exists and id is always 65534
  ASSERT_EQ(ceph_fchown(cmount, fd, 65534, 65534), 0);

  ASSERT_EQ(0, ceph_conf_set(cmount, "client_permissions", "1"));
  ASSERT_EQ(ceph_open(cmount, test_file, O_RDWR, 0), -EACCES);
  ASSERT_EQ(0, ceph_conf_set(cmount, "client_permissions", "0"));

  size_t acl_buf_size = acl_ea_size(5);
  void *acl_buf = malloc(acl_buf_size);
  ASSERT_EQ(generate_test_acl(acl_buf, acl_buf_size, 0750), 0);

  // can't set default acl for non-directory
  ASSERT_EQ(ceph_fsetxattr(cmount, fd, ACL_EA_DEFAULT, acl_buf, acl_buf_size, 0), -EACCES);
  ASSERT_EQ(ceph_fsetxattr(cmount, fd, ACL_EA_ACCESS, acl_buf, acl_buf_size, 0), 0);

  int tmpfd = ceph_open(cmount, test_file, O_RDWR, 0);
  ASSERT_GT(tmpfd, 0);
  ceph_close(cmount, tmpfd);

  struct stat stbuf;
  ASSERT_EQ(ceph_fstat(cmount, fd, &stbuf), 0);
  // mode was modified according to ACL
  ASSERT_EQ(stbuf.st_mode & 0777, 0770u);
  ASSERT_EQ(check_acl_and_mode(acl_buf, acl_buf_size, stbuf.st_mode), 0);

  acl_buf_size = acl_ea_size(3);
  // setting ACL that is equivalent to file mode
  ASSERT_EQ(generate_empty_acl(acl_buf, acl_buf_size, 0600), 0);
  ASSERT_EQ(ceph_fsetxattr(cmount, fd, ACL_EA_ACCESS, acl_buf, acl_buf_size, 0), 0);
  // ACL was deleted
  ASSERT_EQ(ceph_fgetxattr(cmount, fd, ACL_EA_ACCESS, NULL, 0), -ENODATA);

  ASSERT_EQ(ceph_fstat(cmount, fd, &stbuf), 0);
  // mode was modified according to ACL
  ASSERT_EQ(stbuf.st_mode & 0777, 0600u);

  free(acl_buf);
  ceph_close(cmount, fd);
  ceph_shutdown(cmount);
}

TEST(ACL, Chmod) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));
  ASSERT_EQ(0, ceph_conf_set(cmount, "client_acl_type", "posix_acl"));

  char test_file[256];
  sprintf(test_file, "file1_acl_chmod_%d", getpid());

  int fd = ceph_open(cmount, test_file, O_CREAT|O_RDWR, 0600);
  ASSERT_GT(fd, 0);

  int acl_buf_size = acl_ea_size(5);
  void *acl_buf = malloc(acl_buf_size);
  ASSERT_EQ(generate_test_acl(acl_buf, acl_buf_size, 0775), 0);
  ASSERT_EQ(ceph_fsetxattr(cmount, fd, ACL_EA_ACCESS, acl_buf, acl_buf_size, 0), 0);

  struct stat stbuf;
  ASSERT_EQ(ceph_fstat(cmount, fd, &stbuf), 0);
  // mode was updated according to ACL
  ASSERT_EQ(stbuf.st_mode & 0777, 0775u);

  // change mode
  ASSERT_EQ(ceph_fchmod(cmount, fd, 0640), 0);

  ASSERT_EQ(ceph_fstat(cmount, fd, &stbuf), 0);
  ASSERT_EQ(stbuf.st_mode & 0777, 0640u);

  // ACL was updated according to mode
  ASSERT_EQ(ceph_fgetxattr(cmount, fd, ACL_EA_ACCESS, acl_buf, acl_buf_size), acl_buf_size);
  ASSERT_EQ(check_acl_and_mode(acl_buf, acl_buf_size, stbuf.st_mode), 0);

  free(acl_buf);
  ceph_close(cmount, fd);
  ceph_shutdown(cmount);
}

TEST(ACL, DefaultACL) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));
  ASSERT_EQ(0, ceph_conf_set(cmount, "client_acl_type", "posix_acl"));

  int acl_buf_size = acl_ea_size(5);
  void *acl1_buf = malloc(acl_buf_size);
  void *acl2_buf = malloc(acl_buf_size);

  ASSERT_EQ(generate_test_acl(acl1_buf, acl_buf_size, 0750), 0);

  char test_dir1[256];
  sprintf(test_dir1, "dir1_acl_default_%d", getpid());
  ASSERT_EQ(ceph_mkdir(cmount, test_dir1, 0750), 0);

  // set default acl
  ASSERT_EQ(ceph_setxattr(cmount, test_dir1, ACL_EA_DEFAULT, acl1_buf, acl_buf_size, 0), 0);

  char test_dir2[256];
  sprintf(test_dir2, "%s/dir2", test_dir1);
  ASSERT_EQ(ceph_mkdir(cmount, test_dir2, 0755), 0);

  // inherit default acl
  ASSERT_EQ(ceph_getxattr(cmount, test_dir2, ACL_EA_DEFAULT, acl2_buf, acl_buf_size), acl_buf_size);
  ASSERT_EQ(memcmp(acl1_buf, acl2_buf, acl_buf_size), 0);

  // mode and ACL are updated
  ASSERT_EQ(ceph_getxattr(cmount, test_dir2, ACL_EA_ACCESS, acl2_buf, acl_buf_size), acl_buf_size);
  {
    struct stat stbuf;
    ASSERT_EQ(ceph_stat(cmount, test_dir2, &stbuf), 0);
    // other bits of mode &= acl other perm
    ASSERT_EQ(stbuf.st_mode & 0777, 0750u);
    ASSERT_EQ(check_acl_and_mode(acl2_buf, acl_buf_size, stbuf.st_mode), 0);
  }

  char test_file1[256];
  sprintf(test_file1, "%s/file1", test_dir1);
  int fd = ceph_open(cmount, test_file1, O_CREAT|O_RDWR, 0666);
  ASSERT_GT(fd, 0);

  // no default acl
  ASSERT_EQ(ceph_fgetxattr(cmount, fd, ACL_EA_DEFAULT, NULL, 0), -ENODATA);

  // mode and ACL are updated
  ASSERT_EQ(ceph_fgetxattr(cmount, fd, ACL_EA_ACCESS, acl2_buf, acl_buf_size), acl_buf_size);
  {
    struct stat stbuf;
    ASSERT_EQ(ceph_stat(cmount, test_file1, &stbuf), 0);
    // other bits of mode &= acl other perm
    ASSERT_EQ(stbuf.st_mode & 0777, 0660u);
    ASSERT_EQ(check_acl_and_mode(acl2_buf, acl_buf_size, stbuf.st_mode), 0);
  }

  free(acl1_buf);
  free(acl2_buf);
  ceph_close(cmount, fd);
  ceph_shutdown(cmount);
}

TEST(ACL, Disabled) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));
  ASSERT_EQ(0, ceph_conf_set(cmount, "client_acl_type", ""));

  size_t acl_buf_size = acl_ea_size(3);
  void *acl_buf = malloc(acl_buf_size);
  ASSERT_EQ(generate_empty_acl(acl_buf, acl_buf_size, 0755), 0);

  char test_dir[256];
  sprintf(test_dir, "dir1_acl_disabled_%d", getpid());
  ASSERT_EQ(ceph_mkdir(cmount, test_dir, 0750), 0);

  ASSERT_EQ(ceph_setxattr(cmount, test_dir, ACL_EA_DEFAULT, acl_buf, acl_buf_size, 0), -EOPNOTSUPP);
  ASSERT_EQ(ceph_setxattr(cmount, test_dir, ACL_EA_ACCESS, acl_buf, acl_buf_size, 0), -EOPNOTSUPP);
  ASSERT_EQ(ceph_getxattr(cmount, test_dir, ACL_EA_DEFAULT, acl_buf, acl_buf_size), -EOPNOTSUPP);
  ASSERT_EQ(ceph_getxattr(cmount, test_dir, ACL_EA_ACCESS, acl_buf, acl_buf_size), -EOPNOTSUPP);

  free(acl_buf);
  ceph_shutdown(cmount);
}
