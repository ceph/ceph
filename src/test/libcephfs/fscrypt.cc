// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 * Copyright (C) 2023 IBM Corporation
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
#include "include/stringify.h"
#include "include/cephfs/libcephfs.h"
#include "include/fs_types.h"
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

#include "include/fs_types.h"

#include "client/FSCrypt.h"

#ifdef __linux__
#include <limits.h>
#include <sys/xattr.h>
#endif

#include "test.h"

using namespace std;

rados_t cluster;

static string fscrypt_dir;

static char fscrypt_key[32];


int do_fscrypt_mount(struct ceph_mount_info *cmount, const char *root)
{
  int r;

  if (fscrypt_dir.empty()) {
    r = ceph_mount(cmount, root);
  } else {
    string new_root = fscrypt_dir;

    if (root) {
      new_root += root;
    }

    r = ceph_mount(cmount, new_root.c_str());
  }
  if (r < 0) {
    std::clog << __func__ << "() ceph_mount r=" << r << std::endl;
    return r;
  }

  struct ceph_fscrypt_key_identifier kid;

  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), &kid, 0);
  if (r < 0) {
    std::clog << __func__ << "() ceph_mount add_fscrypt_key r=" << r << std::endl;
    return r;
  }

  return 0;
}

string get_unique_dir_name()
{
  pid_t mypid = getpid();
  return string("ceph_test_libcephfs_fscrypt.") + stringify(mypid) + "." + stringify(rand());
}

void generate_remove_key_arg(ceph_fscrypt_key_identifier kid, fscrypt_remove_key_arg* arg){
  fscrypt_key_specifier key_spec;
  key_spec.type = FSCRYPT_KEY_SPEC_TYPE_IDENTIFIER;
  key_spec.__reserved = 0;
  memcpy(key_spec.u.identifier, kid.raw, 16);
  arg->removal_status_flags = 0;
  arg->key_spec = key_spec;
}

int init_mount(struct ceph_mount_info** cmount){
  int r = ceph_create(cmount, NULL);
  if (r < 0) {
    std::clog << __func__ << "(): ceph_create() r=" << r << std::endl;
    return r;
  }

  r = ceph_conf_read_file(*cmount, NULL);
  if (r < 0) {
    std::clog << __func__ << "(): ceph_conf_read_file() r=" << r << std::endl;
    return r;
  }

  r = ceph_conf_parse_env(*cmount, NULL);
  if (r < 0) {
    std::clog << __func__ << "(): ceph_parse_env() r=" << r << std::endl;
    return r;
  }

  r = ceph_mount(*cmount, NULL);
  if (r < 0) {
    std::clog << __func__ << "(): ceph_mount() r=" << r << std::endl;
    return r;
  }
  return 0;
}

int fscrypt_encrypt(const string& dir_path)
{
  struct ceph_mount_info *cmount;
  int r = ceph_create(&cmount, NULL);
  if (r < 0) {
    std::clog << __func__ << "(): ceph_create() r=" << r << std::endl;
    return r;
  }

  r = ceph_conf_read_file(cmount, NULL);
  if (r < 0) {
    std::clog << __func__ << "(): ceph_conf_read_file() r=" << r << std::endl;
    return r;
  }

  r = ceph_conf_parse_env(cmount, NULL);
  if (r < 0) {
    std::clog << __func__ << "(): ceph_parse_env() r=" << r << std::endl;
    return r;
  }

  r = ceph_mount(cmount, NULL);
  if (r < 0) {
    std::clog << __func__ << "(): ceph_mount() r=" << r << std::endl;
    return r;
  }

  Inode *dir, *root;
  struct ceph_statx stx_dir;
  UserPerm *perms = ceph_mount_perms(cmount);

  r = ceph_ll_lookup_root(cmount, &root);
  if (r < 0) {
    std::clog << __func__ << "(): ceph_ll_lookup_root() r=" << r << std::endl;
    return r;
  }

  r = ceph_ll_mkdir(cmount, root, dir_path.c_str(), 0755, &dir, &stx_dir, 0, 0, perms);
  if (r < 0) {
    std::clog << __func__ << "(): ceph_ll_mkdir(" << dir_path << ") r=" << r << std::endl;
    return r;
  }

  for (int i = 0; i < sizeof(fscrypt_key); ++i) {
    fscrypt_key[i] = (char)rand();
  }

  string key_fname = dir_path + ".key";
  int key_fd = ceph_open(cmount, key_fname.c_str(), O_RDWR|O_CREAT|O_TRUNC, 0600);
  if (key_fd < 0) {
    std::clog << __func__ << "(): ceph_open() fd=" << key_fd << std::endl;
    return key_fd;
  }

  r = ceph_write(cmount, key_fd, fscrypt_key, sizeof(fscrypt_key), 0);
  if (r < 0) {
    std::clog << __func__ << "(): ceph_write() r=" << r << std::endl;
    return r;
  }

  ceph_close(cmount, key_fd);

  struct ceph_fscrypt_key_identifier kid;

  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), &kid, 0);
  if (r < 0) {
    std::clog << __func__ << "(): ceph_add_fscrypt_key() r=" << r << std::endl;
    return r;
  }

  struct fscrypt_policy_v2 policy;
  policy.version = 2;
  policy.contents_encryption_mode = FSCRYPT_MODE_AES_256_XTS;
  policy.filenames_encryption_mode = FSCRYPT_MODE_AES_256_CTS;
  policy.flags = FSCRYPT_POLICY_FLAGS_PAD_32;
  memcpy(policy.master_key_identifier, kid.raw, FSCRYPT_KEY_IDENTIFIER_SIZE);

  int fd = ceph_open(cmount, dir_path.c_str(), O_DIRECTORY, 0);
  if (fd < 0) {
    std::clog << __func__ << "(): ceph_open() r=" << r << std::endl;
    return fd;
  }

  r = ceph_set_fscrypt_policy_v2(cmount, fd, &policy);
  if (r < 0) {
    std::clog << __func__ << "(): ceph_set_fscrypt_policy() r=" << r << std::endl;
    return r;
  }

  ceph_shutdown(cmount);

  return 0;
}

static int init_fscrypt()
{
  string name = get_unique_dir_name();
  std::clog << __func__ << "(): fscrypt_dir=" << name << std::endl;

  fscrypt_dir = string("/") + name;

  int r = fscrypt_encrypt(name);
  if (r < 0) {
    return r;
  }

  libcephfs_test_set_mount_call(do_fscrypt_mount);
  std::clog << __func__ << "(): init fscrypt done" << std::endl;

  return 0;
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

TEST(FSCrypt, MultipleUnlockLockClaims) {
  struct ceph_fscrypt_key_identifier kid;
  struct ceph_fscrypt_key_identifier kid2;

  struct ceph_mount_info *cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), &kid, 1091);
  ASSERT_EQ(0, r);

  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), &kid2, 1299);
  ASSERT_EQ(0, r);
  if (r < 0) {
    std::clog << __func__ << "() 1ceph_mount add_fscrypt_key r=" << r << std::endl;
  }

  //remove user 1 of 2, should return 0, but 0x2 status_flag
  fscrypt_remove_key_arg arg;
  generate_remove_key_arg(kid, &arg);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(2, arg.removal_status_flags);

   //remove suser 2 of 2, ret 0, 0x0 status_flag
  fscrypt_remove_key_arg arg2;
  generate_remove_key_arg(kid2, &arg2);

  r = ceph_remove_fscrypt_key(cmount, &arg2, 1091);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0, arg2.removal_status_flags);
 ceph_shutdown(cmount);
}

TEST(FSCrypt, UnlockKeyUserDNE) {
  struct ceph_fscrypt_key_identifier kid;

  struct ceph_mount_info *cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), &kid, 1091);
  ASSERT_EQ(0, r);

  fscrypt_remove_key_arg arg;
  generate_remove_key_arg(kid, &arg);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(2, arg.removal_status_flags);

  ceph_shutdown(cmount);
}

TEST(FSCrypt, UnlockKeyDNE) {
  struct ceph_fscrypt_key_identifier kid;

  struct ceph_mount_info *cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), &kid, 1299);
  ASSERT_EQ(0, r);

  fscrypt_remove_key_arg arg;
  generate_remove_key_arg(kid, &arg);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0, arg.removal_status_flags);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(-ENOKEY, r);
  ASSERT_EQ(0, arg.removal_status_flags);

  ceph_shutdown(cmount);
}

#warning key_remove todo: 'EINVAL: invalid key specifier type, or reserved bits were set' case

TEST(FSCrypt, SetPolicyEmptyDir) {
  struct ceph_fscrypt_key_identifier kid;

  struct ceph_mount_info* cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), &kid, 1299);
  ASSERT_EQ(0, r);

  fscrypt_remove_key_arg arg;
  generate_remove_key_arg(kid, &arg);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0, arg.removal_status_flags);

  ceph_shutdown(cmount);
}

TEST(FSCrypt, SetPolicyNotEmptyFile) {
  struct ceph_fscrypt_key_identifier kid;

  struct ceph_mount_info* cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  string dir_path = "dir1";
  ceph_mkdir(cmount, dir_path.c_str(), 0777);

  string file_path = "dir1/file1";
  int fd = ceph_open(cmount, file_path.c_str(), O_RDWR|O_CREAT|O_TRUNC, 0600);
  r = ceph_write(cmount, fd, fscrypt_key, sizeof(fscrypt_key), 0);
  ceph_close(cmount, fd);

  int fd2 = ceph_open(cmount, dir_path.c_str(), O_DIRECTORY, 0);

  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), &kid, 1299);

  struct fscrypt_policy_v2 policy;
  policy.version = 2;
  policy.contents_encryption_mode = FSCRYPT_MODE_AES_256_XTS;
  policy.filenames_encryption_mode = FSCRYPT_MODE_AES_256_CTS;
  policy.flags = FSCRYPT_POLICY_FLAGS_PAD_32;
  memcpy(policy.master_key_identifier, kid.raw, FSCRYPT_KEY_IDENTIFIER_SIZE);

  r = ceph_set_fscrypt_policy_v2(cmount, fd2, &policy);
  ASSERT_EQ(-ENOTEMPTY, r);

  fscrypt_remove_key_arg arg;
  generate_remove_key_arg(kid, &arg);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0, arg.removal_status_flags);

  ceph_unlink(cmount, file_path.c_str());
  ceph_rmdir(cmount, dir_path.c_str());
  ceph_shutdown(cmount);
}

TEST(FSCrypt, SetPolicyNotEmptyDir) {
  struct ceph_fscrypt_key_identifier kid;

  struct ceph_mount_info* cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  string dir_path = "dir1";
  ceph_mkdir(cmount, dir_path.c_str(), 0777);
  string dir2_path = "dir1/dir2";
  ceph_mkdir(cmount, dir2_path.c_str(), 0777);

  string file_path = "dir1/file1";
  int fd = ceph_open(cmount, file_path.c_str(), O_RDWR|O_CREAT|O_TRUNC, 0600);
  r = ceph_write(cmount, fd, fscrypt_key, sizeof(fscrypt_key), 0);
  ceph_close(cmount, fd);

  int fd2 = ceph_open(cmount, dir_path.c_str(), O_DIRECTORY, 0);

  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), &kid, 1299);

  struct fscrypt_policy_v2 policy;
  policy.version = 2;
  policy.contents_encryption_mode = FSCRYPT_MODE_AES_256_XTS;
  policy.filenames_encryption_mode = FSCRYPT_MODE_AES_256_CTS;
  policy.flags = FSCRYPT_POLICY_FLAGS_PAD_32;
  memcpy(policy.master_key_identifier, kid.raw, FSCRYPT_KEY_IDENTIFIER_SIZE);

  r = ceph_set_fscrypt_policy_v2(cmount, fd2, &policy);
  ASSERT_EQ(-ENOTEMPTY, r);

  fscrypt_remove_key_arg arg;
  generate_remove_key_arg(kid, &arg);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0, arg.removal_status_flags);

  ceph_unlink(cmount, file_path.c_str());
  ceph_rmdir(cmount, dir_path.c_str());
  ceph_shutdown(cmount);
}

TEST(FSCrypt, SetPolicyAlreadyExistSameKey) {
  struct ceph_fscrypt_key_identifier kid;

  struct ceph_mount_info* cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);
  
  string dir_path = "dir2";
  ceph_mkdir(cmount, dir_path.c_str(), 0777);

  int fd = ceph_open(cmount, dir_path.c_str(), O_DIRECTORY, 0);

  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), &kid, 1299);

  struct fscrypt_policy_v2 policy;
  policy.version = 2;
  policy.contents_encryption_mode = FSCRYPT_MODE_AES_256_XTS;
  policy.filenames_encryption_mode = FSCRYPT_MODE_AES_256_CTS;
  policy.flags = FSCRYPT_POLICY_FLAGS_PAD_32;
  memcpy(policy.master_key_identifier, kid.raw, FSCRYPT_KEY_IDENTIFIER_SIZE);

  r = ceph_set_fscrypt_policy_v2(cmount, fd, &policy);
  ASSERT_EQ(0, r);

  r = ceph_set_fscrypt_policy_v2(cmount, fd, &policy);
  ASSERT_EQ(-EEXIST, r);
  
  ceph_rmdir(cmount, dir_path.c_str());
  ceph_shutdown(cmount);
}

TEST(FSCrypt, SetPolicyNonDir) {
  //can be file, symlink, hardlink, device file etc
  struct ceph_fscrypt_key_identifier kid;

  struct ceph_mount_info* cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), &kid, 1299);
  ASSERT_EQ(-ENOTDIR, r);

  fscrypt_remove_key_arg arg;
  generate_remove_key_arg(kid, &arg);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0, arg.removal_status_flags);

  ceph_shutdown(cmount);
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

  r = init_fscrypt();
  if (r < 0)
    exit(1);

  r = RUN_ALL_TESTS();

  rados_shutdown(cluster);

  return r;
}
