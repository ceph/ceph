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

#include "client/FSCrypt.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <condition_variable>
#include <cstring>
#include <iostream>
#include <mutex>
#include <vector>

#include "common/ceph_argparse.h"
#include "gtest/gtest.h"
#include "include/buffer.h"
#include "include/cephfs/libcephfs.h"
#include "include/fs_types.h"
#include "include/rados/librados.h"
#include "include/stringify.h"
#include "json_spirit/json_spirit.h"

#include "fscrypt_conf.h"

#ifdef __linux__
#include <limits.h>
#include <sys/xattr.h>
#endif

#include "test.h"

using namespace std;

rados_t cluster;
#if defined(__linux__)
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

  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), keyid, 0);
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

void generate_remove_key_arg(char *keyid, fscrypt_remove_key_arg* arg){
  fscrypt_key_specifier key_spec;
  key_spec.type = FSCRYPT_KEY_SPEC_TYPE_IDENTIFIER;
  key_spec.__reserved = 0;
  memcpy(key_spec.u.identifier, keyid, FSCRYPT_KEY_IDENTIFIER_SIZE);
  arg->removal_status_flags = 0;
  arg->key_spec = key_spec;
}

void populate_policy(char *keyid, struct fscrypt_policy_v2* policy) {
  memset(policy, 0, sizeof(struct fscrypt_policy_v2));
  policy->version = 2;
  policy->contents_encryption_mode = FSCRYPT_MODE_AES_256_XTS;
  policy->filenames_encryption_mode = FSCRYPT_MODE_AES_256_CTS;
  policy->flags = FSCRYPT_POLICY_FLAGS_PAD_32;
  memcpy(policy->master_key_identifier, keyid, FSCRYPT_KEY_IDENTIFIER_SIZE);
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

  for (int i = 0; i < (int)sizeof(fscrypt_key); ++i) {
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

  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), keyid, 0);
  if (r < 0) {
    std::clog << __func__ << "(): ceph_add_fscrypt_key() r=" << r << std::endl;
    return r;
  }

  struct fscrypt_policy_v2 policy;
  policy.version = 2;
  policy.contents_encryption_mode = FSCRYPT_MODE_AES_256_XTS;
  policy.filenames_encryption_mode = FSCRYPT_MODE_AES_256_CTS;
  policy.flags = FSCRYPT_POLICY_FLAGS_PAD_32;
  memcpy(policy.master_key_identifier, keyid, FSCRYPT_KEY_IDENTIFIER_SIZE);

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
  libcephfs_test_set_dir_prefix(fscrypt_dir);
  libcephfs_test_set_is_encrypted(true);
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
  struct ceph_mount_info *cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), keyid, 1091);
  ASSERT_EQ(0, r);

  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), keyid, 1299);
  ASSERT_EQ(0, r);
  if (r < 0) {
    std::clog << __func__ << "() 1ceph_mount add_fscrypt_key r=" << r << std::endl;
  }

  //remove user 1 of 2, should return 0, but 0x2 status_flag
  fscrypt_remove_key_arg arg;
  generate_remove_key_arg(keyid, &arg);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(2, arg.removal_status_flags);

   //remove suser 2 of 2, ret 0, 0x0 status_flag
  fscrypt_remove_key_arg arg2;
  generate_remove_key_arg(keyid, &arg2);

  r = ceph_remove_fscrypt_key(cmount, &arg2, 1091);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0, arg2.removal_status_flags);
 ceph_shutdown(cmount);
}

TEST(FSCrypt, UnlockKeyUserDNE) {
  struct ceph_mount_info *cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), keyid, 1091);
  ASSERT_EQ(0, r);

  fscrypt_remove_key_arg arg;
  generate_remove_key_arg(keyid, &arg);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(2, arg.removal_status_flags);
  ceph_shutdown(cmount);
}

TEST(FSCrypt, UnlockKeyDNE) {
  struct ceph_mount_info *cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), keyid, 1299);
  ASSERT_EQ(0, r);

  fscrypt_remove_key_arg arg;
  generate_remove_key_arg(keyid, &arg);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0, arg.removal_status_flags);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(-ENOKEY, r);
  ASSERT_EQ(0, arg.removal_status_flags);
  ceph_shutdown(cmount);
}

//#warning key_remove todo: 'EINVAL: invalid key specifier type, or reserved bits were set' case

TEST(FSCrypt, SetPolicyEmptyDir) {
  struct ceph_mount_info* cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), keyid, 1299);
  ASSERT_EQ(0, r);

  fscrypt_remove_key_arg arg;
  generate_remove_key_arg(keyid, &arg);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0, arg.removal_status_flags);
  ceph_shutdown(cmount);
}

TEST(FSCrypt, SetPolicyNotEmptyDir) {
  struct ceph_mount_info* cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  string dir_path = "dir1";
  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path.c_str(), 0777));
  string dir2_path = "dir1/dir2";
  ASSERT_EQ(0, ceph_mkdir(cmount, dir2_path.c_str(), 0777));

  string file_path = "dir1/file1";
  int fd = ceph_open(cmount, file_path.c_str(), O_RDWR|O_CREAT|O_TRUNC, 0600);
  r = ceph_write(cmount, fd, fscrypt_key, sizeof(fscrypt_key), 0);
  ceph_close(cmount, fd);

  int fd2 = ceph_open(cmount, dir_path.c_str(), O_DIRECTORY, 0);

  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), keyid, 1299);

  struct fscrypt_policy_v2 policy;
  populate_policy(keyid, &policy);

  r = ceph_set_fscrypt_policy_v2(cmount, fd2, &policy);
  ASSERT_EQ(-ENOTEMPTY, r);

  ASSERT_EQ(0, ceph_unlink(cmount, file_path.c_str()));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir2_path.c_str()));

  fscrypt_remove_key_arg arg;
  generate_remove_key_arg(keyid, &arg);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0, arg.removal_status_flags);

  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path.c_str()));
  ceph_shutdown(cmount);
}

TEST(FSCrypt, SetPolicyAlreadyExistSamePolicy) {
  struct ceph_mount_info* cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  string dir_path = "dir2";
  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path.c_str(), 0777));

  int fd = ceph_open(cmount, dir_path.c_str(), O_DIRECTORY, 0);

  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), keyid, 1299);

  struct fscrypt_policy_v2 policy;
  populate_policy(keyid, &policy);

  r = ceph_set_fscrypt_policy_v2(cmount, fd, &policy);
  ASSERT_EQ(0, r);

  r = ceph_set_fscrypt_policy_v2(cmount, fd, &policy);
  ASSERT_EQ(0, r);

  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path.c_str()));
  ceph_shutdown(cmount);
}

TEST(FSCrypt, SetPolicyAlreadyExistDifferentPolicy) {
  struct ceph_mount_info* cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  string dir_path = "dir2";
  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path.c_str(), 0777));

  int fd = ceph_open(cmount, dir_path.c_str(), O_DIRECTORY, 0);

  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), keyid, 1299);

  struct fscrypt_policy_v2 policy;
  populate_policy(keyid, &policy);

  r = ceph_set_fscrypt_policy_v2(cmount, fd, &policy);
  ASSERT_EQ(0, r);

  char fscrypt_key2[32];
  for (int i = 0; i < (int)sizeof(fscrypt_key2); ++i) {
    fscrypt_key2[i] = (char)rand();
  }
  char keyid2[FSCRYPT_KEY_IDENTIFIER_SIZE];
  r = ceph_add_fscrypt_key(cmount, fscrypt_key2, sizeof(fscrypt_key2), keyid2, 1299);

  struct fscrypt_policy_v2 policy2;
  populate_policy(keyid2, &policy2);

  r = ceph_set_fscrypt_policy_v2(cmount, fd, &policy2);
  ASSERT_EQ(-EEXIST, r);

  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path.c_str()));
  ceph_shutdown(cmount);
}

TEST(FSCrypt, SetPolicyNonDir) {
  //can be file, symlink, device file etc
  struct ceph_mount_info* cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  //setup policy
  struct fscrypt_policy_v2 policy;
  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  memset(keyid, 0, sizeof(keyid));
  populate_policy(keyid, &policy);

  //file
  string file_path = "file1";
  int fd = ceph_open(cmount, file_path.c_str(), O_RDWR|O_CREAT|O_TRUNC, 0600);
  r = ceph_set_fscrypt_policy_v2(cmount, fd, &policy);
  ASSERT_EQ(-ENOTDIR, r);
  ceph_close(cmount, fd);

  //symlink
  string symlink_path = "symlink1";
  r = ceph_symlink(cmount, file_path.c_str(), symlink_path.c_str());
  ASSERT_EQ(0, r);

  fd = ceph_open(cmount, symlink_path.c_str(), O_RDWR|O_CREAT|O_TRUNC, 0600);
  r = ceph_set_fscrypt_policy_v2(cmount, fd, &policy);
  ASSERT_EQ(-ENOTDIR, r);
  ceph_close(cmount, fd);

  //device file
  string mknod_path = "device1";
  r = ceph_mknod(cmount, mknod_path.c_str(), 0600, 55);
  fd = ceph_open(cmount, mknod_path.c_str(), O_RDWR, 0600);
  r = ceph_set_fscrypt_policy_v2(cmount, fd, &policy);
  ASSERT_EQ(-ENOTDIR, r);

  ceph_close(cmount, fd);
  ASSERT_EQ(0, ceph_unlink(cmount, file_path.c_str()));
  ASSERT_EQ(0, ceph_unlink(cmount, symlink_path.c_str()));
  ASSERT_EQ(0, ceph_unlink(cmount, mknod_path.c_str()));
  ceph_shutdown(cmount);
}

TEST(FSCrypt, SetPolicyNotSupported) {
  struct ceph_mount_info* cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  string dir_path = "dir2";
  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path.c_str(), 0777));

  int fd = ceph_open(cmount, dir_path.c_str(), O_DIRECTORY, 0);

  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), keyid, 1299);

  struct fscrypt_policy_v2 policy;
  policy.version = 2;
  policy.contents_encryption_mode = FSCRYPT_MODE_AES_256_CTS;
  policy.filenames_encryption_mode = FSCRYPT_MODE_AES_256_XTS;
  policy.flags = FSCRYPT_POLICY_FLAGS_PAD_32;
  memset(policy.__reserved, 0, sizeof(policy.__reserved));
  memcpy(policy.master_key_identifier, keyid, FSCRYPT_KEY_IDENTIFIER_SIZE);

  r = ceph_set_fscrypt_policy_v2(cmount, fd, &policy);
  ASSERT_EQ(-EINVAL, r);

  policy.version = 1;
  policy.contents_encryption_mode = FSCRYPT_MODE_AES_256_XTS;
  policy.filenames_encryption_mode = FSCRYPT_MODE_AES_256_CTS;
  policy.flags = FSCRYPT_POLICY_FLAGS_PAD_32;
  memset(policy.__reserved, 0, sizeof(policy.__reserved));
  memcpy(policy.master_key_identifier, keyid, FSCRYPT_KEY_IDENTIFIER_SIZE);

  r = ceph_set_fscrypt_policy_v2(cmount, fd, &policy);
  ASSERT_EQ(-EINVAL, r);

  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path.c_str()));
  ceph_shutdown(cmount);
}


TEST(FSCrypt, LockedListDir) {
  struct ceph_mount_info* cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  string dir_path = "dir1";
  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path.c_str(), 0777));

  string file_path = "dir1/file5";
  int fd = ceph_open(cmount, dir_path.c_str(), O_DIRECTORY, 0);

  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), keyid, 1299);

  struct fscrypt_policy_v2 policy;
  populate_policy(keyid, &policy);

  r = ceph_set_fscrypt_policy_v2(cmount, fd, &policy);

  int fd2 = ceph_open(cmount, file_path.c_str(), O_RDWR|O_CREAT|O_TRUNC, 0600);
  r = ceph_write(cmount, fd2, fscrypt_key, sizeof(fscrypt_key), 0);
  ASSERT_EQ(32, r);

  ceph_close(cmount, fd);
  ceph_close(cmount, fd2);

  ino_t inode = 0;
  struct ceph_dir_result *rdir;
  struct dirent *result;
  ASSERT_EQ(ceph_opendir(cmount, "dir1", &rdir), 0);
  while ((result = ceph_readdir(cmount, rdir)) != NULL) {
    if (strcmp(result->d_name, "file5") == 0) {
      inode = result->d_ino;
    }
  }
  fscrypt_remove_key_arg arg;
  generate_remove_key_arg(keyid, &arg);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0, arg.removal_status_flags);

  ASSERT_EQ(ceph_opendir(cmount, "dir1", &rdir), 0);
  while ((result = ceph_readdir(cmount, rdir)) != NULL) {
    if (result->d_ino == inode){
      file_path = dir_path;
      file_path.append("/");
      file_path.append(result->d_name);

      //check that we can get stat info
      struct ceph_statx stx;
      ceph_statx(cmount, file_path.c_str(), &stx, CEPH_SETATTR_SIZE, 0);

      ASSERT_EQ(sizeof(fscrypt_key), stx.stx_size);
      goto done;
    }
  }
  ASSERT_EQ(0,-1); //will fail

done:
  ASSERT_EQ(0, ceph_unlink(cmount, file_path.c_str()));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path.c_str()));
  ceph_shutdown(cmount);
}

TEST(FSCrypt, ReadLockedDir) {
  struct ceph_mount_info* cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  string dir_path = "dir1";
  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path.c_str(), 0777));

  string file_path = "dir1/file5";
  int fd = ceph_open(cmount, dir_path.c_str(), O_DIRECTORY, 0);

  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), keyid, 1299);

  struct fscrypt_policy_v2 policy;
  populate_policy(keyid, &policy);

  r = ceph_set_fscrypt_policy_v2(cmount, fd, &policy);

  int fd2 = ceph_open(cmount, file_path.c_str(), O_RDWR|O_CREAT|O_TRUNC, 0600);
  r = ceph_write(cmount, fd2, fscrypt_key, sizeof(fscrypt_key), 0);
  ASSERT_EQ(32, r);

  ceph_close(cmount, fd);
  ceph_close(cmount, fd2);

  ino_t inode = 0;
  struct ceph_dir_result *rdir;
  struct dirent *result;
  ASSERT_EQ(ceph_opendir(cmount, "dir1", &rdir), 0);
  while ((result = ceph_readdir(cmount, rdir)) != NULL) {
    if (strcmp(result->d_name, "file5") == 0) {
      inode = result->d_ino;
    }
  }
  fscrypt_remove_key_arg arg;
  generate_remove_key_arg(keyid, &arg);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0, arg.removal_status_flags);

  ASSERT_EQ(ceph_opendir(cmount, "dir1", &rdir), 0);
  while ((result = ceph_readdir(cmount, rdir)) != NULL) {
    if (result->d_ino == inode){
      file_path = dir_path;
      file_path.append("/");
      file_path.append(result->d_name);
      goto read;
    }
  }
  ASSERT_EQ(0,-1); //will fail

read:
  fd2 = ceph_open(cmount, file_path.c_str(), O_RDWR, 0600);
  ASSERT_EQ(-ENOKEY, fd2);

  ASSERT_EQ(0, ceph_unlink(cmount, file_path.c_str()));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path.c_str()));
  ceph_shutdown(cmount);
}

TEST(FSCrypt, WriteLockedDir) {
  struct ceph_mount_info* cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  string dir_path = "dir1";
  string dir_path2 = "dir1/dir3";
  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path.c_str(), 0777));

  string file_path = "dir1/file5";
  int fd = ceph_open(cmount, dir_path.c_str(), O_DIRECTORY, 0);

  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), keyid, 1299);

  struct fscrypt_policy_v2 policy;
  populate_policy(keyid, &policy);

  r = ceph_set_fscrypt_policy_v2(cmount, fd, &policy);

  int fd2 = ceph_open(cmount, file_path.c_str(), O_RDWR|O_CREAT|O_TRUNC, 0600);
  r = ceph_write(cmount, fd2, fscrypt_key, sizeof(fscrypt_key), 0);
  ASSERT_EQ(32, r);

  ceph_close(cmount, fd);
  ceph_close(cmount, fd2);

  ino_t inode = 0;
  struct ceph_dir_result *rdir;
  struct dirent *result;
  ASSERT_EQ(ceph_opendir(cmount, "dir1", &rdir), 0);
  while ((result = ceph_readdir(cmount, rdir)) != NULL) {
    if (strcmp(result->d_name, "file5") == 0) {
      inode = result->d_ino;
    }
  }
  fscrypt_remove_key_arg arg;
  generate_remove_key_arg(keyid, &arg);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0, arg.removal_status_flags);

  ASSERT_EQ(ceph_opendir(cmount, "dir1", &rdir), 0);
  while ((result = ceph_readdir(cmount, rdir)) != NULL) {
    if (result->d_ino == inode){
      file_path = dir_path;
      file_path.append("/");
      file_path.append(result->d_name);
      goto write;
    }
  }
  ASSERT_EQ(0,-1); //will fail

write:
  fd2 = ceph_open(cmount, file_path.c_str(), O_RDWR, 0600);
  ASSERT_EQ(-ENOKEY, fd2);

  ASSERT_EQ(-ENOKEY, ceph_mkdir(cmount, dir_path2.c_str(), 0777));

  ASSERT_EQ(0, ceph_unlink(cmount, file_path.c_str()));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path.c_str()));
  ceph_shutdown(cmount);
}

TEST(FSCrypt, LockedCreateSnap) {
  struct ceph_mount_info* cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  string dir_path = "dir1";
  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path.c_str(), 0777));

  string file_path = "dir1/file5";
  int fd = ceph_open(cmount, dir_path.c_str(), O_DIRECTORY, 0);

  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), keyid, 1299);

  struct fscrypt_policy_v2 policy;
  populate_policy(keyid, &policy);

  r = ceph_set_fscrypt_policy_v2(cmount, fd, &policy);

  int fd2 = ceph_open(cmount, file_path.c_str(), O_RDWR|O_CREAT|O_TRUNC, 0600);
  r = ceph_write(cmount, fd2, fscrypt_key, sizeof(fscrypt_key), 0);
  ASSERT_EQ(32, r);

  ceph_close(cmount, fd);
  ceph_close(cmount, fd2);

  ASSERT_EQ(0, ceph_unlink(cmount, file_path.c_str()));

  fscrypt_remove_key_arg arg;
  generate_remove_key_arg(keyid, &arg);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0, arg.removal_status_flags);

  string snap_name = "snap1";
  ASSERT_EQ(0, ceph_mksnap(cmount, dir_path.c_str(), snap_name.c_str(), 0755, nullptr, 0));
  ASSERT_EQ(0, ceph_rmsnap(cmount, dir_path.c_str(), snap_name.c_str()));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path.c_str()));
  ceph_shutdown(cmount);
}

TEST(FSCrypt, RenameLockedSource) {
  struct ceph_mount_info* cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  string dir_path = "dir1";
  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path.c_str(), 0777));

  string src_path = "dir1/file5";
  int fd = ceph_open(cmount, dir_path.c_str(), O_DIRECTORY, 0);

  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), keyid, 1299);

  struct fscrypt_policy_v2 policy;
  populate_policy(keyid, &policy);

  r = ceph_set_fscrypt_policy_v2(cmount, fd, &policy);

  int fd2 = ceph_open(cmount, src_path.c_str(), O_RDWR|O_CREAT|O_TRUNC, 0600);
  r = ceph_write(cmount, fd2, fscrypt_key, sizeof(fscrypt_key), 0);
  ASSERT_EQ(32, r);

  ceph_close(cmount, fd);
  ceph_close(cmount, fd2);

  ino_t inode = 0;
  struct ceph_dir_result *rdir;
  struct dirent *result;
  ASSERT_EQ(ceph_opendir(cmount, "dir1", &rdir), 0);
  while ((result = ceph_readdir(cmount, rdir)) != NULL) {
    if (strcmp(result->d_name, "file5") == 0) {
      inode = result->d_ino;
    }
  }

  fscrypt_remove_key_arg arg;
  generate_remove_key_arg(keyid, &arg);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0, arg.removal_status_flags);

  ASSERT_EQ(ceph_opendir(cmount, "dir1", &rdir), 0);
  while ((result = ceph_readdir(cmount, rdir)) != NULL) {
    if (result->d_ino == inode){
      src_path = dir_path;
      src_path.append("/");
      src_path.append(result->d_name);
      break;
    }
  }
  ASSERT_EQ(ceph_closedir(cmount, rdir), 0);

  string dest_path = "file_dest";

  ASSERT_EQ(ceph_rename(cmount, src_path.c_str(), dest_path.c_str()), -ENOKEY);

  ASSERT_EQ(0, ceph_unlink(cmount, src_path.c_str()));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path.c_str()));
  ceph_shutdown(cmount);
}

TEST(FSCrypt, RenameLockedDest) {
  struct ceph_mount_info* cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  string dir_path = "dir1";
  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path.c_str(), 0777));

  string src_path = "file_src";
  string dest_path = "dir1/file_dest";
  int fd = ceph_open(cmount, dir_path.c_str(), O_DIRECTORY, 0);

  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), keyid, 1299);

  struct fscrypt_policy_v2 policy;
  populate_policy(keyid, &policy);

  r = ceph_set_fscrypt_policy_v2(cmount, fd, &policy);

  int fd2 = ceph_open(cmount, src_path.c_str(), O_RDWR|O_CREAT|O_TRUNC, 0600);
  r = ceph_write(cmount, fd2, fscrypt_key, sizeof(fscrypt_key), 0);
  ASSERT_EQ(32, r);

  ceph_close(cmount, fd);
  ceph_close(cmount, fd2);

  fscrypt_remove_key_arg arg;
  generate_remove_key_arg(keyid, &arg);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0, arg.removal_status_flags);

  ASSERT_EQ(ceph_rename(cmount, src_path.c_str(), dest_path.c_str()), -ENOKEY);

  ASSERT_EQ(0, ceph_unlink(cmount, src_path.c_str()));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path.c_str()));
  ceph_shutdown(cmount);
}

TEST(FSCrypt, RemoveBusyFile) {
  struct ceph_mount_info *cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  string dir_path = "dir1";
  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path.c_str(), 0777));

  int fd = ceph_open(cmount, dir_path.c_str(), O_DIRECTORY, 0);

  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), keyid, 1299);

  struct fscrypt_policy_v2 policy;
  populate_policy(keyid, &policy);

  r = ceph_set_fscrypt_policy_v2(cmount, fd, &policy);
  ceph_close(cmount, fd);
  
  string src_path = "file_src";
  string path = "";
  path.append(dir_path);
  path.append("/");
  path.append(src_path);
  int fd2 = ceph_open(cmount, path.c_str(), O_RDWR|O_CREAT|O_TRUNC, 0600);
  r = ceph_write(cmount, fd2, fscrypt_key, sizeof(fscrypt_key), 0);
  ASSERT_EQ(32, r);

  fscrypt_remove_key_arg arg;
  generate_remove_key_arg(keyid, &arg);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(FSCRYPT_KEY_REMOVAL_STATUS_FLAG_FILES_BUSY, arg.removal_status_flags);
  ceph_close(cmount, fd2);

  ASSERT_EQ(0, ceph_unlink(cmount, path.c_str()));

  //actually remove the key
  generate_remove_key_arg(keyid, &arg);
  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0, arg.removal_status_flags);

  ceph_close(cmount, fd2);
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path.c_str()));
  ceph_shutdown(cmount);
}

TEST(FSCrypt, RemoveBusyCreate) {
  struct ceph_mount_info *cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  string dir_path = "dir_busy";
  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path.c_str(), 0777));
  int fd = ceph_open(cmount, dir_path.c_str(), O_DIRECTORY, 0);

  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), keyid, 1299);

  struct fscrypt_policy_v2 policy;
  populate_policy(keyid, &policy);

  r = ceph_set_fscrypt_policy_v2(cmount, fd, &policy);
  ceph_close(cmount, fd);
  
  string src_path = "file_src";
  string path = "";
  path.append(dir_path);
  path.append("/");
  path.append(src_path);
  int fd2 = ceph_open(cmount, path.c_str(), O_RDWR|O_CREAT|O_TRUNC, 0600);
  r = ceph_write(cmount, fd2, fscrypt_key, sizeof(fscrypt_key), 0);
  ASSERT_EQ(32, r);

  fscrypt_remove_key_arg arg;
  generate_remove_key_arg(keyid, &arg);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(FSCRYPT_KEY_REMOVAL_STATUS_FLAG_FILES_BUSY, arg.removal_status_flags);

  ceph_close(cmount, fd2);

  string src_path2 = "file2_src";
  string path2 = "";
  path2 = "";
  path2.append(dir_path);
  path2.append("/");
  path2.append(src_path2);
  int fd3 = ceph_open(cmount, path2.c_str(), O_RDWR|O_CREAT|O_TRUNC, 0600);
  ASSERT_EQ(-ENOKEY, fd3);
  
  ASSERT_EQ(0, ceph_unlink(cmount, path.c_str()));

  //actually remove the key
  generate_remove_key_arg(keyid, &arg);
  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0, arg.removal_status_flags);

  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path.c_str()));
  ceph_unmount(cmount);
  ceph_shutdown(cmount);
}

// this test verifies that fuse client doesn't support falloate ops
// FALLOC_FL_COLLAPSE_RANGE, FALLOC_FL_COLLAPSE_RANGE, FALLOC_FL_INSERT_RANGE
// if this test fails, it means that these ops has been impleneted AND we must reject these ops for encrypted files
// see https://www.kernel.org/doc/html/v4.18/filesystems/fscrypt.html Access Semantics section
TEST(FSCrypt, FallocateNotImplemented) {
  struct ceph_mount_info *cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  // init fscrypt on dir
  string dir_path = "dir1";
  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path.c_str(), 0777));

  int fd = ceph_open(cmount, dir_path.c_str(), O_DIRECTORY, 0);
  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), keyid, 1299);
  struct fscrypt_policy_v2 policy;
  populate_policy(keyid, &policy);

  r = ceph_set_fscrypt_policy_v2(cmount, fd, &policy);
  ceph_close(cmount, fd);

  //add file to fscrypt enabled directory and write some data to the file
  string file_name = "file1";
  string file_path = "";
  file_path.append(dir_path);
  file_path.append("/");
  file_path.append(file_name);
  fd = ceph_open(cmount, file_path.c_str(), O_RDWR|O_CREAT|O_TRUNC, 0600);
  r = ceph_write(cmount, fd, fscrypt_key, sizeof(fscrypt_key), 0);
  ASSERT_EQ(32, r);

  // try to fallocate opened file with non supported flags
  r = ceph_fallocate(cmount, fd, FALLOC_FL_COLLAPSE_RANGE, 0, 64);
  ASSERT_EQ(-EOPNOTSUPP, r);
  r = ceph_fallocate(cmount, fd, FALLOC_FL_ZERO_RANGE, 0, 64);
  ASSERT_EQ(-EOPNOTSUPP, r);
  r = ceph_fallocate(cmount, fd, FALLOC_FL_INSERT_RANGE, 0, 64);
  ASSERT_EQ(-EOPNOTSUPP, r);

  // cleanup
  ceph_close(cmount, fd);
  ASSERT_EQ(0, ceph_unlink(cmount, file_path.c_str()));
  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path.c_str()));

  ceph_shutdown(cmount);
}

TEST(FSCrypt, SetPolicyAlreadyExistSamePolicyNotEmpty) {
  struct ceph_mount_info* cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  string dir_path = "dir3";
  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path.c_str(), 0777));

  int fd2 = ceph_open(cmount, dir_path.c_str(), O_DIRECTORY, 0);

  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), keyid, 1299);

  struct fscrypt_policy_v2 policy;
  populate_policy(keyid, &policy);

  r = ceph_set_fscrypt_policy_v2(cmount, fd2, &policy);

  ASSERT_EQ(0, r);

  string file_path = "dir3/file1";
  int fd = ceph_open(cmount, file_path.c_str(), O_RDWR|O_CREAT|O_TRUNC, 0600);
  r = ceph_write(cmount, fd, fscrypt_key, sizeof(fscrypt_key), 0);
  ceph_close(cmount, fd);

  r = ceph_set_fscrypt_policy_v2(cmount, fd2, &policy);

  ASSERT_EQ(0, r);

  ASSERT_EQ(0, ceph_unlink(cmount, file_path.c_str()));

  fscrypt_remove_key_arg arg;
  generate_remove_key_arg(keyid, &arg);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0, arg.removal_status_flags);

  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path.c_str()));
  ceph_shutdown(cmount);
}

TEST(FSCrypt, SetPolicyAlreadyExistDifferentPolicyNotEmpty) {
  struct ceph_mount_info* cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  string dir_path = "dir1";
  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path.c_str(), 0777));

  int fd2 = ceph_open(cmount, dir_path.c_str(), O_DIRECTORY, 0);

  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), keyid, 1299);

  struct fscrypt_policy_v2 policy;
  populate_policy(keyid, &policy);

  r = ceph_set_fscrypt_policy_v2(cmount, fd2, &policy);

  ASSERT_EQ(0, r);

  string file_path = "dir1/file1";
  int fd = ceph_open(cmount, file_path.c_str(), O_RDWR|O_CREAT|O_TRUNC, 0600);
  r = ceph_write(cmount, fd, fscrypt_key, sizeof(fscrypt_key), 0);
  ceph_close(cmount, fd);

  char fscrypt_key2[32];
  for (int i = 0; i < (int)sizeof(fscrypt_key2); ++i) {
    fscrypt_key2[i] = (char)rand();
  }
  char keyid2[FSCRYPT_KEY_IDENTIFIER_SIZE];
  r = ceph_add_fscrypt_key(cmount, fscrypt_key2, sizeof(fscrypt_key2), keyid2, 1299);

  struct fscrypt_policy_v2 policy2;
  populate_policy(keyid2, &policy2);

  r = ceph_set_fscrypt_policy_v2(cmount, fd2, &policy2);

  ASSERT_EQ(-EEXIST, r);

  ASSERT_EQ(0, ceph_unlink(cmount, file_path.c_str()));

  fscrypt_remove_key_arg arg;
  generate_remove_key_arg(keyid, &arg);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0, arg.removal_status_flags);

  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path.c_str()));
  ceph_shutdown(cmount);
}

TEST(FSCrypt, SetPolicyAlreadyExistIgnorePadding) {
  struct ceph_mount_info* cmount;
  int r = init_mount(&cmount);
  ASSERT_EQ(0, r);

  string dir_path = "dir1";
  ASSERT_EQ(0, ceph_mkdir(cmount, dir_path.c_str(), 0777));

  int fd2 = ceph_open(cmount, dir_path.c_str(), O_DIRECTORY, 0);

  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), keyid, 1299);

  struct fscrypt_policy_v2 policy;
  populate_policy(keyid, &policy);

  r = ceph_set_fscrypt_policy_v2(cmount, fd2, &policy);

  ASSERT_EQ(0, r);

  string file_path = "dir1/file1";
  int fd = ceph_open(cmount, file_path.c_str(), O_RDWR|O_CREAT|O_TRUNC, 0600);
  r = ceph_write(cmount, fd, fscrypt_key, sizeof(fscrypt_key), 0);
  ceph_close(cmount, fd);

  r = ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key), keyid, 1299);

  struct fscrypt_policy_v2 policy2;
  populate_policy(keyid, &policy2);

  // Depending on which kernel is used, __reserved len could be 4, 3 or removed.
  // Currently, we do not implement any other features outside original __reserved
  // size of 4. Let's below "initialize" where reserved is without referencing the
  // field. This way in the future if it disappears completely we'll have working
  // code.
  unsigned char* policy2_ptr = (unsigned char*)&policy2;
  *(policy2_ptr+4) = 3;
  *(policy2_ptr+5) = 4;
  *(policy2_ptr+6) = 5;
  *(policy2_ptr+7) = 6;

  r = ceph_set_fscrypt_policy_v2(cmount, fd2, &policy2);
  ASSERT_EQ(0, r);

  ASSERT_EQ(0, ceph_unlink(cmount, file_path.c_str()));

  fscrypt_remove_key_arg arg;
  generate_remove_key_arg(keyid, &arg);

  r = ceph_remove_fscrypt_key(cmount, &arg, 1299);
  ASSERT_EQ(0, r);
  ASSERT_EQ(0, arg.removal_status_flags);

  ASSERT_EQ(0, ceph_rmdir(cmount, dir_path.c_str()));
  ceph_shutdown(cmount);
}

TEST(FSCrypt, FSCryptDummyEncryptionNoExistingRegularPolicy) {
  struct ceph_mount_info* cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));

  string name = get_unique_dir_name();
  name = string("/") + name;

  int r = ceph_mount(cmount, NULL);
  ASSERT_EQ(0, ceph_mkdir(cmount, name.c_str(), 0777));
  ceph_unmount(cmount);

  ASSERT_EQ(0, ceph_conf_set(cmount, "client_fscrypt_dummy_encryption", "true"));
  r = ceph_mount(cmount, name.c_str());
  ASSERT_EQ(0, r);

  int fd = ceph_open(cmount, ".", O_DIRECTORY, 0);

  fscrypt_policy_v2 policy;
  ASSERT_EQ(0, ceph_get_fscrypt_policy_v2(cmount, fd, &policy));


  ceph_shutdown(cmount);
}

namespace {

struct ZcWait {
  std::mutex mtx;
  std::condition_variable cv;
  bool done = false;
  int64_t result = 0;
  ceph_bufferlist* data = nullptr;

  int64_t wait()
  {
    std::unique_lock lock(mtx);
    cv.wait(lock, [this] { return done; });
    return result;
  }
};

void zc_io_cb(int64_t result, ceph_bufferlist* data, void* priv)
{
  auto* w = static_cast<ZcWait*>(priv);
  std::lock_guard lock(w->mtx);
  w->result = result;
  w->data = data;
  w->done = true;
  w->cv.notify_one();
}

/** Create empty dir under root, set fscrypt policy, return dir path. */
string setup_fscrypt_dir(struct ceph_mount_info* cmount, UserPerm* perms)
{
  string dir_path = get_unique_dir_name();
  Inode* root = nullptr;
  EXPECT_EQ(0, ceph_ll_lookup_root(cmount, &root));
  Inode* dir = nullptr;
  struct ceph_statx stx;
  EXPECT_EQ(0, ceph_ll_mkdir(cmount, root, dir_path.c_str(), 0755, &dir, &stx,
                             0, 0, perms));

  char keyid[FSCRYPT_KEY_IDENTIFIER_SIZE];
  EXPECT_EQ(0, ceph_add_fscrypt_key(cmount, fscrypt_key, sizeof(fscrypt_key),
                                    keyid, 0));
  struct fscrypt_policy_v2 policy;
  populate_policy(keyid, &policy);
  int fd = ceph_open(cmount, dir_path.c_str(), O_DIRECTORY, 0);
  EXPECT_GE(fd, 0);
  EXPECT_EQ(0, ceph_set_fscrypt_policy_v2(cmount, fd, &policy));
  ceph_close(cmount, fd);
  return dir_path;
}

void zc_compare_read(ceph_bufferlist* rbl, const string& expected)
{
  ASSERT_NE(nullptr, rbl);
  ASSERT_EQ(expected.size(), ceph_bufferlist_length(rbl));
  struct iovec riov[16];
  int niov = ceph_bufferlist_to_iovec(rbl, riov, 16);
  ASSERT_GT(niov, 0);
  size_t pos = 0;
  for (int i = 0; i < niov; ++i) {
    ASSERT_LE(pos + riov[i].iov_len, expected.size());
    EXPECT_EQ(0,
              memcmp(expected.data() + pos, riov[i].iov_base, riov[i].iov_len));
    pos += riov[i].iov_len;
  }
  EXPECT_EQ(expected.size(), pos);
}

} // namespace

TEST(FSCrypt, ZeroCopyAsyncAlignedRoundTrip)
{
  // Plaintext alloc → async_write → async_read on an FSCrypt inode (4 KiB
  // aligned; crypto still copies into ciphertext/plaintext lists).
  // client_oc=true: ZC routes ciphertext through ObjectCacher so classic
  // ll_read sees the same cached data.
  struct ceph_mount_info* cmount = nullptr;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_set(cmount, "client_oc", "true"));
  ASSERT_EQ(0, ceph_mount(cmount, NULL));
  UserPerm* perms = ceph_mount_perms(cmount);
  string dir_path = setup_fscrypt_dir(cmount, perms);

  Inode* root = nullptr;
  ASSERT_EQ(0, ceph_ll_lookup_root(cmount, &root));
  Inode* dir = nullptr;
  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_ll_lookup(cmount, root, dir_path.c_str(), &dir, &stx, 0, 0,
                              perms));

  Inode* file = nullptr;
  Fh* fh = nullptr;
  ASSERT_EQ(0, ceph_ll_create(cmount, dir, "zc_aligned", 0666,
                              O_RDWR | O_CREAT | O_TRUNC, &file, &fh, &stx, 0,
                              0, perms));

  constexpr size_t len = 4096;
  ceph_bufferlist* wbl = ceph_bufferlist_alloc(len);
  ASSERT_NE(nullptr, wbl);
  struct iovec wiov[1];
  ASSERT_EQ(1, ceph_bufferlist_to_iovec(wbl, wiov, 1));
  memset(wiov[0].iov_base, 0x3c, len);
  string expected(static_cast<char*>(wiov[0].iov_base), len);

  ZcWait wwait;
  struct ceph_io_completion wcomp{zc_io_cb, &wwait};
  ASSERT_EQ(0, ceph_ll_async_write(cmount, fh, 0, wbl, 0, &wcomp));
  ASSERT_EQ(static_cast<int64_t>(len), wwait.wait());

  ZcWait rwait;
  struct ceph_io_completion rcomp{zc_io_cb, &rwait};
  ASSERT_EQ(0, ceph_ll_async_read(cmount, fh, 0, len, &rcomp));
  ASSERT_EQ(static_cast<int64_t>(len), rwait.wait());
  zc_compare_read(rwait.data, expected);
  ceph_bufferlist_put(rwait.data);

  // Cross-check against the classic path.
  char verify[len];
  ASSERT_EQ(static_cast<int>(len), ceph_ll_read(cmount, fh, 0, len, verify));
  EXPECT_EQ(0, memcmp(expected.data(), verify, len));

  ASSERT_EQ(0, ceph_ll_close(cmount, fh));
  ASSERT_EQ(0, ceph_ll_unlink(cmount, dir, "zc_aligned", perms));
  ASSERT_EQ(0, ceph_ll_rmdir(cmount, root, dir_path.c_str(), perms));
  ceph_shutdown(cmount);
}

TEST(FSCrypt, ZeroCopyAsyncUnalignedRoundTrip)
{
  // Unaligned WRITE forces FSCrypt RMW; READ decrypts a block-aligned span.
  // With client_oc, RMW/encrypt dirties ObjectCacher; ll_read must agree.
  struct ceph_mount_info* cmount = nullptr;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_set(cmount, "client_oc", "true"));
  ASSERT_EQ(0, ceph_mount(cmount, NULL));
  UserPerm* perms = ceph_mount_perms(cmount);
  string dir_path = setup_fscrypt_dir(cmount, perms);

  Inode* root = nullptr;
  ASSERT_EQ(0, ceph_ll_lookup_root(cmount, &root));
  Inode* dir = nullptr;
  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_ll_lookup(cmount, root, dir_path.c_str(), &dir, &stx, 0, 0,
                              perms));

  Inode* file = nullptr;
  Fh* fh = nullptr;
  ASSERT_EQ(0, ceph_ll_create(cmount, dir, "zc_unaligned", 0666,
                              O_RDWR | O_CREAT | O_TRUNC, &file, &fh, &stx, 0,
                              0, perms));

  // Seed a full block via classic write so RMW has plaintext neighbors.
  char seed[4096];
  memset(seed, 0xaa, sizeof(seed));
  ASSERT_EQ(static_cast<int>(sizeof(seed)),
            ceph_ll_write(cmount, fh, 0, sizeof(seed), seed));

  constexpr uint64_t off = 100;
  constexpr size_t len = 200;
  ceph_bufferlist* wbl = ceph_bufferlist_alloc(len);
  ASSERT_NE(nullptr, wbl);
  struct iovec wiov[1];
  ASSERT_EQ(1, ceph_bufferlist_to_iovec(wbl, wiov, 1));
  memset(wiov[0].iov_base, 0x5a, len);
  string expected(static_cast<char*>(wiov[0].iov_base), len);

  ZcWait wwait;
  struct ceph_io_completion wcomp{zc_io_cb, &wwait};
  ASSERT_EQ(0, ceph_ll_async_write(cmount, fh, off, wbl, 0, &wcomp));
  ASSERT_EQ(static_cast<int64_t>(len), wwait.wait());

  ZcWait rwait;
  struct ceph_io_completion rcomp{zc_io_cb, &rwait};
  ASSERT_EQ(0, ceph_ll_async_read(cmount, fh, off, len, &rcomp));
  ASSERT_EQ(static_cast<int64_t>(len), rwait.wait());
  zc_compare_read(rwait.data, expected);
  ceph_bufferlist_put(rwait.data);

  char verify[len];
  ASSERT_EQ(static_cast<int>(len), ceph_ll_read(cmount, fh, off, len, verify));
  EXPECT_EQ(0, memcmp(expected.data(), verify, len));

  ASSERT_EQ(0, ceph_ll_close(cmount, fh));
  ASSERT_EQ(0, ceph_ll_unlink(cmount, dir, "zc_unaligned", perms));
  ASSERT_EQ(0, ceph_ll_rmdir(cmount, root, dir_path.c_str(), perms));
  ceph_shutdown(cmount);
}
#endif

int main(int argc, char **argv)
{
  int r;
#if defined(__linux__)
  r = update_root_mode();
  if (r < 0)
    exit(1);
#endif
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

#if defined(__linux__)
  r = init_fscrypt();
  if (r < 0)
    exit(1);
#endif
  // toggle the bool only after we are ready to run tests
  fscrypt_enabled = true;
  r = RUN_ALL_TESTS();

  rados_shutdown(cluster);

  return r;
}
