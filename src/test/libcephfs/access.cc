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
#include "common/ceph_argparse.h"
#include "include/buffer.h"
#include "include/stringify.h"
#include "include/cephfs/libcephfs.h"
#include "include/rados/librados.h"
#include <errno.h>
#include <sys/fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dirent.h>
#include <sys/xattr.h>
#include <sys/uio.h>
#include <iostream>
#include <vector>
#include "json_spirit/json_spirit.h"

#ifdef __linux__
#include <limits.h>
#endif


rados_t cluster;

string key;

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
    return -EINVAL;
  }
  if (outs_len) {
    string s(outs, outs_len);
    std::cout << "outs: " << s << std::endl;
    free(outs);
  }
  return r;
}

string get_unique_dir()
{
  return string("/ceph_test_libcephfs.") + stringify(rand());
}

TEST(AccessTest, Foo) {
  string dir = get_unique_dir();
  string user = "libcephfs_foo_test." + stringify(rand());
  // admin mount to set up test
  struct ceph_mount_info *admin;
  ASSERT_EQ(0, ceph_create(&admin, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(admin, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(admin, NULL));
  ASSERT_EQ(0, ceph_mount(admin, "/"));
  ASSERT_EQ(0, ceph_mkdir(admin, dir.c_str(), 0755));

  // create access key
  string key;
  ASSERT_EQ(0, do_mon_command(
      "{\"prefix\": \"auth get-or-create\", \"entity\": \"client." + user + "\", "
      "\"caps\": [\"mon\", \"allow *\", \"osd\", \"allow rw\", "
      "\"mds\", \"allow rw\""
      "], \"format\": \"json\"}", &key));

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, user.c_str()));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_set(cmount, "key", key.c_str()));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  ceph_shutdown(cmount);

  // clean up
  ASSERT_EQ(0, ceph_rmdir(admin, dir.c_str()));
  ceph_shutdown(admin);
}

TEST(AccessTest, Path) {
  string good = get_unique_dir();
  string bad = get_unique_dir();
  string user = "libcephfs_path_test." + stringify(rand());
  struct ceph_mount_info *admin;
  ASSERT_EQ(0, ceph_create(&admin, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(admin, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(admin, NULL));
  ASSERT_EQ(0, ceph_mount(admin, "/"));
  ASSERT_EQ(0, ceph_mkdir(admin, good.c_str(), 0755));
  ASSERT_EQ(0, ceph_mkdir(admin, string(good + "/p").c_str(), 0755));
  ASSERT_EQ(0, ceph_mkdir(admin, bad.c_str(), 0755));
  ASSERT_EQ(0, ceph_mkdir(admin, string(bad + "/p").c_str(), 0755));
  int fd = ceph_open(admin, string(good + "/q").c_str(), O_CREAT|O_WRONLY, 0755);
  ceph_close(admin, fd);
  fd = ceph_open(admin, string(bad + "/q").c_str(), O_CREAT|O_WRONLY, 0755);
  ceph_close(admin, fd);
  fd = ceph_open(admin, string(bad + "/z").c_str(), O_CREAT|O_WRONLY, 0755);
  ceph_write(admin, fd, "TEST FAILED", 11, 0);
  ceph_close(admin, fd);

  string key;
  ASSERT_EQ(0, do_mon_command(
      "{\"prefix\": \"auth get-or-create\", \"entity\": \"client." + user + "\", "
      "\"caps\": [\"mon\", \"allow r\", \"osd\", \"allow rwx\", "
      "\"mds\", \"allow r, allow rw path=" + good + "\""
      "], \"format\": \"json\"}", &key));

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, user.c_str()));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_set(cmount, "key", key.c_str()));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  // allowed
  ASSERT_GE(ceph_mkdir(cmount, string(good + "/x").c_str(), 0755), 0);
  ASSERT_GE(ceph_rmdir(cmount, string(good + "/p").c_str()), 0);
  ASSERT_GE(ceph_unlink(cmount, string(good + "/q").c_str()), 0);
  fd = ceph_open(cmount, string(good + "/y").c_str(), O_CREAT|O_WRONLY, 0755);
  ASSERT_GE(fd, 0);
  ceph_write(cmount, fd, "bar", 3, 0);
  ceph_close(cmount, fd);
  ASSERT_GE(ceph_unlink(cmount, string(good + "/y").c_str()), 0);
  ASSERT_GE(ceph_rmdir(cmount, string(good + "/x").c_str()), 0);

  fd = ceph_open(cmount, string(bad + "/z").c_str(), O_RDONLY, 0644);
  ASSERT_GE(fd, 0);
  ceph_close(cmount, fd);

  // not allowed
  ASSERT_LT(ceph_mkdir(cmount, string(bad + "/x").c_str(), 0755), 0);
  ASSERT_LT(ceph_rmdir(cmount, string(bad + "/p").c_str()), 0);
  ASSERT_LT(ceph_unlink(cmount, string(bad + "/q").c_str()), 0);
  fd = ceph_open(cmount, string(bad + "/y").c_str(), O_CREAT|O_WRONLY, 0755);
  ASSERT_LT(fd, 0);

  // unlink open file
  fd = ceph_open(cmount, string(good + "/unlinkme").c_str(), O_CREAT|O_WRONLY, 0755);
  ceph_unlink(cmount, string(good + "/unlinkme").c_str());
  ASSERT_GE(ceph_write(cmount, fd, "foo", 3, 0), 0);
  ASSERT_GE(ceph_fchmod(cmount, fd, 0777), 0);
  ASSERT_GE(ceph_ftruncate(cmount, fd, 0), 0);
  ASSERT_GE(ceph_fsetxattr(cmount, fd, "user.any", "bar", 3, 0), 0);
  ceph_close(cmount, fd);

  // rename open file
  fd = ceph_open(cmount, string(good + "/renameme").c_str(), O_CREAT|O_WRONLY, 0755);
  ASSERT_EQ(ceph_rename(admin, string(good + "/renameme").c_str(),
			string(bad + "/asdf").c_str()), 0);
  ASSERT_GE(ceph_write(cmount, fd, "foo", 3, 0), 0);
  ASSERT_GE(ceph_fchmod(cmount, fd, 0777), -EACCES);
  ASSERT_GE(ceph_ftruncate(cmount, fd, 0), -EACCES);
  ASSERT_GE(ceph_fsetxattr(cmount, fd, "user.any", "bar", 3, 0), -EACCES);
  ceph_close(cmount, fd);

  ceph_shutdown(cmount);
  ASSERT_EQ(0, ceph_unlink(admin, string(bad + "/q").c_str()));
  ASSERT_EQ(0, ceph_unlink(admin, string(bad + "/z").c_str()));
  ASSERT_EQ(0, ceph_rmdir(admin, string(bad + "/p").c_str()));
  ASSERT_EQ(0, ceph_unlink(admin, string(bad + "/asdf").c_str()));
  ASSERT_EQ(0, ceph_rmdir(admin, good.c_str()));
  ASSERT_EQ(0, ceph_rmdir(admin, bad.c_str()));
  ceph_shutdown(admin);
}

TEST(AccessTest, ReadOnly) {
  string dir = get_unique_dir();
  string dir2 = get_unique_dir();
  string user = "libcephfs_readonly_test." + stringify(rand());
  struct ceph_mount_info *admin;
  ASSERT_EQ(0, ceph_create(&admin, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(admin, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(admin, NULL));
  ASSERT_EQ(0, ceph_mount(admin, "/"));
  ASSERT_EQ(0, ceph_mkdir(admin, dir.c_str(), 0755));
  int fd = ceph_open(admin, string(dir + "/out").c_str(), O_CREAT|O_WRONLY, 0755);
  ceph_write(admin, fd, "foo", 3, 0);
  ceph_close(admin,fd);

  string key;
  ASSERT_EQ(0, do_mon_command(
      "{\"prefix\": \"auth get-or-create\", \"entity\": \"client." + user + "\", "
      "\"caps\": [\"mon\", \"allow r\", \"osd\", \"allow rw\", "
      "\"mds\", \"allow r\""
      "], \"format\": \"json\"}", &key));

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, user.c_str()));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_set(cmount, "key", key.c_str()));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  // allowed
  fd = ceph_open(cmount, string(dir + "/out").c_str(), O_RDONLY, 0644);
  ASSERT_GE(fd, 0);
  ceph_close(cmount,fd);

  // not allowed
  fd = ceph_open(cmount, string(dir + "/bar").c_str(), O_CREAT|O_WRONLY, 0755);
  ASSERT_LT(fd, 0);
  ASSERT_LT(ceph_mkdir(cmount, dir2.c_str(), 0755), 0);

  ceph_shutdown(cmount);
  ASSERT_EQ(0, ceph_unlink(admin, string(dir + "/out").c_str()));
  ASSERT_EQ(0, ceph_rmdir(admin, dir.c_str()));
  ceph_shutdown(admin);
}

TEST(AccessTest, User) {
  string dir = get_unique_dir();
  string user = "libcephfs_user_test." + stringify(rand());

  // admin mount to set up test
  struct ceph_mount_info *admin;
  ASSERT_EQ(0, ceph_create(&admin, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(admin, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(admin, NULL));
  ASSERT_EQ(0, ceph_conf_set(admin, "client_permissions", "0"));
  ASSERT_EQ(0, ceph_mount(admin, "/"));
  ASSERT_EQ(0, ceph_mkdir(admin, dir.c_str(), 0755));

  // create access key
  string key;
  ASSERT_EQ(0, do_mon_command(
      "{\"prefix\": \"auth get-or-create\", \"entity\": \"client." + user + "\", "
      "\"caps\": [\"mon\", \"allow *\", \"osd\", \"allow rw\", "
      "\"mds\", \"allow rw uid=123 gids=456,789\""
      "], \"format\": \"json\"}", &key));

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, user.c_str()));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_set(cmount, "key", key.c_str()));
  ASSERT_EQ(-EACCES, ceph_mount(cmount, "/"));
  ASSERT_EQ(0, ceph_conf_set(cmount, "client_mount_uid", "123"));
  ASSERT_EQ(0, ceph_conf_set(cmount, "client_mount_gid", "456"));
  ASSERT_EQ(0, ceph_conf_set(cmount, "client_permissions", "0"));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  // user bits
  ASSERT_EQ(0, ceph_chmod(admin, dir.c_str(), 0700));
  ASSERT_EQ(0, ceph_chown(admin, dir.c_str(), 123, 456));
  ASSERT_EQ(0, ceph_mkdir(cmount, string(dir + "/u1").c_str(), 0755));
  ASSERT_EQ(0, ceph_chown(admin, dir.c_str(), 1, 456));
  ASSERT_EQ(-EACCES, ceph_mkdir(cmount, string(dir + "/no").c_str(), 0755));

  // group bits
  ASSERT_EQ(0, ceph_chmod(admin, dir.c_str(), 0770));
  ASSERT_EQ(0, ceph_chown(admin, dir.c_str(), 1, 456));
  ASSERT_EQ(0, ceph_mkdir(cmount, string(dir + "/u2").c_str(), 0755));
  ASSERT_EQ(0, ceph_chown(admin, dir.c_str(), 1, 2));
  ASSERT_EQ(-EACCES, ceph_mkdir(cmount, string(dir + "/no").c_str(), 0755));

  // user overrides group
  ASSERT_EQ(0, ceph_chmod(admin, dir.c_str(), 0470));
  ASSERT_EQ(0, ceph_chown(admin, dir.c_str(), 123, 456));
  ASSERT_EQ(-EACCES, ceph_mkdir(cmount, string(dir + "/no").c_str(), 0755));

  // other
  ASSERT_EQ(0, ceph_chmod(admin, dir.c_str(), 0777));
  ASSERT_EQ(0, ceph_chown(admin, dir.c_str(), 1, 1));
  ASSERT_EQ(0, ceph_mkdir(cmount, string(dir + "/u3").c_str(), 0755));
  ASSERT_EQ(0, ceph_chmod(admin, dir.c_str(), 0770));
  ASSERT_EQ(-EACCES, ceph_mkdir(cmount, string(dir + "/no").c_str(), 0755));

  // user and group overrides other
  ASSERT_EQ(0, ceph_chmod(admin, dir.c_str(), 07));
  ASSERT_EQ(0, ceph_chown(admin, dir.c_str(), 1, 456));
  ASSERT_EQ(-EACCES, ceph_mkdir(cmount, string(dir + "/no").c_str(), 0755));
  ASSERT_EQ(0, ceph_chown(admin, dir.c_str(), 123, 1));
  ASSERT_EQ(-EACCES, ceph_mkdir(cmount, string(dir + "/no").c_str(), 0755));
  ASSERT_EQ(0, ceph_chown(admin, dir.c_str(), 123, 456));
  ASSERT_EQ(-EACCES, ceph_mkdir(cmount, string(dir + "/no").c_str(), 0755));

  // chown and chgrp
  ASSERT_EQ(0, ceph_chmod(admin, dir.c_str(), 0700));
  ASSERT_EQ(0, ceph_chown(admin, dir.c_str(), 123, 456));
  ASSERT_EQ(0, ceph_chown(cmount, dir.c_str(), 123, 789));
  ASSERT_EQ(0, ceph_chown(cmount, dir.c_str(), 123, 456));
  ASSERT_EQ(0, ceph_chown(cmount, dir.c_str(), -1, 789));
  ASSERT_EQ(0, ceph_chown(cmount, dir.c_str(), -1, 456));
  ASSERT_EQ(-EACCES, ceph_chown(cmount, dir.c_str(), 123, 1));
  ASSERT_EQ(-EACCES, ceph_chown(cmount, dir.c_str(), 1, 456));

  ASSERT_EQ(0, ceph_chown(admin, dir.c_str(), 1, 1));
  ASSERT_EQ(-EACCES, ceph_chown(cmount, dir.c_str(), 123, 456));
  ASSERT_EQ(-EACCES, ceph_chown(cmount, dir.c_str(), 123, -1));
  ASSERT_EQ(-EACCES, ceph_chown(cmount, dir.c_str(), -1, 456));

  ASSERT_EQ(0, ceph_chown(admin, dir.c_str(), 1, 456));
  ASSERT_EQ(-EACCES, ceph_chown(cmount, dir.c_str(), 123, 456));
  ASSERT_EQ(-EACCES, ceph_chown(cmount, dir.c_str(), 123, -1));
  ASSERT_EQ(-EACCES, ceph_chown(cmount, dir.c_str(), -1, 456));

  ASSERT_EQ(0, ceph_chown(admin, dir.c_str(), 123, 1));
  ASSERT_EQ(0, ceph_chown(cmount, dir.c_str(), -1, 456));
  ASSERT_EQ(0, ceph_chown(cmount, dir.c_str(), 123, 789));

  ceph_shutdown(cmount);

  // clean up
  ASSERT_EQ(0, ceph_rmdir(admin, string(dir + "/u1").c_str()));
  ASSERT_EQ(0, ceph_rmdir(admin, string(dir + "/u2").c_str()));
  ASSERT_EQ(0, ceph_rmdir(admin, string(dir + "/u3").c_str()));
  ASSERT_EQ(0, ceph_rmdir(admin, dir.c_str()));
  ceph_shutdown(admin);
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

  rados_create(&cluster, NULL);
  rados_conf_read_file(cluster, NULL);
  rados_conf_parse_env(cluster, NULL);
  r = rados_connect(cluster);
  if (r < 0)
    exit(1);

  r = RUN_ALL_TESTS();

  rados_shutdown(cluster);

  return r;
}
