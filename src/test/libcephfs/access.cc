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
#include "include/buffer.h"
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
#include "common/ceph_argparse.h"
#include "json_spirit/json_spirit.h"

#ifdef __linux__
#include <limits.h>
#endif


rados_t cluster;

string key;

int do_mon_command(const char *s, string *key)
{
  char *outs, *outbuf;
  size_t outs_len, outbuf_len;
  int r = rados_mon_command(cluster, (const char **)&s, 1,
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

TEST(AccessTest, Foo) {
  // admin mount to set up test
  struct ceph_mount_info *admin;
  ASSERT_EQ(0, ceph_create(&admin, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(admin, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(admin, NULL));
  ASSERT_EQ(0, ceph_mount(admin, "/"));
  ASSERT_EQ(0, ceph_mkdir(admin, "/foo", 0755));

  // create access key
  string key;
  ASSERT_EQ(0, do_mon_command(
      "{\"prefix\": \"auth get-or-create\", \"entity\": \"client.foo\", "
      "\"caps\": [\"mon\", \"allow *\", \"osd\", \"allow rw\", "
      "\"mds\", \"allow rw\""
      "], \"format\": \"json\"}", &key));

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, "foo"));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_set(cmount, "key", key.c_str()));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  ceph_shutdown(cmount);

  // clean up
  ASSERT_EQ(0, ceph_rmdir(admin, "/foo"));
  ceph_shutdown(admin);
}

TEST(AccessTest, Path){
  struct ceph_mount_info *admin;
  ASSERT_EQ(0, ceph_create(&admin, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(admin, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(admin, NULL));
  ASSERT_EQ(0, ceph_mount(admin, "/"));
  ASSERT_EQ(0, ceph_mkdir(admin, "/bar", 0755));
  ASSERT_EQ(0, ceph_mkdir(admin, "/foobar", 0755));
  ASSERT_EQ(0, ceph_mkdir(admin, "/bar/p", 0755));
  ASSERT_EQ(0, ceph_mkdir(admin, "/foobar/p", 0755));
  int fd = ceph_open(admin, "/bar/q", O_CREAT|O_WRONLY, 0755);
  ceph_close(admin,fd);
  fd = ceph_open(admin, "/foobar/q", O_CREAT|O_WRONLY, 0755);
  ceph_close(admin,fd);
  fd = ceph_open(admin, "/foobar/z", O_CREAT|O_WRONLY, 0755);
  ceph_write(admin, fd, "TEST FAILED", 11, 0);
  ceph_close(admin,fd);

  string key;
  ASSERT_EQ(0, do_mon_command(
      "{\"prefix\": \"auth get-or-create\", \"entity\": \"client.bar\", "
      "\"caps\": [\"mon\", \"allow r\", \"osd\", \"allow rwx\", "
      "\"mds\", \"allow r, allow rw path=/bar\""
      "], \"format\": \"json\"}", &key));

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, "bar"));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_set(cmount, "key", key.c_str()));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  //allowed operations
  ASSERT_GE(ceph_mkdir(cmount, "/bar/x", 0755), 0);
  ASSERT_GE(ceph_rmdir(cmount, "/bar/p"), 0);
  ASSERT_GE(ceph_unlink(cmount, "/bar/q"), 0);
  fd = ceph_open(cmount, "/bar/y", O_CREAT|O_WRONLY, 0755);
  ASSERT_GE(fd, 0);
  ceph_write(cmount, fd, "bar", 3, 0);
  ceph_close(cmount,fd);
  ASSERT_GE(ceph_unlink(cmount, "/bar/y"), 0);
  ASSERT_GE(ceph_rmdir(cmount, "/bar/x"), 0);
  fd = ceph_open(cmount, "/foobar/z", O_RDONLY, 0644);
  ASSERT_GE(fd, 0);
  ceph_close(cmount,fd);

  //not allowed operations
  ASSERT_LT(ceph_mkdir(cmount, "/foobar/x", 0755), 0);
  ASSERT_LT(ceph_rmdir(cmount, "/foobar/p"), 0);
  ASSERT_LT(ceph_unlink(cmount, "/foobar/q"), 0);
  fd = ceph_open(cmount, "/foobar/y", O_CREAT|O_WRONLY, 0755);
  ASSERT_LT(fd, 0);

  ceph_shutdown(cmount);
  ASSERT_EQ(0, ceph_unlink(admin, "/foobar/q"));
  ASSERT_EQ(0, ceph_unlink(admin, "/foobar/z"));
  ASSERT_EQ(0, ceph_rmdir(admin, "/foobar/p"));
  ASSERT_EQ(0, ceph_rmdir(admin, "/bar"));
  ASSERT_EQ(0, ceph_rmdir(admin, "/foobar"));
  ceph_shutdown(admin);
}

TEST(AccessTest, ReadOnly){
  struct ceph_mount_info *admin;
  ASSERT_EQ(0, ceph_create(&admin, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(admin, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(admin, NULL));
  ASSERT_EQ(0, ceph_mount(admin, "/"));
  ASSERT_EQ(0, ceph_mkdir(admin, "/ronly", 0755));
  int fd = ceph_open(admin, "/ronly/out", O_CREAT|O_WRONLY, 0755);
  ceph_write(admin, fd, "foo", 3, 0);
  ceph_close(admin,fd);

  string key;
  ASSERT_EQ(0, do_mon_command(
      "{\"prefix\": \"auth get-or-create\", \"entity\": \"client.ronly\", "
      "\"caps\": [\"mon\", \"allow r\", \"osd\", \"allow rw\", "
      "\"mds\", \"allow r\""
      "], \"format\": \"json\"}", &key));

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, "ronly"));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_set(cmount, "key", key.c_str()));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  //allowed operations
  fd = ceph_open(cmount, "/ronly/out", O_RDONLY, 0644);
  ASSERT_GE(fd, 0);
  ceph_close(cmount,fd);

  //not allowed operations
  fd = ceph_open(cmount, "/ronly/bar", O_CREAT|O_WRONLY, 0755);
  ASSERT_LT(fd, 0);
  ASSERT_LT(ceph_mkdir(cmount, "/newdir", 0755), 0);

  ceph_shutdown(cmount);
  ASSERT_EQ(0, ceph_unlink(admin, "/ronly/out"));
  ASSERT_EQ(0, ceph_rmdir(admin, "/ronly"));
  ceph_shutdown(admin);
}

TEST(AccessTest, update_after_unlink){
  struct ceph_mount_info *admin;
  ASSERT_EQ(0, ceph_create(&admin, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(admin, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(admin, NULL));
  ASSERT_EQ(0, ceph_mount(admin, "/"));
  ASSERT_EQ(0, ceph_mkdir(admin, "/foo", 0755));
  int fd = ceph_open(admin, "/foo/bar", O_CREAT|O_WRONLY, 0755);
  ceph_unlink(admin, "/foo/bar");
  ASSERT_GE(ceph_write(admin, fd, "foo", 3, 0), 0);
  ASSERT_GE(ceph_fchmod(admin, fd, 0777), 0);
  ASSERT_GE(ceph_ftruncate(admin, fd, 0), 0);
  ASSERT_GE(ceph_fsetxattr(admin, fd, "user.any", "bar", 3, 0), 0);
  ceph_close(admin,fd);

  ASSERT_EQ(0, ceph_rmdir(admin, "/foo"));
  ceph_shutdown(admin);
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);

  rados_create(&cluster, NULL);
  rados_conf_read_file(cluster, NULL);
  rados_conf_parse_env(cluster, NULL);
  int r = rados_connect(cluster);
  if (r < 0)
    exit(1);

  r = RUN_ALL_TESTS();

  rados_shutdown(cluster);

  return r;
}
