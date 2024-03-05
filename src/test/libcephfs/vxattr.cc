// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat Inc.
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
#include "include/fs_types.h"
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
#include <string.h>

#ifndef _WIN32
#include <sys/resource.h>
#endif

#include "common/Clock.h"
#include "common/ceph_json.h"

#ifdef __linux__
#include <limits.h>
#include <sys/xattr.h>
#endif

#include <fmt/format.h>
#include <map>
#include <vector>
#include <thread>
#include <regex>
#include <string>

using namespace std;

TEST(LibCephFS, LayoutVerifyDefaultLayout) {

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  ASSERT_EQ(0, ceph_mkdirs(cmount, "test/d0/subdir", 0777));

  {
    char value[1024] = "";
    int r = 0;

    // check for default layout
    r = ceph_getxattr(cmount, "/", "ceph.dir.layout.json", (void*)value, sizeof(value));
    ASSERT_GT(r, 0);
    ASSERT_LT(r, sizeof value);
    std::clog << "layout:" << value << std::endl;
    ASSERT_STRNE((char*)NULL, strstr(value, "\"inheritance\": \"@default\""));
  }

  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0/subdir"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test"));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, LayoutSetAndVerifyNewAndInheritedLayout) {

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  ASSERT_EQ(0, ceph_mkdirs(cmount, "test/d0/subdir", 0777));

  std::string pool_name_set;

  {
    char value[1024] = "";
    int r = 0;

    r = ceph_getxattr(cmount, "/", "ceph.dir.layout.json", (void*)value, sizeof(value));
    ASSERT_GT(r, 0);
    ASSERT_LT(r, sizeof value);

    JSONParser json_parser;
    ASSERT_EQ(json_parser.parse(value, r), 1);
    ASSERT_EQ(json_parser.is_object(), 1);

    std::string pool_name;

    JSONDecoder::decode_json("pool_name", pool_name, &json_parser, true);

    pool_name_set = pool_name;

    // set a new layout
    std::string new_layout;
    new_layout += "{";
    new_layout += "\"stripe_unit\": 65536, ";
    new_layout += "\"stripe_count\": 1, ";
    new_layout += "\"object_size\": 65536, ";
    new_layout += "\"pool_name\": \"" + pool_name + "\"";
    new_layout +=  "}";

    ASSERT_EQ(0, ceph_setxattr(cmount, "test/d0", "ceph.dir.layout.json", (void*)new_layout.c_str(), new_layout.length(), XATTR_CREATE));
  }

  {
    char value[1024] = "";
    int r = 0;

    r = ceph_getxattr(cmount, "test/d0", "ceph.dir.layout.json", (void*)value, sizeof(value));
    ASSERT_GT(r, 0);
    ASSERT_LT(r, sizeof value);
    std::clog << "layout:" << value << std::endl;

    JSONParser json_parser;
    ASSERT_EQ(json_parser.parse(value, r), 1);
    ASSERT_EQ(json_parser.is_object(), 1);

    int64_t object_size;
    int64_t stripe_unit;
    int64_t stripe_count;
    std::string pool_name;
    std::string inheritance;

    JSONDecoder::decode_json("pool_name", pool_name, &json_parser, true);
    JSONDecoder::decode_json("object_size", object_size, &json_parser, true);
    JSONDecoder::decode_json("stripe_unit", stripe_unit, &json_parser, true);
    JSONDecoder::decode_json("stripe_count", stripe_count, &json_parser, true);
    JSONDecoder::decode_json("inheritance", inheritance, &json_parser, true);

    // now verify the layout
    ASSERT_EQ(pool_name.compare(pool_name_set), 0);
    ASSERT_EQ(object_size, 65536);
    ASSERT_EQ(stripe_unit, 65536);
    ASSERT_EQ(stripe_count, 1);
    ASSERT_EQ(inheritance.compare("@set"), 0);
  }

  {
    char value[1024] = "";
    int r = 0;

    JSONParser json_parser;
    std::string inheritance;

    // now check that the subdir layout is inherited
    r = ceph_getxattr(cmount, "test/d0/subdir", "ceph.dir.layout.json", (void*)value, sizeof(value));
    ASSERT_GT(r, 0);
    ASSERT_LT(r, sizeof value);
    std::clog << "layout:" << value << std::endl;
    ASSERT_EQ(json_parser.parse(value, r), 1);
    ASSERT_EQ(json_parser.is_object(), 1);
    JSONDecoder::decode_json("inheritance", inheritance, &json_parser, true);
    ASSERT_EQ(inheritance.compare("@inherited"), 0);
  }

  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0/subdir"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test"));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, LayoutSetBadJSON) {

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  ASSERT_EQ(0, ceph_mkdirs(cmount, "test/d0/subdir", 0777));

  {
    // set a new layout and verify the same
    const char *new_layout = "" // bad json without starting brace
      "\"stripe_unit\": 65536, "
      "\"stripe_count\": 1, "
      "\"object_size\": 65536, "
      "\"pool_name\": \"cephfs.a.data\", "
      "}";
    // try to set a malformed JSON, eg. without an open brace
    ASSERT_EQ(-CEPHFS_EINVAL, ceph_setxattr(cmount, "test/d0", "ceph.dir.layout.json", (void*)new_layout, strlen(new_layout), XATTR_CREATE));
  }

  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0/subdir"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test"));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, LayoutSetBadPoolName) {

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  ASSERT_EQ(0, ceph_mkdirs(cmount, "test/d0/subdir", 0777));

  {
    // try setting a bad pool name
    ASSERT_EQ(-CEPHFS_EINVAL, ceph_setxattr(cmount, "test/d0", "ceph.dir.layout.pool_name", (void*)"UglyPoolName", 12, XATTR_CREATE));
  }

  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0/subdir"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test"));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, LayoutSetBadPoolId) {

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  ASSERT_EQ(0, ceph_mkdirs(cmount, "test/d0/subdir", 0777));

  {
    // try setting a bad pool id
    ASSERT_EQ(-CEPHFS_EINVAL, ceph_setxattr(cmount, "test/d0", "ceph.dir.layout.pool_id", (void*)"300", 3, XATTR_CREATE));
  }

  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0/subdir"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test"));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, LayoutSetInvalidFieldName) {

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  ASSERT_EQ(0, ceph_mkdirs(cmount, "test/d0/subdir", 0777));

  {
    // try to set in invalid field
    ASSERT_EQ(-CEPHFS_ENODATA, ceph_setxattr(cmount, "test/d0", "ceph.dir.layout.bad_field", (void*)"300", 3, XATTR_CREATE));
  }

  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0/subdir"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d0"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test"));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, GetAndSetDirPin) {

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  ASSERT_EQ(0, ceph_mkdirs(cmount, "test/d1", 0777));

  {
    char value[1024] = "";
    int r = ceph_getxattr(cmount, "test/d1", "ceph.dir.pin", (void*)value, sizeof(value));
    ASSERT_GT(r, 0);
    ASSERT_LT(r, sizeof value);
    ASSERT_STREQ("-1", value);
  }

  {
    char value[1024] = "";
    int r = -1;

    ASSERT_EQ(0, ceph_setxattr(cmount, "test/d1", "ceph.dir.pin", (void*)"1", 1, XATTR_CREATE));

    r = ceph_getxattr(cmount, "test/d1", "ceph.dir.pin", (void*)value, sizeof(value));
    ASSERT_GT(r, 0);
    ASSERT_LT(r, sizeof value);
    ASSERT_STREQ("1", value);
  }

  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d1"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test"));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, GetAndSetDirDistribution) {

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  ASSERT_EQ(0, ceph_mkdirs(cmount, "test/d2", 0777));

  {
    char value[1024] = "";
    int r = ceph_getxattr(cmount, "test/d2", "ceph.dir.pin.distributed", (void*)value, sizeof(value));
    ASSERT_GT(r, 0);
    ASSERT_LT(r, sizeof value);
    ASSERT_STREQ("0", value);
  }

  {
    char value[1024] = "";
    int r = -1;

    ASSERT_EQ(0, ceph_setxattr(cmount, "test/d2", "ceph.dir.pin.distributed", (void*)"1", 1, XATTR_CREATE));

    r = ceph_getxattr(cmount, "test/d2", "ceph.dir.pin.distributed", (void*)value, sizeof(value));
    ASSERT_GT(r, 0);
    ASSERT_LT(r, sizeof value);
    ASSERT_STREQ("1", value);
  }

  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d2"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test"));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, GetAndSetDirRandom) {

  struct ceph_mount_info *cmount;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  ASSERT_EQ(0, ceph_mkdirs(cmount, "test/d3", 0777));

  {
    char value[1024] = "";
    int r = ceph_getxattr(cmount, "test/d3", "ceph.dir.pin.random", (void*)value, sizeof(value));
    ASSERT_GT(r, 0);
    ASSERT_LT(r, sizeof value);
    ASSERT_STREQ("0", value);
  }

  {
    double val = (double)1.0/(double)128.0;
    std::stringstream ss;
    ss << val;
    ASSERT_EQ(0, ceph_setxattr(cmount, "test/d3", "ceph.dir.pin.random", (void*)ss.str().c_str(), strlen(ss.str().c_str()), XATTR_CREATE));

    char value[1024] = "";
    int r = -1;

    r = ceph_getxattr(cmount, "test/d3", "ceph.dir.pin.random", (void*)value, sizeof(value));
    ASSERT_GT(r, 0);
    ASSERT_LT(r, sizeof value);
    ASSERT_STREQ(ss.str().c_str(), value);
  }

  ASSERT_EQ(0, ceph_rmdir(cmount, "test/d3"));
  ASSERT_EQ(0, ceph_rmdir(cmount, "test"));

  ceph_shutdown(cmount);
}

TEST(LibCephFS, FsCrypt) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char test_xattr_file[NAME_MAX];
  sprintf(test_xattr_file, "test_fscrypt_%d", getpid());
  int fd = ceph_open(cmount, test_xattr_file, O_RDWR|O_CREAT, 0666);
  ASSERT_GT(fd, 0);

  ASSERT_EQ(0, ceph_fsetxattr(cmount, fd, "ceph.fscrypt.auth", "foo", 3, XATTR_CREATE));
  ASSERT_EQ(0, ceph_fsetxattr(cmount, fd, "ceph.fscrypt.file", "foo", 3, XATTR_CREATE));

  char buf[64];
  ASSERT_EQ(3, ceph_fgetxattr(cmount, fd, "ceph.fscrypt.auth", buf, sizeof(buf)));
  ASSERT_EQ(3, ceph_fgetxattr(cmount, fd, "ceph.fscrypt.file", buf, sizeof(buf)));
  ASSERT_EQ(0, ceph_close(cmount, fd));

  ASSERT_EQ(0, ceph_unmount(cmount));
  ASSERT_EQ(0, ceph_mount(cmount, NULL));

  fd = ceph_open(cmount, test_xattr_file, O_RDWR, 0666);
  ASSERT_GT(fd, 0);
  ASSERT_EQ(3, ceph_fgetxattr(cmount, fd, "ceph.fscrypt.auth", buf, sizeof(buf)));
  ASSERT_EQ(3, ceph_fgetxattr(cmount, fd, "ceph.fscrypt.file", buf, sizeof(buf)));

  ASSERT_EQ(0, ceph_close(cmount, fd));
  ASSERT_EQ(0, ceph_unmount(cmount));
  ceph_shutdown(cmount);
}

#define ACL_EA_ACCESS  "system.posix_acl_access"
#define ACL_EA_DEFAULT "system.posix_acl_default"

TEST(LibCephFS, Removexattr) {
  struct ceph_mount_info *cmount;
  ASSERT_EQ(ceph_create(&cmount, NULL), 0);
  ASSERT_EQ(ceph_conf_read_file(cmount, NULL), 0);
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(ceph_mount(cmount, NULL), 0);

  char test_xattr_file[NAME_MAX];
  sprintf(test_xattr_file, "test_removexattr_%d", getpid());
  int fd = ceph_open(cmount, test_xattr_file, O_RDWR|O_CREAT, 0666);
  ASSERT_GT(fd, 0);

  // remove xattr
  ASSERT_EQ(-ENODATA, ceph_fremovexattr(cmount, fd, "user.remove.xattr"));
  ASSERT_EQ(0, ceph_fsetxattr(cmount, fd, "user.remove.xattr", "foo", 3, XATTR_CREATE));
  ASSERT_EQ(0, ceph_fremovexattr(cmount, fd, "user.remove.xattr"));

  // remove xattr via setxattr & XATTR_REPLACE
  ASSERT_EQ(-ENODATA, ceph_fsetxattr(cmount, fd, "user.remove.xattr", nullptr, 0, XATTR_REPLACE));
  ASSERT_EQ(0, ceph_fsetxattr(cmount, fd, "user.remove.xattr", "foo", 3, XATTR_CREATE));
  ASSERT_EQ(0, ceph_fsetxattr(cmount, fd, "user.remove.xattr", nullptr, 0, XATTR_REPLACE));

  // ACL_EA_ACCESS and ACL_EA_DEFAULT are special and will
  ASSERT_EQ(0, ceph_fremovexattr(cmount, fd, ACL_EA_ACCESS));
  ASSERT_EQ(0, ceph_fremovexattr(cmount, fd, ACL_EA_ACCESS));
  ASSERT_EQ(0, ceph_fremovexattr(cmount, fd, ACL_EA_DEFAULT));
  ASSERT_EQ(0, ceph_fremovexattr(cmount, fd, ACL_EA_DEFAULT));

  ASSERT_EQ(0, ceph_close(cmount, fd));
  ASSERT_EQ(0, ceph_unmount(cmount));
  ceph_shutdown(cmount);
}

