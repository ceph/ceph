// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2020 Red Hat
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
#include "include/fs_types.h"
#include "common/ceph_context.h"
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>

class MonConfig : public ::testing::Test
{
  protected:
    struct ceph_mount_info *ca;

    void SetUp() override {
      ASSERT_EQ(0, ceph_create(&ca, NULL));
      ASSERT_EQ(0, ceph_conf_read_file(ca, NULL));
      ASSERT_EQ(0, ceph_conf_parse_env(ca, NULL));
    }

    void TearDown() override {
      ceph_shutdown(ca);
    }

    // Helper to remove/unset all possible mon information from ConfigProxy
    void clear_mon_config(CephContext *cct) {
      auto& conf = cct->_conf;
      // Clear safe_to_start_threads, allowing updates to config values
      conf._clear_safe_to_start_threads();
      ASSERT_EQ(0, conf.set_val("monmap", "", nullptr));
      ASSERT_EQ(0, conf.set_val("mon_host", "", nullptr));
      ASSERT_EQ(0, conf.set_val("mon_dns_srv_name", "", nullptr));
      conf.set_safe_to_start_threads();
    }

    // Helper to test basic operation on a mount
    void use_mount(struct ceph_mount_info *mnt, std::string name_prefix) {
      char name[20];
      snprintf(name, sizeof(name), "%s.%d", name_prefix.c_str(), getpid());
      int fd = ceph_open(mnt, name, O_CREAT|O_RDWR, 0644);
      ASSERT_LE(0, fd);

      ceph_close(mnt, fd);
    }
};

TEST_F(MonConfig, MonAddrsMissing) {
  CephContext *cct;

  // Test mount failure when there is no known mon config source
  cct = ceph_get_mount_context(ca);
  ASSERT_NE(nullptr, cct);
  clear_mon_config(cct);

  ASSERT_EQ(-CEPHFS_ENOENT, ceph_mount(ca, NULL));
}

TEST_F(MonConfig, MonAddrsInConfigProxy) {
  // Test a successful mount with default mon config source in ConfigProxy
  ASSERT_EQ(0, ceph_mount(ca, NULL));

  use_mount(ca, "foo");
}

TEST_F(MonConfig, MonAddrsInCct) {
  struct ceph_mount_info *cb;
  CephContext *cct;

  // Perform mount to bootstrap mon addrs in CephContext
  ASSERT_EQ(0, ceph_mount(ca, NULL));

  // Reuse bootstrapped CephContext, clearing ConfigProxy mon addr sources
  cct = ceph_get_mount_context(ca);
  ASSERT_NE(nullptr, cct);
  clear_mon_config(cct);
  ASSERT_EQ(0, ceph_create_with_context(&cb, cct));

  // Test a successful mount with only mon values in CephContext
  ASSERT_EQ(0, ceph_mount(cb, NULL));

  use_mount(ca, "bar");
  use_mount(cb, "bar");

  ceph_shutdown(cb);
}
