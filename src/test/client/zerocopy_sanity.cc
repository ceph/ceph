// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <iostream>
#include <errno.h>
#include "gtest/gtest.h"
#include <include/cephfs/libcephfs.h>
#include "TestClient.h"

// the only unittest we can do is to test the comibnation of
// invalid parameters in libcephfs functions that can use zerocopy
// ceph_ll_readv_writev and ceph_ll_nonblocking_readv_writev doesn't support
// zerocopy, only the _v2 API does
TEST(libcephfs, zerocopy_sanity_invalid_params) {
  struct ceph_mount_info *cmount = nullptr;
  auto res = ceph_create(&cmount, NULL);
  ASSERT_EQ(0, res);
  ceph_ll_io_info_v2 io_info;
  iovec iov;
  io_info.base_info.iov = &iov;
  io_info.base_info.iovcnt = 0;
  io_info.base_info.write = false;
  io_info.zerocopy = true;
  res = ceph_ll_readv_writev_v2(cmount, &io_info.base_info, sizeof(ceph_ll_io_info_v2));
  ASSERT_EQ(res, -EINVAL);

  res = ceph_ll_nonblocking_readv_writev_v2(cmount, &io_info.base_info, sizeof(ceph_ll_io_info_v2));
  ASSERT_EQ(res, -EINVAL);

  res = ceph_ll_readv_writev_v2(cmount, &io_info.base_info, sizeof(ceph_ll_io_info) - 1);
  ASSERT_EQ(res, -EINVAL);

  res = ceph_ll_nonblocking_readv_writev_v2(cmount, &io_info.base_info, sizeof(ceph_ll_io_info) - 1);
  ASSERT_EQ(res, -EINVAL);
}