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
TEST(libcephfs, zerocopy_sanity_invalid_params) {
  struct ceph_mount_info *mount_info = nullptr;
  auto res = ceph_create(&mount_info, "test_mount");
  ASSERT_EQ(0, res);
  struct ceph_ll_io_info io_info;
  struct iovec iov;
  io_info.iov = &iov;
  io_info.iovcnt = 0;
  io_info.write = false;
  io_info.zerocopy = true;

  res = ceph_ll_readv_writev(mount_info, &io_info);
  ASSERT_EQ(res, -EINVAL);

  res = ceph_ll_nonblocking_readv_writev(mount_info, &io_info);
  ASSERT_EQ(res, -EINVAL);
}