// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 IBM Corporation
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include <errno.h>

#include <cstring>
#include <vector>

#include "client/ZeroCopyClient.h"
#include "gtest/gtest.h"

TEST(ZeroCopyClientBuffer, AllocBuffers)
{
  auto bl = ceph::ZeroCopyClient::alloc_buffers(0);
  EXPECT_EQ(0u, bl.length());
  EXPECT_EQ(0u, bl.get_num_buffers());

  constexpr size_t len = 8192;
  bl = ceph::ZeroCopyClient::alloc_buffers(len);
  ASSERT_EQ(len, bl.length());
  ASSERT_GE(bl.get_num_buffers(), 1u);

  // Writable Ceph-owned pages.
  char* p = bl.c_str();
  ASSERT_NE(nullptr, p);
  std::memset(p, 0xab, len);
  EXPECT_EQ(static_cast<char>(0xab), p[0]);
  EXPECT_EQ(static_cast<char>(0xab), p[len - 1]);
}

TEST(ZeroCopyClientBuffer, WrapStaticSingle)
{
  char buf[64];
  std::memset(buf, 'x', sizeof(buf));

  auto bl = ceph::ZeroCopyClient::wrap_static(buf, sizeof(buf));
  ASSERT_EQ(sizeof(buf), bl.length());
  ASSERT_EQ(1u, bl.get_num_buffers());
  // Same underlying memory (no copy).
  EXPECT_EQ(buf, bl.buffers().front().c_str());

  auto empty = ceph::ZeroCopyClient::wrap_static(nullptr, 0);
  EXPECT_EQ(0u, empty.length());
}

TEST(ZeroCopyClientBuffer, WrapStaticIovec)
{
  char a[16], b[32];
  std::memset(a, 'a', sizeof(a));
  std::memset(b, 'b', sizeof(b));
  struct iovec iov[2] = {
      {a, sizeof(a)},
      {b, sizeof(b)},
  };

  auto bl = ceph::ZeroCopyClient::wrap_static(iov, 2);
  ASSERT_EQ(sizeof(a) + sizeof(b), bl.length());
  ASSERT_EQ(2u, bl.get_num_buffers());

  auto it = bl.buffers().begin();
  EXPECT_EQ(a, it->c_str());
  ++it;
  EXPECT_EQ(b, it->c_str());
}

TEST(ZeroCopyClientBuffer, BufferlistToIovec)
{
  char a[8], b[8];
  std::memset(a, 1, sizeof(a));
  std::memset(b, 2, sizeof(b));
  struct iovec in[2] = {{a, sizeof(a)}, {b, sizeof(b)}};
  auto bl = ceph::ZeroCopyClient::wrap_static(in, 2);

  struct iovec out[4];
  int n = ceph::ZeroCopyClient::bufferlist_to_iovec(bl, out, 4);
  ASSERT_EQ(2, n);
  EXPECT_EQ(a, out[0].iov_base);
  EXPECT_EQ(sizeof(a), out[0].iov_len);
  EXPECT_EQ(b, out[1].iov_base);
  EXPECT_EQ(sizeof(b), out[1].iov_len);

  EXPECT_EQ(-ERANGE, ceph::ZeroCopyClient::bufferlist_to_iovec(bl, out, 1));

  ceph::bufferlist empty;
  EXPECT_EQ(0, ceph::ZeroCopyClient::bufferlist_to_iovec(empty, out, 4));
}

TEST(ZeroCopyClientBuffer, AsyncRequiresClient)
{
  ceph::ZeroCopyClient zc(nullptr);
  EXPECT_EQ(-EINVAL, zc.async_read(nullptr, 0, 0, [](ceph::IoResult&&) {}));
  EXPECT_EQ(-EINVAL, zc.async_write(
                         nullptr, 0, ceph::bufferlist{},
                         [](ceph::IoResult&&) {}, false, false));
  // empty write payload rejected before client check
  EXPECT_EQ(-EINVAL, zc.write(nullptr, 0, ceph::bufferlist{}, false));
  EXPECT_EQ(-EINVAL, zc.read(nullptr, 0, 0, nullptr));
}
