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

#include <atomic>
#include <cstdlib>
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

namespace {

struct ClaimStats {
  std::atomic<int> calls{0};
  std::atomic<size_t> bytes{0};
};

void claim_count_deleter(void* buf, size_t len, void* priv)
{
  auto* st = static_cast<ClaimStats*>(priv);
  st->calls.fetch_add(1, std::memory_order_relaxed);
  st->bytes.fetch_add(len, std::memory_order_relaxed);
  std::free(buf);
}

} // namespace

TEST(ZeroCopyClientBuffer, ClaimSingleFree)
{
  ClaimStats st;
  constexpr size_t len = 128;
  char* buf = static_cast<char*>(std::malloc(len));
  ASSERT_NE(nullptr, buf);
  std::memset(buf, 0xcd, len);

  {
    auto bl = ceph::ZeroCopyClient::claim(buf, len, claim_count_deleter, &st);
    ASSERT_EQ(len, bl.length());
    ASSERT_EQ(1u, bl.get_num_buffers());
    EXPECT_EQ(buf, bl.buffers().front().c_str());
    EXPECT_EQ(0, st.calls.load());
  }
  EXPECT_EQ(1, st.calls.load());
  EXPECT_EQ(len, st.bytes.load());
}

TEST(ZeroCopyClientBuffer, ClaimIovecCustomDeleter)
{
  ClaimStats st;
  constexpr size_t la = 64;
  constexpr size_t lb = 96;
  char* a = static_cast<char*>(std::malloc(la));
  char* b = static_cast<char*>(std::malloc(lb));
  ASSERT_NE(nullptr, a);
  ASSERT_NE(nullptr, b);

  struct iovec iov[2] = {{a, la}, {b, lb}};

  {
    auto bl = ceph::ZeroCopyClient::claim_iov(iov, 2, claim_count_deleter, &st);
    ASSERT_EQ(la + lb, bl.length());
    ASSERT_EQ(2u, bl.get_num_buffers());
    auto it = bl.buffers().begin();
    EXPECT_EQ(a, it->c_str());
    ++it;
    EXPECT_EQ(b, it->c_str());
    EXPECT_EQ(0, st.calls.load());
  }

  EXPECT_EQ(2, st.calls.load());
  EXPECT_EQ(la + lb, st.bytes.load());
}

TEST(ZeroCopyClientBuffer, ClaimIovecNullDeleterUsesFree)
{
  // nullptr deleter → free(3); just ensure no crash / length correct.
  constexpr size_t len = 32;
  char* buf = static_cast<char*>(std::malloc(len));
  ASSERT_NE(nullptr, buf);
  struct iovec iov = {buf, len};
  auto bl = ceph::ZeroCopyClient::claim_iov(&iov, 1, nullptr, nullptr);
  ASSERT_EQ(len, bl.length());
  bl.clear();
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
                         ceph::BufferOwnership::Allocated,
                         [](ceph::IoResult&&) {}, false, false));
  // empty write payload rejected before client check
  EXPECT_EQ(-EINVAL, zc.write(nullptr, 0, ceph::bufferlist{}, false));
  EXPECT_EQ(-EINVAL, zc.read(nullptr, 0, 0, nullptr));
}
