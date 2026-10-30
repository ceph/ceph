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

#include <cstring>
#include <string>
#include <vector>

#include "client/ZeroCopyClient.h"
#include "common/Cond.h"
#include "test/client/TestClient.h"

namespace {

Fh* create_zc_file(ClientScaffold* client, const UserPerm& myperm,
                   const char* filename)
{
  Inode* root = client->get_root();
  EXPECT_NE(nullptr, root);
  if (!root) {
    return nullptr;
  }

  Inode* file = nullptr;
  Fh* fh = nullptr;
  struct ceph_statx stx;
  int r = client->ll_createx(root, filename, 0666, O_RDWR | O_CREAT | O_TRUNC,
                             &file, &fh, &stx, 0, 0, myperm);
  EXPECT_EQ(0, r);
  return fh;
}

} // namespace

TEST_F(TestClient, ZeroCopySyncRoundTrip)
{
  client->unmount();
  TearDown();
  SetUp();

  char filename[256];
  sprintf(filename, "test_zc_sync_rt_%u", getpid());
  Fh* fh = create_zc_file(client, myperm, filename);
  ASSERT_NE(nullptr, fh);

  ceph::ZeroCopyClient zc(client);
  constexpr size_t len = 4096;
  auto bl = ceph::ZeroCopyClient::alloc_buffers(len);
  ASSERT_EQ(len, bl.length());
  std::memset(bl.c_str(), 0x5a, len);

  std::string expected(bl.c_str(), len);
  ASSERT_EQ(static_cast<int64_t>(len), zc.write(fh, 0, std::move(bl), false));

  ceph::bufferlist out;
  ASSERT_EQ(static_cast<int64_t>(len), zc.read(fh, 0, len, &out));
  ASSERT_EQ(len, out.length());
  EXPECT_EQ(0, std::memcmp(expected.data(), out.c_str(), len));

  ASSERT_EQ(0, client->ll_release(fh));
  ASSERT_EQ(0, client->ll_unlink(client->get_root(), filename, myperm));
}

TEST_F(TestClient, ZeroCopyAsyncRoundTrip)
{
  client->unmount();
  TearDown();
  SetUp();

  char filename[256];
  sprintf(filename, "test_zc_async_rt_%u", getpid());
  Fh* fh = create_zc_file(client, myperm, filename);
  ASSERT_NE(nullptr, fh);

  ceph::ZeroCopyClient zc(client);
  constexpr size_t len = 8192;
  auto bl = ceph::ZeroCopyClient::alloc_buffers(len);
  std::memset(bl.c_str(), 0xa5, len);
  std::string expected(bl.c_str(), len);

  C_SaferCond wcond("zc-async-write");
  ASSERT_EQ(0, zc.async_write(
                   fh, 0, std::move(bl), ceph::BufferOwnership::Allocated,
                   [&wcond](ceph::IoResult&& r) { wcond.complete(r.result); }));
  ASSERT_EQ(static_cast<int64_t>(len), wcond.wait());

  C_SaferCond rcond("zc-async-read");
  ceph::bufferlist out;
  ASSERT_EQ(0, zc.async_read(fh, 0, len, [&rcond, &out](ceph::IoResult&& r) {
    if (r.result >= 0) {
      out = std::move(r.data);
    }
    rcond.complete(r.result);
  }));
  ASSERT_EQ(static_cast<int64_t>(len), rcond.wait());
  ASSERT_EQ(len, out.length());
  EXPECT_EQ(0, std::memcmp(expected.data(), out.c_str(), len));

  // Export segments without copying.
  struct iovec iov[8];
  int niov = ceph::ZeroCopyClient::bufferlist_to_iovec(out, iov, 8);
  ASSERT_GT(niov, 0);
  size_t viewed = 0;
  for (int i = 0; i < niov; ++i) {
    viewed += iov[i].iov_len;
  }
  EXPECT_EQ(len, viewed);

  ASSERT_EQ(0, client->ll_release(fh));
  ASSERT_EQ(0, client->ll_unlink(client->get_root(), filename, myperm));
}

TEST_F(TestClient, ZeroCopyWrapStaticWrite)
{
  client->unmount();
  TearDown();
  SetUp();

  char filename[256];
  sprintf(filename, "test_zc_wrap_%u", getpid());
  Fh* fh = create_zc_file(client, myperm, filename);
  ASSERT_NE(nullptr, fh);

  ceph::ZeroCopyClient zc(client);
  char payload[] = "wrap-static-zero-copy\n";
  auto bl = ceph::ZeroCopyClient::wrap_static(payload, sizeof(payload));
  ASSERT_EQ(sizeof(payload), bl.length());

  C_SaferCond wcond("zc-wrap-write");
  ASSERT_EQ(0, zc.async_write(
                   fh, 0, std::move(bl), ceph::BufferOwnership::Borrowed,
                   [&wcond](ceph::IoResult&& r) { wcond.complete(r.result); }));
  ASSERT_EQ(static_cast<int64_t>(sizeof(payload)), wcond.wait());

  ceph::bufferlist out;
  ASSERT_EQ(static_cast<int64_t>(sizeof(payload)),
            zc.read(fh, 0, sizeof(payload), &out));
  EXPECT_EQ(0, std::memcmp(payload, out.c_str(), sizeof(payload)));

  ASSERT_EQ(0, client->ll_release(fh));
  ASSERT_EQ(0, client->ll_unlink(client->get_root(), filename, myperm));
}

TEST_F(TestClient, ZeroCopyWriteFsync)
{
  client->unmount();
  TearDown();
  SetUp();

  char filename[256];
  sprintf(filename, "test_zc_fsync_%u", getpid());
  Fh* fh = create_zc_file(client, myperm, filename);
  ASSERT_NE(nullptr, fh);

  ceph::ZeroCopyClient zc(client);
  constexpr size_t len = 1024;
  auto bl = ceph::ZeroCopyClient::alloc_buffers(len);
  std::memset(bl.c_str(), 0x11, len);

  C_SaferCond cond("zc-fsync-write");
  ASSERT_EQ(0, zc.async_write(
                   fh, 0, std::move(bl), ceph::BufferOwnership::Allocated,
                   [&cond](ceph::IoResult&& r) { cond.complete(r.result); },
                   true /* fsync */));
  ASSERT_EQ(static_cast<int64_t>(len), cond.wait());

  ceph::bufferlist out;
  ASSERT_EQ(static_cast<int64_t>(len), zc.read(fh, 0, len, &out));
  EXPECT_EQ(len, out.length());

  ASSERT_EQ(0, client->ll_release(fh));
  ASSERT_EQ(0, client->ll_unlink(client->get_root(), filename, myperm));
}

TEST_F(TestClient, ZeroCopyReadPastEOF)
{
  client->unmount();
  TearDown();
  SetUp();

  char filename[256];
  sprintf(filename, "test_zc_eof_%u", getpid());
  Fh* fh = create_zc_file(client, myperm, filename);
  ASSERT_NE(nullptr, fh);

  ceph::ZeroCopyClient zc(client);
  constexpr size_t len = 512;
  auto bl = ceph::ZeroCopyClient::alloc_buffers(len);
  std::memset(bl.c_str(), 'e', len);
  ASSERT_EQ(static_cast<int64_t>(len), zc.write(fh, 0, std::move(bl), false));

  ceph::bufferlist out;
  // Request more than file size; should return only existing bytes.
  ASSERT_EQ(static_cast<int64_t>(len), zc.read(fh, 0, len * 4, &out));
  EXPECT_EQ(len, out.length());

  out.clear();
  ASSERT_EQ(0, zc.read(fh, len, 64, &out));
  EXPECT_EQ(0u, out.length());

  ASSERT_EQ(0, client->ll_release(fh));
  ASSERT_EQ(0, client->ll_unlink(client->get_root(), filename, myperm));
}

TEST_F(TestClient, ZeroCopyOffsetRoundTrip)
{
  client->unmount();
  TearDown();
  SetUp();

  char filename[256];
  sprintf(filename, "test_zc_off_%u", getpid());
  Fh* fh = create_zc_file(client, myperm, filename);
  ASSERT_NE(nullptr, fh);

  ceph::ZeroCopyClient zc(client);
  const char* a = "AAAAAAAA";
  const char* b = "BBBBBBBB";
  auto bla = ceph::ZeroCopyClient::wrap_static(const_cast<char*>(a), 8);
  auto blb = ceph::ZeroCopyClient::wrap_static(const_cast<char*>(b), 8);

  ASSERT_EQ(8, zc.write(fh, 0, std::move(bla), false));
  ASSERT_EQ(8, zc.write(fh, 4096, std::move(blb), false));

  ceph::bufferlist outa, outb;
  ASSERT_EQ(8, zc.read(fh, 0, 8, &outa));
  ASSERT_EQ(8, zc.read(fh, 4096, 8, &outb));
  EXPECT_EQ(0, std::memcmp(a, outa.c_str(), 8));
  EXPECT_EQ(0, std::memcmp(b, outb.c_str(), 8));

  ASSERT_EQ(0, client->ll_release(fh));
  ASSERT_EQ(0, client->ll_unlink(client->get_root(), filename, myperm));
}
