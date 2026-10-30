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
#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <string>

#include "gtest/gtest.h"
#include "include/cephfs/libcephfs.h"

namespace {

struct ZcWait {
  std::mutex mtx;
  std::condition_variable cv;
  bool done = false;
  int64_t result = 0;
  ceph_bufferlist* data = nullptr;

  void reset()
  {
    done = false;
    result = 0;
    data = nullptr;
  }

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

} // namespace

TEST(LibCephFS, ZeroCopyAsyncRoundTrip)
{
  struct ceph_mount_info* cmount = nullptr;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  UserPerm* perms = ceph_mount_perms(cmount);
  Inode* root = nullptr;
  ASSERT_EQ(0, ceph_ll_lookup_root(cmount, &root));

  char filename[256];
  sprintf(filename, "test_libcephfs_zc_async_%u", getpid());

  Inode* file = nullptr;
  Fh* fh = nullptr;
  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_ll_create(cmount, root, filename, 0666,
                              O_RDWR | O_CREAT | O_TRUNC, &file, &fh, &stx, 0,
                              0, perms));

  constexpr size_t len = 4096;
  ceph_bufferlist* wbl = ceph_bufferlist_alloc(len);
  ASSERT_NE(nullptr, wbl);
  ASSERT_EQ(len, ceph_bufferlist_length(wbl));

  struct iovec wiov[1];
  ASSERT_EQ(1, ceph_bufferlist_to_iovec(wbl, wiov, 1));
  std::memset(wiov[0].iov_base, 0x3c, len);
  std::string expected(static_cast<char*>(wiov[0].iov_base), len);

  ZcWait wwait;
  struct ceph_io_completion wcomp{zc_io_cb, &wwait};
  ASSERT_EQ(0, ceph_ll_async_write(cmount, fh, 0, wbl, 0, &wcomp));
  // handle consumed at submit
  ASSERT_EQ(static_cast<int64_t>(len), wwait.wait());
  EXPECT_EQ(nullptr, wwait.data);

  ZcWait rwait;
  struct ceph_io_completion rcomp{zc_io_cb, &rwait};
  ASSERT_EQ(0, ceph_ll_async_read(cmount, fh, 0, len, &rcomp));
  ASSERT_EQ(static_cast<int64_t>(len), rwait.wait());
  ASSERT_NE(nullptr, rwait.data);
  ASSERT_EQ(len, ceph_bufferlist_length(rwait.data));

  struct iovec riov[8];
  int niov = ceph_bufferlist_to_iovec(rwait.data, riov, 8);
  ASSERT_GT(niov, 0);
  size_t pos = 0;
  for (int i = 0; i < niov; ++i) {
    ASSERT_LE(pos + riov[i].iov_len, len);
    EXPECT_EQ(0, std::memcmp(expected.data() + pos, riov[i].iov_base,
                             riov[i].iov_len));
    pos += riov[i].iov_len;
  }
  EXPECT_EQ(len, pos);
  ceph_bufferlist_put(rwait.data);

  ASSERT_EQ(0, ceph_ll_close(cmount, fh));
  ASSERT_EQ(0, ceph_ll_unlink(cmount, root, filename, perms));
  ASSERT_EQ(0, ceph_unmount(cmount));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, ZeroCopyUpperLayerAllocWrite)
{
  // Primary upper-layer WRITE path: Ceph allocates pages, caller fills via
  // to_iovec, async_write consumes — no wrap/claim.
  struct ceph_mount_info* cmount = nullptr;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  UserPerm* perms = ceph_mount_perms(cmount);
  Inode* root = nullptr;
  ASSERT_EQ(0, ceph_ll_lookup_root(cmount, &root));

  char filename[256];
  sprintf(filename, "test_libcephfs_zc_alloc_%u", getpid());

  Inode* file = nullptr;
  Fh* fh = nullptr;
  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_ll_create(cmount, root, filename, 0666,
                              O_RDWR | O_CREAT | O_TRUNC, &file, &fh, &stx, 0,
                              0, perms));

  constexpr size_t len = 8192;
  ceph_bufferlist* wbl = ceph_bufferlist_alloc(len);
  ASSERT_NE(nullptr, wbl);
  ASSERT_EQ(len, ceph_bufferlist_length(wbl));

  struct iovec wiov[1];
  ASSERT_EQ(1, ceph_bufferlist_to_iovec(wbl, wiov, 1));
  ASSERT_EQ(len, wiov[0].iov_len);
  // Page-aligned base for upper-layer readv.
  EXPECT_EQ(0u, reinterpret_cast<uintptr_t>(wiov[0].iov_base) &
                    (static_cast<uintptr_t>(getpagesize()) - 1));
  std::memset(wiov[0].iov_base, 0xa5, len);
  std::string expected(static_cast<char*>(wiov[0].iov_base), len);

  ZcWait wwait;
  struct ceph_io_completion wcomp{zc_io_cb, &wwait};
  ASSERT_EQ(0, ceph_ll_async_write(cmount, fh, 0, wbl, 0, &wcomp));
  ASSERT_EQ(static_cast<int64_t>(len), wwait.wait());
  EXPECT_EQ(nullptr, wwait.data);

  ZcWait rwait;
  struct ceph_io_completion rcomp{zc_io_cb, &rwait};
  ASSERT_EQ(0, ceph_ll_async_read(cmount, fh, 0, len, &rcomp));
  ASSERT_EQ(static_cast<int64_t>(len), rwait.wait());
  ASSERT_NE(nullptr, rwait.data);

  struct iovec riov[8];
  int niov = ceph_bufferlist_to_iovec(rwait.data, riov, 8);
  ASSERT_GT(niov, 0);
  size_t pos = 0;
  for (int i = 0; i < niov; ++i) {
    ASSERT_LE(pos + riov[i].iov_len, len);
    EXPECT_EQ(0, std::memcmp(expected.data() + pos, riov[i].iov_base,
                             riov[i].iov_len));
    pos += riov[i].iov_len;
  }
  EXPECT_EQ(len, pos);
  ceph_bufferlist_put(rwait.data);

  ASSERT_EQ(0, ceph_ll_close(cmount, fh));
  ASSERT_EQ(0, ceph_ll_unlink(cmount, root, filename, perms));
  ASSERT_EQ(0, ceph_unmount(cmount));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, ZeroCopyWrapIovWrite)
{
  struct ceph_mount_info* cmount = nullptr;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  UserPerm* perms = ceph_mount_perms(cmount);
  Inode* root = nullptr;
  ASSERT_EQ(0, ceph_ll_lookup_root(cmount, &root));

  char filename[256];
  sprintf(filename, "test_libcephfs_zc_wrap_%u", getpid());

  Inode* file = nullptr;
  Fh* fh = nullptr;
  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_ll_create(cmount, root, filename, 0666,
                              O_RDWR | O_CREAT | O_TRUNC, &file, &fh, &stx, 0,
                              0, perms));

  char a[16], b[32];
  std::memset(a, 'A', sizeof(a));
  std::memset(b, 'B', sizeof(b));
  struct iovec iov[2] = {{a, sizeof(a)}, {b, sizeof(b)}};
  const size_t total = sizeof(a) + sizeof(b);

  ceph_bufferlist* wbl = ceph_bufferlist_wrap_iov(iov, 2);
  ASSERT_NE(nullptr, wbl);
  ASSERT_EQ(total, ceph_bufferlist_length(wbl));

  ZcWait wwait;
  struct ceph_io_completion wcomp{zc_io_cb, &wwait};
  ASSERT_EQ(0, ceph_ll_async_write(cmount, fh, 0, wbl, CEPH_LL_WRITE_FSYNC,
                                   &wcomp));
  ASSERT_EQ(static_cast<int64_t>(total), wwait.wait());

  ZcWait rwait;
  struct ceph_io_completion rcomp{zc_io_cb, &rwait};
  ASSERT_EQ(0, ceph_ll_async_read(cmount, fh, 0, total, &rcomp));
  ASSERT_EQ(static_cast<int64_t>(total), rwait.wait());
  ASSERT_NE(nullptr, rwait.data);

  struct iovec out[4];
  int n = ceph_bufferlist_to_iovec(rwait.data, out, 4);
  ASSERT_GT(n, 0);
  // Reconstruct contiguous compare against a||b
  std::string got;
  for (int i = 0; i < n; ++i) {
    got.append(static_cast<char*>(out[i].iov_base), out[i].iov_len);
  }
  EXPECT_EQ(total, got.size());
  EXPECT_EQ(0, std::memcmp(a, got.data(), sizeof(a)));
  EXPECT_EQ(0, std::memcmp(b, got.data() + sizeof(a), sizeof(b)));
  ceph_bufferlist_put(rwait.data);

  ASSERT_EQ(0, ceph_ll_close(cmount, fh));
  ASSERT_EQ(0, ceph_ll_unlink(cmount, root, filename, perms));
  ASSERT_EQ(0, ceph_unmount(cmount));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, ZeroCopyReadEmptyFile)
{
  struct ceph_mount_info* cmount = nullptr;
  ASSERT_EQ(0, ceph_create(&cmount, NULL));
  ASSERT_EQ(0, ceph_conf_read_file(cmount, NULL));
  ASSERT_EQ(0, ceph_conf_parse_env(cmount, NULL));
  ASSERT_EQ(0, ceph_mount(cmount, "/"));

  UserPerm* perms = ceph_mount_perms(cmount);
  Inode* root = nullptr;
  ASSERT_EQ(0, ceph_ll_lookup_root(cmount, &root));

  char filename[256];
  sprintf(filename, "test_libcephfs_zc_empty_%u", getpid());

  Inode* file = nullptr;
  Fh* fh = nullptr;
  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_ll_create(cmount, root, filename, 0666,
                              O_RDWR | O_CREAT | O_TRUNC, &file, &fh, &stx, 0,
                              0, perms));

  ZcWait rwait;
  struct ceph_io_completion rcomp{zc_io_cb, &rwait};
  ASSERT_EQ(0, ceph_ll_async_read(cmount, fh, 0, 1024, &rcomp));
  ASSERT_EQ(0, rwait.wait());
  // Empty successful read may pass nullptr or empty handle.
  if (rwait.data) {
    EXPECT_EQ(0u, ceph_bufferlist_length(rwait.data));
    ceph_bufferlist_put(rwait.data);
  }

  ASSERT_EQ(0, ceph_ll_close(cmount, fh));
  ASSERT_EQ(0, ceph_ll_unlink(cmount, root, filename, perms));
  ASSERT_EQ(0, ceph_unmount(cmount));
  ceph_shutdown(cmount);
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

TEST(LibCephFS, ZeroCopyClaimIov)
{
  ClaimStats st;
  constexpr size_t len = 64;
  char* buf = static_cast<char*>(std::malloc(len));
  ASSERT_NE(nullptr, buf);
  std::memset(buf, 0x5a, len);
  struct iovec iov = {buf, len};

  ceph_bufferlist* bl = ceph_bufferlist_claim_iov(&iov, 1, claim_count_deleter,
                                                  &st);
  ASSERT_NE(nullptr, bl);
  EXPECT_EQ(len, ceph_bufferlist_length(bl));
  EXPECT_EQ(0, st.calls.load());

  struct iovec out;
  ASSERT_EQ(1, ceph_bufferlist_to_iovec(bl, &out, 1));
  EXPECT_EQ(buf, out.iov_base);
  EXPECT_EQ(len, out.iov_len);

  ceph_bufferlist_put(bl);
  EXPECT_EQ(1, st.calls.load());
  EXPECT_EQ(len, st.bytes.load());
}

namespace {

int mount_with_client_oc(struct ceph_mount_info** cmount, bool client_oc)
{
  int r = ceph_create(cmount, NULL);
  if (r < 0) {
    return r;
  }
  r = ceph_conf_read_file(*cmount, NULL);
  if (r < 0) {
    return r;
  }
  r = ceph_conf_parse_env(*cmount, NULL);
  if (r < 0) {
    return r;
  }
  r = ceph_conf_set(*cmount, "client_oc", client_oc ? "true" : "false");
  if (r < 0) {
    return r;
  }
  return ceph_mount(*cmount, "/");
}

void zc_expect_payload(ceph_bufferlist* rbl, const std::string& expected)
{
  ASSERT_NE(nullptr, rbl);
  ASSERT_EQ(expected.size(), ceph_bufferlist_length(rbl));
  struct iovec riov[16];
  int niov = ceph_bufferlist_to_iovec(rbl, riov, 16);
  ASSERT_GT(niov, 0);
  size_t pos = 0;
  for (int i = 0; i < niov; ++i) {
    ASSERT_LE(pos + riov[i].iov_len, expected.size());
    EXPECT_EQ(0, std::memcmp(expected.data() + pos, riov[i].iov_base,
                             riov[i].iov_len));
    pos += riov[i].iov_len;
  }
  EXPECT_EQ(expected.size(), pos);
}

} // namespace

TEST(LibCephFS, ZeroCopyObjectCacherRoundTrip)
{
  // alloc → async_write → async_read with ObjectCacher enabled.
  struct ceph_mount_info* cmount = nullptr;
  ASSERT_EQ(0, mount_with_client_oc(&cmount, true));

  UserPerm* perms = ceph_mount_perms(cmount);
  Inode* root = nullptr;
  ASSERT_EQ(0, ceph_ll_lookup_root(cmount, &root));

  char filename[256];
  sprintf(filename, "test_libcephfs_zc_oc_%u", getpid());

  Inode* file = nullptr;
  Fh* fh = nullptr;
  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_ll_create(cmount, root, filename, 0666,
                              O_RDWR | O_CREAT | O_TRUNC, &file, &fh, &stx, 0,
                              0, perms));

  constexpr size_t len = 8192;
  ceph_bufferlist* wbl = ceph_bufferlist_alloc(len);
  ASSERT_NE(nullptr, wbl);
  struct iovec wiov[1];
  ASSERT_EQ(1, ceph_bufferlist_to_iovec(wbl, wiov, 1));
  std::memset(wiov[0].iov_base, 0xc3, len);
  std::string expected(static_cast<char*>(wiov[0].iov_base), len);

  ZcWait wwait;
  struct ceph_io_completion wcomp{zc_io_cb, &wwait};
  ASSERT_EQ(0, ceph_ll_async_write(cmount, fh, 0, wbl, 0, &wcomp));
  ASSERT_EQ(static_cast<int64_t>(len), wwait.wait());

  ZcWait rwait;
  struct ceph_io_completion rcomp{zc_io_cb, &rwait};
  ASSERT_EQ(0, ceph_ll_async_read(cmount, fh, 0, len, &rcomp));
  ASSERT_EQ(static_cast<int64_t>(len), rwait.wait());
  zc_expect_payload(rwait.data, expected);
  ceph_bufferlist_put(rwait.data);

  // Classic path must see the same bytes (ZC updated OC, not Filer-only).
  char verify[len];
  ASSERT_EQ(static_cast<int>(len), ceph_ll_read(cmount, fh, 0, len, verify));
  EXPECT_EQ(0, std::memcmp(expected.data(), verify, len));

  ASSERT_EQ(0, ceph_ll_close(cmount, fh));
  ASSERT_EQ(0, ceph_ll_unlink(cmount, root, filename, perms));
  ASSERT_EQ(0, ceph_unmount(cmount));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, ZeroCopyObjectCacherHitPath)
{
  // Write once, read twice: second read should be served from ObjectCacher.
  struct ceph_mount_info* cmount = nullptr;
  ASSERT_EQ(0, mount_with_client_oc(&cmount, true));

  UserPerm* perms = ceph_mount_perms(cmount);
  Inode* root = nullptr;
  ASSERT_EQ(0, ceph_ll_lookup_root(cmount, &root));

  char filename[256];
  sprintf(filename, "test_libcephfs_zc_oc_hit_%u", getpid());

  Inode* file = nullptr;
  Fh* fh = nullptr;
  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_ll_create(cmount, root, filename, 0666,
                              O_RDWR | O_CREAT | O_TRUNC, &file, &fh, &stx, 0,
                              0, perms));

  constexpr size_t len = 4096;
  ceph_bufferlist* wbl = ceph_bufferlist_alloc(len);
  ASSERT_NE(nullptr, wbl);
  struct iovec wiov[1];
  ASSERT_EQ(1, ceph_bufferlist_to_iovec(wbl, wiov, 1));
  std::memset(wiov[0].iov_base, 0x4d, len);
  std::string expected(static_cast<char*>(wiov[0].iov_base), len);

  ZcWait wwait;
  struct ceph_io_completion wcomp{zc_io_cb, &wwait};
  ASSERT_EQ(0, ceph_ll_async_write(cmount, fh, 0, wbl, 0, &wcomp));
  ASSERT_EQ(static_cast<int64_t>(len), wwait.wait());

  for (int pass = 0; pass < 2; ++pass) {
    ZcWait rwait;
    struct ceph_io_completion rcomp{zc_io_cb, &rwait};
    ASSERT_EQ(0, ceph_ll_async_read(cmount, fh, 0, len, &rcomp));
    ASSERT_EQ(static_cast<int64_t>(len), rwait.wait());
    zc_expect_payload(rwait.data, expected);
    ceph_bufferlist_put(rwait.data);
  }

  ASSERT_EQ(0, ceph_ll_close(cmount, fh));
  ASSERT_EQ(0, ceph_ll_unlink(cmount, root, filename, perms));
  ASSERT_EQ(0, ceph_unmount(cmount));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, ZeroCopyObjectCacherWrapStaysFiler)
{
  // wrap_iov is Borrowed: with client_oc still Filer-only, must not UAF.
  struct ceph_mount_info* cmount = nullptr;
  ASSERT_EQ(0, mount_with_client_oc(&cmount, true));

  UserPerm* perms = ceph_mount_perms(cmount);
  Inode* root = nullptr;
  ASSERT_EQ(0, ceph_ll_lookup_root(cmount, &root));

  char filename[256];
  sprintf(filename, "test_libcephfs_zc_oc_wrap_%u", getpid());

  Inode* file = nullptr;
  Fh* fh = nullptr;
  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_ll_create(cmount, root, filename, 0666,
                              O_RDWR | O_CREAT | O_TRUNC, &file, &fh, &stx, 0,
                              0, perms));

  char payload[128];
  std::memset(payload, 'W', sizeof(payload));
  struct iovec iov = {payload, sizeof(payload)};
  ceph_bufferlist* wbl = ceph_bufferlist_wrap_iov(&iov, 1);
  ASSERT_NE(nullptr, wbl);

  ZcWait wwait;
  struct ceph_io_completion wcomp{zc_io_cb, &wwait};
  ASSERT_EQ(0, ceph_ll_async_write(cmount, fh, 0, wbl, 0, &wcomp));
  ASSERT_EQ(static_cast<int64_t>(sizeof(payload)), wwait.wait());

  ZcWait rwait;
  struct ceph_io_completion rcomp{zc_io_cb, &rwait};
  ASSERT_EQ(0, ceph_ll_async_read(cmount, fh, 0, sizeof(payload), &rcomp));
  ASSERT_EQ(static_cast<int64_t>(sizeof(payload)), rwait.wait());
  zc_expect_payload(rwait.data, std::string(payload, sizeof(payload)));
  ceph_bufferlist_put(rwait.data);

  ASSERT_EQ(0, ceph_ll_close(cmount, fh));
  ASSERT_EQ(0, ceph_ll_unlink(cmount, root, filename, perms));
  ASSERT_EQ(0, ceph_unmount(cmount));
  ceph_shutdown(cmount);
}

TEST(LibCephFS, ZeroCopyObjectCacherUnmountAfterInFlight)
{
  // Several OC ZC writes must complete (Fh refs dropped) before unmount.
  // Submit/wait sequentially so this covers lifetime without stressing
  // concurrent ObjectCacher finisher completions.
  struct ceph_mount_info* cmount = nullptr;
  ASSERT_EQ(0, mount_with_client_oc(&cmount, true));

  UserPerm* perms = ceph_mount_perms(cmount);
  Inode* root = nullptr;
  ASSERT_EQ(0, ceph_ll_lookup_root(cmount, &root));

  char filename[256];
  sprintf(filename, "test_libcephfs_zc_oc_inflight_%u", getpid());

  Inode* file = nullptr;
  Fh* fh = nullptr;
  struct ceph_statx stx;
  ASSERT_EQ(0, ceph_ll_create(cmount, root, filename, 0666,
                              O_RDWR | O_CREAT | O_TRUNC, &file, &fh, &stx, 0,
                              0, perms));

  constexpr int nops = 8;
  constexpr size_t len = 1024;
  for (int i = 0; i < nops; ++i) {
    ceph_bufferlist* wbl = ceph_bufferlist_alloc(len);
    ASSERT_NE(nullptr, wbl);
    struct iovec wiov[1];
    ASSERT_EQ(1, ceph_bufferlist_to_iovec(wbl, wiov, 1));
    std::memset(wiov[0].iov_base, static_cast<char>(0x10 + i), len);
    ZcWait wwait;
    struct ceph_io_completion wcomp{zc_io_cb, &wwait};
    ASSERT_EQ(0, ceph_ll_async_write(cmount, fh, static_cast<uint64_t>(i) * len,
                                     wbl, 0, &wcomp));
    ASSERT_EQ(static_cast<int64_t>(len), wwait.wait());
  }

  ASSERT_EQ(0, ceph_ll_close(cmount, fh));
  ASSERT_EQ(0, ceph_ll_unlink(cmount, root, filename, perms));
  ASSERT_EQ(0, ceph_unmount(cmount));
  ceph_shutdown(cmount);
}
