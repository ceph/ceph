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

#include <condition_variable>
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
  ASSERT_EQ(0, ceph_ll_async_write(cmount, fh, 0, wbl, false, &wcomp));
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
  ASSERT_EQ(0,
            ceph_ll_async_write(cmount, fh, 0, wbl, true /* fsync */, &wcomp));
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
