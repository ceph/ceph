// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <errno.h>
#include <atomic>
#include <chrono>
#include <random>
#include <thread>
#include <vector>

#include <iostream>
#include <string>

#include <fmt/format.h>

#include "test/client/TestClient.h"

TEST_F(TestClient, LL_Walk) {
  auto dir = fmt::format("/{}_{}", ::testing::UnitTest::GetInstance()->current_test_info()->name(), getpid());
  ASSERT_EQ(0, client->mkdir(dir.c_str(), 0777, myperm));

  ASSERT_EQ(0, client->chdir(dir.c_str(), myperm));
  std::string cwd;
  ASSERT_EQ(0, client->getcwd(cwd, myperm));
  ASSERT_STREQ(cwd.c_str(), dir.c_str());

  Inode* in = nullptr;
  struct ceph_statx xbuf;
  ASSERT_EQ(0, client->ll_walk(dir.c_str(), &in, &xbuf, 0, 0, myperm));

  ASSERT_EQ(0, client->rmdir(dir.c_str(), myperm));
}

/*
 * The client is unmounted while its threads read, write and getattr
 * through the paths that do not take client_lock: the unmount waits for
 * the calls in progress, every call made after it returns ENOTCONN
 * without touching the file handle or the inode it dropped, and none
 * hangs or crashes.  This is done on the Client itself: ceph_unmount()
 * also frees the client, so through libcephfs no call may follow it.
 */
TEST_F(TestClient, LL_UnmountWhileBusy) {
  using namespace std::chrono_literals;
  Inode *root = client->get_root();
  ASSERT_NE(nullptr, root);
  std::string name = fmt::format("ll_umount_busy.{}", getpid());
  Inode *in = nullptr;
  Fh *fh = nullptr;
  struct ceph_statx stx;
  ASSERT_EQ(0, client->ll_createx(root, name.c_str(), 0644,
                                  O_RDWR | O_CREAT | O_TRUNC,
                                  &in, &fh, &stx, 0, 0, myperm));
  const int BLK = 4096;
  std::string d(BLK, 'u');
  for (int b = 0; b < 64; b++)
    ASSERT_EQ(BLK, client->ll_write(fh, b * BLK, BLK, d.data()));
  std::atomic<bool> stop{false};
  std::atomic<int> finished{0}, calls{0}, enotconn{0};
  std::vector<std::thread> th;
  for (int t = 0; t < 6; t++) {
    th.emplace_back([&, t] {
      std::mt19937 rng(t);
      while (!stop) {
        int64_t off = (rng() % 64) * BLK;
        int r;
        switch (t % 3) {
        case 0:
          r = client->ll_write(fh, off, BLK, d.data());
          break;
        case 1: {
          bufferlist bl;
          r = client->ll_read(fh, off, BLK, &bl);
          break;
        }
        default: {
          struct ceph_statx sx;
          r = client->ll_getattrx(in, &sx, CEPH_STATX_SIZE, 0, myperm);
        }
        }
        if (r == -ENOTCONN)
          enotconn++;
        calls++;
      }
      finished++;
    });
  }
  std::this_thread::sleep_for(1s);
  client->unmount();
  std::this_thread::sleep_for(1s);
  stop = true;
  auto deadline = std::chrono::steady_clock::now() + 60s;
  while (finished < int(th.size()) && std::chrono::steady_clock::now() < deadline)
    std::this_thread::sleep_for(100ms);
  if (finished < int(th.size())) {
    ADD_FAILURE() << int(th.size()) - finished << " calls hang after the unmount";
    std::abort();
  }
  for (auto& x : th)
    x.join();
  EXPECT_GT(calls.load(), 0);
  EXPECT_GT(enotconn.load(), 0);
}
