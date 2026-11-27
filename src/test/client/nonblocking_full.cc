// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2024 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <errno.h>

#include <iostream>
#include <string>

#include <fmt/format.h>
#include <sys/statvfs.h>

#include "test/client/TestClient.h"

TEST_F(TestClient, LlreadvLlwritevDataPoolFull) {
  /* Test perfoming async I/O after filling the fs and make sure it handles
  the write gracefully */

  Inode *root = nullptr, *file_a = nullptr;
  Fh *fh_a = nullptr;
  struct ceph_statx stx_a;
  root = client->get_root();
  ASSERT_NE(root, (Inode *)NULL);

  int mypid = getpid();
  char fname_a[256];
  sprintf(fname_a, "test_llreadvllwritevdatapoolfullfile_a%u", mypid);
  ASSERT_EQ(0, client->ll_createx(root, fname_a, 0666,
                                  O_RDWR | O_CREAT | O_TRUNC,
                                  &file_a, &fh_a, &stx_a, 0, 0, myperm));                                         

  int64_t rc = 0, bytes_written = 0;

  // this test case cannot handle multiple data pools
  const std::vector<int64_t> &data_pools = client->mdsmap->get_data_pools();
  ASSERT_EQ(data_pools.size(), 1);

  struct statvfs stbuf;
  rc = client->ll_statfs(root, &stbuf, myperm);
  ASSERT_EQ(rc, 0);
  // available size = num of free blocks * size of a block
  size_t data_pool_available_space = stbuf.f_bfree * stbuf.f_bsize;
  ASSERT_GT(data_pool_available_space, 0);

  off_t offset = 0;
  // writing blocks of 1GiB
  const size_t BLOCK_SIZE = 1024 * 1024 * 1024;
  client->ll_write_n_bytes(fh_a, size_t(data_pool_available_space / 2),
                           BLOCK_SIZE, 4, &offset);

  // get a new file
  mypid = getpid();
  char fname_b[256];
  Inode *file_b = nullptr;
  Fh *fh_b = nullptr;
  struct ceph_statx stx_b;
  sprintf(fname_b, "test_llreadvllwritevdatapoolfullfile_b%u", mypid);
  ASSERT_EQ(0, client->ll_createx(root, fname_b, 0666,
                                  O_RDWR | O_CREAT | O_TRUNC,
                                  &file_b, &fh_b, &stx_b, 0, 0, myperm));                                         

  client->ll_write_n_bytes(fh_b, size_t((data_pool_available_space * 1.1) / 2),
                           BLOCK_SIZE, 4, &offset);

  // if we're here then it means the write succeeded but the cluster is full
  // so let us get a new osdmap epoch
  const epoch_t osd_epoch = objecter->with_osdmap(std::mem_fn(&OSDMap::get_epoch));

  objecter->maybe_request_map();

  // wait till we have a new osdmap epoch
  ASSERT_TRUE(client->wait_for_osdmap_epoch_update(osd_epoch));

  // with the new osdmap epoch, the pools should return full flag
  bool data_pool_full = client->wait_until_true([&]()
                        { return client->is_data_pool_full(data_pools[0]); });
  ASSERT_TRUE(data_pool_full);

  // write here should fail since the cluster is full
  const size_t TINY_BLOCK_SIZE = 256 * 1024 * 1024;
  auto out_buf_0 = std::make_unique<char[]>(TINY_BLOCK_SIZE);
  memset(out_buf_0.get(), 0xDD, TINY_BLOCK_SIZE);
  auto out_buf_1 = std::make_unique<char[]>(TINY_BLOCK_SIZE);
  memset(out_buf_1.get(), 0xFF, TINY_BLOCK_SIZE);

  struct iovec iov_out[2] = {
    {out_buf_0.get(), TINY_BLOCK_SIZE},
    {out_buf_1.get(), TINY_BLOCK_SIZE}
  };

  std::unique_ptr<C_SaferCond> writefinish = nullptr;
  writefinish.reset(new C_SaferCond("test-nonblocking-writefinish-datapool-full"));
  rc = client->ll_preadv_pwritev(fh_b, iov_out, 2,
                                 size_t(data_pool_available_space / 2),
                                 true, writefinish.get(), nullptr);
  ASSERT_EQ(rc, 0);
  bytes_written = writefinish->wait();
  ASSERT_EQ(bytes_written, -ENOSPC);

  client->ll_release(fh_a);
  ASSERT_EQ(0, client->ll_unlink(root, fname_a, myperm));
  client->ll_release(fh_b);
  ASSERT_EQ(0, client->ll_unlink(root, fname_b, myperm));
}
