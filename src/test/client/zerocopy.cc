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

/*
 * These tests provide basic testing for zerocopy RW ops. We test here only blocking calls
 * since the decision (in the client) on either to copy the data or not doesn't depend on
 * whether the IO will be blocking or non blocking
 */
class ZeroCopyTest : public TestClient {
protected:
    Fh *fh;
    static constexpr char out0[] = "hello";
    static constexpr char out1[] = "big";
    static constexpr char out2[] = "world!";
    static constexpr int IOV_COUNT = 3;
    struct iovec iov_out[IOV_COUNT];

    bool write = true;
    bool zerocopy = true;

    ssize_t data_length;

    void SetUp() override {
      TestClient::SetUp();
      int mypid = getpid();
      char filename[256];
      sprintf(filename, "test_zerocopy%u", mypid);
      Inode *root, *file;
      root = client->get_root();
      ASSERT_NE(root, (Inode *) NULL);
      struct ceph_statx stx;
      ASSERT_EQ(0, client->ll_createx(root, filename, 0666,
                                      O_RDWR | O_CREAT | O_TRUNC,
                                      &file, &fh, &stx, 0, 0, myperm));
      memset(iov_out, 0, sizeof(iov_out));
      iov_out[0].iov_base = const_cast<char*>(out0);
      iov_out[0].iov_len = sizeof(out0);
      iov_out[1].iov_base = const_cast<char*>(out1);
      iov_out[1].iov_len = sizeof(out1);
      iov_out[2].iov_base = const_cast<char*>(out2);
      iov_out[2].iov_len = sizeof(out2);

      data_length = iov_out[0].iov_len + iov_out[1].iov_len + iov_out[2].iov_len;

      write = true;
      zerocopy = true;
    }
};

TEST_F(ZeroCopyTest, LlPreadvPwritevZerocopyObjectCacherEnabled) {
  client->cct->_conf->client_oc = true;
  auto rc = client->ll_preadv_pwritev(fh, iov_out, IOV_COUNT, 0, write, nullptr, nullptr, false, false, zerocopy);
  ASSERT_EQ(rc, -EINVAL);
}

TEST_F(ZeroCopyTest, LlPreadvPwritevZerocopyInvalidParams) {
  client->cct->_conf->client_oc = false;
  bufferlist  bl;
  // for read we must provide bufferlist so we will not and it should fail
  write = false;
  auto rc = client->ll_preadv_pwritev(fh, iov_out, IOV_COUNT, 0, write, nullptr, nullptr, false, false, zerocopy);
  ASSERT_EQ(rc, -EINVAL);
  // write, with valid iov but negative iov_count should fail as well
  write = true;
  rc = client->ll_preadv_pwritev(fh, iov_out, -1, 0, write, nullptr, &bl, false, false, zerocopy);
  ASSERT_EQ(rc, -EINVAL);
  // write, without iovec
  rc = client->ll_preadv_pwritev(fh, nullptr, IOV_COUNT, 0, write, nullptr, &bl, false, false, zerocopy);
  ASSERT_EQ(rc, -EINVAL);
}

TEST_F(ZeroCopyTest, LlPreadvPwritevZerocopyWrite) {
  client->cct->_conf->client_oc = false;

  int64_t rc;
  bufferlist bl;

  // first test with zerocopy =true
  zerocopy = true;
  write = true;

  auto metrics = client->get_copy_metrics();
  auto copy_ops_before = metrics.first;
  auto copy_size_before = metrics.second;
  // context is null so it will be blocking call
  rc = client->ll_preadv_pwritev(fh, iov_out, IOV_COUNT, 0, write, nullptr, &bl, false, false, zerocopy);
  ASSERT_EQ(data_length, rc);
  metrics = client->get_copy_metrics();
  ASSERT_EQ(copy_ops_before, metrics.first);
  ASSERT_EQ(copy_size_before, metrics.second);

  // now with zerocopy set to false
  zerocopy = false;
  metrics = client->get_copy_metrics();
  copy_ops_before = metrics.first;
  copy_size_before = metrics.second;
  // context is null so it will be blocking call
  rc = client->ll_preadv_pwritev(fh, iov_out, IOV_COUNT, 0, write, nullptr, &bl, false, false, zerocopy);
  ASSERT_EQ(data_length, rc);
  metrics = client->get_copy_metrics();
  ASSERT_EQ(copy_ops_before + IOV_COUNT, metrics.first);
  ASSERT_EQ(copy_size_before + data_length, metrics.second);
}

TEST_F(ZeroCopyTest, LlPreadvPwritevZerocopyRead) {
  client->cct->_conf->client_oc = false;
  // first write the file
  auto metrics = client->get_copy_metrics();
  auto copy_ops_before = metrics.first;
  auto copy_size_before = metrics.second;
  auto rc = client->ll_preadv_pwritev(fh, iov_out, IOV_COUNT, 0, write, nullptr, nullptr, false, false, zerocopy);
  ASSERT_EQ(data_length, rc);
  metrics = client->get_copy_metrics();
  ASSERT_EQ(copy_ops_before, metrics.first);
  ASSERT_EQ(copy_size_before, metrics.second);

  // now read, with zerocopy it should be into the bufferlist
  std::vector<std::vector<char>> in_vector;
  in_vector.emplace_back(iov_out[0].iov_len);
  in_vector.emplace_back(iov_out[1].iov_len);
  in_vector.emplace_back(iov_out[2].iov_len);
  struct iovec iov_in[IOV_COUNT] = {
      {in_vector[0].data(), in_vector[0].size()},
      {in_vector[1].data(), in_vector[1].size()},
      {in_vector[2].data(), in_vector[2].size()},
  };

  bufferlist bl;
  write = false;
  copy_ops_before = metrics.first;
  copy_size_before = metrics.second;
  rc = client->ll_preadv_pwritev(fh, iov_in, IOV_COUNT, 0, write, nullptr, &bl, false, false, zerocopy);
  ASSERT_EQ(data_length, rc);
  metrics = client->get_copy_metrics();
  ASSERT_EQ(copy_ops_before, metrics.first);
  ASSERT_EQ(copy_size_before, metrics.second);
  copy_bufferlist_to_iovec(iov_in, IOV_COUNT, &bl, rc);

  ASSERT_EQ(0, strncmp((const char*)iov_in[0].iov_base, (const char*)iov_out[0].iov_base, iov_out[0].iov_len));
  ASSERT_EQ(0, strncmp((const char*)iov_in[1].iov_base, (const char*)iov_out[1].iov_base, iov_out[1].iov_len));
  ASSERT_EQ(0, strncmp((const char*)iov_in[2].iov_base, (const char*)iov_out[2].iov_base, iov_out[2].iov_len));

  // read without zerocopy
  for (auto& vec : in_vector) {
    std::fill(vec.begin(), vec.end(), 0);
  }
  zerocopy = false;
  bl.clear();
  rc = client->ll_preadv_pwritev(fh, iov_in, IOV_COUNT, 0, write, nullptr, &bl, false, false, zerocopy);
  ASSERT_EQ(data_length, rc);
  metrics = client->get_copy_metrics();
  ASSERT_EQ(copy_ops_before + IOV_COUNT, metrics.first);
  ASSERT_EQ(copy_size_before + data_length, metrics.second);
  copy_bufferlist_to_iovec(iov_in, IOV_COUNT, &bl, rc);

  ASSERT_EQ(0, strncmp((const char*)iov_in[0].iov_base, (const char*)iov_out[0].iov_base, iov_out[0].iov_len));
  ASSERT_EQ(0, strncmp((const char*)iov_in[1].iov_base, (const char*)iov_out[1].iov_base, iov_out[1].iov_len));
  ASSERT_EQ(0, strncmp((const char*)iov_in[2].iov_base, (const char*)iov_out[2].iov_base, iov_out[2].iov_len));
}