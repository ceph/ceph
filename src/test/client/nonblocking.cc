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

#include <errno.h>

#include <iostream>
#include <string>

#include <fmt/format.h>

#include "test/client/TestClient.h"

TEST_F(TestClient, LlreadvLlwritev) {
  int mypid = getpid();
  char filename[256];

  client->unmount();
  TearDown();
  SetUp();

  sprintf(filename, "test_llreadvllwritevfile%u", mypid);

  Inode *root, *file;
  root = client->get_root();
  ASSERT_NE(root, (Inode *)NULL);

  Fh *fh;
  struct ceph_statx stx;

  ASSERT_EQ(0, client->ll_createx(root, filename, 0666,
				  O_RDWR | O_CREAT | O_TRUNC,
				  &file, &fh, &stx, 0, 0, myperm));

  /* Reopen read-only */
  char out0[] = "hello ";
  char out1[] = "world\n";
  struct iovec iov_out[2] = {
	{out0, sizeof(out0)},
	{out1, sizeof(out1)},
  };
  char in0[sizeof(out0)];
  char in1[sizeof(out1)];
  struct iovec iov_in[2] = {
	{in0, sizeof(in0)},
	{in1, sizeof(in1)},
  };

  char out_a_0[] = "hello ";
  char out_a_1[] = "world a is longer\n";
  struct iovec iov_out_a[2] = {
	{out_a_0, sizeof(out_a_0)},
	{out_a_1, sizeof(out_a_1)},
  };
  char in_a_0[sizeof(out_a_0)];
  char in_a_1[sizeof(out_a_1)];
  struct iovec iov_in_a[2] = {
	{in_a_0, sizeof(in_a_0)},
	{in_a_1, sizeof(in_a_1)},
  };

  char out_b_0[] = "hello ";
  char out_b_1[] = "world b is much longer\n";
  struct iovec iov_out_b[2] = {
	{out_b_0, sizeof(out_b_0)},
	{out_b_1, sizeof(out_b_1)},
  };
  char in_b_0[sizeof(out_b_0)];
  char in_b_1[sizeof(out_b_1)];
  struct iovec iov_in_b[2] = {
	{in_b_0, sizeof(in_b_0)},
	{in_b_1, sizeof(in_b_1)},
  };

  ssize_t nwritten = iov_out[0].iov_len + iov_out[1].iov_len;

  std::unique_ptr<C_SaferCond> writefinish = nullptr;
  std::unique_ptr<C_SaferCond> readfinish = nullptr;

  writefinish.reset(new C_SaferCond("test-nonblocking-writefinish"));
  readfinish.reset(new C_SaferCond("test-nonblocking-readfinish"));

  int64_t rc;
  bufferlist bl;

  rc = client->ll_preadv_pwritev(fh, iov_out, 2, 0, true, writefinish.get(), nullptr);
  ASSERT_EQ(0, rc);
  rc = writefinish->wait();
  ASSERT_EQ(nwritten, rc);

  rc = client->ll_preadv_pwritev(fh, iov_in, 2, 0, false, readfinish.get(), &bl);
  ASSERT_EQ(0, rc);
  rc = readfinish.get()->wait();
  ASSERT_EQ(nwritten, rc);
  copy_bufferlist_to_iovec(iov_in, 2, &bl, rc);

  ASSERT_EQ(0, strncmp((const char*)iov_in[0].iov_base, (const char*)iov_out[0].iov_base, iov_out[0].iov_len));
  ASSERT_EQ(0, strncmp((const char*)iov_in[1].iov_base, (const char*)iov_out[1].iov_base, iov_out[1].iov_len));

  // need new condition variables...
  writefinish.reset(new C_SaferCond("test-nonblocking-writefinish"));
  readfinish.reset(new C_SaferCond("test-nonblocking-readfinish"));
  ssize_t nwritten_a = iov_out_a[0].iov_len + iov_out_a[1].iov_len;

  rc = client->ll_preadv_pwritev(fh, iov_out_a, 2, 100, true, writefinish.get(), nullptr);
  ASSERT_EQ(0, rc);
  rc = writefinish->wait();
  ASSERT_EQ(nwritten_a, rc);

  rc = client->ll_preadv_pwritev(fh, iov_in_a, 2, 100, false, readfinish.get(), &bl);
  ASSERT_EQ(0, rc);
  rc = readfinish.get()->wait();
  ASSERT_EQ(nwritten_a, rc);
  copy_bufferlist_to_iovec(iov_in_a, 2, &bl, rc);

  ASSERT_EQ(0, strncmp((const char*)iov_in_a[0].iov_base, (const char*)iov_out_a[0].iov_base, iov_out_a[0].iov_len));
  ASSERT_EQ(0, strncmp((const char*)iov_in_a[1].iov_base, (const char*)iov_out_a[1].iov_base, iov_out_a[1].iov_len));

  // need new condition variables...
  writefinish.reset(new C_SaferCond("test-nonblocking-writefinish"));
  readfinish.reset(new C_SaferCond("test-nonblocking-readfinish"));
  ssize_t nwritten_b = iov_out_b[0].iov_len + iov_out_b[1].iov_len;

  rc = client->ll_preadv_pwritev(fh, iov_out_b, 2, 1000, true, writefinish.get(), nullptr, true, false);
  ASSERT_EQ(0, rc);
  rc = writefinish->wait();
  ASSERT_EQ(nwritten_b, rc);

  rc = client->ll_preadv_pwritev(fh, iov_in_b, 2, 1000, false, readfinish.get(), &bl);
  ASSERT_EQ(0, rc);
  rc = readfinish.get()->wait();
  ASSERT_EQ(nwritten_b, rc);
  copy_bufferlist_to_iovec(iov_in_b, 2, &bl, rc);

  ASSERT_EQ(0, strncmp((const char*)iov_in_b[0].iov_base, (const char*)iov_out_b[0].iov_base, iov_out_b[0].iov_len));
  ASSERT_EQ(0, strncmp((const char*)iov_in_b[1].iov_base, (const char*)iov_out_b[1].iov_base, iov_out_b[1].iov_len));

  client->ll_release(fh);
  ASSERT_EQ(0, client->ll_unlink(root, filename, myperm));
}

TEST_F(TestClient, LlreadvLlwritevNullContext) {
  /* Test that if Client::ll_preadv_pwritev is called with nullptr context
  then it performs a sync call. */

  int mypid = getpid();
  char filename[256];

  client->unmount();
  TearDown();
  SetUp();

  sprintf(filename, "test_llreadvllwritevnullcontextfile%u", mypid);

  Inode *root, *file;
  root = client->get_root();
  ASSERT_NE(root, (Inode *)NULL);

  Fh *fh;
  struct ceph_statx stx;

  ASSERT_EQ(0, client->ll_createx(root, filename, 0666,
				  O_RDWR | O_CREAT | O_TRUNC,
				  &file, &fh, &stx, 0, 0, myperm));

  char out0[] = "hello ";
  char out1[] = "world\n";  
  struct iovec iov_out[2] = {
	  {out0, sizeof(out0)},
	  {out1, sizeof(out1)}
  };

  char in0[sizeof(out0)];
  char in1[sizeof(out1)];
  struct iovec iov_in[2] = {
	  {in0, sizeof(in0)},
	  {in1, sizeof(in1)}
  };

  ssize_t bytes_to_write = iov_out[0].iov_len + iov_out[1].iov_len;

  int64_t rc;
  bufferlist bl;
  rc = client->ll_preadv_pwritev(fh, iov_out, 2, 0, true, nullptr, nullptr);
  ASSERT_EQ(rc, bytes_to_write);

  rc = client->ll_preadv_pwritev(fh, iov_in, 2, 0, false, nullptr, &bl);
  ASSERT_EQ(rc, bytes_to_write);

  copy_bufferlist_to_iovec(iov_in, 2, &bl, rc);
  ASSERT_EQ(0, strncmp((const char*)iov_in[0].iov_base,
                       (const char*)iov_out[0].iov_base,
                       iov_out[0].iov_len));
  ASSERT_EQ(0, strncmp((const char*)iov_in[1].iov_base,
                       (const char*)iov_out[1].iov_base, 
                       iov_out[1].iov_len));

  client->ll_release(fh);
  ASSERT_EQ(0, client->ll_unlink(root, filename, myperm));
}
