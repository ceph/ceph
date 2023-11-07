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
#include <signal.h>
#include <unistd.h>

#include "test/client/TestClient.h"
#include "global/global_context.h"

#define dout_subsys ceph_subsys_client

void signal_handler(int sig) {
  ldout(10, cct) << "Interrupted system call received: " << sig << dendl;
}

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

  char out_empty_buf_0[0];
  char out_empty_buf_1[0];
  struct iovec iov_out_zero_bytes[2] = {
    {out_empty_buf_0, sizeof(out_empty_buf_0)},
    {out_empty_buf_1, sizeof(out_empty_buf_1)}
  };

  char in_empty_buf_0[sizeof(out_empty_buf_0)];
  char in_empty_buf_1[sizeof(out_empty_buf_1)];
  struct iovec iov_in_zero_bytes[2] = {
    {in_empty_buf_0, sizeof(in_empty_buf_0)},
    {in_empty_buf_1, sizeof(in_empty_buf_1)}
  }

  struct iovec iov_out_null_context[2] = {
	{out0, sizeof(out0)},
	{out1, sizeof(out1)}
  };
  
  struct iovec iov_in_null_context[2] = {
	{in0, sizeof(in0)},
	{in1, sizeof(in1)}
  };

  struct iovec iov_out_dataonly_fysnc[2] = {
    {out0, sizeof(out0)},
    {out1, sizeof(out1)}
  };

  struct iovec iov_in_dataonly_fysnc[2] = {
    {in0, sizeof(in0)},
    {in1, sizeof(in1)}
  };

  struct iovec iov_out_empty[0];
  struct iovec iov_in_empty[0];

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
  
  // test zero bytes async IO
  writefinish.reset(new C_SaferCond("test-nonblocking-writefinish-zero-bytes"));
  readfinish.reset(new C_SaferCond("test-nonblocking-readfinish-zero-bytes"));
  ssize_t nwritten_zero_bytes = iov_out_zero_bytes[0].iov_len
                                + iov_out_zero_bytes[1].iov_len;
  ASSERT_EQ(nwritten_zero_bytes, 0);

  rc = client->ll_preadv_pwritev(fh, iov_out_zero_bytes, 2, 0, true,
                                 writefinish.get(), nullptr);
  ASSERT_EQ(0, rc);
  rc = writefinish->wait();
  ASSERT_EQ(nwritten_zero_bytes, rc);

  rc = client->ll_preadv_pwritev(fh, iov_in_zero_bytes, 2, 0, false,
                                 readfinish.get(), &bl);
  ASSERT_EQ(0, rc);
  rc = readfinish->wait();
  ASSERT_EQ(nwritten_zero_bytes, rc);
  copy_bufferlist_to_iovec(iov_in_zero_bytes, 2, &bl, rc);

  ASSERT_EQ(0, strncmp((const char*)iov_in_zero_bytes[0].iov_base,
                       (const char*)iov_out_zero_bytes[0].iov_base,
                       iov_out_zero_bytes[0].iov_len));
  ASSERT_EQ(0, strncmp((const char*)iov_in_zero_bytes[1].iov_base,
                       (const char*)iov_out_zero_bytes[1].iov_base,
                       iov_out_zero_bytes[1].iov_len));

  // test async I/O with nullptr context
  writefinish.reset(new C_SaferCond("test-nonblocking-writefinish-nullptr-context"));
  readfinish.reset(new C_SaferCond("test-nonblocking-readfinish-nullptr-context"));

  ssize_t nwritten_null_context = iov_out_null_context[0].iov_len
                                  + iov_out_null_context[1].iov_len;

  rc = client->ll_preadv_pwritev(fh, iov_out_null_context, 2, 0, true,
                                 nullptr, nullptr);
  ASSERT_EQ(0, rc);
  rc = writefinish->wait();
  ASSERT_EQ(nwritten_null_context, rc);

  rc = client->ll_preadv_pwritev(fh, iov_in_null_context, 2, 0, false,
                                 nullptr, &bl);
  ASSERT_EQ(0, rc);
  rc = readfinish->wait();
  ASSERT_EQ(nwritten_null_context, rc);
  copy_bufferlist_to_iovec(iov_in_null_context, 2, &bl, rc);

  ASSERT_EQ(0, strncmp((const char*)iov_in_null_context[0].iov_base,
                       (const char*)iov_out_null_context[0].iov_base,
                       iov_out_null_context[0].iov_len));
  ASSERT_EQ(0, strncmp((const char*)iov_in_null_context[1].iov_base,
                       (const char*)iov_out_null_context[1].iov_base,
                       iov_out_null_context[1].iov_len));

  // test data-only fysnc calls during async I/O
  writefinish.reset(new C_SaferCond("test-nonblocking-writefinish-dataonly-fsync"));
  readfinish.reset(new C_SaferCond("test-nonblocking-readfinish-dataonly-fsync"));

  ssize_t nwritten_dataonly_fsync = iov_out_dataonly_fysnc[0].iov_len
                                    + iov_out_dataonly_fysnc[1].iov_len;

  rc = client->ll_preadv_pwritev(fh, iov_out_dataonly_fysnc, 2, 100,
                                 true, writefinish.get(), nullptr,
                                 true, true);
  ASSERT_EQ(0, rc);
  rc = writefinish->wait();
  ASSERT_EQ(nwritten_dataonly_fsync, rc);

  rc = client->ll_preadv_pwritev(fh, iov_in_dataonly_fysnc, 2, 100,
                                 false, readfinish.get(), &bl);
  ASSERT_EQ(0, rc);
  rc = readfinish.get()->wait();
  ASSERT_EQ(nwritten_dataonly_fsync, rc);
  copy_bufferlist_to_iovec(iov_in_dataonly_fysnc, 2, &bl, rc);

  ASSERT_EQ(0, strncmp((const char*)iov_in_dataonly_fysnc[0].iov_base,
            (const char*)iov_out_dataonly_fysnc[0].iov_base,
            iov_out_dataonly_fysnc[0].iov_len));
  ASSERT_EQ(0, strncmp((const char*)iov_in_dataonly_fysnc[1].iov_base,
            (const char*)iov_out_dataonly_fysnc[1].iov_base,
            iov_out_dataonly_fysnc[1].iov_len));

  // test async I/O with empty read/write buffer
  writefinish.reset(new C_SaferCond("test-nonblocking-writefinish-empty-write-buffer"));
  readfinish.reset(new C_SaferCond("test-nonblocking-readfinish-empty-read-buffer"));

  rc = client->ll_preadv_pwritev(fh, iov_out_empty, 2, 0, true,
                                 writefinish.get(), nullptr);
  ASSERT_EQ(0, rc);
  rc = writefinish->wait();
  ASSERT_EQ(0, rc);

  rc = client->ll_preadv_pwritev(fh, iov_in_empty, 2, 0, false,
                                 readfinish.get(), &bl);
  ASSERT_EQ(0, rc);
  rc = readfinish->wait();
  ASSERT_EQ(0, rc);

  client->ll_release(fh);
  ASSERT_EQ(0, client->ll_unlink(root, filename, myperm));
}

TEST_F(TestClient, LlreadvLlwritevSignalInterrupt) {
  int mypid = getpid();
  char filename[256];

  client->unmount();
  TearDown();
  SetUp();

  sprintf(filename, "test_llreadvllwritevsingalinterruptfile%u", mypid);

  Inode *root, *file;
  root = client->get_root();
  ASSERT_NE(root, (Inode *)NULL);

  Fh *fh;
  struct ceph_statx stx;

  ASSERT_EQ(0, client->ll_createx(root, filename, 0666,
				  O_RDWR | O_CREAT | O_TRUNC,
				  &file, &fh, &stx, 0, 0, myperm));

  const int BUF_SIZE = 512 * 1024 * 1024;

  char* in_buf_0 = new char[BUF_SIZE];
  char* in_buf_1 = new char[BUF_SIZE];
  ASSERT_NE(in_buf_0, 0);
  ASSERT_NE(in_buf_1, 0);
  
  struct iovec iov_in_signal_interrupt[2] = {
    {in_buf_0, strlen(in_buf_0)},
    {in_buf_1, strlen(in_buf_1)}
  };

  char* out_buf_0 = new char[BUF_SIZE];
  char* out_buf_1 = new char[BUF_SIZE];
  ASSERT_NE(out_buf_0, 0);
  ASSERT_NE(out_buf_1, 0);
  
  memset(out_buf_0, 0xcc, BUF_SIZE);
  memset(out_buf_1, 0xcc, BUF_SIZE);
  
  struct iovec iov_out_signal_interrupt[2] = {
    {out_buf_0, strlen(out_buf_0)},
    {out_buf_1, strlen(out_buf_1)}
  };

  std::unique_ptr<C_SaferCond> writefinish = nullptr;
  std::unique_ptr<C_SaferCond> readfinish = nullptr;

  writefinish.reset(new C_SaferCond("test-nonblocking-writefinish-singal-interrupt"));
  readfinish.reset(new C_SaferCond("test-nonblocking-readfinish-singal-interrupt"));

  int64_t rc;
  bufferlist bl;

  signal(SIGALRM, signal_handler);
  alarm(2);

  rc = client->ll_preadv_pwritev(fh, iov_out_signal_interrupt, 2, 0, true,
                                 writefinish.get(), nullptr);
  ASSERT_NE(rc, 0);
  rc = writefinish->wait();
  ASSERT_EQ(rc, -1);
  ASSERT_EQ(errno, EINTR);

  rc = client->ll_preadv_pwritev(fh, iov_out_signal_interrupt, 2, 0, true,
                                 writefinish.get(), nullptr);
  ASSERT_EQ(rc, 0);
  rc = writefinish->wait();
  ASSERT_EQ(rc, BUF_SIZE);
  
  alarm(2);
  rc = client->ll_preadv_pwritev(fh, iov_in_signal_interrupt, 2, 0, true,
                                 readfinish.get(), &bl);
  ASSERT_NE(rc, 0);
  rc = readfinish->wait();
  ASSERT_EQ(rc, -1);

  signal(SIGALRM, SIG_DFL);  
  delete[] out_buf_0;
  delete[] out_buf_1;
  delete[] in_buf_0;
  delete[] in_buf_1;
  client->ll_release(fh);
  ASSERT_EQ(0, client->ll_unlink(root, filename, myperm)); 
}

TEST_F(TestClient, LlreadvLlwritevNonContiguous) {
  int mypid = getpid();
  char filename[256];

  client->unmount();
  TearDown();
  SetUp();

  sprintf(filename, "test_llreadvllwritevnoncontiguousfile%u", mypid);

  Inode *root, *file;
  root = client->get_root();
  ASSERT_NE(root, (Inode *)NULL);

  Fh *fh;
  struct ceph_statx stx;

  ASSERT_EQ(0, client->ll_createx(root, filename, 0666,
				  O_RDWR | O_CREAT | O_TRUNC,
				  &file, &fh, &stx, 0, 0, myperm));
  
  const int NUM_BUF = 5;
  char out_buf_0[] = "hello ";
  char out_buf_1[] = "world\n";
  char out_buf_2[] = "Ceph - ";
  char out_buf_3[] = "a scalable distributed ";
  char out_buf_4[] = "storage system\n";

  struct iovec iov_out_non_contiguous[NUM_BUF] = {
    {out_buf_0, sizeof(out_buf_0)},
    {out_buf_1, sizeof(out_buf_1)},
    {out_buf_2, sizeof(out_buf_2)},
    {out_buf_3, sizeof(out_buf_3)},
    {out_buf_4, sizeof(out_buf_4)}
  };

  char in_buf_0[sizeof(out_buf_0)];
  char in_buf_1[sizeof(out_buf_1)];
  char in_buf_2[sizeof(out_buf_2)];
  char in_buf_3[sizeof(out_buf_3)];
  char in_buf_4[sizeof(out_buf_4)];

  struct iovec iov_in_contiguous[NUM_BUF] = {
    {in_buf_0, sizeof(in_buf_0)},
    {in_buf_1, sizeof(in_buf_1)},
    {in_buf_2, sizeof(in_buf_2)},
    {in_buf_3, sizeof(in_buf_3)},
    {in_buf_4, sizeof(in_buf_4)}
  };

  struct iovec *current_iov = iov_out_non_contiguous;

  ssize_t bytes_to_write = 0, total_bytes_written = 0, total_bytes_read = 0;
  for(int i = 0; i < NUM_BUF; ++i) {
    bytes_to_write += iov_out_non_contiguous[i].iov_len;
  }
  
  std::unique_ptr<C_SaferCond> writefinish = nullptr;
  std::unique_ptr<C_SaferCond> readfinish = nullptr;

  writefinish.reset(new C_SaferCond("test-nonblocking-writefinish-non-contiguous"));
  readfinish.reset(new C_SaferCond("test-nonblocking-readfinish-contiguous"));

  int64_t rc;
  bufferlist bl;

  for(int i = 0; i < NUM_BUF; ++i) {
    rc = client->ll_preadv_pwritev(fd, current_iov++, 1, i * NUM_BUF * 10,
                                   true, writefinish.get(), nullptr);
    ASSERT_EQ(rc, 0);
    total_bytes_written += writefinish->wait();
  }
  ASSERT_EQ(total_bytes_written, bytes_to_write);

  rc = client->ll_preadv_pwritev(fd, iov_in_contiguous, NUM_BUF, 0, false,
                                 readfinish.get(), &bl);
  ASSERT_EQ(rc, 0);
  total_bytes_read = readfinish->wait();
  ASSERT_LE(total_bytes_read, total_bytes_written);

  readfinish.reset(new C_SaferCond("test-nonblocking-readfinish-non-contiguous"));
  
  memset(in_buf_0, '\0', sizeof(in_buf_0));
  memset(in_buf_1, '\0', sizeof(in_buf_1));
  memset(in_buf_2, '\0', sizeof(in_buf_2));
  memset(in_buf_3, '\0', sizeof(in_buf_3));
  memset(in_buf_4, '\0', sizeof(in_buf_4));

  struct iovec iov_in_non_contiguous[NUM_BUF] = {
    {in_buf_0, sizeof(in_buf_0)},
    {in_buf_1, sizeof(in_buf_1)},
    {in_buf_2, sizeof(in_buf_2)},
    {in_buf_3, sizeof(in_buf_3)},
    {in_buf_4, sizeof(in_buf_4)}
  };

  current_iov = iov_in_non_contiguous;
  bl.clear();
  total_bytes_read = 0;

  for(int i = 0; i < NUM_BUF; ++i) {
    ssize_t bytes_read = 0;
    rc = client->ll_preadv_pwritev(fd, current_iov++, 1, i * NUM_BUF * 10,
                                   false, readfinish.get(), &bl);
    ASSERT_EQ(rc, 0);
    bytes_read = readfinish->wait();
    ASSERT_EQ(bytes_read, iov_out_non_contiguous[i].iov_len);
    total_bytes_read += bytes_read; 
  }
  ASSERT_EQ(total_bytes_read, total_bytes_written);

  copy_bufferlist_to_iovec(iov_in_non_contiguous, NUM_BUF, &bl, total_bytes_read);
  for(int i = 0; i < NUM_BUF; ++i) {
    ASSERT_EQ(0, strncmp((const char*)iov_in_non_contiguous[0].iov_base,
                         (const char*)iov_out_non_contiguous[0].iov_base,
                         iov_out_non_contiguous[0].iov_len));
  }
  
  client->ll_release(fh);
  ASSERT_EQ(0, client->ll_unlink(root, filename, myperm));        
}

TEST_F(TestClient, LlreadvLlwritevOverlimit) {
  int mypid = getpid();
  char filename[256];

  client->unmount();
  TearDown();
  SetUp();

  sprintf(filename, "test_llreadvllwritevoverlimitfile%u", mypid);

  Inode *root, *file;
  root = client->get_root();
  ASSERT_NE(root, (Inode *)NULL);

  Fh *fh;
  struct ceph_statx stx;

  ASSERT_EQ(0, client->ll_createx(root, filename, 0666,
				  O_RDWR | O_CREAT | O_TRUNC,
				  &file, &fh, &stx, 0, 0, myperm));

  // setup buffer array
  const int IOV_SEG_OVERLIMIT = 1500;
  char out_buff[IOV_SEG_OVERLIMIT][5];
  struct iovec iov_out_overlimit[IOV_SEG_OVERLIMIT];
  struct iovec iov_in_overlimit[IOV_SEG_OVERLIMIT];
  for(int i = 0; i < IOV_SEG_OVERLIMIT; ++i) {
    char num[2];
    char out_str[5];
    strcpy(out_str, "foo");
    sprintf(num, "%d", i);
    strcat(out_str, num);

    // fill iovec strcutures for write op
    iov_out_overlimit[i].iov_base = out_str;
    iov_out_overlimit[i].iov_len = sizeof(out_str);

    // set up iovec strcutures for read op
    char in_str[sizeof(out_str)];
    iov_in_overlimit[i].iov_base = in_str;
    iov_in_overlimit[i].iov_len = sizeof(in_str);
  }

  // test if iovcnt exceeds 1024 segments then no data transfer occurs 
  writefinish.reset(new C_SaferCond("test-nonblocking-writefinish-overlimit"));
  readfinish.reset(new C_SaferCond("test-nonblocking-readfinish-overlimit"));

  rc = client->ll_preadv_pwritev(fh, iov_out_overlimit, IOV_SEG_OVERLIMIT,
                                 0, true, writefinish.get(), nullptr);
  ASSERT_EQ(-1, rc);
  rc = writefinish->wait();
  // no data should've been written
  ASSERT_EQ(0, rc);

  rc = client->ll_preadv_pwritev(fh, iov_in_overlimit, IOV_SEG_OVERLIMIT,
                                 0, false, readfinish.get(), &bl);
  ASSERT_EQ(-1, rc);
  rc = readfinish->wait();
  // no data should've been read
  ASSERT_EQ(0, rc);
  copy_bufferlist_to_iovec(iov_in_overlimit, IOV_SEG_OVERLIMIT, &bl, rc);

  // since there was no read, the iov_in_overlimit buffers should be empty
  // and so the strncmp must return a negative val
  for(int i = 0; i < IOV_SEG_OVERLIMIT; ++i) {
    ASSERT_LT(strncmp((const char*)iov_in_overlimit[i].iov_base,
              (const char*)iov_out_overlimit[i].iov_base,
              iov_out_overlimit[i].iov_len), 0);
  }

  client->ll_release(fh);
  ASSERT_EQ(0, client->ll_unlink(root, filename, myperm));
}
