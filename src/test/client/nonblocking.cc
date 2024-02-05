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
#include <sys/statvfs.h>

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

TEST_F(TestClient, LlreadvLlwritevOverlimit) {
  /* From man page: 
  POSIX.1  allows  an  implementation to place a limit on the number of
  items that can be passed in iov.  An implementation can advertise its limit
  by defining IOV_MAX in <limits.h> or at run time via the return value from
  sysconf(_SC_IOV_MAX).  On modern Linux systems, the limit is 1024. Back in
  Linux 2.0 days, this limit was 16 so test if iovcnt exceeds 1024 segments
  then no data transfer occurs.
  
  However going through the libcephfs or the client code, there is no such
  limit imposed in cephfs, therefore async I/O over the limit should succeed.
  */

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
  ssize_t bytes_to_write = 0;
  struct iovec iov_out_overlimit[IOV_SEG_OVERLIMIT];
  struct iovec iov_in_overlimit[IOV_SEG_OVERLIMIT];
  for(int i = 0; i < IOV_SEG_OVERLIMIT; ++i) {
    char num[5];
    char out_str[5];
    strcpy(out_str, "foo");
    sprintf(num, "%d", i);
    strcat(out_str, num);

    // fill iovec strcutures for write op
    iov_out_overlimit[i].iov_base = out_str;
    iov_out_overlimit[i].iov_len = sizeof(out_str);
    bytes_to_write += iov_out_overlimit[i].iov_len;

    // set up iovec strcutures for read op
    char in_str[sizeof(out_str)];
    iov_in_overlimit[i].iov_base = in_str;
    iov_in_overlimit[i].iov_len = sizeof(in_str);
  }

  std::unique_ptr<C_SaferCond> writefinish = nullptr;
  std::unique_ptr<C_SaferCond> readfinish = nullptr;

  writefinish.reset(new C_SaferCond("test-nonblocking-writefinish-overlimit"));
  readfinish.reset(new C_SaferCond("test-nonblocking-readfinish-overlimit"));

  int64_t rc;
  bufferlist bl;

  rc = client->ll_preadv_pwritev(fh, iov_out_overlimit, IOV_SEG_OVERLIMIT,
                                 0, true, writefinish.get(), nullptr);
  ASSERT_EQ(rc, 0);
  ssize_t bytes_written = writefinish->wait();
  ASSERT_EQ(bytes_written, bytes_to_write);

  rc = client->ll_preadv_pwritev(fh, iov_in_overlimit, IOV_SEG_OVERLIMIT,
                                 0, false, readfinish.get(), &bl);
  ASSERT_EQ(rc, 0);
  ssize_t bytes_read = readfinish->wait();
  ASSERT_EQ(bytes_read, bytes_to_write);
  copy_bufferlist_to_iovec(iov_in_overlimit, IOV_SEG_OVERLIMIT, &bl,
                           bytes_read);

  // since there was no read, the iov_in_overlimit buffers should be empty
  // and so the strncmp must return a negative val
  for(int i = 0; i < IOV_SEG_OVERLIMIT; ++i) {
    ASSERT_EQ(strncmp((const char*)iov_in_overlimit[i].iov_base,
              (const char*)iov_out_overlimit[i].iov_base,
              iov_out_overlimit[i].iov_len), 0);
  }

  client->ll_release(fh);
  ASSERT_EQ(0, client->ll_unlink(root, filename, myperm));
}

TEST_F(TestClient, LlreadvLlwritevNonContiguous) {
  /* Test writing at non-contiguous memory locations, and make sure read at
  the exact locations where the buffers are written and the bytes read should
  be equal to the bytes written.  */

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

  struct iovec iov_in_non_contiguous[NUM_BUF] = {
    {in_buf_0, sizeof(in_buf_0)},
    {in_buf_1, sizeof(in_buf_1)},
    {in_buf_2, sizeof(in_buf_2)},
    {in_buf_3, sizeof(in_buf_3)},
    {in_buf_4, sizeof(in_buf_4)}
  };

  ssize_t bytes_to_write = 0, total_bytes_written = 0, total_bytes_read = 0;
  for(int i = 0; i < NUM_BUF; ++i) {
    bytes_to_write += iov_out_non_contiguous[i].iov_len;
  }

  std::unique_ptr<C_SaferCond> writefinish = nullptr;
  std::unique_ptr<C_SaferCond> readfinish = nullptr;

  int64_t rc;
  bufferlist bl, tmpbl;

  struct iovec *current_iov = iov_out_non_contiguous;

  for(int i = 0; i < NUM_BUF; ++i) {
    writefinish.reset(new C_SaferCond("test-nonblocking-writefinish-non-contiguous"));
    rc = client->ll_preadv_pwritev(fh, current_iov++, 1, i * NUM_BUF * 10,
                                   true, writefinish.get(), nullptr);
    ASSERT_EQ(rc, 0);
    total_bytes_written += writefinish->wait();
  }
  ASSERT_EQ(total_bytes_written, bytes_to_write);

  current_iov = iov_in_non_contiguous;

  for(int i = 0; i < NUM_BUF; ++i) {
    readfinish.reset(new C_SaferCond("test-nonblocking-readfinish-non-contiguous"));
    ssize_t bytes_read = 0;
    rc = client->ll_preadv_pwritev(fh, current_iov++, 1, i * NUM_BUF * 10,
                                   false, readfinish.get(), &tmpbl);
    bl.append(tmpbl);
    ASSERT_EQ(rc, 0);
    bytes_read = readfinish->wait();
    ASSERT_EQ(bytes_read, iov_out_non_contiguous[i].iov_len);
    total_bytes_read += bytes_read; 
  }
  ASSERT_EQ(total_bytes_read, bytes_to_write);

  copy_bufferlist_to_iovec(iov_in_non_contiguous, NUM_BUF, &bl, total_bytes_read);
  for(int i = 0; i < NUM_BUF; ++i) {
    ASSERT_EQ(0, strncmp((const char*)iov_in_non_contiguous[i].iov_base,
                         (const char*)iov_out_non_contiguous[i].iov_base,
                         iov_out_non_contiguous[i].iov_len));
  }

  client->ll_release(fh);
  ASSERT_EQ(0, client->ll_unlink(root, filename, myperm));        
}

TEST_F(TestClient, LlreadvLlwritevBufferUnderflow) {
  /* Test with large input buffers and small read buffers to make sure I/O
  happens normally */

  int mypid = getpid();
  char filename[256];

  client->unmount();
  TearDown();
  SetUp();

  sprintf(filename, "test_llreadvllwritevbufferunderflowfile%u", mypid);

  Inode *root, *file;
  root = client->get_root();
  ASSERT_NE(root, (Inode *)NULL);

  Fh *fh;
  struct ceph_statx stx;

  ASSERT_EQ(0, client->ll_createx(root, filename, 0666,
				  O_RDWR | O_CREAT | O_TRUNC,
				  &file, &fh, &stx, 0, 0, myperm));

  const int WRITE_MAX = 1024;
  char out_buf_0[WRITE_MAX], out_buf_1[WRITE_MAX];
  memset(out_buf_0, 0xcc, sizeof(out_buf_0));
  memset(out_buf_1, 0xdd, sizeof(out_buf_1));
  struct iovec iov_out[2] = {
    {out_buf_0, sizeof(out_buf_0)},
    {out_buf_1, sizeof(out_buf_1)}
  };

  ssize_t bytes_to_write = iov_out[0].iov_len + iov_out[1].iov_len;

  // try to read 2x the written data
  char in_buf_0[WRITE_MAX * 2], in_buf_1[WRITE_MAX * 2];
  struct iovec iov_in[2] = {
    {in_buf_0, sizeof(in_buf_0)},
    {in_buf_1, sizeof(in_buf_1)}
  };

  std::unique_ptr<C_SaferCond> writefinish = nullptr;
  std::unique_ptr<C_SaferCond> readfinish = nullptr;

  writefinish.reset(new C_SaferCond("test-nonblocking-writefinish-buffer-underflow"));
  readfinish.reset(new C_SaferCond("test-nonblocking-readfinish-buffer-underflow"));

  int64_t rc;
  bufferlist bl;

  rc = client->ll_preadv_pwritev(fh, iov_out, 2, 0, true, writefinish.get(),
                                 nullptr);
  ASSERT_EQ(rc, 0);
  ssize_t bytes_written = writefinish->wait();
  ASSERT_EQ(bytes_written, bytes_to_write);

  rc = client->ll_preadv_pwritev(fh, iov_in, 2, 0, false, readfinish.get(),
                                 &bl);
  ASSERT_EQ(rc, 0);
  ssize_t bytes_read = readfinish->wait();
  ASSERT_EQ(bytes_read, bytes_to_write);

  client->ll_release(fh);
  ASSERT_EQ(0, client->ll_unlink(root, filename, myperm));
}

TEST_F(TestClient, LlreadvLlwritevWriteOnlyFile) {
  /* Test async I/O with write only file*/
  int mypid = getpid();
  char filename[256];

  client->unmount();
  TearDown();
  SetUp();

  sprintf(filename, "test_llreadvllwritevwriteonlyfile%u", mypid);

  Inode *root, *file;
  root = client->get_root();
  ASSERT_NE(root, (Inode *)NULL);

  Fh *fh;
  struct ceph_statx stx;

  ASSERT_EQ(0, client->ll_createx(root, filename, 0666,
				  O_WRONLY | O_CREAT | O_TRUNC,
				  &file, &fh, &stx, 0, 0, myperm));

  char out_buf_0[] = "hello ";
  char out_buf_1[] = "world\n";
  struct iovec iov_out[2] = {
	  {out_buf_0, sizeof(out_buf_0)},
	  {out_buf_1, sizeof(out_buf_1)},
  };

  char in_buf_0[sizeof(out_buf_0)];
  char in_buf_1[sizeof(out_buf_1)];
  struct iovec iov_in[2] = {
	  {in_buf_0, sizeof(in_buf_0)},
	  {in_buf_1, sizeof(in_buf_1)},
  };

  ssize_t bytes_to_write = iov_out[0].iov_len + iov_out[1].iov_len;

  std::unique_ptr<C_SaferCond> writefinish = nullptr;
  std::unique_ptr<C_SaferCond> readfinish = nullptr;

  int64_t rc;
  bufferlist bl;

  writefinish.reset(new C_SaferCond("test-nonblocking-writefinish-write-only"));
  readfinish.reset(new C_SaferCond("test-nonblocking-readfinish-write-only"));

  rc = client->ll_preadv_pwritev(fh, iov_out, 2, 0, true, writefinish.get(),
                                 nullptr);
  ASSERT_EQ(rc, 0);
  ssize_t total_bytes_written = writefinish->wait();
  ASSERT_EQ(total_bytes_written, bytes_to_write);

  rc = client->ll_preadv_pwritev(fh, iov_in, 2, 0, false, readfinish.get(),
                                 &bl);
  ASSERT_EQ(rc, 0);
  ssize_t total_bytes_read = readfinish->wait();
  ASSERT_EQ(total_bytes_read, -CEPHFS_EBADF);
  ASSERT_EQ(bl.length(), 0);

  client->ll_release(fh);
  ASSERT_EQ(0, client->ll_unlink(root, filename, myperm));
}

TEST_F(TestClient, LlreadvLlwritevFsync) {
  /*Test two scenarios:
  a) async I/O with fsync enabled and sync metadata+data
  b) asynx I/O with fsync enabled and sync data only */

  int mypid = getpid();
  char filename[256];

  client->unmount();
  TearDown();
  SetUp();

  sprintf(filename, "test_llreadvllwritevfsyncfile%u", mypid);

  Inode *root, *file;
  root = client->get_root();
  ASSERT_NE(root, (Inode *)NULL);

  Fh *fh;
  struct ceph_statx stx;

  ASSERT_EQ(0, client->ll_createx(root, filename, 0666,
				  O_RDWR | O_CREAT | O_TRUNC,
				  &file, &fh, &stx, 0, 0, myperm));

  char out_buf_0[] = "hello ";
  char out_buf_1[] = "world b is much longer\n";

  char in_buf_0[sizeof(out_buf_0)];
  char in_buf_1[sizeof(out_buf_1)];

  struct iovec iov_out_fsync_sync_all[2] = {
	  {out_buf_0, sizeof(out_buf_0)},
	  {out_buf_1, sizeof(out_buf_1)},
  };

  struct iovec iov_in_fsync_sync_all[2] = {
	  {in_buf_0, sizeof(in_buf_0)},
	  {in_buf_1, sizeof(in_buf_1)},
  };

  struct iovec iov_out_dataonly_fysnc[2] = {
    {out_buf_0, sizeof(out_buf_0)},
    {out_buf_1, sizeof(out_buf_1)}
  };

  struct iovec iov_in_dataonly_fysnc[2] = {
    {in_buf_0, sizeof(in_buf_0)},
    {in_buf_1, sizeof(in_buf_1)}
  };

  std::unique_ptr<C_SaferCond> writefinish = nullptr;
  std::unique_ptr<C_SaferCond> readfinish = nullptr;

  // fsync - true, syncdataonly - false
  writefinish.reset(new C_SaferCond("test-nonblocking-writefinish-fsync-sync-all"));
  readfinish.reset(new C_SaferCond("test-nonblocking-readfinish-fsync-sync-all"));

  int64_t rc;
  bufferlist bl;
  ssize_t bytes_to_write = 0, bytes_written = 0, bytes_read = 0;

  bytes_to_write = iov_out_fsync_sync_all[0].iov_len
                   + iov_out_fsync_sync_all[1].iov_len;

  rc = client->ll_preadv_pwritev(fh, iov_out_fsync_sync_all, 2, 1000, true,
                                 writefinish.get(), nullptr, true, false);
  ASSERT_EQ(rc, 0);
  bytes_written = writefinish->wait();
  ASSERT_EQ(bytes_written, bytes_to_write);

  rc = client->ll_preadv_pwritev(fh, iov_in_fsync_sync_all, 2, 1000, false,
                                 readfinish.get(), &bl);
  ASSERT_EQ(rc, 0);
  bytes_read = readfinish->wait();
  ASSERT_EQ(bytes_read, bytes_to_write);

  copy_bufferlist_to_iovec(iov_in_fsync_sync_all, 2, &bl, bytes_read);
  for(int i = 0 ; i < 2; ++i) {
    ASSERT_EQ(strncmp((const char*)iov_in_fsync_sync_all[i].iov_base,
                      (const char*)iov_out_fsync_sync_all[i].iov_base,
                      iov_out_fsync_sync_all[i].iov_len), 0);
  }

  // fsync - true, syncdataonly - true
  writefinish.reset(new C_SaferCond("test-nonblocking-writefinish-fsync-syncdataonly"));
  readfinish.reset(new C_SaferCond("test-nonblocking-readfinish-fsync-syndataonly"));

  bytes_to_write = iov_out_dataonly_fysnc[0].iov_len
                   + iov_out_dataonly_fysnc[1].iov_len;

  rc = client->ll_preadv_pwritev(fh, iov_out_dataonly_fysnc, 2, 100,
                                 true, writefinish.get(), nullptr,
                                 true, true);
  ASSERT_EQ(rc, 0);
  bytes_written = writefinish->wait();
  ASSERT_EQ(bytes_written, bytes_to_write);

  rc = client->ll_preadv_pwritev(fh, iov_in_dataonly_fysnc, 2, 100,
                                 false, readfinish.get(), &bl);
  ASSERT_EQ(rc, 0);
  bytes_read = readfinish->wait();
  ASSERT_EQ(bytes_read, bytes_to_write);

  copy_bufferlist_to_iovec(iov_in_dataonly_fysnc, 2, &bl, rc);
  for(int i = 0; i < 2; ++i) {
    ASSERT_EQ(0, strncmp((const char*)iov_in_dataonly_fysnc[i].iov_base,
                         (const char*)iov_out_dataonly_fysnc[i].iov_base,
                         iov_out_dataonly_fysnc[i].iov_len));
  }

  client->ll_release(fh);
  ASSERT_EQ(0, client->ll_unlink(root, filename, myperm));
}

TEST_F(TestClient, LlreadvLlwritevBufferOverflow) {
  /* Provide empty read buffers to see how the function behaves
  when there is no space to store the data*/

  int mypid = getpid();
  char filename[256];

  client->unmount();
  TearDown();
  SetUp();

  sprintf(filename, "test_llreadvllwritevbufferoverflowfile%u", mypid);

  Inode *root, *file;
  root = client->get_root();
  ASSERT_NE(root, (Inode *)NULL);

  Fh *fh;
  struct ceph_statx stx;

  ASSERT_EQ(0, client->ll_createx(root, filename, 0666,
				  O_RDWR | O_CREAT | O_TRUNC,
				  &file, &fh, &stx, 0, 0, myperm));

  char out_buf_0[] = "hello ";
  char out_buf_1[] = "world\n";
  struct iovec iov_out[2] = {
	  {out_buf_0, sizeof(out_buf_0)},
	  {out_buf_1, sizeof(out_buf_1)},
  };

  char in_buf_0[0];
  char in_buf_1[0];
  struct iovec iov_in[2] = {
	  {in_buf_0, sizeof(in_buf_0)},
	  {in_buf_1, sizeof(in_buf_1)},
  };

  ssize_t bytes_to_write = iov_out[0].iov_len + iov_out[1].iov_len;

  std::unique_ptr<C_SaferCond> writefinish = nullptr;
  std::unique_ptr<C_SaferCond> readfinish = nullptr;

  writefinish.reset(new C_SaferCond("test-nonblocking-writefinish-buffer-overflow"));
  readfinish.reset(new C_SaferCond("test-nonblocking-readfinish-buffer-overflow"));

  int64_t rc;
  bufferlist bl;

  rc = client->ll_preadv_pwritev(fh, iov_out, 2, 0, true, writefinish.get(),
                                 nullptr);
  ASSERT_EQ(rc, 0);
  ssize_t bytes_written = writefinish->wait();
  ASSERT_EQ(bytes_written, bytes_to_write);

  rc = client->ll_preadv_pwritev(fh, iov_in, 2, 0, false, readfinish.get(),
                                 &bl);
  ASSERT_EQ(rc, 0);
  ssize_t bytes_read = readfinish->wait();
  ASSERT_EQ(bytes_read, 0);
  ASSERT_EQ(bl.length(), 0);

  client->ll_release(fh);
  ASSERT_EQ(0, client->ll_unlink(root, filename, myperm));
}

TEST_F(TestClient, LlreadvLlwritevDataPoolFull) {
  /* Test perfoming async I/O after filling the fs and make sure it handles
  the read/write gracefully */
  int mypid = getpid();
  char filename[256];

  client->unmount();
  TearDown();
  SetUp();

  sprintf(filename, "test_llreadvllwritevdatapoolfullfile%u", mypid);

  Inode *root, *file;
  root = client->get_root();
  ASSERT_NE(root, (Inode *)NULL);

  Fh *fh;
  struct ceph_statx stx;

  ASSERT_EQ(0, client->ll_createx(root, filename, 0666,
                                  O_RDWR | O_CREAT | O_TRUNC,
				                          &file, &fh, &stx, 0, 0, myperm));

  struct statvfs stbuf;
  int64_t rc;
  rc = client->ll_statfs(root, &stbuf, myperm);
  ASSERT_EQ(rc, 0);
  int64_t fs_available_space = stbuf.f_bfree * stbuf.f_bsize;
  ASSERT_GT(fs_available_space, 0);

  const int64_t BUFSIZE = 1024 * 1024 * 1024;
  int64_t bytes_written = 0, offset = 0;
  char* buf = new char[BUFSIZE];
  memset(buf, 0xCC, BUFSIZE);

  while(fs_available_space) {
    if (fs_available_space >= BUFSIZE) {
      bytes_written = client->ll_write(fh, offset, BUFSIZE, buf);
      ASSERT_GT(bytes_written, 0);
      offset += BUFSIZE;
      fs_available_space -= BUFSIZE;
    } else {
      small_buf = new char[fs_available_space];
      memset(small_buf, 0xDD, fs_available_space);
      bytes_written = client->ll_write(fh, offset, fs_available_space, small_buf);
      ASSERT_GT(bytes_written, 0);
      break;
    }
  }

  std::unique_ptr<C_SaferCond> writefinish = nullptr;
  writefinish.reset(new C_SaferCond("test-nonblocking-writefinish-datapool-full"));

  char* out_buf_0 = new char[BUFSIZE];
  memset(out_buf_0, 0xDD, BUFSIZE);
  char* out_buf_1 = new char[BUFSIZE];
  memset(out_buf_1, 0xFF, BUFSIZE);
  struct iovec iov_out[6] = {
    {out_buf_0, BUFSIZE},
    {out_buf_1, BUFSIZE}
  };

  bufferlist bl;

  rc = client->ll_preadv_pwritev(fh, iov_out, 2, 0, true, writefinish.get(),
                                 nullptr);
  ASSERT_EQ(rc, 0);
  bytes_written = writefinish->wait();
  ASSERT_EQ(bytes_written, -CEPHFS_ENOSPC);

  client->ll_release(fh);
  ASSERT_EQ(0, client->ll_unlink(root, filename, myperm));
  delete[] buf;
  delete[] out_buf_0;
  delete[] out_buf_1;
}
