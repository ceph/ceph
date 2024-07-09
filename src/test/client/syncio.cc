// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

TEST_F(TestClient, LlreadvLlwritevInvalidFileHandleSync) {
    /* Test provding null or invalid file handle returns an error
    as expected*/
    Fh *fh_null = NULL;
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

    int64_t rc;

    rc = client->ll_writev(fh_null, iov_out, 2, 0);
    ASSERT_EQ(rc, -EBADF);

    rc = client->ll_readv(fh_null, iov_in, 2, 0);
    ASSERT_EQ(rc, -EBADF);

    // test after closing the file handle
    int mypid = getpid();
    char filename[256];

    client->unmount();
    TearDown();
    SetUp();

    sprintf(filename, "test_llreadvllwritevinvalidfhfile%u", mypid);

    Inode *root, *file;
    root = client->get_root();
    ASSERT_NE(root, (Inode *)NULL);

    Fh *fh;
    struct ceph_statx stx;

    ASSERT_EQ(0, client->ll_createx(root, filename, 0666,
                    O_RDWR | O_CREAT | O_TRUNC,
                    &file, &fh, &stx, 0, 0, myperm));

    client->ll_release(fh);
    ASSERT_EQ(0, client->ll_unlink(root, filename, myperm));

    rc = client->ll_writev(fh, iov_out, 2, 0);
    ASSERT_EQ(rc, -EBADF);

    rc = client->ll_readv(fh, iov_in, 2, 0);
    ASSERT_EQ(rc, -EBADF);
}

TEST_F(TestClient, LlreadvLlwritevLargeBuffersSync) {
  /* Test that sync I/O code paths handle large buffers (total len >= 4GiB)*/
  int mypid = getpid();
  char filename[256];

  client->unmount();
  TearDown();
  SetUp();

  sprintf(filename, "test_llreadvllwritevlargebufferssync%u", mypid);

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
  const size_t BUFSIZE = (size_t)INT_MAX + 1;
  rc = client->ll_statfs(root, &stbuf, myperm);
  ASSERT_EQ(rc, 0);
  int64_t fs_available_space = stbuf.f_bfree * stbuf.f_bsize;
  ASSERT_GT(fs_available_space, BUFSIZE * 2);

  auto out_buf_0 = std::make_unique<char[]>(BUFSIZE);
  memset(out_buf_0.get(), 0xDD, BUFSIZE);
  auto out_buf_1 = std::make_unique<char[]>(BUFSIZE);
  memset(out_buf_1.get(), 0xFF, BUFSIZE);

  struct iovec iov_out[2] = {
    {out_buf_0.get(), BUFSIZE},
    {out_buf_1.get(), BUFSIZE}
  };

  bufferlist bl;
  auto in_buf_0 = std::make_unique<char[]>(BUFSIZE);
  auto in_buf_1 = std::make_unique<char[]>(BUFSIZE);

  struct iovec iov_in[2] = {
    {in_buf_0.get(), BUFSIZE},
    {in_buf_1.get(), BUFSIZE}
  };

  int maxio_size = INT_MAX;
  if (fse.encrypted) {
    maxio_size = FSCRYPT_MAXIO_SIZE;
  }


  rc = client->ll_writev(fh, iov_out, 2, 0);
  // total write length is clamped to INT_MAX in write paths
  ASSERT_EQ(rc, maxio_size);

  rc = client->ll_readv(fh, iov_in, 2, 0);
  // total write length is clamped to INT_MAX in write paths
  ASSERT_EQ(rc, maxio_size);

  client->ll_release(fh);
  ASSERT_EQ(0, client->ll_unlink(root, filename, myperm));
}

TEST_F(TestClient, SyncIOLlreadvLlwritevSizeOutOfBound) {
    /* sync/async i/o system calls can transfer at most INT_MAX 
    (that is 2147483647 bytes which is ~2GiB) bytes. Therefore, 
    test that sync'd read/write calls returned bytes do not
    exceed this limit.*/

    Inode *root = NULL;
    root = client->get_root();
    ASSERT_NE(root, (Inode *)NULL);

    struct statvfs stbuf;
    ASSERT_EQ(client->ll_statfs(root, &stbuf, myperm), 0);
    size_t initial_available_space = stbuf.f_bfree * stbuf.f_bsize;
    ASSERT_GT(initial_available_space, 0);

    int mypid = getpid();
    char fname[256];
    sprintf(fname, "test_synciollreadvllwritevsizeoutofbound%u", mypid);
    Inode *file = NULL;
    Fh *fh = NULL;
    struct ceph_statx stx;
    ASSERT_EQ(0, client->ll_createx(root, fname, 0666,
        O_RDWR | O_CREAT | O_TRUNC, &file, &fh, &stx, 0, 0, myperm));

    int64_t bytes_written = 0, bytes_read = 0;
    const size_t READ_WRITE_LIMIT = INT_MAX;

    const size_t BUFSIZE = 1024 * 1024 * 1024;
    auto out_buf_0 = std::make_unique<char[]>(BUFSIZE);
    memset(out_buf_0.get(), 'a', BUFSIZE);
    auto out_buf_1 = std::make_unique<char[]>(BUFSIZE);
    memset(out_buf_1.get(), 'b', BUFSIZE);
    auto out_buf_2 = std::make_unique<char[]>(BUFSIZE);
    memset(out_buf_2.get(), 'c', BUFSIZE);
  
    struct iovec iov_out_over_limit[3] = {
        {out_buf_0.get(), BUFSIZE},
        {out_buf_1.get(), BUFSIZE},
        {out_buf_2.get(), BUFSIZE}
    };

    auto in_buf_0 = std::make_unique<char[]>(BUFSIZE);
    auto in_buf_1 = std::make_unique<char[]>(BUFSIZE);
    auto in_buf_2 = std::make_unique<char[]>(BUFSIZE);

    struct iovec iov_in_over_limit[3] = {
      {in_buf_0.get(), BUFSIZE},
      {in_buf_1.get(), BUFSIZE},
      {in_buf_2.get(), BUFSIZE}
    };

    bytes_written = client->ll_writev(fh, iov_out_over_limit, 3, 0);
    ASSERT_EQ(bytes_written, READ_WRITE_LIMIT);

    // flush the buffers and wait for 10 secs since buffers are huge
    ASSERT_EQ(client->sync_fs(), 0);
    sleep(10);

    // make sure the bytes_written corresponds with the available space
    struct statvfs stbuf1;
    ASSERT_EQ(client->ll_statfs(root, &stbuf1, myperm), 0);
    size_t available_space_post_io = stbuf1.f_bfree * stbuf1.f_bsize;
    ASSERT_EQ((initial_available_space - available_space_post_io - 1),
              READ_WRITE_LIMIT);

    bytes_read = client->ll_readv(fh, iov_in_over_limit, 3, 0);
    ASSERT_EQ(bytes_read, READ_WRITE_LIMIT);

    client->ll_release(fh);
    ASSERT_EQ(client->ll_unlink(root, fname, myperm), 0);
}
