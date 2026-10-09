// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
#include "TestClient.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "gtest/gtest-spi.h"
#include "gmock/gmock-matchers.h"
#include "gmock/gmock-more-matchers.h"

TEST_F(TestClient, CheckDummyOP) {
  ASSERT_EQ(client->check_dummy_op(myperm), -EOPNOTSUPP);
}

TEST_F(TestClient, CheckUnknownSessionOp) {
  ASSERT_EQ(client->send_unknown_session_op(-1), 0);
  sleep(5);
  ASSERT_EQ(client->check_client_blocklisted(), true);
}

TEST_F(TestClient, CheckZeroReclaimFlag) {
  ASSERT_EQ(client->check_unknown_reclaim_flag(0), true);
}
TEST_F(TestClient, CheckUnknownReclaimFlag) {
  ASSERT_EQ(client->check_unknown_reclaim_flag(2), true);
}
TEST_F(TestClient, CheckNegativeReclaimFlagUnmasked) {
  ASSERT_EQ(client->check_unknown_reclaim_flag(-1 & ~MClientReclaim::FLAG_FINISH), true);
}
TEST_F(TestClient, CheckNegativeReclaimFlag) {
  ASSERT_EQ(client->check_unknown_reclaim_flag(-1), true);
}

TEST_F(TestClient, SyncFsReportsErrorAfterClose) {
  // A writeback error that comes back after close() has no open Fh to
  // land on; sync_fs() must still report it, once.
  char filename[256];
  sprintf(filename, "test_syncfs_err_after_close%u", getpid());

  int fd = client->open(filename, O_CREAT | O_WRONLY | O_TRUNC, myperm, 0644);
  ASSERT_LE(0, fd);
  ASSERT_EQ(5, client->write(fd, "hello", 5, 0));
  ASSERT_EQ(0, client->close(fd));

  ASSERT_EQ(0, client->inject_async_err(filename, -EIO, myperm));
  ASSERT_EQ(-EIO, client->sync_fs());
  ASSERT_EQ(0, client->sync_fs());

  ASSERT_EQ(0, client->unlink(filename, myperm));
}

TEST_F(TestClient, SyncFsReportsErrorWhileOpen) {
  // An error that an open Fh will see on close() is reported by sync_fs()
  // as well, and close() still gets its copy.
  char filename[256];
  sprintf(filename, "test_syncfs_err_while_open%u", getpid());

  int fd = client->open(filename, O_CREAT | O_WRONLY | O_TRUNC, myperm, 0644);
  ASSERT_LE(0, fd);
  ASSERT_EQ(5, client->write(fd, "hello", 5, 0));

  ASSERT_EQ(0, client->inject_async_err(filename, -ENOSPC, myperm));
  ASSERT_EQ(-ENOSPC, client->sync_fs());
  ASSERT_EQ(0, client->sync_fs());
  ASSERT_EQ(-ENOSPC, client->close(fd));

  ASSERT_EQ(0, client->unlink(filename, myperm));
}
