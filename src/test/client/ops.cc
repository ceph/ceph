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
