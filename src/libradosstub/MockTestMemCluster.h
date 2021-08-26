// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/common_fwd.h"
#include "TestMemCluster.h"
#include "MockTestMemRadosClient.h"
#include "gmock/gmock.h"


namespace librados {

class TestRadosClient;

class MockTestMemCluster : public TestMemCluster {
public:
  MockTestMemCluster() {
    default_to_dispatch();
  }

  MOCK_METHOD1(create_rados_client, TestRadosClient*(CephContext*));
  MockTestMemRadosClient* do_create_rados_client(CephContext *cct) {
    return new ::testing::NiceMock<MockTestMemRadosClient>(cct, this);
  }

  void default_to_dispatch() {
    using namespace ::testing;
    ON_CALL(*this, create_rados_client(_)).WillByDefault(Invoke(this, &MockTestMemCluster::do_create_rados_client));
  }
};

} // namespace librados
