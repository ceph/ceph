// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRADOS_MOCK_TEST_MEM_CLUSTER_H
#define LIBRADOS_MOCK_TEST_MEM_CLUSTER_H

#include "include/common_fwd.h"
#include "test/librados_test_stub/TestMemCluster.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
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

#endif // LIBRADOS_MOCK_TEST_MEM_CLUSTER_H
