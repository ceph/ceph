// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef LIBRADOS_MOCK_TEST_MEM_CLUSTER_H
#define LIBRADOS_MOCK_TEST_MEM_CLUSTER_H

#include "test/librados_test_stub/TestMemCluster.h"
#include "test/librados_test_stub/MockTestMemRadosClient.h"
#include "gmock/gmock.h"

struct CephContext;

namespace librados {

class TestRadosClient;

class MockTestMemCluster : public TestCluster {
public:
  TestRadosClient *create_rados_client(CephContext *cct) override {
    return new ::testing::NiceMock<librados::MockTestMemRadosClient>(
      cct, &m_mem_cluster);
  }

private:
  TestMemCluster m_mem_cluster;

};

} // namespace librados

#endif // LIBRADOS_MOCK_TEST_MEM_CLUSTER_H
