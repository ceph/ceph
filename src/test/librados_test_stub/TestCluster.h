// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_CLUSTER_H
#define CEPH_TEST_CLUSTER_H

#include "test/librados_test_stub/TestWatchNotify.h"

class CephContext;

namespace librados {

class TestRadosClient;
class TestWatchNotify;

class TestCluster {
public:
  virtual ~TestCluster() {
  }

  virtual TestRadosClient *create_rados_client(CephContext *cct) = 0;

  TestWatchNotify *get_watch_notify() {
    return &m_watch_notify;
  }

protected:
  TestWatchNotify m_watch_notify;

};

} // namespace librados

#endif // CEPH_TEST_CLUSTER_H
