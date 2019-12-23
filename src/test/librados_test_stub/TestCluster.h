// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_TEST_CLUSTER_H
#define CEPH_TEST_CLUSTER_H

#include "test/librados_test_stub/TestWatchNotify.h"
#include "include/common_fwd.h"

namespace librados {

class TestRadosClient;
class TestWatchNotify;

class TestCluster {
public:
  struct ObjectLocator {
    std::string nspace;
    std::string name;

    ObjectLocator(const std::string& nspace, const std::string& name)
      : nspace(nspace), name(name) {
    }

    bool operator<(const ObjectLocator& rhs) const {
      if (nspace != rhs.nspace) {
        return nspace < rhs.nspace;
      }
      return name < rhs.name;
    }
  };

  struct ObjectHandler {
    virtual ~ObjectHandler() {}

    virtual void handle_removed(TestRadosClient* test_rados_client) = 0;
  };

  TestCluster() : m_watch_notify(this) {
  }
  virtual ~TestCluster() {
  }

  virtual TestRadosClient *create_rados_client(CephContext *cct) = 0;

  virtual int register_object_handler(int64_t pool_id,
                                      const ObjectLocator& locator,
                                      ObjectHandler* object_handler) = 0;
  virtual void unregister_object_handler(int64_t pool_id,
                                         const ObjectLocator& locator,
                                         ObjectHandler* object_handler) = 0;

  TestWatchNotify *get_watch_notify() {
    return &m_watch_notify;
  }

protected:
  TestWatchNotify m_watch_notify;

};

} // namespace librados

#endif // CEPH_TEST_CLUSTER_H
