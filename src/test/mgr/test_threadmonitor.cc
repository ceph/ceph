// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <memory>
#include <vector>

#include "global/global_init.h"
#include "gtest/gtest.h"
#include "mgr/ThreadMonitor.h"

// Test fixture scaffolding cherry-picked from ceph/ceph PR #71237 (pending its
// merge); reconcile with that PR's src/test/mgr/TestMgr.h when it lands.
class ThreadMonitorTestHelper : public ::testing::Test {
public:
  static inline boost::intrusive_ptr<CephContext> cct;
  std::unique_ptr<ThreadMonitor> thread_monitor;

  static void SetUpTestSuite() {
    if (!cct) {
      std::vector<const char*> args = {"unittest_mgr_threadmonitor"};
      cct = global_init(
          nullptr, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
          CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
      common_init_finish(cct.get());
    }
  }

  void SetUp() override {
    thread_monitor = std::make_unique<ThreadMonitor>(cct.get());
  }

  void TearDown() override {
    thread_monitor.reset();
  }
};

TEST_F(ThreadMonitorTestHelper, BasicCreation) {
  ASSERT_NE(thread_monitor, nullptr);
  ASSERT_NE(cct, nullptr);
}

TEST_F(ThreadMonitorTestHelper, Construction) {
  ThreadMonitor tm(cct.get());
  // Should not crash
}
