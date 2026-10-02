// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "gtest/gtest.h"

#include "common/ceph_context.h"
#include "common/TrackedOp.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "messages/MCommand.h"
#include "mgr/MgrOpRequest.h"

// The MgrOpRequest test harness does not yet exist on the base branch. The
// fixture below (and the BasicSetup test) is cherry-picked from the pending
// scaffolding in ceph/ceph PR #71237 (MgrOpRequestTestHelper). It is kept
// self-contained here as a temporary measure and should be reconciled with
// src/test/mgr/TestMgr.h when that PR lands. #81341 cherry-picks the same
// scaffolding; if both land, the scaffolding is added once.
class MgrOpRequestTestHelper : public ::testing::Test {
public:
  static inline boost::intrusive_ptr<CephContext> cct;
  std::unique_ptr<OpTracker> tracker;

  static void SetUpTestSuite() {
    if (!cct) {
      std::vector<const char*> args = {"unittest_mgr_mgroprequest"};
      cct = global_init(
          nullptr, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
          CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
      common_init_finish(cct.get());
    }
  }

  void SetUp() override {
    tracker = std::make_unique<OpTracker>(cct.get(), true, 1);
  }

  void TearDown() override {
    tracker.reset();
  }
};

TEST_F(MgrOpRequestTestHelper, BasicSetup) {
  auto msg = ceph::make_message<MCommand>();
  msg->set_tid(123);

  auto req = tracker->create_request<MgrOpRequest>(msg);
  ASSERT_TRUE(req);
  ASSERT_EQ(req->get_req(), msg);
}
