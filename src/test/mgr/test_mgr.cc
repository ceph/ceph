// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "TestMgr.h"
#include "mgr/Mgr.h"

TEST_F(MgrTestHelper, ConstructAndDestruct) {
  auto mgr = std::make_unique<Mgr>(
      mc.get(), mgr_map, py_registry.get(), messenger.get(), objecter.get(),
      clog, audit_clog);
  ASSERT_NE(mgr, nullptr);
  EXPECT_FALSE(mgr->is_initialized());
  EXPECT_FALSE(mgr->exceeded_initialization_expiration());
  mgr.reset();
}
