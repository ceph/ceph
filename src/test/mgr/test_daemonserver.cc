// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "TestMgr.h"
#include "mgr/DaemonServer.h"

TEST_F(DaemonServerTestHelper, BasicSetup) {
  ASSERT_NE(mc, nullptr);
  ASSERT_NE(cs, nullptr);
  ASSERT_NE(py_registry, nullptr);
  ASSERT_NE(daemon_state_index, nullptr);
  ASSERT_NE(finisher, nullptr);
  ASSERT_NE(daemon_server, nullptr);
}
