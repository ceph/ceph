// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "TestMgr.h"
#include "mgr/ThreadMonitor.h"

// Smoke test for the fixture: SetUp() constructs a ThreadMonitor against a real
// CephContext. The constructor registers a config observer and reads
// mgr_module_monitor_interval, so this verifies that setup succeeds before any
// other fixture-based test builds on it.
TEST_F(ThreadMonitorTestHelper, BasicCreation) {
  ASSERT_NE(thread_monitor, nullptr);
  ASSERT_NE(cct, nullptr);
}
