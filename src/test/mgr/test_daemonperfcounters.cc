// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "TestMgr.h"
#include "mgr/DaemonPerfCounters.h"
#include "mgr/PerfCounterInstance.h"
#include "common/perf_counters.h"
#include "messages/MMgrReport.h"

TEST_F(DaemonPerfCountersTestHelper, BasicSetup) {
  ASSERT_NE(perf_counters, nullptr);
  ASSERT_TRUE(types.empty());
  ASSERT_TRUE(perf_counters->instances.empty());
}
