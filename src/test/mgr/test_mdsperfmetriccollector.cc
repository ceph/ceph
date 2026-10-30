// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "TestMgr.h"
#include "mgr/MDSPerfMetricCollector.h"
#include "mgr/MetricTypes.h"

TEST_F(MetricCollectorTestHelper, MDSPerfMetricCollectorBasicSetup) {
  ASSERT_NE(cct, nullptr);

  MockMetricListener listener;
  MDSPerfMetricCollector collector(listener);

  auto queries = collector.get_queries();
  ASSERT_TRUE(queries.empty());
  ASSERT_EQ(listener.update_count, 0);
}
