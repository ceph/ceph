// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "TestMgr.h"
#include "mgr/OSDPerfMetricCollector.h"
#include "mgr/MetricTypes.h"

TEST_F(MetricCollectorTestHelper, OSDPerfMetricCollectorBasicSetup) {
  ASSERT_NE(cct, nullptr);

  MockMetricListener listener;
  OSDPerfMetricCollector collector(listener);

  auto queries = collector.get_queries();
  ASSERT_TRUE(queries.empty());
}
