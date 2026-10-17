// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "TestMgr.h"
#include "mgr/DaemonHealthMetricCollector.h"
#include "mgr/DaemonHealthMetric.h"
#include "mgr/DaemonKey.h"
#include "mon/health_check.h"
#include <memory>

TEST_F(MetricCollectorTestHelper, DaemonHealthMetricCollectorBasicSetup) {
  ASSERT_NE(cct, nullptr);

  auto slow_ops_collector = DaemonHealthMetricCollector::create(daemon_metric::SLOW_OPS);
  ASSERT_NE(slow_ops_collector, nullptr);

  auto pending_pgs_collector = DaemonHealthMetricCollector::create(daemon_metric::PENDING_CREATING_PGS);
  ASSERT_NE(pending_pgs_collector, nullptr);

  auto none_collector = DaemonHealthMetricCollector::create(daemon_metric::NONE);
  ASSERT_EQ(none_collector, nullptr);
}
