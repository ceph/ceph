// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "TestMgr.h"
#include "mgr/MetricCollector.h"
#include "mgr/OSDPerfMetricCollector.h"
#include "mgr/MDSPerfMetricCollector.h"

TEST_F(MetricCollectorTestHelper, BasicSetup) {
  ASSERT_NE(cct, nullptr);

  MockMetricListener osd_listener;
  OSDPerfMetricCollector osd_collector(osd_listener);
  ASSERT_EQ(osd_listener.update_count, 0);

  MockMetricListener mds_listener;
  MDSPerfMetricCollector mds_collector(mds_listener);
  ASSERT_EQ(mds_listener.update_count, 0);
}
