// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "gtest/gtest.h"

#include <list>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "mgr/DaemonHealthMetric.h"
#include "mgr/DaemonHealthMetricCollector.h"
#include "mgr/DaemonKey.h"
#include "mon/health_check.h"

namespace {

using Reports =
  std::vector<std::pair<DaemonKey, std::vector<DaemonHealthMetric>>>;

// Mirror DaemonServer::send_report(): one collector per metric type, fed with
// every daemon's metrics, then summarized into a single health_check_map_t.
health_check_map_t collect(const Reports& reports)
{
  std::map<daemon_metric,
           std::unique_ptr<DaemonHealthMetricCollector>> accumulated;
  for (const auto& [key, metrics] : reports) {
    for (const auto& metric : metrics) {
      auto acc = accumulated.find(metric.get_type());
      if (acc == accumulated.end()) {
        auto collector = DaemonHealthMetricCollector::create(metric.get_type());
        if (!collector) {
          continue;
        }
        acc = accumulated.emplace(metric.get_type(),
                                  std::move(collector)).first;
      }
      acc->second->update(key, metric);
    }
  }
  health_check_map_t checks;
  for (const auto& [type, collector] : accumulated) {
    collector->summarize(checks);
  }
  return checks;
}

DaemonKey osd(int id)
{
  return DaemonKey{"osd", std::to_string(id)};
}

} // anonymous namespace

TEST(DaemonHealthMetricCollector, CreateUnknownType)
{
  EXPECT_FALSE(DaemonHealthMetricCollector::create(daemon_metric::NONE));
  EXPECT_TRUE(DaemonHealthMetricCollector::create(daemon_metric::SLOW_OPS));
  EXPECT_TRUE(
    DaemonHealthMetricCollector::create(daemon_metric::PENDING_CREATING_PGS));
  EXPECT_TRUE(
    DaemonHealthMetricCollector::create(daemon_metric::HIGH_DEBUG_LEVEL));
}

TEST(DaemonHealthMetricCollector, NoChecksWhenAllZero)
{
  auto checks = collect({
    {osd(0), {{daemon_metric::SLOW_OPS, 0u, 0u},
              {daemon_metric::PENDING_CREATING_PGS, 0ull},
              {daemon_metric::HIGH_DEBUG_LEVEL, 0u, 0u}}},
    {osd(1), {{daemon_metric::SLOW_OPS, 0u, 0u}}},
  });
  EXPECT_TRUE(checks.empty());
}

TEST(DaemonHealthMetricCollector, SlowOpsSingleDaemon)
{
  auto checks = collect({
    {osd(0), {{daemon_metric::SLOW_OPS, 0u, 0u}}},
    {osd(3), {{daemon_metric::SLOW_OPS, 5u, 12u}}},
  });
  ASSERT_EQ(checks.checks.size(), 1u);
  const auto& check = checks.checks.at("SLOW_OPS");
  EXPECT_EQ(check.severity, HEALTH_WARN);
  EXPECT_EQ(check.count, 1);
  EXPECT_EQ(check.summary,
            "5 slow ops, oldest one blocked for 12 sec, osd.3 has slow ops");
  EXPECT_TRUE(check.detail.empty());
}

TEST(DaemonHealthMetricCollector, SlowOpsSeveralDaemons)
{
  // mgr/prometheus parses this summary to export the number of slow ops, so
  // the format is part of an interface.
  auto checks = collect({
    {osd(0), {{daemon_metric::SLOW_OPS, 40u, 7u}}},
    {osd(1), {{daemon_metric::SLOW_OPS, 0u, 0u}}},
    {osd(3), {{daemon_metric::SLOW_OPS, 2u, 12u}}},
    {DaemonKey{"mds", "a"}, {{daemon_metric::SLOW_OPS, 1u, 3u}}},
  });
  ASSERT_EQ(checks.checks.size(), 1u);
  EXPECT_EQ(checks.checks.at("SLOW_OPS").summary,
            "43 slow ops, oldest one blocked for 12 sec, "
            "daemons [osd.0,osd.3,mds.a] have slow ops.");
}

TEST(DaemonHealthMetricCollector, SlowOpsListsAtMostTenDaemons)
{
  Reports reports;
  for (int i = 0; i < 12; i++) {
    reports.push_back({osd(i), {{daemon_metric::SLOW_OPS, 1u, 1u}}});
  }
  auto checks = collect(reports);
  EXPECT_EQ(checks.checks.at("SLOW_OPS").summary,
            "12 slow ops, oldest one blocked for 1 sec, daemons "
            "[osd.0,osd.1,osd.2,osd.3,osd.4,osd.5,osd.6,osd.7,osd.8,osd.9]"
            "... have slow ops.");
}

TEST(DaemonHealthMetricCollector, PendingCreatingPGs)
{
  auto checks = collect({
    {osd(0), {{daemon_metric::PENDING_CREATING_PGS, 3ull}}},
    {osd(1), {{daemon_metric::PENDING_CREATING_PGS, 0ull}}},
  });
  ASSERT_EQ(checks.checks.size(), 1u);
  const auto& one = checks.checks.at("PENDING_CREATING_PGS");
  EXPECT_EQ(one.severity, HEALTH_WARN);
  EXPECT_EQ(one.summary, "3 PGs pending on creation");
  EXPECT_EQ(one.detail,
            std::list<std::string>{"osd.0 has pending PGs"});

  checks = collect({
    {osd(0), {{daemon_metric::PENDING_CREATING_PGS, 3ull}}},
    {osd(2), {{daemon_metric::PENDING_CREATING_PGS, 4ull}}},
  });
  const auto& many = checks.checks.at("PENDING_CREATING_PGS");
  EXPECT_EQ(many.summary, "7 PGs pending on creation");
  EXPECT_EQ(many.detail,
            std::list<std::string>{"osds [osd.0,osd.2] have pending PGs."});
}

TEST(DaemonHealthMetricCollector, HighDebugLevel)
{
  auto checks = collect({
    {osd(0), {{daemon_metric::HIGH_DEBUG_LEVEL, 2u, 20u}}},
    {osd(1), {{daemon_metric::HIGH_DEBUG_LEVEL, 0u, 0u}}},
    {DaemonKey{"mon", "a"}, {{daemon_metric::HIGH_DEBUG_LEVEL, 1u, 10u}}},
  });
  ASSERT_EQ(checks.checks.size(), 1u);
  const auto& check = checks.checks.at("HIGH_DEBUG_LEVEL");
  EXPECT_EQ(check.severity, HEALTH_WARN);
  EXPECT_EQ(check.summary,
            "3 subsystems across 2 daemon(s) have high debug levels "
            "(max 20): [osd.0,mon.a]");
}

TEST(DaemonHealthMetricCollector, MetricsOfSeveralTypes)
{
  // Each collector only picks up its own metric type, so a daemon reporting
  // several metrics contributes to each check independently.
  auto checks = collect({
    {osd(0), {{daemon_metric::SLOW_OPS, 4u, 30u},
              {daemon_metric::PENDING_CREATING_PGS, 2ull}}},
    {osd(1), {{daemon_metric::PENDING_CREATING_PGS, 1ull},
              {daemon_metric::NONE, 9ull}}},
  });
  ASSERT_EQ(checks.checks.size(), 2u);
  EXPECT_EQ(checks.checks.at("SLOW_OPS").summary,
            "4 slow ops, oldest one blocked for 30 sec, osd.0 has slow ops");
  EXPECT_EQ(checks.checks.at("PENDING_CREATING_PGS").summary,
            "3 PGs pending on creation");
}
