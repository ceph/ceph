// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "gtest/gtest.h"

#include "mon/HealthMonitor.h"
#include "mon/MonMap.h"

TEST(HealthMonitor, clears_removed_checks)
{
  MonMap monmap;
  health_check_map_t pending_health;
  pending_health.add("MON_DOWN", HEALTH_WARN, "1/3 mons down", 1);

  std::map<int, health_check_map_t> quorum_checks;
  health_check_map_t leader_checks;

  EXPECT_FALSE(health_monitor_detail::health_checks_are_current(
    pending_health, quorum_checks, leader_checks, monmap));

  health_monitor_detail::rebuild_health_checks(
    pending_health, quorum_checks, leader_checks, monmap);

  EXPECT_TRUE(pending_health.empty());
  EXPECT_TRUE(health_monitor_detail::health_checks_are_current(
    pending_health, quorum_checks, leader_checks, monmap));
}

TEST(HealthMonitor, rebuilds_current_checks)
{
  MonMap monmap;
  entity_addr_t addr;
  ASSERT_TRUE(addr.parse("127.0.0.1:6789"));
  entity_addrvec_t addrs;
  addrs.v.push_back(addr);
  monmap.add("a", addrs);

  health_check_map_t pending_health;
  pending_health.add("MON_DOWN", HEALTH_WARN, "1/3 mons down", 1);

  std::map<int, health_check_map_t> quorum_checks;
  auto& quorum_check = quorum_checks[0].add(
    "MON_DISK_LOW", HEALTH_WARN,
    "mon%plurals% %names% %isorare% low on disk space", 1);
  quorum_check.detail.push_back("mon.a has low disk space");
  health_check_map_t leader_checks;
  leader_checks.add("MON_CLOCK_SKEW", HEALTH_WARN, "clock skew", 1);

  health_monitor_detail::rebuild_health_checks(
    pending_health, quorum_checks, leader_checks, monmap);

  EXPECT_EQ(2u, pending_health.checks.size());
  EXPECT_EQ("mon a is low on disk space",
            pending_health.checks.at("MON_DISK_LOW").summary);
  EXPECT_TRUE(pending_health.checks.contains("MON_CLOCK_SKEW"));
  EXPECT_FALSE(pending_health.checks.contains("MON_DOWN"));
}
