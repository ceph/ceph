// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2026 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include <chrono>
#include <thread>

#include <gtest/gtest.h>

#include "common/ceph_context.h"
#include "common/debug_level_guard.h"

using namespace std::literals;

class DebugLevelGuardTest : public ::testing::Test {
protected:
  DebugLevelGuard guard;
  boost::intrusive_ptr<CephContext> cct;

  void SetUp() override {
    cct.reset(new CephContext(CEPH_ENTITY_TYPE_OSD), false);
  }

  void set_debug(const std::string& subsys, const std::string& val) {
    cct->_conf.set_val("debug_" + subsys, val);
    cct->_conf.apply_changes(nullptr);
  }

  int get_log_level(unsigned subsys) {
    return cct->_conf->subsys.get_log_level(subsys);
  }

  int get_gather_level(unsigned subsys) {
    return cct->_conf->subsys.get_gather_level(subsys);
  }

  // Apply revert actions to config (simulates what daemon tick does)
  void apply_reverts(const std::vector<DebugLevelGuard::RevertAction>& actions) {
    for (auto& a : actions) {
      auto val = std::to_string(a.to_log) + "/" + std::to_string(a.to_gather);
      cct->_conf.set_val(a.name, val);
    }
    if (!actions.empty()) {
      cct->_conf.apply_changes(nullptr);
    }
  }
};

TEST_F(DebugLevelGuardTest, DisabledWhenThresholdZero) {
  set_debug("osd", "20");
  auto actions = guard.check(cct->_conf->subsys, 0, 1s);
  EXPECT_TRUE(actions.empty());
  EXPECT_EQ(get_log_level(ceph_subsys_osd), 20);
}

TEST_F(DebugLevelGuardTest, BelowThresholdIgnored) {
  set_debug("osd", "4");
  auto actions = guard.check(cct->_conf->subsys, 5, 1s);
  EXPECT_TRUE(actions.empty());
  EXPECT_EQ(get_log_level(ceph_subsys_osd), 4);
}

TEST_F(DebugLevelGuardTest, AtThresholdDetected) {
  set_debug("osd", "5");
  // First check marks as elevated, no revert yet
  auto actions = guard.check(cct->_conf->subsys, 5, 1s);
  EXPECT_TRUE(actions.empty());
  // Second check after timeout should trigger revert
  std::this_thread::sleep_for(1100ms);
  actions = guard.check(cct->_conf->subsys, 5, 1s);
  EXPECT_EQ(actions.size(), 1u);
}

TEST_F(DebugLevelGuardTest, TimeoutZeroMeansWarnOnly) {
  set_debug("osd", "20");
  // First check marks elevated
  guard.check(cct->_conf->subsys, 10, 0s);
  // Second check with timeout=0 should NOT revert (warn only)
  std::this_thread::sleep_for(100ms);
  auto actions = guard.check(cct->_conf->subsys, 10, 0s);
  EXPECT_TRUE(actions.empty());
  // Level should still be 20
  EXPECT_EQ(get_log_level(ceph_subsys_osd), 20);
}

TEST_F(DebugLevelGuardTest, RevertsAfterTimeout) {
  set_debug("osd", "3/5");

  // First call records safe levels at 3/5
  guard.check(cct->_conf->subsys, 10, 1s);
  EXPECT_EQ(get_log_level(ceph_subsys_osd), 3);
  EXPECT_EQ(get_gather_level(ceph_subsys_osd), 5);

  // Elevate
  set_debug("osd", "20");
  // First check marks elevated
  guard.check(cct->_conf->subsys, 10, 1s);
  EXPECT_EQ(get_log_level(ceph_subsys_osd), 20);

  // Wait for timeout
  std::this_thread::sleep_for(1100ms);

  auto actions = guard.check(cct->_conf->subsys, 10, 1s);
  ASSERT_EQ(actions.size(), 1u);
  EXPECT_EQ(actions[0].name, "debug_osd");
  EXPECT_EQ(actions[0].from_level, 20);
  EXPECT_EQ(actions[0].to_log, 3);
  EXPECT_EQ(actions[0].to_gather, 5);

  // Apply and verify
  apply_reverts(actions);
  EXPECT_EQ(get_log_level(ceph_subsys_osd), 3);
  EXPECT_EQ(get_gather_level(ceph_subsys_osd), 5);
}

TEST_F(DebugLevelGuardTest, RestoresPreviousNotDefault) {
  // Set a non-default safe level first
  set_debug("osd", "2/7");
  guard.check(cct->_conf->subsys, 10, 1s);

  // Elevate
  set_debug("osd", "20");
  guard.check(cct->_conf->subsys, 10, 1s);

  // Should trigger revert after timeout
  std::this_thread::sleep_for(1100ms);
  auto actions = guard.check(cct->_conf->subsys, 10, 1s);
  ASSERT_EQ(actions.size(), 1u);

  apply_reverts(actions);
  // Should restore to 2/7, not compiled default
  EXPECT_EQ(get_log_level(ceph_subsys_osd), 2);
  EXPECT_EQ(get_gather_level(ceph_subsys_osd), 7);
}

TEST_F(DebugLevelGuardTest, ManualLowerCancelsRevert) {
  set_debug("osd", "1");
  guard.check(cct->_conf->subsys, 10, 2s);

  // Elevate
  set_debug("osd", "20");
  guard.check(cct->_conf->subsys, 10, 2s);

  // Manually lower before timeout
  set_debug("osd", "3");
  auto actions = guard.check(cct->_conf->subsys, 10, 2s);
  EXPECT_TRUE(actions.empty());

  // Should stay at 3 even after waiting
  std::this_thread::sleep_for(2100ms);
  actions = guard.check(cct->_conf->subsys, 10, 2s);
  EXPECT_TRUE(actions.empty());
  EXPECT_EQ(get_log_level(ceph_subsys_osd), 3);
}

TEST_F(DebugLevelGuardTest, ReElevationResetsTimer) {
  set_debug("osd", "1");
  guard.check(cct->_conf->subsys, 10, 2s);

  set_debug("osd", "20");
  guard.check(cct->_conf->subsys, 10, 2s);

  // Wait 1.5s (less than timeout)
  std::this_thread::sleep_for(1500ms);

  // Lower then re-elevate — should reset timer
  set_debug("osd", "3");
  guard.check(cct->_conf->subsys, 10, 2s);
  set_debug("osd", "25");
  guard.check(cct->_conf->subsys, 10, 2s);

  // Wait 1s — not enough since timer was reset
  std::this_thread::sleep_for(1000ms);
  auto actions = guard.check(cct->_conf->subsys, 10, 2s);
  EXPECT_TRUE(actions.empty());

  // Wait the remaining time
  std::this_thread::sleep_for(1100ms);
  actions = guard.check(cct->_conf->subsys, 10, 2s);
  EXPECT_EQ(actions.size(), 1u);
}

TEST_F(DebugLevelGuardTest, MultipleSubsystemsIndependent) {
  set_debug("osd", "1");
  set_debug("ms", "1");
  guard.check(cct->_conf->subsys, 10, 1s);

  // Elevate osd first
  set_debug("osd", "20");
  guard.check(cct->_conf->subsys, 10, 1s);

  // Wait 0.5s, then elevate ms
  std::this_thread::sleep_for(500ms);
  set_debug("ms", "15");
  guard.check(cct->_conf->subsys, 10, 1s);

  // Wait 0.6s more — osd should trigger (1.1s total), ms should not (0.6s)
  std::this_thread::sleep_for(600ms);
  auto actions = guard.check(cct->_conf->subsys, 10, 1s);

  bool osd_found = false;
  bool ms_found = false;
  for (auto& a : actions) {
    if (a.name == "debug_osd") osd_found = true;
    if (a.name == "debug_ms") ms_found = true;
  }
  EXPECT_TRUE(osd_found);
  EXPECT_FALSE(ms_found);

  // ms should still be elevated
  EXPECT_EQ(get_log_level(ceph_subsys_ms), 15);

  // Apply osd revert
  apply_reverts(actions);

  // Wait for ms to timeout too
  std::this_thread::sleep_for(500ms);
  actions = guard.check(cct->_conf->subsys, 10, 1s);
  ms_found = false;
  for (auto& a : actions) {
    if (a.name == "debug_ms") ms_found = true;
  }
  EXPECT_TRUE(ms_found);

  apply_reverts(actions);
  EXPECT_EQ(get_log_level(ceph_subsys_ms), 1);
}
