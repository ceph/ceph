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

#ifndef CEPH_DEBUG_LEVEL_GUARD_H
#define CEPH_DEBUG_LEVEL_GUARD_H

#include <array>
#include <chrono>
#include <string>
#include <vector>

#include "common/ceph_time.h"
#include "common/subsys_types.h"
#include "log/SubsystemMap.h"

/// Tracks per-subsystem debug levels and identifies subsystems that have
/// been excessively high for longer than a timeout. Called periodically
/// from daemon tick functions.
///
/// On each check:
///  - If a subsystem's level is below threshold, its level is recorded
///    as the "safe" level to revert to.
///  - If a subsystem's level is at or above threshold and was not
///    previously elevated, the elevation timestamp is recorded.
///  - If a subsystem has been elevated longer than the timeout, it is
///    reported for revert.
class DebugLevelGuard {
public:
  DebugLevelGuard() {
    // Seed safe levels from compiled defaults. This is only a fallback
    // for a first-tick race: if a subsystem is already at/above threshold
    // on the very first check() call, we have no prior observation to
    // revert to. Under normal operation these get overwritten with the
    // live user config value on the first check() where the subsystem
    // is seen below threshold.
    constexpr auto defaults = ceph_subsys_get_as_array();
    for (std::size_t i = 0; i < defaults.size(); ++i) {
      m_state[i].safe_log = defaults[i].log_level;
      m_state[i].safe_gather = defaults[i].gather_level;
    }
  }

  struct RevertAction {
    unsigned subsys_id;
    std::string name;      // e.g. "debug_osd"
    int from_level;        // current log level (the one causing I/O)
    int to_log;            // safe log level to revert to
    int to_gather;         // safe gather level to revert to
  };

  /// Check all subsystems against the threshold and timeout. Returns
  /// a list of subsystems that should be reverted. Does NOT apply the
  /// revert — the caller is responsible for that.
  ///
  /// After a subsystem is reported for revert, its elevated flag is
  /// cleared. If the caller does not actually revert the level, it
  /// will be re-detected on the next call.
  std::vector<RevertAction> check(
    const ceph::logging::SubsystemMap& subsys,
    int threshold,
    std::chrono::seconds timeout);

  /// Count subsystems with log level at or above threshold.
  /// Returns {count, max_log_level}. For health metric reporting.
  static std::pair<uint32_t, uint32_t> count_elevated(
    const ceph::logging::SubsystemMap& subsys,
    int threshold)
  {
    if (threshold <= 0) return {0, 0};
    uint32_t count = 0;
    uint32_t max_level = 0;
    for (unsigned i = 0; i < subsys.get_num(); ++i) {
      auto lvl = subsys.get_log_level(i);
      if (lvl >= threshold) {
        count++;
        max_level = std::max(max_level, static_cast<uint32_t>(lvl));
      }
    }
    return {count, max_level};
  }

private:
  struct SubsysState {
    ceph::mono_clock::time_point elevated_since;
    uint8_t safe_log = 0;
    uint8_t safe_gather = 0;
    bool elevated = false;
  };

  std::array<SubsysState, ceph_subsys_get_num()> m_state;
};

#endif
