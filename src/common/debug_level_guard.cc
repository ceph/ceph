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

#include "common/debug_level_guard.h"

#include <fmt/format.h>

std::vector<DebugLevelGuard::RevertAction>
DebugLevelGuard::check(
  const ceph::logging::SubsystemMap& subsys,
  int threshold,
  std::chrono::seconds timeout)
{
  std::vector<RevertAction> actions;

  if (threshold <= 0) {
    return actions;
  }

  auto now = ceph::mono_clock::now();
  auto num = subsys.get_num();

  for (unsigned i = 0; i < num; ++i) {
    int log_level = subsys.get_log_level(i);
    int gather_level = subsys.get_gather_level(i);

    // Only check the log level, not the gather level. High gather
    // levels are safe because those messages stay in memory for crash
    // dumps and don't cause the disk I/O floods this guard prevents.
    if (log_level < threshold) {
      m_state[i].safe_log = log_level;
      m_state[i].safe_gather = gather_level;
      m_state[i].elevated = false;
    } else if (!m_state[i].elevated) {
      m_state[i].elevated = true;
      m_state[i].elevated_since = now;
    } else {
      auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
	now - m_state[i].elevated_since);
      if (timeout.count() > 0 && elapsed >= timeout) {
	actions.push_back({
	  i,
	  // Config option name is "debug_" + subsystem name; this is the
	  // string the caller passes to set_val() to revert the level.
	  fmt::format("debug_{}", subsys.get_name(i)),
	  log_level,
	  m_state[i].safe_log,
	  m_state[i].safe_gather
	});
	m_state[i].elevated = false;
      }
    }
  }

  return actions;
}
