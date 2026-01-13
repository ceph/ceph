// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "nvmeof/NVMeofGwUtils.h"

void determine_subsystem_changes(const BeaconSubsystems& old_subsystems,
                                BeaconSubsystems& new_subsystems) {
  BeaconSubsystems result;

  // for each subsystem in new_subsystems, check if it's added or changed
  for (const auto& new_sub : new_subsystems) {
    auto old_it = std::find_if(old_subsystems.begin(), old_subsystems.end(),
                              [&](const BeaconSubsystem& s) { return s.nqn == new_sub.nqn; });
    if (old_it == old_subsystems.end()) {
      // Subsystem not found in old list - it's new
      BeaconSubsystem added = new_sub;
      added.change_descriptor = subsystem_change_t::SUBSYSTEM_ADDED;
      result.push_back(std::move(added));
    } else {
      // subsystem exists - check if it changed
      if (!(*old_it == new_sub)) {
        BeaconSubsystem changed = new_sub;
        changed.change_descriptor = subsystem_change_t::SUBSYSTEM_CHANGED;
        result.push_back(std::move(changed));
      }
      // else: unchanged, do not add
    }
  }

  // for any subsystem in old_subsystems not present in new_subsystems, add as deleted
  for (const auto& old_sub : old_subsystems) {
    auto found = std::find_if(new_subsystems.begin(), new_subsystems.end(),
                             [&](const BeaconSubsystem& s) { return s.nqn == old_sub.nqn; });
    if (found == new_subsystems.end()) {
      BeaconSubsystem deleted_sub = old_sub;
      deleted_sub.change_descriptor = subsystem_change_t::SUBSYSTEM_DELETED;
      result.push_back(std::move(deleted_sub));
    }
  }

  new_subsystems = std::move(result);
}

