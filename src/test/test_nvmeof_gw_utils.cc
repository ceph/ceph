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
 *
 */

#include "nvmeof/NVMeofGwUtils.h"
#include "mon/NVMeofGwTypes.h"
#include <iostream>
#include <algorithm>
#include "include/ceph_assert.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix *_dout

void test_determine_subsystem_changes() {
  std::cout << __func__ << "\n\n" << std::endl;
  // Prepare old and new subsystems
  BeaconSubsystem sub1_old = { "nqn1", {}, {}, subsystem_change_t::SUBSYSTEM_ADDED };
  BeaconSubsystem sub2_old = { "nqn2", {}, {}, subsystem_change_t::SUBSYSTEM_ADDED };
  BeaconSubsystem sub3_old = { "nqn3", {}, {}, subsystem_change_t::SUBSYSTEM_ADDED };
  BeaconSubsystems old_subs = { sub1_old, sub2_old, sub3_old };

  // sub1 unchanged, sub2 changed, sub4 added, sub3 deleted
  BeaconSubsystem sub1_new = { "nqn1", {}, {}, subsystem_change_t::SUBSYSTEM_ADDED };
  BeaconSubsystem sub2_new = { "nqn2", { {"IPv4", "1.2.3.4", "4420"} }, {}, subsystem_change_t::SUBSYSTEM_ADDED }; // changed listeners
  BeaconSubsystem sub4_new = { "nqn4", {}, {}, subsystem_change_t::SUBSYSTEM_ADDED };
  BeaconSubsystems new_subs = { sub1_new, sub2_new, sub4_new };

  determine_subsystem_changes(old_subs, new_subs);

  // After call, new_subs should only contain changed, added, and deleted subsystems
  // sub1 (unchanged) should be removed
  // sub2 (changed) should be present with SUBSYSTEM_CHANGED
  // sub4 (added) should be present with SUBSYSTEM_ADDED
  // sub3 (deleted) should be present with SUBSYSTEM_DELETED
  bool found_sub2 = false, found_sub3 = false, found_sub4 = false;
  for (const auto& s : new_subs) {
    if (s.nqn == "nqn2") {
      found_sub2 = true;
      ceph_assert(s.change_descriptor == subsystem_change_t::SUBSYSTEM_CHANGED);
    } else if (s.nqn == "nqn3") {
      found_sub3 = true;
      ceph_assert(s.change_descriptor == subsystem_change_t::SUBSYSTEM_DELETED);
    } else if (s.nqn == "nqn4") {
      found_sub4 = true;
      ceph_assert(s.change_descriptor == subsystem_change_t::SUBSYSTEM_ADDED);
    } else {
      ceph_assert(false && "Unexpected subsystem in result");
    }
  }
  ceph_assert(found_sub2 && found_sub3 && found_sub4);
  std::cout << "determine_subsystem_changes test passed" << std::endl;
}

int main(int argc, const char **argv) {
  test_determine_subsystem_changes();
  return 0;
}
