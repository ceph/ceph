// -*- mode:C++; tab-width:8; c-basic-offset:2;
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Author: Gabriel BenHanokh <gbenhano@ibm.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "gc_config_state.hpp"

#include <cassert>
#include <iostream>

int main()
{
  kvrgw::GcPolicy initial;
  initial.interval_sec = 10;
  kvrgw::GcConfigState state(initial);

  assert(state.active_age() == 0);
  const auto snap0 = state.snapshot();
  assert(snap0.active_age == 0);
  assert(snap0.pending_age == 0);

  kvrgw::GcPolicy suspended = initial;
  suspended.suspended = true;
  suspended.interval_sec = 99;
  const auto h1 = state.try_stage(suspended);
  assert(h1.has_value());
  assert(*h1 == 1);
  assert(state.snapshot().pending_age == 1);
  assert(state.active_age() == 0);

  assert(!state.try_stage(suspended).has_value());

  state.maybe_apply_pending();
  assert(state.active_age() == 1);
  const auto snap1 = state.snapshot();
  assert(snap1.active.suspended);
  assert(snap1.active.interval_sec == 99);

  const auto h2 = state.try_stage(initial);
  assert(h2.has_value());
  assert(*h2 == 2);
  state.maybe_apply_pending();
  assert(state.active_age() == 2);
  assert(!state.snapshot().active.suspended);

  std::cout << "gc_config_state_test passed\n";
  return 0;
}
