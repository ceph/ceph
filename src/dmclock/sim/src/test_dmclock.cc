// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 *
 * Author: J. Eric Ivancich <ivancich@redhat.com>
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version
 * 2.1, as published by the Free Software Foundation.  See file
 * COPYING.
 */

#include <type_traits>

#include "dmclock_recs.h"
#include "dmclock_server.h"
#include "dmclock_client.h"

#include "sim_recs.h"
#include "sim_server.h"
#include "sim_client.h"

#include "test_dmclock.h"


namespace test = crimson::test_dmc;


// Note: if this static_assert fails then our two definitions of Cost
// do not match; change crimson::qos_simulation::Cost to match the
// definition of crimson::dmclock::Cost.
static_assert(std::is_same<crimson::qos_simulation::Cost,crimson::dmclock::Cost>::value,
	      "Please make sure the simulator type crimson::qos_simulation::Cost matches the dmclock type crimson::dmclock::Cost.");


void test::dmc_server_accumulate_f(test::DmcAccum& a,
				   const test::dmc::PhaseType& phase) {
  if (test::dmc::PhaseType::reservation == phase) {
    ++a.reservation_count;
  } else {
    ++a.proportion_count;
  }
}


void test::dmc_client_accumulate_f(test::DmcAccum& a,
				   const test::dmc::PhaseType& phase) {
  if (test::dmc::PhaseType::reservation == phase) {
    ++a.reservation_count;
  } else {
    ++a.proportion_count;
  }
}
