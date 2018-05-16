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


#include "dmclock_recs.h"
#include "dmclock_server.h"
#include "dmclock_client.h"

#include "sim_recs.h"
#include "sim_server.h"
#include "sim_client.h"

#include "test_dmclock.h"


namespace test = crimson::test_dmc;


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
