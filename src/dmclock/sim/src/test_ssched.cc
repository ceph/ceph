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


#include "ssched_recs.h"
#include "ssched_server.h"
#include "ssched_client.h"

#include "sim_recs.h"
#include "sim_server.h"
#include "sim_client.h"

#include "test_ssched.h"


namespace test = crimson::test_simple_scheduler;
namespace ssched = crimson::simple_scheduler;


void test::simple_server_accumulate_f(test::SimpleAccum& a,
				      const ssched::NullData& add_info) {
  ++a.request_count;
}


void test::simple_client_accumulate_f(test::SimpleAccum& a,
				      const ssched::NullData& ignore) {
  // empty
}
