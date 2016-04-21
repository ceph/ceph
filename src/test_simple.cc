// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#include "simple_recs.h"
#include "simple_server.h"
#include "simple_client.h"

#include "sim_recs.h"
#include "sim_server.h"
#include "sim_client.h"

#include "test_simple.h"


namespace test = crimson::test_simple_scheduler;
namespace ssched = crimson::simple_scheduler;


void test::simple_server_accumulate_f(test::SimpleAccum& a,
				      const ssched::NullData& add_info) {
  ++a.request_count;
}


void test::simple_client_accumulate_f(test::SimpleAccum& a,
				      const ssched::RespParams<ServerId>& r) {
  // empty
}
