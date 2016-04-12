// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#include "test_recs.h"
#include "test_server.h"
#include "test_client.h"

#include "simple_recs.h"
#include "simple_server.h"

#include "test_simple.h"


namespace test = test_simple;
namespace simp = crimson::simple_scheduler;


void test::simple_server_accumulate_f(test::SimpleAccum& a,
				      const test::SimpleServerAddInfo& add_info) {
  // empty
}


void test::simple_client_accumulate_f(test::SimpleAccum& a,
				      const simp::RespParams<ServerId>& r) {
  // empty
}
