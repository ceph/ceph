// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2016 Red Hat Inc.
 */

#include <atomic>
#include <thread>
#include <chrono>
#include <iostream>

#include "gtest/gtest.h"

#include "test_recs.h"
#include "test_client.h"

#include "test_dmclock.h"


using namespace std::placeholders;

namespace dmc = crimson::dmclock;
namespace test = test_dmc;

using TimePoint = std::chrono::time_point<std::chrono::system_clock>;


static TimePoint now() { return std::chrono::system_clock::now(); }


TEST(test_client, full_bore_timing) {
  std::atomic_ulong count(0);

  ServerId server_id = 3;

  TestResponse resp(0);
  dmc::RespParams<ServerId>
    resp_params(server_id, dmc::PhaseType::priority);
  test::DmcClient* client;


  auto start = now();
  client = new test::DmcClient(ClientId(0),
			       [&] (const ServerId& server,
				    const TestRequest& req,
				    const dmc::ReqParams<ClientId>& req_params) {
				 ++count;
				 client->receive_response(resp, resp_params);
			       },
			       [&] (const uint64_t seed) -> ServerId& {
				 return server_id;
			       },
			       test::dmc_client_accumulate_f,
			       1000, // ops to run
			       100, // iops goal
			       5); // outstanding ops allowed
  client->wait_until_done();
  auto end = now();
  EXPECT_EQ(1000, count) << "didn't get right number of ops";

  int milliseconds = (end - start) / std::chrono::milliseconds(1);
  EXPECT_LT(10000, milliseconds) << "timing too fast to be correct";
  EXPECT_GT(12000, milliseconds) << "timing suspiciously slow";
}


TEST(test_client, paused_timing) {
  std::atomic_ulong count(0);
  std::atomic_ulong unresponded_count(0);
  std::atomic_bool auto_respond(false);

  ServerId server_id = 3;

  TestResponse resp(0);
  dmc::RespParams<ServerId>
    resp_params(server_id, dmc::PhaseType::priority);
  test::DmcClient* client;

  auto start = now();
  client = new test::DmcClient(0,
			       [&] (const ServerId& server,
				    const TestRequest& req,
				    const dmc::ReqParams<ClientId>& req_params) {
				 ++count;
				 if (auto_respond.load()) {
				   client->receive_response(resp, resp_params);
				 } else {
				   ++unresponded_count;
				 }
			       },
			       [&] (const uint64_t seed) -> ServerId& {
				 return server_id;
			       },
			       test::dmc_client_accumulate_f,

			       1000, // ops to run
			       100, // iops goal
			       50); // outstanding ops allowed
  std::thread t([&]() {
      std::this_thread::sleep_for(std::chrono::seconds(5));
      EXPECT_EQ(50, unresponded_count.load()) <<
	"should have 50 unresponded calls";
      auto_respond = true;
      // respond to those 50 calls
      for(int i = 0; i < 50; ++i) {
	client->receive_response(resp, resp_params);
	--unresponded_count;
      }
    });

  client->wait_until_done();
  auto end = now();
  int milliseconds = (end - start) / std::chrono::milliseconds(1);

  // the 50 outstanding ops allowed means the first half-second of
  // requests get responded to during the 5 second pause. So we have
  // to adjust our expectations by a half-second.
  EXPECT_LT(15000 - 500, milliseconds) << "timing too fast to be correct";
  EXPECT_GT(17000 - 500, milliseconds) << "timing suspiciously slow";
  t.join();
}
