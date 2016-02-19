// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#include <chrono>
#include <mutex>
#include <functional>
#include <iostream>


#include "dmclock_client.h"
#include "dmclock_util.h"
#include "gtest/gtest.h"


namespace dmc = crimson::dmclock;


namespace crimson {
  namespace dmclock {

    /*
     * Allows us to test the code provided with the mutex provided locked.
     */
    static void test_locked(std::mutex& mtx, std::function<void()> code) {
      std::lock_guard<std::mutex> l(mtx);
      code();
    }


    TEST(dmclock_client, server_erase) {
      using ServerId = int;
      using ClientId = int;

      ServerId server = 101;
      ClientId client = 3;

      RespParams<ServerId> resp_params(server, dmc::PhaseType::reservation);

      dmc::ServiceTracker<ServerId> st(std::chrono::seconds(2),
                                       std::chrono::seconds(3));

      auto lock_st = [&](std::function<void()> code) {
	test_locked(st.data_mtx, code);
      };

      /* The timeline should be as follows:
       *
       *     0 seconds : request created
       *
       *     1 seconds : map is size 1
       *
       * 2 seconds : clean notes first mark; +2 is base for further calcs
       *
       * 4 seconds : clean does nothing except makes another mark
       *
       *   5 seconds : when we're secheduled to erase (+2 + 3)
       *
       *     5 seconds : since the clean job hasn't run yet, map still size 1
       *
       * 6 seconds : clean erases server
       *
       *     7 seconds : verified server is gone (map size 0)
       */

      lock_st([&] () {
	  EXPECT_EQ(st.server_map.size(), 0) << "server map initially has size 0";
	});

      std::this_thread::sleep_for(std::chrono::seconds(1));

      auto req_params = st.get_req_params(client, server);

      lock_st([&] () {
	  EXPECT_EQ(st.server_map.size(), 1) <<
	    "server map has size 1 after first request";
	});

      std::this_thread::sleep_for(std::chrono::seconds(4));

      lock_st([&] () {
	  EXPECT_EQ(st.server_map.size(), 1) <<
	    "server map has size 1 just before erase";
	});

      std::this_thread::sleep_for(std::chrono::seconds(2));

      lock_st([&] () {
	  EXPECT_EQ(st.server_map.size(), 0) <<
	    "server map has size 0 just after erase";
	});
    } // TEST


    TEST(dmclock_client, delta_rho_values) {
      using ServerId = int;
      using ClientId = int;

      ServerId server1 = 101;
      ServerId server2 = 7;
      ClientId client = 3;

//      RespParams<ServerId> resp_params(server, dmc::PhaseType::reservation);

      dmc::ServiceTracker<ServerId> st(std::chrono::seconds(2),
                                       std::chrono::seconds(3));

      auto rp1 = st.get_req_params(client, server1);

      EXPECT_EQ(rp1.delta, 1) <<
	"delta should be 1 with no intervening responses by other servers";
      EXPECT_EQ(rp1.rho, 1) <<
	"rho should be 1 with no intervening reservation responses by other servers";

      auto rp2 = st.get_req_params(client, server1);

      EXPECT_EQ(rp2.delta, 1) <<
	"delta should be 1 with no intervening responses by other servers";
      EXPECT_EQ(rp2.rho, 1) <<
	"rho should be 1 with no intervening reservation responses by other servers";

      st.track_resp(dmc::RespParams<ServerId>(server1,
					      dmc::PhaseType::priority));

      auto rp3 = st.get_req_params(client, server1);

      EXPECT_EQ(rp3.delta, 1) <<
	"delta should be 1 with no intervening responses by other servers";
      EXPECT_EQ(rp3.rho, 1) <<
	"rho should be 1 with no intervening reservation responses by other servers";


      st.track_resp(dmc::RespParams<ServerId>(server2,
					      dmc::PhaseType::priority));

      auto rp4 = st.get_req_params(client, server1);

      EXPECT_EQ(rp4.delta, 2) <<
	"delta should be 2 with one intervening priority response by another server";
      EXPECT_EQ(rp4.rho, 1) <<
	"rho should be 1 with one intervening priority esponses by another server";

      auto rp5 = st.get_req_params(client, server1);

      EXPECT_EQ(rp5.delta, 1) <<
	"delta should be 1 with no intervening responses by other servers";
      EXPECT_EQ(rp5.rho, 1) <<
	"rho should be 1 with no intervening reservation responses by other servers";

      st.track_resp(dmc::RespParams<ServerId>(server2,
					      dmc::PhaseType::reservation));

      auto rp6 = st.get_req_params(client, server1);

      EXPECT_EQ(rp6.delta, 2) <<
	"delta should be 2 with one intervening reservation response by another server";
      EXPECT_EQ(rp6.rho, 2) <<
	"rho should be 2 with one intervening reservation esponses by another server";

    } // TEST
  } // namespace dmclock
} // namespace crimson
