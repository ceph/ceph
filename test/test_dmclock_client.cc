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

      dmc::PhaseType resp_params = dmc::PhaseType::reservation;

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
	  EXPECT_EQ(0, st.server_map.size()) << "server map initially has size 0";
	});

      std::this_thread::sleep_for(std::chrono::seconds(1));

      auto req_params = st.get_req_params(server);

      lock_st([&] () {
	  EXPECT_EQ(1, st.server_map.size()) <<
	    "server map has size 1 after first request";
	});

      std::this_thread::sleep_for(std::chrono::seconds(4));

      lock_st([&] () {
	  EXPECT_EQ(1, st.server_map.size()) <<
	    "server map has size 1 just before erase";
	});

      std::this_thread::sleep_for(std::chrono::seconds(2));

      lock_st([&] () {
	  EXPECT_EQ(0, st.server_map.size()) <<
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

      auto rp1 = st.get_req_params(server1);

      EXPECT_EQ(1, rp1.delta) <<
	"delta should be 1 with no intervening responses by" <<
	"other servers";
      EXPECT_EQ(1, rp1.rho) <<
	"rho should be 1 with no intervening reservation responses by" <<
	"other servers";

      auto rp2 = st.get_req_params(server1);

      EXPECT_EQ(1, rp2.delta) <<
	"delta should be 1 with no intervening responses by" <<
	"other servers";
      EXPECT_EQ(1, rp2.rho) <<
	"rho should be 1 with no intervening reservation responses by" <<
	"other servers";

      st.track_resp(server1, dmc::PhaseType::priority);

      auto rp3 = st.get_req_params(server1);

      EXPECT_EQ(1, rp3.delta) <<
	"delta should be 1 with no intervening responses by" <<
	"other servers";
      EXPECT_EQ(1, rp3.rho) <<
	"rho should be 1 with no intervening reservation responses by" <<
	"other servers";


      st.track_resp(server1, dmc::PhaseType::priority);

      auto rp4 = st.get_req_params(server1);

      EXPECT_EQ(2, rp4.delta) <<
	"delta should be 2 with one intervening priority response by " <<
	"another server";
      EXPECT_EQ(1, rp4.rho) <<
	"rho should be 1 with one intervening priority responses by " <<
	"another server";

      auto rp5 = st.get_req_params(server1);

      EXPECT_EQ(1, rp5.delta) <<
	"delta should be 1 with no intervening responses by" <<
	"other servers";
      EXPECT_EQ(1, rp5.rho) <<
	"rho should be 1 with no intervening reservation responses by" <<
	"other servers";

      st.track_resp(server1, dmc::PhaseType::reservation);

      auto rp6 = st.get_req_params(server1);

      EXPECT_EQ(2, rp6.delta) <<
	"delta should be 2 with one intervening reservation response by " <<
	"another server";
      EXPECT_EQ(2, rp6.rho) <<
	"rho should be 2 with one intervening reservation responses by " <<
	"another server";

      auto rp6_b = st.get_req_params(server2);

      st.track_resp(server2, dmc::PhaseType::reservation);
      st.track_resp(server1, dmc::PhaseType::priority);
      st.track_resp(server2, dmc::PhaseType::priority);
      st.track_resp(server2, dmc::PhaseType::reservation);
      st.track_resp(server1, dmc::PhaseType::reservation);
      st.track_resp(server1, dmc::PhaseType::priority);
      st.track_resp(server2, dmc::PhaseType::priority);

      auto rp7 = st.get_req_params(server1);

      EXPECT_EQ(5, rp7.delta) <<
	"delta should be 5 with fourintervening responses by " <<
	"another server";
      EXPECT_EQ(3, rp7.rho) <<
	"rho should be 3 with two intervening reservation responses by " <<
	"another server";

      auto rp7b = st.get_req_params(server2);

      EXPECT_EQ(4, rp7b.delta) <<
	"delta should be 4 with three intervening responses by " <<
	"another server";
      EXPECT_EQ(2, rp7b.rho) <<
	"rho should be 2 with one intervening reservation responses by " <<
	"another server";

      auto rp8 = st.get_req_params(server1);

      EXPECT_EQ(1, rp8.delta) <<
	"delta should be 1 with no intervening responses by " <<
	"another server";
      EXPECT_EQ(1, rp8.rho) <<
	"rho should be 1 with no intervening reservation responses by " <<
	"another server";

      auto rp8b = st.get_req_params(server2);
      EXPECT_EQ(1, rp8b.delta) <<
	"delta should be 1 with no intervening responses by " <<
	"another server";
      EXPECT_EQ(1, rp8b.rho) <<
	"rho should be 1 with no intervening reservation responses by " <<
	"another server";
    } // TEST
  } // namespace dmclock
} // namespace crimson
