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
    void test_locked(std::mutex& mtx, std::function<void()> code) {
      std::unique_lock<std::mutex> l(mtx);
      code();
    }

    using ServerId = int;
    using ClientId = int;

    TEST(dmclock_client, server_erase) {
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
  } // namespace dmclock
} // namespace crimson
