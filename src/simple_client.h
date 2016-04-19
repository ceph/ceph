// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#pragma once

// #include <map>
// #include <deque>
// #include <chrono>
// #include <thread>
// #include <mutex>
// #include <condition_variable>

// #include "run_every.h"
#include "simple_recs.h"

// #include "gtest/gtest_prod.h"


namespace crimson {
  namespace simple_scheduler {


    // S is server identifier type
    template<typename S>
    class ServiceTracker {

    public:

      // we have to start the counters at 1, as 0 is used in the
      // cleaning process
      ServiceTracker()
      {
	// emptry
      }


      void track_resp(const RespParams<S>& resp_params) {
	// empty
      }


      /*
       * Returns the ReqParams for the given server.
       */
      template<typename C>
      ReqParams<C> get_req_params(const C& client, const S& server) {
	return ReqParams<C>(client);
      } // get_req_params
    }; // class ServiceTracker
  } // namespace simple_scheduler
} // namespace crimson
