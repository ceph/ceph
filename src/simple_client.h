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

#if 0  // TODO REMOVE
    struct ServerInfo {
      Counter   delta_prev_req;
      Counter   rho_prev_req;
      uint32_t  my_delta;
      uint32_t  my_rho;

      ServerInfo(Counter _delta_prev_req,
		 Counter _rho_prev_req) :
	delta_prev_req(_delta_prev_req),
	rho_prev_req(_rho_prev_req),
	my_delta(0),
	my_rho(0)
      {
	// empty
      }

      inline void req_update(Counter delta, Counter rho) {
	delta_prev_req = delta;
	rho_prev_req = rho;
	my_delta = 0;
	my_rho = 0;
      }

      inline void resp_update(PhaseType phase) {
	++my_delta;
	if (phase == PhaseType::reservation) ++my_rho;
      }
    };
#endif


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
