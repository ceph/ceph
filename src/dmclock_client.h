// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#pragma once

#include <map>
#include <chrono>
#include <thread>
#include <mutex>
#include <condition_variable>

#include "crimson/run_every.h"
#include "dmclock_util.h"
#include "dmclock_recs.h"


namespace crimson {
  namespace dmclock {
    struct ServerInfo {
      Counter delta_prev_req;
      Counter rho_prev_req;
      uint32_t my_delta;
      uint32_t my_rho;

      using TimePoint = decltype(std::chrono::steady_clock::now());

      // track last update to allow clean-up
      TimePoint last_update;

      ServerInfo(Counter _delta_prev_req,
		 Counter _rho_prev_req) :
	delta_prev_req(_delta_prev_req),
	rho_prev_req(_rho_prev_req),
	my_delta(0),
	my_rho(0),
	last_update(std::chrono::steady_clock::now())
      {
	// empty
      }

      inline void req_update(Counter delta, Counter rho) {
	delta_prev_req = delta;
	rho_prev_req = rho;
	my_delta = 0;
	my_rho = 0;
	last_update = std::chrono::steady_clock::now();
      }

      inline void resp_update(PhaseType phase) {
	++my_delta;
	if (phase == PhaseType::reservation) ++my_rho;
      }

      inline bool last_update_before(TimePoint moment) {
	return last_update < moment;
      }
    };

    
    // S is server identifier type
    template<typename S>
    class ServiceTracker {
      using Duration = std::chrono::milliseconds;

      Counter                delta_counter; // # reqs completed
      Counter                rho_counter;   // # reqs completed via reservation
      std::map<S,ServerInfo> service_map;
      mutable std::mutex     data_mtx;      // protects Counters and map

      // clean config

      RunEvery               cleaning_job;

      Duration               clean_age;     // age at which ServerInfo cleaned

      // types

      using DataGuard = std::lock_guard<decltype(data_mtx)>;
      using TimePoint = decltype(std::chrono::steady_clock::now());

    public:

      ServiceTracker(Duration _clean_every, Duration _clean_age) :
	delta_counter(0),
	rho_counter(0),
	clean_age(_clean_age),
	cleaning_job(_clean_every, std::bind(&ServiceTracker::do_clean, this))
      {
	// empty
      }

      ServiceTracker() :
	ServiceTracker(std::chrono::minutes(5), std::chrono::minutes(10))
      {
	// empty
      }

      void track_resp(const RespParams<S>& resp_params) {
	DataGuard g(data_mtx);

	auto it = service_map.find(resp_params.server);
	if (service_map.end() == it) {
	  // this code can only run if a request did not precede the
	  // response or if the record was cleaned up b/w when
	  // the request was made and now
	  ServerInfo si(delta_counter, rho_counter);
	  si.resp_update(resp_params.phase);
	  service_map.emplace(resp_params.server, si);
	} else {
	  it->second.resp_update(resp_params.phase);
	}

	++delta_counter;
	if (PhaseType::reservation == resp_params.phase) {
	  ++rho_counter;
	}
      }

      template<typename C>
      ReqParams<C> get_req_params(const C& client, const S& server) {
	DataGuard g(data_mtx);
	auto it = service_map.find(server);
	if (service_map.end() == it) {
	  service_map.emplace(server, ServerInfo(delta_counter, rho_counter));
	  return ReqParams<C>(client, 1, 1);
	} else {
	  Counter delta =
	    1 + delta_counter - it->second.delta_prev_req - it->second.my_delta;
	  Counter rho =
	    1 + rho_counter - it->second.rho_prev_req - it->second.my_rho;
	  assert(delta >= 1 && rho >= 1);
	  ReqParams<C> result(client, uint32_t(delta), uint32_t(rho));

	  it->second.req_update(delta_counter, rho_counter);

	  return result;
	}
      }

    private:

      void do_clean() {
	TimePoint when = std::chrono::steady_clock::now() - clean_age;
	DataGuard g(data_mtx);
	for (auto it = service_map.begin(); it != service_map.end(); ++it) {
	  if (it->second.last_update_before(when)) {
	    service_map.erase(it);
	  }
	}
      }
    }; // class ServiceTracker
  }
}
