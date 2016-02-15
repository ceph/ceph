// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#pragma once

#include <map>
#include <deque>
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


    // S is server identifier type
    template<typename S>
    class ServiceTracker {
      using TimePoint = decltype(std::chrono::steady_clock::now());
      using Duration = std::chrono::milliseconds;
      using MarkPoint = std::pair<TimePoint,Counter>;

      Counter                 delta_counter; // # reqs completed
      Counter                 rho_counter;   // # reqs completed via reservation
      std::map<S,ServerInfo>  service_map;
      mutable std::mutex      data_mtx;      // protects Counters and map

      // clean config

      RunEvery                cleaning_job;
      std::deque<MarkPoint>   clean_mark_points;
      Duration                clean_age;     // age at which ServerInfo cleaned

      using DataGuard = std::lock_guard<decltype(data_mtx)>;

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


      /*
       * Incorporates the RespParams received into the various counter.
       */
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


      /*
       * Returns the ReqParams for the given server.
       */
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
	  ReqParams<C> result(client, uint32_t(delta), uint32_t(rho));

	  it->second.req_update(delta_counter, rho_counter);

	  return result;
	}
      }

    private:

      /*
       * This is being called regularly by RunEvery. Every time it's
       * called it notes the time and delta counter (mark point) in a
       * deque. It also looks at the deque to find the most recent
       * mark point that is older than clean_age. It then walks the
       * map and delete all server entries that were last used before
       * that mark point.
       */
      void do_clean() {
	TimePoint now = std::chrono::steady_clock::now();
	DataGuard g(data_mtx);
	clean_mark_points.emplace_back(MarkPoint(now, delta_counter));

	Counter earliest = 0;
	auto point = clean_mark_points.front();
	while (point.first <= now - clean_age) {
	  earliest = point.second;
	  clean_mark_points.pop_front();
	  point = clean_mark_points.front();
	}

	if (earliest > 0) {
	  for (auto i = service_map.begin();
	       i != service_map.end();
	       /* empty */) {
	    auto i2 = i++;
	    if (i2->second.delta_prev_req <= earliest) {
	      service_map.erase(i2);
	    }
	  }
	}
      }
    }; // class ServiceTracker
  }
}
