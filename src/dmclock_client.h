// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#pragma once

#include <map>
#include <deque>
#include <chrono>
#include <thread>
#include <mutex>
#include <condition_variable>

#include "run_every.h"
#include "dmclock_util.h"
#include "dmclock_recs.h"

#include "gtest/gtest_prod.h"


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
      FRIEND_TEST(dmclock_client, server_erase);

      using TimePoint = decltype(std::chrono::steady_clock::now());
      using Duration = std::chrono::milliseconds;
      using MarkPoint = std::pair<TimePoint,Counter>;

      Counter                 delta_counter; // # reqs completed
      Counter                 rho_counter;   // # reqs completed via reservation
      std::map<S,ServerInfo>  server_map;
      mutable std::mutex      data_mtx;      // protects Counters and map

      using DataGuard = std::lock_guard<decltype(data_mtx)>;

      // clean config

      std::deque<MarkPoint>     clean_mark_points;
      Duration                  clean_age;     // age at which ServerInfo cleaned

      // NB: All threads declared at end, so they're destructed firs!

      std::unique_ptr<RunEvery> cleaning_job;


    public:

      // we have to start the counters at 1, as 0 is used in the
      // cleaning process
      template<typename Rep, typename Per>
      ServiceTracker(std::chrono::duration<Rep,Per> _clean_every,
		     std::chrono::duration<Rep,Per> _clean_age) :
	delta_counter(1),
	rho_counter(1),
	clean_age(std::chrono::duration_cast<Duration>(_clean_age))
      {
	cleaning_job =
	  std::unique_ptr<RunEvery>(
	    new RunEvery(_clean_every,
			 std::bind(&ServiceTracker::do_clean, this)));
      }


      // the reason we're overloading the constructor rather than
      // using default values for the arguments is so that callers
      // have to either use all defaults or specify all timings; with
      // default arguments they could specify some without others
      ServiceTracker() :
	ServiceTracker(std::chrono::minutes(5), std::chrono::minutes(10))
      {
	// empty
      }


      /*
       * Incorporates the RespParams received into the various counter.
       */
      void track_resp(const S& server_id, const PhaseType& phase) {
	DataGuard g(data_mtx);

	auto it = server_map.find(server_id);
	if (server_map.end() == it) {
	  // this code can only run if a request did not precede the
	  // response or if the record was cleaned up b/w when
	  // the request was made and now
	  ServerInfo si(delta_counter, rho_counter);
	  si.resp_update(phase);
	  server_map.emplace(server_id, si);
	} else {
	  it->second.resp_update(phase);
	}

	++delta_counter;
	if (PhaseType::reservation == phase) {
	  ++rho_counter;
	}
      }


      /*
       * Returns the ReqParams for the given server.
       */
      ReqParams get_req_params(const S& server) {
	DataGuard g(data_mtx);
	auto it = server_map.find(server);
	if (server_map.end() == it) {
	  server_map.emplace(server, ServerInfo(delta_counter, rho_counter));
	  return ReqParams(1, 1);
	} else {
	  Counter delta =
	    1 + delta_counter - it->second.delta_prev_req - it->second.my_delta;
	  Counter rho =
	    1 + rho_counter - it->second.rho_prev_req - it->second.my_rho;
	  
	  it->second.req_update(delta_counter, rho_counter);

	  return ReqParams(uint32_t(delta), uint32_t(rho));
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
	  for (auto i = server_map.begin();
	       i != server_map.end();
	       /* empty */) {
	    auto i2 = i++;
	    if (i2->second.delta_prev_req <= earliest) {
	      server_map.erase(i2);
	    }
	  }
	}
      } // do_clean
    }; // class ServiceTracker
  }
}
