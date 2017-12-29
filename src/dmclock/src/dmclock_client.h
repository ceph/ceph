// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2017 Red Hat Inc.
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


namespace crimson {
  namespace dmclock {

    // OrigTracker is a best-effort implementation of the the original
    // dmClock calculations of delta and rho. It adheres to an
    // interface, implemented via a template type, that allows it to
    // be replaced with an alternative. The interface consists of the
    // static create, prepare_req, resp_update, and get_last_delta
    // functions.
    class OrigTracker {
      Counter   delta_prev_req;
      Counter   rho_prev_req;
      uint32_t  my_delta;
      uint32_t  my_rho;

    public:

      OrigTracker(Counter global_delta,
		 Counter global_rho) :
	delta_prev_req(global_delta),
	rho_prev_req(global_rho),
	my_delta(0),
	my_rho(0)
      { /* empty */ }

      static inline OrigTracker create(Counter the_delta, Counter the_rho) {
	return OrigTracker(the_delta, the_rho);
      }

      inline ReqParams prepare_req(Counter& the_delta, Counter& the_rho) {
	Counter delta_out = 1 + the_delta - delta_prev_req - my_delta;
	Counter rho_out = 1 + the_rho - rho_prev_req - my_rho;
	delta_prev_req = the_delta;
	rho_prev_req = the_rho;
	my_delta = 0;
	my_rho = 0;
	return ReqParams(uint32_t(delta_out), uint32_t(rho_out));
      }

      inline void resp_update(PhaseType phase,
			      Counter& the_delta,
			      Counter& the_rho) {
	++the_delta;
	++my_delta;
	if (phase == PhaseType::reservation) {
	  ++the_rho;
	  ++my_rho;
	}
      }

      inline Counter get_last_delta() const {
	return delta_prev_req;
      }
    }; // struct OrigTracker


    // BorrowingTracker always returns a positive delta and rho. If
    // not enough responses have come in to allow that, we will borrow
    // a future response and repay it later.
    class BorrowingTracker {
      Counter delta_prev_req;
      Counter rho_prev_req;
      Counter delta_borrow;
      Counter rho_borrow;

    public:

      BorrowingTracker(Counter global_delta, Counter global_rho) :
	delta_prev_req(global_delta),
	rho_prev_req(global_rho),
	delta_borrow(0),
	rho_borrow(0)
      { /* empty */ }

      static inline BorrowingTracker create(Counter the_delta,
					    Counter the_rho) {
	return BorrowingTracker(the_delta, the_rho);
      }

      inline Counter calc_with_borrow(const Counter& global,
				      const Counter& previous,
				      Counter& borrow) {
	Counter result = global - previous;
	if (0 == result) {
	  // if no replies have come in, borrow one from the future
	  ++borrow;
	  return 1;
	} else if (result > borrow) {
	  // if we can give back all of what we borrowed, do so
	  result -= borrow;
	  borrow = 0;
	  return result;
	} else {
	  // can only return part of what was borrowed in order to
	  // return positive
	  borrow = borrow - result + 1;
	  return 1;
	}
      }

      inline ReqParams prepare_req(Counter& the_delta, Counter& the_rho) {
	Counter delta_out =
	  calc_with_borrow(the_delta, delta_prev_req, delta_borrow);
	Counter rho_out =
	  calc_with_borrow(the_rho, rho_prev_req, rho_borrow);
	delta_prev_req = the_delta;
	rho_prev_req = the_rho;
	return ReqParams(uint32_t(delta_out), uint32_t(rho_out));
      }

      inline void resp_update(PhaseType phase,
			      Counter& the_delta,
			      Counter& the_rho) {
	++the_delta;
	if (phase == PhaseType::reservation) {
	  ++the_rho;
	}
      }

      inline Counter get_last_delta() const {
	return delta_prev_req;
      }
    }; // struct BorrowingTracker


    // S is server identifier type
    // T is the server info class that adheres to ServerTrackerIfc interface
    template<typename S, typename T = BorrowingTracker>
    class ServiceTracker {
      // we don't want to include gtest.h just for FRIEND_TEST
      friend class dmclock_client_server_erase_Test;

      using TimePoint = decltype(std::chrono::steady_clock::now());
      using Duration = std::chrono::milliseconds;
      using MarkPoint = std::pair<TimePoint,Counter>;

      Counter                 delta_counter; // # reqs completed
      Counter                 rho_counter;   // # reqs completed via reservation
      std::map<S,T>           server_map;
      mutable std::mutex      data_mtx;      // protects Counters and map

      using DataGuard = std::lock_guard<decltype(data_mtx)>;

      // clean config

      std::deque<MarkPoint>     clean_mark_points;
      Duration                  clean_age;     // age at which server tracker cleaned

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
	  auto i = server_map.emplace(server_id,
				      T::create(delta_counter, rho_counter));
	  it = i.first;
	}
	it->second.resp_update(phase, delta_counter, rho_counter);
      }

      /*
       * Returns the ReqParams for the given server.
       */
      ReqParams get_req_params(const S& server) {
	DataGuard g(data_mtx);
	auto it = server_map.find(server);
	if (server_map.end() == it) {
	  server_map.emplace(server,
			     T::create(delta_counter, rho_counter));
	  return ReqParams(1, 1);
	} else {
	  return it->second.prepare_req(delta_counter, rho_counter);
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
	    if (i2->second.get_last_delta() <= earliest) {
	      server_map.erase(i2);
	    }
	  }
	}
      } // do_clean
    }; // class ServiceTracker
  }
}
