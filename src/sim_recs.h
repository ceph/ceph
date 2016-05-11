// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#pragma once


#include <stdint.h>
#include <stdlib.h>
#include <assert.h>

#include <sys/time.h>

#include <cmath>
#include <limits>
#include <string>
#include <mutex>
#include <iostream>


using ClientId = uint;
using ServerId = uint;


namespace crimson {
  namespace qos_simulation {
#if 0 // STILL NEEDED?
    using Time = double;
    static const Time TimeZero = 0.0;
    static const Time TimeMax = std::numeric_limits<Time>::max();
    static const double NaN = nan("");


    inline Time get_time() {
      struct timeval now;
      assert(0 == gettimeofday(&now, NULL));
      return now.tv_sec + (now.tv_usec / 1000000.0);
    }

    std::string format_time(const Time& time, uint modulo = 1000);

    void debugger();
#endif

    template<typename T>
    void time_stats(std::mutex& mtx,
		    T& time_accumulate,
		    std::function<void()> code) {
      auto t1 = std::chrono::steady_clock::now();
      code();
      auto t2 = std::chrono::steady_clock::now();
      auto duration = t2 - t1;
      auto cast_duration = std::chrono::duration_cast<T>(duration);
      std::lock_guard<std::mutex> lock(mtx);
      time_accumulate += cast_duration;
    }

    // unfortunately it's hard for the compiler to infer the types,
    // and therefore when called the template params might have to be
    // explicit
    template<typename T, typename R>
    R time_stats_type(std::mutex& mtx,
		      T& time_accumulate,
		      std::function<R()> code) {
      auto t1 = std::chrono::steady_clock::now();
      R result = code();
      auto t2 = std::chrono::steady_clock::now();
      auto duration = t2 - t1;
      auto cast_duration = std::chrono::duration_cast<T>(duration);
      std::lock_guard<std::mutex> lock(mtx);
      time_accumulate += cast_duration;
      return result;
    }

    template<typename T>
    void count_stats(std::mutex& mtx,
		     T& counter) {
      std::lock_guard<std::mutex> lock(mtx);
      ++counter;
    }

    struct TestRequest {
      ServerId server; // allows debugging
      uint32_t epoch;
      uint32_t op;

      TestRequest(ServerId _server,
		  uint32_t _epoch,
		  uint32_t _op) :
	server(_server),
	epoch(_epoch),
	op(_op)
      {
	// empty
      }

      TestRequest(const TestRequest& r) :
	TestRequest(r.server, r.epoch, r.op)
      {
	// empty
      }
    }; // struct TestRequest


    struct TestResponse {
      uint32_t epoch;

      TestResponse(uint32_t _epoch) :
	epoch(_epoch)
      {
	// empty
      }

      TestResponse(const TestResponse& r) :
	epoch(r.epoch)
      {
	// empty
      }

      friend std::ostream& operator<<(std::ostream& out, const TestResponse& resp) {
	out << "{ ";
	out << "epoch:" << resp.epoch;
	out << " }";
	return out;
      }
    }; // class TestResponse

  }; // namespace qos_simulation
}; // namespace crimson
