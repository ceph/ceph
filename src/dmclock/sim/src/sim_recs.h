// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 *
 * Author: J. Eric Ivancich <ivancich@redhat.com>
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version
 * 2.1, as published by the Free Software Foundation.  See file
 * COPYING.
 */


#pragma once


#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <signal.h>

#include <sys/time.h>

#include <cmath>
#include <limits>
#include <string>
#include <mutex>
#include <iostream>
#include <functional>


using ClientId = unsigned;
using ServerId = unsigned;


namespace crimson {
  namespace qos_simulation {

    using Cost = uint32_t;

    inline void debugger() {
      raise(SIGCONT);
    }

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
    R time_stats_w_return(std::mutex& mtx,
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

      explicit TestResponse(uint32_t _epoch) :
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
