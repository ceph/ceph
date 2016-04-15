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
#include <iostream>


using ClientId = uint;
using ServerId = uint;


namespace crimson {
  namespace queue_testing {
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
  }; // namespace queue_testing
}; // namespace crimson


// TODO : move into namespace above
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


// TODO : move into namespace above
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
