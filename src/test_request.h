// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#pragma once


#include "dmclock_recs.h"


struct TestRequest {
  uint32_t epoch;
  uint32_t op;

  TestRequest(uint32_t _epoch,
	      uint32_t _op) :
    epoch(_epoch), op(_op)
  {
    // empty
  }

  TestRequest(const TestRequest& r) :
    TestRequest(r.epoch, r.op)
  {
    // empty
  }
}; // struct TestRequest


struct TestResponse {
  uint32_t                          epoch;
  crimson::dmclock::RespParams<int> resp_params;

  TestResponse(uint32_t                                 _epoch,
	       const crimson::dmclock::RespParams<int>& _resp_params) :
    epoch(_epoch),
    resp_params(_resp_params)
  {
    // empty
  }

  TestResponse(const TestResponse& r) :
    epoch(r.epoch),
    resp_params(r.resp_params)
  {
    // empty
  }

  friend std::ostream& operator<<(std::ostream& out, const TestResponse& resp) {
    out << "{ server:" << resp.resp_params.server <<
      ", phase:" <<
      (resp.resp_params.phase == crimson::dmclock::PhaseType::reservation ?
       "resv" : "prop") <<
      ", epoch:" << resp.epoch <<
      " }";
    return out;
  }
}; // class TestResponse
