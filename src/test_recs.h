// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#pragma once


#include <string>

#include "dmclock_recs.h"


using ClientId = uint;
using ServerId = uint;


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
