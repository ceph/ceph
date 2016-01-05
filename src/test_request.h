// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#pragma once


struct TestRequest {
  int client;
  uint32_t epoch;
  uint32_t op;

  TestRequest(int _client, uint32_t _epoch, uint32_t _op) :
    client(_client), epoch(_epoch), op(_op)
  {
    // empty
  }

  TestRequest(const TestRequest& r) :
    TestRequest(r.client, r.epoch, r.op)
  {
    // empty
  }
}; // struct TestRequest


struct TestResponse {
  int      server;
  uint32_t epoch;

  TestResponse(int _server,
	       uint32_t _epoch) :
    server(_server),
    epoch(_epoch)
  {
    // empty
  }

  TestResponse(const TestResponse& r) :
    server(r.server),
    epoch(r.epoch)
  {
    // empty
  }
}; // class TestResponse
