// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#include <chrono>
#include <iostream>

#include "test_client.h"


using namespace std::placeholders;


static const bool info = false;


TestClient::TestClient(int _id,
		       const SubmitFunc& _submit_f,
		       int _ops_to_run,
		       int _iops_goal,
		       int _outstanding_ops_allowed) :
  id(_id),
  submit_f(_submit_f),
  ops_to_run(_ops_to_run),
  iops_goal(_iops_goal),
  outstanding_ops_allowed(_outstanding_ops_allowed),
  op_times(ops_to_run),
  outstanding_ops(0),
  requests_complete(false)
{
  thd_resp = std::thread(&TestClient::run_resp, this);
  thd_req = std::thread(&TestClient::run_req, this);
}


TestClient::~TestClient() {
  waitUntilDone();
}


void TestClient::waitUntilDone() {
  if (thd_req.joinable()) thd_req.join();
  if (thd_resp.joinable()) thd_resp.join();
}


void TestClient::run_req() {
  std::chrono::microseconds delay(int(0.5 + 1000000.0 / iops_goal));
  auto now = std::chrono::high_resolution_clock::now();

  if (info) {
    std::cout << "client " << id << " about to run " << ops_to_run <<
      " ops." << std::endl;
  }

  Guard guard(mtx_req);
  for (int i = 0; i < ops_to_run; ++i) {
    auto when = now + delay;
    while ((now = std::chrono::high_resolution_clock::now()) < when) {
      cv_req.wait_until(guard, when);
    }
    while (outstanding_ops >= outstanding_ops_allowed) {
      cv_req.wait(guard);
    }

    guard.unlock();
    TestRequest req(id, i, 12);
    submit_f(req); // TODO needed????
    ++outstanding_ops;
    guard.lock(); // lock for return to top of loop
  }

  requests_complete = true;

  // thread ends
}


void TestClient::run_resp() {
  std::chrono::milliseconds delay(1000);

  Guard g(mtx_resp);

  int op = 0;

  while(!requests_complete.load()) {
    while(resp_queue.empty() && !requests_complete.load()) {
      cv_resp.wait_for(g, delay);
    }
    if (!resp_queue.empty()) {
      op_times[op++] = now();
      if (info) std::cout << "resp->" << id << std::endl;
      resp_queue.pop_front();
      --outstanding_ops;
      cv_req.notify_one();
    }
  }

  if (info) {
    std::cout << "client " << id << " finishing " <<
      outstanding_ops.load() << " ops." << std::endl;
  }

  while(outstanding_ops.load() > 0) {
    while(resp_queue.empty() && outstanding_ops.load() > 0) {
      cv_resp.wait_for(g, delay);
    }
    if (!resp_queue.empty()) {
      op_times[op++] = now();
      if (info) std::cout << "resp->" << id << std::endl;
      resp_queue.pop_front();
      --outstanding_ops;
      // not needed since all requests completed cv_req.notify_one();
    }
  }

  if (info) {
    std::cout << "client " << id << " finished." << std::endl;
  }

  // all responses received, thread ends
}


void TestClient::submitResponse(const TestResponse& resp) {
  Guard g(mtx_resp);
  resp_queue.push_back(resp);
  cv_resp.notify_one();
}
