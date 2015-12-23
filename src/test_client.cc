// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */

#include <chrono>

#include "test_client.h"


TestClient::TestClient(int _id,
		       const SubmitFunc& _submit_f,
		       int _ops_to_run,
		       int _iops_goal,
		       int _outstanding_ops_allowed) :
  id(_id),
  submit_f(_submit_f),
  ops_to_run(_ops_to_run),
  iops_goal(_iops_goal),
  outstanding_ops_allowed(_outstanding_ops_allowed)
{
  thread = std::thread(&TestClient::run, this);
}


TestClient::~TestClient() {
  if (thread.joinable()) {
    thread.join();
  }
}


void TestClient::run() {
  auto request_complete =
    std::bind(&TestClient::submitResponse, this);
  std::chrono::microseconds delay((int) (0.5 + 1000000.0 / iops_goal));
  auto now = std::chrono::high_resolution_clock::now();

  std::unique_lock<std::mutex> lock(mtx);
  for (int i = 0; i < ops_to_run; ++i) {
    auto when = now + delay;
    while ((now = std::chrono::high_resolution_clock::now()) < when) {
      cv.wait_until(lock, when);
    }
    while (outstanding_ops >= outstanding_ops_allowed) {
      cv.wait(lock);
    }
    TestRequest req(id, i, 12);
    submit_f(req, request_complete);
    ++outstanding_ops;
  }

  while (outstanding_ops > 0) {
    cv.wait(lock);
  }

  // all requests have been serviced
}

void TestClient::submitResponse() {
  Guard g(mtx);
  --outstanding_ops;
  cv.notify_one();
}

void TestClient::waitForDone() {
  thread.join();
}
