// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#pragma once


#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>

#include "crimson/queue.h"

#include "test_request.h"


namespace c = crimson;


class TestClient {
  typedef std::function<void(const TestRequest&)> SubmitFunc;

  typedef std::unique_lock<std::mutex> Guard;

  int id;
  SubmitFunc submit_f;
  int ops_to_run;
  int iops_goal; // per second
  int outstanding_ops_allowed;

  std::atomic_ulong        outstanding_ops;
  std::atomic_bool         requests_complete;

  std::deque<TestResponse> resp_queue;

  std::mutex               mtx_req;
  std::condition_variable  cv_req;
  std::thread              thd_req;

  std::mutex               mtx_resp;
  std::condition_variable  cv_resp;
  std::thread              thd_resp;
    
public:

  TestClient(int _id,
	     const SubmitFunc& _submit_f,
	     int _ops_to_run,
	     int _iops_goal,
	     int _outstanding_ops_allowed);

  virtual ~TestClient();

  void submitResponse(const TestResponse&);

  void waitForDone();

protected:

  void run_req();
  void run_resp();
}; // class TestClient
