// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#ifndef _TEST_CLIENT_H
#define _TEST_CLIENT_H


#include <mutex>
#include <condition_variable>
#include <thread>


class TestClient {
  typedef std::function<void(const TestRequest&,
			     std::function<void()>)> SubmitFunc;

  typedef std::lock_guard<std::mutex> Guard;

  int id;
  SubmitFunc submit_f;
  int ops_to_run;
  int iops_goal; // per second
  int outstanding_ops_allowed;
  int outstanding_ops;

  std::mutex mtx;
  std::condition_variable cv;
  std::thread thread;
    
public:

  TestClient(int _id,
	     const SubmitFunc& _submit_f,
	     int _ops_to_run,
	     int _iops_goal,
	     int _outstanding_ops_allowed);

  virtual ~TestClient();

  void submitResponse();

  void waitForDone();

protected:

  void run();
};


#endif // _TEST_CLIENT_H
