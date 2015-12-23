// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */

#include <mutex>
#include <thread>

#include "test_server.h"


class TestClient {

  int id;
  TestServer& server;
  int ops_to_run;
  int iops_goal; // per second
  int outstanding_ops_allowed;
  int outstanding_ops;

  std::mutex mtx;
  std::condition_variable cv;
  std::thread thread;

  typedef std::lock_guard<std::mutex> Guard;
  
    
public:

  TestClient(int _id,
	     TestServer& _server,
	     int _ops_to_run,
	     int _iops_goal,
	     int _outstanding_ops_allowed);

  virtual ~TestClient();

  void submitResponse();

  void waitForDone();

protected:

  void run();
};
