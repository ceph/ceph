// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#ifndef _TEST_SERVER_H
#define _TEST_SERVER_H


#include <mutex>
#include <thread>

#include "dm_clock_srv.h"
#include "test_request.h"


using crimson::dmclock::PriorityQueue;
using crimson::dmclock::ClientInfo;


class TestServer {

  typedef typename std::lock_guard<std::mutex> Guard;

  PriorityQueue<int,TestRequest> queue;
  int                            iops;
  int                            thread_pool_size;

  int                            active_threads;
  std::mutex                     mtx;
  bool                           finishing;

public:

  // TestServer(int _thread_pool_size);
  TestServer(int iops,
	     int _thread_pool_size,
	     const std::function<ClientInfo(int)>& _clientInfoF);

  virtual ~TestServer();

  // void post(double delay, std::function<void()> done);
  void post(const TestRequest& request, std::function<void()> done);

  bool hasAvailThread();

protected:

  // void innerPost(const TestRequest& request, std::function<void()> done);
  void innerPost(std::unique_ptr<TestRequest> request,
		 std::function<void()> done);
	     
  void run(double time, std::function<void()> done);
};


#endif // _TEST_SERVER_H
