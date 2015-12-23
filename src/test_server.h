// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#ifndef _TEST_SERVER_H
#define _TEST_SERVER_H


#include <thread>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <deque>

#include "dm_clock_srv.h"
#include "test_request.h"


using crimson::dmclock::PriorityQueue;
using crimson::dmclock::ClientInfo;


class TestServer {

  typedef typename std::lock_guard<std::mutex> Guard;
  typedef std::pair<std::unique_ptr<TestRequest>,std::function<void()>> QueueItem;

  PriorityQueue<int,TestRequest> priority_queue;
  int                            iops;
  int                            thread_pool_size;

  int                            active_threads;
  bool                           finishing;
  std::chrono::microseconds      op_time;

  std::mutex                     inner_queue_mtx;
  std::condition_variable        inner_queue_cv;
  std::deque<QueueItem>          inner_queue;

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
		 std::function<void()> notify_server_done);
	     
  void run();
};


#endif // _TEST_SERVER_H
