// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#pragma once


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

  typedef std::lock_guard<std::mutex>  Guard;
  typedef std::unique_ptr<TestRequest> QueueItem;

public:

  typedef std::function<void(int,const TestResponse&)> ClientResponseFunc;

protected:

  PriorityQueue<int,TestRequest> priority_queue;
  ClientResponseFunc             client_resp_f;
  int                            iops;
  int                            thread_pool_size;

  bool                           finishing;
  std::chrono::microseconds      op_time;

  std::mutex                     inner_queue_mtx;
  std::condition_variable        inner_queue_cv;
  std::deque<QueueItem>          inner_queue;

  std::thread*                   threads;

public:

  // TestServer(int _thread_pool_size);
  TestServer(int iops,
	     int _thread_pool_size,
	     const std::function<ClientInfo(int)>& _client_info_f,
	     const ClientResponseFunc& _client_resp_f);

  virtual ~TestServer();

  // void post(double delay, std::function<void()> done);
  void post(const TestRequest& request);

  bool hasAvailThread();

protected:

  void innerPost(std::unique_ptr<TestRequest> request);

  void run(std::chrono::milliseconds wait_delay);

  inline void sendResponse(int client, const TestResponse& resp) {
    client_resp_f(client, resp);
  }
}; // class TestServer
