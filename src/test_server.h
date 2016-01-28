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

#include "dmclock_recs.h"
#include "dmclock_server.h"
#include "test_recs.h"




class TestServer {

  struct QueueItem {
    int client;
    std::unique_ptr<TestRequest> request;
    crimson::dmclock::PhaseType phase;

    QueueItem(const int& _client,
	      std::unique_ptr<TestRequest>&& _request,
	      crimson::dmclock::PhaseType _phase) :
      client(_client),
      request(std::move(_request)),
      phase(_phase)
    {
      // empty
    }
  };
  

public:

#warning "rename this ClientRespFunc"
  using ClientRespFunc =
    std::function<void(int,
		       const TestResponse&,
		       const crimson::dmclock::RespParams<int>&)>;

protected:

  const int                      id;
  crimson::dmclock::PriorityQueue<int,TestRequest> priority_queue;
  ClientRespFunc                 client_resp_f;
  int                            iops;
  int                            thread_pool_size;

  bool                           finishing;
  std::chrono::microseconds      op_time;

  std::mutex                     inner_queue_mtx;
  std::condition_variable        inner_queue_cv;
  std::deque<QueueItem>          inner_queue;

  std::thread*                   threads;

  using InnerQGuard = std::lock_guard<decltype(inner_queue_mtx)>;
  using Lock = std::unique_lock<std::mutex>;

public:

  // TestServer(int _thread_pool_size);
  TestServer(int _id,
	     int _iops,
	     int _thread_pool_size,
	     const std::function<crimson::dmclock::ClientInfo(int)>& _client_info_f,
	     const ClientRespFunc& _client_resp_f);

  virtual ~TestServer();

  void post(const TestRequest& request,
	    const crimson::dmclock::ReqParams<int>& req_params);

  bool hasAvailThread();

protected:

  void innerPost(const int& client,
		 std::unique_ptr<TestRequest> request,
		 crimson::dmclock::PhaseType phase);

  void run(std::chrono::milliseconds wait_delay);

  inline void sendResponse(int client,
			   const TestResponse& resp,
			   const crimson::dmclock::RespParams<int>& resp_params) {
    client_resp_f(client, resp, resp_params);
  }
}; // class TestServer
