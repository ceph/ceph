// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
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
    ClientId client;
    std::unique_ptr<TestRequest> request;
    crimson::dmclock::PhaseType phase;

    QueueItem(const ClientId& _client,
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

  using ClientInfoFunc = std::function<crimson::dmclock::ClientInfo(ClientId)>;

  using ClientRespFunc =
    std::function<void(ClientId,
		       const TestResponse&,
		       const crimson::dmclock::RespParams<ServerId>&)>;

protected:

  const ServerId                 id;
  crimson::dmclock::PriorityQueue<ClientId,TestRequest> priority_queue;
  ClientRespFunc                 client_resp_f;
  int                            iops;
  size_t                         thread_pool_size;

  bool                           finishing;
  std::chrono::microseconds      op_time;

  std::mutex                     inner_queue_mtx;
  std::condition_variable        inner_queue_cv;
  std::deque<QueueItem>          inner_queue;

  std::thread*                   threads;

  using InnerQGuard = std::lock_guard<decltype(inner_queue_mtx)>;
  using Lock = std::unique_lock<std::mutex>;

  // data collection

  uint32_t reservation_counter = 0;
  uint32_t proportion_counter = 0;

public:

  TestServer(ServerId _id,
	     int _iops,
	     size_t _thread_pool_size,
	     const ClientInfoFunc& _client_info_f,
	     const ClientRespFunc& _client_resp_f,
	     bool use_soft_limit = false);

  virtual ~TestServer();

  void post(const TestRequest& request,
	    const crimson::dmclock::ReqParams<ClientId>& req_params);

  bool has_avail_thread();

  uint32_t get_res_count() const { return reservation_counter; }
  uint32_t get_prop_count() const { return proportion_counter; }

  const crimson::dmclock::PriorityQueue<ClientId,TestRequest>&
  get_priority_queue() const { return priority_queue; }

protected:

  void inner_post(const ClientId& client,
		  std::unique_ptr<TestRequest> request,
		  crimson::dmclock::PhaseType phase);

  void run(std::chrono::milliseconds wait_delay);

  inline void sendResponse(const ClientId& client,
			   const TestResponse& resp,
			   const crimson::dmclock::RespParams<ServerId>& resp_params) {
    client_resp_f(client, resp, resp_params);
  }
}; // class TestServer
