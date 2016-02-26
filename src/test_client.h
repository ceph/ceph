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
#include <chrono>
#include <vector>
#include <deque>

#include "test_recs.h"
#include "dmclock_client.h"


class TestClient {
public:

  using SubmitFunc =
    std::function<void(const ServerId&,
		       const TestRequest&,
		       const crimson::dmclock::ReqParams<ClientId>&)>;

  using ServerSelectFunc = std::function<const ServerId&(uint64_t seed)>;

  typedef std::chrono::time_point<std::chrono::steady_clock> TimePoint;

  static TimePoint now() { return std::chrono::steady_clock::now(); }

protected:

  struct RespQueueItem {
    TestResponse response;
    crimson::dmclock::RespParams<ServerId> resp_params;
  };

  const ClientId id;
  const SubmitFunc submit_f;
  const ServerSelectFunc server_select_f;

  const int ops_to_run;
  const int iops_goal; // per second
  const int outstanding_ops_allowed;

  crimson::dmclock::ServiceTracker<ServerId> service_tracker;

  std::atomic_ulong        outstanding_ops;
  std::atomic_bool         requests_complete;

  std::deque<RespQueueItem> resp_queue;

  std::mutex               mtx_req;
  std::condition_variable  cv_req;
  std::thread              thd_req;

  std::mutex               mtx_resp;
  std::condition_variable  cv_resp;
  std::thread              thd_resp;

  using RespGuard = std::lock_guard<decltype(mtx_resp)>;
  using Lock = std::unique_lock<std::mutex>;

  // data collection

  std::vector<TimePoint>   op_times;
  uint32_t                 reservation_counter = 0;
  uint32_t                 proportion_counter = 0;

public:

  TestClient(ClientId _id,
	     const SubmitFunc& _submit_f,
	     const ServerSelectFunc& _server_select_f,
	     int _ops_to_run,
	     int _iops_goal,
	     int _outstanding_ops_allowed);

  TestClient(const TestClient&) = delete;
  TestClient(TestClient&&) = delete;
  TestClient& operator=(const TestClient&) = delete;
  TestClient& operator=(TestClient&&) = delete;

  virtual ~TestClient();

  void receive_response(const TestResponse&,
			const crimson::dmclock::RespParams<ServerId>&);

  const std::vector<TimePoint>& get_op_times() const { return op_times; }
  uint32_t get_res_count() const { return reservation_counter; }
  uint32_t get_prop_count() const { return proportion_counter; }

  void wait_until_done();

protected:

  void run_req();
  void run_resp();
}; // class TestClient
