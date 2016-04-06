// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
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


struct req_op_t {};
struct wait_op_t {};
constexpr struct req_op_t req_op {};
constexpr struct wait_op_t wait_op {};

enum class CliOp { req, wait };
struct CliInst {
  CliOp op;
  union {
    std::chrono::milliseconds wait_time;
    struct {
      uint16_t count;
      std::chrono::milliseconds time_bw_reqs;
      uint16_t max_outstanding;
    } req_params;
  } args;

  // D is a duration type
  template<typename D>
  CliInst(wait_op_t, D duration) :
    op(CliOp::wait)
  {
    args.wait_time =
      std::chrono::duration_cast<std::chrono::milliseconds>(duration);
  }

  CliInst(req_op_t,
	  uint16_t count, double ops_per_sec, uint16_t max_outstanding) :
    op(CliOp::req)
  {
    args.req_params.count = count;
    args.req_params.max_outstanding = max_outstanding;
    uint32_t ms = uint32_t(0.5 + 1.0 / ops_per_sec * 1000);
    args.req_params.time_bw_reqs = std::chrono::milliseconds(ms);
  }
};


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

  std::vector<CliInst> instructions;

  crimson::dmclock::ServiceTracker<ServerId> service_tracker;

  // TODO: use lock rather than atomic???
  std::atomic_ulong        outstanding_ops;
  std::atomic_bool         requests_complete;

  std::deque<RespQueueItem> resp_queue;

  std::mutex               mtx_req;
  std::condition_variable  cv_req;

  std::mutex               mtx_resp;
  std::condition_variable  cv_resp;

  using RespGuard = std::lock_guard<decltype(mtx_resp)>;
  using Lock = std::unique_lock<std::mutex>;

  // data collection

  std::vector<TimePoint>   op_times;
  uint32_t                 reservation_counter = 0;
  uint32_t                 proportion_counter = 0;

  std::thread              thd_req;
  std::thread              thd_resp;

public:

  TestClient(ClientId _id,
	     const SubmitFunc& _submit_f,
	     const ServerSelectFunc& _server_select_f,
	     const std::vector<CliInst>& _instrs);

  TestClient(ClientId _id,
	     const SubmitFunc& _submit_f,
	     const ServerSelectFunc& _server_select_f,
	     uint16_t _ops_to_run,
	     double _iops_goal,
	     uint16_t _outstanding_ops_allowed);

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
