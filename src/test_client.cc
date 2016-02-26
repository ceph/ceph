// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#include <chrono>
#include <iostream>

#include "dmclock_recs.h"
#include "test_client.h"


using namespace std::placeholders;
namespace dmc = crimson::dmclock;


static const uint info = 0;


TestClient::TestClient(ClientId _id,
		       const SubmitFunc& _submit_f,
		       const ServerSelectFunc& _server_select_f,
		       int _ops_to_run,
		       int _iops_goal,
		       int _outstanding_ops_allowed) :
  id(_id),
  submit_f(_submit_f),
  server_select_f(_server_select_f),
  ops_to_run(_ops_to_run),
  iops_goal(_iops_goal),
  outstanding_ops_allowed(_outstanding_ops_allowed),
  service_tracker(),
  op_times(ops_to_run),
  outstanding_ops(0),
  requests_complete(false)
{
  thd_resp = std::thread(&TestClient::run_resp, this);
  thd_req = std::thread(&TestClient::run_req, this);
}


TestClient::~TestClient() {
  wait_until_done();
}


void TestClient::wait_until_done() {
  if (thd_req.joinable()) thd_req.join();
  if (thd_resp.joinable()) thd_resp.join();
}


void TestClient::run_req() {
  std::chrono::microseconds delay(int(0.5 + 1000000.0 / iops_goal));

  if (info >= 1) {
    std::cout << "client " << id << " about to run " << ops_to_run <<
      " ops." << std::endl;
  }

  {
    Lock l(mtx_req);
    auto now = std::chrono::steady_clock::now();
    for (uint64_t i = 0; i < ops_to_run; ++i) {
      auto when = now + delay;
      while ((now = std::chrono::steady_clock::now()) < when) {
	cv_req.wait_until(l, when);
      }
      while (outstanding_ops >= outstanding_ops_allowed) {
	cv_req.wait(l);
      }

      l.unlock();
      const ServerId& server = server_select_f(i);
      dmc::ReqParams<ClientId> rp = service_tracker.get_req_params(id, server);
      TestRequest req(i, 12);
      submit_f(server, req, rp);
      ++outstanding_ops;
      l.lock(); // lock for return to top of loop
    }
  }

  requests_complete = true;

  if (info >= 1) {
    std::cout << "client " << id << " finished running " << ops_to_run <<
      " ops." << std::endl;
  }

  // all requests made, thread ends
}


void TestClient::run_resp() {
  std::chrono::milliseconds delay(1000);
  int op = 0;

  Lock l(mtx_resp);

  // since the following code would otherwise be repeated (except for
  // the call to notify_one) in the two loops below; let's avoid
  // repetition and define it once.
  const auto proc_resp = [this, &delay, &op, &l](const bool notify_req_cv) {
    if (!resp_queue.empty()) {
      RespQueueItem item = resp_queue.front();
      resp_queue.pop_front();

      l.unlock();

      // data collection

      op_times[op++] = now();
      if (dmc::PhaseType::reservation == item.resp_params.phase) {
	++reservation_counter;
      } else {
	++proportion_counter;
      }

      // processing

      TestResponse& resp = item.response;

      service_tracker.track_resp(item.resp_params);

      if (info >= 3) {
	std::cout << resp << std::endl;
      }
      --outstanding_ops;
      if (notify_req_cv) {
	cv_req.notify_one();
      }

      l.lock();
    }
  };

  while(!requests_complete.load()) {
    while(resp_queue.empty() && !requests_complete.load()) {
      cv_resp.wait_for(l, delay);
    }
    proc_resp(true);
  }

  if (info >= 1) {
    std::cout << "client " << id << " finishing " <<
      outstanding_ops.load() << " ops." << std::endl;
  }

  while(outstanding_ops.load() > 0) {
    while(resp_queue.empty() && outstanding_ops.load() > 0) {
      cv_resp.wait_for(l, delay);
    }
    proc_resp(false); // don't call notify_one as all requests are complete
  }

  if (info >= 1) {
    std::cout << "client " << id << " finished." << std::endl;
  }

  // all responses received, thread ends
}


void TestClient::receive_response(const TestResponse& resp,
				  const dmc::RespParams<ServerId>& resp_params) {
  RespGuard g(mtx_resp);
  resp_queue.push_back(RespQueueItem{resp, resp_params});
  cv_resp.notify_one();
}
