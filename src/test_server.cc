// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#include <iostream>

#include "dmclock_recs.h"
#include "test_server.h"


using namespace std::placeholders;

namespace dmc = crimson::dmclock;


TestServer::TestServer(ServerId _id,
		       int _iops,
		       size_t _thread_pool_size,
		       const ClientInfoFunc& _client_info_f,
		       const ClientRespFunc& _client_resp_f,
		       bool use_soft_limit) :
  id(_id),
  priority_queue(_client_info_f,
		 std::bind(&TestServer::has_avail_thread, this),
		 std::bind(&TestServer::inner_post, this, _1, _2, _3),
		 use_soft_limit),
  client_resp_f(_client_resp_f),
  iops(_iops),
  thread_pool_size(_thread_pool_size),
  finishing(false)
{
  op_time =
    std::chrono::microseconds((int) (0.5 +
				     thread_pool_size * 1000000.0 / iops));
  std::chrono::milliseconds delay(1000);
  threads = new std::thread[thread_pool_size];
  for (size_t i = 0; i < thread_pool_size; ++i) {
    threads[i] = std::thread(&TestServer::run, this, delay);
  }
}


TestServer::~TestServer() {
  Lock l(inner_queue_mtx);
  finishing = true;
  inner_queue_cv.notify_all();
  l.unlock();

  for (size_t i = 0; i < thread_pool_size; ++i) {
    threads[i].join();
  }

  delete[] threads;
}


void TestServer::run(std::chrono::milliseconds check_period) {
  Lock l(inner_queue_mtx);
  while(true) {
    while(inner_queue.empty() && !finishing) {
      inner_queue_cv.wait_for(l, check_period);
    }
    if (!inner_queue.empty()) {
      auto& front = inner_queue.front();
      auto client = front.client;
      auto req = std::move(front.request);
      auto phase = front.phase;
      inner_queue.pop_front();

      l.unlock();

#if INFO > 3
      std::cout << "start req " << client << std::endl;
#endif
      
      // simulation operation by sleeping; then call function to
      // notify server of completion
      std::this_thread::sleep_for(op_time);

      TestResponse resp(req->epoch);
      sendResponse(client, resp, dmc::RespParams<ServerId>(id, phase));

      priority_queue.request_completed();

#if INFO > 3
	std::cout << "end req " << client << std::endl;
#endif

      l.lock(); // in prep for next iteration of loop
    } else {
      break;
    }
  }
}


void TestServer::post(const TestRequest& request,
		      const dmc::ReqParams<ClientId>& req_params) {
  auto now = dmc::get_time();
  priority_queue.add_request(request, req_params, now);
}


bool TestServer::has_avail_thread() {
  InnerQGuard g(inner_queue_mtx);
  return inner_queue.size() <= thread_pool_size;
}


void TestServer::inner_post(const ClientId& client,
			    std::unique_ptr<TestRequest> request,
			    dmc::PhaseType phase) {
  Lock l(inner_queue_mtx);
  assert(!finishing);
  if (dmc::PhaseType::reservation == phase) {
    ++reservation_counter;
  } else {
    ++proportion_counter;
  }
  inner_queue.emplace_back(QueueItem(client, std::move(request), phase));
  inner_queue_cv.notify_one();
}
