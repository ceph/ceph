// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */

#include <unistd.h>

#include "test_server.h"


using namespace std::placeholders;
namespace dmc = crimson::dmclock;


typedef std::unique_lock<std::mutex> Lock;


TestServer::TestServer(int _iops,
		       int _thread_pool_size,
		       const std::function<ClientInfo(int)>& _clientInfoF) :
  priority_queue(_clientInfoF,
		 std::bind(&TestServer::hasAvailThread, this),
		 std::bind(&TestServer::innerPost, this, _1)),
  iops(_iops),
  thread_pool_size(_thread_pool_size),
  finishing(false)
{
    op_time =
      std::chrono::microseconds((int) (0.5 +
				       thread_pool_size * 1000000.0 / iops));

    std::chrono::milliseconds delay(1000);
    threads = new std::thread[thread_pool_size];
    for (int i = 0; i < thread_pool_size; ++i) {
      threads[i] = std::thread(&TestServer::run, this, delay);
    }
}


TestServer::~TestServer() {
  Lock l(inner_queue_mtx);
  finishing = true;
  inner_queue_cv.notify_all();
  l.unlock();

  for (int i = 0; i < thread_pool_size; ++i) {
    threads[i].join();
  }

  delete[] threads;
}


void TestServer::run(std::chrono::milliseconds wait_delay) {
  Lock l(inner_queue_mtx);
  while(true) {
    while(inner_queue.empty() && !finishing) {
      inner_queue_cv.wait_for(l, wait_delay);
    }
    if (!inner_queue.empty()) {
      auto item = std::move(inner_queue.front());
      inner_queue.pop_front();
      l.unlock();

      // simulation operation by sleeping; then call function to
      // notify server of completion
      std::this_thread::sleep_for(op_time);

      // send response

      priority_queue.requestCompleted();

      l.lock(); // in prep for next iteration of loop
    } else {
      // since finishing and queue is empty, end thread
      l.unlock();
      return;
    }
  }
}


void TestServer::post(const TestRequest& request) {
  auto now = dmc::getTime();
  priority_queue.addRequest(request, request.client, now);
}


bool TestServer::hasAvailThread() {
  Lock l(inner_queue_mtx);
  return inner_queue.size() <= thread_pool_size;
}


void TestServer::innerPost(std::unique_ptr<TestRequest> request) {
  Lock l(inner_queue_mtx);
  assert(!finishing);
  inner_queue.emplace_back(std::move(request));
}
