// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */

#include <unistd.h>

#include "test_server.h"


using namespace std::placeholders;


typedef std::unique_lock<std::mutex> Lock;


TestServer::TestServer(int _iops,
		       int _thread_pool_size,
		       const std::function<ClientInfo(int)>& _clientInfoF) :
  priority_queue(_clientInfoF,
		 std::bind(&TestServer::hasAvailThread, this),
		 std::bind(&TestServer::innerPost, this, _1, _2)),
  iops(_iops),
  thread_pool_size(_thread_pool_size),
  active_threads(0),
  finishing(false)
{
#if 0
    std::chrono::microseconds op_time((int) (0.5 + 1000000.0 / iops / thread_pool_size));
#endif
    op_time = std::chrono::microseconds((int) (0.5 +
					       1000000.0 / iops / thread_pool_size));
}


TestServer::~TestServer() {
  // empty
}


void TestServer::run() {
  std::chrono::seconds delay(1);
  Lock l(inner_queue_mtx);
  while(!finishing) {
    while(inner_queue.size() == 0 && !finishing) {
      inner_queue_cv.wait_for(l, delay);
    }
    if (inner_queue.size() > 0) {
      auto item = std::move(inner_queue.front());
      inner_queue.pop_front();
      l.unlock();

      // simulation operation by sleeping; then call function to
      // notify server of completion
      std::this_thread::sleep_for(op_time);
      item.second();

      l.lock();
    } else {
      // since finishing and queue is empty, end thread
      l.unlock();
      return;
    }
  }
}


#if 0
void TestServer::post(double delay, std::function<void()> done) {
  Guard g(mtx);
  ++active_threads;
  std::thread t(&TestServer::run, this, delay, done);
  t.detach();
}
#endif


void TestServer::post(const TestRequest& request,
		      std::function<void()> done) {
#if 0
  Guard g(mtx);
  ++active_threads;
  std::thread t(&TestServer::run, this, delay, done);
  t.detach();
#endif
}


bool TestServer::hasAvailThread() {
  Lock l(inner_queue_mtx);
  return inner_queue.size() <= thread_pool_size;
}


void TestServer::innerPost(std::unique_ptr<TestRequest> request,
			   std::function<void()> notify_server_done) {
  Lock l(inner_queue_mtx);
  inner_queue.emplace_back(QueueItem(std::move(request), notify_server_done));
}
