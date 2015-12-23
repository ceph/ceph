// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */

#include <unistd.h>

#include "test_server.h"


using namespace std::placeholders;


TestServer::TestServer(int _thread_pool_size,
		       const std::function<ClientInfo(int)>& _clientInfoF) :
  active_threads(0),
  thread_pool_size(_thread_pool_size),
  queue(_clientInfoF,
	std::bind(&TestServer::hasAvailThread, this),
	std::bind(&TestServer::innerPost, this, _1, _2))
{
  // empty
}


TestServer::~TestServer() {
  // empty
}


void TestServer::run(double time, std::function<void()> done) {
  usleep((useconds_t) (1000000 * time));
  done();
  Guard g(mtx);
  --active_threads;
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
  Guard g(mtx);
  return active_threads <= thread_pool_size;
}


void TestServer::innerPost(std::unique_ptr<TestRequest> request,
			   std::function<void()> done) {
  assert(0);
}
