// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */

#include <unistd.h>

#include "test_srv.h"


TestServer::TestServer(int _thread_pool_size) :
  active_threads(0),
  thread_pool_size(_thread_pool_size)
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


void TestServer::post(double delay, std::function<void()> done) {
  Guard g(mtx);
  ++active_threads;
  std::thread t(&TestServer::run, this, delay, done);
  t.detach();
}


bool TestServer::hasAvailThread() {
  Guard g(mtx);
  return active_threads <= thread_pool_size;
}
