// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#ifndef _TEST_SERVER_H
#define _TEST_SERVER_H


#include <mutex>
#include <thread>

#include "dm_clock_srv.h"
#include "test_request.h"


using crimson::dmclock::PriorityQueue;


class TestServer {

  PriorityQueue<int,TestRequest> queue;
  int        active_threads;
  int        thread_pool_size;
  std::mutex mtx;

  typedef typename std::lock_guard<std::mutex> Guard;

public:

  TestServer(int _thread_pool_size);

  virtual ~TestServer();

  void post(double delay, std::function<void()> done);
  void post(const TestRequest& request, std::function<void()> done);

  bool hasAvailThread();

protected:

  void run(double time, std::function<void()> done);
};


#endif // _TEST_SERVER_H
