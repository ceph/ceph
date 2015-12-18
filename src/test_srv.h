// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2015 Red Hat Inc.
 */


#include <mutex>
#include <thread>


class TestServer {

  int        active_threads;
  int        thread_pool_size;
  std::mutex mtx;

  typedef typename std::lock_guard<std::mutex> Guard;

public:

  TestServer(int _thread_pool_size);

  virtual ~TestServer();

  void post(std::function<void()> done);

  bool hasAvailThread();

protected:

  void run(double time, std::function<void()> done);
};
