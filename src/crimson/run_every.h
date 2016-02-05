// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#pragma once

#include <atomic>
#include <chrono>
#include <thread>
#include <mutex>
#include <condition_variable>


namespace crimson {

  // runs a given simple function object waiting wait_period
  // milliseconds between; the destructor stops the other thread
  // immediately
  class RunEvery {
    using Lock      = std::unique_lock<std::mutex>;
    using TimePoint = std::chrono::steady_clock::time_point;
        
    std::atomic_bool          finishing;
    std::chrono::milliseconds wait_period;
    std::thread               thd;
    std::function<void()>     body;
    std::mutex                mtx;
    std::condition_variable   cv;

  public:

    RunEvery(std::chrono::milliseconds _wait_period,
	     std::function<void()>     _body);
    ~RunEvery();

  protected:

    void run();
  };
}
