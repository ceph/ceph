// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Copyright (C) 2017 Red Hat Inc.
 *
 * Author: J. Eric Ivancich <ivancich@redhat.com>
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License version
 * 2.1, as published by the Free Software Foundation.  See file
 * COPYING.
 */


#pragma once

#include <chrono>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <functional>


namespace crimson {
  using std::chrono::duration_cast;
  using std::chrono::milliseconds;

  // runs a given simple function object waiting wait_period
  // milliseconds between; the destructor stops the other thread
  // immediately
  class RunEvery {
    using Lock      = std::unique_lock<std::mutex>;
    using Guard     = std::lock_guard<std::mutex>;
    using TimePoint = std::chrono::steady_clock::time_point;

    bool                      finishing = false;
    std::chrono::milliseconds wait_period;
    std::function<void()>     body;
    std::mutex                mtx;
    std::condition_variable   cv;

    // put threads last so all other variables are initialized first

    std::thread               thd;

  public:

#ifdef ADD_MOVE_SEMANTICS
    RunEvery();
#endif

    template<typename D>
    RunEvery(D                     _wait_period,
	     const std::function<void()>& _body) :
      wait_period(duration_cast<milliseconds>(_wait_period)),
      body(_body)
    {
      thd = std::thread(&RunEvery::run, this);
    }

    RunEvery(const RunEvery& other) = delete;
    RunEvery& operator=(const RunEvery& other) = delete;
    RunEvery(RunEvery&& other) = delete;
#ifdef ADD_MOVE_SEMANTICS
    RunEvery& operator=(RunEvery&& other);
#else
    RunEvery& operator=(RunEvery&& other) = delete;
#endif

    ~RunEvery();

    void join();
    // update wait period in milliseconds
    void try_update(milliseconds _wait_period);

  protected:

    void run();
  };
}
