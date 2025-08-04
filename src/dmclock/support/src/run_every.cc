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


#include "run_every.h"


// can define ADD_MOVE_SEMANTICS, although not fully debugged and tested



#ifdef WITH_CRIMSON
crimson::RunEvery::RunEvery(std::chrono::milliseconds _wait_period,
                   std::function<void()> _body)
  : wait_period(_wait_period), body(std::move(_body))
{
  timer.set_callback([this] {
    on_timer();
  });
}

void crimson::RunEvery::start()
{
  arm_timer();
}

crimson::RunEvery::~RunEvery() {
  timer.cancel();
}

void crimson::RunEvery::try_update(std::chrono::milliseconds _wait_period) {
  wait_period = _wait_period;
}

void crimson::RunEvery::arm_timer() {
  timer.arm(wait_period);
}

void crimson::RunEvery::on_timer() {
  body();
  arm_timer();  // reschedule again
}

#else
namespace chrono = std::chrono;


#ifdef ADD_MOVE_SEMANTICS
crimson::RunEvery::RunEvery()
{
  // empty
}


crimson::RunEvery& crimson::RunEvery::operator=(crimson::RunEvery&& other)
{
  // finish run every thread
  {
    Guard g(mtx);
    finishing = true;
    cv.notify_one();
  }
  if (thd.joinable()) {
    thd.join();
  }

  // transfer info over from previous thread
  finishing.store(other.finishing);
  wait_period = other.wait_period;
  body = other.body;

  // finish other thread
  other.finishing.store(true);
  other.cv.notify_one();

  // start this thread
  thd = std::thread(&RunEvery::run, this);

  return *this;
}
#endif


crimson::RunEvery::~RunEvery() {
  join();
}


void crimson::RunEvery::join() {
  {
    Guard l(mtx);
    if (finishing) return;
    finishing = true;
    cv.notify_all();
  }
  thd.join();
}

// mtx must be held by caller
void crimson::RunEvery::try_update(milliseconds _wait_period) {
  if (_wait_period != wait_period) {
    wait_period = _wait_period;
  }
}

void crimson::RunEvery::run() {
  Lock l(mtx);
  while(!finishing) {
    TimePoint until = chrono::steady_clock::now() + wait_period;
    while (!finishing && chrono::steady_clock::now() < until) {
      cv.wait_until(l, until);
    }
    if (!finishing) {
      body();
    }
  }
}

#endif
