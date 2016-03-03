// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#include <iostream>

#include "run_every.h"


// can define ADD_MOVE_SEMANTICS, although not fully debugged and tested


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
  finishing = true;
  cv.notify_all();
  thd.join();
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
