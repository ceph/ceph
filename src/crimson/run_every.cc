// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (C) 2016 Red Hat Inc.
 */


#include "run_every.h"


crimson::RunEvery::RunEvery(std::chrono::milliseconds _wait_period,
                            std::function<void()>      _body) :
  wait_period(_wait_period),
  body(_body),
  finishing(false),
  thd(&RunEvery::run, this)
{
  // empty
}


crimson::RunEvery::~RunEvery() {
  finishing = true;
  cv.notify_all();
  thd.join();
}


void crimson::RunEvery::run() {
  Lock l(mtx);
  while(!finishing) {
    TimePoint until = std::chrono::steady_clock::now() + wait_period;
    while (std::chrono::steady_clock::now() < until) {
      cv.wait_until(l, until);
      if (finishing) return;
    }
    body();
  }
}
