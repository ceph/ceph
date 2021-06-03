// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/condition-variable.hh>
#include <seastar/core/sharded.hh>

class StopSignal {
public:
  StopSignal();
  ~StopSignal();
  seastar::future<> wait();

private:
  void signaled();

private:
  bool caught;
  seastar::condition_variable should_stop;
};
