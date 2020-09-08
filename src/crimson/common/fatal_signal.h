// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <atomic>

class FatalSignal {
public:
  FatalSignal();

private:
  void signaled(int signum);
  static void print_backtrace(int signum);
  std::atomic<bool> handled = false;
};
