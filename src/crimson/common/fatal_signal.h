// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <csignal>

class FatalSignal {
public:
  FatalSignal();

private:
  static void signaled(int signum, const siginfo_t& siginfo);

  template <int... SigNums>
  void install_oneshot_signals_handler();

  template <int SigNum>
  void install_oneshot_signal_handler();
};
