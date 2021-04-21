// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

class FatalSignal {
public:
  FatalSignal();

private:
  static void signaled(int signum);
  static void print_backtrace(int signum);

  template <int... SigNums>
  void install_oneshot_signals_handler();

  template <int SigNum>
  void install_oneshot_signal_handler();
};
