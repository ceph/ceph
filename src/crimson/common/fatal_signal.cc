// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "fatal_signal.h"

#include <csignal>
#include <iostream>
#include <string_view>

#include <seastar/core/reactor.hh>

FatalSignal::FatalSignal()
{
  install_oneshot_signals_handler<SIGSEGV,
                                  SIGABRT,
                                  SIGBUS,
                                  SIGILL,
                                  SIGFPE,
                                  SIGXCPU,
                                  SIGXFSZ,
                                  SIGSYS>();
}

template <int... SigNums>
void FatalSignal::install_oneshot_signals_handler()
{
  (install_oneshot_signal_handler<SigNums>() , ...);
}

template <int SigNum>
void FatalSignal::install_oneshot_signal_handler()
{
  struct sigaction sa;
  sa.sa_sigaction = [](int sig, siginfo_t *info, void *p) {
    static std::atomic_bool handled = false;
    if (handled.exchange(true)) {
      return;
    }
    FatalSignal::signaled(sig);
    ::signal(sig, SIG_DFL);
  };
  sigfillset(&sa.sa_mask);
  sa.sa_flags = SA_SIGINFO | SA_RESTART;
  if constexpr (SigNum == SIGSEGV) {
      sa.sa_flags |= SA_ONSTACK;
  }
  [[maybe_unused]] auto r = ::sigaction(SigNum, &sa, nullptr);
  assert(r);
}


static void print_with_backtrace(std::string_view cause) {
  std::cerr << cause;
  if (seastar::engine_is_ready()) {
    std::cerr << " on shard " << seastar::this_shard_id();
  }
  std::cerr << ".\nBacktrace:\n";
#if 0
  std::cerr << symbolized::current_backtrace_tasklocal();
#endif
  std::cerr << std::flush;
  // TODO: dump crash related meta data to $crash_dir
  //       see handle_fatal_signal()
}

void FatalSignal::signaled(int signum)
{
  switch (signum) {
  case SIGSEGV:
    print_with_backtrace("Aborting");
    break;
  case SIGABRT:
    print_with_backtrace("Segmentation fault");
    break;
  default:
    print_with_backtrace(fmt::format("Signal {}", signum));
    break;
  }
}
