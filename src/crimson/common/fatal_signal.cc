// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "fatal_signal.h"

#include <csignal>
#include <iostream>
#include <string_view>

#define BOOST_STACKTRACE_USE_ADDR2LINE
#include <boost/stacktrace.hpp>
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
    if (static std::atomic_bool handled{false}; handled.exchange(true)) {
      return;
    }
    FatalSignal::signaled(sig, info);
    ::signal(sig, SIG_DFL);
  };
  sigfillset(&sa.sa_mask);
  sa.sa_flags = SA_SIGINFO | SA_RESTART;
  if constexpr (SigNum == SIGSEGV) {
    sa.sa_flags |= SA_ONSTACK;
  }
  [[maybe_unused]] auto r = ::sigaction(SigNum, &sa, nullptr);
  assert(r == 0);
}


static void print_backtrace(std::string_view cause) {
  std::cerr << cause;
  if (seastar::engine_is_ready()) {
    std::cerr << " on shard " << seastar::this_shard_id();
  }
  std::cerr << ".\nBacktrace:\n";
  std::cerr << boost::stacktrace::stacktrace();
  std::cerr << std::flush;
  // TODO: dump crash related meta data to $crash_dir
  //       see handle_fatal_signal()
}

static void print_segv_info(const siginfo_t* siginfo)
{
  std::cerr << "Fault at location: " << siginfo->si_addr << std::endl;
  std::cerr << std::flush;
}

void FatalSignal::signaled(const int signum, const siginfo_t* siginfo)
{
  switch (signum) {
  case SIGSEGV:
    print_backtrace("Segmentation fault");
    print_segv_info(siginfo);
    break;
  case SIGABRT:
    print_backtrace("Aborting");
    break;
  default:
    print_backtrace(fmt::format("Signal {}", signum));
    break;
  }
}
