// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "fatal_signal.h"

#include <signal.h>
#include <iostream>
#include <string_view>

#include <seastar/core/reactor.hh>

FatalSignal::FatalSignal()
{
  auto fatal_signals = {SIGSEGV,
                        SIGABRT,
                        SIGBUS,
                        SIGILL,
                        SIGFPE,
                        SIGXCPU,
                        SIGXFSZ,
                        SIGSYS};

  for (auto signum : fatal_signals) {
    seastar::engine().handle_signal(signum, [signum, this] {
      signaled(signum);
    });
  }
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
  if (handled.exchange(true)) {
    return;
  }
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
  signal(signum, SIG_DFL);
}
