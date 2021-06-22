// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "fatal_signal.h"

#include <csignal>
#include <iostream>
#include <string_view>

#define BOOST_STACKTRACE_USE_ADDR2LINE
#include <boost/stacktrace.hpp>
#include <seastar/core/reactor.hh>

#include "common/safe_io.h"
#include "include/scope_guard.h"

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
    assert(info);
    FatalSignal::signaled(sig, *info);
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

static void print_segv_info(const siginfo_t& siginfo)
{
  std::cerr \
     << "Dump of siginfo:" << std::endl
     << "  si_signo: " << siginfo.si_signo << std::endl
     << "  si_errno: " << siginfo.si_errno << std::endl
     << "  si_code: " << siginfo.si_code << std::endl
     << "  si_pid: " << siginfo.si_pid << std::endl
     << "  si_uid: " << siginfo.si_uid << std::endl
     << "  si_status: " << siginfo.si_status << std::endl
     << "  si_utime: " << siginfo.si_utime << std::endl
     << "  si_stime: " << siginfo.si_stime << std::endl
     << "  si_int: " << siginfo.si_int << std::endl
     << "  si_ptr: " << siginfo.si_ptr << std::endl
     << "  si_overrun: " << siginfo.si_overrun << std::endl
     << "  si_timerid: " << siginfo.si_timerid << std::endl
     << "  si_addr: " << siginfo.si_addr << std::endl
     << "  si_band: " << siginfo.si_band << std::endl
     << "  si_fd: " << siginfo.si_fd << std::endl
     << "  si_addr_lsb: " << siginfo.si_addr_lsb << std::endl
     << "  si_lower: " << siginfo.si_lower << std::endl
     << "  si_upper: " << siginfo.si_upper << std::endl
     << "  si_pkey: " << siginfo.si_pkey << std::endl
     << "  si_call_addr: " << siginfo.si_call_addr << std::endl
     << "  si_syscall: " << siginfo.si_syscall << std::endl
     << "  si_arch: " << siginfo.si_arch << std::endl;
  std::cerr << std::flush;
}

static void print_proc_maps()
{
  const int fd = ::open("/proc/self/maps", O_RDONLY);
  if (fd < 0) {
    std::cerr << "can't open /proc/self/maps. procfs not mounted?" << std::endl;
    return;
  }
  const auto fd_guard = make_scope_guard([fd] {
    ::close(fd);
  });
  std::cerr << "Content of /proc/self/maps:" << std::endl;
  while (true) {
    char chunk[4096] = {0, };
    const ssize_t r = safe_read(fd, chunk, sizeof(chunk) - 1);
    if (r < 0) {
      std::cerr << "error while reading /proc/self/maps: " << r << std::endl;
      return;
    } else {
      std::cerr << chunk << std::flush;
      if (r < static_cast<ssize_t>(sizeof(chunk) - 1)) {
        return; // eof
      }
    }
  }
}

void FatalSignal::signaled(const int signum, const siginfo_t& siginfo)
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
  print_proc_maps();
}
