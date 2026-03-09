// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.	See file COPYING.
 *
 */

// For ceph_timespec
#include "ceph_time.h"

#include <fmt/chrono.h>
#include <fmt/ostream.h>

#include "include/rados.h" // for struct ceph_timespec
#include "log/LogClock.h"
#include "config.h"
#include "strtol.h"

#include <iomanip> // for std::setw()
#include <sstream>

#if defined(__APPLE__)
#include <mach/mach.h>
#include <mach/mach_time.h>


#ifndef NSEC_PER_SEC
#define NSEC_PER_SEC 1000000000ULL
#endif

int clock_gettime(int clk_id, struct timespec *tp)
{
  if (clk_id == CLOCK_REALTIME) {
    // gettimeofday is much faster than clock_get_time
    struct timeval now;
    int ret = gettimeofday(&now, NULL);
    if (ret)
      return ret;
    tp->tv_sec = now.tv_sec;
    tp->tv_nsec = now.tv_usec * 1000L;
  } else {
    uint64_t t = mach_absolute_time();
    static mach_timebase_info_data_t timebase_info;
    if (timebase_info.denom == 0) {
      (void)mach_timebase_info(&timebase_info);
    }
    auto nanos = t * timebase_info.numer / timebase_info.denom;
    tp->tv_sec = nanos / NSEC_PER_SEC;
    tp->tv_nsec = nanos - (tp->tv_sec * NSEC_PER_SEC);
  }
  return 0;
}
#endif

#if defined __x86_64__ or defined __i386__
#include <cpuid.h>
#endif // __x86_64__ or __i386__

#ifdef __linux__
#include <linux/perf_event.h>
#include <linux/version.h>
#include <sys/mman.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <unistd.h>
#include <memory>
#endif // __linux__

using namespace std::literals;

namespace ceph {
using std::chrono::seconds;
using std::chrono::nanoseconds;
void real_clock::to_ceph_timespec(const time_point& t,
				  struct ceph_timespec& ts) {
  ts.tv_sec = to_time_t(t);
  ts.tv_nsec = (t.time_since_epoch() % 1s).count();
}
struct ceph_timespec real_clock::to_ceph_timespec(const time_point& t) {
  struct ceph_timespec ts;
  to_ceph_timespec(t, ts);
  return ts;
}
real_clock::time_point real_clock::from_ceph_timespec(
  const struct ceph_timespec& ts) {
  return time_point(seconds(ts.tv_sec) + nanoseconds(ts.tv_nsec));
}

void coarse_real_clock::to_ceph_timespec(const time_point& t,
					 struct ceph_timespec& ts) {
  ts.tv_sec = to_time_t(t);
  ts.tv_nsec = (t.time_since_epoch() % seconds(1)).count();
}
struct ceph_timespec coarse_real_clock::to_ceph_timespec(
  const time_point& t) {
  struct ceph_timespec ts;
  to_ceph_timespec(t, ts);
  return ts;
}
coarse_real_clock::time_point coarse_real_clock::from_ceph_timespec(
  const struct ceph_timespec& ts) {
  return time_point(seconds(ts.tv_sec) + nanoseconds(ts.tv_nsec));
}


using std::chrono::duration_cast;
using std::chrono::seconds;
using std::chrono::microseconds;

template<typename Clock,
	 typename std::enable_if<Clock::is_steady>::type*>
std::ostream& operator<<(std::ostream& m,
			 const std::chrono::time_point<Clock>& t) {
  return m << std::fixed << std::chrono::duration<double>(
    t.time_since_epoch()).count()
	   << 's';
}

template<typename Clock,
	 typename std::enable_if<!Clock::is_steady>::type*>
std::ostream& operator<<(std::ostream& m,
			 const std::chrono::time_point<Clock>& t) {
  m.setf(std::ios::right);
  char oldfill = m.fill();
  m.fill('0');
  // localtime.  this looks like an absolute time.
  //  conform to http://en.wikipedia.org/wiki/ISO_8601
  struct tm bdt;
  time_t tt = Clock::to_time_t(t);
  localtime_r(&tt, &bdt);
  char tz[32] = { 0 };
  strftime(tz, sizeof(tz), "%z", &bdt);
  m << std::setw(4) << (bdt.tm_year+1900)  // 2007 -> '07'
    << '-' << std::setw(2) << (bdt.tm_mon+1)
    << '-' << std::setw(2) << bdt.tm_mday
    << 'T'
    << std::setw(2) << bdt.tm_hour
    << ':' << std::setw(2) << bdt.tm_min
    << ':' << std::setw(2) << bdt.tm_sec
    << "." << std::setw(6) << duration_cast<microseconds>(
      t.time_since_epoch() % seconds(1)).count()
    << tz;
  m.fill(oldfill);
  m.unsetf(std::ios::right);
  return m;
}

template std::ostream&
operator<< <mono_clock>(std::ostream& m, const mono_time& t);
template std::ostream&
operator<< <real_clock>(std::ostream& m, const real_time& t);
template std::ostream&
operator<< <coarse_mono_clock>(std::ostream& m, const coarse_mono_time& t);
template std::ostream&
operator<< <coarse_real_clock>(std::ostream& m, const coarse_real_time& t);

std::string timespan_str(timespan t)
{
  // FIXME: somebody pretty please make a version of this function
  // that isn't as lame as this one!
  uint64_t nsec = std::chrono::nanoseconds(t).count();
  std::ostringstream ss;
  if (nsec < 2'000'000'000) {
    ss << ((float)nsec / 1'000'000'000) << "s";
    return ss.str();
  }
  uint64_t sec = nsec / 1'000'000'000;
  if (sec < 120) {
    ss << sec << "s";
    return ss.str();
  }
  uint64_t min = sec / 60;
  if (min < 120) {
    ss << min << "m";
    return ss.str();
  }
  uint64_t hr = min / 60;
  if (hr < 48) {
    ss << hr << "h";
    return ss.str();
  }
  uint64_t day = hr / 24;
  if (day < 14) {
    ss << day << "d";
    return ss.str();
  }
  uint64_t wk = day / 7;
  if (wk < 12) {
    ss << wk << "w";
    return ss.str();
  }
  uint64_t mn = day / 30;
  if (mn < 24) {
    ss << mn << "M";
    return ss.str();
  }
  uint64_t yr = day / 365;
  ss << yr << "y";
  return ss.str();
}

std::string exact_timespan_str(timespan t)
{
  uint64_t nsec = std::chrono::nanoseconds(t).count();
  uint64_t sec = nsec / 1'000'000'000;
  nsec %= 1'000'000'000;
  uint64_t yr = sec / (60 * 60 * 24 * 365);
  std::ostringstream ss;
  if (yr) {
    ss << yr << "y";
    sec -= yr * (60 * 60 * 24 * 365);
  }
  uint64_t mn = sec / (60 * 60 * 24 * 30);
  if (mn >= 3) {
    ss << mn << "mo";
    sec -= mn * (60 * 60 * 24 * 30);
  }
  uint64_t wk = sec / (60 * 60 * 24 * 7);
  if (wk >= 2) {
    ss << wk << "w";
    sec -= wk * (60 * 60 * 24 * 7);
  }
  uint64_t day = sec / (60 * 60 * 24);
  if (day >= 2) {
    ss << day << "d";
    sec -= day * (60 * 60 * 24);
  }
  uint64_t hr = sec / (60 * 60);
  if (hr >= 2) {
    ss << hr << "h";
    sec -= hr * (60 * 60);
  }
  uint64_t min = sec / 60;
  if (min >= 2) {
    ss << min << "m";
    sec -= min * 60;
  }
  if (sec || nsec) {
    if (nsec) {
      ss << (((float)nsec / 1'000'000'000) + sec) << "s";
    } else {
      ss << sec << "s";
    }
  }
  return ss.str();
}

std::chrono::seconds parse_timespan(const std::string& s)
{
  static std::map<std::string,int> units = {
    { "s", 1 },
    { "sec", 1 },
    { "second", 1 },
    { "seconds", 1 },
    { "m", 60 },
    { "min", 60 },
    { "minute", 60 },
    { "minutes", 60 },
    { "h", 60*60 },
    { "hr", 60*60 },
    { "hour", 60*60 },
    { "hours", 60*60 },
    { "d", 24*60*60 },
    { "day", 24*60*60 },
    { "days", 24*60*60 },
    { "w", 7*24*60*60 },
    { "wk", 7*24*60*60 },
    { "week", 7*24*60*60 },
    { "weeks", 7*24*60*60 },
    { "mo", 30*24*60*60 },
    { "month", 30*24*60*60 },
    { "months", 30*24*60*60 },
    { "y", 365*24*60*60 },
    { "yr", 365*24*60*60 },
    { "year", 365*24*60*60 },
    { "years", 365*24*60*60 },
  };

  auto r = 0s;
  auto pos = 0u;
  while (pos < s.size()) {
    // skip whitespace
    while (std::isspace(s[pos])) {
      ++pos;
    }
    if (pos >= s.size()) {
      break;
    }

    // consume any digits
    auto val_start = pos;
    while (std::isdigit(s[pos])) {
      ++pos;
    }
    if (val_start == pos) {
      throw std::invalid_argument("expected digit");
    }
    auto n = s.substr(val_start, pos - val_start);
    std::string err;
    auto val = strict_strtoll(n.c_str(), 10, &err);
    if (err.size()) {
      throw std::invalid_argument(err);
    }

    // skip whitespace
    while (std::isspace(s[pos])) {
      ++pos;
    }

    // consume unit
    auto unit_start = pos;
    while (std::isalpha(s[pos])) {
      ++pos;
    }
    if (unit_start != pos) {
      auto unit = s.substr(unit_start, pos - unit_start);
      auto p = units.find(unit);
      if (p == units.end()) {
	throw std::invalid_argument("unrecogized unit '"s + unit + "'");
      }
      val *= p->second;
    } else if (pos < s.size()) {
      throw std::invalid_argument("unexpected trailing '"s + s.substr(pos) + "'");
    }
    r += std::chrono::seconds(val);
  }
  return r;
}

std::string
to_pretty_timedelta(timespan duration)
{
  using namespace std::chrono;

  auto duration_seconds = duration_cast<seconds>(duration).count();

  if (duration < seconds{120}) {
    return fmt::format("{}s", duration_seconds);
  }
  if (duration < minutes{120}) {
    return fmt::format("{}m", duration_seconds / 60);
  }
  if (duration < hours{48}) {
    return fmt::format("{}h", duration_seconds / 3600);
  }
  if (duration < hours{24 * 14}) {
    return fmt::format("{}d", duration_seconds / (3600 * 24));
  }
  if (duration < hours{24 * 7 * 12}) {
    return fmt::format("{}w", duration_seconds / (3600 * 24 * 7));
  }
  if (duration < hours{24 * 365 * 2}) {
    return fmt::format("{}M", duration_seconds / (3600 * 24 * 30));
  }
  return fmt::format("{}y", duration_seconds / (3600 * 24 * 365));
}

#if defined __x86_64__ or defined __i386__

const bool tsc_clock::is_available = tsc_clock::has_tsc() and
                                       tsc_clock::tsc_allowed();
const bool tsc_clock::is_steady = tsc_clock::has_invariant_tsc();

// check if the processor has a TSC (Time Stamp Counter) and supports the RDTSC instruction
bool
tsc_clock::has_tsc()
{
  unsigned int eax, ebx, ecx, edx;
  if (__get_cpuid(0x01, &eax, &ebx, &ecx, &edx))
    return (edx & bit_TSC) != 0;
  else
    return false;
}

// check if the processor supports the Invariant TSC feature (constant frequency TSC)
bool
tsc_clock::has_invariant_tsc()
{
  unsigned int eax, ebx, ecx, edx;
  if (__get_cpuid(0x80000007, &eax, &ebx, &ecx, &edx))
    return (edx & bit_InvariantTSC) != 0;
  else
    return false;
}

// calibrate TSC with respect to std::chrono::high_resolution_clock
double
tsc_clock::calibrate_tsc_hz_chrono()
{
  if (not has_tsc() or not tsc_allowed())
    return 0;

  constexpr unsigned int sample_size = 1000; // 1000 samples
  constexpr unsigned int sleep_time = 1000; //    1 ms
  unsigned long long ticks[sample_size];
  double times[sample_size];

  auto reference = std::chrono::high_resolution_clock::now();
  for (unsigned int i = 0; i < sample_size; ++i) {
    usleep(sleep_time);
    ticks[i] = rdtsc();
    times[i] = std::chrono::duration_cast<std::chrono::duration<double>>(
                   std::chrono::high_resolution_clock::now() - reference)
                   .count();
  }

  double mean_x = 0, mean_y = 0;
  for (unsigned int i = 0; i < sample_size; ++i) {
    mean_x += (double)times[i];
    mean_y += (double)ticks[i];
  }
  mean_x /= (double)sample_size;
  mean_y /= (double)sample_size;

  double sigma_xy = 0, sigma_xx = 0;
  for (unsigned int i = 0; i < sample_size; ++i) {
    sigma_xx += (double)(times[i] - mean_x) * (double)(times[i] - mean_x);
    sigma_xy += (double)(times[i] - mean_x) * (double)(ticks[i] - mean_y);
  }

  // ticks per second
  return sigma_xy / sigma_xx;
}

#if defined __linux__
double
tsc_clock::calibrate_tsc_hz_perf()
{
  if (not has_tsc() or not tsc_allowed())
    return 0;

  struct perf_event_attr pe = {
      .type = PERF_TYPE_SOFTWARE,
      .size = sizeof(struct perf_event_attr),
      .config = PERF_COUNT_SW_CPU_CLOCK,
      .disabled = 1,
      .exclude_kernel = 1,
      .exclude_hv = 1};

  const int raw_fd = syscall(SYS_perf_event_open, &pe, 0, -1, -1, 0);
  if (raw_fd < 0) {
    return calibrate_tsc_hz_chrono();
  }

  // Use std::unique_ptr to manage the file descriptor.
  // Value -1 is considered an invalid file descriptor.
  struct FdDeleter {
    void operator()(int* f) const {
      if (f) {
        if (*f >= 0) {
          close(*f);
        }
        delete f;
      }
    }
  };
  std::unique_ptr<int, FdDeleter> fd(new int(raw_fd));

  void* addr = mmap(NULL, 4 * 1024, PROT_READ, MAP_SHARED, *fd, 0);
  if (addr == MAP_FAILED) {
    return calibrate_tsc_hz_chrono();
  }

  // Use std::unique_ptr to manage the mmap'ed memory.
  struct MmapDeleter {
    void operator()(void* a) const {
      if (a != MAP_FAILED) {
        munmap(a, 4 * 1024);
      }
    }
  };
  std::unique_ptr<void, MmapDeleter> mmap_guard(addr);

  const struct perf_event_mmap_page* pc =
      reinterpret_cast<perf_event_mmap_page*>(mmap_guard.get());
  if (pc->cap_user_time != 1) {
    // If cap_user_time is not supported, we can't use this method.
    // We should not recurse indefinitely if it keeps failing.
    // The previous code was calling calibrate_tsc_hz_perf() again,
    // which seems risky if cap_user_time is never 1.
    return calibrate_tsc_hz_chrono();
  } else {
    __u32 mult_factor = pc->time_mult;
    __u16 shift_factor = pc->time_shift;
    uint64_t _cpu_ticks_per_sec = (1ULL << shift_factor) * 1000ULL * 1000ULL *
                                  1000ULL / mult_factor;
    return (double)_cpu_ticks_per_sec;
  }
}
#endif // __linux__

double
tsc_clock::calibrate_tsc_hz()
{
#if defined __linux__
  return calibrate_tsc_hz_perf();
#else
  return calibrate_tsc_hz_chrono();
#endif
}

// Check if the RDTSC and RDTSCP instructions are allowed in user space.
// This is controlled by the x86 control register 4, bit 4 (CR4.TSD), but that is only readable by the kernel.
// On Linux, the flag can be read (and possibly set) via the prctl interface.
#ifdef __linux__
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 26)
#define _HAS_PR_TSC_ENABLE
#endif // LINUX_VERSION_CODE >= KERNEL_VERSION(2, 6, 26)
#endif // __linux__

bool
tsc_clock::tsc_allowed()
{
#if defined __linux__ and defined _HAS_PR_TSC_ENABLE
  int tsc_val = 0;
  prctl(PR_SET_TSC, PR_TSC_ENABLE);
  prctl(PR_GET_TSC, &tsc_val);
  return (tsc_val == PR_TSC_ENABLE);
#else
  return true;
#endif
}

#undef _HAS_PR_TSC_ENABLE

// tsc_tick definitions
const double tsc_tick::ticks_per_second = tsc_clock::calibrate_tsc_hz();
const double tsc_tick::seconds_per_tick = 1. / tsc_tick::ticks_per_second;
// Fixed-point value for nanoseconds per tick with a 32-bit shift
const int64_t tsc_tick::nanoseconds_per_tick_shifted =
    (1000000000ll << 32) / tsc_tick::ticks_per_second;
// Fixed-point value for ticks per nanosecond with a 32-bit shift.
// Note: 4.294967296 is 2^32 / 10^9.
//const int64_t tsc_tick::ticks_per_nanosecond_shifted = (int64_t) ((((__int128_t) tsc_tick::ticks_per_second) << 32) / 1000000000ll);
const int64_t tsc_tick::ticks_per_nanosecond_shifted =
    (int64_t)llrint(tsc_tick::ticks_per_second * 4.294967296);
#endif

}

namespace std {
template<typename Rep, typename Period>
ostream& operator<<(ostream& m, const chrono::duration<Rep, Period>& t) {
  if constexpr (chrono::treat_as_floating_point_v<Rep> ||
                Period::den > 1) {
    using seconds_t = chrono::duration<float>;
    ::fmt::print(m, "{:.9}", chrono::duration_cast<seconds_t>(t));
  } else {
    ::fmt::print(m, "{}", t);
  }
  return m;
}

template ostream&
operator<< <::ceph::timespan::rep,
            ::ceph::timespan::period> (ostream&, const ::ceph::timespan&);

template ostream&
operator<< <::ceph::signedspan::rep,
            ::ceph::signedspan::period> (ostream&, const ::ceph::signedspan&);

template ostream&
operator<< <chrono::seconds::rep,
            chrono::seconds::period> (ostream&, const chrono::seconds&);

template ostream&
operator<< <chrono::milliseconds::rep,
            chrono::milliseconds::period> (ostream&, const chrono::milliseconds&);

} // namespace std
