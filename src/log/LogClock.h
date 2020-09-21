// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LOG_CLOCK_H
#define CEPH_LOG_CLOCK_H

#include <cstdio>
#include <chrono>
#include <ctime>
#include <sys/time.h>

#include "include/ceph_assert.h"
#include "common/ceph_time.h"

#ifndef suseconds_t
typedef long suseconds_t;
#endif

namespace ceph {
namespace logging {
namespace _logclock {
// Because the underlying representations of a duration can be any
// arithmetic type we wish, slipping a coarseness tag there is the
// least hacky way to tag them. I'd also considered doing bit-stealing
// and just setting the low bit of the representation unconditionally
// to mark it as fine, BUT that would cut our nanosecond precision in
// half which sort of obviates the point of 'fine'…admittedly real
// computers probably don't care. More to the point it wouldn't be
// durable under arithmetic unless we wrote a whole class to support
// it /anyway/, and if I'm going to do that I may as well add a bool.

// (Yes I know we don't do arithmetic on log timestamps, but I don't
// want everything to suddenly break because someone did something
// that the std::chrono::timepoint contract actually supports.)
struct taggedrep {
  uint64_t count;
  bool coarse;

  explicit taggedrep(uint64_t count) : count(count), coarse(true) {}
  taggedrep(uint64_t count, bool coarse) : count(count), coarse(coarse) {}

  explicit operator uint64_t() {
    return count;
  }
};

// Proper significant figure support would be a bit excessive. Also
// we'd have to know the precision of the clocks on Linux and FreeBSD
// and whatever else we want to support.
inline taggedrep operator +(const taggedrep& l, const taggedrep& r) {
  return { l.count + r.count, l.coarse || r.coarse };
}
inline taggedrep operator -(const taggedrep& l, const taggedrep& r) {
  return { l.count - r.count, l.coarse || r.coarse };
}
inline taggedrep operator *(const taggedrep& l, const taggedrep& r) {
  return { l.count * r.count, l.coarse || r.coarse };
}
inline taggedrep operator /(const taggedrep& l, const taggedrep& r) {
  return { l.count / r.count, l.coarse || r.coarse };
}
inline taggedrep operator %(const taggedrep& l, const taggedrep& r) {
  return { l.count % r.count, l.coarse || r.coarse };
}

// You can compare coarse and fine time. You shouldn't do so in any
// case where ordering actually MATTERS but in practice people won't
// actually ping-pong their logs back and forth between them.
inline bool operator ==(const taggedrep& l, const taggedrep& r) {
  return l.count == r.count;
}
inline bool operator !=(const taggedrep& l, const taggedrep& r) {
  return l.count != r.count;
}
inline bool operator <(const taggedrep& l, const taggedrep& r) {
  return l.count < r.count;
}
inline bool operator <=(const taggedrep& l, const taggedrep& r) {
  return l.count <= r.count;
}
inline bool operator >=(const taggedrep& l, const taggedrep& r) {
  return l.count >= r.count;
}
inline bool operator >(const taggedrep& l, const taggedrep& r) {
  return l.count > r.count;
}
}
class log_clock {
public:
  using rep = _logclock::taggedrep;
  using period = std::nano;
  using duration = std::chrono::duration<rep, period>;
  // The second template parameter defaults to the clock's duration
  // type.
  using time_point = std::chrono::time_point<log_clock>;
  static constexpr const bool is_steady = false;

  time_point now() noexcept {
    return appropriate_now();
  }

  void coarsen() {
    appropriate_now = coarse_now;
  }

  void refine() {
    appropriate_now = fine_now;
  }

  // Since our formatting is done in microseconds and we're using it
  // anyway, we may as well keep this one
  static timeval to_timeval(time_point t) {
    auto rep = t.time_since_epoch().count();
    timespan ts(rep.count);
    return { static_cast<time_t>(std::chrono::duration_cast<std::chrono::seconds>(ts).count()),
             static_cast<suseconds_t>(std::chrono::duration_cast<std::chrono::microseconds>(
               ts % std::chrono::seconds(1)).count()) };
  }
private:
  static time_point coarse_now() {
    return time_point(
      duration(_logclock::taggedrep(coarse_real_clock::now()
				    .time_since_epoch().count(), true)));
  }
  static time_point fine_now() {
    return time_point(
      duration(_logclock::taggedrep(real_clock::now()
				    .time_since_epoch().count(), false)));
  }
  time_point(*appropriate_now)() = coarse_now;
};
using log_time = log_clock::time_point;
inline int append_time(const log_time& t, char *out, int outlen) {
  bool coarse = t.time_since_epoch().count().coarse;
  auto tv = log_clock::to_timeval(t);
  std::tm bdt;
  localtime_r((time_t*)&tv.tv_sec, &bdt);
  char tz[32] = { 0 };
  strftime(tz, sizeof(tz), "%z", &bdt);

  int r;
  if (coarse) {
    r = std::snprintf(out, outlen, "%04d-%02d-%02dT%02d:%02d:%02d.%03ld%s",
		      bdt.tm_year + 1900, bdt.tm_mon + 1, bdt.tm_mday,
		      bdt.tm_hour, bdt.tm_min, bdt.tm_sec,
		      static_cast<long>(tv.tv_usec / 1000), tz);
  } else {
    r = std::snprintf(out, outlen, "%04d-%02d-%02dT%02d:%02d:%02d.%06ld%s",
		      bdt.tm_year + 1900, bdt.tm_mon + 1, bdt.tm_mday,
		      bdt.tm_hour, bdt.tm_min, bdt.tm_sec,
		      static_cast<long>(tv.tv_usec), tz);
  }
  // Since our caller just adds the return value to something without
  // checking it…
  ceph_assert(r >= 0);
  return r;
}
}
}

#endif
