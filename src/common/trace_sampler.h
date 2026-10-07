// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdint>
#include <limits>

#include "include/utime.h"

namespace tracing {

// Chooses the slow requests to trace when there are more than a daemon may
// export. A daemon keeps a request when the request's trace id falls below
// its share of the id space, and sets the share every second from how many
// slow requests it saw in the last one, so that it keeps about max_per_sec.
// An id below one share is below every larger one, so a daemon that keeps
// fewer requests keeps some of the ones that the others keep, not others:
// RGW and the OSDs a request reaches export its whole trace or none of it.
//
// The share follows the rate a second late, so in the first second of a
// burst a daemon keeps the first requests, up to twice max_per_sec, whatever
// their ids. Not thread safe.
class TraceSampler {
 public:
  // the part of a trace id that the sampler compares; its last 8 bytes, which
  // are random in W3C trace ids and a hash in the ones derived from a reqid
  static uint64_t key(const std::array<uint8_t, 16>& trace_id) {
    uint64_t k = 0;
    for (size_t i = 8; i < trace_id.size(); i++) {
      k = (k << 8) | trace_id[i];
    }
    return k;
  }

  bool admit(uint64_t key, utime_t now, uint32_t max_per_sec) {
    if (max_per_sec == 0) {
      return false;
    }
    if (window_start.is_zero()) {
      window_start = now;
    } else if (const double elapsed = now - window_start; elapsed >= 1.0) {
      const double rate = seen / elapsed;
      limit = rate <= max_per_sec ?
        std::numeric_limits<uint64_t>::max() :
        uint64_t(std::ldexp(max_per_sec / rate, 64));
      window_start = now;
      seen = 0;
      kept = 0;
    }
    ++seen;
    if (key > limit || kept >= 2 * uint64_t(max_per_sec)) {
      return false;
    }
    ++kept;
    return true;
  }

 private:
  utime_t window_start;
  uint64_t seen = 0;  // slow requests in this window
  uint64_t kept = 0;
  uint64_t limit = std::numeric_limits<uint64_t>::max();
};

} // namespace tracing
