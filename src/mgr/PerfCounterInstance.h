// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2016 John Spray <john.spray@redhat.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#ifndef PERF_COUNTER_INSTANCE_H_
#define PERF_COUNTER_INSTANCE_H_

#include <cstdint>
#include <optional>

#include <boost/circular_buffer.hpp>

#include "common/perf_counters.h" // for enum perfcounter_type_d
#include "include/utime.h"

// An instance of a performance counter type, within
// a particular daemon.
class PerfCounterInstance
{
  class DataPoint
  {
    public:
    utime_t t;
    uint64_t v;
    DataPoint(utime_t t_, uint64_t v_)
      : t(t_), v(v_)
    {}
  };

  class AvgDataPoint
  {
    public:
    utime_t t;
    uint64_t s;
    uint64_t c;
    AvgDataPoint(utime_t t_, uint64_t s_, uint64_t c_)
      : t(t_), s(s_), c(c_)
    {}
  };

  boost::circular_buffer<DataPoint> buffer;
  boost::circular_buffer<AvgDataPoint> avg_buffer;

  uint64_t get_current() const;

  public:
  const boost::circular_buffer<DataPoint> & get_data() const
  {
    return buffer;
  }
  const DataPoint& get_latest_data() const
  {
    return buffer.back();
  }
  const boost::circular_buffer<AvgDataPoint> & get_data_avg() const
  {
    return avg_buffer;
  }
  const AvgDataPoint& get_latest_data_avg() const
  {
    return avg_buffer.back();
  }
  void push(utime_t t, uint64_t const &v);
  void push_avg(utime_t t, uint64_t const &s, uint64_t const &c);

  // Compute a per-op average latency for display. Prefers the delta
  // between the two most recent samples in which the op count advanced
  // (walking back through the buffer past any idle samples), since that
  // reflects current behavior. `max_age` bounds the walk-back: samples
  // older than `latest.t - max_age` are ignored, so a long idle stretch
  // can't drag a stale "busy" reference point into the displayed value.
  // Pass a zero utime_t to disable the bound.
  //
  // Falls back to the lifetime cumulative `s/c` only when a single
  // sample is buffered (right after a mgr restart, before any delta is
  // possible). When multiple samples are buffered but none within the
  // window show op activity, returns nullopt - the counter is currently
  // idle and the caller can render a dash rather than a stale value.
  std::optional<uint64_t> get_current_avg(utime_t max_age = utime_t()) const {
    if (avg_buffer.empty()) {
      return std::nullopt;
    }
    const auto& latest = avg_buffer.back();
    for (size_t i = avg_buffer.size() - 1; i-- > 0; ) {
      const auto& prev = avg_buffer[i];
      if (!max_age.is_zero() && (latest.t - prev.t) > max_age) {
        break;
      }
      if (latest.c > prev.c) {
        return (latest.s - prev.s) / (latest.c - prev.c);
      }
    }
    if (avg_buffer.size() == 1 && latest.c > 0) {
      return latest.s / latest.c;
    }
    return std::nullopt;
  }

  PerfCounterInstance(enum perfcounter_type_d type)
  {
    if (type & PERFCOUNTER_LONGRUNAVG)
      avg_buffer = boost::circular_buffer<AvgDataPoint>(20);
    else
      buffer = boost::circular_buffer<DataPoint>(20);
  };
};

#endif
