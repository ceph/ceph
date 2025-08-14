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

  PerfCounterInstance(enum perfcounter_type_d type)
  {
    if (type & PERFCOUNTER_LONGRUNAVG)
      avg_buffer = boost::circular_buffer<AvgDataPoint>(20);
    else
      buffer = boost::circular_buffer<DataPoint>(20);
  };
};

#endif
