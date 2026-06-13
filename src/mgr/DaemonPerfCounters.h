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

#ifndef DAEMON_PERF_COUNTERS_H_
#define DAEMON_PERF_COUNTERS_H_

#include <map>
#include <string>

namespace ceph {
  class Formatter;
}

class MMgrReport;
class PerfCounterType;
class PerfCounterInstance;

typedef std::map<std::string, PerfCounterType> PerfCounterTypes;

// Performance counters for one daemon
class DaemonPerfCounters
{
  public:
  // The record of perf stat types, shared between daemons
  PerfCounterTypes &types;

  explicit DaemonPerfCounters(PerfCounterTypes &types_);
  ~DaemonPerfCounters() noexcept;

  std::map<std::string, PerfCounterInstance> instances;

  void update(const MMgrReport& report);

  void clear();
};

#endif
