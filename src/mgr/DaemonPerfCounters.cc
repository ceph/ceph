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

#include "DaemonPerfCounters.h"
#include "PerfCounterInstance.h"
#include "MgrSession.h"
#include "common/Clock.h" // for ceph_clock_now()
#include "common/debug.h"
#include "messages/MMgrReport.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "

DaemonPerfCounters::DaemonPerfCounters(PerfCounterTypes &types_)
  : types(types_)
{}

DaemonPerfCounters::~DaemonPerfCounters() noexcept = default;

void DaemonPerfCounters::update(const MMgrReport& report)
{
  dout(20) << "loading " << report.declare_types.size() << " new types, "
	   << report.undeclare_types.size() << " old types, had "
	   << types.size() << " types, got "
           << report.packed.length() << " bytes of data" << dendl;

  // Retrieve session state
  auto priv = report.get_connection()->get_priv();
  auto session = static_cast<MgrSession*>(priv.get());

  // Load any newly declared types
  for (const auto &t : report.declare_types) {
    types.insert(std::make_pair(t.path, t));
    session->declared_types.insert(t.path);
  }
  // Remove any old types
  for (const auto &t : report.undeclare_types) {
    session->declared_types.erase(t);
  }

  const auto now = ceph_clock_now();

  // Parse packed data according to declared set of types
  auto p = report.packed.cbegin();
  DECODE_START(1, p);
  for (const auto &t_path : session->declared_types) {
    const auto &t = types.at(t_path);
    auto instances_it = instances.find(t_path);
    // Always check the instance exists, as we don't prevent yet
    // multiple sessions from daemons with the same name, and one
    // session clearing stats created by another on open.
    if (instances_it == instances.end()) {
      instances_it = instances.insert({t_path, t.type}).first;
    }
    uint64_t val = 0;
    uint64_t avgcount = 0;
    uint64_t avgcount2 = 0;

    decode(val, p);
    if (t.type & PERFCOUNTER_LONGRUNAVG) {
      decode(avgcount, p);
      decode(avgcount2, p);
      instances_it->second.push_avg(now, val, avgcount);
    } else {
      instances_it->second.push(now, val);
    }
  }
  DECODE_FINISH(p);
}

void DaemonPerfCounters::clear()
{
  instances.clear();
}
