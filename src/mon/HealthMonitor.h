// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#ifndef CEPH_HEALTH_MONITOR_H
#define CEPH_HEALTH_MONITOR_H

#include "mon/QuorumService.h"

//forward declaration
namespace ceph { class Formatter; }
class HealthService;

class HealthMonitor : public QuorumService
{
  map<int,HealthService*> services;

protected:
  void service_shutdown() override;

public:
  HealthMonitor(Monitor *m) : QuorumService(m) { }
  ~HealthMonitor() override {
    assert(services.empty());
  }


  /**
   * @defgroup HealthMonitor_Inherited_h Inherited abstract methods
   * @{
   */
  void init() override;
  void get_health(Formatter *f,
		  list<pair<health_status_t,string> >& summary,
		  list<pair<health_status_t,string> > *detail) override;
  bool service_dispatch(MonOpRequestRef op) override;

  void start_epoch() override;

  void finish_epoch() override;

  void cleanup() override { }
  void service_tick() override { }

  int get_type() override {
    return QuorumService::SERVICE_HEALTH;
  }

  string get_name() const override {
    return "health";
  }

  /**
   * @} // HealthMonitor_Inherited_h
   */
};

#endif // CEPH_HEALTH_MONITOR_H
