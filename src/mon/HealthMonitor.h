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

#include <boost/intrusive_ptr.hpp>
// Because intusive_ptr clobbers our assert...
#include "include/assert.h"

#include "mon/Monitor.h"
#include "mon/QuorumService.h"
#include "mon/HealthService.h"

#include "messages/MMonHealth.h"

#include "common/config.h"
#include "common/Formatter.h"

class HealthMonitor : public QuorumService
{
  map<int,HealthServiceRef> services;

protected:
  virtual void service_shutdown();

public:
  HealthMonitor(Monitor *m) : QuorumService(m) { }
  virtual ~HealthMonitor() { }
  HealthMonitor *get() {
    return static_cast<HealthMonitor *>(RefCountedObject::get());
  }


  /**
   * @defgroup HealthMonitor_Inherited_h Inherited abstract methods
   * @{
   */
  virtual void init();
  virtual void get_health(Formatter *f,
                          list<pair<health_status_t,string> > *detail);
  virtual bool service_dispatch(Message *m);

  virtual void start_epoch() {
    for (map<int,HealthServiceRef>::iterator it = services.begin();
         it != services.end(); ++it) {
      it->second->start(get_epoch());
    }
  }

  virtual void finish_epoch() {
    generic_dout(20) << "HealthMonitor::finish_epoch()" << dendl;
    for (map<int,HealthServiceRef>::iterator it = services.begin();
         it != services.end(); ++it) {
      assert(it->second.get() != NULL);
      it->second->finish();
    }
  }

  virtual void cleanup() { }
  virtual void service_tick() { }

  virtual int get_type() {
    return QuorumService::SERVICE_HEALTH;
  }

  virtual string get_name() const {
    return "health";
  }

  /**
   * @} // HealthMonitor_Inherited_h
   */
};
typedef boost::intrusive_ptr<HealthMonitor> HealthMonitorRef;

#endif // CEPH_HEALTH_MONITOR_H
