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
#ifndef CEPH_MON_QUORUM_SERVICE_H
#define CEPH_MON_QUORUM_SERVICE_H

#include <errno.h>

#include "include/types.h"
#include "include/Context.h"
#include "common/RefCountedObj.h"
#include "common/config.h"

#include "mon/Monitor.h"

class QuorumService
{
  Context *tick_event = nullptr;
  double tick_period;

public:
  enum {
    SERVICE_HEALTH                   = 0x01,
    SERVICE_TIMECHECK                = 0x02,
    SERVICE_CONFIG_KEY               = 0x03,
  };

protected:
  Monitor *mon;
  epoch_t epoch;

  QuorumService(Monitor *m) :
    tick_period(g_conf->mon_tick_interval),
    mon(m),
    epoch(0)
  {
  }

  void cancel_tick() {
    if (tick_event)
      mon->timer.cancel_event(tick_event);
    tick_event = NULL;
  }

  void start_tick() {
    generic_dout(10) << __func__ << dendl;

    cancel_tick();
    if (tick_period <= 0)
      return;

    tick_event = new C_MonContext(mon, [this](int r) {
	if (r < 0)
	  return;
	tick();
      });
    mon->timer.add_event_after(tick_period, tick_event);
  }

  void set_update_period(double t) {
    tick_period = t;
  }

  bool in_quorum() {
    return (mon->is_leader() || mon->is_peon());
  }

  virtual bool service_dispatch(MonOpRequestRef op) = 0;
  virtual void service_tick() = 0;
  virtual void service_shutdown() = 0;

  virtual void start_epoch() = 0;
  virtual void finish_epoch() = 0;
  virtual void cleanup() = 0;

public:
  virtual ~QuorumService() { }

  void start(epoch_t new_epoch) {
    epoch = new_epoch;
    start_epoch();
  }

  void finish() {
    generic_dout(20) << "QuorumService::finish" << dendl;
    finish_epoch();
  }

  epoch_t get_epoch() const {
    return epoch;
  }

  bool dispatch(MonOpRequestRef op) {
    return service_dispatch(op);
  }

  void tick() {
    service_tick();
    start_tick();
  }

  void shutdown() {
    generic_dout(0) << "quorum service shutdown" << dendl;
    cancel_tick();
    service_shutdown();
  }

  virtual void init() { }

  virtual void get_health(Formatter *f,
			  list<pair<health_status_t,string> >& summary,
                          list<pair<health_status_t,string> > *detail) = 0;
  virtual int get_type() = 0;
  virtual string get_name() const = 0;

};

#endif /* CEPH_MON_QUORUM_SERVICE_H */
