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
#ifndef CEPH_MON_DATA_HEALTH_SERVICE_H
#define CEPH_MON_DATA_HEALTH_SERVICE_H

#include <errno.h>

#include "include/types.h"
#include "include/Context.h"
#include "mon/mon_types.h"
#include "mon/QuorumService.h"
#include "mon/HealthService.h"
#include "common/Formatter.h"
#include "common/config.h"
#include "global/signal_handler.h"

class MMonHealth;

class DataHealthService :
  public HealthService
{
  map<entity_inst_t,DataStats> stats;
  int last_warned_percent;

  void handle_tell(MMonHealth *m);
  int update_stats();
  void share_stats();

  void force_shutdown() {
    generic_dout(0) << "** Shutdown via Data Health Service **" << dendl;
    queue_async_signal(SIGINT);
  }

protected:
  virtual void service_tick();
  virtual bool service_dispatch(Message *m) {
    assert(0 == "We should never reach this; only the function below");
    return false;
  }
  virtual bool service_dispatch(MMonHealth *m);
  virtual void service_shutdown() { }

  virtual void start_epoch();
  virtual void finish_epoch() { }
  virtual void cleanup() { }

public:
  DataHealthService(Monitor *m) :
    HealthService(m),
    last_warned_percent(0)
  {
    set_update_period(g_conf->mon_health_data_update_interval);
  }
  virtual ~DataHealthService() { }

  virtual void init() {
    generic_dout(20) << "data_health " << __func__ << dendl;
    start_tick();
  }

  virtual health_status_t get_health(Formatter *f,
                          list<pair<health_status_t,string> > *detail);

  virtual int get_type() {
    return HealthService::SERVICE_HEALTH_DATA;
  }

  virtual string get_name() const {
    return "data_health";
  }
};

#endif /* CEPH_MON_DATA_HEALTH_SERVICE_H */
