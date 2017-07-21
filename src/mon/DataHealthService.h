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
#include "mon/mon_types.h"
#include "mon/HealthService.h"
#include "common/config.h"
#include "global/signal_handler.h"

struct MMonHealth;
namespace ceph { class Formatter; }


class DataHealthService :
  public HealthService
{
  map<entity_inst_t,DataStats> stats;
  int last_warned_percent;

  void handle_tell(MonOpRequestRef op);
  int update_store_stats(DataStats &ours);
  int update_stats();
  void share_stats();

  void force_shutdown() {
    generic_dout(0) << "** Shutdown via Data Health Service **" << dendl;
    queue_async_signal(SIGINT);
  }

protected:
  void service_tick() override;
  bool service_dispatch_op(MonOpRequestRef op) override;
  void service_shutdown() override { }

  void start_epoch() override;
  void finish_epoch() override { }
  void cleanup() override { }

public:
  DataHealthService(Monitor *m) :
    HealthService(m),
    last_warned_percent(0)
  {
    set_update_period(g_conf->mon_health_data_update_interval);
  }
  ~DataHealthService() override { }

  void init() override {
    generic_dout(20) << "data_health " << __func__ << dendl;
    start_tick();
  }

  void get_health(
    list<pair<health_status_t,string> >& summary,
    list<pair<health_status_t,string> > *detail) override;

  int get_type() override {
    return HealthService::SERVICE_HEALTH_DATA;
  }

  string get_name() const override {
    return "data_health";
  }
};

#endif /* CEPH_MON_DATA_HEALTH_SERVICE_H */
