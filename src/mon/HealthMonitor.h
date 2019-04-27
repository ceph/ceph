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

#include "mon/PaxosService.h"

class HealthMonitor : public PaxosService
{
  version_t version = 0;
  map<int,health_check_map_t> quorum_checks;  // for each quorum member
  health_check_map_t leader_checks;           // leader only

public:
  HealthMonitor(Monitor *m, Paxos *p, const string& service_name);

  /**
   * @defgroup HealthMonitor_Inherited_h Inherited abstract methods
   * @{
   */
  void init() override;

  bool preprocess_query(MonOpRequestRef op) override;
  bool prepare_update(MonOpRequestRef op) override;

  bool prepare_health_checks(MonOpRequestRef op);

  bool check_leader_health();
  bool check_member_health();

  void create_initial() override;
  void update_from_paxos(bool *need_bootstrap) override;
  void create_pending() override;
  void encode_pending(MonitorDBStore::TransactionRef t) override;
  version_t get_trim_to() const override;

  void encode_full(MonitorDBStore::TransactionRef t) override { }

  void tick() override;

  /**
   * @} // HealthMonitor_Inherited_h
   */
};

#endif // CEPH_HEALTH_MONITOR_H
