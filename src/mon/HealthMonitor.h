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
  std::map<int,health_check_map_t> quorum_checks;  // for each quorum member
  health_check_map_t leader_checks;           // leader only
  std::map<std::string,health_mute_t> mutes;

  std::map<std::string,health_mute_t> pending_mutes;

public:
  HealthMonitor(Monitor &m, Paxos &p, const std::string& service_name);

  /**
   * @defgroup HealthMonitor_Inherited_h Inherited abstract methods
   * @{
   */
  void init() override;

  bool preprocess_query(MonOpRequestRef op) override;
  bool prepare_update(MonOpRequestRef op) override;

  void create_initial() override;
  void update_from_paxos(bool *need_bootstrap) override;
  void create_pending() override;
  void encode_pending(MonitorDBStore::TransactionRef t) override;
  version_t get_trim_to() const override;

  void encode_full(MonitorDBStore::TransactionRef t) override { }

  void tick() override;

  void gather_all_health_checks(health_check_map_t *all);
  health_status_t get_health_status(
    bool want_detail,
    ceph::Formatter *f,
    std::string *plain,
    const char *sep1 = " ",
    const char *sep2 = "; ");

  /**
   * @} // HealthMonitor_Inherited_h
   */
private:
  bool preprocess_command(MonOpRequestRef op);

  bool prepare_command(MonOpRequestRef op);
  bool prepare_health_checks(MonOpRequestRef op);
  void check_for_older_version(health_check_map_t *checks);
  void check_for_mon_down(health_check_map_t *checks);
  void check_for_clock_skew(health_check_map_t *checks);
  void check_mon_crush_loc_stretch_mode(health_check_map_t *checks);
  void check_if_msgr2_enabled(health_check_map_t *checks);
  bool check_leader_health();
  bool check_member_health();
  bool check_mutes();
};

#endif // CEPH_HEALTH_MONITOR_H
