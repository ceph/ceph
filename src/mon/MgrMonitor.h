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

#ifndef CEPH_MGRMONITOR_H
#define CEPH_MGRMONITOR_H

#include "include/Context.h"
#include "MgrMap.h"
#include "PaxosService.h"

class MgrMonitor: public PaxosService
{
  MgrMap map;
  MgrMap pending_map;
  bool ever_had_active_mgr = false;

  utime_t first_seen_inactive;

  std::map<uint64_t, ceph::coarse_mono_clock::time_point> last_beacon;

  /**
   * If a standby is available, make it active, given that
   * there is currently no active daemon.
   *
   * @return true if a standby was promoted
   */
  bool promote_standby();
  void drop_active();
  void drop_standby(uint64_t gid);

  Context *digest_event = nullptr;
  void cancel_timer();

  bool check_caps(MonOpRequestRef op, const uuid_d& fsid);

  health_status_t should_warn_about_mgr_down();

public:
  MgrMonitor(Monitor *mn, Paxos *p, const string& service_name)
    : PaxosService(mn, p, service_name)
  {}
  ~MgrMonitor() override {}

  void init() override;
  void on_shutdown() override;

  const MgrMap &get_map() const { return map; }

  bool in_use() const { return map.epoch > 0; }

  void create_initial() override;
  void update_from_paxos(bool *need_bootstrap) override;
  void create_pending() override;
  void encode_pending(MonitorDBStore::TransactionRef t) override;

  bool preprocess_query(MonOpRequestRef op) override;
  bool prepare_update(MonOpRequestRef op) override;

  bool preprocess_command(MonOpRequestRef op);
  bool prepare_command(MonOpRequestRef op);

  void encode_full(MonitorDBStore::TransactionRef t) override { }

  bool preprocess_beacon(MonOpRequestRef op);
  bool prepare_beacon(MonOpRequestRef op);

  void check_sub(Subscription *sub);
  void check_subs();
  void send_digests();

  void on_active() override;
  void on_restart() override;

  void get_health(list<pair<health_status_t,string> >& summary,
		  list<pair<health_status_t,string> > *detail,
		  CephContext *cct) const override;
  void tick() override;

  void print_summary(Formatter *f, std::ostream *ss) const;

  friend class C_Updated;
};

#endif
