// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

/*
 * Placement Group Monitor. Placement Groups are logical sets of objects
 * that are replicated by the same set of devices.
 */

#ifndef CEPH_PGMONITOR_H
#define CEPH_PGMONITOR_H

#include <map>
#include <set>
using namespace std;

#include "PGMap.h"
#include "PaxosService.h"
#include "include/types.h"
#include "include/utime.h"
#include "common/histogram.h"
#include "msg/Messenger.h"
#include "mon/MonitorDBStore.h"

class MPGStats;
class MonPGStatService;
class PGMonStatService;

class PGMonitor : public PaxosService {
  PGMap pg_map;
  std::unique_ptr<PGMonStatService> pgservice;

  bool do_delete = false;   ///< propose deleting pgmap data
  bool did_delete = false;  ///< we already deleted pgmap data

private:
  PGMap::Incremental pending_inc;

  bool check_all_pgs = false;

  const char *pgmap_meta_prefix;
  const char *pgmap_pg_prefix;
  const char *pgmap_osd_prefix;

  void create_initial() override;
  void update_from_paxos(bool *need_bootstrap) override;
  void upgrade_format() override;
  void on_upgrade() override;
  void post_paxos_update() override;
  void handle_osd_timeouts();
  void create_pending() override;  // prepare a new pending
  // propose pending update to peers
  version_t get_trim_to() override;
  void update_logger();

  void encode_pending(MonitorDBStore::TransactionRef t) override;
  void read_pgmap_meta();
  void read_pgmap_full();
  void apply_pgmap_delta(bufferlist& bl);

  bool preprocess_query(MonOpRequestRef op) override;  // true if processed.
  bool prepare_update(MonOpRequestRef op) override;

  bool preprocess_pg_stats(MonOpRequestRef op);
  bool pg_stats_have_changed(int from, const MPGStats *stats) const;
  bool prepare_pg_stats(MonOpRequestRef op);
  void _updated_stats(MonOpRequestRef op, MonOpRequestRef ack_op);

  struct C_Stats;

  bool preprocess_command(MonOpRequestRef op);
  bool prepare_command(MonOpRequestRef op);

  // when we last received PG stats from each osd
  map<int,utime_t> last_osd_report;

  epoch_t send_pg_creates(int osd, Connection *con, epoch_t next);

public:
  PGMonitor(Monitor *mn, Paxos *p, const string& service_name);
  ~PGMonitor() override;

  void get_store_prefixes(set<string>& s) override {
    s.insert(get_service_name());
    s.insert(pgmap_meta_prefix);
    s.insert(pgmap_pg_prefix);
    s.insert(pgmap_osd_prefix);
  }

  void on_restart() override;

  /* Courtesy function provided by PaxosService, called when an election
   * finishes and the cluster goes active. We use it here to make sure we
   * haven't lost any PGs from new pools. */
  void on_active() override;

  bool should_stash_full() override {
    return false;  // never
  }
  void encode_full(MonitorDBStore::TransactionRef t) override {
    assert(0 == "unimplemented encode_full");
  }


  void tick() override;  // check state, take actions

  void check_osd_map(epoch_t epoch);

  int _warn_slow_request_histogram(const pow2_hist_t& h, string suffix,
				   list<pair<health_status_t,string> >& summary,
				   list<pair<health_status_t,string> > *detail) const;

  void get_health(list<pair<health_status_t,string> >& summary,
		  list<pair<health_status_t,string> > *detail,
		  CephContext *cct) const override;
  void check_full_osd_health(
    list<pair<health_status_t,string> >& summary,
    list<pair<health_status_t,string> > *detail,
    const mempool::pgmap::set<int>& s,
    const char *desc, health_status_t sev) const;

  void check_subs();
  bool check_sub(Subscription *sub);

  MonPGStatService *get_pg_stat_service();

private:
  // no copying allowed
  PGMonitor(const PGMonitor &rhs);
  PGMonitor &operator=(const PGMonitor &rhs);

  // we don't want to include gtest.h just for FRIEND_TEST
  friend class pgmonitor_dump_object_stat_sum_0_Test;
  friend class pgmonitor_dump_object_stat_sum_1_Test;
  friend class pgmonitor_dump_object_stat_sum_2_Test;
};

#endif
