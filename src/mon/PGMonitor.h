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
#include "common/config.h"
#include "mon/MonitorDBStore.h"

#include "messages/MPGStats.h"
#include "messages/MPGStatsAck.h"
class MStatfs;
class MMonCommand;
class MGetPoolStats;

class RatioMonitor;
class TextTable;

class PGMonitor : public PaxosService {
public:
  PGMap pg_map;

  bool need_check_down_pgs;
  set<int> need_check_down_pg_osds;

private:
  PGMap::Incremental pending_inc;

  const char *pgmap_meta_prefix;
  const char *pgmap_pg_prefix;
  const char *pgmap_osd_prefix;

  void create_initial();
  void update_from_paxos(bool *need_bootstrap);
  void upgrade_format();
  void on_upgrade();
  void post_paxos_update();
  void handle_osd_timeouts();
  void create_pending();  // prepare a new pending
  // propose pending update to peers
  version_t get_trim_to();
  void update_logger();

  void encode_pending(MonitorDBStore::TransactionRef t);
  void read_pgmap_meta();
  void read_pgmap_full();
  void apply_pgmap_delta(bufferlist& bl);

  bool preprocess_query(MonOpRequestRef op);  // true if processed.
  bool prepare_update(MonOpRequestRef op);

  bool preprocess_pg_stats(MonOpRequestRef op);
  bool pg_stats_have_changed(int from, const MPGStats *stats) const;
  bool prepare_pg_stats(MonOpRequestRef op);
  void _updated_stats(MonOpRequestRef op, MonOpRequestRef ack_op);

  struct C_Stats : public C_MonOp {
    PGMonitor *pgmon;
    MonOpRequestRef stats_op_ack;
    entity_inst_t who;
    C_Stats(PGMonitor *p,
            MonOpRequestRef op,
            MonOpRequestRef op_ack)
      : C_MonOp(op), pgmon(p), stats_op_ack(op_ack) {}
    void _finish(int r) {
      if (r >= 0) {
	pgmon->_updated_stats(op, stats_op_ack);
      } else if (r == -ECANCELED) {
        return;
      } else if (r == -EAGAIN) {
	pgmon->dispatch(op);
      } else {
	assert(0 == "bad C_Stats return value");
      }
    }    
  };

  void handle_statfs(MonOpRequestRef op);
  bool preprocess_getpoolstats(MonOpRequestRef op);

  bool preprocess_command(MonOpRequestRef op);
  bool prepare_command(MonOpRequestRef op);

  map<int,utime_t> last_sent_pg_create;  // per osd throttle

  // when we last received PG stats from each osd
  map<int,utime_t> last_osd_report;

  void register_pg(OSDMap *osdmap, pg_pool_t& pool, pg_t pgid,
		   epoch_t epoch, bool new_pool);

  /**
   * check latest osdmap for new pgs to register
   *
   * @return true if we updated pending_inc (and should propose)
   */
  bool register_new_pgs();

  /**
   * recalculate creating pg mappings
   *
   * @return true if we updated pending_inc
   */
  bool map_pg_creates();

  void send_pg_creates();
  epoch_t send_pg_creates(int osd, Connection *con, epoch_t next);

  /**
   * check pgs for down primary osds
   *
   * clears need_check_down_pgs
   * clears need_check_down_pg_osds
   *
   * @return true if we updated pending_inc (and should propose)
   */
  bool check_down_pgs();
  void _try_mark_pg_stale(const OSDMap *osdmap, pg_t pgid,
			  const pg_stat_t& cur_stat);


  /**
   * Dump stats from pgs stuck in specified states.
   *
   * @return 0 on success, negative error code on failure
   */
  int dump_stuck_pg_stats(stringstream &ds, Formatter *f,
			  int threshold,
			  vector<string>& args) const;

  void dump_object_stat_sum(TextTable &tbl, Formatter *f,
			    object_stat_sum_t &sum,
			    uint64_t avail,
			    float raw_used_rate,
			    bool verbose, const pg_pool_t *pool) const;

  int64_t get_rule_avail(OSDMap& osdmap, int ruleno) const;

public:
  PGMonitor(Monitor *mn, Paxos *p, const string& service_name)
    : PaxosService(mn, p, service_name),
      need_check_down_pgs(false),
      pgmap_meta_prefix("pgmap_meta"),
      pgmap_pg_prefix("pgmap_pg"),
      pgmap_osd_prefix("pgmap_osd")
  { }
  ~PGMonitor() { }

  virtual void get_store_prefixes(set<string>& s) {
    s.insert(get_service_name());
    s.insert(pgmap_meta_prefix);
    s.insert(pgmap_pg_prefix);
    s.insert(pgmap_osd_prefix);
  }

  virtual void on_restart();

  /* Courtesy function provided by PaxosService, called when an election
   * finishes and the cluster goes active. We use it here to make sure we
   * haven't lost any PGs from new pools. */
  virtual void on_active();

  bool should_stash_full() {
    return false;  // never
  }
  virtual void encode_full(MonitorDBStore::TransactionRef t) {
    assert(0 == "unimplemented encode_full");
  }


  void tick();  // check state, take actions

  void check_osd_map(epoch_t epoch);

  void dump_pool_stats(stringstream &ss, Formatter *f, bool verbose);
  void dump_fs_stats(stringstream &ss, Formatter *f, bool verbose) const;

  void dump_info(Formatter *f) const;

  int _warn_slow_request_histogram(const pow2_hist_t& h, string suffix,
				   list<pair<health_status_t,string> >& summary,
				   list<pair<health_status_t,string> > *detail) const;

  void get_health(list<pair<health_status_t,string> >& summary,
		  list<pair<health_status_t,string> > *detail,
		  CephContext *cct) const override;
  void check_full_osd_health(list<pair<health_status_t,string> >& summary,
			     list<pair<health_status_t,string> > *detail,
			     const set<int>& s, const char *desc, health_status_t sev) const;

  void check_subs();
  void check_sub(Subscription *sub);

private:
  // no copying allowed
  PGMonitor(const PGMonitor &rhs);
  PGMonitor &operator=(const PGMonitor &rhs);
};

#endif
