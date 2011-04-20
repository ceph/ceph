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
#include "msg/Messenger.h"
#include "common/config.h"

class MPGStats;
class MPGStatsAck;
class MStatfs;
class MMonCommand;
class MGetPoolStats;

class RatioMonitor;

class PGMonitor : public PaxosService {
public:
  PGMap pg_map;

private:
  PGMap::Incremental pending_inc;

  void create_initial(bufferlist& bl);
  bool update_from_paxos();
  void handle_osd_timeouts();
  void create_pending();  // prepare a new pending
  void encode_pending(bufferlist &bl);  // propose pending update to peers

  void committed();

  bool preprocess_query(PaxosServiceMessage *m);  // true if processed.
  bool prepare_update(PaxosServiceMessage *m);

  bool preprocess_pg_stats(MPGStats *stats);
  bool pg_stats_have_changed(int from, const MPGStats *stats) const;
  bool prepare_pg_stats(MPGStats *stats);
  void _updated_stats(MPGStats *req, MPGStatsAck *ack);

  void update_full_ratios(float full_ratio, int nearfull_ratio) {
    if (full_ratio != 0)
      pending_inc.full_ratio = full_ratio;
    if (nearfull_ratio != 0)
      pending_inc.nearfull_ratio = nearfull_ratio;
    propose_pending();
  }

  struct C_Stats : public Context {
    PGMonitor *pgmon;
    MPGStats *req;
    MPGStatsAck *ack;
    entity_inst_t who;
    C_Stats(PGMonitor *p, MPGStats *r, MPGStatsAck *a) : pgmon(p), req(r), ack(a) {}
    void finish(int r) {
      pgmon->_updated_stats(req, ack);
    }    
  };

  void handle_statfs(MStatfs *statfs);
  bool preprocess_getpoolstats(MGetPoolStats *m);

  bool preprocess_command(MMonCommand *m);
  bool prepare_command(MMonCommand *m);

  map<int,utime_t> last_sent_pg_create;  // per osd throttle

  // when we last received PG stats from each osd
  map<int,utime_t> last_osd_report;

  void register_pg(pg_pool_t& pool, pg_t pgid, epoch_t epoch, bool new_pool);
  bool register_new_pgs();
  void send_pg_creates();

public:
  PGMonitor(Monitor *mn, Paxos *p);
  virtual ~PGMonitor();

  virtual void on_election_start();

  void tick();  // check state, take actions

  void check_osd_map(epoch_t epoch);

  enum health_status_t get_health(std::ostream &ss) const;

private:
  // no copying allowed
  PGMonitor(const PGMonitor &rhs);
  PGMonitor &operator=(const PGMonitor &rhs);

  RatioMonitor *ratio_monitor;
  friend class RatioMonitor;
};

#endif
