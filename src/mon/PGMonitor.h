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

#include "messages/MPGStats.h"
#include "messages/MPGStatsAck.h"
class MStatfs;
class MMonCommand;
class MGetPoolStats;

class RatioMonitor;

class PGMonitor : public PaxosService {
public:
  PGMap pg_map;

  bool need_check_down_pgs;

private:
  PGMap::Incremental pending_inc;

  void create_initial();
  void update_from_paxos();
  void handle_osd_timeouts();
  void create_pending();  // prepare a new pending
  void encode_pending(bufferlist &bl);  // propose pending update to peers

  void update_logger();

  bool preprocess_query(PaxosServiceMessage *m);  // true if processed.
  bool prepare_update(PaxosServiceMessage *m);

  bool preprocess_pg_stats(MPGStats *stats);
  bool pg_stats_have_changed(int from, const MPGStats *stats) const;
  bool prepare_pg_stats(MPGStats *stats);
  void _updated_stats(MPGStats *req, MPGStatsAck *ack);

  struct C_Stats : public Context {
    PGMonitor *pgmon;
    MPGStats *req;
    MPGStatsAck *ack;
    entity_inst_t who;
    C_Stats(PGMonitor *p, MPGStats *r, MPGStatsAck *a) : pgmon(p), req(r), ack(a) {}
    void finish(int r) {
      if (r >= 0) {
	pgmon->_updated_stats(req, ack);
      } else if (r == -ECANCELED) {
	req->put();
	ack->put();
      } else {
	ack->put();
	pgmon->dispatch(req);
      }
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

  /**
   * check latest osdmap for new pgs to register
   *
   * @return true if we updated pending_inc (and should propose)
   */
  bool register_new_pgs();

  void send_pg_creates();
  void send_pg_creates(int osd, Connection *con);

  /**
   * check pgs for down primary osds
   *
   * clears need_check_down_pgs
   *
   * @return true if we updated pending_inc (and should propose)
   */
  bool check_down_pgs();

  /**
   * Dump stats from pgs stuck in specified states.
   *
   * @return 0 on success, negative error code on failure
   */
  int dump_stuck_pg_stats(ostream& ss,
			  bufferlist& rdata,
			  vector<const char*>& args) const;

public:
  PGMonitor(Monitor *mn, Paxos *p);
  virtual ~PGMonitor();

  virtual void on_restart();

  /* Courtesy function provided by PaxosService, called when an election
   * finishes and the cluster goes active. We use it here to make sure we
   * haven't lost any PGs from new pools. */
  virtual void on_active();

  void tick();  // check state, take actions

  void check_osd_map(epoch_t epoch);

  void dump_info(Formatter *f);

  void get_health(list<pair<health_status_t,string> >& summary,
		  list<pair<health_status_t,string> > *detail) const;
  void check_full_osd_health(list<pair<health_status_t,string> >& summary,
			     list<pair<health_status_t,string> > *detail,
			     const set<int>& s, const char *desc, health_status_t sev) const;

  void check_sub(Subscription *sub);

private:
  // no copying allowed
  PGMonitor(const PGMonitor &rhs);
  PGMonitor &operator=(const PGMonitor &rhs);
};

#endif
