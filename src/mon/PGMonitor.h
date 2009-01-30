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

#ifndef __PGMONITOR_H
#define __PGMONITOR_H

#include <map>
#include <set>
using namespace std;

#include "include/types.h"
#include "msg/Messenger.h"
#include "PaxosService.h"

#include "PGMap.h"

class MPGStats;
class MPGStatsAck;
class MStatfs;
class MMonCommand;

class PGMonitor : public PaxosService {
public:
  PGMap pg_map;

private:
  PGMap::Incremental pending_inc;

  void create_initial(bufferlist& bl);
  bool update_from_paxos();
  void create_pending();  // prepare a new pending
  void encode_pending(bufferlist &bl);  // propose pending update to peers

  void committed();

  bool preprocess_query(Message *m);  // true if processed.
  bool prepare_update(Message *m);

  bool preprocess_pg_stats(MPGStats *stats);
  bool prepare_pg_stats(MPGStats *stats);
  void _updated_stats(MPGStatsAck *ack, entity_inst_t who);

  struct C_Stats : public Context {
    PGMonitor *pgmon;
    MPGStatsAck *ack;
    entity_inst_t who;
    C_Stats(PGMonitor *p, MPGStatsAck *a, entity_inst_t w) : pgmon(p), ack(a), who(w) {}
    void finish(int r) {
      pgmon->_updated_stats(ack, who);
    }    
  };

  void handle_statfs(MStatfs *statfs);

  bool preprocess_command(MMonCommand *m);
  bool prepare_command(MMonCommand *m);

  map<int,utime_t> last_sent_pg_create;  // per osd throttle

  bool register_new_pgs();
  void send_pg_creates();

 public:
  PGMonitor(Monitor *mn, Paxos *p) : PaxosService(mn, p) { }
  
  void tick();  // check state, take actions


  void check_osd_map(epoch_t epoch);

};

#endif
