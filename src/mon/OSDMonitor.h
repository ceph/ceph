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

/* Object Store Device (OSD) Monitor
 */

#ifndef __OSDMONITOR_H
#define __OSDMONITOR_H

#include <map>
#include <set>
using namespace std;

#include "include/types.h"
#include "msg/Messenger.h"

#include "osd/OSDMap.h"

#include "PaxosService.h"
#include "Session.h"

class Monitor;
class MOSDBoot;
class MMonCommand;
class MPoolSnap;
class MOSDMap;

class OSDMonitor : public PaxosService {
public:
  OSDMap osdmap;

private:
  map<epoch_t, list<PaxosServiceMessage*> > waiting_for_map;

  // [leader]
  OSDMap::Incremental pending_inc;
  map<int,utime_t>    down_pending_out;  // osd down -> out

  map<int,double> osd_weight;
  // svc
public:  
  void create_initial(bufferlist& bl);
private:
  bool update_from_paxos();
  void create_pending();  // prepare a new pending
  void encode_pending(bufferlist &bl);

  void committed();

  void handle_query(PaxosServiceMessage *m);
  bool preprocess_query(PaxosServiceMessage *m);  // true if processed.
  bool prepare_update(PaxosServiceMessage *m);
  bool should_propose(double &delay);

  // ...
  void send_to_waiting();     // send current map to waiters.
  void send_full(PaxosServiceMessage *m);
  MOSDMap *build_incremental(epoch_t from);
  void send_incremental(PaxosServiceMessage *m, epoch_t since);
 
  bool preprocess_failure(class MOSDFailure *m);
  bool prepare_failure(class MOSDFailure *m);
  void _reported_failure(MOSDFailure *m);

  bool preprocess_boot(class MOSDBoot *m);
  bool prepare_boot(class MOSDBoot *m);
  void _booted(MOSDBoot *m, bool logit);

  bool preprocess_alive(class MOSDAlive *m);
  bool prepare_alive(class MOSDAlive *m);
  void _reply_map(PaxosServiceMessage *m, epoch_t e);

  bool preprocess_pgtemp(class MOSDPGTemp *m);
  bool prepare_pgtemp(class MOSDPGTemp *m);

  bool preprocess_pool_op ( class MPoolOp *m);
  bool preprocess_pool_op_create ( class MPoolOp *m);
  bool prepare_pool_op (MPoolOp *m);
  bool prepare_pool_op_create (MPoolOp *m);
  bool prepare_pool_op_delete(MPoolOp *m);
  bool prepare_pool_op_auid(MPoolOp *m);
  int prepare_new_pool(string& name, __u64 auid = CEPH_AUTH_UID_DEFAULT);
  int prepare_new_pool(MPoolOp *m);

  void _pool_op(MPoolOp *m, int replyCode, epoch_t epoch);

  struct C_Booted : public Context {
    OSDMonitor *cmon;
    MOSDBoot *m;
    bool logit;
    C_Booted(OSDMonitor *cm, MOSDBoot *m_, bool l=true) : 
      cmon(cm), m(m_), logit(l) {}
    void finish(int r) {
      if (r >= 0)
	cmon->_booted(m, logit);
      else
	cmon->dispatch((PaxosServiceMessage*)m);
    }
  };

  struct C_ReplyMap : public Context {
    OSDMonitor *osdmon;
    PaxosServiceMessage *m;
    epoch_t e;
    C_ReplyMap(OSDMonitor *o, PaxosServiceMessage *mm, epoch_t ee) : osdmon(o), m(mm), e(ee) {}
    void finish(int r) {
      osdmon->_reply_map(m, e);
    }    
  };
  struct C_Reported : public Context {
    OSDMonitor *cmon;
    MOSDFailure *m;
    C_Reported(OSDMonitor *cm, MOSDFailure *m_) : 
      cmon(cm), m(m_) {}
    void finish(int r) {
      if (r >= 0)
	cmon->_reported_failure(m);
      else
	cmon->dispatch((PaxosServiceMessage*)m);
    }
  };
  struct C_PoolOp : public Context {
    OSDMonitor *osdmon;
    MPoolOp *m;
    int replyCode;
    int epoch;
    C_PoolOp(OSDMonitor * osd, MPoolOp *m_, int rc, int e) : 
      osdmon(osd), m(m_), replyCode(rc), epoch(e) {}
    void finish(int r) {
      osdmon->_pool_op(m, replyCode, epoch);
    }
  };

  bool preprocess_remove_snaps(class MRemoveSnaps *m);
  bool prepare_remove_snaps(class MRemoveSnaps *m);

 public:
  OSDMonitor(Monitor *mn, Paxos *p) : 
    PaxosService(mn, p) { }

  void tick();  // check state, take actions

  bool preprocess_command(MMonCommand *m);
  bool prepare_command(MMonCommand *m);

  void mark_all_down();

  void send_latest(PaxosServiceMessage *m, epoch_t start=0);
  void send_latest_now_nodelete(PaxosServiceMessage *m, epoch_t start=0) {
    send_incremental(m, start);
  }

  void blacklist(entity_addr_t a, utime_t until);

  void check_subs();
  void check_sub(Subscription *sub);

  void add_flag(int flag) {
    if (!(osdmap.flags & flag)) {
      if (pending_inc.new_flags < 0)
	pending_inc.new_flags = osdmap.flags;
      pending_inc.new_flags |= flag;
    }
  }

  void remove_flag(int flag) {
    if(osdmap.flags & flag) {
      if (pending_inc.new_flags < 0)
	pending_inc.new_flags = osdmap.flags;
      pending_inc.new_flags &= ~flag;
    }
  }
};

#endif
