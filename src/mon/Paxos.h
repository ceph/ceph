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
time---->

cccccccccccccccccca????????????????????????????????????????
cccccccccccccccccca????????????????????????????????????????
cccccccccccccccccca???????????????????????????????????????? leader
cccccccccccccccccc????????????????????????????????????????? 
ccccc?????????????????????????????????????????????????????? 

last_committed

pn_from
pn

a 12v 
b 12v
c 14v
d
e 12v


*/


/*
 * NOTE: This libary is based on the Paxos algorithm, but varies in a few key ways:
 *  1- Only a single new value is generated at a time, simplifying the recovery logic.
 *  2- Nodes track "committed" values, and share them generously (and trustingly)
 *  3- A 'leasing' mechism is built-in, allowing nodes to determine when it is safe to 
 *     "read" their copy of the last committed value.
 *
 * This provides a simple replication substrate that services can be built on top of.
 * See PaxosService.h
 */

#ifndef CEPH_MON_PAXOS_H
#define CEPH_MON_PAXOS_H

#include "include/types.h"
#include "mon_types.h"
#include "include/buffer.h"
#include "messages/PaxosServiceMessage.h"
#include "msg/msg_types.h"

#include "include/Context.h"

#include "common/Timer.h"

class Monitor;
class MMonPaxos;
class Paxos;


// i am one state machine.
class Paxos {
  Monitor *mon;

  // my state machine info
  int machine_id;
  const char *machine_name;

  friend class Monitor;
  friend class PaxosService;
  friend class PaxosObserver;



  // LEADER+PEON

  // -- generic state --
public:
  const static int STATE_RECOVERING = 1;  // leader|peon: recovering paxos state
  const static int STATE_ACTIVE     = 2;  // leader|peon: idle.  peon may or may not have valid lease
  const static int STATE_UPDATING   = 3;  // leader|peon: updating to new value
  static const char *get_statename(int s) {
    switch (s) {
    case STATE_RECOVERING: return "recovering";
    case STATE_ACTIVE: return "active";
    case STATE_UPDATING: return "updating";
    default: assert(0); return 0;
    }
  }

private:
  int state;

public:
  bool is_recovering() { return state == STATE_RECOVERING; }
  bool is_active() { return state == STATE_ACTIVE; }
  bool is_updating() { return state == STATE_UPDATING; }

private:
  // recovery (phase 1)
  version_t first_committed_any;
  version_t first_committed;
  version_t last_pn;
  version_t last_committed;
  version_t accepted_pn;
  version_t accepted_pn_from;

  // active (phase 2)
  utime_t lease_expire;
  list<Context*> waiting_for_active;
  list<Context*> waiting_for_readable;

  version_t latest_stashed;

  // -- leader --
  // recovery (paxos phase 1)
  unsigned   num_last;
  version_t  uncommitted_v;
  version_t  uncommitted_pn;
  bufferlist uncommitted_value;

  Context    *collect_timeout_event;

  // active
  set<int>   acked_lease;
  Context    *lease_renew_event;
  Context    *lease_ack_timeout_event;
  Context    *lease_timeout_event;

  // updating (paxos phase 2)
  bufferlist new_value;
  set<int>   accepted;

  Context    *accept_timeout_event;

  list<Context*> waiting_for_writeable;
  list<Context*> waiting_for_commit;

  // observers
  struct Observer {
    entity_inst_t inst;
    version_t last_version;
    utime_t timeout;
    Observer(entity_inst_t& ei, version_t v) : inst(ei), last_version(v) { }
  };
  map<entity_inst_t, Observer *> observers;

  //synchronization warnings
  utime_t last_lease_time_warn;
  int lease_times_warned;


  class C_CollectTimeout : public Context {
    Paxos *paxos;
  public:
    C_CollectTimeout(Paxos *p) : paxos(p) {}
    void finish(int r) {
      paxos->collect_timeout();
    }
  };

  class C_AcceptTimeout : public Context {
    Paxos *paxos;
  public:
    C_AcceptTimeout(Paxos *p) : paxos(p) {}
    void finish(int r) {
      paxos->accept_timeout();
    }
  };

  class C_LeaseAckTimeout : public Context {
    Paxos *paxos;
  public:
    C_LeaseAckTimeout(Paxos *p) : paxos(p) {}
    void finish(int r) {
      paxos->lease_ack_timeout();
    }
  };

  class C_LeaseTimeout : public Context {
    Paxos *paxos;
  public:
    C_LeaseTimeout(Paxos *p) : paxos(p) {}
    void finish(int r) {
      paxos->lease_timeout();
    }
  };

  class C_LeaseRenew : public Context {
    Paxos *paxos;
  public:
    C_LeaseRenew(Paxos *p) : paxos(p) {}
    void finish(int r) {
      paxos->lease_renew_timeout();
    }
  };


  void collect(version_t oldpn);
  void handle_collect(MMonPaxos*);
  void handle_last(MMonPaxos*);
  void collect_timeout();

  void begin(bufferlist& value);
  void handle_begin(MMonPaxos*);
  void handle_accept(MMonPaxos*);
  void accept_timeout();

  void commit();
  void handle_commit(MMonPaxos*);
  void extend_lease();
  void handle_lease(MMonPaxos*);
  void handle_lease_ack(MMonPaxos*);

  void lease_ack_timeout();    // on leader, if lease isn't acked by all peons
  void lease_renew_timeout();  // on leader, to renew the lease
  void lease_timeout();        // on peon, if lease isn't extended

  void cancel_events();

  version_t get_new_proposal_number(version_t gt=0);
  
public:
  Paxos(Monitor *m,
	int mid) : mon(m),
		   machine_id(mid), 
		   machine_name(get_paxos_name(mid)),
		   state(STATE_RECOVERING),
		   collect_timeout_event(0),
		   lease_renew_event(0),
		   lease_ack_timeout_event(0),
		   lease_timeout_event(0),
		   accept_timeout_event(0),
		   lease_times_warned(0) { }

  const char *get_machine_name() const {
    return machine_name;
  }

  void dispatch(PaxosServiceMessage *m);

  void init();

  void election_starting();
  void leader_init();
  void peon_init();

  void share_state(MMonPaxos *m, version_t first_committed, version_t last_committed);
  void store_state(MMonPaxos *m);


  // -- service interface --
  void wait_for_active(Context *c) {
    waiting_for_active.push_back(c);
  }

  void trim_to(version_t first);
  
  // read
  version_t get_version() { return last_committed; }
  bool is_readable(version_t seen=0);
  bool read(version_t v, bufferlist &bl);
  version_t read_current(bufferlist &bl);
  void wait_for_readable(Context *onreadable) {
    //assert(!is_readable());
    waiting_for_readable.push_back(onreadable);
  }

  // write
  bool is_leader();
  bool is_writeable();
  void wait_for_writeable(Context *c) {
    assert(!is_writeable());
    waiting_for_writeable.push_back(c);
  }

  bool propose_new_value(bufferlist& bl, Context *oncommit=0);
  void wait_for_commit(Context *oncommit) {
    waiting_for_commit.push_back(oncommit);
  }
  void wait_for_commit_front(Context *oncommit) {
    waiting_for_commit.push_front(oncommit);
  }

  // if state values are incrementals, it is usefult to keep
  // the latest copy of the complete structure.
  void stash_latest(version_t v, bufferlist& bl);
  version_t get_latest(bufferlist& bl);

  void register_observer(entity_inst_t inst, version_t v);
  void update_observers();
};



#endif

