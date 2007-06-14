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

#ifndef __MON_PAXOS_H
#define __MON_PAXOS_H

#include "include/types.h"
#include "include/buffer.h"
#include "msg/Message.h"

#include "include/Context.h"

#include "common/Timer.h"

class Monitor;
class MMonPaxos;

// i am one state machine.
class Paxos {
  Monitor *mon;
  int whoami;

  // my state machine info
  int machine_id;
  const char *machine_name;

  // LEADER+PEON

  // -- generic state --
  const static int STATE_RECOVERING = 1;  // leader|peon: recovering paxos state
  const static int STATE_ACTIVE     = 2;  // leader|peon: idle.  peon may or may not have valid lease
  const static int STATE_UPDATING   = 3;  // leader|peon: updating to new value
  const char *get_statename(int s) {
    switch (s) {
    case STATE_RECOVERING: return "recovering";
    case STATE_ACTIVE: return "active";
    case STATE_UPDATING: return "updating";
    default: assert(0); return 0;
    }
  }

  int state;
  bool is_recovering() { return state == STATE_RECOVERING; }
  bool is_active() { return state == STATE_ACTIVE; }
  bool is_updating() { return state == STATE_UPDATING; }

  // recovery (phase 1)
  version_t last_committed;
  version_t accepted_pn;
  version_t accepted_pn_from;

  // active (phase 2)
  utime_t lease_timeout;
  list<Context*> waiting_for_readable;


  // -- leader --
  // recovery (phase 1)
  unsigned   num_last;
  version_t  old_accepted_v;
  version_t  old_accepted_pn;
  bufferlist old_accepted_value;

  // updating (phase 2)
  bufferlist new_value;
  unsigned   num_accepted;
  utime_t    accept_timeout;

  list<Context*> waiting_for_writeable;
  list<Context*> waiting_for_commit;


  void collect(version_t oldpn);
  void handle_collect(MMonPaxos*);
  void handle_last(MMonPaxos*);
  void begin(bufferlist& value);
  void handle_begin(MMonPaxos*);
  void handle_accept(MMonPaxos*);
  void commit();
  void handle_commit(MMonPaxos*);
  void extend_lease();
  void handle_lease(MMonPaxos*);

  version_t get_new_proposal_number(version_t gt=0);
  
public:
  Paxos(Monitor *m, int w,
	int mid,const char *mnm) : mon(m), whoami(w), 
				   machine_id(mid), machine_name(mnm) {
  }

  void dispatch(Message *m);

  void leader_init();
  void peon_init();


  // -- service interface --
  // read
  bool is_readable();
  version_t read_current(bufferlist &bl);
  void wait_for_readable(Context *onreadable);

  // write
  bool is_leader();
  bool is_writeable();
  bool propose_new_value(bufferlist& bl, Context *oncommit=0);
  void wait_for_commit(Context *oncommit) {
    waiting_for_commit.push_back(oncommit);
  }

};



#endif

