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
 * This is the top level monitor. It runs on each machine in the Monitor   
 * Cluster. The election of a leader for the paxos algorithm only happens 
 * once per machine via the elector. There is a separate paxos instance (state) 
 * kept for each of the system components: Object Store Device (OSD) Monitor, 
 * Placement Group (PG) Monitor, Metadata Server (MDS) Monitor, and Client Monitor.
 */

#ifndef __MONITOR_H
#define __MONITOR_H

#include "include/types.h"
#include "msg/Messenger.h"

#include "common/Timer.h"

#include "MonMap.h"
#include "Elector.h"
#include "Paxos.h"


class MonitorStore;
class OSDMonitor;
class MDSMonitor;
class ClientMonitor;
class PGMonitor;

class Monitor : public Dispatcher {
public:
  // me
  int whoami;
  Messenger *messenger;
  Mutex lock;

  MonMap *monmap;

  // timer.
  SafeTimer timer;
  Context *tick_timer;
  void cancel_tick();
  void reset_tick();
  friend class C_Mon_Tick;

  // -- local storage --
public:
  MonitorStore *store;

  // -- monitor state --
private:
  const static int STATE_STARTING = 0; // electing
  const static int STATE_LEADER =   1;
  const static int STATE_PEON =     2;
  int state;
  bool stopping;

public:
  bool is_starting() { return state == STATE_STARTING; }
  bool is_leader() { return state == STATE_LEADER; }
  bool is_peon() { return state == STATE_PEON; }
  bool is_stopping() { return stopping; }


  // -- elector --
private:
  Elector elector;
  friend class Elector;
  
  epoch_t  mon_epoch;    // monitor epoch (election instance)
  int leader;            // current leader (to best of knowledge)
  set<int> quorum;       // current active set of monitors (if !starting)
  utime_t last_called_election;  // [starting] last time i called an election
  
public:
  epoch_t get_epoch() { return mon_epoch; }
  int get_leader() { return leader; }
  const set<int>& get_quorum() { return quorum; }

  void call_election();  // initiate election
  void win_election(epoch_t epoch, set<int>& q);  // end election (called by Elector)
  void lose_election(epoch_t epoch, int l);       // end election (called by Elector)


  // -- paxos --
  Paxos paxos_mdsmap;
  Paxos paxos_osdmap;
  Paxos paxos_clientmap;
  Paxos paxos_pgmap;
  friend class Paxos;
  

  // -- services --
  OSDMonitor *osdmon;
  MDSMonitor *mdsmon;
  ClientMonitor *clientmon;
  PGMonitor *pgmon;

  friend class OSDMonitor;
  friend class MDSMonitor;
  friend class ClientMonitor;
  friend class PGMonitor;


  // messages
  void handle_shutdown(Message *m);
  void handle_ping_ack(class MPingAck *m);
  void handle_command(class MMonCommand *m);

  int do_command(vector<string>& cmd, bufferlist& data, 
		 bufferlist& rdata, string &rs);

 public:
  Monitor(int w, Messenger *m, MonMap *mm) : 
    whoami(w), 
    messenger(m),
    monmap(mm),
    timer(lock), tick_timer(0),
    store(0),

    state(STATE_STARTING), stopping(false),

    elector(this, w),
    mon_epoch(0), 
    leader(0),
    
    paxos_mdsmap(this, w, PAXOS_MDSMAP),
    paxos_osdmap(this, w, PAXOS_OSDMAP),
    paxos_clientmap(this, w, PAXOS_CLIENTMAP),
    paxos_pgmap(this, w, PAXOS_PGMAP),

    osdmon(0), mdsmon(0), clientmon(0)
  {
  }
  ~Monitor() {
    delete messenger;
  }

  void init();
  void shutdown();
  void dispatch(Message *m);
  void tick();

  void do_stop();

};

#endif
