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

class PaxosService;

class MMonGetMap;
class MMonObserve;

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
  bool is_full_quorum() {
    return quorum.size() == monmap->size();
  }

  void call_election();  // initiate election
  void win_election(epoch_t epoch, set<int>& q);         // end election (called by Elector)
  void lose_election(epoch_t epoch, set<int>& q, int l); // end election (called by Elector)


  // -- paxos --
  vector<Paxos*> paxos;
  vector<PaxosService*> paxos_service;

  Paxos *add_paxos(int type);

  class PGMonitor *pgmon() { return (class PGMonitor *)paxos_service[PAXOS_PGMAP]; }
  class MDSMonitor *mdsmon() { return (class MDSMonitor *)paxos_service[PAXOS_MDSMAP]; }
  class OSDMonitor *osdmon() { return (class OSDMonitor *)paxos_service[PAXOS_OSDMAP]; }
  class ClientMonitor *clientmon() { return (class ClientMonitor *)paxos_service[PAXOS_CLIENTMAP]; }

  friend class Paxos;
  friend class OSDMonitor;
  friend class MDSMonitor;
  friend class ClientMonitor;
  friend class PGMonitor;
  friend class LogMonitor;


  // messages
  void handle_mon_get_map(MMonGetMap *m);
  void handle_shutdown(Message *m);
  void handle_command(class MMonCommand *m);
  void handle_observe(MMonObserve *m);

  void reply_command(MMonCommand *m, int rc, const string &rs);
  void reply_command(MMonCommand *m, int rc, const string &rs, bufferlist& rdata);

  void inject_args(const entity_inst_t& inst, string& args) {
    vector<string> a(1);
    a[0] = args;
    inject_args(inst, a);
  }
  void inject_args(const entity_inst_t& inst, vector<string>& args);  

public:
  struct C_Command : public Context {
    Monitor *mon;
    MMonCommand *m;
    int rc;
    string rs;
    C_Command(Monitor *_mm, MMonCommand *_m, int r, string& s) :
      mon(_mm), m(_m), rc(r), rs(s) {}
    void finish(int r) {
      mon->reply_command(m, rc, rs);
    }
  };

 private:
  bool dispatch_impl(Message *m);

 public:
  Monitor(int w, MonitorStore *s, Messenger *m, MonMap *map);
  ~Monitor();

  void init();
  void shutdown();
  void tick();

  void stop_cluster();

  int mkfs();

};

#endif
