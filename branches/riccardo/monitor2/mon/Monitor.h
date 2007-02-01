// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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


#ifndef __MONITOR_H
#define __MONITOR_H

#include "include/types.h"
#include "msg/Messenger.h"

#include "MonMap.h"
#include "Elector.h"

class ObjectStore;
class OSDMonitor;
class MDSMonitor;
class ClientMonitor;

class Monitor : public Dispatcher {
protected:
  // me
  int whoami;
  Messenger *messenger;
  Mutex lock;

  MonMap *monmap;

  // timer.
  Context *tick_timer;
  Cond     tick_timer_cond;
  void cancel_tick();
  void reset_tick();
  friend class C_Mon_Tick;

  // my local store
  ObjectStore *store;

  const static int INO_ELECTOR = 1;
  const static int INO_MON_MAP = 2;
  const static int INO_OSD_MAP = 10;
  const static int INO_OSD_INC_MAP = 11;
  const static int INO_MDS_MAP = 20;

  // elector
  Elector elector;
  friend class Elector;

  epoch_t  mon_epoch;    // monitor epoch (election instance)
  set<int> quorum;       // current active set of monitors (if !starting)

  //void call_election();

  // monitor state
  const static int STATE_STARTING = 0; // electing
  const static int STATE_LEADER =   1;
  const static int STATE_PEON =     2;
  int state;

  int leader;                    // current leader (to best of knowledge)
  utime_t last_called_election;  // [starting] last time i called an election

  bool is_starting() { return state == STATE_STARTING; }
  bool is_leader() { return state == STATE_LEADER; }
  bool is_peon() { return state == STATE_PEON; }

  // my public services
  OSDMonitor *osdmon;
  MDSMonitor *mdsmon;
  ClientMonitor *clientmon;

  // messages
  void handle_shutdown(Message *m);
  void handle_ping_ack(class MPingAck *m);

  friend class OSDMonitor;
  friend class MDSMonitor;
  friend class ClientMonitor;


  // initiate election
  void call_election();

  // called by Elector when it's finished
  void win_election(set<int>& active) {
    leader = whoami;
    quorum = active;
    state = STATE_LEADER;
  } 
  void lose_election(int l) {
    state = STATE_PEON;
    leader = l;
  }

 public:
  Monitor(int w, Messenger *m, MonMap *mm) : 
    whoami(w), 
    messenger(m),
    monmap(mm),
    tick_timer(0),
    store(0),
    elector(this, w),
    mon_epoch(0), 
    state(STATE_STARTING),
    leader(0),
    osdmon(0), mdsmon(0), clientmon(0)
  {
    // hack leader, until election works.
    if (whoami == 0)
      state = STATE_LEADER;
    else
      state = STATE_PEON;
  }


  void init();
  void shutdown();
  void dispatch(Message *m);
  void tick(Context *timer);

};

#endif
