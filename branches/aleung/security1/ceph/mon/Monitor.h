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

#include "common/Timer.h"

#include "MonMap.h"
#include "Elector.h"
#include "Paxos.h"

#include "crypto/CryptoLib.h"
using namespace CryptoLib;

class MonitorStore;
class OSDMonitor;
class MDSMonitor;
class ClientMonitor;

#define PAXOS_TEST       0
#define PAXOS_OSDMAP     1
#define PAXOS_MDSMAP     2
#define PAXOS_CLIENTMAP  3

class Monitor : public Dispatcher {
protected:
  // me
  int whoami;
  Messenger *messenger;
  Mutex lock;

  MonMap *monmap;

  // mon pub/priv keys
  esignPriv myPrivKey;
  esignPub myPubKey;

  // timer.
  SafeTimer timer;
  Context *tick_timer;
  void cancel_tick();
  void reset_tick();
  friend class C_Mon_Tick;

  // my local store
  //ObjectStore *store;
  MonitorStore *store;

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

  // paxos
  Paxos test_paxos;
  friend class Paxos;


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

  // end election (called by Elector)
  void win_election(set<int>& q);
  void lose_election(int l);



 public:
  Monitor(int w, Messenger *m, MonMap *mm) : 
    whoami(w), 
    messenger(m),
    monmap(mm),
    timer(lock), tick_timer(0),
    store(0),
    elector(this, w),
    mon_epoch(0), 
    
    test_paxos(this, w, PAXOS_TEST, "tester"),  // tester state machine

    state(STATE_STARTING),
    leader(0),
    osdmon(0), mdsmon(0), clientmon(0)
  {
  }

  //void set_new_private_key(string& pk);
  void set_new_private_key(char *pk);

  void init();
  void shutdown();
  void dispatch(Message *m);
  void tick();

};

#endif
