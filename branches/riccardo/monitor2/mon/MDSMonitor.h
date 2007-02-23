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

#ifndef __MDSMONITOR_H
#define __MDSMONITOR_H

#include <map>
#include <set>
using namespace std;

#include "include/types.h"
#include "msg/Messenger.h"

#include "mds/MDSMap.h"

class Monitor;

class MDSMonitor : public Dispatcher {
  Monitor *mon;
  Messenger *messenger;
  Mutex &lock;

  // mds maps
 public:
  MDSMap mdsmap;

 private:
  bufferlist encoded_map;

  //map<epoch_t, bufferlist> inc_maps;
  //MDSMap::Incremental pending_inc;
  
  list<entity_inst_t> awaiting_map;

  // beacons
  map<int, utime_t> last_beacon;

  bool is_alive(int mds);


  // maps
  void create_initial();
  void send_current();         // send current map to waiters.
  void send_full(entity_inst_t dest);
  void bcast_latest_mds();

  void issue_map();

  void save_map();
  void load_map();
  void print_map();

  //void accept_pending();   // accept pending, new map.
  //void send_incremental(epoch_t since, msg_addr_t dest);

  void handle_mds_state(class MMDSState *m);
  void handle_mds_beacon(class MMDSBeacon *m);
  //void handle_mds_failure(class MMDSFailure *m);
  void handle_mds_getmap(class MMDSGetMap *m);



 public:
  MDSMonitor(Monitor *mn, Messenger *m, Mutex& l) : mon(mn), messenger(m), lock(l) {
  }

  void dispatch(Message *m);
  void tick();  // check state, take actions

  void election_starting();
  void election_finished();

  void send_latest(entity_inst_t dest);

};

#endif
