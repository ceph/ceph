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
  MDSMap *mdsmap;

  map<epoch_t, bufferlist> maps;

  //map<epoch_t, bufferlist> inc_maps;
  //MDSMap::Incremental pending_inc;
  
  map<msg_addr_t,entity_inst_t> awaiting_map;
  

  // maps
  void create_initial();
  void send_current();         // send current map to waiters.
  void send_full(msg_addr_t dest, const entity_inst_t& inst);
  void bcast_latest_mds();

  //void accept_pending();   // accept pending, new map.
  //void send_incremental(epoch_t since, msg_addr_t dest);

  void handle_mds_boot(class MMDSBoot *m);
  void handle_mds_failure(class MMDSFailure *m);
  void handle_mds_getmap(class MMDSGetMap *m);

 public:
  MDSMonitor(Monitor *mn, Messenger *m, Mutex& l) : mon(mn), messenger(m), lock(l), mdsmap(0) {
	create_initial();
  }

  void dispatch(Message *m);
  void tick();  // check state, take actions
};

#endif
