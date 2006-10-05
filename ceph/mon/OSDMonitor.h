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


#ifndef __OSDMONITOR_H
#define __OSDMONITOR_H

#include <map>
#include <set>
using namespace std;

#include "include/types.h"
#include "msg/Messenger.h"

#include "osd/OSDMap.h"

class Monitor;

class OSDMonitor : public Dispatcher {
  Monitor *mon;
  Messenger *messenger;
  Mutex &lock;

  // osd maps
  OSDMap *osdmap;
  map<epoch_t, bufferlist> maps;
  map<epoch_t, bufferlist> inc_maps;

  OSDMap::Incremental pending_inc;
  
  map<epoch_t, map<msg_addr_t, epoch_t> > awaiting_map;
  
  // osd down -> out
  map<int,utime_t>    down_pending_out;
  

  // maps
  void create_initial();
  void accept_pending();   // accept pending, new map.
  void send_current();         // send current map to waiters.
  void send_full(msg_addr_t dest);
  void send_incremental(epoch_t since, msg_addr_t dest);
  void bcast_latest_mds();
  void bcast_latest_osd();

  void handle_osd_boot(class MOSDBoot *m);
  void handle_osd_in(class MOSDIn *m);
  void handle_osd_out(class MOSDOut *m);
  void handle_osd_failure(class MOSDFailure *m);
  void handle_osd_getmap(class MOSDGetMap *m);

 public:
  OSDMonitor(Monitor *mn, Messenger *m, Mutex& l) : mon(mn), messenger(m), lock(l) {
	create_initial();
  }

  void dispatch(Message *m);
  void tick();  // check state, take actions

  void fake_osd_failure(int osd, bool down);
  void fake_osdmap_update();
  void fake_reorg();
};

#endif
