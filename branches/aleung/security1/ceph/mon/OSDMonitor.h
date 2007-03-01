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
public:
  OSDMap osdmap;

private:
  map<entity_name_t, pair<entity_inst_t, epoch_t> > awaiting_map;
  
  void create_initial();
  bool get_map_bl(epoch_t epoch, bufferlist &bl);
  bool get_inc_map_bl(epoch_t epoch, bufferlist &bl);

  void save_map();
  void save_inc_map(OSDMap::Incremental &inc);

  // [leader]
  OSDMap::Incremental pending_inc;
  map<int,utime_t>    down_pending_out;  // osd down -> out

  set<int>            pending_ack; 

  // we are distributed
  const static int STATE_INIT = 0;     // startup
  const static int STATE_SYNC = 1;     // sync map copy (readonly)
  const static int STATE_LOCK = 2;     // [peon] map locked
  const static int STATE_UPDATING = 3; // [leader] map locked, waiting for peon ack

  int state;
  utime_t lease_expire;     // when lease expires
  
  //void init();

  // maps
  void accept_pending();   // accept pending, new map.
  void send_waiting();     // send current map to waiters.
  void send_full(entity_inst_t dest);
  void send_incremental(epoch_t since, entity_inst_t dest);
  void bcast_latest_mds();
  void bcast_latest_osd();
  
  void update_map();

  void handle_osd_boot(class MOSDBoot *m);
  void handle_osd_in(class MOSDIn *m);
  void handle_osd_out(class MOSDOut *m);
  void handle_osd_failure(class MOSDFailure *m);
  void handle_osd_getmap(class MOSDGetMap *m);

  void handle_info(class MMonOSDMapInfo*);
  void handle_lease(class MMonOSDMapLease*);
  void handle_lease_ack(class MMonOSDMapLeaseAck*);
  void handle_update_prepare(class MMonOSDMapUpdatePrepare*);
  void handle_update_ack(class MMonOSDMapUpdateAck*);
  void handle_update_commit(class MMonOSDMapUpdateCommit*);

 public:
  OSDMonitor(Monitor *mn, Messenger *m, Mutex& l) : 
    mon(mn), messenger(m), lock(l),
    state(STATE_SYNC) {
    //init();
  }

  void dispatch(Message *m);
  void tick();  // check state, take actions

  void election_starting();  // abort whatever.
  void election_finished();  // reinitialize whatever.

  void issue_leases();

  void mark_all_down();

  void fake_osd_failure(int osd, bool down);
  void fake_osdmap_update();
  void fake_reorg();
};

#endif
