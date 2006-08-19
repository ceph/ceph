// -*- mode:C++; tab-width:4; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#include <time.h>

#include <map>
#include <set>
using namespace std;

#include "include/types.h"
#include "msg/Messenger.h"

#include "osd/OSDMap.h"

class Monitor : public Dispatcher {
  // me
  int whoami;
  Messenger *messenger;

  // maps
  OSDMap *osdmap;
  map<epoch_t, bufferlist> maps;
  map<epoch_t, bufferlist> inc_maps;

  OSDMap::Incremental pending;

  map<epoch_t, map<msg_addr_t, epoch_t> > awaiting_map;

  // osd down -> out
  map<int,utime_t>  pending_out;

  
  void tick();  // check state, take actions

  // maps
  void accept_pending();   // accept pending, new map.
  void send_map();         // send current map to waiters.
  void send_full_map(msg_addr_t dest);
  void send_incremental_map(epoch_t since, msg_addr_t dest);
  void bcast_latest_osd_map_mds();
  void bcast_latest_osd_map_osd();

  void get_min_epoch();
  void start_read_timer();
  
  /*******************************************
  * Variables used by the election algorithm *
  *******************************************/
  // used during refresh phase
  int ack_msg_count;
  int refresh_num;
  
  // used during read phase
  int read_num;
  int status_msg_count;
  
  // the leader process id
  int leader_id;
  // f-accessible
  int f;
  
  // the processes that compose the group
//   vector<int> processes;
  // parameters for the process
  int main_delta;
  int trip_delta;
  
  // state variables
  hash_map<int, state> registry;
  hash_map<int, view> views;
  hash_map<int, view> old_views;
  
  // round trip timer
  round_trip_task
  /************************************************
  * END> Variables used by the election algorithm *
  *************************************************/

 public:
  Monitor(int w, Messenger *m) : 
	whoami(w),
	messenger(m),
	osdmap(0) {
  }

  void init();

  void dispatch(Message *m);
  void handle_shutdown(Message *m);

  void handle_failure(class MFailure *m);

  void handle_osd_boot(class MOSDBoot *m);
  void handle_osd_getmap(class MOSDGetMap *m);

  void handle_ping_ack(class MPingAck *m);
  
  // handles for election messages
  void handle_ack_msg(class MMonElectionAck);
  void handle_collect_msg(class MMonElectionCollect);
  void handle_refresh_msg(class MMonElectionRefresh);
  void handle_status_msg(class MMoneElectionStatus);
  
  // hack
  void fake_osd_failure(int osd, bool down);
  void fake_reorg();

};

#endif
