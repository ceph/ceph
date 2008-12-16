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
 
/* Metadata Server Monitor
 */

#ifndef __MDSMONITOR_H
#define __MDSMONITOR_H

#include <map>
#include <set>
using namespace std;

#include "include/types.h"
#include "msg/Messenger.h"

#include "mds/MDSMap.h"

#include "PaxosService.h"

class MMDSBeacon;
class MMDSGetMap;
class MMonCommand;

class MDSMonitor : public PaxosService {
 public:
  // mds maps
  MDSMap mdsmap;          // current
  bufferlist mdsmap_bl;   // encoded

  MDSMap pending_mdsmap;  // current + pending updates

  // my helpers
  void print_map(MDSMap &m, int dbl=7);

  class C_Updated : public Context {
    MDSMonitor *mm;
    MMDSBeacon *m;
  public:
    C_Updated(MDSMonitor *a, MMDSBeacon *c) :
      mm(a), m(c) {}
    void finish(int r) {
      if (r >= 0)
	mm->_updated(m);   // success
      else
	mm->dispatch((Message*)m);        // try again
    }
  };


  // service methods
  void create_initial();
  bool update_from_paxos();
  void create_pending(); 
  void encode_pending(bufferlist &bl);
  
  void _updated(MMDSBeacon *m);
 
  bool preprocess_query(Message *m);  // true if processed.
  bool prepare_update(Message *m);
  bool should_propose(double& delay);

  void committed();

  bool preprocess_beacon(class MMDSBeacon *m);
  bool prepare_beacon(class MMDSBeacon *m);
  void handle_mds_getmap(MMDSGetMap *m);

  bool preprocess_command(MMonCommand *m);
  bool prepare_command(MMonCommand *m);

  // beacons
  map<entity_addr_t, utime_t> last_beacon;

public:
  MDSMonitor(Monitor *mn, Paxos *p) : PaxosService(mn, p) { }

  // sending the map
private:
  list<entity_inst_t> waiting_for_map;

  void bcast_latest_mds();
  void send_full(entity_inst_t dest);
  void send_to_waiting();

public:
  void send_latest(entity_inst_t dest);

  void tick();     // check state, take actions
  void do_stop();

};

#endif
