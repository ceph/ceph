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


#ifndef __OSDMONITOR_H
#define __OSDMONITOR_H

#include <time.h>

#include <map>
#include <set>
using namespace std;

#include "include/types.h"
#include "msg/Messenger.h"

class OSDMap;

class OSDMonitor : public Dispatcher {
  // me
  int whoami;
  Messenger *messenger;

  // maps
  OSDMap *osdmap;
  map<epoch_t, OSDMap*> osdmaps;

  // monitoring
  map<int,time_t>  last_heard_from_osd;
  map<int,time_t>  last_pinged_osd; 
  // etc..

  set<int>         failed_osds;
  set<int>         my_osds;

 public:
  OSDMonitor(int w, Messenger *m) : 
	whoami(w),
	messenger(m),
	osdmap(0) {
  }

  void init();

  void dispatch(Message *m);
  void handle_shutdown(Message *m);

  void handle_osd_boot(class MOSDBoot *m);
  void handle_osd_getmap(Message *m);

  void handle_ping_ack(class MPingAck *m);
  void handle_failure(class MFailure *m);

  void bcast_osd_map();


  // hack
  void fake_osd_failure(int osd, bool down);
  void fake_reorg();

};

#endif
