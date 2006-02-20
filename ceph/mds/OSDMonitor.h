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

class MDS;
class Message;

class OSDMonitor {
  MDS *mds;

  map<int,time_t>  last_heard_from_osd;
  map<int,time_t>  last_pinged_osd; 
  // etc..

  set<int>         failed_osds;
  set<int>         my_osds;

 public:
  OSDMonitor(MDS *mds) {
	this->mds = mds;
  }

  void init();

  void proc_message(Message *m);
  void handle_ping_ack(class MPingAck *m);
  void handle_failure(class MFailure *m);

  // hack
  void fake_reorg();

};

#endif
