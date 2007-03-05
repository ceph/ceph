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


#ifndef __MON_ELECTOR_H
#define __MON_ELECTOR_H

#include <map>
using namespace std;

#include "include/types.h"
#include "msg/Message.h"

#include "include/Context.h"

#include "common/Timer.h"

class Monitor;


class Elector {
 private:
  Monitor *mon;
  int whoami;

  Context *expire_event;

  void reset_timer(double plus=0.0);
  void cancel_timer();

  // electing me
  bool     electing_me;
  utime_t  start_stamp;
  set<int> acked_me;

  // electing them
  int     leader_acked;  // who i've acked
  utime_t ack_stamp;     // and when
  
 public:
 
  void start();   // start an electing me
  void defer(int who);
  void expire();  // timer goes off
  void victory();
   
  void handle_propose(class MMonElectionPropose *m);
  void handle_ack(class MMonElectionAck *m);
  void handle_victory(class MMonElectionVictory *m);

  
 public:  
  Elector(Monitor *m, int w) : mon(m), whoami(w) {
    // initialize all those values!
    // ...
  }

  void dispatch(Message *m);
};


#endif
