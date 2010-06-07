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

  Context *expire_event;

  void reset_timer(double plus=0.0);
  void cancel_timer();

  epoch_t epoch;   // latest epoch we've seen.  odd == election, even == stable, 

  // electing me
  bool     electing_me;
  utime_t  start_stamp;
  set<int> acked_me;

  // electing them
  int     leader_acked;  // who i've acked
  utime_t ack_stamp;     // and when
  
  void bump_epoch(epoch_t e=0);  // i just saw a larger epoch

  class C_ElectionExpire : public Context {
    Elector *elector;
  public:
    C_ElectionExpire(Elector *e) : elector(e) { }
    void finish(int r) {
      elector->expire();
    }
  };

  void start();   // start an electing me
  void defer(int who);
  void expire();  // timer goes off
  void victory();
   
  void handle_propose(class MMonElection *m);
  void handle_ack(class MMonElection *m);
  void handle_victory(class MMonElection *m);
  
 public:  
  Elector(Monitor *m) : mon(m),
			       expire_event(0),
			       epoch(0),
			       electing_me(false),
			       leader_acked(-1) { }

  void init();
  void shutdown();

  void dispatch(Message *m);

  void call_election() {
    start();
  }

};


#endif
