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


#ifndef __MON_PAXOS_H
#define __MON_PAXOS_H

#include "include/types.h"
#include "include/buffer.h"
#include "msg/Message.h"

#include "include/Context.h"

#include "common/Timer.h"

class Monitor;
class MMonPaxos;

// i am one state machine.
class Paxos {
  Monitor *mon;
  int whoami;

  // my state machine info
  int machine_id;
  const char *machine_name;
  map<version_t, bufferlist> accepted_values;
  map<version_t, int>        accepted_proposal_number;

  // proposer
  void propose(version_t v, bufferlist& value);
  
  void handle_last(MMonPaxos*);
  void handle_accept(MMonPaxos*);
  void handle_ack(MMonPaxos*);
  void handle_old_round(MMonPaxos*);
  
  version_t get_new_proposal_number(version_t gt=0);
  
  // accepter
  void handle_collect(MMonPaxos*);

  // learner
  void handle_success(MMonPaxos*);
  void handle_begin(MMonPaxos*);
  

public:
  Paxos(Monitor *m, int w,
	int mid,const char *mnm) : mon(m), whoami(w), 
				   machine_id(mid), machine_name(mnm) {
  }

  void dispatch(Message *m);

  void leader_start();

};



#endif

