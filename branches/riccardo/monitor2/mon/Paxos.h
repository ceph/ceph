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

/*
time---->

ccccccccccccccccccaaa??????????????????????????????????????
ccccccccccccccc????????????????????????????????????????????
ccccccccccccccccccaaa??????????????????????????????????????
ccccccccccccccccccaaa??????????????????????????????????????
ccca???????????????????????????????????????????????????????

collect(v>2)
 last and values for each v>2
  or
 oldround ...


what values we\'ve accepted, and for at pn\'s
what values we happen to know have been committed.


dddddddddddddddd_??????????????????????????????????????????
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

  // proposer
  /*
  version_t  last_version; // the last version i'm proposing
  int        num_last;     // number of peers that have responded my collect
  //version_t  last_pn;      // my proposal number
  int        num_accept;   // number of peers that have accepted my propose

  version_t  constrained_thru;
  */
  //map<version_t, version_t>  last_pn;

  // phase 1
  version_t  my_pn;        // my pn
  version_t  last_pn;      // the largest pn i've heard via a LAST
  bufferlist last_value;   // the value i'm proposing
  int        last_num;     // how many LAST's i've received
  
  bool have_majority() {
    return num_accept > (mon->monmap->num_mon / 2)+1;
  }

  void propose(version_t v, bufferlist& value);
  
  void handle_last(MMonPaxos*);
  void handle_accept(MMonPaxos*);
  void handle_ack(MMonPaxos*);
  void handle_old_round(MMonPaxos*);
  
  version_t get_new_proposal_number(version_t gt=0);
  
  // accepter
  map<version_t, bufferlist> accepted_values;
  map<version_t, version_t>  accepted_pn;

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

