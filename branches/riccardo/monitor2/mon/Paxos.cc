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

#include "Paxos.h"
#include "Monitor.h"

#include "messages/MMonPaxos.h"

#include "config.h"
#undef dout
#define  derr(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cerr << g_clock.now() << " mon" << whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".paxos(" << machine_id << ") "
#define  dout(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cout << g_clock.now() << " mon" << whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".paxos(" << machine_id << ") "


// ---------------------------------
// proposer


void Paxos::handle_last(MMonPaxos *m)
{
  dout(10) << "handle_last " << *m << endl;
  
  // ...

  delete m;
}



// ---------------------------------
// accepter


void Paxos::handle_commit(MMonPaxos *m)
{

  // ...

  delete m;
}




// ---------------------------------
// learner





// ---------------------------------

void Paxos::leader_start()
{
  dout(10) << "i am the leader" << endl;

  // .. do something else too 


  int who = 2;
  mon->messenger->send_message(new MMonPaxos(MMonPaxos::OP_COMMIT, machine_id, 12, 122),
							   MSG_ADDR_MON(who), mon->monmap->get_inst(who));
}



void Paxos::dispatch(Message *m)
{
  switch (m->get_type()) {
	
  case MSG_MON_PAXOS:
	{
	  MMonPaxos *pm = (MMonPaxos*)m;

	  // NOTE: these ops are defined in messages/MMonPaxos.h
	  // todo rf
	  switch (pm->op) {
		// learner
	  case MMonPaxos::OP_PROPOSE:
		handle_last(pm);
		break;

	  case MMonPaxos::OP_COMMIT:
		handle_commit(pm);
		break;

	  default:
		assert(0);
	  }
	}
	break;
	
  default:
	assert(0);
  }
}

