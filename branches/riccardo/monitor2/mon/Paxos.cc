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
#include "MonitorStore.h"

#include "messages/MMonPaxos.h"

#include "config.h"
#undef dout
#define  derr(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cerr << g_clock.now() << " mon" << whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".paxos(" << machine_name << ") "
#define  dout(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cout << g_clock.now() << " mon" << whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".paxos(" << machine_name << ") "


// ---------------------------------
// proposer
void Paxos::propose(version_t v, bufferlist& value)
{
//todo high rf
}
  
void Paxos::handle_last(MMonPaxos *m)
{
//todo high rf
  dout(10) << "handle_last " << *m << endl;
  delete m;
}

void Paxos::handle_accept(MMonPaxos *m)
{
//todo high rf
  dout(10) << "handle_accept " << *m << endl;
  delete m;
  
}

void Paxos::handle_ack(MMonPaxos *m)
{
//todo high rf
  dout(10) << "handle_ack " << *m << endl;
  delete m;
}

void Paxos::handle_old_round(MMonPaxos *m)
{
//todo high rf
  dout(10) << "handle_old_round " << *m << endl;
  delete m;
}
  

/*
 * return a globally unique, monotonically increasing proposal number
 */
version_t Paxos::get_new_proposal_number(version_t gt)
{
  // read last
  version_t last = mon->store->get_int("last_paxos_proposal");
  if (last < gt) 
    last = gt;
  
  // update
  last /= 100;
  last++;

  // make it unique among all monitors.
  version_t pn = last*100 + (version_t)whoami;
  
  // write
  mon->store->put_int(pn, "last_paxos_proposal");

  dout(10) << "get_new_proposal_number = " << pn << endl;
  return pn;
}


// ---------------------------------
// accepter
void Paxos::handle_collect(MMonPaxos *m)
{
//todo high rf
  // ...

  delete m;
}




// ---------------------------------
// learner
void Paxos::handle_success(MMonPaxos *m)
{
  //todo high rf
  delete m;
}

void Paxos::handle_begin(MMonPaxos *m)
{
  //todo high rf
  delete m;
}

// ---------------------------------

void Paxos::leader_start()
{
  dout(10) << "i am the leader" << endl;

  // .. do something else too 
  version_t pn = get_new_proposal_number();
  for (int i=0; i<mon->monmap->num_mon; ++i) {
    if (i == whoami) continue;
    // todo high rf I pass the pn twice... what is the last parameter for?
    mon->messenger->send_message(new MMonPaxos(MMonPaxos::OP_COLLECT, whoami, pn, pn),
				 mon->monmap->get_inst(i));
  }
}



void Paxos::dispatch(Message *m)
{
  switch (m->get_type()) {
	
  case MSG_MON_PAXOS:
    {
      MMonPaxos *pm = (MMonPaxos*)m;
      
      // NOTE: these ops are defined in messages/MMonPaxos.h
      switch (pm->op) {
	// learner
      case MMonPaxos::OP_COLLECT:
	handle_collect(pm);
	break;
	
      case MMonPaxos::OP_LAST:
	handle_last(pm);
	break;
	
      case MMonPaxos::OP_OLDROUND:
	handle_old_round(pm);
	break;
	
      case MMonPaxos::OP_BEGIN:
	handle_begin(pm);
	break;
	
      case MMonPaxos::OP_ACCEPT:
	handle_accept(pm);
	break;		
	
      case MMonPaxos::OP_SUCCESS:
	handle_success(pm);
	break;
	
      case MMonPaxos::OP_ACK:
	handle_ack(pm);
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

