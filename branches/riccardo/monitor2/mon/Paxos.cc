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

// PHASE 1

// proposer
void Paxos::leader_start()
{
  dout(10) << "i am the leader, start paxos" << endl;
  
  // reset the number of lasts received
  last_num = 0;
  my_pn = get_new_proposal_number();
  last_pn = 0;

  // send collect
  for (int i=0; i<mon->monmap->num_mon; ++i) {
    if (i == whoami) continue;
    // todo high rf I pass the pn twice... what is the last parameter for?
    mon->messenger->send_message(new MMonPaxos(MMonPaxos::OP_COLLECT, whoami, pn, 0),
				 mon->monmap->get_inst(i));
  }
}

void Paxos::handle_collect(MMonPaxos *m)
{
  if (m->pn > accepted_pn[m->v]) {
    dout(10) << "handle_collect - replied LAST" << *m << endl;
    // send ack/last
    mon->messenger->send_message(new MMonPaxos(MMonPaxos::OP_LAST, machine_id,
					       accepted_pn[m->v], m->v,
					       accepted_value[m->v]));
    accepted_pn[m->v] = m->pn;
  }
  else {
    dout(10) << "handle_collect - replied OLDROUND" << *m << endl;
    // send nak/oldround
    mon->messenger->send_message(new MMonPaxos(MMonPaxos::OP_LAST, machine_id,
					       accepted_pn[m->v], m->v, 
					       accepted_value[m->v]));
  }
  
  delete m;
}

void Paxos::handle_last(MMonPaxos *m)
{
  dout(10) << "handle_last " << *m << endl;

  if (m->pn > last_pn) {
    dout(10) << " accepter accepted pn " << m->pn << ", taking that value" << endl;
    last_value = m->value;
  }

  // majority?
  bool had = have_majority();
  num_last++;

  if (!had && have_majority()) {
    dout(5) << "handle_last - we just got a majority" << endl;

    if (last_pn) {
      // propose previous value
      propose();
    } else {
      // it's unconstrained.
    }
  }




  // use == instead of > so that if we receive additional LAST messages, we will not do this again
  if (num_last == (unsigned)(mon->monmap->num_mon / 2)+1) {
    dout(5) << "handle_last - got majority" << endl;

    // propose
    propose
    
    // bcast to everyone else - begin
    num_accept = 0;
    for (int i=0; i<mon->monmap->num_mon; ++i) {
      if (i == whoami) continue;
      // we only set up our value if we get the majority, so for now we don't save it
      // todo high rf: where is the data we want to send?
      mon->messenger->send_message(new MMonPaxos(MMonPaxos::OP_BEGIN, machine_id, 
						 pn, DATA_TO_SEND),
				   mon->monmap->get_inst(i));
    }
  }
  delete m;
}

void Paxos::handle_oldround(MMonPaxos *m)
{
  dout(10) << "handle_oldround " << *m << endl;

  // the state is already constrained because an acceptor has
  // accepted with a higher pn than ours.
  
  // in any case, we can propose for this state.

  last_value = m->value;
  last_pn = m->pn;
  
  // ?
  if (have_majority())
    propose(); 

  delete m;
}



void Paxos::handle_accept(MMonPaxos *m)
{
  dout(10) << "handle_accept " << *m << endl;
  num_accepts++;
  if (num_accepts == (unsigned)(mon->monmap->num_mon / 2)+1){
    dout(5) << "handle_accept - bcast commit messages" << *m << endl;
    for (int i=0; i<mon->monmap->num_mon; ++i) {
      mon->messenger->send_message(new MMonPaxos(MMonPaxos::OP_COMMIT, 
						 whoami, 
						 pn, 
						 DATA_TO_COMMIT),
				   mon->monmap->get_inst(i));
    }
  }
  
  delete m;
  
}


void Paxos::propose(version_t v, bufferlist& value)
{
  last_value = value;
  last_version = v;

  // kick start whatever
  // send some message
}
  



void Paxos::handle_ack(MMonPaxos *m)
{
//todo high rf: Do we have to do anything here?!?
  dout(10) << "handle_ack " << *m << endl;
  delete m;
}

void Paxos::handle_old_round(MMonPaxos *m)
{
//todo high rf: should we get a new number (higher than the one returned by the other process) and try again?
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
  if (m->pn > accepted_pn[m->v]) {
     dout(10) << "handle_collect - replied LAST" << *m << endl;
     mon->messenger->send_message(new MMonPaxos(MMonPaxos::OP_LAST, machine_id,
						m->pn, m->v));
     accepted_pn[m->v] = m->pn;
  }
  else {
    dout(10) << "handle_collect - replied OLDROUND" << *m << endl;
    mon->messenger->send_message(new MMonPaxos(MMonPaxos::OP_OLDROUND, machine_id,
					       accepted_pn[m->v], m->v, 
					       accepted_value[m->v]));
  }
  
  delete m;
}




// ---------------------------------
// learner
void Paxos::handle_success(MMonPaxos *m)
{
  dout(10) << "handle_success - commit results" << endl;
  //todo high rf: copy from TEMP_RES TO RES
  mon->messenger->send_message(new MMonPaxos(MMonPaxos::OP_ACK, whoaim));
  delete m;
}

void Paxos::handle_begin(MMonPaxos *m)
{
  if (m->pn >= last) {
	   dout(10) << "handle_begin - replied ACCEPT" << *m << endl;
	   // todo high rf: save the new monitor map to TEMP_RES
	   mon->messenger->send_message(new MMonPaxos(MMonPaxos::OP_ACCEPT, whoami, last));
	   last = m->pn;
   }
   else {
	   dout(10) << "handle_begin - replied OLDROUND" << *m << endl;
	   mon->messenger->send_message(new MMonPaxos(MMonPaxos::OP_OLDROUND, whoami, last));
   }
  delete m;
}

// ---------------------------------



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

