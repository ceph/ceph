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
  
void Paxos::collect(version_t oldpn)
{
  // reset the number of lasts received
  accepted_pn = get_new_proposal_number(MAX(accepted_pn, oldpn));
  accepted_pn_from = last_committed;
  num_last = 1;
  old_accepted_pn = 0;
  old_accepted_value.clear();

  dout(10) << "collect with pn " << accepted_pn << endl;

  // send collect
  for (int i=0; i<mon->monmap->num_mon; ++i) {
    if (i == whoami) continue;
    
    MMonPaxos *collect = new MMonPaxos(MMonPaxos::OP_COLLECT, machine_id);
    collect->last_committed = last_committed;
    collect->pn = accepted_pn;
    mon->messenger->send_message(collect, mon->monmap->get_inst(i));
  }
}

void Paxos::handle_collect(MMonPaxos *collect)
{
  dout(10) << "handle_collect " << *collect << endl;

  // reply
  MMonPaxos *last = new MMonPaxos(MMonPaxos::OP_LAST, machine_id);
  last->last_committed = last_committed;
  
  // do we have an accepted but uncommitted value?
  //  (it'll be at last_committed+1)
  bufferlist bl;
  if (mon->store->exists_bl_sn(machine_name, last_committed+1)) {
    mon->store->get_bl_sn(bl, machine_name, last_committed+1);
    assert(bl.length() > 0);
    dout(10) << "sharing our accepted but uncommitted value for " << last_committed+1 << endl;
    last->values[last_committed+1] = bl;
    last->old_accepted_pn = accepted_pn;
  }

  // can we accept this pn?
  if (collect->pn > accepted_pn) {
    // ok, accept it
    accepted_pn = collect->pn;
    accepted_pn_from = collect->pn_from;
    dout(10) << "accepting pn " << accepted_pn << " from " << accepted_pn_from << endl;
  } else {
    // don't accept!
    dout(10) << "NOT accepting pn " << collect->pn << " from " << collect->pn_from 
	     << ", we already accepted " << accepted_pn << " from " << accepted_pn_from 
	     << endl;
  }
  last->pn = accepted_pn;
  last->pn_from = accepted_pn_from;

  // and share whatever data we have
  for (version_t v = collect->last_committed;
       v <= last_committed;
       v++) {
    if (mon->store->exists_bl_sn(machine_name, v)) {
      mon->store->get_bl_sn(last->values[v], machine_name, v);
      dout(10) << " sharing " << v << " " 
	       << last->values[v].length() << " bytes" << endl;
    }
  }

  // send reply
  mon->messenger->send_message(last, collect->get_source_inst());
  delete collect;
}


void Paxos::handle_last(MMonPaxos *last)
{
  dout(10) << "handle_last " << *last << endl;

  // share committed values?
  if (last->last_committed < last_committed) {
    // share committed values
    dout(10) << "sending commit to " << last->get_source() << endl;
    MMonPaxos *commit = new MMonPaxos(MMonPaxos::OP_COMMIT, machine_id);
    for (version_t v = last->last_committed;
	 v <= last_committed;
	 v++) {
      mon->store->get_bl_sn(commit->values[v], machine_name, v);
      dout(10) << "sharing " << v << " " 
	       << commit->values[v].length() << " bytes" << endl;
    }
    mon->messenger->send_message(commit, last->get_source_inst());
  }

  // did we receive committed value?
  if (last->last_committed > last_committed) {
    for (version_t v = last_committed;
	 v <= last->last_committed;
	 v++) {
      mon->store->put_bl_sn(last->values[v], machine_name, v);
      dout(10) << "committing " << v << " " 
	       << last->values[v].length() << " bytes" << endl;
    }
    last_committed = last->last_committed;
    mon->store->put_int(last_committed, machine_name, "last_commtted");
    dout(10) << "last_committed now " << last_committed << endl;
  }
      
  // do they accept your pn?
  if (last->old_accepted_pn > accepted_pn) {
    dout(10) << "uh oh, they have a higher pn than us.  pick a new one." << endl;
    collect(last->old_accepted_pn);
  } else {
    // they accepted our pn.  great.
    num_last++;
    dout(10) << "great, they accepted our pn, we now have " << num_last << endl;

    // did this person send back an accepted but uncommitted value?
    if (last->old_accepted_pn &&
	last->old_accepted_pn > old_accepted_pn) {
      version_t v = last->last_committed+1;
      dout(10) << "we learned an old value for " << v << " pn " << last->old_accepted_pn;
      old_accepted_pn = last->old_accepted_pn;
      old_accepted_value = last->values[v];
    }
    
    // do we have a majority?
    if (num_last == mon->monmap->num_mon/2+1) {
      // do this once.

      // did we learn an old value?
      if (old_accepted_value.length()) {
	dout(10) << "begin on old learned value" << endl;
	begin(old_accepted_value);
      }       
    }
  }

  delete last;
}


void Paxos::begin(bufferlist& v)
{
  dout(10) << "begin for " << last_committed+1 << " " 
	   << new_value.length() << " bytes"
	   << endl;

  // we must already have a majority for this to work.
  assert(num_last > mon->monmap->num_mon/2);

  // and no value, yet.
  assert(new_value.length() == 0);

  // accept it ourselves
  num_accepted = 1;
  new_value = v;
  mon->store->put_bl_sn(new_value, machine_name, last_committed+1);

  // ask others to accept it to!
  for (int i=0; i<mon->monmap->num_mon; ++i) {
    if (i == whoami) continue;

    dout(10) << " sending begin to mon" << i << endl;
    MMonPaxos *begin = new MMonPaxos(MMonPaxos::OP_BEGIN, machine_id);
    begin->values[last_committed+1] = new_value;
    begin->pn = accepted_pn;
    
    mon->messenger->send_message(begin, mon->monmap->get_inst(i));
  }
}

void Paxos::handle_begin(MMonPaxos *begin)
{
  dout(10) << "handle_begin " << *begin << endl;

  // can we accept this?
  if (begin->pn != accepted_pn) {
    dout(10) << " we accepted a higher pn " << accepted_pn << ", ignoring" << endl;
    delete begin;
    return;
  }
  
  // yes.
  version_t v = last_committed+1;
  dout(10) << "accepting value for " << v << " pn " << accepted_pn << endl;
  mon->store->put_bl_sn(begin->values[v], machine_name, v);
  
  // reply
  MMonPaxos *accept = new MMonPaxos(MMonPaxos::OP_ACCEPT, machine_id);
  accept->pn = accepted_pn;
  accept->last_committed = last_committed;
  mon->messenger->send_message(accept, begin->get_source_inst());
  
  delete begin;
}


void Paxos::handle_accept(MMonPaxos *accept)
{
  dout(10) << "handle_accept " << *accept << endl;
  
  if (accept->pn != accepted_pn) {
    // we accepted a higher pn, from some other leader
    dout(10) << " we accepted a higher pn " << accepted_pn << ", ignoring" << endl;
    delete accept;
    return;
  }
  if (accept->last_committed != last_committed) {
    dout(10) << " this is from an old round that's already committed, ignoring" << endl;
    delete accept;
    return;
  }

  num_accepted++;
  dout(10) << "now " << num_accepted << " have accepted" << endl;

  // new majority?
  if (num_accepted == mon->monmap->num_mon/2+1) {
    // yay, commit!
    dout(10) << "we got a majority, committing too" << endl;
    commit();
  }  

}

void Paxos::commit()
{
  dout(10) << "commit " << last_committed+1 << endl;

  // commit locally
  last_committed++;
  mon->store->put_int(last_committed, machine_name, "last_committed");

  // tell everyone
  for (int i=0; i<mon->monmap->num_mon; ++i) {
    if (i == whoami) continue;

    dout(10) << " sending commit to mon" << i << endl;
    MMonPaxos *commit = new MMonPaxos(MMonPaxos::OP_COMMIT, machine_id);
    commit->values[last_committed] = new_value;
    commit->pn = accepted_pn;
    
    mon->messenger->send_message(commit, mon->monmap->get_inst(i));
  }

  // get ready for a new round.
  new_value.clear();

}


void Paxos::handle_commit(MMonPaxos *commit)
{
  dout(10) << "handle_commit on " << commit->last_committed << endl;

  // commit locally.
  last_committed = commit->last_committed;
  mon->store->put_bl_sn(commit->values[last_committed], machine_name, last_committed);
  mon->store->put_int(last_committed, machine_name, "last_committed");
  
  delete commit;
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


void Paxos::leader_start()
{
  dout(10) << "leader_start -- i am the leader, start paxos" << endl;
  collect(0);
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
	
      case MMonPaxos::OP_BEGIN:
	handle_begin(pm);
	break;
	
      case MMonPaxos::OP_ACCEPT:
	handle_accept(pm);
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

