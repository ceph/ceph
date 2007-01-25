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


#include "MDSMonitor.h"
#include "Monitor.h"

#include "messages/MMDSMap.h"
#include "messages/MMDSGetMap.h"
#include "messages/MMDSBeacon.h"

#include "common/Timer.h"

#include "config.h"
#undef dout
#define  dout(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cout << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".mds e" << mdsmap.get_epoch() << " "
#define  derr(l) if (l<=g_conf.debug || l<=g_conf.debug_mon) cerr << g_clock.now() << " mon" << mon->whoami << (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)"))) << ".mds e" << mdsmap.get_epoch() << " "



/********* MDS map **************/

void MDSMonitor::create_initial()
{
  mdsmap.epoch = 0;  // until everyone boots
  mdsmap.ctime = g_clock.now();
  
  /*
  for (int i=0; i<g_conf.num_mds; i++) {
    mdsmap.mds_set.insert(i);
    mdsmap.mds_state[i] = MDSMap::STATE_OUT;
  }
  */

  print_map();
}

void MDSMonitor::dispatch(Message *m)
{
  switch (m->get_type()) {

  case MSG_MDS_BEACON:
    handle_mds_beacon((MMDSBeacon*)m);
    break;
    
  case MSG_MDS_GETMAP:
    handle_mds_getmap((MMDSGetMap*)m);
    break;
    
  default:
    assert(0);
  }  
}

void MDSMonitor::print_map()
{
  dout(7) << "print_map epoch " << mdsmap.get_epoch() << endl;
  entity_inst_t blank;
  for (set<int>::iterator p = mdsmap.get_mds_set().begin();
       p != mdsmap.get_mds_set().end();
       ++p) {
    dout(7) << " mds" << *p 
	    << " : " << MDSMap::get_state_name(mdsmap.get_state(*p))
	    << " : " << (mdsmap.mds_inst.count(*p) ? mdsmap.get_inst(*p) : blank)
	    << endl;
  }
}

/*
void MDSMonitor::handle_mds_state(MMDSState *m)
{
  dout(7) << "mds_state " << MDSMap::get_state_name(m->get_state())
	  << " from " << m->get_source() << " at " << m->get_source_inst() << endl;
  assert(m->get_source().is_mds());
  int from = m->get_source().num();
  int state = m->get_state();
  
  if (state == MDSMap::STATE_STARTING) {
    // MDS BOOT

    // choose an MDS id
    if (from >= 0) {
      // wants to be a specific MDS. 
      if (mdsmap.is_down(from) ||
	  mdsmap.get_inst(from) == m->get_source_inst()) {
	// fine, whatever.
	dout(10) << "mds_state assigning requested mds" << from << endl;
      } else {
	dout(10) << "mds_state not assigning requested mds" << from 
		 << ", that mds is up and someone else" << endl;
	from = -1;
      }
    }
    if (from < 0) {
      // pick a failed mds?
      for (set<int>::iterator p = mdsmap.mds_set.begin();
	   p != mdsmap.mds_set.end();
	   ++p) {
	if (mdsmap.is_failed(*p)) {
	  dout(10) << "mds_state assigned failed mds" << from << endl;
	  from = *p;
	  break;
	}
      }
    }
    if (from < 0) {
      // ok, just pick any unused mds id.
      for (from=0; ; ++from) {
	if (mdsmap.is_out(from)) {
	  dout(10) << "mds_state assigned unused mds" << from << endl;
	  break;
	}
      }
    }
    
    // add to map
    mdsmap.mds_set.insert(from);
    mdsmap.mds_inst[from] = m->get_source_inst();
    if (!mdsmap.mds_state.count(from)) {
      // this is a new MDS!
      state = MDSMap::STATE_CREATING;
    }
  }
  else if (state == MDSMap::STATE_ACTIVE) {
    // MDS now active
    assert(mdsmap.is_starting(from) ||
	   mdsmap.is_creating(from));
  }

  // update mds state
  if (mdsmap.mds_state.count(from)) {
    dout(10) << "mds_state mds" << from << " " << MDSMap::get_state_name(mdsmap.mds_state[from])
	     << " -> " << MDSMap::get_state_name(state)
	     << endl;
  } else {
    dout(10) << "mds_state mds" << from << " is new"
	     << " -> " << MDSMap::get_state_name(state)
	     << endl;
  }
  mdsmap.mds_state[from] = state;

  // inc map version
  mdsmap.inc_epoch();
  mdsmap.encode(maps[mdsmap.get_epoch()]);

  // bcast map
  bcast_latest_mds();
  send_current();
}
*/

void MDSMonitor::handle_mds_beacon(MMDSBeacon *m)
{
  dout(7) << "mds_beacon " << *m
	  << " from " << m->get_source()
	  << " " << m->get_source_inst()
	  << endl;
  int from = m->get_source().num();
  int state = m->get_state();
  version_t seq = m->get_seq();

  // initial boot?
  if (state == MDSMap::STATE_STARTING) {
    bool booted = false;

    // choose an MDS id
    if (from >= 0) {
      // wants to be a specific MDS. 
      if (mdsmap.is_down(from) ||
	  mdsmap.get_inst(from) == m->get_source_inst()) {
	// fine, whatever.
	dout(10) << "mds_beacon assigning requested mds" << from << endl;
	booted = true;
      } else {
	dout(10) << "mds_beacon not assigning requested mds" << from 
		 << ", that mds is up and someone else" << endl;
	from = -1;
      }
    }
    if (from < 0) {
      // pick a failed mds?
      for (set<int>::iterator p = mdsmap.mds_set.begin();
	   p != mdsmap.mds_set.end();
	   ++p) {
	if (mdsmap.is_failed(*p)) {
	  dout(10) << "mds_beacon assigned failed mds" << from << endl;
	  from = *p;
	  booted = true;
	  break;
	}
      }
    }
    if (from < 0) {
      // ok, just pick any unused mds id.
      for (from=0; ; ++from) {
	if (mdsmap.is_out(from)) {
	  dout(10) << "mds_beacon assigned unused mds" << from << endl;
	  booted = true;
	  break;
	}
      }
    }
    
    // make sure it's in the map
    if (booted) {
      mdsmap.mds_set.insert(from);
      mdsmap.mds_inst[from] = m->get_source_inst();
    }

    if (!mdsmap.mds_state.count(from) ||
	mdsmap.mds_state[from] == MDSMap::STATE_CREATING) 
      state = MDSMap::STATE_CREATING;    // mds may not know it needs to create
  }

  // bad beacon?
  if (mdsmap.is_up(from) &&
      mdsmap.get_inst(from) != m->get_source_inst()) {
    dout(7) << "mds_beacon has mismatched inst, dropping" << endl;
    delete m;
    return;
  }

  if (mdsmap.mds_state_seq[from] > seq) {
    dout(7) << "mds_beacon " << *m << " has old seq, ignoring" << endl;
    delete m;
    return;
  }


  // reply to beacon.
  last_beacon[from] = g_clock.now();  // note time
  messenger->send_message(m, MSG_ADDR_MDS(from), m->get_source_inst());


  // did we update the map?
  if (mdsmap.mds_state[from] != state) {
    // update mds state
    dout(10) << "mds_beacon mds" << from << " " << MDSMap::get_state_name(mdsmap.mds_state[from])
	     << " -> " << MDSMap::get_state_name(state)
	     << endl;
    mdsmap.mds_state[from] = state;
    mdsmap.mds_state_seq[from] = seq;
    
    // inc map version
    mdsmap.inc_epoch();
    mdsmap.encode(maps[mdsmap.get_epoch()]);
    
    print_map();

    // bcast map
    bcast_latest_mds();
    send_current();
  }
}


void MDSMonitor::handle_mds_getmap(MMDSGetMap *m)
{
  dout(7) << "mds_getmap from " << m->get_source() << " " << m->get_source_inst() << endl;
  if (mdsmap.get_epoch() > 0)
    send_full(m->get_source(), m->get_source_inst());
  else
    awaiting_map[m->get_source()] = m->get_source_inst();
}


void MDSMonitor::bcast_latest_mds()
{
  dout(10) << "bcast_latest_mds " << mdsmap.get_epoch() << endl;
  
  // tell mds
  for (set<int>::iterator p = mdsmap.get_mds_set().begin();
       p != mdsmap.get_mds_set().end();
       p++) {
    if (mdsmap.is_down(*p)) continue;
    send_full(MSG_ADDR_MDS(*p), mdsmap.get_inst(*p));
  }
}

void MDSMonitor::send_full(msg_addr_t dest, const entity_inst_t& inst)
{
  dout(11) << "send_full to " << dest << " inst " << inst << endl;
  messenger->send_message(new MMDSMap(&mdsmap), dest, inst);
}

void MDSMonitor::send_current()
{
  dout(10) << "mds_send_current " << mdsmap.get_epoch() << endl;
  for (map<msg_addr_t,entity_inst_t>::iterator i = awaiting_map.begin();
       i != awaiting_map.end();
       i++) 
    send_full(i->first, i->second);
  awaiting_map.clear();
}

void MDSMonitor::send_latest(msg_addr_t dest, const entity_inst_t& inst)
{
  // FIXME: check if we're locked, etc.
  if (mdsmap.get_epoch() > 0)
    send_full(dest, inst);
  else
    awaiting_map[dest] = inst;
}
