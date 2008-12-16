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


#include "MDSMonitor.h"
#include "Monitor.h"
#include "MonitorStore.h"
#include "OSDMonitor.h"

#include "messages/MMDSMap.h"
#include "messages/MMDSGetMap.h"
#include "messages/MMDSBeacon.h"
#include "messages/MMonCommand.h"

#include "messages/MGenericMessage.h"


#include "common/Timer.h"

#include <sstream>

#include "config.h"

#define DOUT_SUBSYS mon
#undef dout_prefix
#define dout_prefix _prefix(mon, mdsmap)
static ostream& _prefix(Monitor *mon, MDSMap& mdsmap) {
  return *_dout << dbeginl
		<< "mon" << mon->whoami 
		<< (mon->is_starting() ? (const char*)"(starting)":(mon->is_leader() ? (const char*)"(leader)":(mon->is_peon() ? (const char*)"(peon)":(const char*)"(?\?)")))
		<< ".mds e" << mdsmap.get_epoch() << " ";
}



// my methods

void MDSMonitor::print_map(MDSMap &m, int dbl)
{
  dout(7) << "print_map";
  m.print(*_dout);
  *_dout << dendl;
}



// service methods

void MDSMonitor::create_initial()
{
  dout(10) << "create_initial" << dendl;
  pending_mdsmap.max_mds = 1;
  pending_mdsmap.created = g_clock.now();
  print_map(pending_mdsmap);
}


bool MDSMonitor::update_from_paxos()
{
  assert(paxos->is_active());

  version_t paxosv = paxos->get_version();
  if (paxosv == mdsmap.epoch) return true;
  assert(paxosv >= mdsmap.epoch);

  dout(10) << "update_from_paxos paxosv " << paxosv 
	   << ", my e " << mdsmap.epoch << dendl;

  // read and decode
  mdsmap_bl.clear();
  bool success = paxos->read(paxosv, mdsmap_bl);
  assert(success);
  dout(10) << "update_from_paxos  got " << paxosv << dendl;
  mdsmap.decode(mdsmap_bl);

  // save as 'latest', too.
  paxos->stash_latest(paxosv, mdsmap_bl);

  // new map
  dout(4) << "new map" << dendl;
  print_map(mdsmap, 0);

  if (mon->is_leader()) {
    // bcast map to mds
    bcast_latest_mds();
  }

  send_to_waiting();

  return true;
}

void MDSMonitor::create_pending()
{
  pending_mdsmap = mdsmap;
  pending_mdsmap.epoch++;
  dout(10) << "create_pending e" << pending_mdsmap.epoch << dendl;
}

void MDSMonitor::encode_pending(bufferlist &bl)
{
  dout(10) << "encode_pending e" << pending_mdsmap.epoch << dendl;
  
  //print_map(pending_mdsmap);

  // apply to paxos
  assert(paxos->get_version() + 1 == pending_mdsmap.epoch);
  pending_mdsmap.encode(bl);
}


bool MDSMonitor::preprocess_query(Message *m)
{
  dout(10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;

  switch (m->get_type()) {
    
  case MSG_MDS_BEACON:
    return preprocess_beacon((MMDSBeacon*)m);
    
  case CEPH_MSG_MDS_GETMAP:
    handle_mds_getmap((MMDSGetMap*)m);
    return true;

  case MSG_MON_COMMAND:
    return preprocess_command((MMonCommand*)m);

  default:
    assert(0);
    delete m;
    return true;
  }
}

void MDSMonitor::handle_mds_getmap(MMDSGetMap *m)
{
  if (m->want <= mdsmap.get_epoch())
    send_full(m->get_orig_source_inst());
  else
    waiting_for_map.push_back(m->get_orig_source_inst());
}


bool MDSMonitor::preprocess_beacon(MMDSBeacon *m)
{
  int from = m->get_orig_source_inst().name.num();
  entity_addr_t addr = m->get_orig_source_inst().addr;
  int state = m->get_state();
  version_t seq = m->get_seq();

  if (!ceph_fsid_equal(&m->get_fsid(), &mon->monmap->fsid)) {
    dout(0) << "preprocess_beacon on fsid " << m->get_fsid() << " != " << mon->monmap->fsid << dendl;
    goto out;
  }

  dout(12) << "preprocess_beacon " << *m
	   << " from " << m->get_orig_source_inst()
	   << dendl;

  // fw to leader?
  if (!mon->is_leader())
    return false;

  // booted, but not in map?
  if (state != MDSMap::STATE_BOOT &&
      pending_mdsmap.get_addr_rank(addr) < 0 &&
      !pending_mdsmap.is_standby(addr)) {
    dout(7) << "mds_beacon " << *m << " is not in mdsmap" << dendl;
    send_latest(m->get_orig_source_inst());
    return true;
  }

  // can i handle this query without a map update?

  // no longer laggy?
  if (pending_mdsmap.laggy.count(addr)) {
    return false;  // need to update map.
  }
  // boot?
  else if (state == MDSMap::STATE_BOOT) {
    // already booted?
    if (pending_mdsmap.get_addr_rank(addr) == -1)
      return false; // not booted|booting|standby yet

    // ignore.
    goto out;
  }
  else if (state == MDSMap::STATE_STANDBY) {
    // standby?
    if (!pending_mdsmap.is_standby(addr) &&
	!mdsmap.is_standby(addr)) {
      dout(7) << "mds_beacon " << *m << " claiming standby, but not, ignoring" << dendl;
      goto out;
    }
    // reply.
  }
  else {
    // old seq?
    if (mdsmap.mds_state_seq[from] > seq) {
      dout(7) << "mds_beacon " << *m << " has old seq, ignoring" << dendl;
      goto out;
    }
    
    // is there a state change here?
    if (mdsmap.mds_state.count(from) == 0) { 
      dout(1) << "mds_beacon " << *m << " announcing non-boot|standby state, ignoring" << dendl;
      goto out;
    }

    if (mdsmap.mds_state[from] != state) {
      if (mdsmap.get_epoch() == m->get_last_epoch_seen()) 
	return false;  // need to update map
      dout(10) << "mds_beacon " << *m << " ignoring requested state, because mds hasn't seen latest map" << dendl;
    }
  }

  // note time and reply
  dout(15) << "mds_beacon " << *m << " noting time and replying" << dendl;
  last_beacon[addr] = g_clock.now();  
  mon->messenger->send_message(new MMDSBeacon(mon->monmap->fsid,
					      mdsmap.get_epoch(), state, seq, 0), 
			       m->get_orig_source_inst());

  // done
 out:
  delete m;
  return true;
}


bool MDSMonitor::prepare_update(Message *m)
{
  dout(7) << "prepare_update " << *m << dendl;

  switch (m->get_type()) {
    
  case MSG_MDS_BEACON:
    return prepare_beacon((MMDSBeacon*)m);

  case MSG_MON_COMMAND:
    return prepare_command((MMonCommand*)m);
  
  default:
    assert(0);
    delete m;
  }

  return true;
}



bool MDSMonitor::prepare_beacon(MMDSBeacon *m)
{
  // -- this is an update --
  dout(12) << "handle_beacon " << *m
	   << " from " << m->get_orig_source_inst()
	   << dendl;
  int from = m->get_orig_source_inst().name.num();
  entity_addr_t addr = m->get_orig_source_inst().addr;
  int state = m->get_state();
  version_t seq = m->get_seq();

  if (pending_mdsmap.laggy.count(addr)) {
    dout(10) << "prepare_beacon clearly laggy flag on " << addr << dendl;
    pending_mdsmap.laggy.erase(addr);
  }
  
  // boot?
  int standby_for = -1;
  if (state == MDSMap::STATE_BOOT) {
    from = -1;  

    // standby for a given rank?
    standby_for = m->get_want_rank();
    if (standby_for >= pending_mdsmap.max_mds) {
      dout(10) << "mds_beacon boot: wanted standby for mds" << from 
	       << " >= max_mds " << pending_mdsmap.max_mds
	       << ", will be shared standby" << dendl;
      standby_for = -1;
    }
    if (standby_for >= 0 && pending_mdsmap.is_down(standby_for)) {
      // wants to be a specific MDS, who is down
      from = standby_for;
      switch (pending_mdsmap.get_state(standby_for)) {
      case MDSMap::STATE_STOPPED:
	state = MDSMap::STATE_STARTING;
	break;
      case MDSMap::STATE_DNE:
	state = MDSMap::STATE_CREATING;
	break;
      case MDSMap::STATE_FAILED:
	state = MDSMap::STATE_REPLAY;
	break;
      default:
	assert(0);
      }
      dout(10) << "mds_beacon boot: mds" << from
	       << " was " << MDSMap::get_state_name(pending_mdsmap.get_state(from))
	       << ", " << MDSMap::get_state_name(state) 
	       << dendl;
    }
    else if (standby_for < 0) {
      // pick another failed mds?
      set<int> failed;
      pending_mdsmap.get_failed_mds_set(failed);
      if (!failed.empty()) {
	from = *failed.begin();
	dout(10) << "mds_beacon boot: assigned failed mds" << from << dendl;
	state = MDSMap::STATE_REPLAY;
      }
    }
    if (from < 0 && standby_for < 0 && 
	!pending_mdsmap.is_degraded()) {
      // ok, just pick any unused mds rank
      //  that doesn't make us overfull
      for (int i=0; i<pending_mdsmap.max_mds; i++) {
	if (pending_mdsmap.would_be_overfull_with(i)) continue;
	if (pending_mdsmap.is_dne(i)) {
	  from = i;
	  dout(10) << "mds_beacon boot: assigned new mds" << from << dendl;
	  state = MDSMap::STATE_CREATING;
	  break;
	} else if (pending_mdsmap.is_stopped(i)) {
	  from = i;
	  dout(10) << "mds_beacon boot: assigned stopped mds" << from << dendl;
	  state = MDSMap::STATE_STARTING;
	  break;
	}
      }
    }

    if (from < 0) {
      // standby
      if (standby_for < 0) {
	dout(10) << "mds_beacon boot: standby for any" << dendl;
	pending_mdsmap.standby_any.insert(addr);
      } else {
	dout(10) << "mds_beacon boot: standby for mds" << standby_for << dendl;
	pending_mdsmap.standby_for[standby_for].insert(addr);
      }
      pending_mdsmap.standby[addr].mds = standby_for;
      pending_mdsmap.standby[addr].state = MDSMap::STATE_STANDBY;
      state = MDSMap::STATE_STANDBY;
    } else {
      // join|takeover
      assert(state == MDSMap::STATE_CREATING ||
	     state == MDSMap::STATE_STARTING ||
	     state == MDSMap::STATE_REPLAY);
    
      pending_mdsmap.mds_inst[from].addr = addr;
      pending_mdsmap.mds_inst[from].name = entity_name_t::MDS(from);
      pending_mdsmap.mds_inc[from]++;
      pending_mdsmap.mds_state[from] = state;
      pending_mdsmap.mds_state_seq[from] = seq;
    }

    // initialize the beacon timer
    last_beacon[addr] = g_clock.now();

  } else {
    // state change
    dout(10) << "mds_beacon mds" << from << " " << MDSMap::get_state_name(mdsmap.mds_state[from])
	     << " -> " << MDSMap::get_state_name(state)
	     << dendl;

    // change the state
    pending_mdsmap.mds_state[from] = state;
    if (pending_mdsmap.is_up(from))
      pending_mdsmap.mds_state_seq[from] = seq;
    else
      pending_mdsmap.mds_state_seq.erase(from);
  }

  dout(7) << "pending map now:" << dendl;
  print_map(pending_mdsmap);
  
  paxos->wait_for_commit(new C_Updated(this, from, m));

  return true;
}

bool MDSMonitor::should_propose(double& delay)
{
  delay = 0.0;
  return true;
}

void MDSMonitor::_updated(int from, MMDSBeacon *m)
{
  if (from < 0) {
    dout(10) << "_updated (booted) mds" << from << " " << *m << dendl;
    mon->osdmon()->send_latest(m->get_orig_source_inst());
  } else {
    dout(10) << "_updated mds" << from << " " << *m << dendl;
  }
  if (m->get_state() == MDSMap::STATE_STOPPED) {
    // send the map manually (they're out of the map, so they won't get it automatic)
    send_latest(m->get_orig_source_inst());
  }

  delete m;
}


void MDSMonitor::committed()
{
  // check for failed
  set<int> failed;
  pending_mdsmap.get_failed_mds_set(failed);

  if (!pending_mdsmap.standby.empty() && !failed.empty()) {
    bool didtakeover = false;
    set<int>::iterator p = failed.begin();
    while (p != failed.end()) {
      int f = *p++;
      
      // someone standby for me?
      if (pending_mdsmap.standby_for.count(f) &&
	  !pending_mdsmap.standby_for[f].empty()) {
	dout(0) << "mds" << f << " standby " << *pending_mdsmap.standby_for[f].begin() << " taking over" << dendl;
	take_over(*pending_mdsmap.standby_for[f].begin(), f);
	didtakeover = true;
      }
      else if (!pending_mdsmap.standby_any.empty()) {
	dout(0) << "standby " << pending_mdsmap.standby.begin()->first << " taking over for mds" << f << dendl;
	take_over(pending_mdsmap.standby.begin()->first, f);
	didtakeover = true;
      }
    }
    if (didtakeover) {
      dout(7) << "pending map now:" << dendl;
      print_map(pending_mdsmap);
      propose_pending();
    }
  }

  // hackish: did all mds's shut down?
  if (mon->is_leader() &&
      g_conf.mon_stop_with_last_mds &&
      mdsmap.get_epoch() > 1 &&
      mdsmap.is_stopped()) 
    mon->messenger->send_message(new MGenericMessage(CEPH_MSG_SHUTDOWN), 
				 mon->monmap->get_inst(mon->whoami));
}

void MDSMonitor::take_over(entity_addr_t addr, int mds)
{
  pending_mdsmap.mds_inst[mds].addr = addr;
  pending_mdsmap.mds_inst[mds].name = entity_name_t::MDS(mds);
  pending_mdsmap.mds_inc[mds]++;
  pending_mdsmap.mds_state[mds] = MDSMap::STATE_REPLAY;
  pending_mdsmap.mds_state_seq[mds] = 0;

  // remove from standby list(s)
  pending_mdsmap.standby.erase(addr);
  pending_mdsmap.standby_for[mds].erase(addr);
  pending_mdsmap.standby_any.erase(addr);
}


bool MDSMonitor::preprocess_command(MMonCommand *m)
{
  int r = -1;
  bufferlist rdata;
  stringstream ss;

  if (m->cmd.size() > 1) {
    if (m->cmd[1] == "stat") {
      ss << mdsmap;
      r = 0;
    } 
    else if (m->cmd[1] == "dump") {
      stringstream ds;
      mdsmap.print(ds);
      rdata.append(ds);
      ss << "dumped mdsmap epoch " << mdsmap.get_epoch();
      r = 0;
    }
    else if (m->cmd[1] == "getmap") {
      mdsmap.encode(rdata);
      ss << "got mdsmap epoch " << mdsmap.get_epoch();
      r = 0;
    }
    else if (m->cmd[1] == "injectargs" && m->cmd.size() == 4) {
      if (m->cmd[2] == "*") {
	for (int i=0; i<mdsmap.get_max_mds(); i++)
	  if (mdsmap.is_active(i))
	    mon->inject_args(mdsmap.get_inst(i), m->cmd[3]);
	r = 0;
	ss << "ok bcast";
      } else {
	errno = 0;
	int who = strtol(m->cmd[2].c_str(), 0, 10);
	if (!errno && who >= 0 && mdsmap.is_active(who)) {
	  mon->inject_args(mdsmap.get_inst(who), m->cmd[3]);
	  r = 0;
	  ss << "ok";
	} else 
	  ss << "specify mds number or *";
      }
    }
  }

  if (r != -1) {
    string rs;
    getline(ss, rs);
    mon->reply_command(m, r, rs, rdata);
    return true;
  } else
    return false;
}

bool MDSMonitor::prepare_command(MMonCommand *m)
{
  int r = -EINVAL;
  stringstream ss;
  bufferlist rdata;

  if (m->cmd.size() > 1) {
    if (m->cmd[1] == "stop" && m->cmd.size() > 2) {
      int who = atoi(m->cmd[2].c_str());
      if (mdsmap.is_active(who)) {
	r = 0;
	ss << "telling mds" << who << " to stop";
	pending_mdsmap.mds_state[who] = MDSMap::STATE_STOPPING;
      } else {
	r = -EEXIST;
	ss << "mds" << who << " not active (" 
	   << mdsmap.get_state_name(mdsmap.get_state(who)) << ")";
      }
    }
    else if (m->cmd[1] == "set_max_mds" && m->cmd.size() > 2) {
      pending_mdsmap.max_mds = atoi(m->cmd[2].c_str());
      r = 0;
      ss << "max_mds = " << pending_mdsmap.max_mds;
    }
  }
  if (r == -EINVAL) 
    ss << "unrecognized command";
  string rs;
  getline(ss, rs);

  if (r >= 0) {
    // success.. delay reply
    paxos->wait_for_commit(new Monitor::C_Command(mon, m, r, rs));
    return true;
  } else {
    // reply immediately
    mon->reply_command(m, r, rs, rdata);
    return false;
  }
}



void MDSMonitor::bcast_latest_mds()
{
  dout(10) << "bcast_latest_mds " << mdsmap.get_epoch() << dendl;
  
  // tell mds
  set<int> up;
  mdsmap.get_up_mds_set(up);
  for (set<int>::iterator p = up.begin();
       p != up.end();
       p++) 
    send_full(mdsmap.get_inst(*p));

  // standby too
  entity_inst_t inst;
  inst.name = entity_name_t::MDS(-1);
  for (map<entity_addr_t,MDSMap::standby_t>::iterator p = mdsmap.standby.begin();
       p != mdsmap.standby.end();
       p++) {
    inst.addr = p->first;
    send_full(inst);
  }
}

void MDSMonitor::send_full(entity_inst_t dest)
{
  dout(11) << "send_full to " << dest << dendl;
  mon->messenger->send_message(new MMDSMap(mon->monmap->fsid, &mdsmap), dest);
}

void MDSMonitor::send_to_waiting()
{
  dout(10) << "send_to_waiting " << mdsmap.get_epoch() << dendl;
  for (list<entity_inst_t>::iterator i = waiting_for_map.begin();
       i != waiting_for_map.end();
       i++) 
    send_full(*i);
  waiting_for_map.clear();
}

void MDSMonitor::send_latest(entity_inst_t dest)
{
  if (paxos->is_readable()) 
    send_full(dest);
  else
    waiting_for_map.push_back(dest);
}

void MDSMonitor::tick()
{
  // make sure mds's are still alive
  // ...if i am an active leader
  if (!paxos->is_active()) return;

  update_from_paxos();
  dout(10) << mdsmap << dendl;
  
  bool do_propose = false;

  if (!mon->is_leader()) return;

  // expand mds cluster?
  int cursize = pending_mdsmap.get_num_in_mds() +
    pending_mdsmap.get_num_mds(MDSMap::STATE_CREATING) +
    pending_mdsmap.get_num_mds(MDSMap::STATE_STARTING);
  if (cursize < pending_mdsmap.get_max_mds() &&
      !pending_mdsmap.is_degraded() &&
      pending_mdsmap.get_num_standby_any() > 0) {
    entity_addr_t addr = *pending_mdsmap.standby_any.begin();    
    int mds = 0;
    while (pending_mdsmap.is_in(mds) ||
	   pending_mdsmap.is_creating(mds) ||
	   pending_mdsmap.is_starting(mds))
      mds++;
    dout(1) << "adding standby " << addr << " as mds" << mds << dendl;
    
    pending_mdsmap.mds_inst[mds].addr = addr;
    pending_mdsmap.mds_inst[mds].name = entity_name_t::MDS(mds);
    pending_mdsmap.mds_inc[mds]++;
    if (mdsmap.is_dne(mds))
      pending_mdsmap.mds_state[mds] = MDSMap::STATE_CREATING;
    else if (mdsmap.is_stopped(mds))
      pending_mdsmap.mds_state[mds] = MDSMap::STATE_STARTING;
    else 
      assert(0); // whoops!
    pending_mdsmap.mds_state_seq[mds] = 0;

    // remove from standby list(s)
    pending_mdsmap.standby.erase(addr);
    pending_mdsmap.standby_any.erase(addr);
    do_propose = true;
  }

  // check beacon timestamps
  utime_t now = g_clock.now();
  utime_t cutoff = now;
  cutoff -= g_conf.mds_beacon_grace;

  // make sure last_beacon is populated
  for (map<int32_t,entity_inst_t>::iterator p = mdsmap.mds_inst.begin();
       p != mdsmap.mds_inst.end();
       ++p) 
    if (last_beacon.count(p->second.addr) == 0 &&
	mdsmap.get_state(p->first) != MDSMap::STATE_DNE &&
	mdsmap.get_state(p->first) != MDSMap::STATE_STOPPED &&
	mdsmap.get_state(p->first) != MDSMap::STATE_FAILED)
      last_beacon[p->second.addr] = g_clock.now();
  for (map<entity_addr_t,MDSMap::standby_t>::iterator p = mdsmap.standby.begin();
       p != mdsmap.standby.end();
       ++p )
    if (last_beacon.count(p->first) == 0)
      last_beacon[p->first] = g_clock.now();


  if (mon->osdmon()->paxos->is_writeable()) {
    map<entity_addr_t, utime_t>::iterator p = last_beacon.begin();
    while (p != last_beacon.end()) {
      entity_addr_t addr = p->first;
      utime_t since = p->second;
      p++;
      
      if (last_beacon[addr] >= cutoff)
	continue;
      
      int mds = pending_mdsmap.get_addr_rank(addr);
      
      if ((mds < 0 || pending_mdsmap.standby_for.count(mds) == 0) &&
	  pending_mdsmap.standby_any.empty()) {
	// laggy!
	dout(10) << "no beacon from mds" << mds << " " << addr << " since " << since
		 << ", marking laggy" << dendl;
	pending_mdsmap.laggy.insert(addr);
	do_propose = true;
	continue;
      }
      
      if (mds >= 0) {
	// failure!
	int curstate = pending_mdsmap.get_state(mds);
	int newstate = curstate;
	switch (curstate) {
	case MDSMap::STATE_CREATING:
	case MDSMap::STATE_DNE:
	  newstate = MDSMap::STATE_DNE;	// didn't finish creating
	  last_beacon.erase(addr);
	  break;
	  
	case MDSMap::STATE_STARTING:
	  newstate = MDSMap::STATE_STOPPED;
	  break;
	  
	case MDSMap::STATE_STOPPED:
	break;
	
	case MDSMap::STATE_REPLAY:
	case MDSMap::STATE_RESOLVE:
	case MDSMap::STATE_RECONNECT:
	case MDSMap::STATE_REJOIN:
	case MDSMap::STATE_ACTIVE:
	case MDSMap::STATE_STOPPING:
	case MDSMap::STATE_FAILED:
	  newstate = MDSMap::STATE_FAILED;
	  pending_mdsmap.last_failure = pending_mdsmap.epoch;
	  break;
	  
	default:
	  assert(0);
	}
	
	dout(10) << "no beacon from mds" << mds << " " << addr << " since " << since
		 << ", marking " << pending_mdsmap.get_state_name(newstate)
		 << dendl;

	// blacklist
	utime_t until = now;
	until += g_conf.mds_blacklist_interval;
	mon->osdmon()->blacklist(addr, until);
	mon->osdmon()->propose_pending();

	// update map
	pending_mdsmap.mds_state[mds] = newstate;
	pending_mdsmap.mds_state_seq.erase(mds);
	pending_mdsmap.laggy.erase(addr);
      } 
      else if (pending_mdsmap.is_standby(addr)) {
	dout(10) << "no beacon from standby " << addr << " since " << last_beacon[addr]
		 << ", removing from standby list"
		 << dendl;
	if (pending_mdsmap.standby[addr].mds >= 0)
	  pending_mdsmap.standby_for[pending_mdsmap.standby[addr].mds].erase(addr);
	else
	  pending_mdsmap.standby_any.erase(addr);
	pending_mdsmap.standby.erase(addr);
	pending_mdsmap.laggy.erase(addr);
      } 
      else {
	dout(0) << "BUG: removing stray " << addr << " from last_beacon map" << dendl;
      }
      
      last_beacon.erase(addr);
      do_propose = true;
    }
  }

  if (do_propose)
    propose_pending();
}


void MDSMonitor::do_stop()
{
  // hrm...
  if (!mon->is_leader() ||
      !paxos->is_active()) {
    dout(-10) << "do_stop can't stop right now, mdsmap not writeable" << dendl;
    return;
  }

  dout(7) << "do_stop stopping active mds nodes" << dendl;
  print_map(mdsmap);

  for (map<int32_t,int32_t>::iterator p = mdsmap.mds_state.begin();
       p != mdsmap.mds_state.end();
       ++p) {
    switch (p->second) {
    case MDSMap::STATE_ACTIVE:
    case MDSMap::STATE_STOPPING:
      pending_mdsmap.mds_state[p->first] = MDSMap::STATE_STOPPING;
      break;
    case MDSMap::STATE_CREATING:
      pending_mdsmap.mds_state[p->first] = MDSMap::STATE_DNE;
      last_beacon.erase(pending_mdsmap.mds_inst[p->first].addr);
      break;
    case MDSMap::STATE_STARTING:
      pending_mdsmap.mds_state[p->first] = MDSMap::STATE_STOPPED;
      break;
    case MDSMap::STATE_REPLAY:
    case MDSMap::STATE_RESOLVE:
    case MDSMap::STATE_RECONNECT:
    case MDSMap::STATE_REJOIN:
      // BUG: hrm, if this is the case, the STOPPING gusy won't be able to stop, will they?
      pending_mdsmap.mds_state[p->first] = MDSMap::STATE_FAILED;
      pending_mdsmap.last_failure = pending_mdsmap.epoch;
      break;
    }
  }
  // hose standby list
  pending_mdsmap.standby.clear();
  pending_mdsmap.standby_for.clear();
  pending_mdsmap.standby_any.clear();

  propose_pending();
}
