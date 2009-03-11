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
  dout(7) << "print_map\n";
  m.print(*_dout);
  *_dout << dendl;
}



// service methods

void MDSMonitor::create_initial(bufferlist& bl)
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
  entity_addr_t addr = m->get_orig_source_inst().addr;
  int state = m->get_state();
  version_t seq = m->get_seq();
  MDSMap::mds_info_t info;

  if (ceph_fsid_compare(&m->get_fsid(), &mon->monmap->fsid)) {
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
  if (pending_mdsmap.is_dne(addr)) {
    if (state != MDSMap::STATE_BOOT) {
      dout(7) << "mds_beacon " << *m << " is not in mdsmap" << dendl;
      send_latest(m->get_orig_source_inst());
      goto out;
    } else {
      return false;  // not booted yet.
    }
  }
  info = pending_mdsmap.get_info(addr);

  // old seq?
  if (info.state_seq > seq) {
    dout(7) << "mds_beacon " << *m << " has old seq, ignoring" << dendl;
    goto out;
  }

  // can i handle this query without a map update?

  if (info.laggy()) {
    return false;  // no longer laggy, need to update map.
  }
  else if (state == MDSMap::STATE_BOOT) {
    // ignore, already booted.
    goto out;
  }
  else {
    // is there a state change here?
    if (info.state != state) {
      if (mdsmap.get_epoch() != m->get_last_epoch_seen()) {
	dout(10) << "mds_beacon " << *m
		 << " ignoring requested state, because mds hasn't seen latest map" << dendl;
	goto ignore;
      }
      
      // legal state change?
      if ((info.state == MDSMap::STATE_STANDBY ||
	   info.state == MDSMap::STATE_STANDBY_REPLAY) && state > 0) {
	dout(10) << "mds_beacon mds can't activate itself (" << MDSMap::get_state_name(info.state)
		 << " -> " << MDSMap::get_state_name(state) << ")" << dendl;
	goto ignore;
      }

      if (info.state == MDSMap::STATE_STANDBY &&
	  state == MDSMap::STATE_STANDBY_REPLAY &&
	  (pending_mdsmap.is_degraded() ||
	   pending_mdsmap.get_state(info.rank) < MDSMap::STATE_ACTIVE)) {
	dout(10) << "mds_beacon can't standby-replay mds" << info.rank << " at this time (cluster degraded, or mds not active)" << dendl;
	goto ignore;
      }
      
      return false;  // need to update map
    }
  }

 ignore:
  // note time and reply
  dout(15) << "mds_beacon " << *m << " noting time and replying" << dendl;
  last_beacon[addr] = g_clock.now();  
  mon->messenger->send_message(new MMDSBeacon(mon->monmap->fsid, m->get_name(),
					      mdsmap.get_epoch(), state, seq), 
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
  dout(12) << "prepare_beacon " << *m << " from " << m->get_orig_source_inst() << dendl;
  entity_addr_t addr = m->get_orig_source_inst().addr;
  int state = m->get_state();
  version_t seq = m->get_seq();

  // boot?
  if (state == MDSMap::STATE_BOOT) {
    // add
    MDSMap::mds_info_t& info = pending_mdsmap.mds_info[addr];
    info.name = m->get_name();
    info.rank = -1;
    info.addr = addr;
    info.state = MDSMap::STATE_STANDBY;
    info.standby_for_rank = m->get_standby_for_rank();
    info.standby_for_name = m->get_standby_for_name();

    // initialize the beacon timer
    last_beacon[addr] = g_clock.now();

  } else {
    // state change
    MDSMap::mds_info_t& info = pending_mdsmap.get_info(addr);

    if (info.laggy()) {
      dout(10) << "prepare_beacon clearly laggy flag on " << addr << dendl;
      info.clear_laggy();
    }
  
    dout(10) << "prepare_beacon mds" << info.rank
	     << " " << MDSMap::get_state_name(info.state)
	     << " -> " << MDSMap::get_state_name(state)
	     << dendl;
    if (state == MDSMap::STATE_STOPPED) {
      pending_mdsmap.up.erase(info.rank);
      pending_mdsmap.mds_info.erase(addr);
      pending_mdsmap.stopped.insert(info.rank);
      pending_mdsmap.in.erase(info.rank);
    } else {
      info.state = state;
      info.state_seq = seq;
    }
  }

  dout(7) << "prepare_beacon pending map now:" << dendl;
  print_map(pending_mdsmap);
  
  paxos->wait_for_commit(new C_Updated(this, m));

  return true;
}

bool MDSMonitor::should_propose(double& delay)
{
  delay = 0.0;
  return true;
}

void MDSMonitor::_updated(MMDSBeacon *m)
{
  dout(10) << "_updated " << m->get_orig_source() << " " << *m << dendl;

  if (m->get_state() == MDSMap::STATE_STOPPED) {
    // send the map manually (they're out of the map, so they won't get it automatic)
    send_latest(m->get_orig_source_inst());
  }

  delete m;
}


void MDSMonitor::committed()
{
  // hackish: did all mds's shut down?
  if (mon->is_leader() &&
      g_conf.mon_stop_with_last_mds &&
      mdsmap.get_epoch() > 1 &&
      mdsmap.is_stopped()) 
    mon->messenger->send_message(new MGenericMessage(CEPH_MSG_SHUTDOWN), 
				 mon->monmap->get_inst(mon->whoami));
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
	for (unsigned i=0; i<mdsmap.get_max_mds(); i++)
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
	entity_addr_t a = pending_mdsmap.up[who];
	ss << "telling mds" << who << " " << a << " to stop";
	pending_mdsmap.mds_info[a].state = MDSMap::STATE_STOPPING;
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
  
  for (map<entity_addr_t,MDSMap::mds_info_t>::iterator p = mdsmap.mds_info.begin();
       p != mdsmap.mds_info.end();
       p++)
    send_full(p->second.get_inst());
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

  // expand mds cluster (add new nodes to @in)?
  while (pending_mdsmap.get_num_mds() < pending_mdsmap.get_max_mds() &&
	 !pending_mdsmap.is_degraded()) {
    int mds = 0;
    string name;
    while (pending_mdsmap.is_in(mds))
      mds++;
    entity_addr_t addr;
    if (!pending_mdsmap.find_standby_for(mds, name, addr))
      break;

    dout(1) << "adding standby " << addr << " as mds" << mds << dendl;
    
    MDSMap::mds_info_t& info = pending_mdsmap.mds_info[addr];
    info.rank = mds;
    if (pending_mdsmap.stopped.count(mds))
      info.state = MDSMap::STATE_STARTING;
    else
      info.state = MDSMap::STATE_CREATING;
    info.inc = ++pending_mdsmap.inc[mds];
    pending_mdsmap.in.insert(mds);
    pending_mdsmap.up[mds] = addr;
    do_propose = true;
  }

  // check beacon timestamps
  utime_t now = g_clock.now();
  utime_t cutoff = now;
  cutoff -= g_conf.mds_beacon_grace;

  // make sure last_beacon is fully populated
  for (map<entity_addr_t,MDSMap::mds_info_t>::iterator p = mdsmap.mds_info.begin();
       p != mdsmap.mds_info.end();
       ++p) 
    if (last_beacon.count(p->second.addr) == 0)
      last_beacon[p->second.addr] = g_clock.now();

  if (mon->osdmon()->paxos->is_writeable()) {

    bool propose_osdmap = false;

    map<entity_addr_t, utime_t>::iterator p = last_beacon.begin();
    while (p != last_beacon.end()) {
      entity_addr_t addr = p->first;
      utime_t since = p->second;
      p++;
      
      if (last_beacon[addr] >= cutoff)
	continue;

      MDSMap::mds_info_t& info = pending_mdsmap.mds_info[addr];

      dout(10) << "no beacon from " << addr << " mds" << info.rank << "." << info.inc
	       << " " << MDSMap::get_state_name(info.state)
	       << " since " << since << dendl;
      
      // are we in?
      // and is there a non-laggy standby that can take over for us?
      entity_addr_t sa;
      if (info.rank >= 0 &&
	  info.state > 0 && //|| info.state == MDSMap::STATE_STANDBY_REPLAY) &&
	  pending_mdsmap.find_standby_for(info.rank, info.name, sa)) {
	MDSMap::mds_info_t& si = pending_mdsmap.mds_info[sa];
	dout(10) << " replacing " << addr << " mds" << info.rank << "." << info.inc
		 << " " << MDSMap::get_state_name(info.state)
		 << " with " << si.name << " " << sa << dendl;
	switch (info.state) {
	case MDSMap::STATE_CREATING:
	case MDSMap::STATE_STARTING:
	case MDSMap::STATE_STANDBY_REPLAY:
	  si.state = info.state;
	  break;
	case MDSMap::STATE_REPLAY:
	case MDSMap::STATE_RESOLVE:
	case MDSMap::STATE_RECONNECT:
	case MDSMap::STATE_REJOIN:
	case MDSMap::STATE_ACTIVE:
	case MDSMap::STATE_STOPPING:
	  si.state = MDSMap::STATE_REPLAY;
	  break;
	default:
	  assert(0);
	}
	si.rank = info.rank;
	if (si.state > 0) {
	  si.inc = ++pending_mdsmap.inc[info.rank];
	  pending_mdsmap.up[info.rank] = sa;
	  pending_mdsmap.last_failure = pending_mdsmap.epoch;
	}
	pending_mdsmap.mds_info.erase(addr);

	if (si.state > 0) {
	  // blacklist
	  utime_t until = now;
	  until += g_conf.mds_blacklist_interval;
	  mon->osdmon()->blacklist(addr, until);
	  propose_osdmap = true;
	}
	
	do_propose = true;
      } else if (info.state == MDSMap::STATE_STANDBY_REPLAY) {
	dout(10) << " failing " << addr << " mds" << info.rank << "." << info.inc
		 << " " << MDSMap::get_state_name(info.state)
		 << dendl;
	pending_mdsmap.mds_info.erase(addr);
	do_propose = true;
      } else if (!info.laggy()) {
	// just mark laggy
	dout(10) << " marking " << addr << " mds" << info.rank << "." << info.inc
		 << " " << MDSMap::get_state_name(info.state)
		 << " laggy" << dendl;
	info.laggy_since = now;
	do_propose = true;
      }
      
      last_beacon.erase(addr);
    }

    if (propose_osdmap)
      mon->osdmon()->propose_pending();

  }


  // have a standby take over?
  set<int> failed;
  pending_mdsmap.get_failed_mds_set(failed);
  if (!failed.empty()) {
    set<int>::iterator p = failed.begin();
    while (p != failed.end()) {
      int f = *p++;
      entity_addr_t sa;
      string name;  // FIXME
      if (pending_mdsmap.find_standby_for(f, name, sa)) {
	dout(0) << " taking over failed mds" << f << " with " << sa << dendl;
	MDSMap::mds_info_t& si = pending_mdsmap.mds_info[sa];
	si.state = MDSMap::STATE_REPLAY;
	si.rank = f;
	si.inc = ++pending_mdsmap.inc[f];
	pending_mdsmap.in.insert(f);
	pending_mdsmap.up[f] = sa;
	do_propose = true;
      }
    }
  }

  // have a standby replay/shadow an active mds?
  if (false &&
      !pending_mdsmap.is_degraded() &&
      pending_mdsmap.get_num_mds(MDSMap::STATE_STANDBY) >= pending_mdsmap.get_num_mds()) {
    // see which nodes are shadowed
    set<int> shadowed;
    map<int, set<entity_addr_t> > avail;
    for (map<entity_addr_t,MDSMap::mds_info_t>::iterator p = pending_mdsmap.mds_info.begin();
	 p != pending_mdsmap.mds_info.end();
	 p++) {
      if (p->second.state == MDSMap::STATE_STANDBY_REPLAY) 
	shadowed.insert(p->second.rank);
      if (p->second.state == MDSMap::STATE_STANDBY &&
	  !p->second.laggy())
	avail[p->second.rank].insert(p->first);
    }

    // find an mds that needs a standby
    set<int> in;
    pending_mdsmap.get_mds_set(in);
    for (set<int>::iterator p = in.begin(); p != in.end(); p++) {
      if (shadowed.count(*p))
	continue;  // already shadowed.
      if (pending_mdsmap.get_state(*p) < MDSMap::STATE_ACTIVE)
	continue;  // only shadow active mds
      entity_addr_t s;
      if (avail[*p].size()) {
	s = *avail[*p].begin();
	avail[*p].erase(avail[*p].begin());
      } else if (avail[-1].size()) {
	s = *avail[-1].begin();
	avail[-1].erase(avail[-1].begin());
      } else
	continue;
      dout(10) << "mds" << *p << " will be shadowed by " << s << dendl;

      MDSMap::mds_info_t& info = pending_mdsmap.mds_info[s];
      info.rank = *p;
      info.state = MDSMap::STATE_STANDBY_REPLAY;
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

  map<entity_addr_t,MDSMap::mds_info_t>::iterator p = pending_mdsmap.mds_info.begin();
  while (p != pending_mdsmap.mds_info.end()) {
    MDSMap::mds_info_t& info = p->second;
    p++;
    switch (info.state) {
    case MDSMap::STATE_ACTIVE:
    case MDSMap::STATE_STOPPING:
      info.state = MDSMap::STATE_STOPPING;
      break;
    case MDSMap::STATE_STARTING:
      pending_mdsmap.stopped.insert(info.rank);
    case MDSMap::STATE_CREATING:
      pending_mdsmap.up.erase(info.rank);
      pending_mdsmap.mds_info.erase(info.addr);
      pending_mdsmap.in.erase(info.rank);
      break;
    case MDSMap::STATE_REPLAY:
    case MDSMap::STATE_RESOLVE:
    case MDSMap::STATE_RECONNECT:
    case MDSMap::STATE_REJOIN:
      // BUG: hrm, if this is the case, the STOPPING guys won't be able to stop, will they?
      pending_mdsmap.failed.insert(info.rank);
      pending_mdsmap.up.erase(info.rank);
      pending_mdsmap.mds_info.erase(info.addr);
      pending_mdsmap.in.erase(info.rank);
      break;
    }
  }

  propose_pending();
}
