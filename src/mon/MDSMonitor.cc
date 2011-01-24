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
#include "messages/MMDSBeacon.h"
#include "messages/MMDSLoadTargets.h"
#include "messages/MMonCommand.h"

#include "messages/MGenericMessage.h"


#include "common/Timer.h"

#include <sstream>

#include "config.h"

#define DOUT_SUBSYS mon
#undef dout_prefix
#define dout_prefix _prefix(mon, mdsmap)
static ostream& _prefix(Monitor *mon, MDSMap& mdsmap) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
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

void MDSMonitor::create_new_fs(MDSMap &m, int metadata_pool, int data_pool)
{
  m.max_mds = g_conf.max_mds;
  m.created = g_clock.now();
  m.data_pg_pools.push_back(data_pool);
  m.metadata_pg_pool = metadata_pool;
  m.cas_pg_pool = CEPH_CASDATA_RULE;
  m.compat = mdsmap_compat;
  print_map(m);
}


// service methods
void MDSMonitor::create_initial(bufferlist& bl)
{
  dout(10) << "create_initial" << dendl;
  create_new_fs(pending_mdsmap, CEPH_METADATA_RULE, CEPH_DATA_RULE);
}


bool MDSMonitor::update_from_paxos()
{
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

  check_subs();

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

  pending_mdsmap.modified = g_clock.now();

  //print_map(pending_mdsmap);

  // apply to paxos
  assert(paxos->get_version() + 1 == pending_mdsmap.epoch);
  pending_mdsmap.encode(bl);
}


bool MDSMonitor::preprocess_query(PaxosServiceMessage *m)
{
  dout(10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;

  switch (m->get_type()) {
    
  case MSG_MDS_BEACON:
    return preprocess_beacon((MMDSBeacon*)m);
    
  case MSG_MON_COMMAND:
    return preprocess_command((MMonCommand*)m);

  case MSG_MDS_OFFLOAD_TARGETS:
    return preprocess_offload_targets((MMDSLoadTargets*)m);

  default:
    assert(0);
    m->put();
    return true;
  }
}

void MDSMonitor::_note_beacon(MMDSBeacon *m)
{
  uint64_t gid = m->get_global_id();
  version_t seq = m->get_seq();

  dout(15) << "_note_beacon " << *m << " noting time" << dendl;
  last_beacon[gid].stamp = g_clock.now();  
  last_beacon[gid].seq = seq;
}

bool MDSMonitor::preprocess_beacon(MMDSBeacon *m)
{
  entity_addr_t addr = m->get_orig_source_inst().addr;
  int state = m->get_state();
  uint64_t gid = m->get_global_id();
  version_t seq = m->get_seq();
  MDSMap::mds_info_t info;

  // check privileges, ignore if fails
  MonSession *session = m->get_session();
  if (!session)
    goto out;
  if (!session->caps.check_privileges(PAXOS_MDSMAP, MON_CAP_X)) {
    dout(0) << "preprocess_beason got MMDSBeacon from entity with insufficient privileges "
	    << session->caps << dendl;
    goto out;
  }

  if (ceph_fsid_compare(&m->get_fsid(), &mon->monmap->fsid)) {
    dout(0) << "preprocess_beacon on fsid " << m->get_fsid() << " != " << mon->monmap->fsid << dendl;
    goto out;
  }

  dout(12) << "preprocess_beacon " << *m
	   << " from " << m->get_orig_source_inst()
	   << " " << m->get_compat()
	   << dendl;

  // check compat
  if (!m->get_compat().writeable(mdsmap.compat)) {
    dout(1) << " mds " << m->get_source_inst() << " can't write to mdsmap " << mdsmap.compat << dendl;
    goto out;
  }

  // fw to leader?
  if (!mon->is_leader())
    return false;

  // booted, but not in map?
  if (pending_mdsmap.is_dne_gid(gid)) {
    if (state != MDSMap::STATE_BOOT) {
      dout(7) << "mds_beacon " << *m << " is not in mdsmap" << dendl;
      mon->send_reply(m, new MMDSMap(mon->monmap->fsid, &mdsmap));
      goto out;
    } else {
      return false;  // not booted yet.
    }
  }
  info = pending_mdsmap.get_info_gid(gid);

  // old seq?
  if (info.state_seq > seq) {
    dout(7) << "mds_beacon " << *m << " has old seq, ignoring" << dendl;
    goto out;
  }

  if (mdsmap.get_epoch() != m->get_last_epoch_seen()) {
    dout(10) << "mds_beacon " << *m
	     << " ignoring requested state, because mds hasn't seen latest map" << dendl;
    goto ignore;
  }

  if (info.laggy()) {
    _note_beacon(m);
    return false;  // no longer laggy, need to update map.
  }
  if (state == MDSMap::STATE_BOOT) {
    // ignore, already booted.
    goto out;
  }
  // is there a state change here?
  if (info.state != state) {    
    // legal state change?
    if ((info.state == MDSMap::STATE_STANDBY ||
	 info.state == MDSMap::STATE_STANDBY_REPLAY ||
	 info.state == MDSMap::STATE_ONESHOT_REPLAY) && state > 0) {
      dout(10) << "mds_beacon mds can't activate itself (" << ceph_mds_state_name(info.state)
	       << " -> " << ceph_mds_state_name(state) << ")" << dendl;
      goto ignore;
    }
    
    if (info.state == MDSMap::STATE_STANDBY &&
	(state == MDSMap::STATE_STANDBY_REPLAY ||
	    state == MDSMap::STATE_ONESHOT_REPLAY) &&
	(pending_mdsmap.is_degraded() ||
	 ((m->get_standby_for_rank() >= 0) &&
	     pending_mdsmap.get_state(m->get_standby_for_rank()) < MDSMap::STATE_ACTIVE))) {
      dout(10) << "mds_beacon can't standby-replay mds" << m->get_standby_for_rank() << " at this time (cluster degraded, or mds not active)" << dendl;
      dout(10) << "pending_mdsmap.is_degraded()==" << pending_mdsmap.is_degraded()
          << " rank state: " << ceph_mds_state_name(pending_mdsmap.get_state(m->get_standby_for_rank())) << dendl;
      goto ignore;
    }
    _note_beacon(m);
    return false;  // need to update map
  }

 ignore:
  // note time and reply
  _note_beacon(m);
  mon->send_reply(m,
		  new MMDSBeacon(mon->monmap->fsid, m->get_global_id(), m->get_name(),
				 mdsmap.get_epoch(), state, seq));
  
  // done
 out:
  m->put();
  return true;
}

bool MDSMonitor::preprocess_offload_targets(MMDSLoadTargets* m)
{
  dout(10) << "preprocess_offload_targets " << *m << " from " << m->get_orig_source() << dendl;
  uint64_t gid;
  
  // check privileges, ignore message if fails
  MonSession *session = m->get_session();
  if (!session)
    goto done;
  if (!session->caps.check_privileges(PAXOS_MDSMAP, MON_CAP_X)) {
    dout(0) << "preprocess_offload_targets got MMDSLoadTargets from entity with insufficient caps "
	    << session->caps << dendl;
    goto done;
  }

  gid = m->global_id;
  if (mdsmap.mds_info.count(gid) &&
      m->targets == mdsmap.mds_info[gid].export_targets)
    goto done;

  return false;

 done:
  m->put();
  return true;
}


bool MDSMonitor::prepare_update(PaxosServiceMessage *m)
{
  dout(7) << "prepare_update " << *m << dendl;

  switch (m->get_type()) {
    
  case MSG_MDS_BEACON:
    return prepare_beacon((MMDSBeacon*)m);

  case MSG_MON_COMMAND:
    return prepare_command((MMonCommand*)m);

  case MSG_MDS_OFFLOAD_TARGETS:
    return prepare_offload_targets((MMDSLoadTargets*)m);
  
  default:
    assert(0);
    m->put();
  }

  return true;
}



bool MDSMonitor::prepare_beacon(MMDSBeacon *m)
{
  // -- this is an update --
  dout(12) << "prepare_beacon " << *m << " from " << m->get_orig_source_inst() << dendl;
  entity_addr_t addr = m->get_orig_source_inst().addr;
  uint64_t gid = m->get_global_id();
  int state = m->get_state();
  version_t seq = m->get_seq();

  // boot?
  if (state == MDSMap::STATE_BOOT) {
    // add
    MDSMap::mds_info_t& info = pending_mdsmap.mds_info[gid];
    info.global_id = gid;
    info.name = m->get_name();
    info.rank = -1;
    info.addr = addr;
    info.state = MDSMap::STATE_STANDBY;
    info.state_seq = seq;
    info.standby_for_rank = m->get_standby_for_rank();
    info.standby_for_name = m->get_standby_for_name();


    if (!info.standby_for_name.empty()) {
      if (mdsmap.find_by_name(info.standby_for_name))
        info.standby_for_rank =
            mdsmap.find_by_name(info.standby_for_name)->rank;
    }
    if (info.standby_for_rank >= 0 && !mdsmap.is_dne(info.standby_for_rank)) {
      info.state = MDSMap::STATE_STANDBY_REPLAY;
    }

    // initialize the beacon timer
    last_beacon[gid].stamp = g_clock.now();
    last_beacon[gid].seq = seq;

    // new incompat?
    if (!pending_mdsmap.compat.writeable(m->get_compat())) {
      dout(10) << " mdsmap " << pending_mdsmap.compat << " can't write to new mds' " << m->get_compat()
	       << ", updating mdsmap and killing old mds's"
	       << dendl;
      pending_mdsmap.compat = m->get_compat();
    }

  } else {
    // state change
    MDSMap::mds_info_t& info = pending_mdsmap.get_info_gid(gid);

    if (info.laggy()) {
      dout(10) << "prepare_beacon clearing laggy flag on " << addr << dendl;
      info.clear_laggy();
    }
  
    dout(10) << "prepare_beacon mds" << info.rank
	     << " " << ceph_mds_state_name(info.state)
	     << " -> " << ceph_mds_state_name(state)
	     << "  standby_for_rank=" << m->get_standby_for_rank()
	     << dendl;
    if (state == MDSMap::STATE_STOPPED) {
      pending_mdsmap.up.erase(info.rank);
      pending_mdsmap.in.erase(info.rank);
      pending_mdsmap.stopped.insert(info.rank);
      pending_mdsmap.mds_info.erase(gid);  // last! info is a ref into this map
      last_beacon.erase(gid);
    } else if (state == MDSMap::STATE_STANDBY_REPLAY) {
      if (m->get_standby_for_rank() == MDSMap::MDS_STANDBY_NAME) {
        /* convert name to rank. If we don't have it, do nothing. The
	 mds will stay in standby and keep requesting the state change */
        dout(20) << "looking for mds " << m->get_standby_for_name()
                  << " to STANDBY_REPLAY for" << dendl;
        const MDSMap::mds_info_t *found_mds = NULL;
        if ((found_mds = mdsmap.find_by_name(m->get_standby_for_name())) &&
            (found_mds->rank >= 0)) {
          info.standby_for_rank = found_mds->rank;
          dout(10) <<" found mds " << m->get_standby_for_name()
                       << "; it has rank " << info.standby_for_rank << dendl;
          info.state = MDSMap::STATE_STANDBY_REPLAY;
          info.state_seq = seq;
        } else {
          m->put();
          return false;
        }
      } else if (m->get_standby_for_rank() >= 0 &&
                 !mdsmap.is_dne(m->get_standby_for_rank())) {
        /* switch to standby-replay for this MDS*/
        info.state = MDSMap::STATE_STANDBY_REPLAY;
        info.state_seq = seq;
        info.standby_for_rank = m->get_standby_for_rank();
      } else { //it's a standby for anybody, and is already in the list
        assert(pending_mdsmap.get_mds_info().count(info.global_id));
        m->put();
        return false;
      }
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

bool MDSMonitor::prepare_offload_targets(MMDSLoadTargets *m)
{
  uint64_t gid = m->global_id;
  if (pending_mdsmap.mds_info.count(gid)) {
    dout(10) << "prepare_offload_targets " << gid << " " << m->targets << dendl;
    pending_mdsmap.mds_info[gid].export_targets = m->targets;
  } else {
    dout(10) << "prepare_offload_targets " << gid << " not in map" << dendl;
  }
  m->put();
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
  mon->clog.info() << m->get_orig_source_inst() << " "
	  << ceph_mds_state_name(m->get_state()) << "\n";

  if (m->get_state() == MDSMap::STATE_STOPPED) {
    // send the map manually (they're out of the map, so they won't get it automatic)
    mon->send_reply(m, new MMDSMap(mon->monmap->fsid, &mdsmap));
  }

  m->put();
}


void MDSMonitor::committed()
{
  tick();
}

enum health_status_t MDSMonitor::get_health(ostream &oss) const
{
  return mdsmap.get_health(oss);
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
      MDSMap *p = &mdsmap;
      if (m->cmd.size() > 2) {
	epoch_t e = atoi(m->cmd[2].c_str());
	bufferlist b;
	mon->store->get_bl_sn(b,"mdsmap",e);
	if (!b.length()) {
	  p = 0;
	  r = -ENOENT;
	} else {
	  p = new MDSMap;
	  p->decode(b);
	}
      }
      if (p) {
	stringstream ds;
	p->print(ds);
	rdata.append(ds);
	ss << "dumped mdsmap epoch " << p->get_epoch();
	if (p != &mdsmap)
	  delete p;
	r = 0;
      }
    }
    else if (m->cmd[1] == "getmap") {
      if (m->cmd.size() > 2) {
	epoch_t e = atoi(m->cmd[2].c_str());
	bufferlist b;
	mon->store->get_bl_sn(b,"mdsmap",e);
	if (!b.length()) {
	  r = -ENOENT;
	} else {
	  MDSMap m;
	  m.decode(b);
	  m.encode(rdata);
	  ss << "got mdsmap epoch " << m.get_epoch();
	}
      } else {
	mdsmap.encode(rdata);
	ss << "got mdsmap epoch " << mdsmap.get_epoch();
      }
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
	if (!errno && who >= 0) {
	  if (mdsmap.is_up(who)) {
	    mon->inject_args(mdsmap.get_inst(who), m->cmd[3]);
	    r = 0;
	    ss << "ok";
	  } else {
	    ss << "mds" << who << " not up";
	    r = -ENOENT;
	  }
	} else
	  ss << "specify mds number or *";
      }
    }
    else if (m->cmd[1] == "tell") {
      m->cmd.erase(m->cmd.begin()); //take out first two args; don't need them
      m->cmd.erase(m->cmd.begin());
      if (m->cmd[0] == "*") {
	m->cmd.erase(m->cmd.begin()); //and now we're done with the target num
	for (unsigned i = 0; i < mdsmap.get_max_mds(); ++i) {
	  if (mdsmap.is_active(i))
	    mon->send_command(mdsmap.get_inst(i), m->cmd, paxos->get_version());
	}
      } else {
	errno = 0;
	int who = strtol(m->cmd[0].c_str(), 0, 10);
	m->cmd.erase(m->cmd.begin()); //done with target num now
	if (!errno && who >= 0) {
	  if (mdsmap.is_up(who)) {
	    mon->send_command(mdsmap.get_inst(who), m->cmd, paxos->get_version());
	    r = 0;
	    ss << "ok";
	  } else {
	    ss << "mds" << who << " no up";
	    r = -ENOENT;
	  }
	} else ss << "specify mds number or *";
      }
    }
    else if (m->cmd[1] == "compat") {
      if (m->cmd.size() >= 3) {
	if (m->cmd[2] == "show") {
	  ss << mdsmap.compat;
	  r = 0;
	} else if (m->cmd[2] == "help") {
	  ss << "mds compat <rm_compat|rm_ro_compat|rm_incompat> <id>";
	  r = 0;
	}
      } else {
	ss << "mds compat <rm_compat|rm_ro_compat|rm_incompat> <id>";
	r = 0;
      }
    }
  }

  if (r != -1) {
    string rs;
    getline(ss, rs);
    mon->reply_command(m, r, rs, rdata, paxos->get_version());
    return true;
  } else
    return false;
}

int MDSMonitor::fail_mds(std::ostream &ss, const std::string &arg)
{
  std::string err;
  int w = strict_strtol(arg.c_str(), 10, &err);
  if (!err.empty()) {
    // Try to interpret the arg as an MDS name
    const MDSMap::mds_info_t *mds_info = mdsmap.find_by_name(arg);
    if (!mds_info) {
      ss << "Can't find any MDS named '" << arg << "'";
      return -ENOENT;
    }
    w = mds_info->rank;
  }

  if (pending_mdsmap.up.count(w) &&
      pending_mdsmap.mds_info.count(pending_mdsmap.up[w])) {
    utime_t until = g_clock.now();
    until += g_conf.mds_blacklist_interval;
    MDSMap::mds_info_t& info = pending_mdsmap.mds_info[pending_mdsmap.up[w]];
    pending_mdsmap.last_failure_osd_epoch = mon->osdmon()->blacklist(info.addr, until);
    mon->osdmon()->propose_pending();
  }
  pending_mdsmap.failed.insert(w);
  pending_mdsmap.up.erase(w);
  ss << "failed mds" << w;
  return 0;
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
	uint64_t gid = pending_mdsmap.up[who];
	ss << "telling mds" << who << " " << pending_mdsmap.mds_info[gid].addr << " to stop";
	pending_mdsmap.mds_info[gid].state = MDSMap::STATE_STOPPING;
      } else {
	r = -EEXIST;
	ss << "mds" << who << " not active (" 
	   << ceph_mds_state_name(mdsmap.get_state(who)) << ")";
      }
    }
    else if (m->cmd[1] == "set_max_mds" && m->cmd.size() > 2) {
      pending_mdsmap.max_mds = atoi(m->cmd[2].c_str());
      r = 0;
      ss << "max_mds = " << pending_mdsmap.max_mds;
    }
    else if (m->cmd[1] == "setmap" && m->cmd.size() == 3) {
      MDSMap map;
      map.decode(m->get_data());
      epoch_t e = atoi(m->cmd[2].c_str());
      //if (ceph_fsid_compare(&map.get_fsid(), &mon->monmap->fsid) == 0) {
      if (pending_mdsmap.epoch == e) {
	map.epoch = pending_mdsmap.epoch;  // make sure epoch is correct
	pending_mdsmap = map;
	string rs = "set mds map";
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	return true;
      } else
	ss << "next mdsmap epoch " << pending_mdsmap.epoch << " != " << e;
	//} else
	//ss << "mdsmap fsid " << map.fsid << " does not match monitor fsid " << mon->monmap->fsid;
    }
    else if (m->cmd[1] == "set_state" && m->cmd.size() == 4) {
      uint64_t gid = atoi(m->cmd[2].c_str());
      int state = atoi(m->cmd[3].c_str());
      if (!pending_mdsmap.is_dne_gid(gid)) {
	MDSMap::mds_info_t& info = pending_mdsmap.get_info_gid(gid);
	info.state = state;
	stringstream ss;
	ss << "set mds gid " << gid << " to state " << state << " " << ceph_mds_state_name(state);
	string rs;
	getline(ss, rs);
	paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
	return true;
      }
    }
    else if (m->cmd[1] == "fail" && m->cmd.size() == 3) {
      r = fail_mds(ss, m->cmd[2]);
    }
    else if (m->cmd[1] == "rm" && m->cmd.size() == 3) {
      uint64_t gid = atoll(m->cmd[2].c_str());
      pending_mdsmap.mds_info.erase(gid);
      stringstream ss;
      ss << "removed mds gid " << gid;
      string rs;
      getline(ss, rs);
      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
      return true;
    }
    else if (m->cmd[1] == "rmfailed" && m->cmd.size() == 3) {
      int w = atoi(m->cmd[2].c_str());
      pending_mdsmap.failed.erase(w);
      stringstream ss;
      ss << "removed failed mds" << w;
      string rs;
      getline(ss, rs);
      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
      return true;
    }
    else if (m->cmd[1] == "compat" && m->cmd.size() == 4) {
      uint64_t f = atoll(m->cmd[3].c_str());
      if (m->cmd[2] == "rm_compat") {
	if (pending_mdsmap.compat.compat.contains(f)) {
	  ss << "removing compat feature " << f;
	  pending_mdsmap.compat.compat.remove(f);
	  r = 0;
	} else {
	  ss << "compat feature " << f << " not present in " << pending_mdsmap.compat;
	  r = -ENOENT;
	}
      } else if (m->cmd[2] == "rm_incompat") {
	if (pending_mdsmap.compat.incompat.contains(f)) {
	  ss << "removing incompat feature " << f;
	  pending_mdsmap.compat.incompat.remove(f);
	  r = 0;
	} else {
	  ss << "incompat feature " << f << " not present in " << pending_mdsmap.compat;
	  r = -ENOENT;
	}
      }
    } else if (m->cmd[1] == "newfs" && m->cmd.size() == 4) {
      MDSMap newmap;
      int metadata = atoi(m->cmd[2].c_str());
      int data = atoi(m->cmd[3].c_str());
      pending_mdsmap = newmap;
      pending_mdsmap.epoch = mdsmap.epoch + 1;
      create_new_fs(pending_mdsmap, metadata, data);
      ss << "new fs with metadata pool " << metadata << " and data pool " << data;
      string rs;
      getline(ss, rs);
      paxos->wait_for_commit(new Monitor::C_Command(mon, m, 0, rs, paxos->get_version()));
      return true;
    }    
  }
  if (r == -EINVAL) 
    ss << "unrecognized command";
  string rs;
  getline(ss, rs);

  if (r >= 0) {
    // success.. delay reply
    paxos->wait_for_commit(new Monitor::C_Command(mon, m, r, rs, paxos->get_version()));
    return true;
  } else {
    // reply immediately
    mon->reply_command(m, r, rs, rdata, paxos->get_version());
    return false;
  }
}


void MDSMonitor::check_subs()
{
  string type = "mdsmap";
  xlist<Subscription*>::iterator p = mon->session_map.subs[type].begin();
  while (!p.end()) {
    Subscription *sub = *p;
    ++p;
    check_sub(sub);
  }
}

void MDSMonitor::check_sub(Subscription *sub)
{
  if (sub->next <= mdsmap.get_epoch()) {
    mon->messenger->send_message(new MMDSMap(mon->monmap->fsid, &mdsmap),
				 sub->session->inst);
    if (sub->onetime)
      mon->session_map.remove_sub(sub);
    else
      sub->next = mdsmap.get_epoch() + 1;
  }
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
    uint64_t newgid = pending_mdsmap.find_unused_for(mds, name);
    if (!newgid)
      break;

    MDSMap::mds_info_t& info = pending_mdsmap.mds_info[newgid];
    dout(1) << "adding standby " << info.addr << " as mds" << mds << dendl;
    
    info.rank = mds;
    if (pending_mdsmap.stopped.count(mds)) {
      info.state = MDSMap::STATE_STARTING;
      pending_mdsmap.stopped.erase(mds);
    } else
      info.state = MDSMap::STATE_CREATING;
    info.inc = ++pending_mdsmap.inc[mds];
    pending_mdsmap.in.insert(mds);
    pending_mdsmap.up[mds] = newgid;
    do_propose = true;
  }

  // check beacon timestamps
  utime_t now = g_clock.now();
  utime_t cutoff = now;
  cutoff -= g_conf.mds_beacon_grace;

  // make sure last_beacon is fully populated
  for (map<uint64_t,MDSMap::mds_info_t>::iterator p = pending_mdsmap.mds_info.begin();
       p != pending_mdsmap.mds_info.end();
       ++p) {
    if (last_beacon.count(p->first) == 0) {
      const MDSMap::mds_info_t& info = p->second;
      dout(10) << " adding " << p->second.addr << " mds" << info.rank << "." << info.inc
	       << " " << ceph_mds_state_name(info.state)
	       << " to last_beacon" << dendl;
      last_beacon[p->first].stamp = g_clock.now();
      last_beacon[p->first].seq = 0;
    }
  }

  if (mon->osdmon()->paxos->is_writeable()) {

    bool propose_osdmap = false;

    map<uint64_t, beacon_info_t>::iterator p = last_beacon.begin();
    while (p != last_beacon.end()) {
      uint64_t gid = p->first;
      utime_t since = p->second.stamp;
      uint64_t seq = p->second.seq;
      p++;
      
      if (pending_mdsmap.mds_info.count(gid) == 0) {
	// clean it out
	last_beacon.erase(gid);
	continue;
      }

      if (since >= cutoff && pending_mdsmap.mds_info[gid].standby_for_rank != MDSMap::MDS_STANDBY_ANY)
	continue;


      MDSMap::mds_info_t& info = pending_mdsmap.mds_info[gid];

      if (since >= cutoff && info.standby_for_rank == MDSMap::MDS_STANDBY_ANY) {
        /* this mds is not laggy, but has no rank assigned.
         * See if we can find it somebody to shadow
         */
        int gid = 0;
        for (map<uint64_t,MDSMap::mds_info_t>::iterator i = mdsmap.mds_info.begin();
            i != mdsmap.mds_info.end();
            ++i) {
          if (i->second.rank >= 0 &&
              (gid = pending_mdsmap.find_standby_for(i->first, i->second.name)))
            if (pending_mdsmap.get_info_gid(gid).state == MDSMap::STATE_STANDBY_REPLAY)
              continue; // this MDS already has a standby
          // hey, we found an MDS without a standby. Pair them!
          info.standby_for_rank = i->second.rank;
          dout(10) << "setting to shadow mds rank " << info.standby_for_rank << dendl;
          info.state = MDSMap::STATE_STANDBY_REPLAY;
          do_propose = true;
          break;
        }
        continue;
      }
      dout(10) << "no beacon from " << info.addr << " mds" << info.rank << "." << info.inc
	       << " " << ceph_mds_state_name(info.state)
	       << " since " << since << dendl;
      
      // are we in?
      // and is there a non-laggy standby that can take over for us?
      uint64_t sgid;
      if (info.rank >= 0 &&
	  info.state != CEPH_MDS_STATE_STANDBY &&
	  (sgid = mdsmap.find_replacement_for(info.rank, info.name)) != 0) {
	MDSMap::mds_info_t& si = pending_mdsmap.mds_info[sgid];
	dout(10) << " replacing " << info.addr << " mds" << info.rank << "." << info.inc
		 << " " << ceph_mds_state_name(info.state)
		 << " with " << sgid << "/" << si.name << " " << si.addr << dendl;
	switch (info.state) {
	case MDSMap::STATE_CREATING:
	case MDSMap::STATE_STARTING:
	  si.state = info.state;
	  break;
        case MDSMap::STATE_STANDBY_REPLAY:
	case MDSMap::STATE_REPLAY:
	case MDSMap::STATE_RESOLVE:
	case MDSMap::STATE_RECONNECT:
	case MDSMap::STATE_REJOIN:
	case MDSMap::STATE_CLIENTREPLAY:
	case MDSMap::STATE_ACTIVE:
	case MDSMap::STATE_STOPPING:
	  si.state = MDSMap::STATE_REPLAY;
	  break;
	default:
	  assert(0);
	}

	info.state_seq = seq;
	si.rank = info.rank;
	si.inc = ++pending_mdsmap.inc[info.rank];
	pending_mdsmap.up[info.rank] = sgid;
	if (si.state > 0)
	  pending_mdsmap.last_failure = pending_mdsmap.epoch;
	if (si.state > 0 ||
	    si.state == MDSMap::STATE_CREATING ||
	    si.state == MDSMap::STATE_STARTING) {
	  // blacklist laggy mds
	  utime_t until = now;
	  until += g_conf.mds_blacklist_interval;
	  pending_mdsmap.last_failure_osd_epoch = mon->osdmon()->blacklist(info.addr, until);
	  propose_osdmap = true;
	}
	pending_mdsmap.mds_info.erase(gid);
	last_beacon.erase(gid);
	do_propose = true;
      } else if (info.state == MDSMap::STATE_STANDBY_REPLAY) {
	dout(10) << " failing " << info.addr << " mds" << info.rank << "." << info.inc
		 << " " << ceph_mds_state_name(info.state)
		 << dendl;
	pending_mdsmap.mds_info.erase(gid);
	last_beacon.erase(gid);
	do_propose = true;
      } else if (!info.laggy()) {
	if (info.state == MDSMap::STATE_STANDBY) {
	  // remove it
	  dout(10) << " removing " << info.addr << " mds" << info.rank << "." << info.inc
		   << " " << ceph_mds_state_name(info.state)
		   << " (laggy)" << dendl;
	  pending_mdsmap.mds_info.erase(gid);
	} else {
	  dout(10) << " marking " << info.addr << " mds" << info.rank << "." << info.inc
		   << " " << ceph_mds_state_name(info.state)
		   << " laggy" << dendl;
	  info.laggy_since = now;
	}
	last_beacon.erase(gid);
	do_propose = true;
      }
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
      uint64_t sgid;
      string name;  // FIXME
      sgid = pending_mdsmap.find_replacement_for(f, name);
      if (sgid) {
	MDSMap::mds_info_t& si = pending_mdsmap.mds_info[sgid];
	dout(0) << " taking over failed mds" << f << " with " << sgid << "/" << si.name << " " << si.addr << dendl;
	si.state = MDSMap::STATE_REPLAY;
	si.rank = f;
	si.inc = ++pending_mdsmap.inc[f];
	pending_mdsmap.in.insert(f);
	pending_mdsmap.up[f] = sgid;
	do_propose = true;
      }
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
    dout(0) << "do_stop can't stop right now, mdsmap not writeable" << dendl;
    return;
  }

  dout(7) << "do_stop stopping active mds nodes" << dendl;
  print_map(mdsmap);

  bool propose_osdmap = false;

  map<uint64_t,MDSMap::mds_info_t>::iterator p = pending_mdsmap.mds_info.begin();
  while (p != pending_mdsmap.mds_info.end()) {
    uint64_t gid = p->first;
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
      pending_mdsmap.mds_info.erase(gid);
      pending_mdsmap.in.erase(info.rank);
      break;
    case MDSMap::STATE_REPLAY:
    case MDSMap::STATE_RESOLVE:
    case MDSMap::STATE_RECONNECT:
    case MDSMap::STATE_REJOIN:
    case MDSMap::STATE_CLIENTREPLAY:
      // BUG: hrm, if this is the case, the STOPPING guys won't be able to stop, will they?
      {
	utime_t until = g_clock.now();
	until += g_conf.mds_blacklist_interval;
	pending_mdsmap.last_failure_osd_epoch = mon->osdmon()->blacklist(info.addr, until);
	propose_osdmap = true;
      }
      pending_mdsmap.failed.insert(info.rank);
      pending_mdsmap.up.erase(info.rank);
      pending_mdsmap.mds_info.erase(gid);
      pending_mdsmap.in.erase(info.rank);
      break;
    }
  }

  propose_pending();
  if (propose_osdmap)
    mon->osdmon()->propose_pending();
}
