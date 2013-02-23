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
#include "MonitorDBStore.h"
#include "OSDMonitor.h"

#include "common/strtol.h"
#include "common/ceph_argparse.h"

#include "messages/MMDSMap.h"
#include "messages/MMDSBeacon.h"
#include "messages/MMDSLoadTargets.h"
#include "messages/MMonCommand.h"

#include "messages/MGenericMessage.h"

#include "common/perf_counters.h"
#include "common/Timer.h"

#include <sstream>

#include "common/config.h"
#include "include/assert.h"

#include "MonitorDBStore.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, mdsmap)
static ostream& _prefix(std::ostream *_dout, Monitor *mon, MDSMap& mdsmap) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").mds e" << mdsmap.get_epoch() << " ";
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
  m.max_mds = g_conf->max_mds;
  m.created = ceph_clock_now(g_ceph_context);
  m.data_pools.insert(data_pool);
  m.metadata_pool = metadata_pool;
  m.cas_pool = -1;
  m.compat = get_mdsmap_compat_set();

  m.session_timeout = g_conf->mds_session_timeout;
  m.session_autoclose = g_conf->mds_session_autoclose;
  m.max_file_size = g_conf->mds_max_file_size;

  print_map(m);
}


// service methods
void MDSMonitor::create_initial()
{
  dout(10) << "create_initial" << dendl;
  create_new_fs(pending_mdsmap, CEPH_METADATA_RULE, CEPH_DATA_RULE);
}


void MDSMonitor::update_from_paxos()
{
  version_t version = get_version();
  if (version == mdsmap.epoch)
    return;
  assert(version >= mdsmap.epoch);

  dout(10) << __func__ << " version " << version
	   << ", my e " << mdsmap.epoch << dendl;

  // read and decode
  mdsmap_bl.clear();
  get_version(version, mdsmap_bl);
  assert(mdsmap_bl.length() > 0);
  dout(10) << __func__ << " got " << version << dendl;
  mdsmap.decode(mdsmap_bl);

  // new map
  dout(4) << "new map" << dendl;
  print_map(mdsmap, 0);

  check_subs();
  update_logger();
}

void MDSMonitor::create_pending()
{
  pending_mdsmap = mdsmap;
  pending_mdsmap.epoch++;
  dout(10) << "create_pending e" << pending_mdsmap.epoch << dendl;
}

void MDSMonitor::encode_pending(MonitorDBStore::Transaction *t)
{
  dout(10) << "encode_pending e" << pending_mdsmap.epoch << dendl;

  pending_mdsmap.modified = ceph_clock_now(g_ceph_context);

  //print_map(pending_mdsmap);

  // apply to paxos
  assert(get_version() + 1 == pending_mdsmap.epoch);
  bufferlist mdsmap_bl;
  pending_mdsmap.encode(mdsmap_bl, mon->get_quorum_features());

  /* put everything in the transaction */
  put_version(t, pending_mdsmap.epoch, mdsmap_bl);
  put_last_committed(t, pending_mdsmap.epoch);
}

void MDSMonitor::update_logger()
{
  dout(10) << "update_logger" << dendl;

  mon->cluster_logger->set(l_cluster_num_mds_up, mdsmap.get_num_up_mds());
  mon->cluster_logger->set(l_cluster_num_mds_in, mdsmap.get_num_in_mds());
  mon->cluster_logger->set(l_cluster_num_mds_failed, mdsmap.get_num_failed_mds());
  mon->cluster_logger->set(l_cluster_mds_epoch, mdsmap.get_epoch());
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
  last_beacon[gid].stamp = ceph_clock_now(g_ceph_context);  
  last_beacon[gid].seq = seq;
}

bool MDSMonitor::preprocess_beacon(MMDSBeacon *m)
{
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

  if (m->get_fsid() != mon->monmap->fsid) {
    dout(0) << "preprocess_beacon on fsid " << m->get_fsid() << " != " << mon->monmap->fsid << dendl;
    goto out;
  }

  dout(12) << "preprocess_beacon " << *m
	   << " from " << m->get_orig_source_inst()
	   << " " << m->get_compat()
	   << dendl;

  // make sure the address has a port
  if (m->get_orig_source_addr().get_port() == 0) {
    dout(1) << " ignoring boot message without a port" << dendl;
    goto out;
  }

  // check compat
  if (!m->get_compat().writeable(mdsmap.compat)) {
    dout(1) << " mds " << m->get_source_inst() << " can't write to mdsmap " << mdsmap.compat << dendl;
    goto out;
  }

  // fw to leader?
  if (!mon->is_leader())
    return false;

  if (pending_mdsmap.test_flag(CEPH_MDSMAP_DOWN)) {
    dout(7) << " mdsmap DOWN flag set, ignoring mds " << m->get_source_inst() << " beacon" << dendl;
    goto out;
  }

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
      dout(10) << "mds_beacon can't standby-replay mds." << m->get_standby_for_rank() << " at this time (cluster degraded, or mds not active)" << dendl;
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
    // zap previous instance of this name?
    if (g_conf->mds_enforce_unique_name) {
      while (uint64_t existing = pending_mdsmap.find_mds_gid_by_name(m->get_name())) {
	fail_mds_gid(existing);
      }
    }

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
      const MDSMap::mds_info_t *leaderinfo = mdsmap.find_by_name(info.standby_for_name);
      if (leaderinfo && (leaderinfo->rank >= 0)) {
        info.standby_for_rank =
            mdsmap.find_by_name(info.standby_for_name)->rank;
        if (mdsmap.is_followable(info.standby_for_rank)) {
          info.state = MDSMap::STATE_STANDBY_REPLAY;
        }
      }
    }

    // initialize the beacon timer
    last_beacon[gid].stamp = ceph_clock_now(g_ceph_context);
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
  
    dout(10) << "prepare_beacon mds." << info.rank
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
            (found_mds->rank >= 0) &&
	    mdsmap.is_followable(found_mds->rank)) {
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
		 mdsmap.is_followable(m->get_standby_for_rank())) {
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
  
  wait_for_finished_proposal(new C_Updated(this, m));

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

void MDSMonitor::on_active()
{
  tick();
  update_logger();

  if (mon->is_leader())
    mon->clog.info() << "mdsmap " << mdsmap << "\n";
}

void MDSMonitor::get_health(list<pair<health_status_t, string> >& summary,
			    list<pair<health_status_t, string> > *detail) const
{
  mdsmap.get_health(summary, detail);
}

void MDSMonitor::dump_info(Formatter *f)
{
  f->open_object_section("mdsmap");
  mdsmap.dump(f);
  f->close_section();
}

bool MDSMonitor::preprocess_command(MMonCommand *m)
{
  int r = -1;
  bufferlist rdata;
  stringstream ss;

  MonSession *session = m->get_session();
  if (!session ||
      (!session->caps.get_allow_all() &&
       !session->caps.check_privileges(PAXOS_MDSMAP, MON_CAP_R) &&
       !mon->_allowed_command(session, m->cmd))) {
    mon->reply_command(m, -EACCES, "access denied", rdata, get_version());
    return true;
  }

  vector<const char*> args;
  for (unsigned i = 1; i < m->cmd.size(); i++)
    args.push_back(m->cmd[i].c_str());

  if (m->cmd.size() > 1) {
    if (m->cmd[1] == "stat") {
      ss << mdsmap;
      r = 0;
    } 
    else if (m->cmd[1] == "dump") {
      string format = "plain";
      string val;
      epoch_t epoch = 0;
      for (std::vector<const char*>::iterator i = args.begin()+1; i != args.end(); ) {
	if (ceph_argparse_double_dash(args, i))
	  break;
	else if (ceph_argparse_witharg(args, i, &val, "-f", "--format", (char*)NULL))
	  format = val;
	else if (!epoch) {
	  long l = parse_pos_long(*i++, &ss);
	  if (l < 0) {
	    r = -EINVAL;
	    goto out;
	  }
	  epoch = l;
	} else
	  i++;
      }

      MDSMap *p = &mdsmap;
      if (epoch) {
	bufferlist b;
	get_version(epoch, b);
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
	if (format == "json") {
	  JSONFormatter jf(true);
	  jf.open_object_section("mdsmap");
	  p->dump(&jf);
	  jf.close_section();
	  jf.flush(ds);
	  r = 0;
	} else if (format == "plain") {
	  p->print(ds);
	  r = 0;
	} else {
	  ss << "unrecognized format '" << format << "'";
	  r = -EINVAL;
	}
	if (r == 0) {
	  rdata.append(ds);
	  ss << "dumped mdsmap epoch " << p->get_epoch();
	}
	if (p != &mdsmap)
	  delete p;
      }
    }
    else if (m->cmd[1] == "getmap") {
      if (m->cmd.size() > 2) {
	long l = parse_pos_long(m->cmd[2].c_str(), &ss);
	if (l < 0) {
	  r = -EINVAL;
	  goto out;
	}
	epoch_t e = l;
	bufferlist b;
	get_version(e, b);
	if (!b.length()) {
	  r = -ENOENT;
	} else {
	  MDSMap mm;
	  mm.decode(b);
	  mm.encode(rdata, m->get_connection()->get_features());
	  ss << "got mdsmap epoch " << mm.get_epoch();
	}
      } else {
	mdsmap.encode(rdata, m->get_connection()->get_features());
	ss << "got mdsmap epoch " << mdsmap.get_epoch();
      }
      r = 0;
    }
    else if (m->cmd[1] == "tell") {
      m->cmd.erase(m->cmd.begin()); //take out first two args; don't need them
      m->cmd.erase(m->cmd.begin());
      if (m->cmd[0] == "*") {
	m->cmd.erase(m->cmd.begin()); //and now we're done with the target num
	r = -ENOENT;
	const map<uint64_t, MDSMap::mds_info_t> mds_info = mdsmap.get_mds_info();
	for (map<uint64_t, MDSMap::mds_info_t>::const_iterator i = mds_info.begin();
	     i != mds_info.end();
	     ++i) {
	  mon->send_command(i->second.get_inst(), m->cmd, get_version());
	  r = 0;
	}
	if (r == -ENOENT) {
	  ss << "no mds active";
	} else {
	  ss << "ok";
	}
      } else {
	errno = 0;
	int who = strtol(m->cmd[0].c_str(), 0, 10);
	m->cmd.erase(m->cmd.begin()); //done with target num now
	if (!errno && who >= 0) {
	  if (mdsmap.is_up(who)) {
	    mon->send_command(mdsmap.get_inst(who), m->cmd, get_version());
	    r = 0;
	    ss << "ok";
	  } else {
	    ss << "mds." << who << " no up";
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

 out:
  if (r != -1) {
    string rs;
    getline(ss, rs);
    mon->reply_command(m, r, rs, rdata, get_version());
    return true;
  } else
    return false;
}

void MDSMonitor::fail_mds_gid(uint64_t gid)
{
  assert(pending_mdsmap.mds_info.count(gid));
  MDSMap::mds_info_t& info = pending_mdsmap.mds_info[gid];
  dout(10) << "fail_mds_gid " << gid << " mds." << info.name << " rank " << info.rank << dendl;

  utime_t until = ceph_clock_now(g_ceph_context);
  until += g_conf->mds_blacklist_interval;

  pending_mdsmap.last_failure_osd_epoch = mon->osdmon()->blacklist(info.addr, until);
  mon->osdmon()->propose_pending();

  if (info.rank >= 0) {
    pending_mdsmap.up.erase(info.rank);
    pending_mdsmap.failed.insert(info.rank);
  }

  pending_mdsmap.mds_info.erase(gid);
}

int MDSMonitor::fail_mds(std::ostream &ss, const std::string &arg)
{
  std::string err;
  int w = strict_strtoll(arg.c_str(), 10, &err);
  if (!err.empty()) {
    // Try to interpret the arg as an MDS name
    const MDSMap::mds_info_t *mds_info = mdsmap.find_by_name(arg);
    if (!mds_info) {
      ss << "Can't find any MDS named '" << arg << "'";
      return -ENOENT;
    }
    w = mds_info->rank;
  }

  if (pending_mdsmap.up.count(w)) {
    uint64_t gid = pending_mdsmap.up[w];
    if (pending_mdsmap.mds_info.count(gid))
      fail_mds_gid(gid);
    ss << "failed mds." << w;
  } else if (pending_mdsmap.mds_info.count(w)) {
    fail_mds_gid(w);
    ss << "failed mds gid " << w;
  }
  return 0;
}

int MDSMonitor::cluster_fail(std::ostream &ss)
{
  dout(10) << "cluster_fail" << dendl;

  if (!pending_mdsmap.test_flag(CEPH_MDSMAP_DOWN)) {
    ss << "mdsmap must be marked DOWN first ('mds cluster_down')";
    return -EPERM;
  }
  if (pending_mdsmap.up.size() && !mon->osdmon()->is_writeable()) {
    ss << "osdmap not writeable, can't blacklist up mds's";
    return -EAGAIN;
  }

  // --- reset the cluster map ---
  if (pending_mdsmap.mds_info.size()) {
    // blacklist all old mds's
    utime_t until = ceph_clock_now(g_ceph_context);
    until += g_conf->mds_blacklist_interval;
    for (map<int32_t,uint64_t>::iterator p = pending_mdsmap.up.begin();
	 p != pending_mdsmap.up.end();
	 ++p) {
      MDSMap::mds_info_t& info = pending_mdsmap.mds_info[p->second];
      dout(10) << " blacklisting gid " << p->second << " " << info.addr << dendl;
      pending_mdsmap.last_failure_osd_epoch = mon->osdmon()->blacklist(info.addr, until);
    }
    mon->osdmon()->propose_pending();
  }
  pending_mdsmap.up.clear();
  pending_mdsmap.failed.insert(pending_mdsmap.in.begin(),
			       pending_mdsmap.in.end());
  pending_mdsmap.in.clear();
  pending_mdsmap.mds_info.clear();

  ss << "failed all mds cluster members";
  return 0;
}

bool MDSMonitor::prepare_command(MMonCommand *m)
{
  int r = -EINVAL;
  stringstream ss;
  bufferlist rdata;

  MonSession *session = m->get_session();
  if (!session ||
      (!session->caps.get_allow_all() &&
       !session->caps.check_privileges(PAXOS_MDSMAP, MON_CAP_W) &&
       !mon->_allowed_command(session, m->cmd))) {
    mon->reply_command(m, -EACCES, "access denied", rdata, get_version());
    return true;
  }

  if (m->cmd.size() > 1) {
    if ((m->cmd[1] == "stop" || m->cmd[1] == "deactivate") && m->cmd.size() > 2) {
      int who = parse_pos_long(m->cmd[2].c_str(), &ss);
      if (who < 0)
	goto out;
      if (!pending_mdsmap.is_active(who)) {
	r = -EEXIST;
	ss << "mds." << who << " not active (" 
	   << ceph_mds_state_name(pending_mdsmap.get_state(who)) << ")";
      } else if ((pending_mdsmap.get_root() == who ||
		  pending_mdsmap.get_tableserver() == who) &&
		 pending_mdsmap.get_num_in_mds() > 1) {
	r = -EBUSY;
	ss << "can't tell the root (" << pending_mdsmap.get_root()
	   << ") or tableserver (" << pending_mdsmap.get_tableserver()
	   << " to deactivate unless it is the last mds in the cluster";
      } else if (pending_mdsmap.get_num_in_mds() <= pending_mdsmap.get_max_mds()) {
	r = -EBUSY;
	ss << "must decrease max_mds or else MDS will immediately reactivate";
      } else {
	r = 0;
	uint64_t gid = pending_mdsmap.up[who];
	ss << "telling mds." << who << " " << pending_mdsmap.mds_info[gid].addr << " to deactivate";
	pending_mdsmap.mds_info[gid].state = MDSMap::STATE_STOPPING;
      }
    }
    else if (m->cmd[1] == "set_max_mds" && m->cmd.size() > 2) {
      long l = parse_pos_long(m->cmd[2].c_str(), &ss);
      if (l < 0)
	goto out;
      pending_mdsmap.max_mds = l;
      r = 0;
      ss << "max_mds = " << pending_mdsmap.max_mds;
    }
    else if (m->cmd[1] == "setmap" && m->cmd.size() == 3) {
      MDSMap map;
      map.decode(m->get_data());
      long l = parse_pos_long(m->cmd[2].c_str(), &ss);
      if (l < 0)
	goto out;
      epoch_t e = l;
      //if (ceph_fsid_compare(&map.get_fsid(), &mon->monmap->fsid) == 0) {
      if (pending_mdsmap.epoch == e) {
	map.epoch = pending_mdsmap.epoch;  // make sure epoch is correct
	pending_mdsmap = map;
	string rs = "set mds map";
	wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	return true;
      } else
	ss << "next mdsmap epoch " << pending_mdsmap.epoch << " != " << e;
	//} else
	//ss << "mdsmap fsid " << map.fsid << " does not match monitor fsid " << mon->monmap->fsid;
    }
    else if (m->cmd[1] == "set_state" && m->cmd.size() == 4) {
      long l = parse_pos_long(m->cmd[2].c_str(), &ss);
      if (l < 0)
	goto out;
      uint64_t gid = l;
      long state = parse_pos_long(m->cmd[3].c_str(), &ss);
      if (state < 0)
	goto out;
      if (!pending_mdsmap.is_dne_gid(gid)) {
	MDSMap::mds_info_t& info = pending_mdsmap.get_info_gid(gid);
	info.state = state;
	stringstream ss;
	ss << "set mds gid " << gid << " to state " << state << " " << ceph_mds_state_name(state);
	string rs;
	getline(ss, rs);
	wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	return true;
      }
    }
    else if (m->cmd[1] == "fail" && m->cmd.size() == 3) {
      r = fail_mds(ss, m->cmd[2]);
    }
    else if (m->cmd[1] == "rm" && m->cmd.size() == 3) {
      int64_t gid = parse_pos_long(m->cmd[2].c_str(), &ss);
      if (gid < 0)
	goto out;
      int state = pending_mdsmap.get_state_gid(gid);
      if (state == 0) {
	ss << "mds gid " << gid << " dne";
	r = 0;
      } else if (state > 0) {
	ss << "cannot remove active mds." << pending_mdsmap.get_info_gid(gid).name
	   << " rank " << pending_mdsmap.get_info_gid(gid).rank;
	r = -EBUSY;
      } else {
	pending_mdsmap.mds_info.erase(gid);
	stringstream ss;
	ss << "removed mds gid " << gid;
	string rs;
	getline(ss, rs);
	wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	return true;
      }
    }
    else if (m->cmd[1] == "rmfailed" && m->cmd.size() == 3) {
      long w = parse_pos_long(m->cmd[2].c_str(), &ss);
      if (w < 0)
	goto out;
      pending_mdsmap.failed.erase(w);
      stringstream ss;
      ss << "removed failed mds." << w;
      string rs;
      getline(ss, rs);
      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
      return true;
    }
    else if (m->cmd[1] == "cluster_fail") {
      r = cluster_fail(ss);
    }
    else if (m->cmd[1] == "cluster_down") {
      if (pending_mdsmap.test_flag(CEPH_MDSMAP_DOWN)) {
	ss << "mdsmap already marked DOWN";
	r = -EPERM;
      } else {
	pending_mdsmap.set_flag(CEPH_MDSMAP_DOWN);
	ss << "marked mdsmap DOWN";
	r = 0;
      }
    }
    else if (m->cmd[1] == "cluster_up") {
      if (pending_mdsmap.test_flag(CEPH_MDSMAP_DOWN)) {
	pending_mdsmap.clear_flag(CEPH_MDSMAP_DOWN);
	ss << "unmarked mdsmap DOWN";
	r = 0;
      } else {
	ss << "mdsmap not marked DOWN";
	r = -EPERM;
      }
    }
    else if (m->cmd[1] == "compat" && m->cmd.size() == 4) {
      int64_t f = parse_pos_long(m->cmd[3].c_str(), &ss);
      if (f < 0)
	goto out;
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
    } else if (m->cmd[1] == "add_data_pool" && m->cmd.size() == 3) {
      int64_t poolid = parse_pos_long(m->cmd[2].c_str(), &ss);
      if (poolid < 0)
	goto out;
      pending_mdsmap.add_data_pool(poolid);
      ss << "added data pool " << poolid << " to mdsmap";
      r = 0;
    } else if (m->cmd[1] == "remove_data_pool" && m->cmd.size() == 3) {
      int64_t poolid = parse_pos_long(m->cmd[2].c_str(), &ss);
      if (poolid < 0)
	goto out;
      r = pending_mdsmap.remove_data_pool(poolid);
      if (r == 0)
	ss << "removed data pool " << poolid << " from mdsmap";
    } else if (m->cmd[1] == "newfs" && m->cmd.size() >= 4) {
      MDSMap newmap;
      long metadata = parse_pos_long(m->cmd[2].c_str(), &ss);
      if (metadata < 0)
	goto out;
      long data = parse_pos_long(m->cmd[3].c_str());
      if (data < 0)
	goto out;
      if (m->cmd.size() < 5 || m->cmd[4] != "--yes-i-really-mean-it") {
	ss << "this is DANGEROUS and will wipe out the mdsmap's fs, and may clobber data in the new pools you specify.  add --yes-i-really-mean-it if you do.";
	r = -EPERM;
      } else {
	pending_mdsmap = newmap;
	pending_mdsmap.epoch = mdsmap.epoch + 1;
	create_new_fs(pending_mdsmap, metadata, data);
	ss << "new fs with metadata pool " << metadata << " and data pool " << data;
	string rs;
	getline(ss, rs);
	wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs, get_version()));
	return true;
      }
    }    
  }
  if (r == -EINVAL) 
    ss << "unrecognized command";
 out:
  string rs;
  getline(ss, rs);

  if (r >= 0) {
    // success.. delay reply
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, r, rs, get_version()));
    return true;
  } else {
    // reply immediately
    mon->reply_command(m, r, rs, rdata, get_version());
    return false;
  }
}


void MDSMonitor::check_subs()
{
  string type = "mdsmap";
  if (mon->session_map.subs.count(type) == 0)
    return;
  xlist<Subscription*>::iterator p = mon->session_map.subs[type]->begin();
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
  if (!is_active()) return;

  update_from_paxos();
  dout(10) << mdsmap << dendl;
  
  bool do_propose = false;

  if (!mon->is_leader()) return;

  // expand mds cluster (add new nodes to @in)?
  while (pending_mdsmap.get_num_in_mds() < pending_mdsmap.get_max_mds() &&
	 !pending_mdsmap.is_degraded()) {
    int mds = 0;
    string name;
    while (pending_mdsmap.is_in(mds))
      mds++;
    uint64_t newgid = pending_mdsmap.find_replacement_for(mds, name);
    if (!newgid)
      break;

    MDSMap::mds_info_t& info = pending_mdsmap.mds_info[newgid];
    dout(1) << "adding standby " << info.addr << " as mds." << mds << dendl;
    
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
  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t cutoff = now;
  cutoff -= g_conf->mds_beacon_grace;

  // make sure last_beacon is fully populated
  for (map<uint64_t,MDSMap::mds_info_t>::iterator p = pending_mdsmap.mds_info.begin();
       p != pending_mdsmap.mds_info.end();
       ++p) {
    if (last_beacon.count(p->first) == 0) {
      const MDSMap::mds_info_t& info = p->second;
      dout(10) << " adding " << p->second.addr << " mds." << info.rank << "." << info.inc
	       << " " << ceph_mds_state_name(info.state)
	       << " to last_beacon" << dendl;
      last_beacon[p->first].stamp = ceph_clock_now(g_ceph_context);
      last_beacon[p->first].seq = 0;
    }
  }

  if (mon->osdmon()->is_writeable()) {

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

      if (since >= cutoff)
	continue;

      MDSMap::mds_info_t& info = pending_mdsmap.mds_info[gid];

      dout(10) << "no beacon from " << gid << " " << info.addr << " mds." << info.rank << "." << info.inc
	       << " " << ceph_mds_state_name(info.state)
	       << " since " << since << dendl;
      
      // are we in?
      // and is there a non-laggy standby that can take over for us?
      uint64_t sgid;
      if (info.rank >= 0 &&
	  info.state != MDSMap::STATE_STANDBY &&
	  info.state != MDSMap::STATE_STANDBY_REPLAY &&
	  (sgid = pending_mdsmap.find_replacement_for(info.rank, info.name)) != 0) {
	MDSMap::mds_info_t& si = pending_mdsmap.mds_info[sgid];
	dout(10) << " replacing " << gid << " " << info.addr << " mds." << info.rank << "." << info.inc
		 << " " << ceph_mds_state_name(info.state)
		 << " with " << sgid << "/" << si.name << " " << si.addr << dendl;
	switch (info.state) {
	case MDSMap::STATE_CREATING:
	case MDSMap::STATE_STARTING:
	  si.state = info.state;
	  break;
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
	  until += g_conf->mds_blacklist_interval;
	  pending_mdsmap.last_failure_osd_epoch = mon->osdmon()->blacklist(info.addr, until);
	  propose_osdmap = true;
	}
	pending_mdsmap.mds_info.erase(gid);
	last_beacon.erase(gid);
	do_propose = true;
      } else if (info.state == MDSMap::STATE_STANDBY_REPLAY) {
	dout(10) << " failing " << gid << " " << info.addr << " mds." << info.rank << "." << info.inc
		 << " " << ceph_mds_state_name(info.state)
		 << dendl;
	pending_mdsmap.mds_info.erase(gid);
	last_beacon.erase(gid);
	do_propose = true;
      } else {
	if (info.state == MDSMap::STATE_STANDBY ||
	    info.state == MDSMap::STATE_STANDBY_REPLAY) {
	  // remove it
	  dout(10) << " removing " << gid << " " << info.addr << " mds." << info.rank << "." << info.inc
		   << " " << ceph_mds_state_name(info.state)
		   << " (laggy)" << dendl;
	  pending_mdsmap.mds_info.erase(gid);
	} else if (!info.laggy()) {
	  dout(10) << " marking " << gid << " " << info.addr << " mds." << info.rank << "." << info.inc
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
	dout(0) << " taking over failed mds." << f << " with " << sgid << "/" << si.name << " " << si.addr << dendl;
	si.state = MDSMap::STATE_REPLAY;
	si.rank = f;
	si.inc = ++pending_mdsmap.inc[f];
	pending_mdsmap.in.insert(f);
	pending_mdsmap.up[f] = sgid;
	pending_mdsmap.failed.erase(f);
	do_propose = true;
      }
    }
  }

  // have a standby follow someone?
  if (failed.empty()) {
    for (map<uint64_t,MDSMap::mds_info_t>::iterator j = pending_mdsmap.mds_info.begin();
	 j != pending_mdsmap.mds_info.end();
	 ++j) {
      MDSMap::mds_info_t& info = j->second;
      
      if (info.state != MDSMap::STATE_STANDBY)
	continue;

      /*
       * This mds is standby but has no rank assigned.
       * See if we can find it somebody to shadow
       */
      dout(20) << "gid " << j->first << " is standby and following nobody" << dendl;
      
      // standby for someone specific?
      if (info.standby_for_rank >= 0) {
	if (pending_mdsmap.is_followable(info.standby_for_rank) &&
	    try_standby_replay(info, pending_mdsmap.mds_info[pending_mdsmap.up[info.standby_for_rank]]))
	  do_propose = true;
	continue;
      }

      // check everyone
      for (map<uint64_t,MDSMap::mds_info_t>::iterator i = pending_mdsmap.mds_info.begin();
	   i != pending_mdsmap.mds_info.end();
	   ++i) {
	if (i->second.rank >= 0 && pending_mdsmap.is_followable(i->second.rank)) {
	  if ((info.standby_for_name.length() && info.standby_for_name != i->second.name) ||
	      info.standby_for_rank >= 0)
	    continue;   // we're supposed to follow someone else

	  if (info.standby_for_rank == MDSMap::MDS_STANDBY_ANY &&
	      try_standby_replay(info, i->second)) {
	    do_propose = true;
	    break;
	  }
	  continue;
	}
      }
    }
  }

  if (do_propose)
    propose_pending();
}

bool MDSMonitor::try_standby_replay(MDSMap::mds_info_t& finfo, MDSMap::mds_info_t& ainfo)
{
  // someone else already following?
  uint64_t lgid = pending_mdsmap.find_standby_for(ainfo.rank, ainfo.name);
  if (lgid) {
    MDSMap::mds_info_t& sinfo = pending_mdsmap.mds_info[lgid];
    dout(20) << " mds." << ainfo.rank
	     << " standby gid " << lgid << " with state "
	     << ceph_mds_state_name(sinfo.state)
	     << dendl;
    if (sinfo.state == MDSMap::STATE_STANDBY_REPLAY) {
      dout(20) << "  skipping this MDS since it has a follower!" << dendl;
      return false; // this MDS already has a standby
    }
  }

  // hey, we found an MDS without a standby. Pair them!
  finfo.standby_for_rank = ainfo.rank;
  dout(10) << "  setting to shadow mds rank " << finfo.standby_for_rank << dendl;
  finfo.state = MDSMap::STATE_STANDBY_REPLAY;
  return true;
}


void MDSMonitor::do_stop()
{
  // hrm...
  if (!mon->is_leader() ||
      !is_active()) {
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
	utime_t until = ceph_clock_now(g_ceph_context);
	until += g_conf->mds_blacklist_interval;
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
