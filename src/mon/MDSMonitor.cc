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

#include <sstream>

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

#include "common/config.h"
#include "include/assert.h"

#include "MonitorDBStore.h"
#include "common/cmdparse.h"
#include "include/str_list.h"

#include "mds/mdstypes.h"

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
  dout(dbl) << "print_map\n";
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
  m.compat = get_mdsmap_compat_set_default();

  m.session_timeout = g_conf->mds_session_timeout;
  m.session_autoclose = g_conf->mds_session_autoclose;
  m.max_file_size = g_conf->mds_max_file_size;

  print_map(m);
}


// service methods
void MDSMonitor::create_initial()
{
  dout(10) << "create_initial" << dendl;
  create_new_fs(pending_mdsmap, MDS_METADATA_POOL, MDS_DATA_POOL);
}


void MDSMonitor::update_from_paxos(bool *need_bootstrap)
{
  version_t version = get_last_committed();
  if (version == mdsmap.epoch)
    return;
  assert(version >= mdsmap.epoch);

  dout(10) << __func__ << " version " << version
	   << ", my e " << mdsmap.epoch << dendl;

  // read and decode
  mdsmap_bl.clear();
  int err = get_version(version, mdsmap_bl);
  assert(err == 0);

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

  // print map iff 'debug mon = 30' or higher
  print_map(pending_mdsmap, 30);

  // apply to paxos
  assert(get_last_committed() + 1 == pending_mdsmap.epoch);
  bufferlist mdsmap_bl;
  pending_mdsmap.encode(mdsmap_bl, mon->get_quorum_features());

  /* put everything in the transaction */
  put_version(t, pending_mdsmap.epoch, mdsmap_bl);
  put_last_committed(t, pending_mdsmap.epoch);
}

version_t MDSMonitor::get_trim_to()
{
  version_t floor = 0;
  if (g_conf->mon_mds_force_trim_to > 0 &&
      g_conf->mon_mds_force_trim_to < (int)get_last_committed()) {
    floor = g_conf->mon_mds_force_trim_to;
    dout(10) << __func__ << " explicit mon_mds_force_trim_to = "
             << floor << dendl;
  }

  unsigned max = g_conf->mon_max_mdsmap_epochs;
  version_t last = get_last_committed();

  if (last - get_first_committed() > max && floor < last - max)
    return last - max;
  return floor;
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
    return preprocess_beacon(static_cast<MMDSBeacon*>(m));
    
  case MSG_MON_COMMAND:
    return preprocess_command(static_cast<MMonCommand*>(m));

  case MSG_MDS_OFFLOAD_TARGETS:
    return preprocess_offload_targets(static_cast<MMDSLoadTargets*>(m));

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
  assert(session);
  if (!session->is_capable("mds", MON_CAP_X)) {
    dout(0) << "preprocess_beacon got MMDSBeacon from entity with insufficient privileges "
	    << session->caps << dendl;
    goto ignore;
  }

  if (m->get_fsid() != mon->monmap->fsid) {
    dout(0) << "preprocess_beacon on fsid " << m->get_fsid() << " != " << mon->monmap->fsid << dendl;
    goto ignore;
  }

  dout(12) << "preprocess_beacon " << *m
	   << " from " << m->get_orig_source_inst()
	   << " " << m->get_compat()
	   << dendl;

  // make sure the address has a port
  if (m->get_orig_source_addr().get_port() == 0) {
    dout(1) << " ignoring boot message without a port" << dendl;
    goto ignore;
  }

  // check compat
  if (!m->get_compat().writeable(mdsmap.compat)) {
    dout(1) << " mds " << m->get_source_inst() << " can't write to mdsmap " << mdsmap.compat << dendl;
    goto ignore;
  }

  // fw to leader?
  if (!mon->is_leader())
    return false;

  if (pending_mdsmap.test_flag(CEPH_MDSMAP_DOWN)) {
    dout(7) << " mdsmap DOWN flag set, ignoring mds " << m->get_source_inst() << " beacon" << dendl;
    goto ignore;
  }

  // booted, but not in map?
  if (pending_mdsmap.is_dne_gid(gid)) {
    if (state != MDSMap::STATE_BOOT) {
      dout(7) << "mds_beacon " << *m << " is not in mdsmap" << dendl;
      mon->send_reply(m, new MMDSMap(mon->monmap->fsid, &mdsmap));
      m->put();
      return true;
    } else {
      return false;  // not booted yet.
    }
  }
  info = pending_mdsmap.get_info_gid(gid);

  // old seq?
  if (info.state_seq > seq) {
    dout(7) << "mds_beacon " << *m << " has old seq, ignoring" << dendl;
    goto ignore;
  }

  if (mdsmap.get_epoch() != m->get_last_epoch_seen()) {
    dout(10) << "mds_beacon " << *m
	     << " ignoring requested state, because mds hasn't seen latest map" << dendl;
    goto reply;
  }

  if (info.laggy()) {
    _note_beacon(m);
    return false;  // no longer laggy, need to update map.
  }
  if (state == MDSMap::STATE_BOOT) {
    // ignore, already booted.
    goto ignore;
  }
  // is there a state change here?
  if (info.state != state) {
    // legal state change?
    if ((info.state == MDSMap::STATE_STANDBY ||
	 info.state == MDSMap::STATE_STANDBY_REPLAY ||
	 info.state == MDSMap::STATE_ONESHOT_REPLAY) && state > 0) {
      dout(10) << "mds_beacon mds can't activate itself (" << ceph_mds_state_name(info.state)
	       << " -> " << ceph_mds_state_name(state) << ")" << dendl;
      goto reply;
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
      goto reply;
    }
    _note_beacon(m);
    return false;  // need to update map
  }

 reply:
  // note time and reply
  _note_beacon(m);
  mon->send_reply(m,
		  new MMDSBeacon(mon->monmap->fsid, m->get_global_id(), m->get_name(),
				 mdsmap.get_epoch(), state, seq));
  m->put();
  return true;

 ignore:
  // I won't reply this beacon, drop it.
  mon->no_reply(m);
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
  if (!session->is_capable("mds", MON_CAP_X)) {
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
    return prepare_beacon(static_cast<MMDSBeacon*>(m));

  case MSG_MON_COMMAND:
    return prepare_command(static_cast<MMonCommand*>(m));

  case MSG_MDS_OFFLOAD_TARGETS:
    return prepare_offload_targets(static_cast<MMDSLoadTargets*>(m));
  
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
      bool failed_mds = false;
      while (uint64_t existing = pending_mdsmap.find_mds_gid_by_name(m->get_name())) {
        if (!mon->osdmon()->is_writeable()) {
          mon->osdmon()->wait_for_writeable(new C_RetryMessage(this, m));
          return false;
        }
	fail_mds_gid(existing);
        failed_mds = true;
      }
      if (failed_mds) {
        assert(mon->osdmon()->is_writeable());
        request_proposal(mon->osdmon());
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
  // delegate to PaxosService to assess whether we should propose
  return PaxosService::should_propose(delay);
}

void MDSMonitor::_updated(MMDSBeacon *m)
{
  dout(10) << "_updated " << m->get_orig_source() << " " << *m << dendl;
  mon->clog.info() << m->get_orig_source_inst() << " "
	  << ceph_mds_state_name(m->get_state()) << "\n";

  if (m->get_state() == MDSMap::STATE_STOPPED) {
    // send the map manually (they're out of the map, so they won't get it automatic)
    mon->send_reply(m, new MMDSMap(mon->monmap->fsid, &mdsmap));
  } else {
    mon->send_reply(m, new MMDSBeacon(mon->monmap->fsid,
				      m->get_global_id(),
				      m->get_name(),
				      mdsmap.get_epoch(),
				      m->get_state(),
				      m->get_seq()));
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

  f->dump_unsigned("mdsmap_first_committed", get_first_committed());
  f->dump_unsigned("mdsmap_last_committed", get_last_committed());
}

bool MDSMonitor::preprocess_command(MMonCommand *m)
{
  int r = -1;
  bufferlist rdata;
  stringstream ss, ds;

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    // ss has reason for failure
    string rs = ss.str();
    mon->reply_command(m, -EINVAL, rs, rdata, get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);
  string format;
  cmd_getval(g_ceph_context, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(new_formatter(format));

  MonSession *session = m->get_session();
  if (!session) {
    mon->reply_command(m, -EACCES, "access denied", rdata, get_last_committed());
    return true;
  }

  if (prefix == "mds stat") {
    if (f) {
      f->open_object_section("mds_stat");
      dump_info(f.get());
      f->close_section();
      f->flush(ds);
    } else {
      ds << mdsmap;
    }
    r = 0;
  } else if (prefix == "mds dump") {
    int64_t epocharg;
    epoch_t epoch;

    MDSMap *p = &mdsmap;
    if (cmd_getval(g_ceph_context, cmdmap, "epoch", epocharg)) {
      epoch = epocharg;
      bufferlist b;
      int err = get_version(epoch, b);
      if (err == -ENOENT) {
	p = 0;
	r = -ENOENT;
      } else {
	assert(err == 0);
	assert(b.length());
	p = new MDSMap;
	p->decode(b);
      }
    }
    if (p) {
      stringstream ds;
      if (f != NULL) {
	f->open_object_section("mdsmap");
	p->dump(f.get());
	f->close_section();
	f->flush(ds);
	r = 0;
      } else {
	p->print(ds);
	r = 0;
      } 
      if (r == 0) {
	rdata.append(ds);
	ss << "dumped mdsmap epoch " << p->get_epoch();
      }
      if (p != &mdsmap)
	delete p;
    }
  } else if (prefix == "mds getmap") {
    epoch_t e;
    int64_t epocharg;
    bufferlist b;
    if (cmd_getval(g_ceph_context, cmdmap, "epoch", epocharg)) {
      e = epocharg;
      int err = get_version(e, b);
      if (err == -ENOENT) {
	r = -ENOENT;
      } else {
	assert(r == 0);
	assert(b.length());
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
  } else if (prefix == "mds tell") {
    string whostr;
    cmd_getval(g_ceph_context, cmdmap, "who", whostr);
    vector<string>args_vec;
    cmd_getval(g_ceph_context, cmdmap, "args", args_vec);

    if (whostr == "*") {
      r = -ENOENT;
      const map<uint64_t, MDSMap::mds_info_t> mds_info = mdsmap.get_mds_info();
      for (map<uint64_t, MDSMap::mds_info_t>::const_iterator i = mds_info.begin();
	   i != mds_info.end();
	   ++i) {
	m->cmd = args_vec;
	mon->send_command(i->second.get_inst(), m->cmd);
	r = 0;
      }
      if (r == -ENOENT) {
	ss << "no mds active";
      } else {
	ss << "ok";
      }
    } else {
      errno = 0;
      long who = strtol(whostr.c_str(), 0, 10);
      if (!errno && who >= 0) {
	if (mdsmap.is_up(who)) {
	  m->cmd = args_vec;
	  mon->send_command(mdsmap.get_inst(who), m->cmd);
	  r = 0;
	  ss << "ok";
	} else {
	  ss << "mds." << who << " not up";
	  r = -ENOENT;
	}
      } else ss << "specify mds number or *";
    }
  } else if (prefix == "mds compat show") {
      if (f) {
	f->open_object_section("mds_compat");
	mdsmap.compat.dump(f.get());
	f->close_section();
	f->flush(ds);
      } else {
	ds << mdsmap.compat;
      }
      r = 0;
  }

  if (r != -1) {
    rdata.append(ds);
    string rs;
    getline(ss, rs);
    mon->reply_command(m, r, rs, rdata, get_last_committed());
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

  if (info.rank >= 0) {
    if (info.state == MDSMap::STATE_CREATING) {
      // If this gid didn't make it past CREATING, then forget
      // the rank ever existed so that next time it's handed out
      // to a gid it'll go back into CREATING.
      pending_mdsmap.in.erase(info.rank);
    } else {
      // Put this rank into the failed list so that the next available STANDBY will
      // pick it up.
      pending_mdsmap.failed.insert(info.rank);
    }
    pending_mdsmap.up.erase(info.rank);
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

  if (!mon->osdmon()->is_writeable()) {
    return -EAGAIN;
 }

  bool failed_mds_gid = false;
  if (pending_mdsmap.up.count(w)) {
    uint64_t gid = pending_mdsmap.up[w];
    if (pending_mdsmap.mds_info.count(gid)) {
      fail_mds_gid(gid);
      failed_mds_gid = true;
    }
    ss << "failed mds." << w;
  } else if (pending_mdsmap.mds_info.count(w)) {
    fail_mds_gid(w);
    failed_mds_gid = true;
    ss << "failed mds gid " << w;
  }

  if (failed_mds_gid) {
    assert(mon->osdmon()->is_writeable());
    request_proposal(mon->osdmon());
  }
  return 0;
}

bool MDSMonitor::prepare_command(MMonCommand *m)
{
  int r = -EINVAL;
  stringstream ss;
  bufferlist rdata;

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(m, -EINVAL, rs, rdata, get_last_committed());
    return true;
  }

  string prefix;

  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  MonSession *session = m->get_session();
  if (!session) {
    mon->reply_command(m, -EACCES, "access denied", rdata, get_last_committed());
    return true;
  }

  string whostr;
  cmd_getval(g_ceph_context, cmdmap, "who", whostr);
  if (prefix == "mds stop" ||
      prefix == "mds deactivate") {
    int who = parse_pos_long(whostr.c_str(), &ss);
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

  } else if (prefix == "mds set_max_mds") {
    // NOTE: see also "mds set max_mds", which can modify the same field.
    int64_t maxmds;
    if (!cmd_getval(g_ceph_context, cmdmap, "maxmds", maxmds) || maxmds < 0)
      goto out;
    pending_mdsmap.max_mds = maxmds;
    r = 0;
    ss << "max_mds = " << pending_mdsmap.max_mds;
  } else if (prefix == "mds set") {
    string var;
    if (!cmd_getval(g_ceph_context, cmdmap, "var", var) || var.empty()) {
      ss << "Invalid variable";
      goto out;
    }
    string val;
    string interr;
    int64_t n = 0;
    if (!cmd_getval(g_ceph_context, cmdmap, "val", val))
      goto out;
    // we got a string.  see if it contains an int.
    n = strict_strtoll(val.c_str(), 10, &interr);
    if (var == "max_mds") {
      // NOTE: see also "mds set_max_mds", which can modify the same field.
      if (interr.length())
	goto out;
      pending_mdsmap.max_mds = n;
    } else if (var == "inline_data") {
      if (val == "true" || val == "yes" || (!interr.length() && n == 1)) {
	string confirm;
	if (!cmd_getval(g_ceph_context, cmdmap, "confirm", confirm) ||
	    confirm != "--yes-i-really-mean-it") {
	  ss << "inline data is new and experimental; you must specify --yes-i-really-mean-it";
	  r = -EPERM;
	  goto out;
	}
	ss << "inline data enabled";
	pending_mdsmap.set_inline_data_enabled(true);
	pending_mdsmap.compat.incompat.insert(MDS_FEATURE_INCOMPAT_INLINE);
      } else if (val == "false" || val == "no" ||
		 (!interr.length() && n == 0)) {
	ss << "inline data disabled";
	pending_mdsmap.set_inline_data_enabled(false);
      } else {
	ss << "value must be false|no|0 or true|yes|1";
	r = -EINVAL;
	goto out;
      }
    } else if (var == "max_file_size") {
      if (interr.length()) {
	ss << var << " requires an integer value";
	goto out;
      }
      if (n < CEPH_MIN_STRIPE_UNIT) {
	r = -ERANGE;
	ss << var << " must at least " << CEPH_MIN_STRIPE_UNIT;
	goto out;
      }
      pending_mdsmap.max_file_size = n;
    } else if (var == "allow_new_snaps") {
      if (val == "false" || val == "no" || (interr.length() == 0 && n == 0)) {
	pending_mdsmap.clear_snaps_allowed();
	ss << "disabled new snapshots";
      } else if (val == "true" || val == "yes" || (interr.length() == 0 && n == 1)) {
	string confirm;
	if (!cmd_getval(g_ceph_context, cmdmap, "confirm", confirm) ||
	    confirm != "--yes-i-really-mean-it") {
	  ss << "Snapshots are unstable and will probably break your FS! Set to --yes-i-really-mean-it if you are sure you want to enable them";
	  r = -EPERM;
	  goto out;
	}
	pending_mdsmap.set_snaps_allowed();
	ss << "enabled new snapshots";
      } else {
	ss << "value must be true|yes|1 or false|no|0";
	r = -EINVAL;
	goto out;
      }
    } else {
      ss << "unknown variable " << var;
      goto out;
    }
    r = 0;
  } else if (prefix == "mds setmap") {
    MDSMap map;
    map.decode(m->get_data());
    epoch_t e = 0;
    int64_t epochnum;
    if (cmd_getval(g_ceph_context, cmdmap, "epoch", epochnum))
      e = epochnum;

    if (pending_mdsmap.epoch == e) {
      map.epoch = pending_mdsmap.epoch;  // make sure epoch is correct
      pending_mdsmap = map;
      string rs = "set mds map";
      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
						get_last_committed() + 1));
      return true;
    } else {
      ss << "next mdsmap epoch " << pending_mdsmap.epoch << " != " << e;
    }

  } else if (prefix == "mds set_state") {
    int64_t gid;
    if (!cmd_getval(g_ceph_context, cmdmap, "gid", gid)) {
      ss << "error parsing 'gid' value '"
         << cmd_vartype_stringify(cmdmap["gid"]) << "'";
      r = -EINVAL;
      goto out;
    }
    int64_t state;
    if (!cmd_getval(g_ceph_context, cmdmap, "state", state)) {
      ss << "error parsing 'state' string value '"
         << cmd_vartype_stringify(cmdmap["state"]) << "'";
      r = -EINVAL;
      goto out;
    }
    if (!pending_mdsmap.is_dne_gid(gid)) {
      MDSMap::mds_info_t& info = pending_mdsmap.get_info_gid(gid);
      info.state = state;
      stringstream ss;
      ss << "set mds gid " << gid << " to state " << state << " " << ceph_mds_state_name(state);
      string rs;
      getline(ss, rs);
      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
						get_last_committed() + 1));
      return true;
    }

  } else if (prefix == "mds fail") {
    string who;
    cmd_getval(g_ceph_context, cmdmap, "who", who);
    r = fail_mds(ss, who);
    if (r < 0 && r == -EAGAIN) {
      mon->osdmon()->wait_for_writeable(new C_RetryMessage(this, m));
      return false; // don't propose yet; wait for message to be retried
    }

  } else if (prefix == "mds rm") {
    int64_t gid;
    if (!cmd_getval(g_ceph_context, cmdmap, "gid", gid)) {
      ss << "error parsing 'gid' value '"
         << cmd_vartype_stringify(cmdmap["gid"]) << "'";
      r = -EINVAL;
      goto out;
    }
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
      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
						get_last_committed() + 1));
      return true;
    }
  } else if (prefix == "mds rmfailed") {
    int64_t w;
    if (!cmd_getval(g_ceph_context, cmdmap, "who", w)) {
      ss << "error parsing 'who' value '"
         << cmd_vartype_stringify(cmdmap["who"]) << "'";
      r = -EINVAL;
      goto out;
    }
    pending_mdsmap.failed.erase(w);
    stringstream ss;
    ss << "removed failed mds." << w;
    string rs;
    getline(ss, rs);
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
					      get_last_committed() + 1));
    return true;
  } else if (prefix == "mds cluster_down") {
    if (pending_mdsmap.test_flag(CEPH_MDSMAP_DOWN)) {
      ss << "mdsmap already marked DOWN";
    } else {
      pending_mdsmap.set_flag(CEPH_MDSMAP_DOWN);
      ss << "marked mdsmap DOWN";
    }
    r = 0;
  } else if (prefix == "mds cluster_up") {
    if (pending_mdsmap.test_flag(CEPH_MDSMAP_DOWN)) {
      pending_mdsmap.clear_flag(CEPH_MDSMAP_DOWN);
      ss << "unmarked mdsmap DOWN";
    } else {
      ss << "mdsmap not marked DOWN";
    }
    r = 0;
  } else if (prefix == "mds compat rm_compat") {
    int64_t f;
    if (!cmd_getval(g_ceph_context, cmdmap, "feature", f)) {
      ss << "error parsing feature value '"
         << cmd_vartype_stringify(cmdmap["feature"]) << "'";
      r = -EINVAL;
      goto out;
    }
    if (pending_mdsmap.compat.compat.contains(f)) {
      ss << "removing compat feature " << f;
      pending_mdsmap.compat.compat.remove(f);
      r = 0;
    } else {
      ss << "compat feature " << f << " not present in " << pending_mdsmap.compat;
      r = 0;
    }
  } else if (prefix == "mds compat rm_incompat") {
    int64_t f;
    if (!cmd_getval(g_ceph_context, cmdmap, "feature", f)) {
      ss << "error parsing feature value '"
         << cmd_vartype_stringify(cmdmap["feature"]) << "'";
      r = -EINVAL;
      goto out;
    }
    if (pending_mdsmap.compat.incompat.contains(f)) {
      ss << "removing incompat feature " << f;
      pending_mdsmap.compat.incompat.remove(f);
      r = 0;
    } else {
      ss << "incompat feature " << f << " not present in " << pending_mdsmap.compat;
      r = 0;
    }

  } else if (prefix == "mds add_data_pool") {
    string poolname;
    cmd_getval(g_ceph_context, cmdmap, "pool", poolname);
    int64_t poolid = mon->osdmon()->osdmap.lookup_pg_pool_name(poolname);
    if (poolid < 0) {
      string err;
      poolid = strict_strtol(poolname.c_str(), 10, &err);
      if (err.length()) {
	r = -ENOENT;
	poolid = -1;
	ss << "pool '" << poolname << "' does not exist";
      }
    }
    if (poolid >= 0) {
      pending_mdsmap.add_data_pool(poolid);
      ss << "added data pool " << poolid << " to mdsmap";
      r = 0;
    }
  } else if (prefix == "mds remove_data_pool") {
    string poolname;
    cmd_getval(g_ceph_context, cmdmap, "pool", poolname);
    int64_t poolid = mon->osdmon()->osdmap.lookup_pg_pool_name(poolname);
    if (poolid < 0) {
      string err;
      poolid = strict_strtol(poolname.c_str(), 10, &err);
      if (err.length()) {
	r = -ENOENT;
	poolid = -1;
	ss << "pool '" << poolname << "' does not exist";
      }
    }

    if (pending_mdsmap.get_first_data_pool() == poolid) {
      r = -EINVAL;
      poolid = -1;
      ss << "cannot remove default data pool";
    }

    if (poolid >= 0) {
      cmd_getval(g_ceph_context, cmdmap, "poolid", poolid);
      r = pending_mdsmap.remove_data_pool(poolid);
      if (r == -ENOENT)
	r = 0;
      if (r == 0)
	ss << "removed data pool " << poolid << " from mdsmap";
    }
  } else if (prefix == "mds newfs") {
    MDSMap newmap;
    int64_t metadata, data;
    if (!cmd_getval(g_ceph_context, cmdmap, "metadata", metadata)) {
      ss << "error parsing 'metadata' value '"
         << cmd_vartype_stringify(cmdmap["metadata"]) << "'";
      r = -EINVAL;
      goto out;
    }
    if (!cmd_getval(g_ceph_context, cmdmap, "data", data)) {
      ss << "error parsing 'data' value '"
         << cmd_vartype_stringify(cmdmap["data"]) << "'";
      r = -EINVAL;
      goto out;
    }
    string sure;
    cmd_getval(g_ceph_context, cmdmap, "sure", sure);
    if (sure != "--yes-i-really-mean-it") {
      ss << "this is DANGEROUS and will wipe out the mdsmap's fs, and may clobber data in the new pools you specify.  add --yes-i-really-mean-it if you do.";
      r = -EPERM;
    } else {
      newmap.inc = pending_mdsmap.inc;
      pending_mdsmap = newmap;
      pending_mdsmap.epoch = mdsmap.epoch + 1;
      create_new_fs(pending_mdsmap, metadata, data);
      ss << "new fs with metadata pool " << metadata << " and data pool " << data;
      string rs;
      getline(ss, rs);
      wait_for_finished_proposal(new Monitor::C_Command(mon, m, 0, rs,
						get_last_committed() + 1));
      return true;
    }
  } else {
    ss << "unrecognized command";
  }
 out:
  string rs;
  getline(ss, rs);

  if (r >= 0) {
    // success.. delay reply
    wait_for_finished_proposal(new Monitor::C_Command(mon, m, r, rs,
					      get_last_committed() + 1));
    return true;
  } else {
    // reply immediately
    mon->reply_command(m, r, rs, rdata, get_last_committed());
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
      ++p;
      
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
	  do_propose = true;
	} else if (!info.laggy()) {
	  dout(10) << " marking " << gid << " " << info.addr << " mds." << info.rank << "." << info.inc
		   << " " << ceph_mds_state_name(info.state)
		   << " laggy" << dendl;
	  info.laggy_since = now;
	  do_propose = true;
	}
	last_beacon.erase(gid);
      }
    }

    if (propose_osdmap)
      request_proposal(mon->osdmon());

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
