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

#include <regex>
#include <sstream>
#include <boost/utility.hpp>

#include "MDSMonitor.h"
#include "FSCommands.h"
#include "Monitor.h"
#include "MonitorDBStore.h"
#include "OSDMonitor.h"

#include "common/strtol.h"
#include "common/perf_counters.h"
#include "common/config.h"
#include "common/cmdparse.h"
#include "messages/MMDSMap.h"
#include "messages/MFSMap.h"
#include "messages/MFSMapUser.h"
#include "messages/MMDSLoadTargets.h"
#include "messages/MMonCommand.h"
#include "messages/MGenericMessage.h"

#include "include/assert.h"
#include "include/str_list.h"
#include "include/stringify.h"
#include "mds/mdstypes.h"
#include "Session.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, fsmap)
static ostream& _prefix(std::ostream *_dout, Monitor *mon, FSMap const& fsmap) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").mds e" << fsmap.get_epoch() << " ";
}

static const string MDS_METADATA_PREFIX("mds_metadata");
static const string MDS_HEALTH_PREFIX("mds_health");


/*
 * Specialized implementation of cmd_getval to allow us to parse
 * out strongly-typedef'd types
 */
template<> bool cmd_getval(CephContext *cct, const cmdmap_t& cmdmap,
			   const std::string& k, mds_gid_t &val)
{
  return cmd_getval(cct, cmdmap, k, (int64_t&)val);
}

template<> bool cmd_getval(CephContext *cct, const cmdmap_t& cmdmap,
			   const std::string& k, mds_rank_t &val)
{
  return cmd_getval(cct, cmdmap, k, (int64_t&)val);
}

template<> bool cmd_getval(CephContext *cct, const cmdmap_t& cmdmap,
			   const std::string& k, MDSMap::DaemonState &val)
{
  return cmd_getval(cct, cmdmap, k, (int64_t&)val);
}

// my methods

template <int dblV>
void MDSMonitor::print_map(FSMap &m)
{
  dout(dblV) << "print_map\n";
  m.print(*_dout);
  *_dout << dendl;
}

// service methods
void MDSMonitor::create_initial()
{
  dout(10) << "create_initial" << dendl;
}

void MDSMonitor::get_store_prefixes(std::set<string>& s) const
{
  s.insert(service_name);
  s.insert(MDS_METADATA_PREFIX);
  s.insert(MDS_HEALTH_PREFIX);
}

void MDSMonitor::update_from_paxos(bool *need_bootstrap)
{
  version_t version = get_last_committed();
  if (version == fsmap.epoch)
    return;

  dout(10) << __func__ << " version " << version
	   << ", my e " << fsmap.epoch << dendl;
  assert(version > fsmap.epoch);

  load_health();

  // read and decode
  bufferlist fsmap_bl;
  fsmap_bl.clear();
  int err = get_version(version, fsmap_bl);
  assert(err == 0);

  assert(fsmap_bl.length() > 0);
  dout(10) << __func__ << " got " << version << dendl;
  fsmap.decode(fsmap_bl);

  // new map
  dout(4) << "new map" << dendl;
  print_map<0>(fsmap);
  if (!g_conf->mon_mds_skip_sanity) {
    fsmap.sanity();
  }

  check_subs();
}

void MDSMonitor::init()
{
  (void)load_metadata(pending_metadata);
}

void MDSMonitor::create_pending()
{
  pending_fsmap = fsmap;
  pending_fsmap.epoch++;

  if (mon->osdmon()->is_readable()) {
    auto &osdmap = mon->osdmon()->osdmap;
    pending_fsmap.sanitize([&osdmap](int64_t pool){return osdmap.have_pg_pool(pool);});
  }

  dout(10) << "create_pending e" << pending_fsmap.epoch << dendl;
}

void MDSMonitor::encode_pending(MonitorDBStore::TransactionRef t)
{
  dout(10) << "encode_pending e" << pending_fsmap.epoch << dendl;


  // print map iff 'debug mon = 30' or higher
  print_map<30>(pending_fsmap);
  if (!g_conf->mon_mds_skip_sanity) {
    pending_fsmap.sanity();
  }

  // Set 'modified' on maps modified this epoch
  for (auto &i : fsmap.filesystems) {
    if (i.second->mds_map.epoch == fsmap.epoch) {
      i.second->mds_map.modified = ceph_clock_now();
    }
  }

  // apply to paxos
  assert(get_last_committed() + 1 == pending_fsmap.epoch);
  bufferlist fsmap_bl;
  pending_fsmap.encode(fsmap_bl, mon->get_quorum_con_features());

  /* put everything in the transaction */
  put_version(t, pending_fsmap.epoch, fsmap_bl);
  put_last_committed(t, pending_fsmap.epoch);

  // Encode MDSHealth data
  for (std::map<uint64_t, MDSHealth>::iterator i = pending_daemon_health.begin();
      i != pending_daemon_health.end(); ++i) {
    bufferlist bl;
    i->second.encode(bl);
    t->put(MDS_HEALTH_PREFIX, stringify(i->first), bl);
  }

  for (std::set<uint64_t>::iterator i = pending_daemon_health_rm.begin();
      i != pending_daemon_health_rm.end(); ++i) {
    t->erase(MDS_HEALTH_PREFIX, stringify(*i));
  }
  pending_daemon_health_rm.clear();
  remove_from_metadata(t);

  // health
  health_check_map_t new_checks;
  const auto info_map = pending_fsmap.get_mds_info();
  for (const auto &i : info_map) {
    const auto &gid = i.first;
    const auto &info = i.second;
    if (pending_daemon_health_rm.count(gid)) {
      continue;
    }
    MDSHealth health;
    auto p = pending_daemon_health.find(gid);
    if (p != pending_daemon_health.end()) {
      health = p->second;
    } else {
      bufferlist bl;
      mon->store->get(MDS_HEALTH_PREFIX, stringify(gid), bl);
      if (!bl.length()) {
	derr << "Missing health data for MDS " << gid << dendl;
	continue;
      }
      bufferlist::iterator bl_i = bl.begin();
      health.decode(bl_i);
    }
    for (const auto &metric : health.metrics) {
      const int rank = info.rank;
      health_check_t *check = &new_checks.get_or_add(
	mds_metric_name(metric.type),
	metric.sev,
	mds_metric_summary(metric.type));
      ostringstream ss;
      ss << "mds" << info.name << "(mds." << rank << "): " << metric.message;
      for (auto p = metric.metadata.begin();
	   p != metric.metadata.end();
	   ++p) {
	if (p != metric.metadata.begin()) {
	  ss << ", ";
	}
	ss << p->first << ": " << p->second;
      }
      check->detail.push_back(ss.str());
    }
  }
  pending_fsmap.get_health_checks(&new_checks);
  for (auto& p : new_checks.checks) {
    p.second.summary = std::regex_replace(
      p.second.summary,
      std::regex("%num%"),
      stringify(p.second.detail.size()));
    p.second.summary = std::regex_replace(
      p.second.summary,
      std::regex("%plurals%"),
      p.second.detail.size() > 1 ? "s" : "");
    p.second.summary = std::regex_replace(
      p.second.summary,
      std::regex("%isorare%"),
      p.second.detail.size() > 1 ? "are" : "is");
    p.second.summary = std::regex_replace(
      p.second.summary,
      std::regex("%hasorhave%"),
      p.second.detail.size() > 1 ? "have" : "has");
  }
  encode_health(new_checks, t);
}

version_t MDSMonitor::get_trim_to() const
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

bool MDSMonitor::preprocess_query(MonOpRequestRef op)
{
  op->mark_mdsmon_event(__func__);
  PaxosServiceMessage *m = static_cast<PaxosServiceMessage*>(op->get_req());
  dout(10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;

  switch (m->get_type()) {
    
  case MSG_MDS_BEACON:
    return preprocess_beacon(op);
    
  case MSG_MON_COMMAND:
    return preprocess_command(op);

  case MSG_MDS_OFFLOAD_TARGETS:
    return preprocess_offload_targets(op);

  default:
    ceph_abort();
    return true;
  }
}

void MDSMonitor::_note_beacon(MMDSBeacon *m)
{
  mds_gid_t gid = mds_gid_t(m->get_global_id());
  version_t seq = m->get_seq();

  dout(15) << "_note_beacon " << *m << " noting time" << dendl;
  last_beacon[gid].stamp = ceph_clock_now();
  last_beacon[gid].seq = seq;
}

bool MDSMonitor::preprocess_beacon(MonOpRequestRef op)
{
  op->mark_mdsmon_event(__func__);
  MMDSBeacon *m = static_cast<MMDSBeacon*>(op->get_req());
  MDSMap::DaemonState state = m->get_state();
  mds_gid_t gid = m->get_global_id();
  version_t seq = m->get_seq();
  MDSMap::mds_info_t info;
  epoch_t effective_epoch = 0;

  // check privileges, ignore if fails
  MonSession *session = m->get_session();
  if (!session)
    goto ignore;
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
  if (!m->get_compat().writeable(fsmap.compat)) {
    dout(1) << " mds " << m->get_source_inst() << " can't write to fsmap " << fsmap.compat << dendl;
    goto ignore;
  }

  // fw to leader?
  if (!mon->is_leader())
    return false;

  // booted, but not in map?
  if (!pending_fsmap.gid_exists(gid)) {
    if (state != MDSMap::STATE_BOOT) {
      dout(7) << "mds_beacon " << *m << " is not in fsmap (state "
              << ceph_mds_state_name(state) << ")" << dendl;

      MDSMap null_map;
      null_map.epoch = fsmap.epoch;
      null_map.compat = fsmap.compat;
      mon->send_reply(op, new MMDSMap(mon->monmap->fsid, &null_map));
      return true;
    } else {
      return false;  // not booted yet.
    }
  }
  dout(10) << __func__ << ": GID exists in map: " << gid << dendl;
  info = pending_fsmap.get_info_gid(gid);

  // old seq?
  if (info.state_seq > seq) {
    dout(7) << "mds_beacon " << *m << " has old seq, ignoring" << dendl;
    goto ignore;
  }

  // Work out the latest epoch that this daemon should have seen
  {
    fs_cluster_id_t fscid = pending_fsmap.mds_roles.at(gid);
    if (fscid == FS_CLUSTER_ID_NONE) {
      effective_epoch = pending_fsmap.standby_epochs.at(gid);
    } else {
      effective_epoch = pending_fsmap.get_filesystem(fscid)->mds_map.epoch;
    }
    if (effective_epoch != m->get_last_epoch_seen()) {
      dout(10) << "mds_beacon " << *m
               << " ignoring requested state, because mds hasn't seen latest map" << dendl;
      goto reply;
    }
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
	 info.state == MDSMap::STATE_STANDBY_REPLAY) && state > 0) {
      dout(10) << "mds_beacon mds can't activate itself (" << ceph_mds_state_name(info.state)
	       << " -> " << ceph_mds_state_name(state) << ")" << dendl;
      goto reply;
    }

    if ((state == MDSMap::STATE_STANDBY || state == MDSMap::STATE_STANDBY_REPLAY)
        && info.rank != MDS_RANK_NONE)
    {
      dout(4) << "mds_beacon MDS can't go back into standby after taking rank: "
                 "held rank " << info.rank << " while requesting state "
              << ceph_mds_state_name(state) << dendl;
      goto reply;
    }
    
    _note_beacon(m);
    return false;
  }

  // Comparing known daemon health with m->get_health()
  // and return false (i.e. require proposal) if they
  // do not match, to update our stored
  if (!(pending_daemon_health[gid] == m->get_health())) {
    dout(20) << __func__ << " health metrics for gid " << gid << " were updated" << dendl;
    _note_beacon(m);
    return false;
  }

 reply:
  // note time and reply
  assert(effective_epoch > 0);
  _note_beacon(m);
  mon->send_reply(op,
		  new MMDSBeacon(mon->monmap->fsid, m->get_global_id(), m->get_name(),
				 effective_epoch, state, seq,
				 CEPH_FEATURES_SUPPORTED_DEFAULT));
  return true;

 ignore:
  // I won't reply this beacon, drop it.
  mon->no_reply(op);
  return true;
}

bool MDSMonitor::preprocess_offload_targets(MonOpRequestRef op)
{
  op->mark_mdsmon_event(__func__);
  MMDSLoadTargets *m = static_cast<MMDSLoadTargets*>(op->get_req());
  dout(10) << "preprocess_offload_targets " << *m << " from " << m->get_orig_source() << dendl;
  
  // check privileges, ignore message if fails
  MonSession *session = m->get_session();
  if (!session)
    goto done;
  if (!session->is_capable("mds", MON_CAP_X)) {
    dout(0) << "preprocess_offload_targets got MMDSLoadTargets from entity with insufficient caps "
	    << session->caps << dendl;
    goto done;
  }

  if (fsmap.gid_exists(m->global_id) &&
      m->targets == fsmap.get_info_gid(m->global_id).export_targets)
    goto done;

  return false;

 done:
  return true;
}


bool MDSMonitor::prepare_update(MonOpRequestRef op)
{
  op->mark_mdsmon_event(__func__);
  PaxosServiceMessage *m = static_cast<PaxosServiceMessage*>(op->get_req());
  dout(7) << "prepare_update " << *m << dendl;

  switch (m->get_type()) {
    
  case MSG_MDS_BEACON:
    return prepare_beacon(op);

  case MSG_MON_COMMAND:
    return prepare_command(op);

  case MSG_MDS_OFFLOAD_TARGETS:
    return prepare_offload_targets(op);
  
  default:
    ceph_abort();
  }

  return true;
}

bool MDSMonitor::prepare_beacon(MonOpRequestRef op)
{
  op->mark_mdsmon_event(__func__);
  MMDSBeacon *m = static_cast<MMDSBeacon*>(op->get_req());
  // -- this is an update --
  dout(12) << "prepare_beacon " << *m << " from " << m->get_orig_source_inst() << dendl;
  entity_addr_t addr = m->get_orig_source_inst().addr;
  mds_gid_t gid = m->get_global_id();
  MDSMap::DaemonState state = m->get_state();
  version_t seq = m->get_seq();

  dout(20) << __func__ << " got health from gid " << gid << " with " << m->get_health().metrics.size() << " metrics." << dendl;

  // Calculate deltas of health metrics created and removed
  // Do this by type rather than MDSHealthMetric equality, because messages can
  // change a lot when they include e.g. a number of items.
  const auto &old_health = pending_daemon_health[gid].metrics;
  const auto &new_health = m->get_health().metrics;

  std::set<mds_metric_t> old_types;
  for (const auto &i : old_health) {
    old_types.insert(i.type);
  }

  std::set<mds_metric_t> new_types;
  for (const auto &i : new_health) {
    new_types.insert(i.type);
  }

  for (const auto &new_metric: new_health) {
    if (old_types.count(new_metric.type) == 0) {
      std::stringstream msg;
      msg << "MDS health message (" << m->get_orig_source_inst().name << "): "
          << new_metric.message;
      if (new_metric.sev == HEALTH_ERR) {
        mon->clog->error() << msg.str();
      } else if (new_metric.sev == HEALTH_WARN) {
        mon->clog->warn() << msg.str();
      } else {
        mon->clog->info() << msg.str();
      }
    }
  }

  // Log the disappearance of health messages at INFO
  for (const auto &old_metric : old_health) {
    if (new_types.count(old_metric.type) == 0) {
      mon->clog->info() << "MDS health message cleared ("
        << m->get_orig_source_inst().name << "): " << old_metric.message;
    }
  }

  // Store health
  pending_daemon_health[gid] = m->get_health();

  // boot?
  if (state == MDSMap::STATE_BOOT) {
    // zap previous instance of this name?
    if (g_conf->mds_enforce_unique_name) {
      bool failed_mds = false;
      while (mds_gid_t existing = pending_fsmap.find_mds_gid_by_name(m->get_name())) {
        if (!mon->osdmon()->is_writeable()) {
          mon->osdmon()->wait_for_writeable(op, new C_RetryMessage(this, op));
          return false;
        }
        const MDSMap::mds_info_t &existing_info =
          pending_fsmap.get_info_gid(existing);
        mon->clog->info() << existing_info.human_name() << " restarted";
	fail_mds_gid(existing);
        failed_mds = true;
      }
      if (failed_mds) {
        assert(mon->osdmon()->is_writeable());
        request_proposal(mon->osdmon());
      }
    }

    // Add this daemon to the map
    if (pending_fsmap.mds_roles.count(gid) == 0) {
      MDSMap::mds_info_t new_info;
      new_info.global_id = gid;
      new_info.name = m->get_name();
      new_info.addr = addr;
      new_info.mds_features = m->get_mds_features();
      new_info.state = MDSMap::STATE_STANDBY;
      new_info.state_seq = seq;
      new_info.standby_for_rank = m->get_standby_for_rank();
      new_info.standby_for_name = m->get_standby_for_name();
      new_info.standby_for_fscid = m->get_standby_for_fscid();
      new_info.standby_replay = m->get_standby_replay();
      pending_fsmap.insert(new_info);
    }

    // Resolve standby_for_name to a rank
    const MDSMap::mds_info_t &info = pending_fsmap.get_info_gid(gid);
    if (!info.standby_for_name.empty()) {
      const MDSMap::mds_info_t *leaderinfo = fsmap.find_by_name(
          info.standby_for_name);
      if (leaderinfo && (leaderinfo->rank >= 0)) {
        auto fscid = pending_fsmap.mds_roles.at(leaderinfo->global_id);
        auto fs = pending_fsmap.get_filesystem(fscid);

        pending_fsmap.modify_daemon(gid, [fscid, leaderinfo](
              MDSMap::mds_info_t *info) {
            info->standby_for_rank = leaderinfo->rank;
            info->standby_for_fscid = fscid;
        });
      }
    }

    // initialize the beacon timer
    last_beacon[gid].stamp = ceph_clock_now();
    last_beacon[gid].seq = seq;

    // new incompat?
    if (!pending_fsmap.compat.writeable(m->get_compat())) {
      dout(10) << " fsmap " << pending_fsmap.compat
               << " can't write to new mds' " << m->get_compat()
	       << ", updating fsmap and killing old mds's"
	       << dendl;
      pending_fsmap.update_compat(m->get_compat());
    }

    update_metadata(m->get_global_id(), m->get_sys_info());
  } else {
    // state update
    const MDSMap::mds_info_t &info = pending_fsmap.get_info_gid(gid);
    // Old MDS daemons don't mention that they're standby replay until
    // after they've sent their boot beacon, so update this field.
    if (info.standby_replay != m->get_standby_replay()) {
      pending_fsmap.modify_daemon(info.global_id, [&m](
            MDSMap::mds_info_t *i)
        {
          i->standby_replay = m->get_standby_replay();
        });
    }

    if (info.state == MDSMap::STATE_STOPPING && state != MDSMap::STATE_STOPPED ) {
      // we can't transition to any other states from STOPPING
      dout(0) << "got beacon for MDS in STATE_STOPPING, ignoring requested state change"
	       << dendl;
      _note_beacon(m);
      return true;
    }

    if (info.laggy()) {
      dout(10) << "prepare_beacon clearing laggy flag on " << addr << dendl;
      pending_fsmap.modify_daemon(info.global_id, [](MDSMap::mds_info_t *info)
        {
          info->clear_laggy();
        }
      );
    }
  
    dout(10) << "prepare_beacon mds." << info.rank
	     << " " << ceph_mds_state_name(info.state)
	     << " -> " << ceph_mds_state_name(state)
	     << "  standby_for_rank=" << m->get_standby_for_rank()
	     << dendl;
    if (state == MDSMap::STATE_STOPPED) {
      const auto fscid = pending_fsmap.mds_roles.at(gid);
      auto fs = pending_fsmap.get_filesystem(fscid);

      mon->clog->info() << info.human_name() << " finished "
                        << "deactivating rank " << info.rank << " in filesystem "
                        << fs->mds_map.fs_name << " (now has "
                        << fs->mds_map.get_num_in_mds() - 1 << " ranks)";

      auto erased = pending_fsmap.stop(gid);
      erased.push_back(gid);

      for (const auto &erased_gid : erased) {
        last_beacon.erase(erased_gid);
        if (pending_daemon_health.count(erased_gid)) {
          pending_daemon_health.erase(erased_gid);
          pending_daemon_health_rm.insert(erased_gid);
        }
      }


    } else if (state == MDSMap::STATE_DAMAGED) {
      if (!mon->osdmon()->is_writeable()) {
        dout(4) << __func__ << ": DAMAGED from rank " << info.rank
                << " waiting for osdmon writeable to blacklist it" << dendl;
        mon->osdmon()->wait_for_writeable(op, new C_RetryMessage(this, op));
        return false;
      }

      // Record this MDS rank as damaged, so that other daemons
      // won't try to run it.
      dout(4) << __func__ << ": marking rank "
              << info.rank << " damaged" << dendl;

      utime_t until = ceph_clock_now();
      until += g_conf->get_val<double>("mon_mds_blacklist_interval");
      const auto blacklist_epoch = mon->osdmon()->blacklist(info.addr, until);
      request_proposal(mon->osdmon());
      pending_fsmap.damaged(gid, blacklist_epoch);
      last_beacon.erase(gid);

      // Respond to MDS, so that it knows it can continue to shut down
      mon->send_reply(op,
		      new MMDSBeacon(
			mon->monmap->fsid, m->get_global_id(),
			m->get_name(), fsmap.get_epoch(), state, seq,
			CEPH_FEATURES_SUPPORTED_DEFAULT));
    } else if (state == MDSMap::STATE_DNE) {
      if (!mon->osdmon()->is_writeable()) {
        dout(4) << __func__ << ": DNE from rank " << info.rank
                << " waiting for osdmon writeable to blacklist it" << dendl;
        mon->osdmon()->wait_for_writeable(op, new C_RetryMessage(this, op));
        return false;
      }

      fail_mds_gid(gid);
      assert(mon->osdmon()->is_writeable());
      request_proposal(mon->osdmon());

      // Respond to MDS, so that it knows it can continue to shut down
      mon->send_reply(op,
		      new MMDSBeacon(
			mon->monmap->fsid, m->get_global_id(),
			m->get_name(), fsmap.get_epoch(), state, seq,
			CEPH_FEATURES_SUPPORTED_DEFAULT));
    } else if (info.state == MDSMap::STATE_STANDBY && state != info.state) {
      // Standby daemons should never modify their own
      // state.  Reject any attempts to do so.
      derr << "standby " << gid << " attempted to change state to "
           << ceph_mds_state_name(state) << ", rejecting" << dendl;
      return true;
    } else if (info.state != MDSMap::STATE_STANDBY && state != info.state &&
               !MDSMap::state_transition_valid(info.state, state)) {
      // Validate state transitions for daemons that hold a rank
      derr << "daemon " << gid << " (rank " << info.rank << ") "
           << "reported invalid state transition "
           << ceph_mds_state_name(info.state) << " -> "
           << ceph_mds_state_name(state) << dendl;
      return true;
    } else {
      if (info.state != MDSMap::STATE_ACTIVE && state == MDSMap::STATE_ACTIVE) {
        auto fscid = pending_fsmap.mds_roles.at(gid);
        auto fs = pending_fsmap.get_filesystem(fscid);
        mon->clog->info() << info.human_name() << " is now active in "
                          << "filesystem " << fs->mds_map.fs_name << " as rank "
                          << info.rank;
      }

      // Made it through special cases and validations, record the
      // daemon's reported state to the FSMap.
      pending_fsmap.modify_daemon(gid, [state, seq](MDSMap::mds_info_t *info) {
        info->state = state;
        info->state_seq = seq;
      });
    }
  }

  dout(7) << "prepare_beacon pending map now:" << dendl;
  print_map(pending_fsmap);
  
  wait_for_finished_proposal(op, new FunctionContext([op, this](int r){
    if (r >= 0)
      _updated(op);   // success
    else if (r == -ECANCELED) {
      mon->no_reply(op);
    } else {
      dispatch(op);        // try again
    }
  }));

  return true;
}

bool MDSMonitor::prepare_offload_targets(MonOpRequestRef op)
{
  op->mark_mdsmon_event(__func__);
  MMDSLoadTargets *m = static_cast<MMDSLoadTargets*>(op->get_req());
  mds_gid_t gid = m->global_id;
  if (pending_fsmap.gid_has_rank(gid)) {
    dout(10) << "prepare_offload_targets " << gid << " " << m->targets << dendl;
    pending_fsmap.update_export_targets(gid, m->targets);
  } else {
    dout(10) << "prepare_offload_targets " << gid << " not in map" << dendl;
  }
  return true;
}

bool MDSMonitor::should_propose(double& delay)
{
  // delegate to PaxosService to assess whether we should propose
  return PaxosService::should_propose(delay);
}

void MDSMonitor::_updated(MonOpRequestRef op)
{
  op->mark_mdsmon_event(__func__);
  MMDSBeacon *m = static_cast<MMDSBeacon*>(op->get_req());
  dout(10) << "_updated " << m->get_orig_source() << " " << *m << dendl;
  mon->clog->debug() << m->get_orig_source_inst() << " "
	  << ceph_mds_state_name(m->get_state());

  if (m->get_state() == MDSMap::STATE_STOPPED) {
    // send the map manually (they're out of the map, so they won't get it automatic)
    MDSMap null_map;
    null_map.epoch = fsmap.epoch;
    null_map.compat = fsmap.compat;
    mon->send_reply(op, new MMDSMap(mon->monmap->fsid, &null_map));
  } else {
    mon->send_reply(op, new MMDSBeacon(mon->monmap->fsid,
				       m->get_global_id(),
				       m->get_name(),
				       fsmap.get_epoch(),
				       m->get_state(),
				       m->get_seq(),
				       CEPH_FEATURES_SUPPORTED_DEFAULT));
  }
}

void MDSMonitor::on_active()
{
  tick();

  if (mon->is_leader()) {
    mon->clog->debug() << "fsmap " << fsmap;
  }
}

void MDSMonitor::dump_info(Formatter *f)
{
  f->open_object_section("fsmap");
  fsmap.dump(f);
  f->close_section();

  f->dump_unsigned("mdsmap_first_committed", get_first_committed());
  f->dump_unsigned("mdsmap_last_committed", get_last_committed());
}

bool MDSMonitor::preprocess_command(MonOpRequestRef op)
{
  op->mark_mdsmon_event(__func__);
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  int r = -1;
  bufferlist rdata;
  stringstream ss, ds;

  cmdmap_t cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    // ss has reason for failure
    string rs = ss.str();
    mon->reply_command(op, -EINVAL, rs, rdata, get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);
  string format;
  cmd_getval(g_ceph_context, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  MonSession *session = m->get_session();
  if (!session) {
    mon->reply_command(op, -EACCES, "access denied", rdata, get_last_committed());
    return true;
  }

  if (prefix == "mds stat") {
    if (f) {
      f->open_object_section("mds_stat");
      dump_info(f.get());
      f->close_section();
      f->flush(ds);
    } else {
      ds << fsmap;
    }
    r = 0;
  } else if (prefix == "fs dump") {
    int64_t epocharg;
    epoch_t epoch;

    FSMap *p = &fsmap;
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
	p = new FSMap;
	p->decode(b);
      }
    }
    if (p) {
      stringstream ds;
      if (f != NULL) {
	f->open_object_section("fsmap");
	p->dump(f.get());
	f->close_section();
	f->flush(ds);
	r = 0;
      } else {
	p->print(ds);
	r = 0;
      }

      rdata.append(ds);
      ss << "dumped fsmap epoch " << p->get_epoch();

      if (p != &fsmap)
	delete p;
    }
  } else if (prefix == "mds metadata") {
    if (!f)
      f.reset(Formatter::create("json-pretty"));

    string who;
    bool all = !cmd_getval(g_ceph_context, cmdmap, "who", who);
    dout(1) << "all = " << all << dendl;
    if (all) {
      r = 0;
      // Dump all MDSs' metadata
      const auto all_info = fsmap.get_mds_info();

      f->open_array_section("mds_metadata");
      for(const auto &i : all_info) {
        const auto &info = i.second;

        f->open_object_section("mds");
        f->dump_string("name", info.name);
        std::ostringstream get_err;
        r = dump_metadata(info.name, f.get(), get_err);
        if (r == -EINVAL || r == -ENOENT) {
          // Drop error, list what metadata we do have
          dout(1) << get_err.str() << dendl;
          r = 0;
        } else if (r != 0) {
          derr << "Unexpected error reading metadata: " << cpp_strerror(r)
               << dendl;
          ss << get_err.str();
          f->close_section();
          break;
        }
        f->close_section();
      }
      f->close_section();
    } else {
      // Dump a single daemon's metadata
      f->open_object_section("mds_metadata");
      r = dump_metadata(who, f.get(), ss);
      f->close_section();
    }
    f->flush(ds);
  } else if (prefix == "mds versions") {
    if (!f)
      f.reset(Formatter::create("json-pretty"));
    count_metadata("ceph_version", f.get());
    f->flush(ds);
    r = 0;
  } else if (prefix == "mds count-metadata") {
    if (!f)
      f.reset(Formatter::create("json-pretty"));
    string field;
    cmd_getval(g_ceph_context, cmdmap, "property", field);
    count_metadata(field, f.get());
    f->flush(ds);
    r = 0;
  } else if (prefix == "mds compat show") {
      if (f) {
	f->open_object_section("mds_compat");
	fsmap.compat.dump(f.get());
	f->close_section();
	f->flush(ds);
      } else {
	ds << fsmap.compat;
      }
      r = 0;
  } else if (prefix == "fs get") {
    string fs_name;
    cmd_getval(g_ceph_context, cmdmap, "fs_name", fs_name);
    auto fs = fsmap.get_filesystem(fs_name);
    if (fs == nullptr) {
      ss << "filesystem '" << fs_name << "' not found";
      r = -ENOENT;
    } else {
      if (f != nullptr) {
        f->open_object_section("filesystem");
        fs->dump(f.get());
        f->close_section();
        f->flush(ds);
        r = 0;
      } else {
        fs->print(ds);
        r = 0;
      }
    }
  } else if (prefix == "fs ls") {
    if (f) {
      f->open_array_section("filesystems");
      {
        for (const auto i : fsmap.filesystems) {
          const auto fs = i.second;
          f->open_object_section("filesystem");
          {
            const MDSMap &mds_map = fs->mds_map;
            f->dump_string("name", mds_map.fs_name);
            /* Output both the names and IDs of pools, for use by
             * humans and machines respectively */
            f->dump_string("metadata_pool", mon->osdmon()->osdmap.get_pool_name(
                  mds_map.metadata_pool));
            f->dump_int("metadata_pool_id", mds_map.metadata_pool);
            f->open_array_section("data_pool_ids");
            {
              for (auto dpi = mds_map.data_pools.begin();
                   dpi != mds_map.data_pools.end(); ++dpi) {
                f->dump_int("data_pool_id", *dpi);
              }
            }
            f->close_section();

            f->open_array_section("data_pools");
            {
                for (auto dpi = mds_map.data_pools.begin();
                   dpi != mds_map.data_pools.end(); ++dpi) {
                  const auto &name = mon->osdmon()->osdmap.get_pool_name(
                      *dpi);
                  f->dump_string("data_pool", name);
                }
            }

            f->close_section();
          }
          f->close_section();
        }
      }
      f->close_section();
      f->flush(ds);
    } else {
      for (const auto i : fsmap.filesystems) {
        const auto fs = i.second;
        const MDSMap &mds_map = fs->mds_map;
        const string &md_pool_name = mon->osdmon()->osdmap.get_pool_name(
            mds_map.metadata_pool);
        
        ds << "name: " << mds_map.fs_name << ", metadata pool: "
           << md_pool_name << ", data pools: [";
        for (auto dpi : mds_map.data_pools) {
          const string &pool_name = mon->osdmon()->osdmap.get_pool_name(dpi);
          ds << pool_name << " ";
        }
        ds << "]" << std::endl;
      }

      if (fsmap.filesystems.empty()) {
        ds << "No filesystems enabled" << std::endl;
      }
    }
    r = 0;
  }

  if (r != -1) {
    rdata.append(ds);
    string rs;
    getline(ss, rs);
    mon->reply_command(op, r, rs, rdata, get_last_committed());
    return true;
  } else
    return false;
}

bool MDSMonitor::fail_mds_gid(mds_gid_t gid)
{
  const MDSMap::mds_info_t info = pending_fsmap.get_info_gid(gid);
  dout(10) << "fail_mds_gid " << gid << " mds." << info.name << " role " << info.rank << dendl;

  epoch_t blacklist_epoch = 0;
  if (info.rank >= 0 && info.state != MDSMap::STATE_STANDBY_REPLAY) {
    utime_t until = ceph_clock_now();
    until += g_conf->get_val<double>("mon_mds_blacklist_interval");
    blacklist_epoch = mon->osdmon()->blacklist(info.addr, until);
  }

  pending_fsmap.erase(gid, blacklist_epoch);
  last_beacon.erase(gid);
  if (pending_daemon_health.count(gid)) {
    pending_daemon_health.erase(gid);
    pending_daemon_health_rm.insert(gid);
  }

  return blacklist_epoch != 0;
}

mds_gid_t MDSMonitor::gid_from_arg(const std::string& arg, std::ostream &ss)
{
  const FSMap *relevant_fsmap = mon->is_leader() ? &pending_fsmap : &fsmap;

  // Try parsing as a role
  mds_role_t role;
  std::ostringstream ignore_err;  // Don't spam 'ss' with parse_role errors
  int r = parse_role(arg, &role, ignore_err);
  if (r == 0) {
    // See if a GID is assigned to this role
    auto fs = relevant_fsmap->get_filesystem(role.fscid);
    assert(fs != nullptr);  // parse_role ensures it exists
    if (fs->mds_map.is_up(role.rank)) {
      dout(10) << __func__ << ": validated rank/GID " << role
               << " as a rank" << dendl;
      return fs->mds_map.get_mds_info(role.rank).global_id;
    }
  }

  // Try parsing as a gid
  std::string err;
  unsigned long long maybe_gid = strict_strtoll(arg.c_str(), 10, &err);
  if (!err.empty()) {
    // Not a role or a GID, try as a daemon name
    const MDSMap::mds_info_t *mds_info = relevant_fsmap->find_by_name(arg);
    if (!mds_info) {
      ss << "MDS named '" << arg
	 << "' does not exist, or is not up";
      return MDS_GID_NONE;
    }
    dout(10) << __func__ << ": resolved MDS name '" << arg
             << "' to GID " << mds_info->global_id << dendl;
    return mds_info->global_id;
  } else {
    // Not a role, but parses as a an integer, might be a GID
    dout(10) << __func__ << ": treating MDS reference '" << arg
	     << "' as an integer " << maybe_gid << dendl;

    if (relevant_fsmap->gid_exists(mds_gid_t(maybe_gid))) {
      return mds_gid_t(maybe_gid);
    }
  }

  dout(1) << __func__ << ": rank/GID " << arg
	  << " not a existent rank or GID" << dendl;
  return MDS_GID_NONE;
}

int MDSMonitor::fail_mds(std::ostream &ss, const std::string &arg,
    MDSMap::mds_info_t *failed_info)
{
  assert(failed_info != nullptr);

  mds_gid_t gid = gid_from_arg(arg, ss);
  if (gid == MDS_GID_NONE) {
    return 0;
  }
  if (!mon->osdmon()->is_writeable()) {
    return -EAGAIN;
  }

  // Take a copy of the info before removing the MDS from the map,
  // so that the caller knows which mds (if any) they ended up removing.
  *failed_info = pending_fsmap.get_info_gid(gid);

  fail_mds_gid(gid);
  ss << "failed mds gid " << gid;
  assert(mon->osdmon()->is_writeable());
  request_proposal(mon->osdmon());
  return 0;
}

bool MDSMonitor::prepare_command(MonOpRequestRef op)
{
  op->mark_mdsmon_event(__func__);
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  int r = -EINVAL;
  stringstream ss;
  bufferlist rdata;

  cmdmap_t cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    string rs = ss.str();
    mon->reply_command(op, -EINVAL, rs, rdata, get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  /* Refuse access if message not associated with a valid session */
  MonSession *session = m->get_session();
  if (!session) {
    mon->reply_command(op, -EACCES, "access denied", rdata, get_last_committed());
    return true;
  }

  bool batched_propose = false;
  for (auto h : handlers) {
    if (h->can_handle(prefix)) {
      batched_propose = h->batched_propose();
      if (batched_propose) {
        paxos->plug();
      }
      r = h->handle(mon, pending_fsmap, op, cmdmap, ss);
      if (batched_propose) {
        paxos->unplug();
      }

      if (r == -EAGAIN) {
        // message has been enqueued for retry; return.
        dout(4) << __func__ << " enqueue for retry by prepare_command" << dendl;
        return false;
      } else {
        if (r == 0) {
          // On successful updates, print the updated map
          print_map(pending_fsmap);
        }
        // Successful or not, we're done: respond.
        goto out;
      }
    }
  }

  r = filesystem_command(op, prefix, cmdmap, ss);
  if (r >= 0) {
    goto out;
  } else if (r == -EAGAIN) {
    // Do not reply, the message has been enqueued for retry
    dout(4) << __func__ << " enqueue for retry by filesystem_command" << dendl;
    return false;
  } else if (r != -ENOSYS) {
    goto out;
  }

  // Only handle legacy commands if there is a filesystem configured
  if (pending_fsmap.legacy_client_fscid == FS_CLUSTER_ID_NONE) {
    if (pending_fsmap.filesystems.size() == 0) {
      ss << "No filesystem configured: use `ceph fs new` to create a filesystem";
    } else {
      ss << "No filesystem set for use with legacy commands";
    }
    r = -EINVAL;
    goto out;
  }

  if (r == -ENOSYS && ss.str().empty()) {
    ss << "unrecognized command";
  }

out:
  dout(4) << __func__ << " done, r=" << r << dendl;
  /* Compose response */
  string rs;
  getline(ss, rs);

  if (r >= 0) {
    // success.. delay reply
    wait_for_finished_proposal(op, new Monitor::C_Command(mon, op, r, rs,
					      get_last_committed() + 1));
    if (batched_propose) {
      force_immediate_propose();
    }
    return true;
  } else {
    // reply immediately
    mon->reply_command(op, r, rs, rdata, get_last_committed());
    return false;
  }
}


/**
 * Given one of the following forms:
 *   <fs name>:<rank>
 *   <fs id>:<rank>
 *   <rank>
 *
 * Parse into a mds_role_t.  The rank-only form is only valid
 * if legacy_client_ns is set.
 */
int MDSMonitor::parse_role(
    const std::string &role_str,
    mds_role_t *role,
    std::ostream &ss)
{
  const FSMap *relevant_fsmap = &fsmap;
  if (mon->is_leader()) {
    relevant_fsmap = &pending_fsmap;
  }
  return relevant_fsmap->parse_role(role_str, role, ss);
}

int MDSMonitor::filesystem_command(
    MonOpRequestRef op,
    std::string const &prefix,
    const cmdmap_t& cmdmap,
    std::stringstream &ss)
{
  dout(4) << __func__ << " prefix='" << prefix << "'" << dendl;
  op->mark_mdsmon_event(__func__);
  int r = 0;
  string whostr;
  cmd_getval(g_ceph_context, cmdmap, "role", whostr);

  if (prefix == "mds deactivate") {
    mds_role_t role;
    r = parse_role(whostr, &role, ss);
    if (r < 0 ) {
      return r;
    }
    auto fs = pending_fsmap.get_filesystem(role.fscid);

    if (!fs->mds_map.is_active(role.rank)) {
      r = -EEXIST;
      ss << "mds." << role << " not active (" 
	 << ceph_mds_state_name(fs->mds_map.get_state(role.rank)) << ")";
    } else if (fs->mds_map.get_root() == role.rank ||
		fs->mds_map.get_tableserver() == role.rank) {
      r = -EINVAL;
      ss << "can't tell the root (" << fs->mds_map.get_root()
	 << ") or tableserver (" << fs->mds_map.get_tableserver()
	 << ") to deactivate";
    } else if (role.rank != fs->mds_map.get_last_in_mds()) {
      r = -EINVAL;
      ss << "mds." << role << " doesn't have the max rank ("
	 << fs->mds_map.get_last_in_mds() << ")";
    } else if (fs->mds_map.get_num_in_mds() <= size_t(fs->mds_map.get_max_mds())) {
      r = -EBUSY;
      ss << "must decrease max_mds or else MDS will immediately reactivate";
    } else {
      r = 0;
      mds_gid_t gid = fs->mds_map.up.at(role.rank);
      ss << "telling mds." << role << " "
         << pending_fsmap.get_info_gid(gid).addr << " to deactivate";

      pending_fsmap.modify_daemon(gid, [](MDSMap::mds_info_t *info) {
        info->state = MDSMap::STATE_STOPPING;
      });
    }
  } else if (prefix == "mds set_state") {
    mds_gid_t gid;
    if (!cmd_getval(g_ceph_context, cmdmap, "gid", gid)) {
      ss << "error parsing 'gid' value '"
         << cmd_vartype_stringify(cmdmap.at("gid")) << "'";
      return -EINVAL;
    }
    MDSMap::DaemonState state;
    if (!cmd_getval(g_ceph_context, cmdmap, "state", state)) {
      ss << "error parsing 'state' string value '"
         << cmd_vartype_stringify(cmdmap.at("state")) << "'";
      return -EINVAL;
    }
    if (pending_fsmap.gid_exists(gid)) {
      pending_fsmap.modify_daemon(gid, [state](MDSMap::mds_info_t *info) {
        info->state = state;
      });
      ss << "set mds gid " << gid << " to state " << state << " "
         << ceph_mds_state_name(state);
      return 0;
    }
  } else if (prefix == "mds fail") {
    string who;
    cmd_getval(g_ceph_context, cmdmap, "role_or_gid", who);

    MDSMap::mds_info_t failed_info;
    r = fail_mds(ss, who, &failed_info);
    if (r < 0 && r == -EAGAIN) {
      mon->osdmon()->wait_for_writeable(op, new C_RetryMessage(this, op));
      return -EAGAIN; // don't propose yet; wait for message to be retried
    } else if (r == 0) {
      // Only log if we really did something (not when was already gone)
      if (failed_info.global_id != MDS_GID_NONE) {
        mon->clog->info() << failed_info.human_name() << " marked failed by "
                          << op->get_session()->entity_name;
      }
    }
  } else if (prefix == "mds rm") {
    mds_gid_t gid;
    if (!cmd_getval(g_ceph_context, cmdmap, "gid", gid)) {
      ss << "error parsing 'gid' value '"
         << cmd_vartype_stringify(cmdmap.at("gid")) << "'";
      return -EINVAL;
    }
    if (!pending_fsmap.gid_exists(gid)) {
      ss << "mds gid " << gid << " dne";
      r = 0;
    } else {
      MDSMap::DaemonState state = pending_fsmap.get_info_gid(gid).state;
      if (state > 0) {
        ss << "cannot remove active mds." << pending_fsmap.get_info_gid(gid).name
           << " rank " << pending_fsmap.get_info_gid(gid).rank;
        return -EBUSY;
      } else {
        pending_fsmap.erase(gid, {});
        ss << "removed mds gid " << gid;
        return 0;
      }
    }
  } else if (prefix == "mds rmfailed") {
    string confirm;
    if (!cmd_getval(g_ceph_context, cmdmap, "confirm", confirm) ||
       confirm != "--yes-i-really-mean-it") {
         ss << "WARNING: this can make your filesystem inaccessible! "
               "Add --yes-i-really-mean-it if you are sure you wish to continue.";
         return -EPERM;
    }
    
    std::string role_str;
    cmd_getval(g_ceph_context, cmdmap, "role", role_str);
    mds_role_t role;
    int r = parse_role(role_str, &role, ss);
    if (r < 0) {
      ss << "invalid role '" << role_str << "'";
      return -EINVAL;
    }

    pending_fsmap.modify_filesystem(
        role.fscid,
        [role](std::shared_ptr<Filesystem> fs)
    {
      fs->mds_map.failed.erase(role.rank);
    });

    ss << "removed failed mds." << role;
    return 0;
  } else if (prefix == "mds compat rm_compat") {
    int64_t f;
    if (!cmd_getval(g_ceph_context, cmdmap, "feature", f)) {
      ss << "error parsing feature value '"
         << cmd_vartype_stringify(cmdmap.at("feature")) << "'";
      return -EINVAL;
    }
    if (pending_fsmap.compat.compat.contains(f)) {
      ss << "removing compat feature " << f;
      CompatSet modified = pending_fsmap.compat;
      modified.compat.remove(f);
      pending_fsmap.update_compat(modified);
    } else {
      ss << "compat feature " << f << " not present in " << pending_fsmap.compat;
    }
    r = 0;
  } else if (prefix == "mds compat rm_incompat") {
    int64_t f;
    if (!cmd_getval(g_ceph_context, cmdmap, "feature", f)) {
      ss << "error parsing feature value '"
         << cmd_vartype_stringify(cmdmap.at("feature")) << "'";
      return -EINVAL;
    }
    if (pending_fsmap.compat.incompat.contains(f)) {
      ss << "removing incompat feature " << f;
      CompatSet modified = pending_fsmap.compat;
      modified.incompat.remove(f);
      pending_fsmap.update_compat(modified);
    } else {
      ss << "incompat feature " << f << " not present in " << pending_fsmap.compat;
    }
    r = 0;
  } else if (prefix == "mds repaired") {
    std::string role_str;
    cmd_getval(g_ceph_context, cmdmap, "role", role_str);
    mds_role_t role;
    r = parse_role(role_str, &role, ss);
    if (r < 0) {
      return r;
    }

    bool modified = pending_fsmap.undamaged(role.fscid, role.rank);
    if (modified) {
      dout(4) << "repaired: restoring rank " << role << dendl;
    } else {
      dout(4) << "repaired: no-op on rank " << role << dendl;
    }

    r = 0;
  } else {
    return -ENOSYS;
  }

  return r;
}

void MDSMonitor::check_subs()
{
  std::list<std::string> types;

  // Subscriptions may be to "mdsmap" (MDS and legacy clients),
  // "mdsmap.<namespace>", or to "fsmap" for the full state of all
  // filesystems.  Build a list of all the types we service
  // subscriptions for.
  types.push_back("fsmap");
  types.push_back("fsmap.user");
  types.push_back("mdsmap");
  for (const auto &i : fsmap.filesystems) {
    auto fscid = i.first;
    std::ostringstream oss;
    oss << "mdsmap." << fscid;
    types.push_back(oss.str());
  }

  for (const auto &type : types) {
    if (mon->session_map.subs.count(type) == 0)
      continue;
    xlist<Subscription*>::iterator p = mon->session_map.subs[type]->begin();
    while (!p.end()) {
      Subscription *sub = *p;
      ++p;
      check_sub(sub);
    }
  }
}


void MDSMonitor::check_sub(Subscription *sub)
{
  dout(20) << __func__ << ": " << sub->type << dendl;

  if (sub->type == "fsmap") {
    if (sub->next <= fsmap.get_epoch()) {
      sub->session->con->send_message(new MFSMap(mon->monmap->fsid, fsmap));
      if (sub->onetime) {
        mon->session_map.remove_sub(sub);
      } else {
        sub->next = fsmap.get_epoch() + 1;
      }
    }
  } else if (sub->type == "fsmap.user") {
    if (sub->next <= fsmap.get_epoch()) {
      FSMapUser fsmap_u;
      fsmap_u.epoch = fsmap.get_epoch();
      fsmap_u.legacy_client_fscid = fsmap.legacy_client_fscid;
      for (auto p = fsmap.filesystems.begin();
	   p != fsmap.filesystems.end();
	   ++p) {
	FSMapUser::fs_info_t& fs_info = fsmap_u.filesystems[p->first];
	fs_info.cid = p->first;
	fs_info.name= p->second->mds_map.fs_name;
      }
      sub->session->con->send_message(new MFSMapUser(mon->monmap->fsid, fsmap_u));
      if (sub->onetime) {
	mon->session_map.remove_sub(sub);
      } else {
	sub->next = fsmap.get_epoch() + 1;
      }
    }
  } else if (sub->type.compare(0, 6, "mdsmap") == 0) {
    if (sub->next > fsmap.get_epoch()) {
      return;
    }

    const bool is_mds = sub->session->inst.name.is_mds();
    mds_gid_t mds_gid = MDS_GID_NONE;
    fs_cluster_id_t fscid = FS_CLUSTER_ID_NONE;
    if (is_mds) {
      // What (if any) namespace are you assigned to?
      auto mds_info = fsmap.get_mds_info();
      for (const auto &i : mds_info) {
        if (i.second.addr == sub->session->inst.addr) {
          mds_gid = i.first;
          fscid = fsmap.mds_roles.at(mds_gid);
        }
      }
    } else {
      // You're a client.  Did you request a particular
      // namespace?
      if (sub->type.compare(0, 7, "mdsmap.") == 0) {
        auto namespace_id_str = sub->type.substr(std::string("mdsmap.").size());
        dout(10) << __func__ << ": namespace_id " << namespace_id_str << dendl;
        std::string err;
        fscid = strict_strtoll(namespace_id_str.c_str(), 10, &err);
        if (!err.empty()) {
          // Client asked for a non-existent namespace, send them nothing
          dout(1) << "Invalid client subscription '" << sub->type
                  << "'" << dendl;
          return;
        }
        if (fsmap.filesystems.count(fscid) == 0) {
          // Client asked for a non-existent namespace, send them nothing
          // TODO: something more graceful for when a client has a filesystem
          // mounted, and the fileysstem is deleted.  Add a "shut down you fool"
          // flag to MMDSMap?
          dout(1) << "Client subscribed to non-existent namespace '" <<
                  fscid << "'" << dendl;
          return;
        }
      } else {
        // Unqualified request for "mdsmap": give it the one marked
        // for use by legacy clients.
        if (fsmap.legacy_client_fscid != FS_CLUSTER_ID_NONE) {
          fscid = fsmap.legacy_client_fscid;
        } else {
          dout(1) << "Client subscribed for legacy filesystem but "
                     "none is configured" << dendl;
          return;
        }
      }
    }
    dout(10) << __func__ << ": is_mds=" << is_mds << ", fscid= " << fscid << dendl;

    // Work out the effective latest epoch
    MDSMap *mds_map = nullptr;
    MDSMap null_map;
    null_map.compat = fsmap.compat;
    if (fscid == FS_CLUSTER_ID_NONE) {
      // For a client, we should have already dropped out
      assert(is_mds);

      if (fsmap.standby_daemons.count(mds_gid)) {
        // For an MDS, we need to feed it an MDSMap with its own state in
        null_map.mds_info[mds_gid] = fsmap.standby_daemons[mds_gid];
        null_map.epoch = fsmap.standby_epochs[mds_gid];
      } else {
        null_map.epoch = fsmap.epoch;
      }
      mds_map = &null_map;
    } else {
      // Check the effective epoch 
      mds_map = &(fsmap.filesystems.at(fscid)->mds_map);
    }

    assert(mds_map != nullptr);
    dout(10) << __func__ << " selected MDS map epoch " <<
      mds_map->epoch << " for namespace " << fscid << " for subscriber "
      << sub->session->inst.name << " who wants epoch " << sub->next << dendl;

    if (sub->next > mds_map->epoch) {
      return;
    }
    auto msg = new MMDSMap(mon->monmap->fsid, mds_map);

    sub->session->con->send_message(msg);
    if (sub->onetime) {
      mon->session_map.remove_sub(sub);
    } else {
      sub->next = mds_map->get_epoch() + 1;
    }
  }
}


void MDSMonitor::update_metadata(mds_gid_t gid,
				 const map<string, string>& metadata)
{
  if (metadata.empty()) {
    return;
  }
  pending_metadata[gid] = metadata;

  MonitorDBStore::TransactionRef t = paxos->get_pending_transaction();
  bufferlist bl;
  encode(pending_metadata, bl);
  t->put(MDS_METADATA_PREFIX, "last_metadata", bl);
  paxos->trigger_propose();
}

void MDSMonitor::remove_from_metadata(MonitorDBStore::TransactionRef t)
{
  bool update = false;
  for (map<mds_gid_t, Metadata>::iterator i = pending_metadata.begin();
       i != pending_metadata.end(); ) {
    if (!pending_fsmap.gid_exists(i->first)) {
      pending_metadata.erase(i++);
      update = true;
    } else {
      ++i;
    }
  }
  if (!update)
    return;
  bufferlist bl;
  encode(pending_metadata, bl);
  t->put(MDS_METADATA_PREFIX, "last_metadata", bl);
}

int MDSMonitor::load_metadata(map<mds_gid_t, Metadata>& m)
{
  bufferlist bl;
  int r = mon->store->get(MDS_METADATA_PREFIX, "last_metadata", bl);
  if (r) {
    dout(1) << "Unable to load 'last_metadata'" << dendl;
    return r;
  }

  bufferlist::iterator it = bl.begin();
  decode(m, it);
  return 0;
}

void MDSMonitor::count_metadata(const string& field, map<string,int> *out)
{
  map<mds_gid_t,Metadata> meta;
  load_metadata(meta);
  for (auto& p : meta) {
    auto q = p.second.find(field);
    if (q == p.second.end()) {
      (*out)["unknown"]++;
    } else {
      (*out)[q->second]++;
    }
  }
}

void MDSMonitor::count_metadata(const string& field, Formatter *f)
{
  map<string,int> by_val;
  count_metadata(field, &by_val);
  f->open_object_section(field.c_str());
  for (auto& p : by_val) {
    f->dump_int(p.first.c_str(), p.second);
  }
  f->close_section();
}

int MDSMonitor::dump_metadata(const std::string &who, Formatter *f, ostream& err)
{
  assert(f);

  mds_gid_t gid = gid_from_arg(who, err);
  if (gid == MDS_GID_NONE) {
    return -EINVAL;
  }

  map<mds_gid_t, Metadata> metadata;
  if (int r = load_metadata(metadata)) {
    err << "Unable to load 'last_metadata'";
    return r;
  }

  if (!metadata.count(gid)) {
    return -ENOENT;
  }
  const Metadata& m = metadata[gid];
  for (Metadata::const_iterator p = m.begin(); p != m.end(); ++p) {
    f->dump_string(p->first.c_str(), p->second);
  }
  return 0;
}

int MDSMonitor::print_nodes(Formatter *f)
{
  assert(f);

  map<mds_gid_t, Metadata> metadata;
  if (int r = load_metadata(metadata)) {
    return r;
  }

  map<string, list<int> > mdses; // hostname => rank
  for (map<mds_gid_t, Metadata>::iterator it = metadata.begin();
       it != metadata.end(); ++it) {
    const Metadata& m = it->second;
    Metadata::const_iterator hostname = m.find("hostname");
    if (hostname == m.end()) {
      // not likely though
      continue;
    }
    const mds_gid_t gid = it->first;
    if (!fsmap.gid_exists(gid)) {
      dout(5) << __func__ << ": GID " << gid << " not existent" << dendl;
      continue;
    }
    const MDSMap::mds_info_t& mds_info = fsmap.get_info_gid(gid);
    // FIXME: include filesystem name with rank here
    mdses[hostname->second].push_back(mds_info.rank);
  }

  dump_services(f, mdses, "mds");
  return 0;
}

/**
 * If a cluster is undersized (with respect to max_mds), then
 * attempt to find daemons to grow it.
 */
bool MDSMonitor::maybe_expand_cluster(std::shared_ptr<Filesystem> fs)
{
  bool do_propose = false;

  if (fs->mds_map.test_flag(CEPH_MDSMAP_DOWN)) {
    return do_propose;
  }

  while (fs->mds_map.get_num_in_mds() < size_t(fs->mds_map.get_max_mds()) &&
	 !fs->mds_map.is_degraded()) {
    mds_rank_t mds = mds_rank_t(0);
    string name;
    while (fs->mds_map.is_in(mds)) {
      mds++;
    }
    mds_gid_t newgid = pending_fsmap.find_replacement_for({fs->fscid, mds},
                         name, g_conf->mon_force_standby_active);
    if (newgid == MDS_GID_NONE) {
      break;
    }

    const auto &new_info = pending_fsmap.get_info_gid(newgid);
    dout(1) << "assigned standby " << new_info.addr
            << " as mds." << mds << dendl;

    mon->clog->info() << new_info.human_name() << " assigned to "
                         "filesystem " << fs->mds_map.fs_name << " as rank "
                      << mds << " (now has " << fs->mds_map.get_num_in_mds() + 1
                      << " ranks)";
    pending_fsmap.promote(newgid, fs, mds);
    do_propose = true;
  }

  return do_propose;
}


/**
 * If a daemon is laggy, and a suitable replacement
 * is available, fail this daemon (remove from map) and pass its
 * role to another daemon.
 */
void MDSMonitor::maybe_replace_gid(mds_gid_t gid, const MDSMap::mds_info_t& info,
    bool *mds_propose, bool *osd_propose)
{
  assert(mds_propose != nullptr);
  assert(osd_propose != nullptr);

  const auto fscid = pending_fsmap.mds_roles.at(gid);

  // We will only take decisive action (replacing/removing a daemon)
  // if we have some indicating that some other daemon(s) are successfully
  // getting beacons through recently.
  utime_t latest_beacon;
  for (const auto & i : last_beacon) {
    latest_beacon = std::max(i.second.stamp, latest_beacon);
  }
  const bool may_replace = latest_beacon >
    (ceph_clock_now() -
     std::max(g_conf->mds_beacon_interval, g_conf->mds_beacon_grace * 0.5));

  // are we in?
  // and is there a non-laggy standby that can take over for us?
  mds_gid_t sgid;
  if (info.rank >= 0 &&
      info.state != MDSMap::STATE_STANDBY &&
      info.state != MDSMap::STATE_STANDBY_REPLAY &&
      may_replace &&
      !pending_fsmap.get_filesystem(fscid)->mds_map.test_flag(CEPH_MDSMAP_DOWN) &&
      (sgid = pending_fsmap.find_replacement_for({fscid, info.rank}, info.name,
                g_conf->mon_force_standby_active)) != MDS_GID_NONE)
  {
    
    MDSMap::mds_info_t si = pending_fsmap.get_info_gid(sgid);
    dout(10) << " replacing " << gid << " " << info.addr << " mds."
      << info.rank << "." << info.inc
      << " " << ceph_mds_state_name(info.state)
      << " with " << sgid << "/" << si.name << " " << si.addr << dendl;

    mon->clog->warn() << info.human_name() 
                      << " is not responding, replacing it "
                      << "as rank " << info.rank
                      << " with standby " << si.human_name();

    // Remember what NS the old one was in
    const fs_cluster_id_t fscid = pending_fsmap.mds_roles.at(gid);

    // Remove the old one
    *osd_propose |= fail_mds_gid(gid);

    // Promote the replacement
    auto fs = pending_fsmap.filesystems.at(fscid);
    pending_fsmap.promote(sgid, fs, info.rank);

    *mds_propose = true;
  } else if ((info.state == MDSMap::STATE_STANDBY_REPLAY ||
             info.state == MDSMap::STATE_STANDBY) && may_replace) {
    dout(10) << " failing and removing " << gid << " " << info.addr << " mds." << info.rank 
      << "." << info.inc << " " << ceph_mds_state_name(info.state)
      << dendl;
    mon->clog->info() << "Standby " << info.human_name() << " is not "
                         "responding, dropping it";
    fail_mds_gid(gid);
    *mds_propose = true;
  } else if (!info.laggy()) {
      dout(10) << " marking " << gid << " " << info.addr << " mds." << info.rank << "." << info.inc
        << " " << ceph_mds_state_name(info.state)
        << " laggy" << dendl;
      pending_fsmap.modify_daemon(info.global_id, [](MDSMap::mds_info_t *info) {
          info->laggy_since = ceph_clock_now();
      });
      *mds_propose = true;
  }
}

bool MDSMonitor::maybe_promote_standby(std::shared_ptr<Filesystem> fs)
{
  assert(!fs->mds_map.test_flag(CEPH_MDSMAP_DOWN));

  bool do_propose = false;

  // have a standby take over?
  set<mds_rank_t> failed;
  fs->mds_map.get_failed_mds_set(failed);
  if (!failed.empty()) {
    set<mds_rank_t>::iterator p = failed.begin();
    while (p != failed.end()) {
      mds_rank_t f = *p++;
      mds_gid_t sgid = pending_fsmap.find_replacement_for({fs->fscid, f}, {},
          g_conf->mon_force_standby_active);
      if (sgid) {
        const MDSMap::mds_info_t si = pending_fsmap.get_info_gid(sgid);
        dout(0) << " taking over failed mds." << f << " with " << sgid
                << "/" << si.name << " " << si.addr << dendl;
        mon->clog->info() << "Standby " << si.human_name()
                          << " assigned to filesystem " << fs->mds_map.fs_name
                          << " as rank " << f;

        pending_fsmap.promote(sgid, fs, f);
	do_propose = true;
      }
    }
  } else {
    // There were no failures to replace, so try using any available standbys
    // as standby-replay daemons.

    // Take a copy of the standby GIDs so that we can iterate over
    // them while perhaps-modifying standby_daemons during the loop
    // (if we promote anyone they are removed from standby_daemons)
    std::vector<mds_gid_t> standby_gids;
    for (const auto &j : pending_fsmap.standby_daemons) {
      standby_gids.push_back(j.first);
    }

    for (const auto &gid : standby_gids) {
      const auto &info = pending_fsmap.standby_daemons.at(gid);
      assert(info.state == MDSMap::STATE_STANDBY);

      if (!info.standby_replay) {
        continue;
      }

      /*
       * This mds is standby but has no rank assigned.
       * See if we can find it somebody to shadow
       */
      dout(20) << "gid " << gid << " is standby and following nobody" << dendl;
      
      // standby for someone specific?
      if (info.standby_for_rank >= 0) {
        // The mds_info_t may or may not tell us exactly which filesystem
        // the standby_for_rank refers to: lookup via legacy_client_fscid
        mds_role_t target_role = {
          info.standby_for_fscid == FS_CLUSTER_ID_NONE ?
            pending_fsmap.legacy_client_fscid : info.standby_for_fscid,
          info.standby_for_rank};

        // It is possible that the map contains a standby_for_fscid
        // that doesn't correspond to an existing filesystem, especially
        // if we loaded from a version with a bug (#17466)
        if (info.standby_for_fscid != FS_CLUSTER_ID_NONE
            && !pending_fsmap.filesystem_exists(info.standby_for_fscid)) {
          derr << "gid " << gid << " has invalid standby_for_fscid "
               << info.standby_for_fscid << dendl;
          continue;
        }

        // If we managed to resolve a full target role
        if (target_role.fscid != FS_CLUSTER_ID_NONE) {
          auto fs = pending_fsmap.get_filesystem(target_role.fscid);
          if (fs->mds_map.is_followable(target_role.rank)) {
            do_propose |= try_standby_replay(
                info,
                *fs,
                fs->mds_map.get_info(target_role.rank));
          }
        }

	continue;
      }

      // check everyone
      for (auto fs_i : pending_fsmap.filesystems) {
        const MDSMap &mds_map = fs_i.second->mds_map;
        for (auto mds_i : mds_map.mds_info) {
          MDSMap::mds_info_t &cand_info = mds_i.second;
          if (cand_info.rank >= 0 && mds_map.is_followable(cand_info.rank)) {
            if ((info.standby_for_name.length() && info.standby_for_name != cand_info.name) ||
                info.standby_for_rank != MDS_RANK_NONE) {
              continue;   // we're supposed to follow someone else
            }

            if (try_standby_replay(info, *(fs_i.second), cand_info)) {
              do_propose = true;
              break;
            }
            continue;
          }
        }
      }
    }
  }

  return do_propose;
}

void MDSMonitor::tick()
{
  // make sure mds's are still alive
  // ...if i am an active leader
  if (!is_active()) return;

  dout(10) << fsmap << dendl;

  bool do_propose = false;

  if (!mon->is_leader()) return;

  do_propose |= pending_fsmap.check_health();

  // expand mds cluster (add new nodes to @in)?
  for (auto i : pending_fsmap.filesystems) {
    do_propose |= maybe_expand_cluster(i.second);
  }

  const auto now = ceph_clock_now();
  if (last_tick.is_zero()) {
    last_tick = now;
  }

  if (now - last_tick > (g_conf->mds_beacon_grace - g_conf->mds_beacon_interval)) {
    // This case handles either local slowness (calls being delayed
    // for whatever reason) or cluster election slowness (a long gap
    // between calls while an election happened)
    dout(4) << __func__ << ": resetting beacon timeouts due to mon delay "
            "(slow election?) of " << now - last_tick << " seconds" << dendl;
    for (auto &i : last_beacon) {
      i.second.stamp = now;
    }
  }

  last_tick = now;

  // check beacon timestamps
  utime_t cutoff = now;
  cutoff -= g_conf->mds_beacon_grace;

  // make sure last_beacon is fully populated
  for (const auto &p : pending_fsmap.mds_roles) {
    auto &gid = p.first;
    if (last_beacon.count(gid) == 0) {
      last_beacon[gid].stamp = now;
      last_beacon[gid].seq = 0;
    }
  }

  bool propose_osdmap = false;
  bool osdmap_writeable = mon->osdmon()->is_writeable();
  auto p = last_beacon.begin();
  while (p != last_beacon.end()) {
    mds_gid_t gid = p->first;
    auto beacon_info = p->second;
    ++p;

    if (!pending_fsmap.gid_exists(gid)) {
      // clean it out
      last_beacon.erase(gid);
      continue;
    }

    if (beacon_info.stamp < cutoff) {
      auto &info = pending_fsmap.get_info_gid(gid);
      dout(1) << "no beacon from mds." << info.rank << "." << info.inc
              << " (gid: " << gid << " addr: " << info.addr
              << " state: " << ceph_mds_state_name(info.state) << ")"
              << " since " << beacon_info.stamp << dendl;
      // If the OSDMap is writeable, we can blacklist things, so we can
      // try failing any laggy MDS daemons.  Consider each one for failure.
      if (osdmap_writeable) {
        maybe_replace_gid(gid, info, &do_propose, &propose_osdmap);
      }
    }
  }
  if (propose_osdmap) {
    request_proposal(mon->osdmon());
  }

  for (auto i : pending_fsmap.filesystems) {
    auto fs = i.second;
    if (!fs->mds_map.test_flag(CEPH_MDSMAP_DOWN)) {
      do_propose |= maybe_promote_standby(fs);
    }
  }

  if (do_propose) {
    propose_pending();
  }
}

/**
 * finfo: the would-be follower
 * leader_fs: the Filesystem containing the would-be leader
 * ainfo: the would-be leader
 */
bool MDSMonitor::try_standby_replay(
    const MDSMap::mds_info_t& finfo,
    const Filesystem &leader_fs,
    const MDSMap::mds_info_t& ainfo)
{
  // someone else already following?
  if (leader_fs.has_standby_replay(ainfo.global_id)) {
    dout(20) << " mds." << ainfo.rank << " already has a follower" << dendl;
    return false;
  } else {
    // Assign the new role to the standby
    dout(10) << "  setting to follow mds rank " << ainfo.rank << dendl;
    pending_fsmap.assign_standby_replay(finfo.global_id, leader_fs.fscid, ainfo.rank);
    return true;
  }
}

MDSMonitor::MDSMonitor(Monitor *mn, Paxos *p, string service_name)
  : PaxosService(mn, p, service_name)
{
  handlers = FileSystemCommandHandler::load(p);
}

void MDSMonitor::on_restart()
{
  // Clear out the leader-specific state.
  last_tick = utime_t();
  last_beacon.clear();
}

