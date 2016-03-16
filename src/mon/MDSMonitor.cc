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
#include <boost/utility.hpp>

#include "MDSMonitor.h"
#include "Monitor.h"
#include "MonitorDBStore.h"
#include "OSDMonitor.h"

#include "common/strtol.h"
#include "common/ceph_argparse.h"
#include "common/perf_counters.h"
#include "common/Timer.h"
#include "common/config.h"
#include "common/cmdparse.h"

#include "messages/MMDSMap.h"
#include "messages/MFSMap.h"
#include "messages/MMDSBeacon.h"
#include "messages/MMDSLoadTargets.h"
#include "messages/MMonCommand.h"
#include "messages/MGenericMessage.h"

#include "include/assert.h"
#include "include/str_list.h"

#include "mds/mdstypes.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, fsmap)
static ostream& _prefix(std::ostream *_dout, Monitor *mon, FSMap const& fsmap) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
		<< "(" << mon->get_state_name()
		<< ").mds e" << fsmap.get_epoch() << " ";
}

/*
 * Specialized implementation of cmd_getval to allow us to parse
 * out strongly-typedef'd types
 */
template<> bool cmd_getval(CephContext *cct, const cmdmap_t& cmdmap,
                std::string k, mds_gid_t &val)
{
  return cmd_getval(cct, cmdmap, k, (int64_t&)val);
}

template<> bool cmd_getval(CephContext *cct, const cmdmap_t& cmdmap,
                std::string k, mds_rank_t &val)
{
  return cmd_getval(cct, cmdmap, k, (int64_t&)val);
}

template<> bool cmd_getval(CephContext *cct, const cmdmap_t& cmdmap,
                std::string k, MDSMap::DaemonState &val)
{
  return cmd_getval(cct, cmdmap, k, (int64_t&)val);
}

static const string MDS_METADATA_PREFIX("mds_metadata");


// my methods

void MDSMonitor::print_map(FSMap &m, int dbl)
{
  dout(dbl) << "print_map\n";
  m.print(*_dout);
  *_dout << dendl;
}

void MDSMonitor::create_new_fs(FSMap &fsm, const std::string &name,
    int metadata_pool, int data_pool)
{
  auto fs = std::make_shared<Filesystem>();
  fs->mds_map.fs_name = name;
  fs->mds_map.max_mds = g_conf->max_mds;
  fs->mds_map.data_pools.insert(data_pool);
  fs->mds_map.metadata_pool = metadata_pool;
  fs->mds_map.cas_pool = -1;
  fs->mds_map.max_file_size = g_conf->mds_max_file_size;
  fs->mds_map.compat = fsm.compat;
  fs->mds_map.created = ceph_clock_now(g_ceph_context);
  fs->mds_map.modified = ceph_clock_now(g_ceph_context);
  fs->mds_map.session_timeout = g_conf->mds_session_timeout;
  fs->mds_map.session_autoclose = g_conf->mds_session_autoclose;
  fs->mds_map.enabled = true;
  fs->fscid = fsm.next_filesystem_id++;
  fsm.filesystems[fs->fscid] = fs;

  // ANONYMOUS is only for upgrades from legacy mdsmaps, we should
  // have initialized next_filesystem_id such that it's never used here.
  assert(fs->fscid != FS_CLUSTER_ID_ANONYMOUS);

  // Created first filesystem?  Set it as the one
  // for legacy clients to use
  if (fsm.filesystems.size() == 1) {
    fsm.legacy_client_fscid = fs->fscid;
  }

  print_map(fsm);
}


// service methods
void MDSMonitor::create_initial()
{
  dout(10) << "create_initial" << dendl;
}


void MDSMonitor::update_from_paxos(bool *need_bootstrap)
{
  version_t version = get_last_committed();
  if (version == fsmap.epoch)
    return;

  dout(10) << __func__ << " version " << version
	   << ", my e " << fsmap.epoch << dendl;
  assert(version >= fsmap.epoch);

  // read and decode
  fsmap_bl.clear();
  int err = get_version(version, fsmap_bl);
  assert(err == 0);

  assert(fsmap_bl.length() > 0);
  dout(10) << __func__ << " got " << version << dendl;
  fsmap.decode(fsmap_bl);

  // new map
  dout(4) << "new map" << dendl;
  print_map(fsmap, 0);
  if (!g_conf->mon_mds_skip_sanity) {
    fsmap.sanity();
  }

  check_subs();
  update_logger();
}

void MDSMonitor::init()
{
  (void)load_metadata(pending_metadata);
}

void MDSMonitor::create_pending()
{
  pending_fsmap = fsmap;
  pending_fsmap.epoch++;

  dout(10) << "create_pending e" << pending_fsmap.epoch << dendl;
}

void MDSMonitor::encode_pending(MonitorDBStore::TransactionRef t)
{
  dout(10) << "encode_pending e" << pending_fsmap.epoch << dendl;


  // print map iff 'debug mon = 30' or higher
  print_map(pending_fsmap, 30);
  if (!g_conf->mon_mds_skip_sanity) {
    pending_fsmap.sanity();
  }

  // Set 'modified' on maps modified this epoch
  for (auto &i : fsmap.filesystems) {
    if (i.second->mds_map.epoch == fsmap.epoch) {
      i.second->mds_map.modified = ceph_clock_now(g_ceph_context);
    }
  }

  // apply to paxos
  assert(get_last_committed() + 1 == pending_fsmap.epoch);
  bufferlist fsmap_bl;
  pending_fsmap.encode(fsmap_bl, mon->get_quorum_features());

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

  uint64_t up = 0;
  uint64_t in = 0;
  uint64_t failed = 0;
  for (const auto &i : fsmap.filesystems) {
    const MDSMap &mds_map = i.second->mds_map;

    up += mds_map.get_num_up_mds();
    in += mds_map.get_num_in_mds();
    failed += mds_map.get_num_failed_mds();
  }
  mon->cluster_logger->set(l_cluster_num_mds_up, up);
  mon->cluster_logger->set(l_cluster_num_mds_in, in);
  mon->cluster_logger->set(l_cluster_num_mds_failed, failed);
  mon->cluster_logger->set(l_cluster_mds_epoch, fsmap.get_epoch());
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
    assert(0);
    return true;
  }
}

void MDSMonitor::_note_beacon(MMDSBeacon *m)
{
  mds_gid_t gid = mds_gid_t(m->get_global_id());
  version_t seq = m->get_seq();

  dout(15) << "_note_beacon " << *m << " noting time" << dendl;
  last_beacon[gid].stamp = ceph_clock_now(g_ceph_context);  
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
	 info.state == MDSMap::STATE_STANDBY_REPLAY ||
	 info.state == MDSMap::STATE_ONESHOT_REPLAY) && state > 0) {
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
    assert(0);
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

  // Store health
  dout(20) << __func__ << " got health from gid " << gid << " with " << m->get_health().metrics.size() << " metrics." << dendl;
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
        bool followable = fs->mds_map.is_followable(leaderinfo->rank);

        pending_fsmap.modify_daemon(gid, [fscid, leaderinfo, followable](
              MDSMap::mds_info_t *info) {
            info->standby_for_rank = leaderinfo->rank;
            info->standby_for_ns = fscid;
        });
      }
    }

    // initialize the beacon timer
    last_beacon[gid].stamp = ceph_clock_now(g_ceph_context);
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
      pending_fsmap.stop(gid);
      last_beacon.erase(gid);
    } else if (state == MDSMap::STATE_STANDBY_REPLAY) {
      if (m->get_standby_for_rank() == MDSMap::MDS_STANDBY_NAME) {
        dout(20) << "looking for mds " << m->get_standby_for_name()
                  << " to STANDBY_REPLAY for" << dendl;
        auto target_info = pending_fsmap.find_by_name(m->get_standby_for_name());
        if (target_info == nullptr) {
          // This name is unknown, do nothing, stay in standby
          return false;
        }

        auto target_ns = pending_fsmap.mds_roles.at(target_info->global_id);
        if (target_ns == FS_CLUSTER_ID_NONE) {
          // The named daemon is not in a Filesystem, do nothing.
          return false;
        }

        auto target_fs = pending_fsmap.get_filesystem(target_ns);
        if (target_fs->mds_map.is_followable(info.rank)) {
          dout(10) <<" found mds " << m->get_standby_for_name()
                       << "; it has rank " << target_info->rank << dendl;
          pending_fsmap.modify_daemon(info.global_id,
              [target_info, target_ns, seq](MDSMap::mds_info_t *info) {
            info->standby_for_rank = target_info->rank;
            info->standby_for_ns = target_ns;
            info->state = MDSMap::STATE_STANDBY_REPLAY;
            info->state_seq = seq;
          });
        } else {
          // The named daemon has a rank but isn't followable, do nothing
          return false;
        }
      } else if (m->get_standby_for_rank() >= 0) {
        // TODO get this from MDS message
        // >>
        fs_cluster_id_t target_ns = FS_CLUSTER_ID_NONE;
        // <<

        mds_role_t target_role = {
          target_ns == FS_CLUSTER_ID_NONE ?
            pending_fsmap.legacy_client_fscid : info.standby_for_ns,
          m->get_standby_for_rank()};

        if (target_role.fscid != FS_CLUSTER_ID_NONE) {
          auto fs = pending_fsmap.get_filesystem(target_role.fscid);
          if (fs->mds_map.is_followable(target_role.rank)) {
            pending_fsmap.modify_daemon(info.global_id,
                [target_role, seq](MDSMap::mds_info_t *info) {
              info->standby_for_rank = target_role.rank;
              info->standby_for_ns = target_role.fscid;
              info->state = MDSMap::STATE_STANDBY_REPLAY;
              info->state_seq = seq;
            });
          } else {
            // We know who they want to follow, but he's not in a suitable state
            return false;
          }
        } else {
          // Couldn't resolve to a particular filesystem
          return false;
        }
      } else { //it's a standby for anybody, and is already in the list
        assert(pending_fsmap.get_mds_info().count(info.global_id));
        return false;
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

      const utime_t until = ceph_clock_now(g_ceph_context);
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
    } else {
      pending_fsmap.modify_daemon(gid, [state, seq](MDSMap::mds_info_t *info) {
        info->state = state;
        info->state_seq = seq;
      });
    }
  }

  dout(7) << "prepare_beacon pending map now:" << dendl;
  print_map(pending_fsmap);
  
  wait_for_finished_proposal(op, new C_Updated(this, op));

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
  mon->clog->info() << m->get_orig_source_inst() << " "
	  << ceph_mds_state_name(m->get_state()) << "\n";

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
  update_logger();

  if (mon->is_leader())
    mon->clog->info() << "fsmap " << fsmap << "\n";
}

void MDSMonitor::get_health(list<pair<health_status_t, string> >& summary,
			    list<pair<health_status_t, string> > *detail,
			    CephContext* cct) const
{
  fsmap.get_health(summary, detail);

  // For each MDS GID...
  const auto info_map = fsmap.get_mds_info();
  for (const auto &i : info_map) {
    const auto &gid = i.first;
    const auto &info = i.second;

    // Decode MDSHealth
    bufferlist bl;
    mon->store->get(MDS_HEALTH_PREFIX, stringify(gid), bl);
    if (!bl.length()) {
      derr << "Missing health data for MDS " << gid << dendl;
      continue;
    }
    MDSHealth health;
    bufferlist::iterator bl_i = bl.begin();
    health.decode(bl_i);

    for (std::list<MDSHealthMetric>::iterator j = health.metrics.begin(); j != health.metrics.end(); ++j) {
      int const rank = info.rank;
      std::ostringstream message;
      message << "mds" << rank << ": " << j->message;
      summary.push_back(std::make_pair(j->sev, message.str()));

      if (detail) {
        // There is no way for us to clealy associate detail entries with summary entries (#7192), so
        // we duplicate the summary message in the detail string and tag the metadata on.
        std::ostringstream detail_message;
        detail_message << message.str();
        if (j->metadata.size()) {
          detail_message << "(";
          std::map<std::string, std::string>::iterator k = j->metadata.begin();
          while (k != j->metadata.end()) {
            detail_message << k->first << ": " << k->second;
            if (boost::next(k) != j->metadata.end()) {
              detail_message << ", ";
            }
            ++k;
          }
          detail_message << ")";
        }
        detail->push_back(std::make_pair(j->sev, detail_message.str()));
      }
    }
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

class FileSystemCommandHandler
{
protected:
  std::string prefix;

  /**
   * Parse true|yes|1 style boolean string from `bool_str`
   * `result` must be non-null.
   * `ss` will be populated with error message on error.
   *
   * @return 0 on success, else -EINVAL
   */
  int parse_bool(
      const std::string &bool_str,
      bool *result,
      std::ostream &ss)
  {
    assert(result != nullptr);

    string interr;
    int64_t n = strict_strtoll(bool_str.c_str(), 10, &interr);

    if (bool_str == "false" || bool_str == "no"
        || (interr.length() == 0 && n == 0)) {
      *result = false;
      return 0;
    } else if (bool_str == "true" || bool_str == "yes"
        || (interr.length() == 0 && n == 1)) {
      *result = true;
      return 0;
    } else {
      ss << "value must be false|no|0 or true|yes|1";
      return -EINVAL;
    }
  }

  /**
   * Return 0 if the pool is suitable for use with CephFS, or
   * in case of errors return a negative error code, and populate
   * the passed stringstream with an explanation.
   */
  int _check_pool(
      OSDMap &osd_map,
      const int64_t pool_id,
      std::stringstream *ss) const
  {
    assert(ss != NULL);

    const pg_pool_t *pool = osd_map.get_pg_pool(pool_id);
    if (!pool) {
      *ss << "pool id '" << pool_id << "' does not exist";
      return -ENOENT;
    }

    const string& pool_name = osd_map.get_pool_name(pool_id);

    if (pool->is_erasure()) {
      // EC pools are only acceptable with a cache tier overlay
      if (!pool->has_tiers() || !pool->has_read_tier() || !pool->has_write_tier()) {
        *ss << "pool '" << pool_name << "' (id '" << pool_id << "')"
           << " is an erasure-code pool";
        return -EINVAL;
      }

      // That cache tier overlay must be writeback, not readonly (it's the
      // write operations like modify+truncate we care about support for)
      const pg_pool_t *write_tier = osd_map.get_pg_pool(
          pool->write_tier);
      assert(write_tier != NULL);  // OSDMonitor shouldn't allow DNE tier
      if (write_tier->cache_mode == pg_pool_t::CACHEMODE_FORWARD
          || write_tier->cache_mode == pg_pool_t::CACHEMODE_READONLY) {
        *ss << "EC pool '" << pool_name << "' has a write tier ("
            << osd_map.get_pool_name(pool->write_tier)
            << ") that is configured "
               "to forward writes.  Use a cache mode such as 'writeback' for "
               "CephFS";
        return -EINVAL;
      }
    }

    if (pool->is_tier()) {
      *ss << " pool '" << pool_name << "' (id '" << pool_id
        << "') is already in use as a cache tier.";
      return -EINVAL;
    }

    // Nothing special about this pool, so it is permissible
    return 0;
  }

  virtual std::string const &get_prefix() {return prefix;}

public:
  FileSystemCommandHandler(const std::string &prefix_)
    : prefix(prefix_)
  {

  }
  virtual ~FileSystemCommandHandler()
  {
  }

  bool can_handle(std::string const &prefix_)
  {
    return get_prefix() == prefix_;
  }

  virtual int handle(
    Monitor *mon,
    FSMap &fsmap,
    MonOpRequestRef op,
    map<string, cmd_vartype> &cmdmap,
    std::stringstream &ss) = 0;
};

bool MDSMonitor::preprocess_command(MonOpRequestRef op)
{
  op->mark_mdsmon_event(__func__);
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  int r = -1;
  bufferlist rdata;
  stringstream ss, ds;

  map<string, cmd_vartype> cmdmap;
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
  } else if (prefix == "mds dump") {
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
      const MDSMap *mdsmap = nullptr;
      MDSMap blank;
      blank.epoch = fsmap.epoch;
      if (fsmap.legacy_client_fscid != FS_CLUSTER_ID_NONE) {
        mdsmap = &(fsmap.filesystems[fsmap.legacy_client_fscid]->mds_map);
      } else {
        mdsmap = &blank;
      }
      if (f != NULL) {
	f->open_object_section("mdsmap");
	mdsmap->dump(f.get());
	f->close_section();
	f->flush(ds);
	r = 0;
      } else {
	mdsmap->print(ds);
	r = 0;
      } 
      if (r == 0) {
	rdata.append(ds);
	ss << "dumped fsmap epoch " << p->get_epoch();
      }
      if (p != &fsmap) {
	delete p;
      }
    }
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
      if (r == 0) {
	rdata.append(ds);
	ss << "dumped fsmap epoch " << p->get_epoch();
      }
      if (p != &fsmap)
	delete p;
    }
  } else if (prefix == "mds metadata") {
    string who;
    cmd_getval(g_ceph_context, cmdmap, "who", who);
    if (!f)
      f.reset(Formatter::create("json-pretty"));
    f->open_object_section("mds_metadata");
    r = dump_metadata(who, f.get(), ss);
    f->close_section();
    f->flush(ds);
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
	assert(err == 0);
	assert(b.length());
	FSMap mm;
	mm.decode(b);
	mm.encode(rdata, m->get_connection()->get_features());
	ss << "got fsmap epoch " << mm.get_epoch();
	r = 0;
      }
    } else {
      fsmap.encode(rdata, m->get_connection()->get_features());
      ss << "got fsmap epoch " << fsmap.get_epoch();
      r = 0;
    }
  } else if (prefix == "mds tell") {
    string whostr;
    cmd_getval(g_ceph_context, cmdmap, "who", whostr);
    vector<string>args_vec;
    cmd_getval(g_ceph_context, cmdmap, "args", args_vec);

    if (whostr == "*") {
      r = -ENOENT;
      const auto info_map = fsmap.get_mds_info();
      for (const auto &i : info_map) {
	m->cmd = args_vec;
	mon->send_command(i.second.get_inst(), m->cmd);
	r = 0;
      }
      if (r == -ENOENT) {
	ss << "no mds active";
      } else {
	ss << "ok";
      }
    } else {
      if (fsmap.legacy_client_fscid) {
        auto fs = fsmap.filesystems.at(fsmap.legacy_client_fscid);
        errno = 0;
        long who_l = strtol(whostr.c_str(), 0, 10);
        if (!errno && who_l >= 0) {
          mds_rank_t who = mds_rank_t(who_l);
          if (fs->mds_map.is_up(who)) {
            m->cmd = args_vec;
            mon->send_command(fs->mds_map.get_inst(who), m->cmd);
            r = 0;
            ss << "ok";
          } else {
            ss << "mds." << who << " not up";
            r = -ENOENT;
          }
        } else {
          ss << "specify mds number or *";
        }
      } else {
        ss << "no legacy filesystem set";
      }
    }
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
        for (std::set<int64_t>::iterator dpi = mds_map.data_pools.begin();
           dpi != mds_map.data_pools.end(); ++dpi) {
          const string &pool_name = mon->osdmon()->osdmap.get_pool_name(*dpi);
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
    utime_t until = ceph_clock_now(g_ceph_context);
    until += g_conf->mds_blacklist_interval;
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
  // Try parsing as a role
  mds_role_t role;
  std::ostringstream ignore_err;  // Don't spam 'ss' with parse_role errors
  int r = parse_role(arg, &role, ignore_err);
  if (r == 0) {
    // See if a GID is assigned to this role
    auto fs = pending_fsmap.get_filesystem(role.fscid);
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
    const MDSMap::mds_info_t *mds_info = fsmap.find_by_name(arg);
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
    if (mon->is_leader()) {
      if (pending_fsmap.gid_exists(mds_gid_t(maybe_gid))) {
        return mds_gid_t(maybe_gid);
      }
    } else {
      if (fsmap.gid_exists(mds_gid_t(maybe_gid))) {
        return mds_gid_t(maybe_gid);
      }
    }
  }

  dout(1) << __func__ << ": rank/GID " << arg
	  << " not a existent rank or GID" << dendl;
  return MDS_GID_NONE;
}

int MDSMonitor::fail_mds(std::ostream &ss, const std::string &arg)
{
  mds_gid_t gid = gid_from_arg(arg, ss);
  if (gid == MDS_GID_NONE) {
    return 0;
  }
  if (!mon->osdmon()->is_writeable()) {
    return -EAGAIN;
  }
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

  map<string, cmd_vartype> cmdmap;
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

  for (auto h : handlers) {
    if (h->can_handle(prefix)) {
      r = h->handle(mon, pending_fsmap, op, cmdmap, ss);
      if (r == -EAGAIN) {
        // message has been enqueued for retry; return.
        dout(4) << __func__ << " enqueue for retry by management_command" << dendl;
        return false;
      } else {
        // Successful or not, we're done: respond.
        goto out;
      }
    }
  }

  /* Execute filesystem add/remove, or pass through to filesystem_command */
  r = management_command(op, prefix, cmdmap, ss);
  if (r >= 0)
    goto out;
  
  if (r == -EAGAIN) {
    // message has been enqueued for retry; return.
    dout(4) << __func__ << " enqueue for retry by management_command" << dendl;
    return false;
  } else if (r != -ENOSYS) {
    // MDSMonitor::management_command() returns -ENOSYS if it knows nothing
    // about the command passed to it, in which case we will check whether
    // MDSMonitor::filesystem_command() knows about it.  If on the other hand
    // the error code is different from -ENOSYS, we will treat it as is and
    // behave accordingly.
    goto out;
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

  r = legacy_filesystem_command(op, prefix, cmdmap, ss);

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
    return true;
  } else {
    // reply immediately
    mon->reply_command(op, r, rs, rdata, get_last_committed());
    return false;
  }
}


/**
 * Return 0 if the pool is suitable for use with CephFS, or
 * in case of errors return a negative error code, and populate
 * the passed stringstream with an explanation.
 */
int MDSMonitor::_check_pool(
    const int64_t pool_id,
    std::stringstream *ss) const
{
  assert(ss != NULL);

  const pg_pool_t *pool = mon->osdmon()->osdmap.get_pg_pool(pool_id);
  if (!pool) {
    *ss << "pool id '" << pool_id << "' does not exist";
    return -ENOENT;
  }

  const string& pool_name = mon->osdmon()->osdmap.get_pool_name(pool_id);

  if (pool->is_erasure()) {
    // EC pools are only acceptable with a cache tier overlay
    if (!pool->has_tiers() || !pool->has_read_tier() || !pool->has_write_tier()) {
      *ss << "pool '" << pool_name << "' (id '" << pool_id << "')"
         << " is an erasure-code pool";
      return -EINVAL;
    }

    // That cache tier overlay must be writeback, not readonly (it's the
    // write operations like modify+truncate we care about support for)
    const pg_pool_t *write_tier = mon->osdmon()->osdmap.get_pg_pool(
        pool->write_tier);
    assert(write_tier != NULL);  // OSDMonitor shouldn't allow DNE tier
    if (write_tier->cache_mode == pg_pool_t::CACHEMODE_FORWARD
        || write_tier->cache_mode == pg_pool_t::CACHEMODE_READONLY) {
      *ss << "EC pool '" << pool_name << "' has a write tier ("
          << mon->osdmon()->osdmap.get_pool_name(pool->write_tier)
          << ") that is configured "
             "to forward writes.  Use a cache mode such as 'writeback' for "
             "CephFS";
      return -EINVAL;
    }
  }

  if (pool->is_tier()) {
    *ss << " pool '" << pool_name << "' (id '" << pool_id
      << "') is already in use as a cache tier.";
    return -EINVAL;
  }

  // Nothing special about this pool, so it is permissible
  return 0;
}

class FlagSetHandler : public FileSystemCommandHandler
{
  public:
  FlagSetHandler()
    : FileSystemCommandHandler("fs flag set")
  {
  }

  int handle(
      Monitor *mon,
      FSMap &fsmap,
      MonOpRequestRef op,
      map<string, cmd_vartype> &cmdmap,
      std::stringstream &ss)
  {
    string flag_name;
    cmd_getval(g_ceph_context, cmdmap, "flag_name", flag_name);

    string flag_val;
    cmd_getval(g_ceph_context, cmdmap, "val", flag_val);

    if (flag_name == "enable_multiple") {
      bool flag_bool = false;
      int r = parse_bool(flag_val, &flag_bool, ss);
      if (r != 0) {
        ss << "Invalid boolean value '" << flag_val << "'";
        return r;
      }
      fsmap.set_enable_multiple(flag_bool);
      return 0;
    } else {
      ss << "Unknown flag '" << flag_name << "'";
      return -EINVAL;
    }
  }
};

/**
 * Handle a command for creating or removing a filesystem.
 *
 * @retval 0        Command was successfully handled and has side effects
 * @retval -EAGAIN  Message has been queued for retry
 * @retval -ENOSYS  Unknown command
 * @retval < 0      An error has occurred; **ss** may have been set.
 */
int MDSMonitor::management_command(
    MonOpRequestRef op,
    std::string const &prefix,
    map<string, cmd_vartype> &cmdmap,
    std::stringstream &ss)
{
  op->mark_mdsmon_event(__func__);

  if (prefix == "mds newfs") {
    // newfs is the legacy command that in single-filesystem times
    // used to be equivalent to doing an "fs rm ; fs new".  We
    // can't do this in a sane way in multi-filesystem world.
    ss << "'newfs' no longer available.  Please use 'fs new'.";
    return -EINVAL;
  } else if (prefix == "fs new") {
    string metadata_name;
    cmd_getval(g_ceph_context, cmdmap, "metadata", metadata_name);
    int64_t metadata = mon->osdmon()->osdmap.lookup_pg_pool_name(metadata_name);
    if (metadata < 0) {
      ss << "pool '" << metadata_name << "' does not exist";
      return -ENOENT;
    }

    string data_name;
    cmd_getval(g_ceph_context, cmdmap, "data", data_name);
    int64_t data = mon->osdmon()->osdmap.lookup_pg_pool_name(data_name);
    if (data < 0) {
      ss << "pool '" << data_name << "' does not exist";
      return -ENOENT;
    }
    if (data == 0) {
      ss << "pool '" << data_name << "' has id 0, which CephFS does not allow. Use another pool or recreate it to get a non-zero pool id.";
      return -EINVAL;
    }
   
    string fs_name;
    cmd_getval(g_ceph_context, cmdmap, "fs_name", fs_name);
    if (fs_name.empty()) {
        // Ensure fs name is not empty so that we can implement
        // commmands that refer to FS by name in future.
        ss << "Filesystem name may not be empty";
        return -EINVAL;
    }

    if (pending_fsmap.any_filesystems()
        && !pending_fsmap.get_enable_multiple()) {
      ss << "Creation of multiple filesystems is disabled.  To enable "
            "this experimental feature, use 'ceph fs flag set enable_multiple "
            "true'";
      return -EINVAL;
    }

    if (pending_fsmap.get_filesystem(fs_name)) {
      auto fs = pending_fsmap.get_filesystem(fs_name);
      if (*(fs->mds_map.data_pools.begin()) == data
          && fs->mds_map.metadata_pool == metadata) {
        // Identical FS created already, this is a no-op
        ss << "filesystem '" << fs_name << "' already exists";
        return 0;
      } else {
        ss << "filesystem already exists with name '" << fs_name << "'";
        return -EINVAL;
      }
    }

    pg_pool_t const *data_pool = mon->osdmon()->osdmap.get_pg_pool(data);
    assert(data_pool != NULL);  // Checked it existed above
    pg_pool_t const *metadata_pool = mon->osdmon()->osdmap.get_pg_pool(metadata);
    assert(metadata_pool != NULL);  // Checked it existed above

    // we must make these checks before we even allow ourselves to *think*
    // about requesting a proposal to the osdmonitor and bail out now if
    // we believe we must.  bailing out *after* we request the proposal is
    // bad business as we could have changed the osdmon's state and ending up
    // returning an error to the user.
    int r = _check_pool(data, &ss);
    if (r < 0) {
      return r;
    }

    r = _check_pool(metadata, &ss);
    if (r < 0) {
      return r;
    }

    // Automatically set crash_replay_interval on data pool if it
    // isn't already set.
    if (data_pool->get_crash_replay_interval() == 0) {
      // We will be changing osdmon's state and requesting the osdmon to
      // propose.  We thus need to make sure the osdmon is writeable before
      // we do this, waiting if it's not.
      if (!mon->osdmon()->is_writeable()) {
        mon->osdmon()->wait_for_writeable(op, new C_RetryMessage(this, op));
        return -EAGAIN;
      }

      r = mon->osdmon()->set_crash_replay_interval(data, g_conf->osd_default_data_pool_replay_window);
      assert(r == 0);  // We just did get_pg_pool so it must exist and be settable
      request_proposal(mon->osdmon());
    }

    // All checks passed, go ahead and create.
    create_new_fs(pending_fsmap, fs_name, metadata, data);
    ss << "new fs with metadata pool " << metadata << " and data pool " << data;
    return 0;
  } else if (prefix == "fs rm") {
    // Check caller has correctly named the FS to delete
    // (redundant while there is only one FS, but command
    //  syntax should apply to multi-FS future)
    string fs_name;
    cmd_getval(g_ceph_context, cmdmap, "fs_name", fs_name);
    auto fs = pending_fsmap.get_filesystem(fs_name);
    if (fs == nullptr) {
        // Consider absence success to make deletes idempotent
        ss << "filesystem '" << fs_name << "' does not exist";
        return 0;
    }

    // Check that no MDS daemons are active
    if (!fs->mds_map.up.empty()) {
      ss << "all MDS daemons must be inactive before removing filesystem";
      return -EINVAL;
    }

    // Check for confirmation flag
    string sure;
    cmd_getval(g_ceph_context, cmdmap, "sure", sure);
    if (sure != "--yes-i-really-mean-it") {
      ss << "this is a DESTRUCTIVE operation and will make data in your filesystem permanently" \
            "inaccessible.  Add --yes-i-really-mean-it if you are sure you wish to continue.";
      return -EPERM;
    }

    if (pending_fsmap.legacy_client_fscid == fs->fscid) {
      pending_fsmap.legacy_client_fscid = FS_CLUSTER_ID_NONE;
    }

    // There may be standby_replay daemons left here
    for (const auto &i : fs->mds_map.mds_info) {
      assert(i.second.state == MDSMap::STATE_STANDBY_REPLAY);
      fail_mds_gid(i.first);
    }

    pending_fsmap.filesystems.erase(fs->fscid);

    return 0;
  } else if (prefix == "fs reset") {
    string fs_name;
    cmd_getval(g_ceph_context, cmdmap, "fs_name", fs_name);
    auto fs = pending_fsmap.get_filesystem(fs_name);
    if (fs == nullptr) {
        ss << "filesystem '" << fs_name << "' does not exist";
        // Unlike fs rm, we consider this case an error
        return -ENOENT;
    }

    // Check that no MDS daemons are active
    if (!fs->mds_map.up.empty()) {
      ss << "all MDS daemons must be inactive before resetting filesystem: set the cluster_down flag"
            " and use `ceph mds fail` to make this so";
      return -EINVAL;
    }

    // Check for confirmation flag
    string sure;
    cmd_getval(g_ceph_context, cmdmap, "sure", sure);
    if (sure != "--yes-i-really-mean-it") {
      ss << "this is a potentially destructive operation, only for use by experts in disaster recovery.  "
        "Add --yes-i-really-mean-it if you are sure you wish to continue.";
      return -EPERM;
    }

    FSMap newmap;

    auto new_fs = std::make_shared<Filesystem>();

    // Populate rank 0 as existing (so don't go into CREATING)
    // but failed (so that next available MDS is assigned the rank)
    new_fs->mds_map.in.insert(mds_rank_t(0));
    new_fs->mds_map.failed.insert(mds_rank_t(0));

    // Carry forward what makes sense
    new_fs->fscid = fs->fscid;
    new_fs->mds_map.inc = fs->mds_map.inc;
    new_fs->mds_map.inline_data_enabled = fs->mds_map.inline_data_enabled;
    new_fs->mds_map.max_mds = g_conf->max_mds;
    new_fs->mds_map.data_pools = fs->mds_map.data_pools;
    new_fs->mds_map.metadata_pool = fs->mds_map.metadata_pool;
    new_fs->mds_map.cas_pool = fs->mds_map.cas_pool;
    new_fs->mds_map.fs_name = fs->mds_map.fs_name;
    new_fs->mds_map.max_file_size = g_conf->mds_max_file_size;
    new_fs->mds_map.compat = fsmap.compat;
    new_fs->mds_map.created = ceph_clock_now(g_ceph_context);
    new_fs->mds_map.modified = ceph_clock_now(g_ceph_context);
    new_fs->mds_map.session_timeout = g_conf->mds_session_timeout;
    new_fs->mds_map.session_autoclose = g_conf->mds_session_autoclose;
    new_fs->mds_map.enabled = true;

    // Persist the new FSMap
    pending_fsmap.filesystems[new_fs->fscid] = new_fs;
    return 0;
  } else {
    return -ENOSYS;
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


class SetHandler : public FileSystemCommandHandler
{
public:
  SetHandler()
    : FileSystemCommandHandler("fs set")
  {}

  virtual int handle(
      Monitor *mon,
      FSMap &fsmap,
      MonOpRequestRef op,
      map<string, cmd_vartype> &cmdmap,
      std::stringstream &ss)
  {
    std::string fs_name;
    if (!cmd_getval(g_ceph_context, cmdmap, "fs_name", fs_name) || fs_name.empty()) {
      ss << "Missing filesystem name";
      return -EINVAL;
    }

    auto fs = fsmap.get_filesystem(fs_name);
    if (fs == nullptr) {
      ss << "Not found: '" << fs_name << "'";
      return -ENOENT;
    }

    string var;
    if (!cmd_getval(g_ceph_context, cmdmap, "var", var) || var.empty()) {
      ss << "Invalid variable";
      return -EINVAL;
    }
    string val;
    string interr;
    int64_t n = 0;
    if (!cmd_getval(g_ceph_context, cmdmap, "val", val)) {
      return -EINVAL;
    }
    // we got a string.  see if it contains an int.
    n = strict_strtoll(val.c_str(), 10, &interr);
    if (var == "max_mds") {
      // NOTE: see also "mds set_max_mds", which can modify the same field.
      if (interr.length()) {
	return -EINVAL;
      }
      if (n > MAX_MDS) {
        ss << "may not have more than " << MAX_MDS << " MDS ranks";
        return -EINVAL;
      }
      fsmap.modify_filesystem(
          fs->fscid,
          [n](std::shared_ptr<Filesystem> fs)
      {
        fs->mds_map.set_max_mds(n);
      });
    } else if (var == "inline_data") {
      bool enable_inline = false;
      int r = parse_bool(val, &enable_inline, ss);
      if (r != 0) {
        return r;
      }

      if (enable_inline) {
	string confirm;
	if (!cmd_getval(g_ceph_context, cmdmap, "confirm", confirm) ||
	    confirm != "--yes-i-really-mean-it") {
	  ss << "inline data is new and experimental; you must specify --yes-i-really-mean-it";
	  return -EPERM;
	}
	ss << "inline data enabled";

        fsmap.modify_filesystem(
            fs->fscid,
            [](std::shared_ptr<Filesystem> fs)
        {
          fs->mds_map.set_inline_data_enabled(true);
        });

        // Update `compat`
        CompatSet c = fsmap.get_compat();
        c.incompat.insert(MDS_FEATURE_INCOMPAT_INLINE);
        fsmap.update_compat(c);
      } else {
	ss << "inline data disabled";
        fsmap.modify_filesystem(
            fs->fscid,
            [](std::shared_ptr<Filesystem> fs)
        {
          fs->mds_map.set_inline_data_enabled(false);
        });
      }
    } else if (var == "max_file_size") {
      if (interr.length()) {
	ss << var << " requires an integer value";
	return -EINVAL;
      }
      if (n < CEPH_MIN_STRIPE_UNIT) {
	ss << var << " must at least " << CEPH_MIN_STRIPE_UNIT;
	return -ERANGE;
      }
      fsmap.modify_filesystem(
          fs->fscid,
          [n](std::shared_ptr<Filesystem> fs)
      {
        fs->mds_map.set_max_filesize(n);
      });
    } else if (var == "allow_new_snaps") {
      bool enable_snaps = false;
      int r = parse_bool(val, &enable_snaps, ss);
      if (r != 0) {
        return r;
      }

      if (!enable_snaps) {
        fsmap.modify_filesystem(
            fs->fscid,
            [](std::shared_ptr<Filesystem> fs)
        {
          fs->mds_map.clear_snaps_allowed();
        });
	ss << "disabled new snapshots";
      } else {
	string confirm;
	if (!cmd_getval(g_ceph_context, cmdmap, "confirm", confirm) ||
	    confirm != "--yes-i-really-mean-it") {
	  ss << "Snapshots are unstable and will probably break your FS! Set to --yes-i-really-mean-it if you are sure you want to enable them";
	  return -EPERM;
	}
        fsmap.modify_filesystem(
            fs->fscid,
            [](std::shared_ptr<Filesystem> fs)
        {
          fs->mds_map.set_snaps_allowed();
        });
	ss << "enabled new snapshots";
      }
    } else if (var == "cluster_down") {
      bool is_down = false;
      int r = parse_bool(val, &is_down, ss);
      if (r != 0) {
        return r;
      }

      fsmap.modify_filesystem(
          fs->fscid,
          [is_down](std::shared_ptr<Filesystem> fs)
      {
        if (is_down) {
          fs->mds_map.set_flag(CEPH_MDSMAP_DOWN);
        } else {
          fs->mds_map.clear_flag(CEPH_MDSMAP_DOWN);
        }
      });

      ss << "marked " << (is_down ? "down" : "up");
    } else {
      ss << "unknown variable " << var;
      return -EINVAL;
    }

    return 0;
  }
};

class AddDataPoolHandler : public FileSystemCommandHandler
{
  public:
  AddDataPoolHandler()
    : FileSystemCommandHandler("fs add_data_pool")
  {}

  int handle(
      Monitor *mon,
      FSMap &fsmap,
      MonOpRequestRef op,
      map<string, cmd_vartype> &cmdmap,
      std::stringstream &ss)
  {
    string poolname;
    cmd_getval(g_ceph_context, cmdmap, "pool", poolname);

    std::string fs_name;
    if (!cmd_getval(g_ceph_context, cmdmap, "fs_name", fs_name)
        || fs_name.empty()) {
      ss << "Missing filesystem name";
      return -EINVAL;
    }

    auto fs = fsmap.get_filesystem(fs_name);
    if (fs == nullptr) {
      ss << "Not found: '" << fs_name << "'";
      return -ENOENT;
    }

    int64_t poolid = mon->osdmon()->osdmap.lookup_pg_pool_name(poolname);
    if (poolid < 0) {
      string err;
      poolid = strict_strtol(poolname.c_str(), 10, &err);
      if (err.length()) {
	ss << "pool '" << poolname << "' does not exist";
	return -ENOENT;
      }
    }

    int r = _check_pool(mon->osdmon()->osdmap, poolid, &ss);
    if (r != 0) {
      return r;
    }

    fsmap.modify_filesystem(
        fs->fscid,
        [poolid](std::shared_ptr<Filesystem> fs)
    {
      fs->mds_map.add_data_pool(poolid);
    });

    ss << "added data pool " << poolid << " to fsmap";

    return 0;
  }
};


class RemoveDataPoolHandler : public FileSystemCommandHandler
{
  public:
  RemoveDataPoolHandler()
    : FileSystemCommandHandler("fs remove_data_pool")
  {}

  int handle(
      Monitor *mon,
      FSMap &fsmap,
      MonOpRequestRef op,
      map<string, cmd_vartype> &cmdmap,
      std::stringstream &ss)
  {
    string poolname;
    cmd_getval(g_ceph_context, cmdmap, "pool", poolname);

    std::string fs_name;
    if (!cmd_getval(g_ceph_context, cmdmap, "fs_name", fs_name)
        || fs_name.empty()) {
      ss << "Missing filesystem name";
      return -EINVAL;
    }

    auto fs = fsmap.get_filesystem(fs_name);
    if (fs == nullptr) {
      ss << "Not found: '" << fs_name << "'";
      return -ENOENT;
    }

    int64_t poolid = mon->osdmon()->osdmap.lookup_pg_pool_name(poolname);
    if (poolid < 0) {
      string err;
      poolid = strict_strtol(poolname.c_str(), 10, &err);
      if (err.length()) {
	poolid = -1;
	ss << "pool '" << poolname << "' does not exist";
        return -ENOENT;
      } else if (poolid < 0) {
        ss << "invalid pool id '" << poolid << "'";
        return -EINVAL;
      }
    }

    assert(poolid >= 0);  // Checked by parsing code above

    if (fs->mds_map.get_first_data_pool() == poolid) {
      poolid = -1;
      ss << "cannot remove default data pool";
      return -EINVAL;
    }


    int r = 0;
    fsmap.modify_filesystem(fs->fscid,
        [&r, poolid](std::shared_ptr<Filesystem> fs)
    {
      r = fs->mds_map.remove_data_pool(poolid);
    });
    if (r == -ENOENT) {
      // It was already removed, succeed in silence
      return 0;
    } else if (r == 0) {
      // We removed it, succeed
      ss << "removed data pool " << poolid << " from fsmap";
      return 0;
    } else {
      // Unexpected error, bubble up
      return r;
    }
  }
};


/**
 * For commands that refer to a particular filesystem,
 * enable wrapping to implement the legacy version of
 * the command (like "mds add_data_pool" vs "fs add_data_pool")
 *
 * The wrapped handler must expect a fs_name argument in
 * its command map.
 */
template<typename T>
class LegacyHandler : public T
{
  std::string legacy_prefix;

  public:
  LegacyHandler(const std::string &new_prefix)
    : T()
  {
    legacy_prefix = new_prefix;
  }

  virtual std::string const &get_prefix() {return legacy_prefix;}

  int handle(
      Monitor *mon,
      FSMap &fsmap,
      MonOpRequestRef op,
      map<string, cmd_vartype> &cmdmap,
      std::stringstream &ss)
  {
    auto fs = fsmap.get_legacy_filesystem();
    if (fs == nullptr) {
      ss << "No filesystem configured";
      return -ENOENT;
    }
    std::map<string, cmd_vartype> modified = cmdmap;
    modified["fs_name"] = fs->mds_map.get_fs_name();
    return T::handle(mon, fsmap, op, modified, ss);
  }
};

int MDSMonitor::filesystem_command(
    MonOpRequestRef op,
    std::string const &prefix,
    map<string, cmd_vartype> &cmdmap,
    std::stringstream &ss)
{
  dout(4) << __func__ << " prefix='" << prefix << "'" << dendl;
  op->mark_mdsmon_event(__func__);
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  int r = 0;
  string whostr;
  cmd_getval(g_ceph_context, cmdmap, "who", whostr);

  if (prefix == "mds stop" ||
      prefix == "mds deactivate") {

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
  } else if (prefix == "mds setmap") {
    string confirm;
    if (!cmd_getval(g_ceph_context, cmdmap, "confirm", confirm) ||
       confirm != "--yes-i-really-mean-it") {
      ss << "WARNING: this can make your filesystem inaccessible! "
           "Add --yes-i-really-mean-it if you are sure you wish to continue.";
      return -EINVAL;;
    }
    
    FSMap map;
    try {
      map.decode(m->get_data());
    } catch(buffer::error &e) {
      ss << "invalid mdsmap";
      return -EINVAL;
    }
    epoch_t e = 0;
    int64_t epochnum;
    if (cmd_getval(g_ceph_context, cmdmap, "epoch", epochnum))
      e = epochnum;

    if (pending_fsmap.epoch == e) {
      map.epoch = pending_fsmap.epoch;  // make sure epoch is correct
      pending_fsmap = map;
      ss << "set mds map";
    } else {
      ss << "next fsmap epoch " << pending_fsmap.epoch << " != " << e;
      return -EINVAL;
    }
  } else if (prefix == "mds set_state") {
    mds_gid_t gid;
    if (!cmd_getval(g_ceph_context, cmdmap, "gid", gid)) {
      ss << "error parsing 'gid' value '"
         << cmd_vartype_stringify(cmdmap["gid"]) << "'";
      return -EINVAL;
    }
    MDSMap::DaemonState state;
    if (!cmd_getval(g_ceph_context, cmdmap, "state", state)) {
      ss << "error parsing 'state' string value '"
         << cmd_vartype_stringify(cmdmap["state"]) << "'";
      return -EINVAL;
    }
    if (pending_fsmap.gid_exists(gid)) {
      pending_fsmap.modify_daemon(gid, [state](MDSMap::mds_info_t *info) {
        info->state = state;
      });
      stringstream ss;
      ss << "set mds gid " << gid << " to state " << state << " "
         << ceph_mds_state_name(state);
      return 0;
    }
  } else if (prefix == "mds fail") {
    string who;
    cmd_getval(g_ceph_context, cmdmap, "who", who);
    r = fail_mds(ss, who);
    if (r < 0 && r == -EAGAIN) {
      mon->osdmon()->wait_for_writeable(op, new C_RetryMessage(this, op));
      return -EAGAIN; // don't propose yet; wait for message to be retried
    }
  } else if (prefix == "mds rm") {
    mds_gid_t gid;
    if (!cmd_getval(g_ceph_context, cmdmap, "gid", gid)) {
      ss << "error parsing 'gid' value '"
         << cmd_vartype_stringify(cmdmap["gid"]) << "'";
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
        stringstream ss;
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
    cmd_getval(g_ceph_context, cmdmap, "who", role_str);
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

    stringstream ss;
    ss << "removed failed mds." << role;
    return 0;
  } else if (prefix == "mds compat rm_compat") {
    int64_t f;
    if (!cmd_getval(g_ceph_context, cmdmap, "feature", f)) {
      ss << "error parsing feature value '"
         << cmd_vartype_stringify(cmdmap["feature"]) << "'";
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
         << cmd_vartype_stringify(cmdmap["feature"]) << "'";
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
    cmd_getval(g_ceph_context, cmdmap, "rank", role_str);
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

/**
 * Helper to legacy_filesystem_command
 */
void MDSMonitor::modify_legacy_filesystem(
    std::function<void(std::shared_ptr<Filesystem> )> fn)
{
  pending_fsmap.modify_filesystem(
    pending_fsmap.legacy_client_fscid,
    fn
  );
}



/**
 * Handle a command that affects the filesystem (i.e. a filesystem
 * must exist for the command to act upon).
 *
 * @retval 0        Command was successfully handled and has side effects
 * @retval -EAGAIN  Messages has been requeued for retry
 * @retval -ENOSYS  Unknown command
 * @retval < 0      An error has occurred; **ss** may have been set.
 */
int MDSMonitor::legacy_filesystem_command(
    MonOpRequestRef op,
    std::string const &prefix,
    map<string, cmd_vartype> &cmdmap,
    std::stringstream &ss)
{
  dout(4) << __func__ << " prefix='" << prefix << "'" << dendl;
  op->mark_mdsmon_event(__func__);
  int r = 0;
  string whostr;
  cmd_getval(g_ceph_context, cmdmap, "who", whostr);

  assert (pending_fsmap.legacy_client_fscid != FS_CLUSTER_ID_NONE);

  if (prefix == "mds set_max_mds") {
    // NOTE: deprecated by "fs set max_mds"
    int64_t maxmds;
    if (!cmd_getval(g_ceph_context, cmdmap, "maxmds", maxmds) || maxmds < 0) {
      return -EINVAL;
    }
    if (maxmds > MAX_MDS) {
      ss << "may not have more than " << MAX_MDS << " MDS ranks";
      return -EINVAL;
    }

    modify_legacy_filesystem(
        [maxmds](std::shared_ptr<Filesystem> fs)
    {
      fs->mds_map.set_max_mds(maxmds);
    });

    r = 0;
    ss << "max_mds = " << maxmds;
  } else if (prefix == "mds cluster_down") {
    // NOTE: deprecated by "fs set cluster_down"
    modify_legacy_filesystem(
        [](std::shared_ptr<Filesystem> fs)
    {
      fs->mds_map.set_flag(CEPH_MDSMAP_DOWN);
    });
    ss << "marked fsmap DOWN";
    r = 0;
  } else if (prefix == "mds cluster_up") {
    // NOTE: deprecated by "fs set cluster_up"
    modify_legacy_filesystem(
        [](std::shared_ptr<Filesystem> fs)
    {
      fs->mds_map.clear_flag(CEPH_MDSMAP_DOWN);
    });
    ss << "unmarked fsmap DOWN";
    r = 0;
  } else {
    return -ENOSYS;
  }

  return r;
}


void MDSMonitor::check_subs()
{
  std::list<std::string> types;

  // Subscriptions may be to "fsmap" (MDS and legacy clients),
  // "fsmap.<namespace>", or to "fsmap" for the full state of all
  // filesystems.  Build a list of all the types we service
  // subscriptions for.
  types.push_back("mdsmap");
  types.push_back("fsmap");
  for (const auto &i : fsmap.filesystems) {
    auto fscid = i.first;
    std::ostringstream oss;
    oss << "fsmap." << fscid;
    types.push_back(oss.str());
  }

  for (const auto &type : types) {
    if (mon->session_map.subs.count(type) == 0)
      return;
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
  } else {
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
      if (sub->type.find("mdsmap.") == 0) {
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

    dout(10) << __func__ << " selected MDS map epoch " <<
      mds_map->epoch << " for namespace " << fscid << " for subscriber "
      << sub->session->inst.name << " who wants epoch " << sub->next << dendl;

    assert(mds_map != nullptr);
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
  ::encode(pending_metadata, bl);
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
  ::encode(pending_metadata, bl);
  t->put(MDS_METADATA_PREFIX, "last_metadata", bl);
}

int MDSMonitor::load_metadata(map<mds_gid_t, Metadata>& m)
{
  bufferlist bl;
  int r = mon->store->get(MDS_METADATA_PREFIX, "last_metadata", bl);
  if (r)
    return r;

  bufferlist::iterator it = bl.begin();
  ::decode(m, it);
  return 0;
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
    mds_gid_t newgid = pending_fsmap.find_replacement_for({fs->fscid, mds}, name,
                         g_conf->mon_force_standby_active);
    if (newgid == MDS_GID_NONE) {
      break;
    }

    dout(1) << "adding standby " << pending_fsmap.get_info_gid(newgid).addr
            << " as mds." << mds << dendl;
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
void MDSMonitor::maybe_replace_gid(mds_gid_t gid,
    const beacon_info_t &beacon,
    bool *mds_propose, bool *osd_propose)
{
  assert(mds_propose != nullptr);
  assert(osd_propose != nullptr);

  const MDSMap::mds_info_t info = pending_fsmap.get_info_gid(gid);
  const auto fscid = pending_fsmap.mds_roles.at(gid);

  dout(10) << "no beacon from " << gid << " " << info.addr << " mds."
    << info.rank << "." << info.inc
    << " " << ceph_mds_state_name(info.state)
    << " since " << beacon.stamp << dendl;

  // are we in?
  // and is there a non-laggy standby that can take over for us?
  mds_gid_t sgid;
  if (info.rank >= 0 &&
      info.state != MDSMap::STATE_STANDBY &&
      info.state != MDSMap::STATE_STANDBY_REPLAY &&
      !pending_fsmap.get_filesystem(fscid)->mds_map.test_flag(CEPH_MDSMAP_DOWN) &&
      (sgid = pending_fsmap.find_replacement_for({fscid, info.rank}, info.name,
                g_conf->mon_force_standby_active)) != MDS_GID_NONE) {
    
    MDSMap::mds_info_t si = pending_fsmap.get_info_gid(sgid);
    dout(10) << " replacing " << gid << " " << info.addr << " mds."
      << info.rank << "." << info.inc
      << " " << ceph_mds_state_name(info.state)
      << " with " << sgid << "/" << si.name << " " << si.addr << dendl;

    // Remember what NS the old one was in
    const fs_cluster_id_t fscid = pending_fsmap.mds_roles.at(gid);

    // Remove the old one
    *osd_propose |= fail_mds_gid(gid);

    // Promote the replacement
    auto fs = pending_fsmap.filesystems.at(fscid);
    pending_fsmap.promote(sgid, fs, info.rank);

    *mds_propose = true;
  } else if (info.state == MDSMap::STATE_STANDBY_REPLAY) {
    dout(10) << " failing " << gid << " " << info.addr << " mds." << info.rank << "." << info.inc
      << " " << ceph_mds_state_name(info.state)
      << dendl;
    fail_mds_gid(gid);
    *mds_propose = true;
  } else {
    if (info.state == MDSMap::STATE_STANDBY ||
        info.state == MDSMap::STATE_STANDBY_REPLAY) {
      // remove it
      dout(10) << " removing " << gid << " " << info.addr << " mds." << info.rank << "." << info.inc
        << " " << ceph_mds_state_name(info.state)
        << " (laggy)" << dendl;
      fail_mds_gid(gid);
      *mds_propose = true;
    } else if (!info.laggy()) {
      dout(10) << " marking " << gid << " " << info.addr << " mds." << info.rank << "." << info.inc
        << " " << ceph_mds_state_name(info.state)
        << " laggy" << dendl;
      pending_fsmap.modify_daemon(info.global_id, [](MDSMap::mds_info_t *info) {
          info->laggy_since = ceph_clock_now(g_ceph_context);
      });
      *mds_propose = true;
    }
    last_beacon.erase(gid);
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
        pending_fsmap.promote(sgid, fs, f);
	do_propose = true;
      }
    }
  }

  // There were no failures to replace, so try using any available standbys
  // as standby-replay daemons.
  if (failed.empty()) {
    for (const auto &j : pending_fsmap.standby_daemons) {
      const auto &gid = j.first;
      const auto &info = j.second;
      assert(info.state == MDSMap::STATE_STANDBY);

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
          info.standby_for_ns == FS_CLUSTER_ID_NONE ?
            pending_fsmap.legacy_client_fscid : info.standby_for_ns,
          info.standby_for_rank};

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

            if (info.standby_for_rank == MDSMap::MDS_STANDBY_ANY &&
                try_standby_replay(info, *(fs_i.second), cand_info)) {
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

  // expand mds cluster (add new nodes to @in)?
  for (auto i : pending_fsmap.filesystems) {
    do_propose |= maybe_expand_cluster(i.second);
  }

  // check beacon timestamps
  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t cutoff = now;
  cutoff -= g_conf->mds_beacon_grace;

  // make sure last_beacon is fully populated
  for (const auto &p : pending_fsmap.mds_roles) {
    auto &gid = p.first;
    if (last_beacon.count(gid) == 0) {
      last_beacon[gid].stamp = ceph_clock_now(g_ceph_context);
      last_beacon[gid].seq = 0;
    }
  }

  // If the OSDMap is writeable, we can blacklist things, so we can
  // try failing any laggy MDS daemons.  Consider each one for failure.
  if (mon->osdmon()->is_writeable()) {
    bool propose_osdmap = false;

    map<mds_gid_t, beacon_info_t>::iterator p = last_beacon.begin();
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
        maybe_replace_gid(gid, beacon_info, &do_propose, &propose_osdmap);
      }
    }

    if (propose_osdmap) {
      request_proposal(mon->osdmon());
    }
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
  handlers.push_back(std::make_shared<SetHandler>());
  handlers.push_back(std::make_shared<LegacyHandler<SetHandler> >("mds set"));
  handlers.push_back(std::make_shared<FlagSetHandler>());
  handlers.push_back(std::make_shared<AddDataPoolHandler>());
  handlers.push_back(std::make_shared<LegacyHandler<AddDataPoolHandler> >(
        "mds add_data_pool"));
  handlers.push_back(std::make_shared<RemoveDataPoolHandler>());
  handlers.push_back(std::make_shared<LegacyHandler<RemoveDataPoolHandler> >(
        "mds remove_data_pool"));
}

