// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2012 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */


#include "common/dout.h"
#include "common/HeartbeatMap.h"
#include "include/stringify.h"
#include "include/util.h"

#include "messages/MMDSBeacon.h"
#include "mon/MonClient.h"
#include "mds/MDLog.h"
#include "mds/MDSRank.h"
#include "mds/MDSMap.h"
#include "mds/Locker.h"

#include "Beacon.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds.beacon." << name << ' '


Beacon::Beacon(CephContext *cct_, MonClient *monc_, std::string name_) :
  Dispatcher(cct_), lock("Beacon"), monc(monc_), timer(g_ceph_context, lock),
  name(name_), standby_for_rank(MDS_RANK_NONE),
  standby_for_fscid(FS_CLUSTER_ID_NONE), want_state(MDSMap::STATE_BOOT),
  awaiting_seq(-1)
{
  last_seq = 0;
  sender = NULL;
  was_laggy = false;

  epoch = 0;
}


Beacon::~Beacon()
{
}


void Beacon::init(MDSMap const *mdsmap)
{
  Mutex::Locker l(lock);
  assert(mdsmap != NULL);

  _notify_mdsmap(mdsmap);
  standby_for_rank = mds_rank_t(g_conf->mds_standby_for_rank);
  standby_for_name = g_conf->mds_standby_for_name;
  standby_for_fscid = fs_cluster_id_t(g_conf->mds_standby_for_fscid);
  standby_replay = g_conf->mds_standby_replay;

  // Spawn threads and start messaging
  timer.init();
  _send();
}


void Beacon::shutdown()
{
  Mutex::Locker l(lock);
  if (sender) {
    timer.cancel_event(sender);
    sender = NULL;
  }
  timer.shutdown();
}


bool Beacon::ms_dispatch(Message *m)
{
  if (m->get_type() == MSG_MDS_BEACON) {
    if (m->get_connection()->get_peer_type() == CEPH_ENTITY_TYPE_MON) {
      handle_mds_beacon(static_cast<MMDSBeacon*>(m));
    }
    return true;
  }

  return false;
}


/**
 * Update lagginess state based on response from remote MDSMonitor
 *
 * This function puts the passed message before returning
 */
void Beacon::handle_mds_beacon(MMDSBeacon *m)
{
  Mutex::Locker l(lock);
  assert(m != NULL);

  version_t seq = m->get_seq();

  // update lab
  if (seq_stamp.count(seq)) {
    utime_t now = ceph_clock_now(g_ceph_context);
    if (seq_stamp[seq] > last_acked_stamp) {
      last_acked_stamp = seq_stamp[seq];
      utime_t rtt = now - last_acked_stamp;

      dout(10) << "handle_mds_beacon " << ceph_mds_state_name(m->get_state())
	       << " seq " << m->get_seq() << " rtt " << rtt << dendl;

      if (was_laggy && rtt < g_conf->mds_beacon_grace) {
	dout(0) << "handle_mds_beacon no longer laggy" << dendl;
	was_laggy = false;
	laggy_until = now;
      }
    } else {
      // Mark myself laggy if system clock goes backwards. Hopping
      // later beacons will clear it.
      dout(1) << "handle_mds_beacon system clock goes backwards, "
	      << "mark myself laggy" << dendl;
      last_acked_stamp = now - utime_t(g_conf->mds_beacon_grace + 1, 0);
      was_laggy = true;
    }

    // clean up seq_stamp map
    while (!seq_stamp.empty() &&
	   seq_stamp.begin()->first <= seq)
      seq_stamp.erase(seq_stamp.begin());

    // Wake a waiter up if present
    if (awaiting_seq == seq) {
      waiting_cond.Signal();
    }
  } else {
    dout(10) << "handle_mds_beacon " << ceph_mds_state_name(m->get_state())
	     << " seq " << m->get_seq() << " dne" << dendl;
  }
}


void Beacon::send()
{
  Mutex::Locker l(lock);
  _send();
}


void Beacon::send_and_wait(const double duration)
{
  Mutex::Locker l(lock);
  _send();
  awaiting_seq = last_seq;
  dout(20) << __func__ << ": awaiting " << awaiting_seq
           << " for up to " << duration << "s" << dendl;

  utime_t timeout;
  timeout.set_from_double(ceph_clock_now(cct) + duration);
  while ((!seq_stamp.empty() && seq_stamp.begin()->first <= awaiting_seq)
         && ceph_clock_now(cct) < timeout) {
    waiting_cond.WaitUntil(lock, timeout);
  }

  awaiting_seq = -1;
}


/**
 * Call periodically, or when you have updated the desired state
 */
void Beacon::_send()
{
  if (sender) {
    timer.cancel_event(sender);
  }
  sender = new C_MDS_BeaconSender(this);
  timer.add_event_after(g_conf->mds_beacon_interval, sender);

  if (!cct->get_heartbeat_map()->is_healthy()) {
    /* If anything isn't progressing, let avoid sending a beacon so that
     * the MDS will consider us laggy */
    dout(1) << __func__ << " skipping beacon, heartbeat map not healthy" << dendl;
    return;
  }

  ++last_seq;
  dout(10) << __func__ << " " << ceph_mds_state_name(want_state)
	   << " seq " << last_seq
	   << dendl;

  seq_stamp[last_seq] = ceph_clock_now(g_ceph_context);

  assert(want_state != MDSMap::STATE_NULL);
  
  MMDSBeacon *beacon = new MMDSBeacon(
      monc->get_fsid(), mds_gid_t(monc->get_global_id()),
      name,
      epoch,
      want_state,
      last_seq,
      CEPH_FEATURES_SUPPORTED_DEFAULT);

  beacon->set_standby_for_rank(standby_for_rank);
  beacon->set_standby_for_name(standby_for_name);
  beacon->set_standby_for_fscid(standby_for_fscid);
  beacon->set_standby_replay(standby_replay);
  beacon->set_health(health);
  beacon->set_compat(compat);
  // piggyback the sys info on beacon msg
  if (want_state == MDSMap::STATE_BOOT) {
    map<string, string> sys_info;
    collect_sys_info(&sys_info, cct);
    beacon->set_sys_info(sys_info);
  }
  monc->send_mon_message(beacon);
}

/**
 * Call this when there is a new MDSMap available
 */
void Beacon::notify_mdsmap(MDSMap const *mdsmap)
{
  Mutex::Locker l(lock);
  assert(mdsmap != NULL);

  _notify_mdsmap(mdsmap);
}

void Beacon::_notify_mdsmap(MDSMap const *mdsmap)
{
  assert(mdsmap != NULL);
  assert(mdsmap->get_epoch() >= epoch);

  if (mdsmap->get_epoch() != epoch) {
    epoch = mdsmap->get_epoch();
    compat = get_mdsmap_compat_set_default();
    compat.merge(mdsmap->compat);
  }
}


bool Beacon::is_laggy()
{
  Mutex::Locker l(lock);

  if (last_acked_stamp == utime_t())
    return false;

  utime_t now = ceph_clock_now(g_ceph_context);
  utime_t since = now - last_acked_stamp;
  if (since > g_conf->mds_beacon_grace) {
    dout(5) << "is_laggy " << since << " > " << g_conf->mds_beacon_grace
	    << " since last acked beacon" << dendl;
    was_laggy = true;
    if (since > (g_conf->mds_beacon_grace*2) &&
	now > last_mon_reconnect + g_conf->mds_beacon_interval) {
      // maybe it's not us?
      dout(5) << "initiating monitor reconnect; maybe we're not the slow one"
              << dendl;
      last_mon_reconnect = now;
      monc->reopen_session();
    }
    return true;
  }
  return false;
}

utime_t Beacon::get_laggy_until() const
{
  Mutex::Locker l(lock);

  return laggy_until;
}

void Beacon::set_want_state(MDSMap const *mdsmap, MDSMap::DaemonState const newstate)
{
  Mutex::Locker l(lock);

  // Update mdsmap epoch atomically with updating want_state, so that when
  // we send a beacon with the new want state it has the latest epoch, and
  // once we have updated to the latest epoch, we are not sending out
  // a stale want_state (i.e. one from before making it through MDSMap
  // handling)
  _notify_mdsmap(mdsmap);

  if (want_state != newstate) {
    dout(10) << __func__ << ": "
      << ceph_mds_state_name(want_state) << " -> "
      << ceph_mds_state_name(newstate) << dendl;
    want_state = newstate;
  }
}


/**
 * We are 'shown' an MDS briefly in order to update
 * some health metrics that we will send in the next
 * beacon.
 */
void Beacon::notify_health(MDSRank const *mds)
{
  Mutex::Locker l(lock);
  if (!mds) {
    // No MDS rank held
    return;
  }

  // I'm going to touch this MDS, so it must be locked
  assert(mds->mds_lock.is_locked_by_me());

  health.metrics.clear();

  // Detect presence of entries in DamageTable
  if (!mds->damage_table.empty()) {
    MDSHealthMetric m(MDS_HEALTH_DAMAGE, HEALTH_ERR, std::string(
          "Metadata damage detected"));
    health.metrics.push_back(m);
  }

  // Detect MDS_HEALTH_TRIM condition
  // Arbitrary factor of 2, indicates MDS is not trimming promptly
  {
    if (mds->mdlog->get_num_segments() > (size_t)(g_conf->mds_log_max_segments * 2)) {
      std::ostringstream oss;
      oss << "Behind on trimming (" << mds->mdlog->get_num_segments()
        << "/" << g_conf->mds_log_max_segments << ")";

      MDSHealthMetric m(MDS_HEALTH_TRIM, HEALTH_WARN, oss.str());
      m.metadata["num_segments"] = stringify(mds->mdlog->get_num_segments());
      m.metadata["max_segments"] = stringify(g_conf->mds_log_max_segments);
      health.metrics.push_back(m);
    }
  }

  // Detect clients failing to respond to modifications to capabilities in
  // CLIENT_CAPS messages.
  {
    std::list<client_t> late_clients;
    mds->locker->get_late_revoking_clients(&late_clients);
    std::list<MDSHealthMetric> late_cap_metrics;

    for (std::list<client_t>::iterator i = late_clients.begin(); i != late_clients.end(); ++i) {

      // client_t is equivalent to session.info.inst.name.num
      // Construct an entity_name_t to lookup into SessionMap
      entity_name_t ename(CEPH_ENTITY_TYPE_CLIENT, i->v);
      Session const *s = mds->sessionmap.get_session(ename);
      if (s == NULL) {
        // Shouldn't happen, but not worth crashing if it does as this is
        // just health-reporting code.
        derr << "Client ID without session: " << i->v << dendl;
        continue;
      }

      std::ostringstream oss;
      oss << "Client " << s->get_human_name() << " failing to respond to capability release";
      MDSHealthMetric m(MDS_HEALTH_CLIENT_LATE_RELEASE, HEALTH_WARN, oss.str());
      m.metadata["client_id"] = stringify(i->v);
      late_cap_metrics.push_back(m);
    }

    if (late_cap_metrics.size() <= (size_t)g_conf->mds_health_summarize_threshold) {
      health.metrics.splice(health.metrics.end(), late_cap_metrics);
    } else {
      std::ostringstream oss;
      oss << "Many clients (" << late_cap_metrics.size()
          << ") failing to respond to capability release";
      MDSHealthMetric m(MDS_HEALTH_CLIENT_LATE_RELEASE_MANY, HEALTH_WARN, oss.str());
      m.metadata["client_count"] = stringify(late_cap_metrics.size());
      health.metrics.push_back(m);
      late_cap_metrics.clear();
    }
  }

  // Detect clients failing to generate cap releases from CEPH_SESSION_RECALL_STATE
  // messages. May be due to buggy client or resource-hogging application.
  //
  // Detect clients failing to advance their old_client_tid
  {
    set<Session*> sessions;
    mds->sessionmap.get_client_session_set(sessions);

    utime_t cutoff = ceph_clock_now(g_ceph_context);
    cutoff -= g_conf->mds_recall_state_timeout;
    utime_t last_recall = mds->mdcache->last_recall_state;

    std::list<MDSHealthMetric> late_recall_metrics;
    std::list<MDSHealthMetric> large_completed_requests_metrics;
    for (set<Session*>::iterator i = sessions.begin(); i != sessions.end(); ++i) {
      Session *session = *i;
      if (!session->recalled_at.is_zero()) {
        dout(20) << "Session servicing RECALL " << session->info.inst
          << ": " << session->recalled_at << " " << session->recall_release_count
          << "/" << session->recall_count << dendl;
	if (last_recall < cutoff || session->last_recall_sent < last_recall) {
	  dout(20) << "  no longer recall" << dendl;
	  session->clear_recalled_at();
	} else if (session->recalled_at < cutoff) {
          dout(20) << "  exceeded timeout " << session->recalled_at << " vs. " << cutoff << dendl;
          std::ostringstream oss;
	  oss << "Client " << session->get_human_name() << " failing to respond to cache pressure";
          MDSHealthMetric m(MDS_HEALTH_CLIENT_RECALL, HEALTH_WARN, oss.str());
          m.metadata["client_id"] = stringify(session->info.inst.name.num());
          late_recall_metrics.push_back(m);
        } else {
          dout(20) << "  within timeout " << session->recalled_at << " vs. " << cutoff << dendl;
        }
      }
      if ((session->get_num_trim_requests_warnings() > 0 &&
	   session->get_num_completed_requests() >= g_conf->mds_max_completed_requests) ||
	  (session->get_num_trim_flushes_warnings() > 0 &&
	   session->get_num_completed_flushes() >= g_conf->mds_max_completed_flushes)) {
	std::ostringstream oss;
	oss << "Client " << session->get_human_name() << " failing to advance its oldest client/flush tid";
	MDSHealthMetric m(MDS_HEALTH_CLIENT_OLDEST_TID, HEALTH_WARN, oss.str());
	m.metadata["client_id"] = stringify(session->info.inst.name.num());
	large_completed_requests_metrics.push_back(m);
      }
    }

    if (late_recall_metrics.size() <= (size_t)g_conf->mds_health_summarize_threshold) {
      health.metrics.splice(health.metrics.end(), late_recall_metrics);
    } else {
      std::ostringstream oss;
      oss << "Many clients (" << late_recall_metrics.size()
          << ") failing to respond to cache pressure";
      MDSHealthMetric m(MDS_HEALTH_CLIENT_RECALL_MANY, HEALTH_WARN, oss.str());
      m.metadata["client_count"] = stringify(late_recall_metrics.size());
      health.metrics.push_back(m);
      late_recall_metrics.clear();
    }

    if (large_completed_requests_metrics.size() <= (size_t)g_conf->mds_health_summarize_threshold) {
      health.metrics.splice(health.metrics.end(), large_completed_requests_metrics);
    } else {
      std::ostringstream oss;
      oss << "Many clients (" << large_completed_requests_metrics.size()
	<< ") failing to advance their oldest client/flush tid";
      MDSHealthMetric m(MDS_HEALTH_CLIENT_OLDEST_TID_MANY, HEALTH_WARN, oss.str());
      m.metadata["client_count"] = stringify(large_completed_requests_metrics.size());
      health.metrics.push_back(m);
      large_completed_requests_metrics.clear();
    }
  }

  // Report a health warning if we are readonly
  if (mds->mdcache->is_readonly()) {
    MDSHealthMetric m(MDS_HEALTH_READ_ONLY, HEALTH_WARN,
                      "MDS in read-only mode");
    health.metrics.push_back(m);
  }
}

MDSMap::DaemonState Beacon::get_want_state() const
{
  Mutex::Locker l(lock);
  return want_state;
}

