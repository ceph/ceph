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

#include "messages/MMDSBeacon.h"
#include "mon/MonClient.h"
#include "mds/MDS.h"
#include "mds/MDLog.h"

#include "Beacon.h"

#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds.beacon." << name << ' '


Beacon::Beacon(CephContext *cct_, MonClient *monc_, std::string name_) :
  Dispatcher(cct_), lock("Beacon"), monc(monc_), timer(g_ceph_context, lock), name(name_)
{
  last_seq = 0;
  sender = NULL;
  was_laggy = false;

  standby_for_rank = MDSMap::MDS_NO_STANDBY_PREF;
  epoch = 0;
}


Beacon::~Beacon()
{
}


void Beacon::init(MDSMap const *mdsmap, MDSMap::DaemonState want_state_, int standby_rank_, std::string const & standby_name_)
{
  Mutex::Locker l(lock);
  assert(mdsmap != NULL);

  // Initialize copies of MDS state
  want_state = want_state_;
  _notify_mdsmap(mdsmap);
  standby_for_rank = standby_rank_;
  standby_for_name = standby_name_;

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
    assert(seq_stamp[seq] > last_acked_stamp);
    last_acked_stamp = seq_stamp[seq];
    utime_t now = ceph_clock_now(g_ceph_context);
    utime_t rtt = now - last_acked_stamp;

    dout(10) << "handle_mds_beacon " << ceph_mds_state_name(m->get_state())
	     << " seq " << m->get_seq() 
	     << " rtt " << rtt << dendl;

    if (was_laggy && rtt < g_conf->mds_beacon_grace) {
      dout(0) << "handle_mds_beacon no longer laggy" << dendl;
      was_laggy = false;
      laggy_until = now;
    }

    // clean up seq_stamp map
    while (!seq_stamp.empty() &&
	   seq_stamp.begin()->first <= seq)
      seq_stamp.erase(seq_stamp.begin());
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
  
  MMDSBeacon *beacon = new MMDSBeacon(
      monc->get_fsid(), monc->get_global_id(),
      name,
      epoch,
      want_state,
      last_seq);

  beacon->set_standby_for_rank(standby_for_rank);
  beacon->set_standby_for_name(standby_for_name);
  beacon->set_health(health);
  beacon->set_compat(compat);

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

  epoch = mdsmap->get_epoch();
  compat = get_mdsmap_compat_set_default();
  compat.merge(mdsmap->compat);
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

void Beacon::notify_want_state(MDSMap::DaemonState const newstate)
{
  Mutex::Locker l(lock);

  want_state = newstate;
}


/**
 * We are 'shown' an MDS briefly in order to update
 * some health metrics that we will send in the next
 * beacon.
 */
void Beacon::notify_health(MDS const *mds)
{
  Mutex::Locker l(lock);

  // I'm going to touch this MDS, so it must be locked
  assert(mds->mds_lock.is_locked_by_me());

  health.metrics.clear();

  // Detect MDS_HEALTH_TRIM condition
  // Arbitrary factor of 2, indicates MDS is not trimming promptly
  if (mds->mdlog->get_num_segments() > (size_t)(g_conf->mds_log_max_segments * 2)) {
    std::ostringstream oss;
    oss << "Behind on trimming (" << mds->mdlog->get_num_segments()
      << "/" << g_conf->mds_log_max_segments << ")";

    MDSHealthMetric m(MDS_HEALTH_TRIM, HEALTH_WARN, oss.str());
    m.metadata["num_segments"] = mds->mdlog->get_num_segments();
    m.metadata["max_segments"] = g_conf->mds_log_max_segments;
    health.metrics.push_back(m);
  }
}

