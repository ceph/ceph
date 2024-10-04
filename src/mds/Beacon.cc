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
#include "common/likely.h"
#include "common/HeartbeatMap.h"

#include "include/stringify.h"
#include "include/util.h"

#include "mon/MonClient.h"
#include "mds/MDLog.h"
#include "mds/MDSRank.h"
#include "mds/MDSMap.h"
#include "mds/Locker.h"

#include "Beacon.h"

#include <chrono>

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_mds
#undef dout_prefix
#define dout_prefix *_dout << "mds.beacon." << name << ' '

using std::map;
using std::string;

using namespace std::chrono_literals;

Beacon::Beacon(CephContext *cct, MonClient *monc, std::string_view name)
  :
    Dispatcher(cct),
    beacon_interval(g_conf()->mds_beacon_interval),
    monc(monc),
    name(name),
    compat(MDSMap::get_compat_set_all())
{
}

Beacon::~Beacon()
{
  shutdown();
}

void Beacon::shutdown()
{
  std::unique_lock<std::mutex> lock(mutex);
  if (!finished) {
    finished = true;
    lock.unlock();
    if (sender.joinable())
      sender.join();
  }
}

void Beacon::init(const MDSMap &mdsmap)
{
  std::unique_lock lock(mutex);

  _notify_mdsmap(mdsmap);

  sender = std::thread([this]() {
    std::unique_lock<std::mutex> lock(mutex);
    bool sent;
    while (!finished) {
      auto now = clock::now();
      auto since = std::chrono::duration<double>(now-last_send).count();
      auto interval = beacon_interval;
      sent = false;
      if (since >= interval*.90) {
        if (!_send()) {
          interval = 0.5; /* 500ms */
        }
        else {
          sent = true;
        }
      } else {
        interval -= since;
      }
      dout(20) << "sender thread waiting interval " << interval << "s" << dendl;
      if (cvar.wait_for(lock, interval*1s) == std::cv_status::timeout) {
        if (sent) {
          //missed beacon ack because we timedout after a beacon send
          dout(0) << "missed beacon ack from the monitors" << dendl;
          missed_beacon_ack_dump = true;
        }
      }
    }
  });
}

bool Beacon::ms_dispatch2(const ref_t<Message>& m)
{
  dout(25) << __func__ << ": processing " << m << dendl;
  if (m->get_type() == MSG_MDS_BEACON) {
    if (m->get_connection()->get_peer_type() == CEPH_ENTITY_TYPE_MON) {
      handle_mds_beacon(ref_cast<MMDSBeacon>(m));
    }
    return true;
  }

  return false;
}


/**
 * Update lagginess state based on response from remote MDSMonitor
 */
void Beacon::handle_mds_beacon(const cref_t<MMDSBeacon> &m)
{
  std::unique_lock lock(mutex);

  version_t seq = m->get_seq();

  // update lab
  auto it = seq_stamp.find(seq);
  if (it != seq_stamp.end()) {
    auto now = clock::now();

    last_acked_stamp = it->second;
    auto rtt = std::chrono::duration<double>(now - last_acked_stamp).count();

    dout(5) << "received beacon reply " << ceph_mds_state_name(m->get_state()) << " seq " << m->get_seq() << " rtt " << rtt << dendl;

    if (laggy && rtt < g_conf()->mds_beacon_grace) {
      dout(0) << " MDS is no longer laggy" << dendl;
      laggy = false;
      last_laggy = now;
    }

    // clean up seq_stamp map
    seq_stamp.erase(seq_stamp.begin(), ++it);

    // Wake a waiter up if present
    cvar.notify_all();
  } else {
    dout(1) << "discarding unexpected beacon reply " << ceph_mds_state_name(m->get_state())
	    << " seq " << m->get_seq() << " dne" << dendl;
  }
}


void Beacon::send()
{
  std::unique_lock lock(mutex);
  _send();
}


void Beacon::send_and_wait(const double duration)
{
  std::unique_lock lock(mutex);
  _send();
  auto awaiting_seq = last_seq;
  dout(20) << __func__ << ": awaiting " << awaiting_seq
           << " for up to " << duration << "s" << dendl;

  auto start = clock::now();
  while (!seq_stamp.empty() && seq_stamp.begin()->first <= awaiting_seq) {
    auto now = clock::now();
    auto s = duration*.95-std::chrono::duration<double>(now-start).count();
    if (s < 0) {
      //missed beacon ACKs
      missed_beacon_ack_dump = true;
      break;
    }
    cvar.wait_for(lock, s*1s);
  }
}


/**
 * Call periodically, or when you have updated the desired state
 */
bool Beacon::_send()
{
  auto now = clock::now();
  auto since = std::chrono::duration<double>(now-last_acked_stamp).count();

  if (!cct->get_heartbeat_map()->is_healthy()) {
    /* If anything isn't progressing, let avoid sending a beacon so that
     * the MDS will consider us laggy */
    dout(0) << "Skipping beacon heartbeat to monitors (last acked " << since << "s ago); MDS internal heartbeat is not healthy!" << dendl;
    //missed internal heartbeat
    missed_internal_heartbeat_dump = true;
    return false;
  }

  ++last_seq;
  dout(5) << "Sending beacon " << ceph_mds_state_name(want_state) << " seq " << last_seq << dendl;

  seq_stamp[last_seq] = now;

  ceph_assert(want_state != MDSMap::STATE_NULL);
  
  auto beacon = make_message<MMDSBeacon>(
      monc->get_fsid(), mds_gid_t(monc->get_global_id()),
      name,
      epoch,
      want_state,
      last_seq,
      CEPH_FEATURES_SUPPORTED_DEFAULT);
  beacon->set_health(health);
  beacon->set_compat(compat);
  beacon->set_fs(g_conf().get_val<std::string>("mds_join_fs"));
  // piggyback the sys info on beacon msg
  if (want_state == MDSMap::STATE_BOOT) {
    map<string, string> sys_info;
    collect_sys_info(&sys_info, cct);
    sys_info["addr"] = stringify(monc->get_myaddrs());
    beacon->set_sys_info(sys_info);
  }
  monc->send_mon_message(beacon.detach());
  last_send = now;
  return true;
}

/**
 * Call this when there is a new MDSMap available
 */
void Beacon::notify_mdsmap(const MDSMap &mdsmap)
{
  std::unique_lock lock(mutex);

  _notify_mdsmap(mdsmap);
}

void Beacon::_notify_mdsmap(const MDSMap &mdsmap)
{
  ceph_assert(mdsmap.get_epoch() >= epoch);

  if (mdsmap.get_epoch() >= epoch) {
    epoch = mdsmap.get_epoch();
  }
}


bool Beacon::is_laggy()
{
  std::unique_lock lock(mutex);

  auto now = clock::now();
  auto since = std::chrono::duration<double>(now-last_acked_stamp).count();
  if (since > g_conf()->mds_beacon_grace) {
    if (!laggy) {
      dout(1) << "MDS connection to Monitors appears to be laggy; " << since
	      << "s since last acked beacon" << dendl;
    }
    laggy = true;
    return true;
  }
  return false;
}

void Beacon::set_want_state(const MDSMap &mdsmap, MDSMap::DaemonState newstate)
{
  std::unique_lock lock(mutex);

  // Update mdsmap epoch atomically with updating want_state, so that when
  // we send a beacon with the new want state it has the latest epoch, and
  // once we have updated to the latest epoch, we are not sending out
  // a stale want_state (i.e. one from before making it through MDSMap
  // handling)
  _notify_mdsmap(mdsmap);

  if (want_state != newstate) {
    dout(5) << __func__ << ": "
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
  std::unique_lock lock(mutex);
  if (!mds) {
    // No MDS rank held
    return;
  }

  // I'm going to touch this MDS, so it must be locked
  ceph_assert(ceph_mutex_is_locked_by_me(mds->mds_lock));

  health.metrics.clear();

  if (unlikely(g_conf().get_val<bool>("mds_inject_health_dummy"))) {
    MDSHealthMetric m(MDS_HEALTH_DUMMY, HEALTH_ERR, std::string("dummy"));
    health.metrics.push_back(m);
  }

  // Detect presence of entries in DamageTable
  if (!mds->damage_table.empty()) {
    MDSHealthMetric m(MDS_HEALTH_DAMAGE, HEALTH_ERR, std::string(
          "Metadata damage detected"));
    health.metrics.push_back(m);
  }

  // Detect MDS_HEALTH_TRIM condition
  // Indicates MDS is not trimming promptly
  {
    const auto log_max_segments = mds->mdlog->get_max_segments();
    const auto log_warn_factor = g_conf().get_val<double>("mds_log_warn_factor");
    if (mds->mdlog->get_num_segments() > (size_t)(log_max_segments * log_warn_factor)) {
      CachedStackStringStream css;
      *css << "Behind on trimming (" << mds->mdlog->get_num_segments()
        << "/" << log_max_segments << ")";

      MDSHealthMetric m(MDS_HEALTH_TRIM, HEALTH_WARN, css->strv());
      m.metadata["num_segments"] = stringify(mds->mdlog->get_num_segments());
      m.metadata["max_segments"] = stringify(log_max_segments);
      health.metrics.push_back(m);
    }
  }

  // Detect clients failing to respond to modifications to capabilities in
  // CLIENT_CAPS messages.
  {
    auto&& late_clients = mds->locker->get_late_revoking_clients(mds->mdsmap->get_session_timeout());
    std::vector<MDSHealthMetric> late_cap_metrics;

    for (const auto& client : late_clients) {
      // client_t is equivalent to session.info.inst.name.num
      // Construct an entity_name_t to lookup into SessionMap
      entity_name_t ename(CEPH_ENTITY_TYPE_CLIENT, client.v);
      Session const *s = mds->sessionmap.get_session(ename);
      if (s == NULL) {
        // Shouldn't happen, but not worth crashing if it does as this is
        // just health-reporting code.
        derr << "Client ID without session: " << client.v << dendl;
        continue;
      }

      CachedStackStringStream css;
      *css << "Client " << s->get_human_name() << " failing to respond to capability release";
      MDSHealthMetric m(MDS_HEALTH_CLIENT_LATE_RELEASE, HEALTH_WARN, css->strv());
      m.metadata["client_id"] = stringify(client.v);
      late_cap_metrics.emplace_back(std::move(m));
    }

    if (late_cap_metrics.size() <= (size_t)g_conf()->mds_health_summarize_threshold) {
      auto&& m = late_cap_metrics;
      health.metrics.insert(std::end(health.metrics), std::cbegin(m), std::cend(m));
    } else {
      CachedStackStringStream css;
      *css << "Many clients (" << late_cap_metrics.size()
          << ") failing to respond to capability release";
      MDSHealthMetric m(MDS_HEALTH_CLIENT_LATE_RELEASE_MANY, HEALTH_WARN, css->strv());
      m.metadata["client_count"] = stringify(late_cap_metrics.size());
      health.metrics.push_back(std::move(m));
    }
  }

  // Detect clients failing to generate cap releases from CEPH_SESSION_RECALL_STATE
  // messages. May be due to buggy client or resource-hogging application.
  //
  // Detect clients failing to advance their old_client_tid
  {
    std::set<Session*> sessions;
    mds->sessionmap.get_client_session_set(sessions);

    const auto min_caps_working_set = g_conf().get_val<uint64_t>("mds_min_caps_working_set");
    const auto recall_warning_threshold = g_conf().get_val<Option::size_t>("mds_recall_warning_threshold");
    const auto max_completed_requests = g_conf()->mds_max_completed_requests;
    const auto max_completed_flushes = g_conf()->mds_max_completed_flushes;
    std::vector<MDSHealthMetric> late_recall_metrics;
    std::vector<MDSHealthMetric> large_completed_requests_metrics;
    for (auto& session : sessions) {
      const uint64_t num_caps = session->get_num_caps();
      const uint64_t recall_caps = session->get_recall_caps();
      if (recall_caps > recall_warning_threshold && num_caps > min_caps_working_set) {
        dout(2) << "Session " << *session <<
             " is not releasing caps fast enough. Recalled caps at " << recall_caps
          << " > " << recall_warning_threshold << " (mds_recall_warning_threshold)." << dendl;
        CachedStackStringStream css;
        *css << "Client " << session->get_human_name() << " failing to respond to cache pressure";
        MDSHealthMetric m(MDS_HEALTH_CLIENT_RECALL, HEALTH_WARN, css->strv());
        m.metadata["client_id"] = stringify(session->get_client());
        late_recall_metrics.emplace_back(std::move(m));
      }
      if ((session->get_num_trim_requests_warnings() > 0 &&
	   session->get_num_completed_requests() >= max_completed_requests) ||
	  (session->get_num_trim_flushes_warnings() > 0 &&
	   session->get_num_completed_flushes() >= max_completed_flushes)) {
	CachedStackStringStream css;
	*css << "Client " << session->get_human_name() << " failing to advance its oldest client/flush tid. ";
	MDSHealthMetric m(MDS_HEALTH_CLIENT_OLDEST_TID, HEALTH_WARN, css->strv());
	m.metadata["client_id"] = stringify(session->get_client());
	large_completed_requests_metrics.emplace_back(std::move(m));
      }
    }

    if (late_recall_metrics.size() <= (size_t)g_conf()->mds_health_summarize_threshold) {
      auto&& m = late_recall_metrics;
      health.metrics.insert(std::end(health.metrics), std::cbegin(m), std::cend(m));
    } else {
      CachedStackStringStream css;
      *css << "Many clients (" << late_recall_metrics.size()
          << ") failing to respond to cache pressure";
      MDSHealthMetric m(MDS_HEALTH_CLIENT_RECALL_MANY, HEALTH_WARN, css->strv());
      m.metadata["client_count"] = stringify(late_recall_metrics.size());
      health.metrics.push_back(m);
      late_recall_metrics.clear();
    }

    if (large_completed_requests_metrics.size() <= (size_t)g_conf()->mds_health_summarize_threshold) {
      auto&& m = large_completed_requests_metrics;
      health.metrics.insert(std::end(health.metrics), std::cbegin(m), std::cend(m));
    } else {
      CachedStackStringStream css;
      *css << "Many clients (" << large_completed_requests_metrics.size()
	<< ") failing to advance their oldest client/flush tid";
      MDSHealthMetric m(MDS_HEALTH_CLIENT_OLDEST_TID_MANY, HEALTH_WARN, css->strv());
      m.metadata["client_count"] = stringify(large_completed_requests_metrics.size());
      health.metrics.push_back(m);
      large_completed_requests_metrics.clear();
    }
  }

  // Detect MDS_HEALTH_SLOW_REQUEST condition
  {
    int slow = mds->get_mds_slow_req_count();
    if (slow) {
      dout(20) << slow << " slow request found" << dendl;
      CachedStackStringStream css;
      *css << slow << " slow requests are blocked > " << g_conf()->mds_op_complaint_time << " secs";

      MDSHealthMetric m(MDS_HEALTH_SLOW_REQUEST, HEALTH_WARN, css->strv());
      health.metrics.push_back(m);
    }
  }

  {
    auto complaint_time = g_conf()->osd_op_complaint_time;
    auto now = clock::now();
    auto cutoff = now - ceph::make_timespan(complaint_time);

    std::string count;
    ceph::coarse_mono_time oldest;
    if (MDSIOContextBase::check_ios_in_flight(cutoff, count, oldest)) {
      dout(20) << count << " slow metadata IOs found" << dendl;

      auto oldest_secs = std::chrono::duration<double>(now - oldest).count();
      CachedStackStringStream css;
      *css << count << " slow metadata IOs are blocked > " << complaint_time
	  << " secs, oldest blocked for " << (int64_t)oldest_secs << " secs";

      MDSHealthMetric m(MDS_HEALTH_SLOW_METADATA_IO, HEALTH_WARN, css->strv());
      health.metrics.push_back(m);
    }
  }

  // Report a health warning if we are readonly
  if (mds->mdcache->is_readonly()) {
    MDSHealthMetric m(MDS_HEALTH_READ_ONLY, HEALTH_WARN,
                      "MDS in read-only mode");
    health.metrics.push_back(m);
  }

  // Report a health warning if clients have broken root_squash
  if (auto c = mds->sessionmap.num_broken_root_squash_clients(); c > 0) {
    std::vector<MDSHealthMetric> metrics;

    for (auto&& session : mds->sessionmap.get_broken_root_squash_clients()) {
      CachedStackStringStream css;
      *css << "Client " << session->get_human_name() << " has broken root_squash implementation";
      MDSHealthMetric m(MDS_HEALTH_CLIENTS_BROKEN_ROOTSQUASH, HEALTH_ERR, css->strv());
      m.metadata["client_id"] = stringify(session->get_client());
      metrics.emplace_back(std::move(m));
    }

    if (metrics.size() <= (size_t)g_conf()->mds_health_summarize_threshold) {
      health.metrics.insert(std::end(health.metrics), std::make_move_iterator(std::begin(metrics)), std::make_move_iterator(std::end(metrics)));
    } else {
      CachedStackStringStream css;
      *css << "There are " << c << " clients with broken root_squash implementations";
      dout(20) << css->strv() << dendl;
      MDSHealthMetric m(MDS_HEALTH_CLIENTS_BROKEN_ROOTSQUASH, HEALTH_ERR, css->strv());
      m.metadata["client_count"] = stringify(c);
      health.metrics.push_back(std::move(m));
    }
  }

  // Report if we have significantly exceeded our cache size limit
  if (mds->mdcache->cache_overfull()) {
    CachedStackStringStream css;
    *css << "MDS cache is too large (" << bytes2str(mds->mdcache->cache_size())
        << "/" << bytes2str(mds->mdcache->cache_limit_memory()) << ")";
    // Don't include inode and stray counters in the report for standby-replay
    // MDSs. Since it is standby-replay, both will be zero, which might
    // confuse users.
    if (!mds->is_standby_replay()) {
	*css << "; " << mds->mdcache->num_inodes_with_caps << " inodes in "
	     << "use by clients, " << mds->mdcache->get_num_strays()
	     << " stray files";
    }

    MDSHealthMetric m(MDS_HEALTH_CACHE_OVERSIZED, HEALTH_WARN, css->strv());
    health.metrics.push_back(m);
  }

  // Report laggy client(s) due to laggy OSDs
  {
    bool defer_client_eviction =
    g_conf().get_val<bool>("defer_client_eviction_on_laggy_osds")
    && mds->objecter->with_osdmap([](const OSDMap &map) {
      return map.any_osd_laggy(); });
    auto&& laggy_clients = mds->server->get_laggy_clients();
    if (defer_client_eviction && !laggy_clients.empty()) {
      if (laggy_clients.size() <= (size_t)g_conf()->mds_health_summarize_threshold) {
	std::vector<MDSHealthMetric> laggy_clients_metrics;
	for (const auto& laggy_client: laggy_clients) {
	  CachedStackStringStream css;
	  *css << "Client " << laggy_client << " is laggy; not evicted"
	       << " because some OSD(s) is/are laggy";
	  MDSHealthMetric m(MDS_HEALTH_CLIENTS_LAGGY, HEALTH_WARN, css->strv());
	  laggy_clients_metrics.emplace_back(std::move(m));
	}
	auto&& m = laggy_clients_metrics;
	health.metrics.insert(std::end(health.metrics), std::cbegin(m),
			      std::cend(m));
      } else {
	CachedStackStringStream css;
	*css << "Many client (" << laggy_clients.size()
	     << ") are laggy; not evicting since some OSD(s) are laggy";
	MDSHealthMetric m(MDS_HEALTH_CLIENTS_LAGGY_MANY, HEALTH_WARN, css->strv());
	m.metadata["client_count"] = stringify(laggy_clients.size());
	health.metrics.push_back(std::move(m));
      }
    }
  }
}

MDSMap::DaemonState Beacon::get_want_state() const
{
  std::unique_lock lock(mutex);
  return want_state;
}

