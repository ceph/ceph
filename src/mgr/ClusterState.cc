// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 John Spray <john.spray@inktank.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include "messages/MMgrDigest.h"
#include "messages/MPGStats.h"

#include "mgr/ClusterState.h"

#define dout_subsys ceph_subsys_mgr
#undef dout_prefix
#define dout_prefix *_dout << "mgr " << __func__ << " "

ClusterState::ClusterState(MonClient *monc_, Objecter *objecter_)
  : monc(monc_), objecter(objecter_), lock("ClusterState")
{}

void ClusterState::set_objecter(Objecter *objecter_)
{
  Mutex::Locker l(lock);

  objecter = objecter_;
}

void ClusterState::set_fsmap(FSMap const &new_fsmap)
{
  Mutex::Locker l(lock);

  fsmap = new_fsmap;
}

void ClusterState::load_digest(MMgrDigest *m)
{
  health_json = std::move(m->health_json);
  mon_status_json = std::move(m->mon_status_json);
}

void ClusterState::ingest_pgstats(MPGStats *stats)
{
  Mutex::Locker l(lock);
  PGMap::Incremental pending_inc;
  pending_inc.version = pg_map.version + 1; // to make apply_incremental happy

  const int from = stats->get_orig_source().num();
  bool is_in = false;
  objecter->with_osdmap([&is_in, from](const OSDMap &osd_map){
      is_in = osd_map.is_in(from);
  });

  if (is_in) {
    pending_inc.update_stat(from, stats->epoch, stats->osd_stat);
  } else {
    pending_inc.update_stat(from, stats->epoch, osd_stat_t());
  }

  for (auto p : stats->pg_stat) {
    pg_t pgid = p.first;
    const auto &pg_stats = p.second;

    // In case we're hearing about a PG that according to last
    // OSDMap update should not exist
    if (pg_map.pg_stat.count(pgid) == 0) {
      dout(15) << " got " << pgid << " reported at " << pg_stats.reported_epoch << ":"
               << pg_stats.reported_seq
               << " state " << pg_state_string(pg_stats.state)
               << " but DNE in pg_map; pool was probably deleted."
               << dendl;
      continue;
    }

    // In case we already heard about more recent stats from this PG
    // from another OSD
    if (pg_map.pg_stat.count(pgid) &&
        pg_map.pg_stat[pgid].get_version_pair() > pg_stats.get_version_pair()) {
      dout(15) << " had " << pgid << " from " << pg_map.pg_stat[pgid].reported_epoch << ":"
               << pg_map.pg_stat[pgid].reported_seq << dendl;
      continue;
    }

    pending_inc.pg_stat_updates[pgid] = pg_stats;
  }

  pg_map.apply_incremental(g_ceph_context, pending_inc);
}

void ClusterState::notify_osdmap(const OSDMap &osd_map)
{
  Mutex::Locker l(lock);

  PGMap::Incremental pending_inc;
  pending_inc.version = pg_map.version + 1; // to make apply_incremental happy

  PGMapUpdater::update_creating_pgs(osd_map, &pg_map, &pending_inc);
  PGMapUpdater::register_new_pgs(osd_map, &pg_map, &pending_inc);

  pg_map.apply_incremental(g_ceph_context, pending_inc);

  // TODO: Complete the separation of PG state handling so
  // that a cut-down set of functionality remains in PGMonitor
  // while the full-blown PGMap lives only here.
}

