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

  _update_creating_pgs(osd_map, &pending_inc);
  _register_new_pgs(osd_map, &pending_inc);

  pg_map.apply_incremental(g_ceph_context, pending_inc);

  // TODO: Reinstate check_down_pgs logic?
}

void ClusterState::_register_new_pgs(
    const OSDMap &osd_map,
    PGMap::Incremental *pending_inc)
{
  // iterate over crush mapspace
  epoch_t epoch = osd_map.get_epoch();
  dout(10) << "checking pg pools for osdmap epoch " << epoch
           << ", last_pg_scan " << pg_map.last_pg_scan << dendl;

  int created = 0;
  for (const auto & p : osd_map.pools) {
    int64_t poolid = p.first;
    const pg_pool_t &pool = p.second;

    int ruleno = osd_map.crush->find_rule(pool.get_crush_ruleset(),
                                          pool.get_type(), pool.get_size());
    if (ruleno < 0 || !osd_map.crush->rule_exists(ruleno))
      continue;

    if (pool.get_last_change() <= pg_map.last_pg_scan ||
        pool.get_last_change() <= pending_inc->pg_scan) {
      dout(10) << " no change in pool " << poolid << " " << pool << dendl;
      continue;
    }

    dout(10) << "scanning pool " << poolid
             << " " << pool << dendl;

    // first pgs in this pool
    bool new_pool = pg_map.pg_pool_sum.count(poolid) == 0;

    for (ps_t ps = 0; ps < pool.get_pg_num(); ps++) {
      pg_t pgid(ps, poolid, -1);
      if (pg_map.pg_stat.count(pgid)) {
	dout(20) << "register_new_pgs  have " << pgid << dendl;
	continue;
      }
      created++;
      _register_pg(osd_map, pgid, pool.get_last_change(), new_pool,
          pending_inc);
    }
  }

  int removed = 0;
  for (const auto &p : pg_map.creating_pgs) {
    if (p.preferred() >= 0) {
      dout(20) << " removing creating_pg " << p
               << " because it is localized and obsolete" << dendl;
      pending_inc->pg_remove.insert(p);
      removed++;
    }
    if (!osd_map.have_pg_pool(p.pool())) {
      dout(20) << " removing creating_pg " << p
               << " because containing pool deleted" << dendl;
      pending_inc->pg_remove.insert(p);
      ++removed;
    }
  }

  // deleted pools?
  for (const auto & p : pg_map.pg_stat) {
    if (!osd_map.have_pg_pool(p.first.pool())) {
      dout(20) << " removing pg_stat " << p.first << " because "
               << "containing pool deleted" << dendl;
      pending_inc->pg_remove.insert(p.first);
      ++removed;
    }
    if (p.first.preferred() >= 0) {
      dout(20) << " removing localized pg " << p.first << dendl;
      pending_inc->pg_remove.insert(p.first);
      ++removed;
    }
  }

  // we don't want to redo this work if we can avoid it.
  pending_inc->pg_scan = epoch;

  dout(10) << "register_new_pgs registered " << created << " new pgs, removed "
           << removed << " uncreated pgs" << dendl;
}

void ClusterState::_register_pg(
    const OSDMap &osd_map,
    pg_t pgid, epoch_t epoch,
    bool new_pool,
    PGMap::Incremental *pending_inc)
{
  pg_t parent;
  int split_bits = 0;
  bool parent_found = false;
  if (!new_pool) {
    parent = pgid;
    while (1) {
      // remove most significant bit
      int msb = cbits(parent.ps());
      if (!msb)
	break;
      parent.set_ps(parent.ps() & ~(1<<(msb-1)));
      split_bits++;
      dout(30) << " is " << pgid << " parent " << parent << " ?" << dendl;
      if (pg_map.pg_stat.count(parent) &&
          pg_map.pg_stat[parent].state != PG_STATE_CREATING) {
	dout(10) << "  parent is " << parent << dendl;
	parent_found = true;
	break;
      }
    }
  }

  pg_stat_t &stats = pending_inc->pg_stat_updates[pgid];
  stats.state = PG_STATE_CREATING;
  stats.created = epoch;
  stats.parent = parent;
  stats.parent_split_bits = split_bits;
  stats.mapping_epoch = epoch;

  if (parent_found) {
    pg_stat_t &ps = pg_map.pg_stat[parent];
    stats.last_fresh = ps.last_fresh;
    stats.last_active = ps.last_active;
    stats.last_change = ps.last_change;
    stats.last_peered = ps.last_peered;
    stats.last_clean = ps.last_clean;
    stats.last_unstale = ps.last_unstale;
    stats.last_undegraded = ps.last_undegraded;
    stats.last_fullsized = ps.last_fullsized;
    stats.last_scrub_stamp = ps.last_scrub_stamp;
    stats.last_deep_scrub_stamp = ps.last_deep_scrub_stamp;
    stats.last_clean_scrub_stamp = ps.last_clean_scrub_stamp;
  } else {
    utime_t now = ceph_clock_now(g_ceph_context);
    stats.last_fresh = now;
    stats.last_active = now;
    stats.last_change = now;
    stats.last_peered = now;
    stats.last_clean = now;
    stats.last_unstale = now;
    stats.last_undegraded = now;
    stats.last_fullsized = now;
    stats.last_scrub_stamp = now;
    stats.last_deep_scrub_stamp = now;
    stats.last_clean_scrub_stamp = now;
  }

  osd_map.pg_to_up_acting_osds(
    pgid,
    &stats.up,
    &stats.up_primary,
    &stats.acting,
    &stats.acting_primary);

  if (split_bits == 0) {
    dout(10) << " will create " << pgid
             << " primary " << stats.acting_primary
             << " acting " << stats.acting
             << dendl;
  } else {
    dout(10) << " will create " << pgid
             << " primary " << stats.acting_primary
             << " acting " << stats.acting
             << " parent " << parent
             << " by " << split_bits << " bits"
             << dendl;
  }
}

// This was PGMonitor::map_pg_creates
void ClusterState::_update_creating_pgs(
    const OSDMap &osd_map,
    PGMap::Incremental *pending_inc)
{
  assert(pending_inc != nullptr);

  dout(10) << "to " << pg_map.creating_pgs.size()
           << " pgs, osdmap epoch " << osd_map.get_epoch()
           << dendl;

  for (set<pg_t>::const_iterator p = pg_map.creating_pgs.begin();
       p != pg_map.creating_pgs.end();
       ++p) {
    pg_t pgid = *p;
    pg_t on = pgid;
    ceph::unordered_map<pg_t,pg_stat_t>::const_iterator q =
      pg_map.pg_stat.find(pgid);
    assert(q != pg_map.pg_stat.end());
    const pg_stat_t *s = &q->second;

    if (s->parent_split_bits)
      on = s->parent;

    vector<int> up, acting;
    int up_primary, acting_primary;
    osd_map.pg_to_up_acting_osds(
      on,
      &up,
      &up_primary,
      &acting,
      &acting_primary);

    if (up != s->up ||
        up_primary != s->up_primary ||
        acting !=  s->acting ||
        acting_primary != s->acting_primary) {
      pg_stat_t *ns = &pending_inc->pg_stat_updates[pgid];
      dout(20) << pgid << " "
               << " acting_primary: " << s->acting_primary
               << " -> " << acting_primary
               << " acting: " << s->acting << " -> " << acting
               << " up_primary: " << s->up_primary << " -> " << up_primary
               << " up: " << s->up << " -> " << up
               << dendl;

      // only initialize if it wasn't already a pending update
      if (ns->reported_epoch == 0)
        *ns = *s;

      // note epoch if the target of the create message changed
      if (acting_primary != ns->acting_primary)
        ns->mapping_epoch = osd_map.get_epoch();

      ns->up = up;
      ns->up_primary = up_primary;
      ns->acting = acting;
      ns->acting_primary = acting_primary;
    }
  }
}

