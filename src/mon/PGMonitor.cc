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


#include "json_spirit/json_spirit.h"
#include "common/debug.h"               // undo damage
#include "PGMonitor.h"
#include "Monitor.h"
#include "MDSMonitor.h"
#include "OSDMonitor.h"
#include "MonitorDBStore.h"

#include "messages/MPGStats.h"
#include "messages/MPGStatsAck.h"
#include "messages/MGetPoolStats.h"
#include "messages/MGetPoolStatsReply.h"

#include "messages/MStatfs.h"
#include "messages/MStatfsReply.h"
#include "messages/MOSDPGCreate.h"
#include "messages/MMonCommand.h"
#include "messages/MOSDScrub.h"

#include "common/Timer.h"
#include "common/Formatter.h"
#include "common/ceph_argparse.h"
#include "common/perf_counters.h"
#include "common/TextTable.h"

#include "include/stringify.h"

#include "osd/osd_types.h"

#include "common/config.h"
#include "common/errno.h"
#include "common/strtol.h"
#include "include/str_list.h"
#include <sstream>
#include <boost/variant.hpp>
#include "common/cmdparse.h"

#define dout_subsys ceph_subsys_mon
#undef dout_prefix
#define dout_prefix _prefix(_dout, mon, pg_map)
static ostream& _prefix(std::ostream *_dout, const Monitor *mon, const PGMap& pg_map) {
  return *_dout << "mon." << mon->name << "@" << mon->rank
                << "(" << mon->get_state_name()
                << ").pg v" << pg_map.version << " ";
}

/*
   Tick function to update the map based on performance every N seconds
 */

void PGMonitor::on_restart()
{
  // clear leader state
  last_sent_pg_create.clear();
  last_osd_report.clear();
}

void PGMonitor::on_active()
{
  if (mon->is_leader()) {
    check_osd_map(mon->osdmon()->osdmap.epoch);
    need_check_down_pgs = true;
  }

  update_logger();

  if (mon->is_leader())
    mon->clog->info() << "pgmap " << pg_map << "\n";
}

void PGMonitor::update_logger()
{
  dout(10) << "update_logger" << dendl;

  mon->cluster_logger->set(l_cluster_osd_bytes, pg_map.osd_sum.kb * 1024ull);
  mon->cluster_logger->set(l_cluster_osd_bytes_used,
                           pg_map.osd_sum.kb_used * 1024ull);
  mon->cluster_logger->set(l_cluster_osd_bytes_avail,
                           pg_map.osd_sum.kb_avail * 1024ull);

  mon->cluster_logger->set(l_cluster_num_pool, pg_map.pg_pool_sum.size());
  mon->cluster_logger->set(l_cluster_num_pg, pg_map.pg_stat.size());

  unsigned active = 0, active_clean = 0, peering = 0;
  for (ceph::unordered_map<int,int>::iterator p = pg_map.num_pg_by_state.begin();
       p != pg_map.num_pg_by_state.end();
       ++p) {
    if (p->first & PG_STATE_ACTIVE) {
      active += p->second;
      if (p->first & PG_STATE_CLEAN)
	active_clean += p->second;
    }
    if (p->first & PG_STATE_PEERING)
      peering += p->second;
  }
  mon->cluster_logger->set(l_cluster_num_pg_active_clean, active_clean);
  mon->cluster_logger->set(l_cluster_num_pg_active, active);
  mon->cluster_logger->set(l_cluster_num_pg_peering, peering);

  mon->cluster_logger->set(l_cluster_num_object, pg_map.pg_sum.stats.sum.num_objects);
  mon->cluster_logger->set(l_cluster_num_object_degraded, pg_map.pg_sum.stats.sum.num_objects_degraded);
  mon->cluster_logger->set(l_cluster_num_object_misplaced, pg_map.pg_sum.stats.sum.num_objects_misplaced);
  mon->cluster_logger->set(l_cluster_num_object_unfound, pg_map.pg_sum.stats.sum.num_objects_unfound);
  mon->cluster_logger->set(l_cluster_num_bytes, pg_map.pg_sum.stats.sum.num_bytes);
}

void PGMonitor::tick()
{
  if (!is_active()) return;

  handle_osd_timeouts();

  if (!pg_map.pg_sum_deltas.empty()) {
    utime_t age = ceph_clock_now(g_ceph_context) - pg_map.stamp;
    if (age > 2 * g_conf->mon_delta_reset_interval) {
      dout(10) << " clearing pg_map delta (" << age << " > " << g_conf->mon_delta_reset_interval << " seconds old)" << dendl;
      pg_map.clear_delta();
    }
  }

  /* If we have deltas for pools, run through pgmap's 'per_pool_sum_delta' and
   * clear any deltas that are old enough.
   *
   * Note that 'per_pool_sum_delta' keeps a pool id as key, and a pair containing
   * the calc'ed stats delta and an absolute timestamp from when those stats were
   * obtained -- the timestamp IS NOT a delta itself.
   */
  if (!pg_map.per_pool_sum_deltas.empty()) {
    ceph::unordered_map<uint64_t,pair<pool_stat_t,utime_t> >::iterator it;
    for (it = pg_map.per_pool_sum_delta.begin();
         it != pg_map.per_pool_sum_delta.end(); ) {
      utime_t age = ceph_clock_now(g_ceph_context) - it->second.second;
      if (age > 2*g_conf->mon_delta_reset_interval) {
	dout(10) << " clearing pg_map delta for pool " << it->first
	         << " (" << age << " > " << g_conf->mon_delta_reset_interval
	         << " seconds old)" << dendl;
	pg_map.per_pool_sum_deltas.erase(it->first);
	pg_map.per_pool_sum_deltas_stamps.erase(it->first);
	pg_map.per_pool_sum_delta.erase((it++)->first);
      } else {
	++it;
      }
    }
  }

  dout(10) << pg_map << dendl;
}

void PGMonitor::create_initial()
{
  dout(10) << "create_initial -- creating initial map" << dendl;
  format_version = 1;
}

void PGMonitor::update_from_paxos(bool *need_bootstrap)
{
  version_t version = get_last_committed();
  if (version == pg_map.version)
    return;

  assert(version >= pg_map.version);

  if (format_version == 0) {
    // old format

    /* Obtain latest full pgmap version, if available and whose version is
     * greater than the current pgmap's version.
     */
    version_t latest_full = get_version_latest_full();
    if ((latest_full > 0) && (latest_full > pg_map.version)) {
      bufferlist latest_bl;
      int err = get_version_full(latest_full, latest_bl);
      assert(err == 0);
      dout(7) << __func__ << " loading latest full pgmap v"
              << latest_full << dendl;
      try {
	PGMap tmp_pg_map;
	bufferlist::iterator p = latest_bl.begin();
	tmp_pg_map.decode(p);
	pg_map = tmp_pg_map;
      } catch (const std::exception& e) {
	dout(0) << __func__ << ": error parsing update: "
	        << e.what() << dendl;
	assert(0 == "update_from_paxos: error parsing update");
	return;
      }
    }

    // walk through incrementals
    while (version > pg_map.version) {
      bufferlist bl;
      int err = get_version(pg_map.version+1, bl);
      assert(err == 0);
      assert(bl.length());

      dout(7) << "update_from_paxos  applying incremental " << pg_map.version+1 << dendl;
      PGMap::Incremental inc;
      try {
	bufferlist::iterator p = bl.begin();
	inc.decode(p);
      } catch (const std::exception &e)   {
	dout(0) << "update_from_paxos: error parsing "
	        << "incremental update: " << e.what() << dendl;
	assert(0 == "update_from_paxos: error parsing incremental update");
	return;
      }

      pg_map.apply_incremental(g_ceph_context, inc);

      dout(10) << pg_map << dendl;

      if (inc.pg_scan)
	last_sent_pg_create.clear();  // reset pg_create throttle timer
    }

  } else if (format_version == 1) {
    // pg/osd keys in leveldb

    // read meta
    epoch_t last_pg_scan = pg_map.last_pg_scan;

    while (version > pg_map.version) {
      // load full state?
      if (pg_map.version == 0) {
	dout(10) << __func__ << " v0, read_full" << dendl;
	read_pgmap_full();
	goto out;
      }

      // incremental state?
      dout(10) << __func__ << " read_incremental" << dendl;
      bufferlist bl;
      int r = get_version(pg_map.version + 1, bl);
      if (r == -ENOENT) {
	dout(10) << __func__ << " failed to read_incremental, read_full" << dendl;
	read_pgmap_full();
	goto out;
      }
      assert(r == 0);
      apply_pgmap_delta(bl);
    }

    read_pgmap_meta();

out:
    if (last_pg_scan != pg_map.last_pg_scan)
      last_sent_pg_create.clear();  // reset pg_create throttle timer
  }

  assert(version == pg_map.version);

  update_logger();
}

void PGMonitor::on_upgrade()
{
  dout(1) << __func__ << " discarding in-core PGMap" << dendl;
  pg_map = PGMap();
}

void PGMonitor::upgrade_format()
{
  unsigned current = 1;
  assert(format_version <= current);
  if (format_version == current)
    return;

  dout(1) << __func__ << " to " << current << dendl;

  // upgrade by dirtying it all
  pg_map.dirty_all(pending_inc);

  format_version = current;
  propose_pending();
}

void PGMonitor::post_paxos_update()
{
  dout(10) << __func__ << dendl;
  if (mon->osdmon()->osdmap.get_epoch()) {
    send_pg_creates();
  }
}

void PGMonitor::handle_osd_timeouts()
{
  if (!mon->is_leader())
    return;

  utime_t now(ceph_clock_now(g_ceph_context));
  utime_t timeo(g_conf->mon_osd_report_timeout, 0);
  if (now - mon->get_leader_since() < timeo) {
    // We haven't been the leader for long enough to consider OSD timeouts
    return;
  }

  if (mon->osdmon()->is_writeable())
    mon->osdmon()->handle_osd_timeouts(now, last_osd_report);
}

void PGMonitor::create_pending()
{
  pending_inc = PGMap::Incremental();
  pending_inc.version = pg_map.version + 1;
  if (pg_map.version == 0) {
    // pull initial values from first leader mon's config
    pending_inc.full_ratio = g_conf->mon_osd_full_ratio;
    if (pending_inc.full_ratio > 1.0)
      pending_inc.full_ratio /= 100.0;
    pending_inc.nearfull_ratio = g_conf->mon_osd_nearfull_ratio;
    if (pending_inc.nearfull_ratio > 1.0)
      pending_inc.nearfull_ratio /= 100.0;
  } else {
    pending_inc.full_ratio = pg_map.full_ratio;
    pending_inc.nearfull_ratio = pg_map.nearfull_ratio;
  }
  dout(10) << "create_pending v " << pending_inc.version << dendl;
}

void PGMonitor::read_pgmap_meta()
{
  dout(10) << __func__ << dendl;

  string prefix = pgmap_meta_prefix;

  version_t version = mon->store->get(prefix, "version");
  epoch_t last_osdmap_epoch = mon->store->get(prefix, "last_osdmap_epoch");
  epoch_t last_pg_scan = mon->store->get(prefix, "last_pg_scan");
  pg_map.set_version(version);
  pg_map.set_last_osdmap_epoch(last_osdmap_epoch);

  if (last_pg_scan != pg_map.get_last_pg_scan()) {
    pg_map.set_last_pg_scan(last_pg_scan);
  }

  float full_ratio, nearfull_ratio;
  {
    bufferlist bl;
    mon->store->get(prefix, "full_ratio", bl);
    bufferlist::iterator p = bl.begin();
    ::decode(full_ratio, p);
  }
  {
    bufferlist bl;
    mon->store->get(prefix, "nearfull_ratio", bl);
    bufferlist::iterator p = bl.begin();
    ::decode(nearfull_ratio, p);
  }
  pg_map.set_full_ratios(full_ratio, nearfull_ratio);
  {
    bufferlist bl;
    mon->store->get(prefix, "stamp", bl);
    bufferlist::iterator p = bl.begin();
    utime_t stamp;
    ::decode(stamp, p);
    pg_map.set_stamp(stamp);
  }
}

void PGMonitor::read_pgmap_full()
{
  read_pgmap_meta();

  string prefix = pgmap_pg_prefix;
  for (KeyValueDB::Iterator i = mon->store->get_iterator(prefix); i->valid(); i->next()) {
    string key = i->key();
    pg_t pgid;
    if (!pgid.parse(key.c_str())) {
      dout(0) << "unable to parse key " << key << dendl;
      continue;
    }
    bufferlist bl = i->value();
    pg_map.update_pg(pgid, bl);
    dout(20) << " got " << pgid << dendl;
  }

  prefix = pgmap_osd_prefix;
  for (KeyValueDB::Iterator i = mon->store->get_iterator(prefix); i->valid(); i->next()) {
    string key = i->key();
    int osd = atoi(key.c_str());
    bufferlist bl = i->value();
    pg_map.update_osd(osd, bl);
    dout(20) << " got osd." << osd << dendl;
  }
}

void PGMonitor::apply_pgmap_delta(bufferlist& bl)
{
  version_t v = pg_map.version + 1;

  utime_t inc_stamp;
  bufferlist dirty_pgs, dirty_osds;
  {
    bufferlist::iterator p = bl.begin();
    ::decode(inc_stamp, p);
    ::decode(dirty_pgs, p);
    ::decode(dirty_osds, p);
  }

  pool_stat_t pg_sum_old = pg_map.pg_sum;
  ceph::unordered_map<uint64_t, pool_stat_t> pg_pool_sum_old;

  // pgs
  set<int64_t> deleted_pools;
  bufferlist::iterator p = dirty_pgs.begin();
  while (!p.end()) {
    pg_t pgid;
    ::decode(pgid, p);

    int r;
    bufferlist pgbl;
    if (deleted_pools.count(pgid.pool())) {
      r = -ENOENT;
    } else {
      r = mon->store->get(pgmap_pg_prefix, stringify(pgid), pgbl);
      if (pg_pool_sum_old.count(pgid.pool()) == 0)
	pg_pool_sum_old[pgid.pool()] = pg_map.pg_pool_sum[pgid.pool()];
    }

    if (r >= 0) {
      pg_map.update_pg(pgid, pgbl);
      dout(20) << " refreshing pg " << pgid
	       << " " << pg_map.pg_stat[pgid].reported_epoch
	       << ":" << pg_map.pg_stat[pgid].reported_seq
	       << " " << pg_state_string(pg_map.pg_stat[pgid].state)
	       << dendl;
    } else {
      dout(20) << " removing pg " << pgid << dendl;
      pg_map.remove_pg(pgid);
      if (pgid.ps() == 0)
	deleted_pools.insert(pgid.pool());
    }
  }

  // osds
  p = dirty_osds.begin();
  while (!p.end()) {
    int32_t osd;
    ::decode(osd, p);
    dout(20) << " refreshing osd." << osd << dendl;
    bufferlist bl;
    int r = mon->store->get(pgmap_osd_prefix, stringify(osd), bl);
    if (r >= 0) {
      pg_map.update_osd(osd, bl);
    } else {
      pg_map.remove_osd(osd);
    }
  }

  pg_map.update_global_delta(g_ceph_context, inc_stamp, pg_sum_old);
  pg_map.update_pool_deltas(g_ceph_context, inc_stamp, pg_pool_sum_old);

  // clean up deleted pools after updating the deltas
  for (set<int64_t>::iterator p = deleted_pools.begin();
       p != deleted_pools.end();
       ++p) {
    dout(20) << " deleted pool " << *p << dendl;
    pg_map.deleted_pool(*p);
  }

  // ok, we're now on the new version
  pg_map.version = v;
}


void PGMonitor::encode_pending(MonitorDBStore::TransactionRef t)
{
  version_t version = pending_inc.version;
  dout(10) << __func__ << " v " << version << dendl;
  assert(get_last_committed() + 1 == version);
  pending_inc.stamp = ceph_clock_now(g_ceph_context);

  uint64_t features = mon->get_quorum_features();

  string prefix = pgmap_meta_prefix;

  t->put(prefix, "version", pending_inc.version);
  {
    bufferlist bl;
    ::encode(pending_inc.stamp, bl);
    t->put(prefix, "stamp", bl);
  }

  if (pending_inc.osdmap_epoch)
    t->put(prefix, "last_osdmap_epoch", pending_inc.osdmap_epoch);
  if (pending_inc.pg_scan)
    t->put(prefix, "last_pg_scan", pending_inc.pg_scan);
  if (pending_inc.full_ratio > 0) {
    bufferlist bl;
    ::encode(pending_inc.full_ratio, bl);
    t->put(prefix, "full_ratio", bl);
  }
  if (pending_inc.nearfull_ratio > 0) {
    bufferlist bl;
    ::encode(pending_inc.nearfull_ratio, bl);
    t->put(prefix, "nearfull_ratio", bl);
  }

  bufferlist incbl;
  ::encode(pending_inc.stamp, incbl);
  {
    bufferlist dirty;
    string prefix = pgmap_pg_prefix;
    for (map<pg_t,pg_stat_t>::const_iterator p = pending_inc.pg_stat_updates.begin();
         p != pending_inc.pg_stat_updates.end();
         ++p) {
      ::encode(p->first, dirty);
      bufferlist bl;
      ::encode(p->second, bl, features);
      t->put(prefix, stringify(p->first), bl);
    }
    for (set<pg_t>::const_iterator p = pending_inc.pg_remove.begin(); p != pending_inc.pg_remove.end(); ++p) {
      ::encode(*p, dirty);
      t->erase(prefix, stringify(*p));
    }
    ::encode(dirty, incbl);
  }
  {
    bufferlist dirty;
    string prefix = pgmap_osd_prefix;
    for (map<int32_t,osd_stat_t>::const_iterator p =
           pending_inc.get_osd_stat_updates().begin();
         p != pending_inc.get_osd_stat_updates().end();
         ++p) {
      ::encode(p->first, dirty);
      bufferlist bl;
      ::encode(p->second, bl, features);
      ::encode(pending_inc.get_osd_epochs().find(p->first)->second, bl);
      t->put(prefix, stringify(p->first), bl);
    }
    for (set<int32_t>::const_iterator p =
           pending_inc.get_osd_stat_rm().begin();
         p != pending_inc.get_osd_stat_rm().end();
         ++p) {
      ::encode(*p, dirty);
      t->erase(prefix, stringify(*p));
    }
    ::encode(dirty, incbl);
  }

  put_version(t, version, incbl);

  put_last_committed(t, version);
}

version_t PGMonitor::get_trim_to()
{
  unsigned max = g_conf->mon_max_pgmap_epochs;
  version_t version = get_last_committed();
  if (mon->is_leader() && (version > max))
    return version - max;

  return 0;
}

bool PGMonitor::preprocess_query(MonOpRequestRef op)
{
  op->mark_pgmon_event(__func__);
  PaxosServiceMessage *m = static_cast<PaxosServiceMessage*>(op->get_req());
  dout(10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case CEPH_MSG_STATFS:
    handle_statfs(op);
    return true;

  case MSG_GETPOOLSTATS:
    return preprocess_getpoolstats(op);

  case MSG_PGSTATS:
    return preprocess_pg_stats(op);

  case MSG_MON_COMMAND:
    return preprocess_command(op);


  default:
    assert(0);
    return true;
  }
}

bool PGMonitor::prepare_update(MonOpRequestRef op)
{
  op->mark_pgmon_event(__func__);
  PaxosServiceMessage *m = static_cast<PaxosServiceMessage*>(op->get_req());
  dout(10) << "prepare_update " << *m << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case MSG_PGSTATS:
    return prepare_pg_stats(op);

  case MSG_MON_COMMAND:
    return prepare_command(op);

  default:
    assert(0);
    return false;
  }
}

void PGMonitor::handle_statfs(MonOpRequestRef op)
{
  op->mark_pgmon_event(__func__);
  MStatfs *statfs = static_cast<MStatfs*>(op->get_req());
  // check caps
  MonSession *session = statfs->get_session();
  if (!session)
    return;

  if (!session->is_capable("pg", MON_CAP_R)) {
    dout(0) << "MStatfs received from entity with insufficient privileges "
            << session->caps << dendl;
    return;
  }
  MStatfsReply *reply;

  dout(10) << "handle_statfs " << *statfs << " from " << statfs->get_orig_source() << dendl;

  if (statfs->fsid != mon->monmap->fsid) {
    dout(0) << "handle_statfs on fsid " << statfs->fsid << " != " << mon->monmap->fsid << dendl;
    return;
  }

  // fill out stfs
  reply = new MStatfsReply(mon->monmap->fsid, statfs->get_tid(), get_last_committed());

  // these are in KB.
  reply->h.st.kb = pg_map.osd_sum.kb;
  reply->h.st.kb_used = pg_map.osd_sum.kb_used;
  reply->h.st.kb_avail = pg_map.osd_sum.kb_avail;
  reply->h.st.num_objects = pg_map.pg_sum.stats.sum.num_objects;

  // reply
  mon->send_reply(op, reply);
}

bool PGMonitor::preprocess_getpoolstats(MonOpRequestRef op)
{
  op->mark_pgmon_event(__func__);
  MGetPoolStats *m = static_cast<MGetPoolStats*>(op->get_req());
  MGetPoolStatsReply *reply;

  MonSession *session = m->get_session();
  if (!session)
    goto out;
  if (!session->is_capable("pg", MON_CAP_R)) {
    dout(0) << "MGetPoolStats received from entity with insufficient caps "
            << session->caps << dendl;
    goto out;
  }

  if (m->fsid != mon->monmap->fsid) {
    dout(0) << "preprocess_getpoolstats on fsid " << m->fsid << " != " << mon->monmap->fsid << dendl;
    goto out;
  }

  reply = new MGetPoolStatsReply(m->fsid, m->get_tid(), get_last_committed());

  for (list<string>::iterator p = m->pools.begin();
       p != m->pools.end();
       ++p) {
    int64_t poolid = mon->osdmon()->osdmap.lookup_pg_pool_name(p->c_str());
    if (poolid < 0)
      continue;
    if (pg_map.pg_pool_sum.count(poolid) == 0)
      continue;
    reply->pool_stats[*p] = pg_map.pg_pool_sum[poolid];
  }

  mon->send_reply(op, reply);

out:
  return true;
}


bool PGMonitor::preprocess_pg_stats(MonOpRequestRef op)
{
  op->mark_pgmon_event(__func__);
  MPGStats *stats = static_cast<MPGStats*>(op->get_req());
  // check caps
  MonSession *session = stats->get_session();
  if (!session) {
    dout(10) << "PGMonitor::preprocess_pg_stats: no monitor session!" << dendl;
    return true;
  }
  if (!session->is_capable("pg", MON_CAP_R)) {
    derr << "PGMonitor::preprocess_pg_stats: MPGStats received from entity "
         << "with insufficient privileges " << session->caps << dendl;
    return true;
  }

  // First, just see if they need a new osdmap. But
  // only if they've had the map for a while.
  if (stats->had_map_for > 30.0 &&
      mon->osdmon()->is_readable() &&
      stats->epoch < mon->osdmon()->osdmap.get_epoch())
    mon->osdmon()->send_latest_now_nodelete(op, stats->epoch+1);

  // Always forward the PGStats to the leader, even if they are the same as
  // the old PGStats. The leader will mark as down osds that haven't sent
  // PGStats for a few minutes.
  return false;
}

bool PGMonitor::pg_stats_have_changed(int from, const MPGStats *stats) const
{
  // any new osd info?
  ceph::unordered_map<int,osd_stat_t>::const_iterator s = pg_map.osd_stat.find(from);
  if (s == pg_map.osd_stat.end())
    return true;

  if (s->second != stats->osd_stat)
    return true;

  // any new pg info?
  for (map<pg_t,pg_stat_t>::const_iterator p = stats->pg_stat.begin();
       p != stats->pg_stat.end(); ++p) {
    ceph::unordered_map<pg_t,pg_stat_t>::const_iterator t = pg_map.pg_stat.find(p->first);
    if (t == pg_map.pg_stat.end())
      return true;

    if (t->second.reported_epoch != p->second.reported_epoch ||
        t->second.reported_seq != p->second.reported_seq)
      return true;
  }

  return false;
}

bool PGMonitor::prepare_pg_stats(MonOpRequestRef op)
{
  op->mark_pgmon_event(__func__);
  MPGStats *stats = static_cast<MPGStats*>(op->get_req());
  dout(10) << "prepare_pg_stats " << *stats << " from " << stats->get_orig_source() << dendl;
  int from = stats->get_orig_source().num();

  if (stats->fsid != mon->monmap->fsid) {
    dout(0) << "prepare_pg_stats on fsid " << stats->fsid << " != " << mon->monmap->fsid << dendl;
    return false;
  }

  last_osd_report[from] = ceph_clock_now(g_ceph_context);

  if (!stats->get_orig_source().is_osd() ||
      !mon->osdmon()->osdmap.is_up(from) ||
      stats->get_orig_source_inst() != mon->osdmon()->osdmap.get_inst(from)) {
    dout(1) << " ignoring stats from non-active osd." << dendl;
    return false;
  }

  if (!pg_stats_have_changed(from, stats)) {
    dout(10) << " message contains no new osd|pg stats" << dendl;
    MPGStatsAck *ack = new MPGStatsAck;
    ack->set_tid(stats->get_tid());
    for (map<pg_t,pg_stat_t>::const_iterator p = stats->pg_stat.begin();
         p != stats->pg_stat.end();
         ++p) {
      ack->pg_stat[p->first] = make_pair(p->second.reported_seq, p->second.reported_epoch);
    }
    mon->send_reply(op, ack);
    return false;
  }

  // osd stat
  if (mon->osdmon()->osdmap.is_in(from)) {
    pending_inc.update_stat(from, stats->epoch, stats->osd_stat);
  } else {
    pending_inc.update_stat(from, stats->epoch, osd_stat_t());
  }

  if (pg_map.osd_stat.count(from))
    dout(10) << " got osd." << from << " " << stats->osd_stat << " (was " << pg_map.osd_stat[from] << ")" << dendl;
  else
    dout(10) << " got osd." << from << " " << stats->osd_stat << " (first report)" << dendl;

  // pg stats
  MPGStatsAck *ack = new MPGStatsAck;
  MonOpRequestRef ack_op = mon->op_tracker.create_request<MonOpRequest>(ack);
  ack->set_tid(stats->get_tid());
  for (map<pg_t,pg_stat_t>::iterator p = stats->pg_stat.begin();
       p != stats->pg_stat.end();
       ++p) {
    pg_t pgid = p->first;
    ack->pg_stat[pgid] = make_pair(p->second.reported_seq, p->second.reported_epoch);

    if (pg_map.pg_stat.count(pgid) &&
        pg_map.pg_stat[pgid].get_version_pair() > p->second.get_version_pair()) {
      dout(15) << " had " << pgid << " from " << pg_map.pg_stat[pgid].reported_epoch << ":"
               << pg_map.pg_stat[pgid].reported_seq << dendl;
      continue;
    }
    if (pending_inc.pg_stat_updates.count(pgid) &&
        pending_inc.pg_stat_updates[pgid].get_version_pair() > p->second.get_version_pair()) {
      dout(15) << " had " << pgid << " from " << pending_inc.pg_stat_updates[pgid].reported_epoch << ":"
               << pending_inc.pg_stat_updates[pgid].reported_seq << " (pending)" << dendl;
      continue;
    }

    if (pg_map.pg_stat.count(pgid) == 0) {
      dout(15) << " got " << pgid << " reported at " << p->second.reported_epoch << ":"
               << p->second.reported_seq
               << " state " << pg_state_string(p->second.state)
               << " but DNE in pg_map; pool was probably deleted."
               << dendl;
      continue;
    }

    dout(15) << " got " << pgid
             << " reported at " << p->second.reported_epoch << ":" << p->second.reported_seq
             << " state " << pg_state_string(pg_map.pg_stat[pgid].state)
             << " -> " << pg_state_string(p->second.state)
             << dendl;
    pending_inc.pg_stat_updates[pgid] = p->second;
  }

  wait_for_finished_proposal(op, new C_Stats(this, op, ack_op));
  return true;
}

void PGMonitor::_updated_stats(MonOpRequestRef op, MonOpRequestRef ack_op)
{
  op->mark_pgmon_event(__func__);
  ack_op->mark_pgmon_event(__func__);
  MPGStats *ack = static_cast<MPGStats*>(ack_op->get_req());
  ack->get();  // MonOpRequestRef owns one ref; give the other to send_reply.
  dout(7) << "_updated_stats for "
          << op->get_req()->get_orig_source_inst() << dendl;
  mon->send_reply(op, ack);
}


// ------------------------

struct RetryCheckOSDMap : public Context {
  PGMonitor *pgmon;
  epoch_t epoch;
  RetryCheckOSDMap(PGMonitor *p, epoch_t e) : pgmon(p), epoch(e) {
  }
  void finish(int r) {
    if (r == -ECANCELED)
      return;

    pgmon->check_osd_map(epoch);
  }
};

void PGMonitor::check_osd_map(epoch_t epoch)
{
  if (mon->is_peon())
    return;  // whatever.

  if (pg_map.last_osdmap_epoch >= epoch) {
    dout(10) << __func__ << " already seen " << pg_map.last_osdmap_epoch
             << " >= " << epoch << dendl;
    return;
  }

  if (!mon->osdmon()->is_readable()) {
    dout(10) << __func__ << " -- osdmap not readable, waiting" << dendl;
    mon->osdmon()->wait_for_readable_ctx(new RetryCheckOSDMap(this, epoch));
    return;
  }

  if (!is_writeable()) {
    dout(10) << __func__ << " -- pgmap not writeable, waiting" << dendl;
    wait_for_writeable_ctx(new RetryCheckOSDMap(this, epoch));
    return;
  }

  // apply latest map(s)
  for (epoch_t e = pg_map.last_osdmap_epoch+1;
       e <= epoch;
       e++) {
    dout(10) << __func__ << " applying osdmap e" << e << " to pg_map" << dendl;
    bufferlist bl;
    int err = mon->osdmon()->get_version(e, bl);
    assert(err == 0);

    assert(bl.length());
    OSDMap::Incremental inc(bl);
    for (map<int32_t,uint32_t>::iterator p = inc.new_weight.begin();
         p != inc.new_weight.end();
         ++p)
      if (p->second == CEPH_OSD_OUT) {
	dout(10) << __func__ << "  osd." << p->first << " went OUT" << dendl;
	pending_inc.stat_osd_out(p->first);
      }


    // this is conservative: we want to know if any osds (maybe) got marked down.
    for (map<int32_t,uint8_t>::iterator p = inc.new_state.begin();
         p != inc.new_state.end();
         ++p) {
      if (p->second & CEPH_OSD_UP) {   // true if marked up OR down,
	                               // but we're too lazy to check
	                               // which
	need_check_down_pg_osds.insert(p->first);

	// clear out the last_osd_report for this OSD
	map<int, utime_t>::iterator report = last_osd_report.find(p->first);
	if (report != last_osd_report.end()) {
	  last_osd_report.erase(report);
	}

	// clear out osd_stat slow request histogram
	dout(20) << __func__ << " clearing osd." << p->first
	         << " request histogram" << dendl;
	pending_inc.stat_osd_down_up(p->first, pg_map);
      }

      if (p->second & CEPH_OSD_EXISTS) {
	// whether it was created *or* destroyed, we can safely drop
	// it's osd_stat_t record.
	dout(10) << __func__ << "  osd." << p->first
	         << " created or destroyed" << dendl;
	pending_inc.rm_stat(p->first);

	// and adjust full, nearfull set
	pg_map.nearfull_osds.erase(p->first);
	pg_map.full_osds.erase(p->first);
      }
    }
  }

  bool propose = false;
  if (pg_map.last_osdmap_epoch < epoch) {
    pending_inc.osdmap_epoch = epoch;
    propose = true;
  }

  if (map_pg_creates())
    propose = true;
  if (register_new_pgs())
    propose = true;

  if ((need_check_down_pgs || !need_check_down_pg_osds.empty()) &&
      check_down_pgs())
    propose = true;

  if (propose)
    propose_pending();
}

void PGMonitor::register_pg(OSDMap *osdmap,
                            pg_pool_t& pool, pg_t pgid, epoch_t epoch,
                            bool new_pool)
{
  pg_t parent;
  int split_bits = 0;
  bool parent_found = false;
  if (!new_pool) {
    parent = pgid;
    while (1) {
      // remove most significant bit
      int msb = pool.calc_bits_of(parent.ps());
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

  pg_stat_t &stats = pending_inc.pg_stat_updates[pgid];
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

  osdmap->pg_to_up_acting_osds(
    pgid,
    &stats.up,
    &stats.up_primary,
    &stats.acting,
    &stats.acting_primary);

  if (split_bits == 0) {
    dout(10) << __func__ << "  will create " << pgid
             << " primary " << stats.acting_primary
             << " acting " << stats.acting
             << dendl;
  } else {
    dout(10) << __func__ << "  will create " << pgid
             << " primary " << stats.acting_primary
             << " acting " << stats.acting
             << " parent " << parent
             << " by " << split_bits << " bits"
             << dendl;
  }
}

bool PGMonitor::register_new_pgs()
{
  // iterate over crush mapspace
  OSDMap *osdmap = &mon->osdmon()->osdmap;
  epoch_t epoch = osdmap->get_epoch();
  dout(10) << __func__ << " checking pg pools for osdmap epoch " << epoch
           << ", last_pg_scan " << pg_map.last_pg_scan << dendl;

  int created = 0;
  for (map<int64_t,pg_pool_t>::iterator p = osdmap->pools.begin();
       p != osdmap->pools.end();
       ++p) {
    int64_t poolid = p->first;
    pg_pool_t &pool = p->second;
    int ruleno = osdmap->crush->find_rule(pool.get_crush_ruleset(),
                                          pool.get_type(), pool.get_size());
    if (ruleno < 0 || !osdmap->crush->rule_exists(ruleno))
      continue;

    if (pool.get_last_change() <= pg_map.last_pg_scan ||
        pool.get_last_change() <= pending_inc.pg_scan) {
      dout(10) << " no change in pool " << p->first << " " << pool << dendl;
      continue;
    }

    dout(10) << __func__ << " scanning pool " << p->first
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
      register_pg(osdmap, pool, pgid, pool.get_last_change(), new_pool);
    }
  }

  int removed = 0;
  for (set<pg_t>::iterator p = pg_map.creating_pgs.begin();
       p != pg_map.creating_pgs.end();
       ++p) {
    if (p->preferred() >= 0) {
      dout(20) << " removing creating_pg " << *p
               << " because it is localized and obsolete" << dendl;
      pending_inc.pg_remove.insert(*p);
      removed++;
    }
    if (!osdmap->have_pg_pool(p->pool())) {
      dout(20) << " removing creating_pg " << *p
               << " because containing pool deleted" << dendl;
      pending_inc.pg_remove.insert(*p);
      ++removed;
    }
  }

  // deleted pools?
  for (ceph::unordered_map<pg_t,pg_stat_t>::const_iterator p =
         pg_map.pg_stat.begin();
       p != pg_map.pg_stat.end(); ++p) {
    if (!osdmap->have_pg_pool(p->first.pool())) {
      dout(20) << " removing pg_stat " << p->first << " because "
               << "containing pool deleted" << dendl;
      pending_inc.pg_remove.insert(p->first);
      ++removed;
    }
    if (p->first.preferred() >= 0) {
      dout(20) << " removing localized pg " << p->first << dendl;
      pending_inc.pg_remove.insert(p->first);
      ++removed;
    }
  }

  // we don't want to redo this work if we can avoid it.
  pending_inc.pg_scan = epoch;

  dout(10) << "register_new_pgs registered " << created << " new pgs, removed "
           << removed << " uncreated pgs" << dendl;
  return (created || removed);
}

bool PGMonitor::map_pg_creates()
{
  OSDMap *osdmap = &mon->osdmon()->osdmap;

  dout(10) << __func__ << " to " << pg_map.creating_pgs.size()
           << " pgs, osdmap epoch " << osdmap->get_epoch()
           << dendl;

  unsigned changed = 0;
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
    osdmap->pg_to_up_acting_osds(
      on,
      &up,
      &up_primary,
      &acting,
      &acting_primary);

    if (up != s->up ||
        up_primary != s->up_primary ||
        acting !=  s->acting ||
        acting_primary != s->acting_primary) {
      pg_stat_t *ns = &pending_inc.pg_stat_updates[pgid];
      if (osdmap->get_epoch() > ns->reported_epoch) {
	dout(20) << __func__ << "  " << pgid << " "
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
	  ns->mapping_epoch = osdmap->get_epoch();

	ns->up = up;
	ns->up_primary = up_primary;
	ns->acting = acting;
	ns->acting_primary = acting_primary;

	++changed;
      } else {
	dout(20) << __func__ << "  " << pgid << " has pending update from newer"
		 << " epoch " << ns->reported_epoch
		 << dendl;
      }
    }
  }
  if (changed) {
    dout(10) << __func__ << " " << changed << " pgs changed primary" << dendl;
    return true;
  }
  return false;
}

void PGMonitor::send_pg_creates()
{
  // We only need to do this old, spammy way of broadcasting create messages
  // to every osd (even those that aren't connected) if there are old OSDs in
  // the cluster. As soon as everybody has upgraded we can flipt to the new
  // behavior instead
  OSDMap& osdmap = mon->osdmon()->osdmap;
  if (osdmap.get_num_up_osds() == 0)
    return;

  if (osdmap.get_up_osd_features() & CEPH_FEATURE_MON_STATEFUL_SUB) {
    check_subs();
    return;
  }

  dout(10) << "send_pg_creates to " << pg_map.creating_pgs.size()
           << " pgs" << dendl;

  utime_t now = ceph_clock_now(g_ceph_context);
  for (map<int, map<epoch_t, set<pg_t> > >::iterator p =
         pg_map.creating_pgs_by_osd_epoch.begin();
       p != pg_map.creating_pgs_by_osd_epoch.end();
       ++p) {
    int osd = p->first;

    // throttle?
    if (last_sent_pg_create.count(osd) &&
        now - g_conf->mon_pg_create_interval < last_sent_pg_create[osd])
      continue;

    if (osdmap.is_up(osd))
      send_pg_creates(osd, NULL, 0);
  }
}

epoch_t PGMonitor::send_pg_creates(int osd, Connection *con, epoch_t next)
{
  dout(30) << __func__ << " " << pg_map.creating_pgs_by_osd_epoch << dendl;
  map<int, map<epoch_t, set<pg_t> > >::iterator p =
    pg_map.creating_pgs_by_osd_epoch.find(osd);
  if (p == pg_map.creating_pgs_by_osd_epoch.end())
    return next;

  assert(p->second.size() > 0);

  MOSDPGCreate *m = NULL;
  epoch_t last = 0;
  for (map<epoch_t, set<pg_t> >::iterator q = p->second.lower_bound(next);
       q != p->second.end();
       ++q) {
    dout(20) << __func__ << " osd." << osd << " from " << next
             << " : epoch " << q->first << " " << q->second.size() << " pgs"
             << dendl;
    last = q->first;
    for (set<pg_t>::iterator r = q->second.begin(); r != q->second.end(); ++r) {
      pg_stat_t &st = pg_map.pg_stat[*r];
      if (!m)
	m = new MOSDPGCreate(pg_map.last_osdmap_epoch);
      m->mkpg[*r] = pg_create_t(st.created,
                                st.parent,
                                st.parent_split_bits);
      // Need the create time from the monitor using its clock to set
      // last_scrub_stamp upon pg creation.
      m->ctimes[*r] = pg_map.pg_stat[*r].last_scrub_stamp;
    }
  }
  if (!m) {
    dout(20) << "send_pg_creates osd." << osd << " from " << next
             << " has nothing to send" << dendl;
    return next;
  }

  if (con) {
    con->send_message(m);
  } else {
    assert(mon->osdmon()->osdmap.is_up(osd));
    mon->messenger->send_message(m, mon->osdmon()->osdmap.get_inst(osd));
  }
  last_sent_pg_create[osd] = ceph_clock_now(g_ceph_context);

  // sub is current through last + 1
  return last + 1;
}

void PGMonitor::_try_mark_pg_stale(
  const OSDMap *osdmap,
  pg_t pgid,
  const pg_stat_t& cur_stat)
{
  map<pg_t,pg_stat_t>::iterator q = pending_inc.pg_stat_updates.find(pgid);
  pg_stat_t *stat;
  if (q == pending_inc.pg_stat_updates.end()) {
    stat = &pending_inc.pg_stat_updates[pgid];
    *stat = cur_stat;
  } else {
    stat = &q->second;
  }
  if ((stat->acting_primary == cur_stat.acting_primary) ||
      ((stat->state & PG_STATE_STALE) == 0 &&
       stat->acting_primary != -1 &&
       osdmap->is_down(stat->acting_primary))) {
    dout(10) << " marking pg " << pgid << " stale (acting_primary "
	     << stat->acting_primary << ")" << dendl;
    stat->state |= PG_STATE_STALE;  
    stat->last_unstale = ceph_clock_now(g_ceph_context);
  }
}

bool PGMonitor::check_down_pgs()
{
  dout(10) << "check_down_pgs last_osdmap_epoch "
	   << pg_map.last_osdmap_epoch << dendl;
  if (pg_map.last_osdmap_epoch == 0)
    return false;

  // use the OSDMap that matches the one pg_map has consumed.
  std::unique_ptr<OSDMap> osdmap;
  bufferlist bl;
  int err = mon->osdmon()->get_version_full(pg_map.last_osdmap_epoch, bl);
  assert(err == 0);
  osdmap.reset(new OSDMap);
  osdmap->decode(bl);

  bool ret = false;

  // if a large number of osds changed state, just iterate over the whole
  // pg map.
  if (need_check_down_pg_osds.size() > (unsigned)osdmap->get_num_osds() *
      g_conf->mon_pg_check_down_all_threshold)
    need_check_down_pgs = true;

  if (need_check_down_pgs) {
    for (auto p : pg_map.pg_stat) {
      if ((p.second.state & PG_STATE_STALE) == 0 &&
          p.second.acting_primary != -1 &&
          osdmap->is_down(p.second.acting_primary)) {
	_try_mark_pg_stale(osdmap.get(), p.first, p.second);
	ret = true;
      }
    }
  } else {
    for (auto osd : need_check_down_pg_osds) {
      if (osdmap->is_down(osd)) {
	for (auto pgid : pg_map.pg_by_osd[osd]) {
	  const pg_stat_t &stat = pg_map.pg_stat[pgid];
	  assert(stat.acting_primary == osd);
	  if ((stat.state & PG_STATE_STALE) == 0) {
	    _try_mark_pg_stale(osdmap.get(), pgid, stat);
	    ret = true;
	  }
	}
      }
    }
  }
  need_check_down_pgs = false;
  need_check_down_pg_osds.clear();

  return ret;
}

inline string percentify(const float& a) {
  stringstream ss;
  if (a < 0.01)
    ss << "0";
  else
    ss << std::fixed << std::setprecision(2) << a;
  return ss.str();
}

//void PGMonitor::dump_object_stat_sum(stringstream& ss, Formatter *f,
void PGMonitor::dump_object_stat_sum(TextTable &tbl, Formatter *f,
                                     object_stat_sum_t &sum, uint64_t avail,
                                     float raw_used_rate, bool verbose) const
{
  float curr_object_copies_rate = 0.0;
  if (sum.num_object_copies > 0)
    curr_object_copies_rate = (float)(sum.num_object_copies - sum.num_objects_degraded) / sum.num_object_copies;

  if (f) {
    f->dump_int("kb_used", SHIFT_ROUND_UP(sum.num_bytes, 10));
    f->dump_int("bytes_used", sum.num_bytes);
    f->dump_unsigned("max_avail", avail);
    f->dump_int("objects", sum.num_objects);
    if (verbose) {
      f->dump_int("dirty", sum.num_objects_dirty);
      f->dump_int("rd", sum.num_rd);
      f->dump_int("rd_bytes", sum.num_rd_kb * 1024ull);
      f->dump_int("wr", sum.num_wr);
      f->dump_int("wr_bytes", sum.num_wr_kb * 1024ull);
      f->dump_int("raw_bytes_used", sum.num_bytes * raw_used_rate * curr_object_copies_rate);
    }
  } else {
    tbl << stringify(si_t(sum.num_bytes));
    int64_t kb_used = SHIFT_ROUND_UP(sum.num_bytes, 10);
    float used = 0.0;
    if (pg_map.osd_sum.kb > 0)
      used = (float)kb_used * raw_used_rate * curr_object_copies_rate / pg_map.osd_sum.kb;
    tbl << percentify(used*100);
    tbl << si_t(avail);
    tbl << sum.num_objects;
    if (verbose) {
      tbl << stringify(si_t(sum.num_objects_dirty))
          << stringify(si_t(sum.num_rd))
          << stringify(si_t(sum.num_wr))
          << stringify(si_t(sum.num_bytes * raw_used_rate * curr_object_copies_rate));
    }
  }
}

int64_t PGMonitor::get_rule_avail(OSDMap& osdmap, int ruleno) const
{
  map<int,float> wm;
  int r = osdmap.crush->get_rule_weight_osd_map(ruleno, &wm);
  if (r < 0)
    return r;

  if(wm.empty())
    return 0;

  int64_t min = -1;
  for (map<int,float>::iterator p = wm.begin(); p != wm.end(); ++p) {
    ceph::unordered_map<int32_t,osd_stat_t>::const_iterator osd_info = pg_map.osd_stat.find(p->first);
    if (osd_info != pg_map.osd_stat.end()) {
      if (osd_info->second.kb == 0 || p->second == 0) {
	// osd must be out, hence its stats have been zeroed
	// (unless we somehow managed to have a disk with size 0...)
	//
	// (p->second == 0), if osd weight is 0, no need to
	// calculate proj below.
	continue;
      }
      int64_t proj = (int64_t)((double)((osd_info->second).kb_avail * 1024ull) /
                     (double)p->second);
      if (min < 0 || proj < min)
	min = proj;
    } else {
      dout(0) << "Cannot get stat of OSD " << p->first << dendl;
    }
  }
  return min;
}

void PGMonitor::dump_pool_stats(stringstream &ss, Formatter *f, bool verbose)
{
  TextTable tbl;

  if (f) {
    f->open_array_section("pools");
  } else {
    tbl.define_column("NAME", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("ID", TextTable::LEFT, TextTable::LEFT);
    if (verbose)
      tbl.define_column("CATEGORY", TextTable::LEFT, TextTable::LEFT);
    tbl.define_column("USED", TextTable::LEFT, TextTable::RIGHT);
    tbl.define_column("%USED", TextTable::LEFT, TextTable::RIGHT);
    tbl.define_column("MAX AVAIL", TextTable::LEFT, TextTable::RIGHT);
    tbl.define_column("OBJECTS", TextTable::LEFT, TextTable::RIGHT);
    if (verbose) {
      tbl.define_column("DIRTY", TextTable::LEFT, TextTable::RIGHT);
      tbl.define_column("READ", TextTable::LEFT, TextTable::RIGHT);
      tbl.define_column("WRITE", TextTable::LEFT, TextTable::RIGHT);
      tbl.define_column("RAW USED", TextTable::LEFT, TextTable::RIGHT);
    }
  }

  map<int,uint64_t> avail_by_rule;
  OSDMap &osdmap = mon->osdmon()->osdmap;
  for (map<int64_t,pg_pool_t>::const_iterator p = osdmap.get_pools().begin();
       p != osdmap.get_pools().end(); ++p) {
    int64_t pool_id = p->first;
    if ((pool_id < 0) || (pg_map.pg_pool_sum.count(pool_id) == 0))
      continue;
    const string& pool_name = osdmap.get_pool_name(pool_id);
    pool_stat_t &stat = pg_map.pg_pool_sum[pool_id];

    const pg_pool_t *pool = osdmap.get_pg_pool(pool_id);
    int ruleno = osdmap.crush->find_rule(pool->get_crush_ruleset(),
                                         pool->get_type(),
                                         pool->get_size());
    int64_t avail;
    float raw_used_rate;
    if (avail_by_rule.count(ruleno) == 0) {
      avail = get_rule_avail(osdmap, ruleno);
      if (avail < 0)
	avail = 0;
      avail_by_rule[ruleno] = avail;
    } else {
      avail = avail_by_rule[ruleno];
    }
    switch (pool->get_type()) {
    case pg_pool_t::TYPE_REPLICATED:
      avail /= pool->get_size();
      raw_used_rate = pool->get_size();
      break;
    case pg_pool_t::TYPE_ERASURE:
    {
      const map<string,string>& ecp =
        osdmap.get_erasure_code_profile(pool->erasure_code_profile);
      map<string,string>::const_iterator pm = ecp.find("m");
      map<string,string>::const_iterator pk = ecp.find("k");
      if (pm != ecp.end() && pk != ecp.end()) {
	int k = atoi(pk->second.c_str());
	int m = atoi(pm->second.c_str());
	avail = avail * k / (m + k);
	raw_used_rate = (float)(m + k) / k;
      } else {
	raw_used_rate = 0.0;
      }
    }
    break;
    default:
      assert(0 == "unrecognized pool type");
    }

    if (f) {
      f->open_object_section("pool");
      f->dump_string("name", pool_name);
      f->dump_int("id", pool_id);
      f->open_object_section("stats");
    } else {
      tbl << pool_name
          << pool_id;
      if (verbose)
	tbl << "-";
    }
    dump_object_stat_sum(tbl, f, stat.stats.sum, avail, raw_used_rate, verbose);
    if (f)
      f->close_section();  // stats
    else
      tbl << TextTable::endrow;

    if (f)
      f->close_section();  // pool
  }
  if (f)
    f->close_section();
  else {
    ss << "POOLS:\n";
    tbl.set_indent(4);
    ss << tbl;
  }
}

void PGMonitor::dump_fs_stats(stringstream &ss, Formatter *f, bool verbose) const
{
  if (f) {
    f->open_object_section("stats");
    f->dump_int("total_bytes", pg_map.osd_sum.kb * 1024ull);
    f->dump_int("total_used_bytes", pg_map.osd_sum.kb_used * 1024ull);
    f->dump_int("total_avail_bytes", pg_map.osd_sum.kb_avail * 1024ull);
    if (verbose) {
      f->dump_int("total_objects", pg_map.pg_sum.stats.sum.num_objects);
    }
    f->close_section();
  } else {
    TextTable tbl;
    tbl.define_column("SIZE", TextTable::LEFT, TextTable::RIGHT);
    tbl.define_column("AVAIL", TextTable::LEFT, TextTable::RIGHT);
    tbl.define_column("RAW USED", TextTable::LEFT, TextTable::RIGHT);
    tbl.define_column("%RAW USED", TextTable::LEFT, TextTable::RIGHT);
    if (verbose) {
      tbl.define_column("OBJECTS", TextTable::LEFT, TextTable::RIGHT);
    }
    tbl << stringify(si_t(pg_map.osd_sum.kb*1024))
        << stringify(si_t(pg_map.osd_sum.kb_avail*1024))
        << stringify(si_t(pg_map.osd_sum.kb_used*1024));
    float used = 0.0;
    if (pg_map.osd_sum.kb > 0) {
      used = ((float)pg_map.osd_sum.kb_used / pg_map.osd_sum.kb);
    }
    tbl << percentify(used*100);
    if (verbose) {
      tbl << stringify(si_t(pg_map.pg_sum.stats.sum.num_objects));
    }
    tbl << TextTable::endrow;

    ss << "GLOBAL:\n";
    tbl.set_indent(4);
    ss << tbl;
  }
}


void PGMonitor::dump_info(Formatter *f) const
{
  f->open_object_section("pgmap");
  pg_map.dump(f);
  f->close_section();

  f->dump_unsigned("pgmap_first_committed", get_first_committed());
  f->dump_unsigned("pgmap_last_committed", get_last_committed());
}

bool PGMonitor::preprocess_command(MonOpRequestRef op)
{
  op->mark_pgmon_event(__func__);
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  int r = -1;
  bufferlist rdata;
  stringstream ss, ds;
  bool primary = false;

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    // ss has reason for failure
    string rs = ss.str();
    mon->reply_command(op, -EINVAL, rs, rdata, get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  MonSession *session = m->get_session();
  if (!session) {
    mon->reply_command(op, -EACCES, "access denied", rdata, get_last_committed());
    return true;
  }

  // perhaps these would be better in the parsing, but it's weird
  if (prefix == "pg dump_json") {
    vector<string> v;
    v.push_back(string("all"));
    cmd_putval(g_ceph_context, cmdmap, "format", string("json"));
    cmd_putval(g_ceph_context, cmdmap, "dumpcontents", v);
    prefix = "pg dump";
  } else if (prefix == "pg dump_pools_json") {
    vector<string> v;
    v.push_back(string("pools"));
    cmd_putval(g_ceph_context, cmdmap, "format", string("json"));
    cmd_putval(g_ceph_context, cmdmap, "dumpcontents", v);
    prefix = "pg dump";
  } else if (prefix == "pg ls-by-primary") {
    primary = true;
    prefix = "pg ls";
  } else if (prefix == "pg ls-by-osd") {
    prefix = "pg ls";
  } else if (prefix == "pg ls-by-pool") {
    prefix = "pg ls";
    string poolstr;
    cmd_getval(g_ceph_context, cmdmap, "poolstr", poolstr);
    int64_t pool = mon->osdmon()->osdmap.lookup_pg_pool_name(poolstr.c_str());
    if (pool < 0) {
      r = -ENOENT;
      ss << "pool " << poolstr << " does not exist";
      string rs = ss.str();
      mon->reply_command(op, r, rs, get_last_committed());
      return true;
    }
    cmd_putval(g_ceph_context, cmdmap, "pool", pool);
  }


  string format;
  cmd_getval(g_ceph_context, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  if (prefix == "pg stat") {
    if (f) {
      f->open_object_section("pg_summary");
      pg_map.print_oneline_summary(f.get(), NULL);
      f->close_section();
      f->flush(ds);
    } else {
      ds << pg_map;
    }
    r = 0;
  } else if (prefix == "pg getmap") {
    pg_map.encode(rdata);
    ss << "got pgmap version " << pg_map.version;
    r = 0;
  } else if (prefix == "pg dump") {
    string val;
    vector<string> dumpcontents;
    set<string> what;
    if (cmd_getval(g_ceph_context, cmdmap, "dumpcontents", dumpcontents)) {
      copy(dumpcontents.begin(), dumpcontents.end(),
           inserter(what, what.end()));
    }
    if (what.empty())
      what.insert("all");
    if (f) {
      vector<string> dumpcontents;
      if (cmd_getval(g_ceph_context, cmdmap, "dumpcontents", dumpcontents)) {
	copy(dumpcontents.begin(), dumpcontents.end(),
	     inserter(what, what.end()));
      }
      if (what.count("all")) {
	f->open_object_section("pg_map");
	pg_map.dump(f.get());
	f->close_section();
      } else if (what.count("summary") || what.count("sum")) {
	f->open_object_section("pg_map");
	pg_map.dump_basic(f.get());
	f->close_section();
      } else {
	if (what.count("pools")) {
	  pg_map.dump_pool_stats(f.get());
	}
	if (what.count("osds")) {
	  pg_map.dump_osd_stats(f.get());
	}
	if (what.count("pgs")) {
	  pg_map.dump_pg_stats(f.get(), false);
	}
	if (what.count("pgs_brief")) {
	  pg_map.dump_pg_stats(f.get(), true);
	}
	if (what.count("delta")) {
	  f->open_object_section("delta");
	  pg_map.dump_delta(f.get());
	  f->close_section();
	}
      }
      f->flush(ds);
    } else {
      if (what.count("all")) {
	pg_map.dump(ds);
      } else if (what.count("summary") || what.count("sum")) {
	pg_map.dump_basic(ds);
	pg_map.dump_pg_sum_stats(ds, true);
	pg_map.dump_osd_sum_stats(ds);
      } else {
	if (what.count("pgs_brief")) {
	  pg_map.dump_pg_stats(ds, true);
	}
	bool header = true;
	if (what.count("pgs")) {
	  pg_map.dump_pg_stats(ds, false);
	  header = false;
	}
	if (what.count("pools")) {
	  pg_map.dump_pool_stats(ds, header);
	}
	if (what.count("osds")) {
	  pg_map.dump_osd_stats(ds);
	}
      }
    }
    ss << "dumped " << what << " in format " << format;
    r = 0;
  } else if (prefix == "pg ls") {
    int64_t osd = -1;
    int64_t pool = -1;
    vector<string>states;
    set<pg_t> pgs;
    set<string> what;
    cmd_getval(g_ceph_context, cmdmap, "pool", pool);
    cmd_getval(g_ceph_context, cmdmap, "osd", osd);
    cmd_getval(g_ceph_context, cmdmap, "states", states);
    if (pool >= 0 && !mon->osdmon()->osdmap.have_pg_pool(pool)) {
      r = -ENOENT;
      ss << "pool " << pool << " does not exist";
      goto reply;
    }
    if (osd >= 0 && !mon->osdmon()->osdmap.is_up(osd)) {
      ss << "osd " << osd << " is not up";
      r = -EAGAIN;
      goto reply;
    }
    if (states.empty())
      states.push_back("all");
    while (!states.empty()) {
      string state = states.back();
      what.insert(state);
      pg_map.get_filtered_pg_stats(state,pool,osd,primary,pgs);
      states.pop_back();
    }
    if (f && !pgs.empty()) {
      pg_map.dump_filtered_pg_stats(f.get(),pgs);
      f->flush(ds);
    } else if (!pgs.empty()) {
      pg_map.dump_filtered_pg_stats(ds,pgs);
    }
    r = 0;
  } else if (prefix == "pg dump_stuck") {
    vector<string> stuckop_vec;
    cmd_getval(g_ceph_context, cmdmap, "stuckops", stuckop_vec);
    if (stuckop_vec.empty())
      stuckop_vec.push_back("unclean");
    int64_t threshold;
    cmd_getval(g_ceph_context, cmdmap, "threshold", threshold,
               int64_t(g_conf->mon_pg_stuck_threshold));

    r = dump_stuck_pg_stats(ds, f.get(), (int)threshold, stuckop_vec);
    if (r < 0)
      ss << "failed";  
    else 
      ss << "ok";
    r = 0;
  } else if (prefix == "pg map") {
    pg_t pgid;
    string pgidstr;
    cmd_getval(g_ceph_context, cmdmap, "pgid", pgidstr);
    if (!pgid.parse(pgidstr.c_str())) {
      ss << "invalid pgid '" << pgidstr << "'";
      r = -EINVAL;
      goto reply;
    }
    vector<int> up, acting;
    if (!mon->osdmon()->osdmap.have_pg_pool(pgid.pool())) {
      r = -ENOENT;
      ss << "pg '" << pgidstr << "' does not exist";
      goto reply;
    }
    pg_t mpgid = mon->osdmon()->osdmap.raw_pg_to_pg(pgid);
    mon->osdmon()->osdmap.pg_to_up_acting_osds(pgid, up, acting);
    if (f) {
      f->open_object_section("pg_map");
      f->dump_unsigned("epoch", mon->osdmon()->osdmap.get_epoch());
      f->dump_stream("raw_pgid") << pgid;
      f->dump_stream("pgid") << mpgid;

      f->open_array_section("up");
      for (vector<int>::iterator it = up.begin(); it != up.end(); ++it)
	f->dump_int("up_osd", *it);
      f->close_section();

      f->open_array_section("acting");
      for (vector<int>::iterator it = acting.begin(); it != acting.end(); ++it)
	f->dump_int("acting_osd", *it);
      f->close_section();

      f->close_section();
      f->flush(ds);
    } else {
      ds << "osdmap e" << mon->osdmon()->osdmap.get_epoch()
         << " pg " << pgid << " (" << mpgid << ")"
         << " -> up " << up << " acting " << acting;
    }
    r = 0;
  } else if (prefix == "pg scrub" ||
             prefix == "pg repair" ||
             prefix == "pg deep-scrub") {
    string scrubop = prefix.substr(3, string::npos);
    pg_t pgid;
    string pgidstr;
    cmd_getval(g_ceph_context, cmdmap, "pgid", pgidstr);
    if (!pgid.parse(pgidstr.c_str())) {
      ss << "invalid pgid '" << pgidstr << "'";
      r = -EINVAL;
      goto reply;
    }
    if (!pg_map.pg_stat.count(pgid)) {
      ss << "pg " << pgid << " dne";
      r = -ENOENT;
      goto reply;
    }
    if (pg_map.pg_stat[pgid].acting_primary == -1) {
      ss << "pg " << pgid << " has no primary osd";
      r = -EAGAIN;
      goto reply;
    }
    int osd = pg_map.pg_stat[pgid].acting_primary;
    if (!mon->osdmon()->osdmap.is_up(osd)) {
      ss << "pg " << pgid << " primary osd." << osd << " not up";
      r = -EAGAIN;
      goto reply;
    }
    vector<pg_t> pgs(1);
    pgs[0] = pgid;
    mon->try_send_message(new MOSDScrub(mon->monmap->fsid, pgs,
                                        scrubop == "repair",
                                        scrubop == "deep-scrub"),
                          mon->osdmon()->osdmap.get_inst(osd));
    ss << "instructing pg " << pgid << " on osd." << osd << " to " << scrubop;
    r = 0;
  } else if (prefix == "pg debug") {
    string debugop;
    cmd_getval(g_ceph_context, cmdmap, "debugop", debugop, string("unfound_objects_exist"));
    if (debugop == "unfound_objects_exist") {
      bool unfound_objects_exist = false;
      ceph::unordered_map<pg_t,pg_stat_t>::const_iterator end = pg_map.pg_stat.end();
      for (ceph::unordered_map<pg_t,pg_stat_t>::const_iterator s = pg_map.pg_stat.begin();
           s != end; ++s) {
	if (s->second.stats.sum.num_objects_unfound > 0) {
	  unfound_objects_exist = true;
	  break;
	}
      }
      if (unfound_objects_exist)
	ds << "TRUE";
      else
	ds << "FALSE";
      r = 0;
    } else if (debugop == "degraded_pgs_exist") {
      bool degraded_pgs_exist = false;
      ceph::unordered_map<pg_t,pg_stat_t>::const_iterator end = pg_map.pg_stat.end();
      for (ceph::unordered_map<pg_t,pg_stat_t>::const_iterator s = pg_map.pg_stat.begin();
           s != end; ++s) {
	if (s->second.stats.sum.num_objects_degraded > 0) {
	  degraded_pgs_exist = true;
	  break;
	}
      }
      if (degraded_pgs_exist)
	ds << "TRUE";
      else
	ds << "FALSE";
      r = 0;
    }
  }

  if (r == -1)
    return false;

reply:
  string rs;
  getline(ss, rs);
  rdata.append(ds);
  mon->reply_command(op, r, rs, rdata, get_last_committed());
  return true;
}

bool PGMonitor::prepare_command(MonOpRequestRef op)
{
  op->mark_pgmon_event(__func__);
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  stringstream ss;
  pg_t pgid;
  epoch_t epoch = mon->osdmon()->osdmap.get_epoch();
  int r = 0;
  string rs;

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    // ss has reason for failure
    string rs = ss.str();
    mon->reply_command(op, -EINVAL, rs, get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  MonSession *session = m->get_session();
  if (!session) {
    mon->reply_command(op, -EACCES, "access denied", get_last_committed());
    return true;
  }

  if (prefix == "pg force_create_pg") {
    string pgidstr;
    cmd_getval(g_ceph_context, cmdmap, "pgid", pgidstr);
    if (!pgid.parse(pgidstr.c_str())) {
      ss << "pg " << pgidstr << " invalid";
      r = -EINVAL;
      goto reply;
    }
    if (!pg_map.pg_stat.count(pgid)) {
      ss << "pg " << pgid << " dne";
      r = -ENOENT;
      goto reply;
    }
    if (pg_map.creating_pgs.count(pgid)) {
      ss << "pg " << pgid << " already creating";
      r = 0;
      goto reply;
    }
    {
      pg_stat_t& s = pending_inc.pg_stat_updates[pgid];
      s.state = PG_STATE_CREATING;
      s.created = epoch;
      s.last_change = ceph_clock_now(g_ceph_context);
    }
    ss << "pg " << pgidstr << " now creating, ok";
    goto update;
  } else if (prefix == "pg set_full_ratio" ||
             prefix == "pg set_nearfull_ratio") {
    double n;
    if (!cmd_getval(g_ceph_context, cmdmap, "ratio", n)) {
      ss << "unable to parse 'ratio' value '"
         << cmd_vartype_stringify(cmdmap["who"]) << "'";
      r = -EINVAL;
      goto reply;
    }
    string op = prefix.substr(3, string::npos);
    if (op == "set_full_ratio")
      pending_inc.full_ratio = n;
    else if (op == "set_nearfull_ratio")
      pending_inc.nearfull_ratio = n;
    goto update;
  } else {
    r = -EINVAL;
    goto reply;
  }

reply:
  getline(ss, rs);
  if (r < 0 && rs.length() == 0)
    rs = cpp_strerror(r);
  mon->reply_command(op, r, rs, get_last_committed());
  return false;

update:
  getline(ss, rs);
  wait_for_finished_proposal(op, new Monitor::C_Command(
                               mon, op, r, rs, get_last_committed() + 1));
  return true;
}

// Only called with a single bit set in "what"
static void note_stuck_detail(int what,
                              ceph::unordered_map<pg_t,pg_stat_t>& stuck_pgs,
                              list<pair<health_status_t,string> > *detail)
{
  for (ceph::unordered_map<pg_t,pg_stat_t>::iterator p = stuck_pgs.begin();
       p != stuck_pgs.end();
       ++p) {
    ostringstream ss;
    utime_t since;
    const char *whatname = 0;
    switch (what) {
    case PGMap::STUCK_INACTIVE:
      since = p->second.last_active;
      whatname = "inactive";
      break;
    case PGMap::STUCK_UNCLEAN:
      since = p->second.last_clean;
      whatname = "unclean";
      break;
    case PGMap::STUCK_DEGRADED:
      since = p->second.last_undegraded;
      whatname = "degraded";
      break;
    case PGMap::STUCK_UNDERSIZED:
      since = p->second.last_fullsized;
      whatname = "undersized";
      break;
    case PGMap::STUCK_STALE:
      since = p->second.last_unstale;
      whatname = "stale";
      break;
    default:
      assert(0);
    }
    ss << "pg " << p->first << " is stuck " << whatname;
    if (since == utime_t()) {
      ss << " since forever";
    } else {
      utime_t dur = ceph_clock_now(g_ceph_context) - since;
      ss << " for " << dur;
    }
    ss << ", current state " << pg_state_string(p->second.state)
       << ", last acting " << p->second.acting;
    detail->push_back(make_pair(HEALTH_WARN, ss.str()));
  }
}

int PGMonitor::_warn_slow_request_histogram(const pow2_hist_t& h, string suffix,
                                            list<pair<health_status_t,string> >& summary,
                                            list<pair<health_status_t,string> > *detail) const
{
  unsigned sum = 0;
  for (unsigned i = h.h.size() - 1; i > 0; --i) {
    float ub = (float)(1 << i) / 1000.0;
    if (ub < g_conf->mon_osd_max_op_age)
      break;
    ostringstream ss;
    if (h.h[i]) {
      ss << h.h[i] << " ops are blocked > " << ub << " sec" << suffix;
      if (detail)
	detail->push_back(make_pair(HEALTH_WARN, ss.str()));
      sum += h.h[i];
    }
  }
  return sum;
}

namespace {
  enum class scrubbed_or_deepscrubbed_t { SCRUBBED, DEEPSCRUBBED };

  void print_unscrubbed_detailed(const std::pair<const pg_t,pg_stat_t> &pg_entry,
				 list<pair<health_status_t,string> > *detail,
				 scrubbed_or_deepscrubbed_t how_scrubbed) {

    std::stringstream ss;
    const auto& pg_stat(pg_entry.second);

    ss << "pg " << pg_entry.first << " is not ";
    if (how_scrubbed == scrubbed_or_deepscrubbed_t::SCRUBBED) {
      ss << "scrubbed, last_scrub_stamp "
	 << pg_stat.last_scrub_stamp;
    } else if (how_scrubbed == scrubbed_or_deepscrubbed_t::DEEPSCRUBBED) {
      ss << "deep-scrubbed, last_deep_scrub_stamp "
	 << pg_stat.last_deep_scrub_stamp;
    }

    detail->push_back(make_pair(HEALTH_WARN, ss.str()));
  }


  using pg_stat_map_t = const ceph::unordered_map<pg_t,pg_stat_t>;

  void print_unscrubbed_pgs(pg_stat_map_t& pg_stats,
			    list<pair<health_status_t,string> > &summary,
			    list<pair<health_status_t,string> > *detail,
			    const CephContext* cct) {
    int pgs_count = 0;
    const utime_t now = ceph_clock_now(nullptr);
    for (const auto& pg_entry : pg_stats) {
      const auto& pg_stat(pg_entry.second);
      const utime_t time_since_ls = now - pg_stat.last_scrub_stamp;
      const utime_t time_since_lds = now - pg_stat.last_deep_scrub_stamp;

      const int mon_warn_not_scrubbed =
	cct->_conf->mon_warn_not_scrubbed + cct->_conf->mon_scrub_interval;

      const int mon_warn_not_deep_scrubbed =
	cct->_conf->mon_warn_not_deep_scrubbed + cct->_conf->mon_scrub_interval;

      bool not_scrubbed = (time_since_ls >= mon_warn_not_scrubbed &&
			   cct->_conf->mon_warn_not_scrubbed != 0);

      bool not_deep_scrubbed = (time_since_lds >= mon_warn_not_deep_scrubbed &&
				cct->_conf->mon_warn_not_deep_scrubbed != 0);

      if (detail != nullptr) {
	if (not_scrubbed) {
	  print_unscrubbed_detailed(pg_entry,
				    detail,
				    scrubbed_or_deepscrubbed_t::SCRUBBED);
	} else if (not_deep_scrubbed) {
	  print_unscrubbed_detailed(pg_entry,
				    detail,
				    scrubbed_or_deepscrubbed_t::DEEPSCRUBBED);
	}
      }
      if (not_scrubbed || not_deep_scrubbed) {
	++pgs_count;
      }
    }

    if (pgs_count > 0) {
      std::stringstream ss;
      ss << pgs_count << " unscrubbed pgs";
      summary.push_back(make_pair(HEALTH_WARN, ss.str()));
    }

  }
}

void PGMonitor::get_health(list<pair<health_status_t,string> >& summary,
			   list<pair<health_status_t,string> > *detail,
			   CephContext *cct) const
{
  map<string,int> note;
  ceph::unordered_map<int,int>::const_iterator p = pg_map.num_pg_by_state.begin();
  ceph::unordered_map<int,int>::const_iterator p_end = pg_map.num_pg_by_state.end();
  for (; p != p_end; ++p) {
    if (p->first & PG_STATE_STALE)
      note["stale"] += p->second;
    if (p->first & PG_STATE_DOWN)
      note["down"] += p->second;
    if (p->first & PG_STATE_UNDERSIZED)
      note["undersized"] += p->second;
    if (p->first & PG_STATE_DEGRADED)
      note["degraded"] += p->second;
    if (p->first & PG_STATE_INCONSISTENT)
      note["inconsistent"] += p->second;
    if (p->first & PG_STATE_PEERING)
      note["peering"] += p->second;
    if (p->first & PG_STATE_REPAIR)
      note["repair"] += p->second;
    if (p->first & PG_STATE_SPLITTING)
      note["splitting"] += p->second;
    if (p->first & PG_STATE_RECOVERING)
      note["recovering"] += p->second;
    if (p->first & PG_STATE_RECOVERY_WAIT)
      note["recovery_wait"] += p->second;
    if (p->first & PG_STATE_INCOMPLETE)
      note["incomplete"] += p->second;
    if (p->first & PG_STATE_BACKFILL_WAIT)
      note["backfill_wait"] += p->second;
    if (p->first & PG_STATE_BACKFILL)
      note["backfilling"] += p->second;
    if (p->first & PG_STATE_BACKFILL_TOOFULL)
      note["backfill_toofull"] += p->second;
  }

  ceph::unordered_map<pg_t, pg_stat_t> stuck_pgs;
  utime_t now(ceph_clock_now(g_ceph_context));
  utime_t cutoff = now - utime_t(g_conf->mon_pg_stuck_threshold, 0);
  uint64_t num_inactive_pgs = 0;
  
  if (detail) {
    
    // we need to collect details of stuck pgs, first do a quick check
    // whether this will yield any results
    if (pg_map.get_stuck_counts(cutoff, note)) {
      
      // there are stuck pgs. gather details for specified statuses
      // only if we know that there are pgs stuck in that status
      
      if (note.find("stuck inactive") != note.end()) {
        pg_map.get_stuck_stats(PGMap::STUCK_INACTIVE, cutoff, stuck_pgs);
        note["stuck inactive"] = stuck_pgs.size();
        num_inactive_pgs += stuck_pgs.size();
        note_stuck_detail(PGMap::STUCK_INACTIVE, stuck_pgs, detail);
        stuck_pgs.clear();
      }

      if (note.find("stuck unclean") != note.end()) {
        pg_map.get_stuck_stats(PGMap::STUCK_UNCLEAN, cutoff, stuck_pgs);
        note["stuck unclean"] = stuck_pgs.size();
        note_stuck_detail(PGMap::STUCK_UNCLEAN, stuck_pgs, detail);
        stuck_pgs.clear();
      }

      if (note.find("stuck undersized") != note.end()) {
        pg_map.get_stuck_stats(PGMap::STUCK_UNDERSIZED, cutoff, stuck_pgs);
        note["stuck undersized"] = stuck_pgs.size();
        note_stuck_detail(PGMap::STUCK_UNDERSIZED, stuck_pgs, detail);
        stuck_pgs.clear();
      }

      if (note.find("stuck degraded") != note.end()) {
        pg_map.get_stuck_stats(PGMap::STUCK_DEGRADED, cutoff, stuck_pgs);
        note["stuck degraded"] = stuck_pgs.size();
        note_stuck_detail(PGMap::STUCK_DEGRADED, stuck_pgs, detail);
        stuck_pgs.clear();
      }

      if (note.find("stuck stale") != note.end()) {
        pg_map.get_stuck_stats(PGMap::STUCK_STALE, cutoff, stuck_pgs);
        note["stuck stale"] = stuck_pgs.size();
        num_inactive_pgs += stuck_pgs.size();
        note_stuck_detail(PGMap::STUCK_STALE, stuck_pgs, detail);
      }
    }
  } else {
    pg_map.get_stuck_counts(cutoff, note);
    map<string,int>::const_iterator p = note.find("stuck inactive");
    if (p != note.end()) 
      num_inactive_pgs += p->second;
    p = note.find("stuck stale");
    if (p != note.end()) 
      num_inactive_pgs += p->second;
  }

  if (g_conf->mon_pg_min_inactive > 0 && num_inactive_pgs >= g_conf->mon_pg_min_inactive) {
    ostringstream ss;
    ss << num_inactive_pgs << " pgs are stuck inactive for more than " << g_conf->mon_pg_stuck_threshold << " seconds";
    summary.push_back(make_pair(HEALTH_ERR, ss.str()));
  }

  if (!note.empty()) {
    for (map<string,int>::iterator p = note.begin(); p != note.end(); ++p) {
      ostringstream ss;
      ss << p->second << " pgs " << p->first;
      summary.push_back(make_pair(HEALTH_WARN, ss.str()));
    }
    if (detail) {
      for (ceph::unordered_map<pg_t,pg_stat_t>::const_iterator p = pg_map.pg_stat.begin();
           p != pg_map.pg_stat.end();
           ++p) {
	if ((p->second.state & (PG_STATE_STALE |
	                        PG_STATE_DOWN |
	                        PG_STATE_UNDERSIZED |
	                        PG_STATE_DEGRADED |
	                        PG_STATE_INCONSISTENT |
	                        PG_STATE_PEERING |
	                        PG_STATE_REPAIR |
	                        PG_STATE_SPLITTING |
	                        PG_STATE_RECOVERING |
	                        PG_STATE_RECOVERY_WAIT |
	                        PG_STATE_INCOMPLETE |
	                        PG_STATE_BACKFILL_WAIT |
	                        PG_STATE_BACKFILL |
	                        PG_STATE_BACKFILL_TOOFULL)) &&
	    stuck_pgs.count(p->first) == 0) {
	  ostringstream ss;
	  ss << "pg " << p->first << " is " << pg_state_string(p->second.state);
	  ss << ", acting " << p->second.acting;
	  if (p->second.stats.sum.num_objects_unfound)
	    ss << ", " << p->second.stats.sum.num_objects_unfound << " unfound";
	  if (p->second.state & PG_STATE_INCOMPLETE) {
	    const pg_pool_t *pi = mon->osdmon()->osdmap.get_pg_pool(p->first.pool());
	    if (pi && pi->min_size > 1) {
	      ss << " (reducing pool " << mon->osdmon()->osdmap.get_pool_name(p->first.pool())
	         << " min_size from " << (int)pi->min_size << " may help; search ceph.com/docs for 'incomplete')";
	    }
	  }
	  detail->push_back(make_pair(HEALTH_WARN, ss.str()));
	}
      }
    }
  }

  // slow requests
  if (g_conf->mon_osd_max_op_age > 0 &&
      pg_map.osd_sum.op_queue_age_hist.upper_bound() > g_conf->mon_osd_max_op_age) {
    unsigned sum = _warn_slow_request_histogram(pg_map.osd_sum.op_queue_age_hist, "", summary, NULL);
    if (sum > 0) {
      ostringstream ss;
      ss << sum << " requests are blocked > " << g_conf->mon_osd_max_op_age << " sec";
      summary.push_back(make_pair(HEALTH_WARN, ss.str()));

      if (detail) {
	unsigned num_slow_osds = 0;
	// do per-osd warnings
	for (ceph::unordered_map<int32_t,osd_stat_t>::const_iterator p = pg_map.osd_stat.begin();
	     p != pg_map.osd_stat.end();
	     ++p) {
	  if (_warn_slow_request_histogram(p->second.op_queue_age_hist,
	                                   string(" on osd.") + stringify(p->first),
	                                   summary, detail))
	    ++num_slow_osds;
	}
	ostringstream ss2;
	ss2 << num_slow_osds << " osds have slow requests";
	summary.push_back(make_pair(HEALTH_WARN, ss2.str()));
	detail->push_back(make_pair(HEALTH_WARN, ss2.str()));
      }
    }
  }

  // recovery
  list<string> sl;
  pg_map.overall_recovery_summary(NULL, &sl);
  for (list<string>::iterator p = sl.begin(); p != sl.end(); ++p) {
    summary.push_back(make_pair(HEALTH_WARN, "recovery " + *p));
    if (detail)
      detail->push_back(make_pair(HEALTH_WARN, "recovery " + *p));
  }

  // full/nearfull
  check_full_osd_health(summary, detail, pg_map.full_osds, "full", HEALTH_ERR);
  check_full_osd_health(summary, detail, pg_map.nearfull_osds, "near full", HEALTH_WARN);

  // near-target max pools
  const map<int64_t,pg_pool_t>& pools = mon->osdmon()->osdmap.get_pools();
  for (map<int64_t,pg_pool_t>::const_iterator p = pools.begin();
       p != pools.end(); ++p) {
    if ((!p->second.target_max_objects && !p->second.target_max_bytes) ||
        !pg_map.pg_pool_sum.count(p->first))
      continue;
    bool nearfull = false;
    const string& name = mon->osdmon()->osdmap.get_pool_name(p->first);
    const pool_stat_t& st = pg_map.get_pg_pool_sum_stat(p->first);
    uint64_t ratio = p->second.cache_target_full_ratio_micro +
                     ((1000000 - p->second.cache_target_full_ratio_micro) *
                      g_conf->mon_cache_target_full_warn_ratio);
    if (p->second.target_max_objects && (uint64_t)(st.stats.sum.num_objects - st.stats.sum.num_objects_hit_set_archive) >
        p->second.target_max_objects * (ratio / 1000000.0)) {
      nearfull = true;
      if (detail) {
	ostringstream ss;
	ss << "cache pool '" << name << "' with "
	   << si_t(st.stats.sum.num_objects)
	   << " objects at/near target max "
	   << si_t(p->second.target_max_objects) << " objects";
	detail->push_back(make_pair(HEALTH_WARN, ss.str()));
      }
    }
    if (p->second.target_max_bytes && (uint64_t)(st.stats.sum.num_bytes - st.stats.sum.num_bytes_hit_set_archive) >
        p->second.target_max_bytes * (ratio / 1000000.0)) {
      nearfull = true;
      if (detail) {
	ostringstream ss;
	ss << "cache pool '" << name
	   << "' with " << si_t(st.stats.sum.num_bytes)
	   << "B at/near target max "
	   << si_t(p->second.target_max_bytes) << "B";
	detail->push_back(make_pair(HEALTH_WARN, ss.str()));
      }
    }
    if (nearfull) {
      ostringstream ss;
      ss << "'" << name << "' at/near target max";
      summary.push_back(make_pair(HEALTH_WARN, ss.str()));
    }
  }

  // scrub
  if (pg_map.pg_sum.stats.sum.num_scrub_errors) {
    ostringstream ss;
    ss << pg_map.pg_sum.stats.sum.num_scrub_errors << " scrub errors";
    summary.push_back(make_pair(HEALTH_ERR, ss.str()));
    if (detail) {
      detail->push_back(make_pair(HEALTH_ERR, ss.str()));
    }
  }

  // pg skew
  int num_in = mon->osdmon()->osdmap.get_num_in_osds();
  int sum_pg_up = MAX(pg_map.pg_sum.up, static_cast<int32_t>(pg_map.pg_stat.size()));
  if (num_in && g_conf->mon_pg_warn_min_per_osd > 0) {
    int per = sum_pg_up / num_in;
    if (per < g_conf->mon_pg_warn_min_per_osd && per) {
      ostringstream ss;
      ss << "too few PGs per OSD (" << per << " < min " << g_conf->mon_pg_warn_min_per_osd << ")";
      summary.push_back(make_pair(HEALTH_WARN, ss.str()));
      if (detail)
	detail->push_back(make_pair(HEALTH_WARN, ss.str()));
    }
  }
  if (num_in && g_conf->mon_pg_warn_max_per_osd > 0) {
    int per = sum_pg_up / num_in;
    if (per > g_conf->mon_pg_warn_max_per_osd) {
      ostringstream ss;
      ss << "too many PGs per OSD (" << per << " > max " << g_conf->mon_pg_warn_max_per_osd << ")";
      summary.push_back(make_pair(HEALTH_WARN, ss.str()));
      if (detail)
	detail->push_back(make_pair(HEALTH_WARN, ss.str()));
    }
  }
  if (!pg_map.pg_stat.empty()) {
    for (ceph::unordered_map<int,pool_stat_t>::const_iterator p = pg_map.pg_pool_sum.begin();
         p != pg_map.pg_pool_sum.end();
         ++p) {
      const pg_pool_t *pi = mon->osdmon()->osdmap.get_pg_pool(p->first);
      if (!pi)
	continue;   // in case osdmap changes haven't propagated to PGMap yet
      const string& name = mon->osdmon()->osdmap.get_pool_name(p->first);
      if (pi->get_pg_num() > pi->get_pgp_num()) {
	ostringstream ss;
	ss << "pool " << name << " pg_num "
	   << pi->get_pg_num() << " > pgp_num " << pi->get_pgp_num();
	summary.push_back(make_pair(HEALTH_WARN, ss.str()));
	if (detail)
	  detail->push_back(make_pair(HEALTH_WARN, ss.str()));
      }
      int average_objects_per_pg = pg_map.pg_sum.stats.sum.num_objects / pg_map.pg_stat.size();
      if (average_objects_per_pg > 0 &&
          pg_map.pg_sum.stats.sum.num_objects >= g_conf->mon_pg_warn_min_objects &&
          p->second.stats.sum.num_objects >= g_conf->mon_pg_warn_min_pool_objects) {
	int objects_per_pg = p->second.stats.sum.num_objects / pi->get_pg_num();
	float ratio = (float)objects_per_pg / (float)average_objects_per_pg;
	if (g_conf->mon_pg_warn_max_object_skew > 0 &&
	    ratio > g_conf->mon_pg_warn_max_object_skew) {
	  ostringstream ss;
	  ss << "pool " << name << " has many more objects per pg than average (too few pgs?)";
	  summary.push_back(make_pair(HEALTH_WARN, ss.str()));
	  if (detail) {
	    ostringstream ss;
	    ss << "pool " << name << " objects per pg ("
	       << objects_per_pg << ") is more than " << ratio << " times cluster average ("
	       << average_objects_per_pg << ")";
	    detail->push_back(make_pair(HEALTH_WARN, ss.str()));
	  }
	}
      }
    }
  }

  print_unscrubbed_pgs(pg_map.pg_stat, summary, detail, cct);

}

void PGMonitor::check_full_osd_health(list<pair<health_status_t,string> >& summary,
                                      list<pair<health_status_t,string> > *detail,
                                      const set<int>& s, const char *desc,
                                      health_status_t sev) const
{
  if (!s.empty()) {
    ostringstream ss;
    ss << s.size() << " " << desc << " osd(s)";
    summary.push_back(make_pair(sev, ss.str()));
    if (detail) {
      for (set<int>::const_iterator p = s.begin(); p != s.end(); ++p) {
	ostringstream ss;
	const osd_stat_t& os = pg_map.osd_stat.find(*p)->second;
	int ratio = (int)(((float)os.kb_used) / (float) os.kb * 100.0);
	ss << "osd." << *p << " is " << desc << " at " << ratio << "%";
	detail->push_back(make_pair(sev, ss.str()));
      }
    }
  }
}

int PGMonitor::dump_stuck_pg_stats(stringstream &ds,
                                   Formatter *f,
                                   int threshold,
                                   vector<string>& args) const
{
  int stuck_types = 0;

  for (vector<string>::iterator i = args.begin(); i != args.end(); ++i) {
    if (*i == "inactive")
      stuck_types |= PGMap::STUCK_INACTIVE;
    else if (*i == "unclean")
      stuck_types |= PGMap::STUCK_UNCLEAN;
    else if (*i == "undersized")
      stuck_types |= PGMap::STUCK_UNDERSIZED;
    else if (*i == "degraded")
      stuck_types |= PGMap::STUCK_DEGRADED;
    else if (*i == "stale")
      stuck_types |= PGMap::STUCK_STALE;
    else {
      ds << "Unknown type: " << *i << std::endl;
      return -EINVAL;
    }
  }

  utime_t now(ceph_clock_now(g_ceph_context));
  utime_t cutoff = now - utime_t(threshold, 0);

  if (!f) {
    pg_map.dump_stuck_plain(ds, stuck_types, cutoff);
  } else {
    pg_map.dump_stuck(f, stuck_types, cutoff);
    f->flush(ds);
  }

  return 0;
}

void PGMonitor::check_subs()
{
  dout(10) << __func__ << dendl;
  string type = "osd_pg_creates";
  if (mon->session_map.subs.count(type) == 0)
    return;

  xlist<Subscription*>::iterator p = mon->session_map.subs[type]->begin();
  while (!p.end()) {
    Subscription *sub = *p;
    ++p;
    dout(20) << __func__ << " .. " << sub->session->inst << dendl;
    check_sub(sub);
  }
}

void PGMonitor::check_sub(Subscription *sub)
{
  if (sub->type == "osd_pg_creates") {
    // only send these if the OSD is up.  we will check_subs() when they do
    // come up so they will get the creates then.
    if (sub->session->inst.name.is_osd() &&
        mon->osdmon()->osdmap.is_up(sub->session->inst.name.num())) {
      sub->next = send_pg_creates(sub->session->inst.name.num(),
                                  sub->session->con.get(),
                                  sub->next);
    }
  }
}
