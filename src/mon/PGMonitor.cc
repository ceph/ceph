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
#include "common/debug.h"		// undo damage
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

  if (mon->is_leader()) {
    bool propose = false;
    
    if (need_check_down_pgs && check_down_pgs())
      propose = true;
    
    if (propose) {
      propose_pending();
    }
  }

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
      }
      catch (const std::exception &e) {
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

  if (mon->osdmon()->osdmap.get_epoch()) {
    map_pg_creates();
    send_pg_creates();
  }

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
  if (mon->osdmon()->osdmap.get_epoch()) {
    map_pg_creates();
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
    // clear our osdmap epoch so that map_pg_creates() will re-run
    last_map_pg_create_osd_epoch = 0;
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
    bufferlist bl;
    int r = mon->store->get(pgmap_pg_prefix, stringify(pgid), bl);
    dout(20) << " refreshing pg " << pgid << " got " << r << " len "
	     << bl.length() << dendl;

    if (pg_pool_sum_old.count(pgid.pool()) == 0)
      pg_pool_sum_old[pgid.pool()] = pg_map.pg_pool_sum[pgid.pool()];

    if (r >= 0) {
      pg_map.update_pg(pgid, bl);
    } else {
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

bool PGMonitor::preprocess_query(PaxosServiceMessage *m)
{
  dout(10) << "preprocess_query " << *m << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case CEPH_MSG_STATFS:
    handle_statfs(static_cast<MStatfs*>(m));
    return true;
  case MSG_GETPOOLSTATS:
    return preprocess_getpoolstats(static_cast<MGetPoolStats*>(m));
    
  case MSG_PGSTATS:
    return preprocess_pg_stats(static_cast<MPGStats*>(m));

  case MSG_MON_COMMAND:
    return preprocess_command(static_cast<MMonCommand*>(m));


  default:
    assert(0);
    m->put();
    return true;
  }
}

bool PGMonitor::prepare_update(PaxosServiceMessage *m)
{
  dout(10) << "prepare_update " << *m << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case MSG_PGSTATS:
    return prepare_pg_stats((MPGStats*)m);

  case MSG_MON_COMMAND:
    return prepare_command(static_cast<MMonCommand*>(m));

  default:
    assert(0);
    m->put();
    return false;
  }
}

void PGMonitor::handle_statfs(MStatfs *statfs)
{
  // check caps
  MonSession *session = statfs->get_session();
  if (!session)
    goto out;
  if (!session->is_capable("pg", MON_CAP_R)) {
    dout(0) << "MStatfs received from entity with insufficient privileges "
	    << session->caps << dendl;
    goto out;
  }
  MStatfsReply *reply;

  dout(10) << "handle_statfs " << *statfs << " from " << statfs->get_orig_source() << dendl;

  if (statfs->fsid != mon->monmap->fsid) {
    dout(0) << "handle_statfs on fsid " << statfs->fsid << " != " << mon->monmap->fsid << dendl;
    goto out;
  }

  // fill out stfs
  reply = new MStatfsReply(mon->monmap->fsid, statfs->get_tid(), get_last_committed());

  // these are in KB.
  reply->h.st.kb = pg_map.osd_sum.kb;
  reply->h.st.kb_used = pg_map.osd_sum.kb_used;
  reply->h.st.kb_avail = pg_map.osd_sum.kb_avail;
  reply->h.st.num_objects = pg_map.pg_sum.stats.sum.num_objects;

  // reply
  mon->send_reply(statfs, reply);
 out:
  statfs->put();
}

bool PGMonitor::preprocess_getpoolstats(MGetPoolStats *m)
{
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

  mon->send_reply(m, reply);

 out:
  m->put();
  return true;
}


bool PGMonitor::preprocess_pg_stats(MPGStats *stats)
{
  // check caps
  MonSession *session = stats->get_session();
  if (!session) {
    dout(10) << "PGMonitor::preprocess_pg_stats: no monitor session!" << dendl;
    stats->put();
    return true;
  }
  if (!session->is_capable("pg", MON_CAP_R)) {
    derr << "PGMonitor::preprocess_pg_stats: MPGStats received from entity "
         << "with insufficient privileges " << session->caps << dendl;
    stats->put();
    return true;
  }

  // First, just see if they need a new osdmap. But
  // only if they've had the map for a while.
  if (stats->had_map_for > 30.0 && 
      mon->osdmon()->is_readable() &&
      stats->epoch < mon->osdmon()->osdmap.get_epoch())
    mon->osdmon()->send_latest_now_nodelete(stats, stats->epoch+1);

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

bool PGMonitor::prepare_pg_stats(MPGStats *stats) 
{
  dout(10) << "prepare_pg_stats " << *stats << " from " << stats->get_orig_source() << dendl;
  int from = stats->get_orig_source().num();

  if (stats->fsid != mon->monmap->fsid) {
    dout(0) << "prepare_pg_stats on fsid " << stats->fsid << " != " << mon->monmap->fsid << dendl;
    stats->put();
    return false;
  }

  last_osd_report[from] = ceph_clock_now(g_ceph_context);

  if (!stats->get_orig_source().is_osd() ||
      !mon->osdmon()->osdmap.is_up(from) ||
      stats->get_orig_source_inst() != mon->osdmon()->osdmap.get_inst(from)) {
    dout(1) << " ignoring stats from non-active osd." << dendl;
    stats->put();
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
    mon->send_reply(stats, ack);
    stats->put();
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

    /*
    // we don't care much about consistency, here; apply to live map.
    pg_map.stat_pg_sub(pgid, pg_map.pg_stat[pgid]);
    pg_map.pg_stat[pgid] = p->second;
    pg_map.stat_pg_add(pgid, pg_map.pg_stat[pgid]);
    */
  }
  
  wait_for_finished_proposal(new C_Stats(this, stats, ack));
  return true;
}

void PGMonitor::_updated_stats(MPGStats *req, MPGStatsAck *ack)
{
  dout(7) << "_updated_stats for " << req->get_orig_source_inst() << dendl;
  mon->send_reply(req, ack);
  req->put();
}



// ------------------------

struct RetryCheckOSDMap : public Context {
  PGMonitor *pgmon;
  epoch_t epoch;
  RetryCheckOSDMap(PGMonitor *p, epoch_t e) : pgmon(p), epoch(e) {}
  void finish(int r) {
    if (r == -ECANCELED)
      return;
    pgmon->check_osd_map(epoch);
  }
};

void PGMonitor::check_osd_map(epoch_t epoch)
{
  if (mon->is_peon()) 
    return; // whatever.

  if (pg_map.last_osdmap_epoch >= epoch) {
    dout(10) << "check_osd_map already seen " << pg_map.last_osdmap_epoch << " >= " << epoch << dendl;
    return;
  }

  if (!mon->osdmon()->is_readable()) {
    dout(10) << "check_osd_map -- osdmap not readable, waiting" << dendl;
    mon->osdmon()->wait_for_readable(new RetryCheckOSDMap(this, epoch));
    return;
  }

  if (!is_writeable()) {
    dout(10) << "check_osd_map -- pgmap not writeable, waiting" << dendl;
    wait_for_writeable(new RetryCheckOSDMap(this, epoch));
    return;
  }

  // apply latest map(s)
  for (epoch_t e = pg_map.last_osdmap_epoch+1;
       e <= epoch;
       e++) {
    dout(10) << "check_osd_map applying osdmap e" << e << " to pg_map" << dendl;
    bufferlist bl;
    int err = mon->osdmon()->get_version(e, bl);
    assert(err == 0);

    assert(bl.length());
    OSDMap::Incremental inc(bl);
    for (map<int32_t,uint32_t>::iterator p = inc.new_weight.begin();
	 p != inc.new_weight.end();
	 ++p)
      if (p->second == CEPH_OSD_OUT) {
	dout(10) << "check_osd_map  osd." << p->first << " went OUT" << dendl;
	pending_inc.stat_osd_out(p->first);
      }

    // this is conservative: we want to know if any osds (maybe) got marked down.
    for (map<int32_t,uint8_t>::iterator p = inc.new_state.begin();
	 p != inc.new_state.end();
	 ++p) {
      if (p->second & CEPH_OSD_UP) {   // true if marked up OR down, but we're too lazy to check which
	need_check_down_pgs = true;

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
	dout(10) << "check_osd_map  osd." << p->first << " created or destroyed" << dendl;
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

  // scan pg space?
  if (register_new_pgs())
    propose = true;

  if (need_check_down_pgs && check_down_pgs())
    propose = true;
  
  if (propose)
    propose_pending();

  if (mon->osdmon()->osdmap.get_epoch()) {
    map_pg_creates();
    send_pg_creates();
  }
}

void PGMonitor::register_pg(pg_pool_t& pool, pg_t pgid, epoch_t epoch, bool new_pool)
{
  pg_t parent;
  int split_bits = 0;
  bool parent_found = false;
  if (!new_pool) {
    parent = pgid;
    while (1) {
      // remove most significant bit
      int msb = pool.calc_bits_of(parent.ps());
      if (!msb) break;
      parent.set_ps(parent.ps() & ~(1<<(msb-1)));
      split_bits++;
      dout(10) << " is " << pgid << " parent " << parent << " ?" << dendl;
      //if (parent.u.pg.ps < mon->osdmon->osdmap.get_pgp_num()) {
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

  if (parent_found) {
    stats.last_scrub_stamp = pg_map.pg_stat[parent].last_scrub_stamp;
    stats.last_deep_scrub_stamp = pg_map.pg_stat[parent].last_deep_scrub_stamp;
    stats.last_clean_scrub_stamp = pg_map.pg_stat[parent].last_clean_scrub_stamp;
  } else {
    utime_t now = ceph_clock_now(g_ceph_context);
    stats.last_scrub_stamp = now;
    stats.last_deep_scrub_stamp = now;
    stats.last_clean_scrub_stamp = now;
  }


  if (split_bits == 0) {
    dout(10) << "register_new_pgs  will create " << pgid << dendl;
  } else {
    dout(10) << "register_new_pgs  will create " << pgid
	     << " parent " << parent
	     << " by " << split_bits << " bits"
	     << dendl;
  }
}

bool PGMonitor::register_new_pgs()
{
  // iterate over crush mapspace
  epoch_t epoch = mon->osdmon()->osdmap.get_epoch();
  dout(10) << "register_new_pgs checking pg pools for osdmap epoch " << epoch
	   << ", last_pg_scan " << pg_map.last_pg_scan << dendl;

  OSDMap *osdmap = &mon->osdmon()->osdmap;

  int created = 0;
  for (map<int64_t,pg_pool_t>::iterator p = osdmap->pools.begin();
       p != osdmap->pools.end();
       ++p) {
    int64_t poolid = p->first;
    pg_pool_t &pool = p->second;
    int ruleno = osdmap->crush->find_rule(pool.get_crush_ruleset(), pool.get_type(), pool.get_size());
    if (ruleno < 0 || !osdmap->crush->rule_exists(ruleno))
      continue;

    if (pool.get_last_change() <= pg_map.last_pg_scan ||
	pool.get_last_change() <= pending_inc.pg_scan) {
      dout(10) << " no change in pool " << p->first << " " << pool << dendl;
      continue;
    }

    dout(10) << "register_new_pgs scanning pool " << p->first << " " << pool << dendl;

    bool new_pool = pg_map.pg_pool_sum.count(poolid) == 0;  // first pgs in this pool

    for (ps_t ps = 0; ps < pool.get_pg_num(); ps++) {
      pg_t pgid(ps, poolid, -1);
      if (pg_map.pg_stat.count(pgid)) {
	dout(20) << "register_new_pgs  have " << pgid << dendl;
	continue;
      }
      created++;
      register_pg(pool, pgid, pool.get_last_change(), new_pool);
    }
  }

  int removed = 0;
  for (set<pg_t>::iterator p = pg_map.creating_pgs.begin();
       p != pg_map.creating_pgs.end();
       ++p) {
    if (p->preferred() >= 0) {
      dout(20) << " removing creating_pg " << *p << " because it is localized and obsolete" << dendl;
      pending_inc.pg_remove.insert(*p);
      removed++;
    }
    if (!osdmap->have_pg_pool(p->pool())) {
      dout(20) << " removing creating_pg " << *p << " because containing pool deleted" << dendl;
      pending_inc.pg_remove.insert(*p);
      ++removed;
    }
  }

  // deleted pools?
  for (ceph::unordered_map<pg_t,pg_stat_t>::const_iterator p = pg_map.pg_stat.begin();
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

  dout(10) << "register_new_pgs registered " << created << " new pgs, removed "
	   << removed << " uncreated pgs" << dendl;
  if (created || removed) {
    pending_inc.pg_scan = epoch;
    return true;
  }
  return false;
}

void PGMonitor::map_pg_creates()
{
  OSDMap *osdmap = &mon->osdmon()->osdmap;
  if (osdmap->get_epoch() == last_map_pg_create_osd_epoch) {
    dout(10) << "map_pg_creates to " << pg_map.creating_pgs.size() << " pgs -- no change" << dendl;
    return;
  }

  dout(10) << "map_pg_creates to " << pg_map.creating_pgs.size() << " pgs osdmap epoch " << osdmap->get_epoch() << dendl;
  last_map_pg_create_osd_epoch = osdmap->get_epoch();

  for (set<pg_t>::iterator p = pg_map.creating_pgs.begin();
       p != pg_map.creating_pgs.end();
       ++p) {
    pg_t pgid = *p;
    pg_t on = pgid;
    pg_stat_t *s = NULL;
    ceph::unordered_map<pg_t,pg_stat_t>::iterator q = pg_map.pg_stat.find(pgid);
    if (q == pg_map.pg_stat.end()) {
      s = &pg_map.pg_stat[pgid];
    } else {
      s = &q->second;
      pg_map.stat_pg_sub(pgid, *s, true);
    }

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

    if (s->acting_primary != -1) {
      pg_map.creating_pgs_by_osd[s->acting_primary].erase(pgid);
      if (pg_map.creating_pgs_by_osd[s->acting_primary].size() == 0)
        pg_map.creating_pgs_by_osd.erase(s->acting_primary);
    }
    s->up = up;
    s->up_primary = up_primary;
    s->acting = acting;
    s->acting_primary = acting_primary;
    pg_map.stat_pg_add(pgid, *s, true);

    // don't send creates for localized pgs
    if (pgid.preferred() >= 0)
      continue;

    // don't send creates for splits
    if (s->parent_split_bits)
      continue;

    if (acting_primary != -1) {
      pg_map.creating_pgs_by_osd[acting_primary].insert(pgid);
    } else {
      dout(20) << "map_pg_creates  " << pgid << " -> no osds in epoch "
	       << mon->osdmon()->osdmap.get_epoch() << ", skipping" << dendl;
      continue;  // blarney!
    }
  }
  for (map<int, set<pg_t> >::iterator p = pg_map.creating_pgs_by_osd.begin();
       p != pg_map.creating_pgs_by_osd.end();
       ++p) {
    dout(10) << "map_pg_creates osd." << p->first << " has " << p->second.size() << " pgs" << dendl;
  }
}

void PGMonitor::send_pg_creates()
{
  dout(10) << "send_pg_creates to " << pg_map.creating_pgs.size() << " pgs" << dendl;

  utime_t now = ceph_clock_now(g_ceph_context);
  for (map<int, set<pg_t> >::iterator p = pg_map.creating_pgs_by_osd.begin();
       p != pg_map.creating_pgs_by_osd.end();
       ++p) {
    int osd = p->first;

    // throttle?
    if (last_sent_pg_create.count(osd) &&
	now - g_conf->mon_pg_create_interval < last_sent_pg_create[osd]) 
      continue;
      
    if (mon->osdmon()->osdmap.is_up(osd))
      send_pg_creates(osd, NULL);
  }
}

void PGMonitor::send_pg_creates(int osd, Connection *con)
{
  map<int, set<pg_t> >::iterator p = pg_map.creating_pgs_by_osd.find(osd);
  if (p == pg_map.creating_pgs_by_osd.end())
    return;
  assert(p->second.size() > 0);

  dout(20) << "send_pg_creates osd." << osd << " pgs " << p->second << dendl;
  MOSDPGCreate *m = new MOSDPGCreate(mon->osdmon()->osdmap.get_epoch());
  for (set<pg_t>::iterator q = p->second.begin(); q != p->second.end(); ++q) {
    m->mkpg[*q] = pg_create_t(pg_map.pg_stat[*q].created,
			      pg_map.pg_stat[*q].parent,
			      pg_map.pg_stat[*q].parent_split_bits);
    // Need the create time from the monitor using his clock to set last_scrub_stamp
    // upon pg creation.
    m->ctimes[*q] = pg_map.pg_stat[*q].last_scrub_stamp;
  }

  if (con) {
    con->send_message(m);
  } else {
    assert(mon->osdmon()->osdmap.is_up(osd));
    mon->messenger->send_message(m, mon->osdmon()->osdmap.get_inst(osd));
  }
  last_sent_pg_create[osd] = ceph_clock_now(g_ceph_context);
}

bool PGMonitor::check_down_pgs()
{
  dout(10) << "check_down_pgs" << dendl;

  OSDMap *osdmap = &mon->osdmon()->osdmap;
  bool ret = false;

  for (ceph::unordered_map<pg_t,pg_stat_t>::iterator p = pg_map.pg_stat.begin();
       p != pg_map.pg_stat.end();
       ++p) {
    if ((p->second.state & PG_STATE_STALE) == 0 &&
	p->second.acting_primary != -1 &&
	osdmap->is_down(p->second.acting_primary)) {
      dout(10) << " marking pg " << p->first << " stale with acting " << p->second.acting << dendl;

      map<pg_t,pg_stat_t>::iterator q = pending_inc.pg_stat_updates.find(p->first);
      pg_stat_t *stat;
      if (q == pending_inc.pg_stat_updates.end()) {
	stat = &pending_inc.pg_stat_updates[p->first];
	*stat = p->second;
      } else {
	stat = &q->second;
      }
      stat->state |= PG_STATE_STALE;
      stat->last_unstale = ceph_clock_now(g_ceph_context);
      ret = true;
    }
  }
  need_check_down_pgs = false;

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
				     bool verbose)
{
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
    }
  } else {
    tbl << stringify(si_t(sum.num_bytes));
    int64_t kb_used = SHIFT_ROUND_UP(sum.num_bytes, 10);
    float used = 0.0;
    if (pg_map.osd_sum.kb > 0)
      used = (float)kb_used / pg_map.osd_sum.kb;
    tbl << percentify(used*100);
    tbl << si_t(avail);
    tbl << sum.num_objects;
    if (verbose) {
      tbl << stringify(si_t(sum.num_objects_dirty))
	  << stringify(si_t(sum.num_rd))
          << stringify(si_t(sum.num_wr));
    }
  }
}

int64_t PGMonitor::get_rule_avail(OSDMap& osdmap, int ruleno)
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
      if (osd_info->second.kb == 0) {
        // osd must be out, hence its stats have been zeroed
        // (unless we somehow managed to have a disk with size 0...)
        continue;
      }
      int64_t proj = (float)((osd_info->second).kb_avail * 1024ull) /
        (double)p->second;
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
    dump_object_stat_sum(tbl, f, stat.stats.sum, avail, verbose);
    if (f)
      f->close_section(); // stats
    else
      tbl << TextTable::endrow;

    if (f)
      f->close_section(); // pool
  }
  if (f)
    f->close_section();
  else {
    ss << "POOLS:\n";
    tbl.set_indent(4);
    ss << tbl;
  }
}

void PGMonitor::dump_fs_stats(stringstream &ss, Formatter *f, bool verbose)
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


void PGMonitor::dump_info(Formatter *f)
{
  f->open_object_section("pgmap");
  pg_map.dump(f);
  f->close_section();

  f->dump_unsigned("pgmap_first_committed", get_first_committed());
  f->dump_unsigned("pgmap_last_committed", get_last_committed());
}

bool PGMonitor::preprocess_command(MMonCommand *m)
{
  int r = -1;
  bufferlist rdata;
  stringstream ss, ds;
  bool primary = false;

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    // ss has reason for failure
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
    int64_t pool = -2;
    pool = mon->osdmon()->osdmap.lookup_pg_pool_name(poolstr.c_str());
    if (pool < 0) {
      r = -ENOENT;
      ss << "pool " << poolstr << " does not exist";
      string rs = ss.str();
      mon->reply_command(m, r, rs, get_last_committed());
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
  } else if (prefix == "pg map_pg_creates") {
    map_pg_creates();
    ss << "mapped pg creates ";
    r = 0;
  } else if (prefix == "pg send_pg_creates") {
    send_pg_creates();
    ss << "sent pg creates ";
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
	  header = false;
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
    if (f && !pgs.empty()){
      pg_map.dump_filtered_pg_stats(f.get(),pgs);
      f->flush(ds);
    } else if (!pgs.empty()){
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
  mon->reply_command(m, r, rs, rdata, get_last_committed());
  return true;
}

bool PGMonitor::prepare_command(MMonCommand *m)
{
  stringstream ss;
  pg_t pgid;
  epoch_t epoch = mon->osdmon()->osdmap.get_epoch();
  int r = 0;
  string rs;

  map<string, cmd_vartype> cmdmap;
  if (!cmdmap_from_json(m->cmd, &cmdmap, ss)) {
    // ss has reason for failure
    string rs = ss.str();
    mon->reply_command(m, -EINVAL, rs, get_last_committed());
    return true;
  }

  string prefix;
  cmd_getval(g_ceph_context, cmdmap, "prefix", prefix);

  MonSession *session = m->get_session();
  if (!session) {
    mon->reply_command(m, -EACCES, "access denied", get_last_committed());
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
  mon->reply_command(m, r, rs, get_last_committed());
  return false;

 update:
  getline(ss, rs);
  wait_for_finished_proposal(new Monitor::C_Command(mon, m, r, rs,
						    get_last_committed() + 1));
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
    }else {
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

void PGMonitor::get_health(list<pair<health_status_t,string> >& summary,
			   list<pair<health_status_t,string> > *detail) const
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
      note["backfill"] += p->second;
    if (p->first & PG_STATE_BACKFILL)
      note["backfilling"] += p->second;
    if (p->first & PG_STATE_BACKFILL_TOOFULL)
      note["backfill_toofull"] += p->second;
  }

  ceph::unordered_map<pg_t, pg_stat_t> stuck_pgs;
  utime_t now(ceph_clock_now(g_ceph_context));
  utime_t cutoff = now - utime_t(g_conf->mon_pg_stuck_threshold, 0);

  pg_map.get_stuck_stats(PGMap::STUCK_INACTIVE, cutoff, stuck_pgs);
  if (!stuck_pgs.empty()) {
    note["stuck inactive"] = stuck_pgs.size();
    if (detail)
      note_stuck_detail(PGMap::STUCK_INACTIVE, stuck_pgs, detail);
  }
  stuck_pgs.clear();

  pg_map.get_stuck_stats(PGMap::STUCK_UNCLEAN, cutoff, stuck_pgs);
  if (!stuck_pgs.empty()) {
    note["stuck unclean"] = stuck_pgs.size();
    if (detail)
      note_stuck_detail(PGMap::STUCK_UNCLEAN, stuck_pgs, detail);
  }
  stuck_pgs.clear();

  pg_map.get_stuck_stats(PGMap::STUCK_UNDERSIZED, cutoff, stuck_pgs);
  if (!stuck_pgs.empty()) {
    note["stuck undersized"] = stuck_pgs.size();
    if (detail)
      note_stuck_detail(PGMap::STUCK_UNDERSIZED, stuck_pgs, detail);
  }
  stuck_pgs.clear();

  pg_map.get_stuck_stats(PGMap::STUCK_DEGRADED, cutoff, stuck_pgs);
  if (!stuck_pgs.empty()) {
    note["stuck degraded"] = stuck_pgs.size();
    if (detail)
      note_stuck_detail(PGMap::STUCK_DEGRADED, stuck_pgs, detail);
  }
  stuck_pgs.clear();

  pg_map.get_stuck_stats(PGMap::STUCK_STALE, cutoff, stuck_pgs);
  if (!stuck_pgs.empty()) {
    note["stuck stale"] = stuck_pgs.size();
    if (detail)
      note_stuck_detail(PGMap::STUCK_STALE, stuck_pgs, detail);
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
    unsigned sum = _warn_slow_request_histogram(pg_map.osd_sum.op_queue_age_hist, "", summary, detail);
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
    if (p->second.target_max_objects && (uint64_t)st.stats.sum.num_objects >
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
    if (p->second.target_max_bytes && (uint64_t)st.stats.sum.num_bytes >
	p->second.target_max_bytes * (ratio / 1000000.0)) {
      nearfull = true;
      if (detail) {
	ostringstream ss;
	ss << "cache pool '" << mon->osdmon()->osdmap.get_pool_name(p->first)
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
    if (per < g_conf->mon_pg_warn_min_per_osd) {
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
      if (pi->get_pg_num() > pi->get_pgp_num()) {
	ostringstream ss;
	ss << "pool " << mon->osdmon()->osdmap.get_pool_name(p->first) << " pg_num "
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
	  ss << "pool " << mon->osdmon()->osdmap.get_pool_name(p->first) << " has too few pgs";
	  summary.push_back(make_pair(HEALTH_WARN, ss.str()));
	  if (detail) {
	    ostringstream ss;
	    ss << "pool " << mon->osdmon()->osdmap.get_pool_name(p->first) << " objects per pg ("
	       << objects_per_pg << ") is more than " << ratio << " times cluster average ("
	       << average_objects_per_pg << ")";
	    detail->push_back(make_pair(HEALTH_WARN, ss.str()));
	  }
	}
      }
    }
  }
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

  for (vector<string>::iterator i = args.begin() ; i != args.end(); ++i) {
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
      return 0;
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

void PGMonitor::check_sub(Subscription *sub)
{
  if (sub->type == "osd_pg_creates") {
    send_pg_creates(sub->session->inst.name.num(),
		    sub->session->con.get());
  }
}
