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
#include "OSDMonitor.h"
#include "MonitorDBStore.h"
#include "PGStatService.h"

#include "messages/MPGStats.h"
#include "messages/MPGStatsAck.h"

#include "messages/MOSDPGCreate.h"
#include "messages/MMonCommand.h"
#include "messages/MOSDScrub.h"

#include "common/Formatter.h"
#include "common/config.h"

#include "include/stringify.h"

#include "osd/osd_types.h"

#include "common/config.h"
#include "common/errno.h"
#include "common/strtol.h"
#include "include/str_list.h"
#include <sstream>

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
  last_osd_report.clear();
}

void PGMonitor::on_active()
{
  if (mon->is_leader()) {
    check_all_pgs = true;
    check_osd_map(mon->osdmon()->osdmap.get_epoch());
  }

  update_logger();

  if (mon->is_leader() &&
      mon->osdmon()->osdmap.require_osd_release < CEPH_RELEASE_LUMINOUS) {
    mon->clog->info() << "pgmap " << pg_map;
  }
}

void PGMonitor::update_logger()
{
  if (mon->osdmon()->osdmap.require_osd_release >= CEPH_RELEASE_LUMINOUS) {
    return;
  }
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
  if (mon->osdmon()->osdmap.require_osd_release >= CEPH_RELEASE_LUMINOUS) {
    return;
  }

  handle_osd_timeouts();

  if (!pg_map.pg_sum_deltas.empty()) {
    utime_t age = ceph_clock_now() - pg_map.stamp;
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
      utime_t age = ceph_clock_now() - it->second.second;
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
  if (did_delete)
    return;

  if (get_value("deleted")) {
    did_delete = true;
    dout(10) << __func__ << " deleted, clearing in-memory PGMap" << dendl;
    pg_map = PGMap();
    pending_inc = PGMap::Incremental();
    pgservice.reset();
    last_osd_report.clear();
    return;
  }

  version_t version = get_last_committed();
  if (version == pg_map.version)
    return;

  assert(version >= pg_map.version);
  if (format_version < 1) {
    derr << __func__ << "unsupported monitor protocol: "
	 << get_service_name() << ".format_version = "
	 << format_version << dendl;
  }
  assert(format_version >= 1);

  // pg/osd keys in leveldb
  // read meta
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
      // reset pg map
      pg_map = PGMap();
      read_pgmap_full();
      goto out;
    }
    assert(r == 0);
    apply_pgmap_delta(bl);
  }

  read_pgmap_meta();

out:
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
  assert(format_version == current);
}

void PGMonitor::post_paxos_update()
{
  if (did_delete)
    return;
  dout(10) << __func__ << dendl;
  OSDMap& osdmap = mon->osdmon()->osdmap;
  if (mon->monmap->get_required_features().contains_all(
	ceph::features::mon::FEATURE_LUMINOUS)) {
    // let OSDMonitor take care of the pg-creates subscriptions.
    return;
  }
  if (osdmap.get_epoch()) {
    if (osdmap.get_num_up_osds() > 0) {
      assert(osdmap.get_up_osd_features() & CEPH_FEATURE_MON_STATEFUL_SUB);
      check_subs();
    }
  }
}

void PGMonitor::handle_osd_timeouts()
{
  if (!mon->is_leader())
    return;
  if (did_delete)
    return;

  utime_t now(ceph_clock_now());
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
  if (did_delete)
    return;
  do_delete = false;
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
  mempool::pgmap::unordered_map<uint64_t, pool_stat_t> pg_pool_sum_old;

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
  if (did_delete)
    return;

  string prefix = pgmap_meta_prefix;
  if (do_delete) {
    dout(1) << __func__ << " clearing pgmap data at v" << pending_inc.version
	    << dendl;
    do_delete = false;
    for (auto key : { "version", "stamp", "last_osdmap_epoch",
	  "last_pg_scan", "full_ratio", "nearfull_ratio" }) {
      t->erase(prefix, key);
    }
    for (auto& p : pg_map.pg_stat) {
      t->erase(prefix, stringify(p.first));
    }
    for (auto& p : pg_map.osd_stat) {
      t->erase(prefix, stringify(p.first));
    }
    put_last_committed(t, pending_inc.version);
    put_value(t, "deleted", 1);
    return;
  }

  assert(mon->osdmon()->osdmap.require_osd_release < CEPH_RELEASE_LUMINOUS ||
	 pending_inc.version == 1  /* rebuild-mondb.yaml case */);

  version_t version = pending_inc.version;
  dout(10) << __func__ << " v " << version << dendl;
  assert(get_last_committed() + 1 == version);
  pending_inc.stamp = ceph_clock_now();

  uint64_t features = mon->get_quorum_con_features();

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
  dout(10) << "preprocess_query " << *m
	   << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case MSG_PGSTATS:
    if (mon->osdmon()->osdmap.require_osd_release >= CEPH_RELEASE_LUMINOUS) {
      return true;
    }
    return preprocess_pg_stats(op);

  case MSG_MON_COMMAND:
    if (mon->osdmon()->osdmap.require_osd_release >= CEPH_RELEASE_LUMINOUS) {
      bufferlist rdata;
      mon->reply_command(op, -EOPNOTSUPP, "this command is obsolete", rdata,
			 get_last_committed());
      return true;
    }
    return preprocess_command(op);

  default:
    ceph_abort();
    return true;
  }
}

bool PGMonitor::prepare_update(MonOpRequestRef op)
{
  if (mon->osdmon()->osdmap.require_osd_release >= CEPH_RELEASE_LUMINOUS) {
    return false;
  }

  op->mark_pgmon_event(__func__);
  PaxosServiceMessage *m = static_cast<PaxosServiceMessage*>(op->get_req());
  dout(10) << "prepare_update " << *m << " from " << m->get_orig_source_inst() << dendl;
  switch (m->get_type()) {
  case MSG_PGSTATS:
    return prepare_pg_stats(op);

  case MSG_MON_COMMAND:
    return prepare_command(op);

  default:
    ceph_abort();
    return false;
  }
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

  if (stats->fsid != mon->monmap->fsid) {
    dout(0) << __func__ << " drop message on fsid " << stats->fsid << " != "
            << mon->monmap->fsid << " for " << *stats << dendl;
    return true;
  }

  // First, just see if they need a new osdmap. But
  // only if they've had the map for a while.
  if (stats->had_map_for > 30.0 &&
      mon->osdmon()->is_readable() &&
      stats->epoch < mon->osdmon()->osdmap.get_epoch() &&
      !session->proxy_con)
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

struct PGMonitor::C_Stats : public C_MonOp {
  PGMonitor *pgmon;
  MonOpRequestRef stats_op_ack;
  entity_inst_t who;
  C_Stats(PGMonitor *p,
          MonOpRequestRef op,
          MonOpRequestRef op_ack)
    : C_MonOp(op), pgmon(p), stats_op_ack(op_ack) {}
  void _finish(int r) override {
    if (r >= 0) {
      pgmon->_updated_stats(op, stats_op_ack);
    } else if (r == -ECANCELED) {
      return;
    } else if (r == -EAGAIN) {
      pgmon->dispatch(op);
    } else {
      assert(0 == "bad C_Stats return value");
    }
  }
};

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

  if (!stats->get_orig_source().is_osd() ||
      !mon->osdmon()->osdmap.is_up(from) ||
      stats->get_orig_source_inst() != mon->osdmon()->osdmap.get_inst(from)) {
    dout(1) << " ignoring stats from non-active osd." << dendl;
    return false;
  }
      
  last_osd_report[from] = ceph_clock_now();

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
    pending_inc.update_stat(from, stats->epoch, std::move(stats->osd_stat));
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
  void finish(int r) override {
    if (r == -ECANCELED)
      return;

    pgmon->check_osd_map(epoch);
  }
};

void PGMonitor::check_osd_map(epoch_t epoch)
{
  if (mon->is_peon())
    return;  // whatever.

  if (did_delete)
    return;

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

  const OSDMap& osdmap = mon->osdmon()->osdmap;
  if (!did_delete && osdmap.require_osd_release >= CEPH_RELEASE_LUMINOUS) {
    // delete all my data
    dout(1) << __func__ << " will clear pg_map data" << dendl;
    do_delete = true;
    propose_pending();
    return;
  }

  // osds that went up or down
  set<int> need_check_down_pg_osds;

  // apply latest map(s)
  epoch = std::max(epoch, osdmap.get_epoch());
  for (epoch_t e = pg_map.last_osdmap_epoch+1;
       e <= epoch;
       e++) {
    dout(10) << __func__ << " applying osdmap e" << e << " to pg_map" << dendl;
    bufferlist bl;
    int err = mon->osdmon()->get_version(e, bl);
    assert(err == 0);

    assert(bl.length());
    OSDMap::Incremental inc(bl);

    PGMapUpdater::check_osd_map(inc, &need_check_down_pg_osds,
                                &last_osd_report, &pg_map, &pending_inc);
  }

  assert(pg_map.last_osdmap_epoch < epoch);
  pending_inc.osdmap_epoch = epoch;
  PGMapUpdater::update_creating_pgs(osdmap, pg_map, &pending_inc);
  PGMapUpdater::register_new_pgs(osdmap, pg_map, &pending_inc);

  PGMapUpdater::check_down_pgs(osdmap, pg_map, check_all_pgs,
			       need_check_down_pg_osds, &pending_inc);
  check_all_pgs = false;

  propose_pending();
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

  con->send_message(m);

  // sub is current through last + 1
  return last + 1;
}

bool PGMonitor::preprocess_command(MonOpRequestRef op)
{
  op->mark_pgmon_event(__func__);
  MMonCommand *m = static_cast<MMonCommand*>(op->get_req());
  int r = -1;
  bufferlist rdata;
  stringstream ss, ds;

  if (m->fsid != mon->monmap->fsid) {
    dout(0) << __func__ << " drop message on fsid " << m->fsid << " != "
            << mon->monmap->fsid << " for " << *m << dendl;
    return true;
  }

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

  string format;
  cmd_getval(g_ceph_context, cmdmap, "format", format, string("plain"));
  boost::scoped_ptr<Formatter> f(Formatter::create(format));

  if (prefix == "pg scrub" ||
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
    int osd = pg_map.pg_stat[pgid].acting_primary;
    if (osd == -1) {
      ss << "pg " << pgid << " has no primary osd";
      r = -EAGAIN;
      goto reply;
    }
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
  } else {
    r = process_pg_map_command(prefix, cmdmap, pg_map, mon->osdmon()->osdmap,
			       f.get(), &ss, &rdata);
  }

  if (r == -EOPNOTSUPP)
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
  if (m->fsid != mon->monmap->fsid) {
    dout(0) << __func__ << " drop message on fsid " << m->fsid << " != "
            << mon->monmap->fsid << " for " << *m << dendl;
    return true;
  }
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
      PGMapUpdater::register_pg(
	mon->osdmon()->osdmap,
	pgid,
	epoch,
	true,
	pg_map,
	&pending_inc);
    }
    ss << "pg " << pgidstr << " now creating, ok";
    goto update;
  } else if (prefix == "pg set_full_ratio" ||
             prefix == "pg set_nearfull_ratio") {
    if (mon->osdmon()->osdmap.require_osd_release >= CEPH_RELEASE_LUMINOUS) {
      ss << "please use the new luminous interfaces"
	 << " ('osd set-full-ratio' and 'osd set-nearfull-ratio')";
      r = -EPERM;
      goto reply;
    }
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

void PGMonitor::get_health(list<pair<health_status_t,string> >& summary,
			   list<pair<health_status_t,string> > *detail,
			   CephContext *cct) const
{
  // legacy pre-luminous full/nearfull
  if (mon->osdmon()->osdmap.require_osd_release < CEPH_RELEASE_LUMINOUS) {
    check_full_osd_health(summary, detail, pg_map.full_osds, "full",
			  HEALTH_ERR);
    check_full_osd_health(summary, detail, pg_map.nearfull_osds, "near full",
			  HEALTH_WARN);
    pg_map.get_health(cct, mon->osdmon()->osdmap, summary, detail);
  }
}

void PGMonitor::check_full_osd_health(list<pair<health_status_t,string> >& summary,
                                      list<pair<health_status_t,string> > *detail,
                                      const mempool::pgmap::set<int>& s, const char *desc,
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

void PGMonitor::check_subs()
{
  if (mon->osdmon()->osdmap.require_osd_release >= CEPH_RELEASE_LUMINOUS) {
    return;
  }

  dout(10) << __func__ << dendl;
  const string type = "osd_pg_creates";

  mon->with_session_map([this, &type](const MonSessionMap& session_map) {
      if (mon->session_map.subs.count(type) == 0)
	return;

      auto p = mon->session_map.subs[type]->begin();
      while (!p.end()) {
	Subscription *sub = *p;
	++p;
	dout(20) << __func__ << " .. " << sub->session->inst << dendl;
	check_sub(sub);
      }
    });
}

bool PGMonitor::check_sub(Subscription *sub)
{
  OSDMap& osdmap = mon->osdmon()->osdmap;
  if (sub->type == "osd_pg_creates") {
    // only send these if the OSD is up.  we will check_subs() when they do
    // come up so they will get the creates then.
    if (sub->session->inst.name.is_osd() &&
        osdmap.is_up(sub->session->inst.name.num())) {
      sub->next = send_pg_creates(sub->session->inst.name.num(),
                                  sub->session->con.get(),
                                  sub->next);
    }
  }
  return true;
}

class PGMonStatService : public MonPGStatService, public PGMapStatService {
  PGMonitor *pgmon;
public:
  PGMonStatService(const PGMap& o, PGMonitor *pgm)
    : MonPGStatService(), PGMapStatService(o), pgmon(pgm) {}
       

  bool is_readable() const override { return pgmon->is_readable(); }

  unsigned maybe_add_creating_pgs(epoch_t scan_epoch,
     const mempool::osdmap::map<int64_t,pg_pool_t>& pools,
     creating_pgs_t *pending_creates) const override
  {
    if (pgmap.last_pg_scan < scan_epoch) {
      return 0;
    }
    unsigned added = 0;
    for (auto& pgid : pgmap.creating_pgs) {
      if (!pools.count(pgid.pool())) {
	continue;
      }
      auto st = pgmap.pg_stat.find(pgid);
      assert(st != pgmap.pg_stat.end());
      auto created = make_pair(st->second.created,
			       st->second.last_scrub_stamp);
      // no need to add the pg, if it already exists in creating_pgs
      if (pending_creates->pgs.emplace(pgid, created).second) {
	added++;
      }
    }
    return added;
  }
  void maybe_trim_creating_pgs(creating_pgs_t *creates) const override {
    auto p = creates->pgs.begin();
    while (p != creates->pgs.end()) {
      auto q = pgmap.pg_stat.find(p->first);
      if (q != pgmap.pg_stat.end() &&
	  !(q->second.state & PG_STATE_CREATING)) {
	p = creates->pgs.erase(p);
	creates->created_pools.insert(q->first.pool());
      } else {
	++p;
      }
    }
  }
  void dump_info(Formatter *f) const override {
    f->dump_object("pgmap", pgmap);
    f->dump_unsigned("pgmap_first_committed", pgmon->get_first_committed());
    f->dump_unsigned("pgmap_last_committed", pgmon->get_last_committed());
  }
  int process_pg_command(const string& prefix,
			 const map<string,cmd_vartype>& cmdmap,
			 const OSDMap& osdmap,
			 Formatter *f,
			 stringstream *ss,
			 bufferlist *odata) const override {
    return process_pg_map_command(prefix, cmdmap, pgmap, osdmap, f, ss, odata);
  }

  int reweight_by_utilization(const OSDMap &osd_map,
			      int oload,
			      double max_changef,
			      int max_osds,
			      bool by_pg, const set<int64_t> *pools,
			      bool no_increasing,
			      mempool::osdmap::map<int32_t, uint32_t>* new_weights,
			      std::stringstream *ss,
			      std::string *out_str,
			      Formatter *f) const override {
    return reweight::by_utilization(osd_map, pgmap, oload, max_changef,
				    max_osds, by_pg, pools, no_increasing,
				    new_weights, ss, out_str, f);
  }
};

MonPGStatService *PGMonitor::get_pg_stat_service()
{
  if (!pgservice) {
    pgservice.reset(new PGMonStatService(pg_map, this));
  }
  return pgservice.get();
}

PGMonitor::PGMonitor(Monitor *mn, Paxos *p, const string& service_name)
  : PaxosService(mn, p, service_name),
    pgmap_meta_prefix("pgmap_meta"),
    pgmap_pg_prefix("pgmap_pg"),
    pgmap_osd_prefix("pgmap_osd")
{}

PGMonitor::~PGMonitor() = default;
