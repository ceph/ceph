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

#include "PG.h"
#include "common/errno.h"
#include "common/config.h"
#include "OSD.h"
#include "OpRequest.h"

#include "common/Timer.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDPGNotify.h"
#include "messages/MOSDPGLog.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGTrim.h"
#include "messages/MOSDPGScan.h"
#include "messages/MOSDPGBackfill.h"
#include "messages/MBackfillReserve.h"
#include "messages/MRecoveryReserve.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPushReply.h"
#include "messages/MOSDPGPull.h"
#include "messages/MOSDECSubOpWrite.h"
#include "messages/MOSDECSubOpWriteReply.h"
#include "messages/MOSDECSubOpRead.h"
#include "messages/MOSDECSubOpReadReply.h"

#include "messages/MOSDSubOp.h"
#include "messages/MOSDSubOpReply.h"
#include "common/BackTrace.h"

#include <sstream>

#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)
template <class T>
static ostream& _prefix(std::ostream *_dout, T *t)
{
  return *_dout << t->gen_prefix();
}

void PG::get(const string &tag) 
{
  ref.inc();
#ifdef PG_DEBUG_REFS
  Mutex::Locker l(_ref_id_lock);
  if (!_tag_counts.count(tag)) {
    _tag_counts[tag] = 0;
  }
  _tag_counts[tag]++;
#endif
}

void PG::put(const string &tag)
{
#ifdef PG_DEBUG_REFS
  {
    Mutex::Locker l(_ref_id_lock);
    assert(_tag_counts.count(tag));
    _tag_counts[tag]--;
    if (_tag_counts[tag] == 0) {
      _tag_counts.erase(tag);
    }
  }
#endif
  if (ref.dec() == 0)
    delete this;
}

#ifdef PG_DEBUG_REFS
uint64_t PG::get_with_id()
{
  ref.inc();
  Mutex::Locker l(_ref_id_lock);
  uint64_t id = ++_ref_id;
  BackTrace bt(0);
  stringstream ss;
  bt.print(ss);
  dout(20) << __func__ << ": " << info.pgid << " got id " << id << dendl;
  assert(!_live_ids.count(id));
  _live_ids.insert(make_pair(id, ss.str()));
  return id;
}

void PG::put_with_id(uint64_t id)
{
  dout(20) << __func__ << ": " << info.pgid << " put id " << id << dendl;
  {
    Mutex::Locker l(_ref_id_lock);
    assert(_live_ids.count(id));
    _live_ids.erase(id);
  }
  if (ref.dec() == 0)
    delete this;
}

void PG::dump_live_ids()
{
  Mutex::Locker l(_ref_id_lock);
  dout(0) << "\t" << __func__ << ": " << info.pgid << " live ids:" << dendl;
  for (map<uint64_t, string>::iterator i = _live_ids.begin();
       i != _live_ids.end();
       ++i) {
    dout(0) << "\t\tid: " << *i << dendl;
  }
  dout(0) << "\t" << __func__ << ": " << info.pgid << " live tags:" << dendl;
  for (map<string, uint64_t>::iterator i = _tag_counts.begin();
       i != _tag_counts.end();
       ++i) {
    dout(0) << "\t\tid: " << *i << dendl;
  }
}
#endif

void PGPool::update(OSDMapRef map)
{
  const pg_pool_t *pi = map->get_pg_pool(id);
  assert(pi);
  info = *pi;
  auid = pi->auid;
  name = map->get_pool_name(id);
  if (pi->get_snap_epoch() == map->get_epoch()) {
    pi->build_removed_snaps(newly_removed_snaps);
    newly_removed_snaps.subtract(cached_removed_snaps);
    cached_removed_snaps.union_of(newly_removed_snaps);
    snapc = pi->get_snap_context();
  } else {
    newly_removed_snaps.clear();
  }
  lgeneric_subdout(g_ceph_context, osd, 20)
    << "PGPool::update cached_removed_snaps "
    << cached_removed_snaps
    << " newly_removed_snaps "
    << newly_removed_snaps
    << " snapc " << snapc
    << (pi->get_snap_epoch() == map->get_epoch() ?
	" (updated)":" (no change)")
    << dendl;
}

PG::PG(OSDService *o, OSDMapRef curmap,
       const PGPool &_pool, spg_t p, const hobject_t& loid,
       const hobject_t& ioid) :
  osd(o),
  cct(o->cct),
  osdriver(osd->store, coll_t(), OSD::make_snapmapper_oid()),
  snap_mapper(
    &osdriver,
    p.ps(),
    p.get_split_bits(curmap->get_pg_num(_pool.id)),
    _pool.id,
    p.shard),
  map_lock("PG::map_lock"),
  osdmap_ref(curmap), last_persisted_osdmap_ref(curmap), pool(_pool),
  _lock("PG::_lock"),
  ref(0),
  #ifdef PG_DEBUG_REFS
  _ref_id_lock("PG::_ref_id_lock"), _ref_id(0),
  #endif
  deleting(false), dirty_info(false), dirty_big_info(false),
  info(p),
  info_struct_v(0),
  coll(p), pg_log(cct), log_oid(loid), biginfo_oid(ioid),
  missing_loc(this),
  recovery_item(this), scrub_item(this), scrub_finalize_item(this), snap_trim_item(this), stat_queue_item(this),
  recovery_ops_active(0),
  role(0),
  state(0),
  send_notify(false),
  pg_whoami(osd->whoami, p.shard),
  need_up_thru(false),
  last_peering_reset(0),
  heartbeat_peer_lock("PG::heartbeat_peer_lock"),
  backfill_reserved(0),
  backfill_reserving(0),
  flushes_in_progress(0),
  pg_stats_publish_lock("PG::pg_stats_publish_lock"),
  pg_stats_publish_valid(false),
  osr(osd->osr_registry.lookup_or_create(p, (stringify(p)))),
  finish_sync_event(NULL),
  scrub_after_recovery(false),
  active_pushes(0),
  recovery_state(this)
{
#ifdef PG_DEBUG_REFS
  osd->add_pgid(p, this);
#endif
}

PG::~PG()
{
#ifdef PG_DEBUG_REFS
  osd->remove_pgid(info.pgid, this);
#endif
}

void PG::lock_suspend_timeout(ThreadPool::TPHandle &handle)
{
  handle.suspend_tp_timeout();
  lock();
  handle.reset_tp_timeout();
}

void PG::lock(bool no_lockdep)
{
  _lock.Lock(no_lockdep);
  // if we have unrecorded dirty state with the lock dropped, there is a bug
  assert(!dirty_info);
  assert(!dirty_big_info);

  dout(30) << "lock" << dendl;
}

std::string PG::gen_prefix() const
{
  stringstream out;
  OSDMapRef mapref = osdmap_ref;
  if (_lock.is_locked_by_me()) {
    out << "osd." << osd->whoami
	<< " pg_epoch: " << (mapref ? mapref->get_epoch():0)
	<< " " << *this << " ";
  } else {
    out << "osd." << osd->whoami
	<< " pg_epoch: " << (mapref ? mapref->get_epoch():0)
	<< " pg[" << info.pgid << "(unlocked)] ";
  }
  return out.str();
}
  
/********* PG **********/

void PG::proc_master_log(
  ObjectStore::Transaction& t, pg_info_t &oinfo,
  pg_log_t &olog, pg_missing_t& omissing, pg_shard_t from)
{
  dout(10) << "proc_master_log for osd." << from << ": "
	   << olog << " " << omissing << dendl;
  assert(!is_active() && is_primary());

  // merge log into our own log to build master log.  no need to
  // make any adjustments to their missing map; we are taking their
  // log to be authoritative (i.e., their entries are by definitely
  // non-divergent).
  merge_log(t, oinfo, olog, from);
  peer_info[from] = oinfo;
  dout(10) << " peer osd." << from << " now " << oinfo << " " << omissing << dendl;
  might_have_unfound.insert(from);

  peer_missing[from].swap(omissing);
}
    
void PG::proc_replica_log(
  ObjectStore::Transaction& t,
  pg_info_t &oinfo, pg_log_t &olog, pg_missing_t& omissing,
  pg_shard_t from)
{
  dout(10) << "proc_replica_log for osd." << from << ": "
	   << oinfo << " " << olog << " " << omissing << dendl;

  pg_log.proc_replica_log(t, oinfo, olog, omissing, from);

  peer_info[from] = oinfo;
  dout(10) << " peer osd." << from << " now " << oinfo << " " << omissing << dendl;
  might_have_unfound.insert(from);

  for (map<hobject_t, pg_missing_t::item>::iterator i = omissing.missing.begin();
       i != omissing.missing.end();
       ++i) {
    dout(20) << " after missing " << i->first << " need " << i->second.need
	     << " have " << i->second.have << dendl;
  }
  peer_missing[from].swap(omissing);
}

bool PG::proc_replica_info(pg_shard_t from, const pg_info_t &oinfo)
{
  map<pg_shard_t, pg_info_t>::iterator p = peer_info.find(from);
  if (p != peer_info.end() && p->second.last_update == oinfo.last_update) {
    dout(10) << " got dup osd." << from << " info " << oinfo << ", identical to ours" << dendl;
    return false;
  }

  dout(10) << " got osd." << from << " " << oinfo << dendl;
  assert(is_primary());
  peer_info[from] = oinfo;
  might_have_unfound.insert(from);
  
  unreg_next_scrub();
  if (info.history.merge(oinfo.history))
    dirty_info = true;
  reg_next_scrub();
  
  // stray?
  if (!is_up(from) && !is_acting(from)) {
    dout(10) << " osd." << from << " has stray content: " << oinfo << dendl;
    stray_set.insert(from);
    if (is_clean()) {
      purge_strays();
    }
  }

  // was this a new info?  if so, update peers!
  if (p == peer_info.end())
    update_heartbeat_peers();

  return true;
}

void PG::remove_snap_mapped_object(
  ObjectStore::Transaction &t, const hobject_t &soid)
{
  t.remove(
    coll,
    ghobject_t(soid, ghobject_t::NO_GEN, pg_whoami.shard));
  clear_object_snap_mapping(&t, soid);
}

void PG::clear_object_snap_mapping(
  ObjectStore::Transaction *t, const hobject_t &soid)
{
  OSDriver::OSTransaction _t(osdriver.get_transaction(t));
  if (soid.snap < CEPH_MAXSNAP) {
    int r = snap_mapper.remove_oid(
      soid,
      &_t);
    if (!(r == 0 || r == -ENOENT)) {
      derr << __func__ << ": remove_oid returned " << cpp_strerror(r) << dendl;
      assert(0);
    }
  }
}

void PG::update_object_snap_mapping(
  ObjectStore::Transaction *t, const hobject_t &soid, const set<snapid_t> &snaps)
{
  OSDriver::OSTransaction _t(osdriver.get_transaction(t));
  assert(soid.snap < CEPH_MAXSNAP);
  int r = snap_mapper.remove_oid(
    soid,
    &_t);
  if (!(r == 0 || r == -ENOENT)) {
    derr << __func__ << ": remove_oid returned " << cpp_strerror(r) << dendl;
    assert(0);
  }
  snap_mapper.add_oid(
    soid,
    snaps,
    &_t);
}

void PG::merge_log(
  ObjectStore::Transaction& t, pg_info_t &oinfo, pg_log_t &olog, pg_shard_t from)
{
  PGLogEntryHandler rollbacker;
  pg_log.merge_log(
    t, oinfo, olog, from, info, &rollbacker, dirty_info, dirty_big_info);
  rollbacker.apply(this, &t);
}

void PG::rewind_divergent_log(ObjectStore::Transaction& t, eversion_t newhead)
{
  PGLogEntryHandler rollbacker;
  pg_log.rewind_divergent_log(
    t, newhead, info, &rollbacker, dirty_info, dirty_big_info);
  rollbacker.apply(this, &t);
}

/*
 * Process information from a replica to determine if it could have any
 * objects that i need.
 *
 * TODO: if the missing set becomes very large, this could get expensive.
 * Instead, we probably want to just iterate over our unfound set.
 */
bool PG::search_for_missing(
  const pg_info_t &oinfo, const pg_missing_t &omissing,
  pg_shard_t from,
  RecoveryCtx *ctx)
{
  unsigned num_unfound_before = missing_loc.num_unfound();
  bool found_missing = missing_loc.add_source_info(
    from, oinfo, omissing);
  if (found_missing && num_unfound_before != missing_loc.num_unfound())
    publish_stats_to_osd();
  if (found_missing &&
    (get_osdmap()->get_features(NULL) & CEPH_FEATURE_OSD_ERASURE_CODES)) {
    pg_info_t tinfo(oinfo);
    tinfo.pgid.shard = pg_whoami.shard;
    (*(ctx->info_map))[from.osd].push_back(
      make_pair(
	pg_notify_t(
	  from.shard, pg_whoami.shard,
	  get_osdmap()->get_epoch(),
	  get_osdmap()->get_epoch(),
	  tinfo),
	past_intervals));
  }
  return found_missing;
}

bool PG::MissingLoc::readable_with_acting(
  const hobject_t &hoid,
  const set<pg_shard_t> &acting) const {
  if (!needs_recovery(hoid)) return true;
  if (!missing_loc.count(hoid)) return false;
  const set<pg_shard_t> &locs = missing_loc.find(hoid)->second;
  dout(10) << __func__ << ": locs:" << locs << dendl;
  set<pg_shard_t> have_acting;
  for (set<pg_shard_t>::const_iterator i = locs.begin();
       i != locs.end();
       ++i) {
    if (acting.count(*i))
      have_acting.insert(*i);
  }
  return (*is_readable)(have_acting);
}

bool PG::MissingLoc::add_source_info(
  pg_shard_t fromosd,
  const pg_info_t &oinfo,
  const pg_missing_t &omissing)
{
  bool found_missing = false;;
  // found items?
  for (map<hobject_t,pg_missing_t::item>::const_iterator p = needs_recovery_map.begin();
       p != needs_recovery_map.end();
       ++p) {
    const hobject_t &soid(p->first);
    eversion_t need = p->second.need;
    if (oinfo.last_update < need) {
      dout(10) << "search_for_missing " << soid << " " << need
	       << " also missing on osd." << fromosd
	       << " (last_update " << oinfo.last_update << " < needed " << need << ")"
	       << dendl;
      continue;
    }
    if (p->first >= oinfo.last_backfill) {
      // FIXME: this is _probably_ true, although it could conceivably
      // be in the undefined region!  Hmm!
      dout(10) << "search_for_missing " << soid << " " << need
	       << " also missing on osd." << fromosd
	       << " (past last_backfill " << oinfo.last_backfill << ")"
	       << dendl;
      continue;
    }
    if (oinfo.last_complete < need) {
      if (omissing.is_missing(soid)) {
	dout(10) << "search_for_missing " << soid << " " << need
		 << " also missing on osd." << fromosd << dendl;
	continue;
      }
    }

    dout(10) << "search_for_missing " << soid << " " << need
	     << " is on osd." << fromosd << dendl;

    missing_loc[soid].insert(fromosd);
    missing_loc_sources.insert(fromosd);
    found_missing = true;
  }

  dout(20) << "needs_recovery_map missing " << needs_recovery_map << dendl;
  return found_missing;
}

void PG::discover_all_missing(map<int, map<spg_t,pg_query_t> > &query_map)
{
  const pg_missing_t &missing = pg_log.get_missing();
  assert(have_unfound());

  dout(10) << __func__ << " "
	   << missing.num_missing() << " missing, "
	   << get_num_unfound() << " unfound"
	   << dendl;

  std::set<pg_shard_t>::const_iterator m = might_have_unfound.begin();
  std::set<pg_shard_t>::const_iterator mend = might_have_unfound.end();
  for (; m != mend; ++m) {
    pg_shard_t peer(*m);
    
    if (!get_osdmap()->is_up(peer.osd)) {
      dout(20) << __func__ << " skipping down osd." << peer << dendl;
      continue;
    }

    map<pg_shard_t, pg_info_t>::const_iterator iter = peer_info.find(peer);
    if (iter != peer_info.end() &&
        (iter->second.is_empty() || iter->second.dne())) {
      // ignore empty peers
      continue;
    }

    // If we've requested any of this stuff, the pg_missing_t information
    // should be on its way.
    // TODO: coalsce requested_* into a single data structure
    if (peer_missing.find(peer) != peer_missing.end()) {
      dout(20) << __func__ << ": osd." << peer
	       << ": we already have pg_missing_t" << dendl;
      continue;
    }
    if (peer_log_requested.find(peer) != peer_log_requested.end()) {
      dout(20) << __func__ << ": osd." << peer
	       << ": in peer_log_requested" << dendl;
      continue;
    }
    if (peer_missing_requested.find(peer) != peer_missing_requested.end()) {
      dout(20) << __func__ << ": osd." << peer
	       << ": in peer_missing_requested" << dendl;
      continue;
    }

    // Request missing
    dout(10) << __func__ << ": osd." << peer << ": requesting pg_missing_t"
	     << dendl;
    peer_missing_requested.insert(peer);
    query_map[peer.osd][spg_t(info.pgid.pgid, peer.shard)] =
      pg_query_t(
	pg_query_t::FULLLOG,
	peer.shard, pg_whoami.shard,
	info.history, get_osdmap()->get_epoch());
  }
}

/******* PG ***********/
bool PG::needs_recovery() const
{
  assert(is_primary());

  bool ret = false;

  const pg_missing_t &missing = pg_log.get_missing();

  if (missing.num_missing()) {
    dout(10) << __func__ << " primary has " << missing.num_missing()
      << " missing" << dendl;

    ret = true;
  }

  assert(!actingbackfill.empty());
  set<pg_shard_t>::const_iterator end = actingbackfill.end();
  set<pg_shard_t>::const_iterator a = actingbackfill.begin();
  assert(a != end);
  for (; a != end; ++a) {
    if (*a == get_primary()) continue;
    pg_shard_t peer = *a;
    map<pg_shard_t, pg_missing_t>::const_iterator pm = peer_missing.find(peer);
    if (pm == peer_missing.end()) {
      dout(10) << __func__ << " osd." << peer << " doesn't have missing set"
        << dendl;
      ret = true;
      continue;
    }
    if (pm->second.num_missing()) {
      dout(10) << __func__ << " osd." << peer << " has "
        << pm->second.num_missing() << " missing" << dendl;
      ret = true;
    }
  }

  if (!ret)
    dout(10) << __func__ << " is recovered" << dendl;
  return ret;
}

bool PG::needs_backfill() const
{
  assert(is_primary());

  bool ret = false;

  // We can assume that only possible osds that need backfill
  // are on the backfill_targets vector nodes.
  set<pg_shard_t>::const_iterator end = backfill_targets.end();
  set<pg_shard_t>::const_iterator a = backfill_targets.begin();
  for (; a != end; ++a) {
    pg_shard_t peer = *a;
    map<pg_shard_t, pg_info_t>::const_iterator pi = peer_info.find(peer);
    if (!pi->second.last_backfill.is_max()) {
      dout(10) << __func__ << " osd." << peer << " has last_backfill " << pi->second.last_backfill << dendl;
      ret = true;
    }
  }

  if (!ret)
    dout(10) << __func__ << " does not need backfill" << dendl;
  return ret;
}

bool PG::_calc_past_interval_range(epoch_t *start, epoch_t *end)
{
  *end = info.history.same_interval_since;

  // Do we already have the intervals we want?
  map<epoch_t,pg_interval_t>::const_iterator pif = past_intervals.begin();
  if (pif != past_intervals.end()) {
    if (pif->first <= info.history.last_epoch_clean) {
      dout(10) << __func__ << ": already have past intervals back to "
	       << info.history.last_epoch_clean << dendl;
      return false;
    }
    *end = past_intervals.begin()->first;
  }

  *start = MAX(MAX(info.history.epoch_created,
		   info.history.last_epoch_clean),
	       osd->get_superblock().oldest_map);
  if (*start >= *end) {
    dout(10) << __func__ << " start epoch " << *start << " >= end epoch " << *end
	     << ", nothing to do" << dendl;
    return false;
  }

  return true;
}


void PG::generate_past_intervals()
{
  epoch_t cur_epoch, end_epoch;
  if (!_calc_past_interval_range(&cur_epoch, &end_epoch)) {
    return;
  }

  OSDMapRef last_map, cur_map;
  int primary = -1;
  vector<int> acting, up, old_acting, old_up;

  cur_map = osd->get_map(cur_epoch);
  cur_map->pg_to_up_acting_osds(
    get_pgid().pgid, &up, 0, &acting, &primary);
  epoch_t same_interval_since = cur_epoch;
  dout(10) << __func__ << " over epochs " << cur_epoch << "-"
	   << end_epoch << dendl;
  ++cur_epoch;
  for (; cur_epoch <= end_epoch; ++cur_epoch) {
    int old_primary = primary;
    last_map.swap(cur_map);
    old_up.swap(up);
    old_acting.swap(acting);

    cur_map = osd->get_map(cur_epoch);
    cur_map->pg_to_up_acting_osds(
      get_pgid().pgid, &up, 0, &acting, &primary);

    std::stringstream debug;
    bool new_interval = pg_interval_t::check_new_interval(
      old_primary,
      primary,
      old_acting,
      acting,
      old_up,
      up,
      same_interval_since,
      info.history.last_epoch_clean,
      cur_map,
      last_map,
      info.pgid.pool(),
      info.pgid.pgid,
      &past_intervals,
      &debug);
    if (new_interval) {
      dout(10) << debug.str() << dendl;
      same_interval_since = cur_epoch;
    }
  }

  // record our work.
  dirty_info = true;
  dirty_big_info = true;
}

/*
 * Trim past_intervals.
 *
 * This gets rid of all the past_intervals that happened before last_epoch_clean.
 */
void PG::trim_past_intervals()
{
  std::map<epoch_t,pg_interval_t>::iterator pif = past_intervals.begin();
  std::map<epoch_t,pg_interval_t>::iterator end = past_intervals.end();
  while (pif != end) {
    if (pif->second.last >= info.history.last_epoch_clean)
      return;
    dout(10) << __func__ << ": trimming " << pif->second << dendl;
    past_intervals.erase(pif++);
    dirty_big_info = true;
  }
}


bool PG::adjust_need_up_thru(const OSDMapRef osdmap)
{
  epoch_t up_thru = get_osdmap()->get_up_thru(osd->whoami);
  if (need_up_thru &&
      up_thru >= info.history.same_interval_since) {
    dout(10) << "adjust_need_up_thru now " << up_thru << ", need_up_thru now false" << dendl;
    need_up_thru = false;
    return true;
  }
  return false;
}

void PG::remove_down_peer_info(const OSDMapRef osdmap)
{
  // Remove any downed osds from peer_info
  bool removed = false;
  map<pg_shard_t, pg_info_t>::iterator p = peer_info.begin();
  while (p != peer_info.end()) {
    if (!osdmap->is_up(p->first.osd)) {
      dout(10) << " dropping down osd." << p->first << " info " << p->second << dendl;
      peer_missing.erase(p->first);
      peer_log_requested.erase(p->first);
      peer_missing_requested.erase(p->first);
      peer_info.erase(p++);
      removed = true;
    } else
      ++p;
  }

  // if we removed anyone, update peers (which include peer_info)
  if (removed)
    update_heartbeat_peers();
  check_recovery_sources(osdmap);
}

/*
 * Returns true unless there is a non-lost OSD in might_have_unfound.
 */
bool PG::all_unfound_are_queried_or_lost(const OSDMapRef osdmap) const
{
  assert(is_primary());

  set<pg_shard_t>::const_iterator peer = might_have_unfound.begin();
  set<pg_shard_t>::const_iterator mend = might_have_unfound.end();
  for (; peer != mend; ++peer) {
    if (peer_missing.count(*peer))
      continue;
    map<pg_shard_t, pg_info_t>::const_iterator iter = peer_info.find(*peer);
    if (iter != peer_info.end() &&
        (iter->second.is_empty() || iter->second.dne()))
      continue;
    const osd_info_t &osd_info(osdmap->get_info(peer->osd));
    if (osd_info.lost_at <= osd_info.up_from) {
      // If there is even one OSD in might_have_unfound that isn't lost, we
      // still might retrieve our unfound.
      return false;
    }
  }
  dout(10) << "all_unfound_are_queried_or_lost all of might_have_unfound " << might_have_unfound 
	   << " have been queried or are marked lost" << dendl;
  return true;
}

void PG::build_prior(std::auto_ptr<PriorSet> &prior_set)
{
  if (1) {
    // sanity check
    for (map<pg_shard_t,pg_info_t>::iterator it = peer_info.begin();
	 it != peer_info.end();
	 ++it) {
      assert(info.history.last_epoch_started >= it->second.history.last_epoch_started);
    }
  }
  prior_set.reset(
    new PriorSet(
      pool.info.ec_pool(),
      get_pgbackend()->get_is_recoverable_predicate(),
      *get_osdmap(),
      past_intervals,
      up,
      acting,
      info,
      this));
  PriorSet &prior(*prior_set.get());
				 
  if (prior.pg_down) {
    state_set(PG_STATE_DOWN);
  }

  if (get_osdmap()->get_up_thru(osd->whoami) < info.history.same_interval_since) {
    dout(10) << "up_thru " << get_osdmap()->get_up_thru(osd->whoami)
	     << " < same_since " << info.history.same_interval_since
	     << ", must notify monitor" << dendl;
    need_up_thru = true;
  } else {
    dout(10) << "up_thru " << get_osdmap()->get_up_thru(osd->whoami)
	     << " >= same_since " << info.history.same_interval_since
	     << ", all is well" << dendl;
    need_up_thru = false;
  }
  set_probe_targets(prior_set->probe);
}

void PG::clear_primary_state()
{
  dout(10) << "clear_primary_state" << dendl;

  // clear peering state
  stray_set.clear();
  peer_log_requested.clear();
  peer_missing_requested.clear();
  peer_info.clear();
  peer_missing.clear();
  need_up_thru = false;
  peer_last_complete_ondisk.clear();
  peer_activated.clear();
  min_last_complete_ondisk = eversion_t();
  pg_trim_to = eversion_t();
  stray_purged.clear();
  might_have_unfound.clear();

  last_update_ondisk = eversion_t();

  snap_trimq.clear();

  finish_sync_event = 0;  // so that _finish_recvoery doesn't go off in another thread

  missing_loc.clear();

  pg_log.reset_recovery_pointers();

  scrubber.reserved_peers.clear();
  scrub_after_recovery = false;

  osd->recovery_wq.dequeue(this);
  osd->snap_trim_wq.dequeue(this);

  agent_clear();

  osd->remove_want_pg_temp(info.pgid.pgid);
}

/**
 * find_best_info
 *
 * Returns an iterator to the best info in infos sorted by:
 *  1) Prefer newer last_update
 *  2) Prefer longer tail if it brings another info into contiguity
 *  3) Prefer current primary
 */
map<pg_shard_t, pg_info_t>::const_iterator PG::find_best_info(
  const map<pg_shard_t, pg_info_t> &infos) const
{
  eversion_t min_last_update_acceptable = eversion_t::max();
  epoch_t max_last_epoch_started_found = 0;
  for (map<pg_shard_t, pg_info_t>::const_iterator i = infos.begin();
       i != infos.end();
       ++i) {
    if (max_last_epoch_started_found < i->second.last_epoch_started) {
      min_last_update_acceptable = eversion_t::max();
      max_last_epoch_started_found = i->second.last_epoch_started;
    }
    if (max_last_epoch_started_found == i->second.last_epoch_started) {
      if (min_last_update_acceptable > i->second.last_update)
	min_last_update_acceptable = i->second.last_update;
    }
  }
  assert(min_last_update_acceptable != eversion_t::max());

  map<pg_shard_t, pg_info_t>::const_iterator best = infos.end();
  // find osd with newest last_update (oldest for ec_pool).
  // if there are multiples, prefer
  //  - a longer tail, if it brings another peer into log contiguity
  //  - the current primary
  for (map<pg_shard_t, pg_info_t>::const_iterator p = infos.begin();
       p != infos.end();
       ++p) {
    // Only consider peers with last_update >= min_last_update_acceptable
    if (p->second.last_update < min_last_update_acceptable)
      continue;
    // Disquality anyone who is incomplete (not fully backfilled)
    if (p->second.is_incomplete())
      continue;
    if (best == infos.end()) {
      best = p;
      continue;
    }
    // Prefer newer last_update
    if (pool.info.require_rollback()) {
      if (p->second.last_update > best->second.last_update)
	continue;
      if (p->second.last_update < best->second.last_update) {
	best = p;
	continue;
      }
    } else {
      if (p->second.last_update < best->second.last_update)
	continue;
      if (p->second.last_update > best->second.last_update) {
	best = p;
	continue;
      }
    }

    // Prefer longer tail
    if (p->second.log_tail > best->second.log_tail) {
      continue;
    } else if (p->second.log_tail < best->second.log_tail) {
      best = p;
      continue;
    }

    // prefer current primary (usually the caller), all things being equal
    if (p->first == pg_whoami) {
      dout(10) << "calc_acting prefer osd." << p->first
	       << " because it is current primary" << dendl;
      best = p;
      continue;
    }
  }
  return best;
}

void PG::calc_ec_acting(
  map<pg_shard_t, pg_info_t>::const_iterator auth_log_shard,
  unsigned size,
  const vector<int> &acting,
  pg_shard_t acting_primary,
  const vector<int> &up,
  pg_shard_t up_primary,
  const map<pg_shard_t, pg_info_t> &all_info,
  bool compat_mode,
  vector<int> *_want,
  set<pg_shard_t> *backfill,
  set<pg_shard_t> *acting_backfill,
  pg_shard_t *want_primary,
  ostream &ss) {
  vector<int> want(size, CRUSH_ITEM_NONE);
  map<shard_id_t, set<pg_shard_t> > all_info_by_shard;
  unsigned usable = 0;
  for(map<pg_shard_t, pg_info_t>::const_iterator i = all_info.begin();
      i != all_info.end();
      ++i) {
    all_info_by_shard[i->first.shard].insert(i->first);
  }
  for (shard_id_t i = 0; i < want.size(); ++i) {
    ss << "For position " << (unsigned)i << ": ";
    if (up.size() > (unsigned)i && up[i] != CRUSH_ITEM_NONE &&
	!all_info.find(pg_shard_t(up[i], i))->second.is_incomplete() &&
	all_info.find(pg_shard_t(up[i], i))->second.last_update >=
	auth_log_shard->second.log_tail) {
      ss << " selecting up[i]: " << pg_shard_t(up[i], i) << std::endl;
      want[i] = up[i];
      ++usable;
      continue;
    }
    if (up.size() > (unsigned)i && up[i] != CRUSH_ITEM_NONE) {
      ss << " backfilling up[i]: " << pg_shard_t(up[i], i)
	 << " and ";
      backfill->insert(pg_shard_t(up[i], i));
    }

    if (acting.size() > (unsigned)i && acting[i] != CRUSH_ITEM_NONE &&
	!all_info.find(pg_shard_t(acting[i], i))->second.is_incomplete() &&
	all_info.find(pg_shard_t(acting[i], i))->second.last_update >=
	auth_log_shard->second.log_tail) {
      ss << " selecting acting[i]: " << pg_shard_t(acting[i], i) << std::endl;
      want[i] = acting[i];
      ++usable;
    } else {
      for (set<pg_shard_t>::iterator j = all_info_by_shard[i].begin();
	   j != all_info_by_shard[i].end();
	   ++j) {
	assert(j->shard == i);
	if (!all_info.find(*j)->second.is_incomplete() &&
	    all_info.find(*j)->second.last_update >=
	    auth_log_shard->second.log_tail) {
	  ss << " selecting stray: " << *j << std::endl;
	  want[i] = j->osd;
	  ++usable;
	  break;
	}
      }
      if (want[i] == CRUSH_ITEM_NONE)
	ss << " failed to fill position " << i << std::endl;
    }
  }

  bool found_primary = false;
  for (shard_id_t i = 0; i < want.size(); ++i) {
    if (want[i] != CRUSH_ITEM_NONE) {
      acting_backfill->insert(pg_shard_t(want[i], i));
      if (!found_primary) {
	*want_primary = pg_shard_t(want[i], i);
	found_primary = true;
      }
    }
  }
  acting_backfill->insert(backfill->begin(), backfill->end());
  _want->swap(want);
}

/**
 * calculate the desired acting set.
 *
 * Choose an appropriate acting set.  Prefer up[0], unless it is
 * incomplete, or another osd has a longer tail that allows us to
 * bring other up nodes up to date.
 */
void PG::calc_replicated_acting(
  map<pg_shard_t, pg_info_t>::const_iterator auth_log_shard,
  unsigned size,
  const vector<int> &acting,
  pg_shard_t acting_primary,
  const vector<int> &up,
  pg_shard_t up_primary,
  const map<pg_shard_t, pg_info_t> &all_info,
  bool compat_mode,
  vector<int> *want,
  set<pg_shard_t> *backfill,
  set<pg_shard_t> *acting_backfill,
  pg_shard_t *want_primary,
  ostream &ss)
{
  ss << "calc_acting newest update on osd." << auth_log_shard->first
     << " with " << auth_log_shard->second << std::endl;
  pg_shard_t auth_log_shard_id = auth_log_shard->first;
  
  // select primary
  map<pg_shard_t,pg_info_t>::const_iterator primary;
  if (up.size() &&
      !all_info.find(up_primary)->second.is_incomplete() &&
      all_info.find(up_primary)->second.last_update >=
        auth_log_shard->second.log_tail) {
    ss << "up_primary: " << up_primary << ") selected as primary" << std::endl;
    primary = all_info.find(up_primary); // prefer up[0], all thing being equal
  } else {
    assert(!auth_log_shard->second.is_incomplete());
    ss << "up[0] needs backfill, osd." << auth_log_shard_id
       << " selected as primary instead" << std::endl;
    primary = auth_log_shard;
  }

  ss << "calc_acting primary is osd." << primary->first
     << " with " << primary->second << std::endl;
  *want_primary = primary->first;
  want->push_back(primary->first.osd);
  acting_backfill->insert(primary->first);
  unsigned usable = 1;

  // select replicas that have log contiguity with primary.
  // prefer up, then acting, then any peer_info osds 
  for (vector<int>::const_iterator i = up.begin();
       i != up.end();
       ++i) {
    pg_shard_t up_cand = pg_shard_t(*i, ghobject_t::no_shard());
    if (up_cand == primary->first)
      continue;
    const pg_info_t &cur_info = all_info.find(up_cand)->second;
    if (cur_info.is_incomplete() ||
      cur_info.last_update < MIN(
	primary->second.log_tail,
	auth_log_shard->second.log_tail)) {
      /* We include auth_log_shard->second.log_tail because in GetLog,
       * we will request logs back to the min last_update over our
       * acting_backfill set, which will result in our log being extended
       * as far backwards as necessary to pick up any peers which can
       * be log recovered by auth_log_shard's log */
      ss << " shard " << up_cand << " (up) backfill " << cur_info << std::endl;
      if (compat_mode) {
	if (backfill->empty()) {
	  backfill->insert(up_cand);
	  want->push_back(*i);
	  acting_backfill->insert(up_cand);
	}
      } else {
	backfill->insert(up_cand);
	acting_backfill->insert(up_cand);
      }
    } else {
      want->push_back(*i);
      acting_backfill->insert(up_cand);
      usable++;
      ss << " osd." << *i << " (up) accepted " << cur_info << std::endl;
    }
  }

  // This no longer has backfill OSDs, but they are covered above.
  for (vector<int>::const_iterator i = acting.begin();
       i != acting.end();
       ++i) {
    pg_shard_t acting_cand(*i, ghobject_t::no_shard());
    if (usable >= size)
      break;

    // skip up osds we already considered above
    if (acting_cand == primary->first)
      continue;
    vector<int>::const_iterator up_it = find(up.begin(), up.end(), acting_cand.osd);
    if (up_it != up.end())
      continue;

    const pg_info_t &cur_info = all_info.find(acting_cand)->second;
    if (cur_info.is_incomplete() ||
	cur_info.last_update < primary->second.log_tail) {
      ss << " shard " << acting_cand << " (stray) REJECTED "
	       << cur_info << std::endl;
    } else {
      want->push_back(*i);
      acting_backfill->insert(acting_cand);
      ss << " shard " << acting_cand << " (stray) accepted "
	 << cur_info << std::endl;
      usable++;
    }
  }

  for (map<pg_shard_t,pg_info_t>::const_iterator i = all_info.begin();
       i != all_info.end();
       ++i) {
    if (usable >= size)
      break;

    // skip up osds we already considered above
    if (i->first == primary->first)
      continue;
    vector<int>::const_iterator up_it = find(up.begin(), up.end(), i->first.osd);
    if (up_it != up.end())
      continue;
    vector<int>::const_iterator acting_it = find(
      acting.begin(), acting.end(), i->first.osd);
    if (acting_it != acting.end())
      continue;

    if (i->second.is_incomplete() ||
	i->second.last_update < primary->second.log_tail) {
      ss << " shard " << i->first << " (stray) REJECTED "
	 << i->second << std::endl;
    } else {
      want->push_back(i->first.osd);
      acting_backfill->insert(i->first);
      ss << " shard " << i->first << " (stray) accepted "
	 << i->second << std::endl;
      usable++;
    }
  }
}

/**
 * choose acting
 *
 * calculate the desired acting, and request a change with the monitor
 * if it differs from the current acting.
 */
bool PG::choose_acting(pg_shard_t &auth_log_shard_id)
{
  map<pg_shard_t, pg_info_t> all_info(peer_info.begin(), peer_info.end());
  all_info[pg_whoami] = info;

  for (map<pg_shard_t, pg_info_t>::iterator p = all_info.begin();
       p != all_info.end();
       ++p) {
    dout(10) << "calc_acting osd." << p->first << " " << p->second << dendl;
  }

  map<pg_shard_t, pg_info_t>::const_iterator auth_log_shard =
    find_best_info(all_info);

  if (auth_log_shard == all_info.end()) {
    if (up != acting) {
      dout(10) << "choose_acting no suitable info found (incomplete backfills?),"
	       << " reverting to up" << dendl;
      want_acting = up;
      vector<int> empty;
      osd->queue_want_pg_temp(info.pgid.pgid, empty);
    } else {
      dout(10) << "choose_acting failed" << dendl;
      assert(want_acting.empty());
    }
    return false;
  }

  if ((up.size() &&
      !all_info.find(up_primary)->second.is_incomplete() &&
      all_info.find(up_primary)->second.last_update >=
       auth_log_shard->second.log_tail) &&
      auth_log_shard->second.is_incomplete()) {
    map<pg_shard_t, pg_info_t> complete_infos;
    for (map<pg_shard_t, pg_info_t>::const_iterator i = all_info.begin();
	 i != all_info.end();
	 ++i) {
      if (!i->second.is_incomplete())
	complete_infos.insert(*i);
    }
    map<pg_shard_t, pg_info_t>::const_iterator i = find_best_info(
      complete_infos);
    if (i != complete_infos.end()) {
      auth_log_shard = all_info.find(i->first);
    }
  }

  auth_log_shard_id = auth_log_shard->first;

  // Determine if compatibility needed
  bool compat_mode = !cct->_conf->osd_debug_override_acting_compat;
  if (compat_mode) {
    bool all_support = true;
    OSDMapRef osdmap = get_osdmap();

    for (map<pg_shard_t, pg_info_t>::iterator it = all_info.begin();
	 it != all_info.end();
	 ++it) {
      pg_shard_t peer = it->first;

      const osd_xinfo_t& xi = osdmap->get_xinfo(peer.osd);
      if (!(xi.features & CEPH_FEATURE_OSD_ERASURE_CODES)) {
	all_support = false;
	break;
      }
    }
    if (all_support)
      compat_mode = false;
  }

  set<pg_shard_t> want_backfill, want_acting_backfill;
  vector<int> want;
  pg_shard_t want_primary;
  stringstream ss;
  if (!pool.info.ec_pool())
    calc_replicated_acting(
      auth_log_shard,
      get_osdmap()->get_pg_size(info.pgid.pgid),
      acting,
      primary,
      up,
      up_primary,
      all_info,
      compat_mode,
      &want,
      &want_backfill,
      &want_acting_backfill,
      &want_primary,
      ss);
  else
    calc_ec_acting(
      auth_log_shard,
      get_osdmap()->get_pg_size(info.pgid.pgid),
      acting,
      primary,
      up,
      up_primary,
      all_info,
      compat_mode,
      &want,
      &want_backfill,
      &want_acting_backfill,
      &want_primary,
      ss);
  dout(10) << ss.str() << dendl;

  // This might cause a problem if min_size is large
  // and we need to backfill more than 1 osd.  Older
  // code would only include 1 backfill osd and now we
  // have the resize above.
  if (want_acting_backfill.size() < pool.info.min_size) {
    want_acting.clear();
    return false;
  }

  /* Check whether we have enough acting shards to later perform recovery */
  boost::scoped_ptr<PGBackend::IsRecoverablePredicate> recoverable_predicate(
    get_pgbackend()->get_is_recoverable_predicate());
  set<pg_shard_t> have;
  for (int i = 0; i < (int)want.size(); ++i) {
    if (want[i] != CRUSH_ITEM_NONE)
      have.insert(
	pg_shard_t(
	  want[i],
	  pool.info.ec_pool() ? i : ghobject_t::NO_SHARD));
  }
  if (!(*recoverable_predicate)(have)) {
    want_acting.clear();
    return false;
  }

  if (want != acting) {
    dout(10) << "choose_acting want " << want << " != acting " << acting
	     << ", requesting pg_temp change" << dendl;
    want_acting = want;

    if (want_acting == up) {
      // There can't be any pending backfill if
      // want is the same as crush map up OSDs.
      assert(compat_mode || want_backfill.empty());
      vector<int> empty;
      osd->queue_want_pg_temp(info.pgid.pgid, empty);
    } else
      osd->queue_want_pg_temp(info.pgid.pgid, want);
    return false;
  }
  want_acting.clear();
  actingbackfill = want_acting_backfill;
  dout(10) << "actingbackfill is " << actingbackfill << dendl;
  assert(backfill_targets.empty() || backfill_targets == want_backfill);
  if (backfill_targets.empty()) {
    // Caller is GetInfo
    backfill_targets = want_backfill;
    for (set<pg_shard_t>::iterator i = backfill_targets.begin();
	 i != backfill_targets.end();
	 ++i) {
      assert(!stray_set.count(*i));
    }
  } else {
    // Will not change if already set because up would have had to change
    assert(backfill_targets == want_backfill);
    // Verify that nothing in backfill is in stray_set
    for (set<pg_shard_t>::iterator i = want_backfill.begin();
	 i != want_backfill.end();
	 ++i) {
      assert(stray_set.find(*i) == stray_set.end());
    }
  }
  dout(10) << "choose_acting want " << want << " (== acting) backfill_targets " 
	   << want_backfill << dendl;
  return true;
}

/* Build the might_have_unfound set.
 *
 * This is used by the primary OSD during recovery.
 *
 * This set tracks the OSDs which might have unfound objects that the primary
 * OSD needs. As we receive pg_missing_t from each OSD in might_have_unfound, we
 * will remove the OSD from the set.
 */
void PG::build_might_have_unfound()
{
  assert(might_have_unfound.empty());
  assert(is_primary());

  dout(10) << __func__ << dendl;

  // Make sure that we have past intervals.
  generate_past_intervals();

  // We need to decide who might have unfound objects that we need
  std::map<epoch_t,pg_interval_t>::const_reverse_iterator p = past_intervals.rbegin();
  std::map<epoch_t,pg_interval_t>::const_reverse_iterator end = past_intervals.rend();
  for (; p != end; ++p) {
    const pg_interval_t &interval(p->second);
    // We already have all the objects that exist at last_epoch_clean,
    // so there's no need to look at earlier intervals.
    if (interval.last < info.history.last_epoch_clean)
      break;

    // If nothing changed, we don't care about this interval.
    if (!interval.maybe_went_rw)
      continue;

    int i = 0;
    std::vector<int>::const_iterator a = interval.acting.begin();
    std::vector<int>::const_iterator a_end = interval.acting.end();
    for (; a != a_end; ++a, ++i) {
      pg_shard_t shard(*a, pool.info.ec_pool() ? i : ghobject_t::NO_SHARD);
      if (*a != CRUSH_ITEM_NONE && shard != pg_whoami)
	might_have_unfound.insert(shard);
    }
  }

  // include any (stray) peers
  for (map<pg_shard_t, pg_info_t>::iterator p = peer_info.begin();
       p != peer_info.end();
       ++p)
    might_have_unfound.insert(p->first);

  dout(15) << __func__ << ": built " << might_have_unfound << dendl;
}

struct C_PG_ActivateCommitted : public Context {
  PGRef pg;
  epoch_t epoch;
  C_PG_ActivateCommitted(PG *p, epoch_t e)
    : pg(p), epoch(e) {}
  void finish(int r) {
    pg->_activate_committed(epoch);
  }
};

void PG::activate(ObjectStore::Transaction& t,
		  epoch_t query_epoch,
		  list<Context*>& tfin,
		  map<int, map<spg_t,pg_query_t> >& query_map,
		  map<int,
		      vector<
			pair<pg_notify_t,
			     pg_interval_map_t> > > *activator_map,
                  RecoveryCtx *ctx)
{
  assert(!is_active());
  assert(scrubber.callbacks.empty());
  assert(callbacks_for_degraded_object.empty());

  // -- crash recovery?
  if (is_primary() &&
      pool.info.crash_replay_interval > 0 &&
      may_need_replay(get_osdmap())) {
    replay_until = ceph_clock_now(cct);
    replay_until += pool.info.crash_replay_interval;
    dout(10) << "activate starting replay interval for " << pool.info.crash_replay_interval
	     << " until " << replay_until << dendl;
    state_set(PG_STATE_REPLAY);

    // TODOSAM: osd->osd-> is no good
    osd->osd->replay_queue_lock.Lock();
    osd->osd->replay_queue.push_back(pair<spg_t,utime_t>(
	info.pgid, replay_until));
    osd->osd->replay_queue_lock.Unlock();
  }

  // twiddle pg state
  state_clear(PG_STATE_DOWN);

  send_notify = false;

  if (is_acting(pg_whoami))
    info.last_epoch_started = query_epoch;

  const pg_missing_t &missing = pg_log.get_missing();

  if (is_primary()) {
    last_update_ondisk = info.last_update;
    min_last_complete_ondisk = eversion_t(0,0);  // we don't know (yet)!
  }
  last_update_applied = info.last_update;


  need_up_thru = false;

  // write pg info, log
  dirty_info = true;
  dirty_big_info = true; // maybe

  // find out when we commit
  t.register_on_complete(new C_PG_ActivateCommitted(this, query_epoch));
  
  // initialize snap_trimq
  if (is_primary()) {
    dout(20) << "activate - purged_snaps " << info.purged_snaps
	     << " cached_removed_snaps " << pool.cached_removed_snaps << dendl;
    snap_trimq = pool.cached_removed_snaps;
    snap_trimq.subtract(info.purged_snaps);
    dout(10) << "activate - snap_trimq " << snap_trimq << dendl;
    if (!snap_trimq.empty() && is_clean())
      queue_snap_trim();
  }

  // init complete pointer
  if (missing.num_missing() == 0) {
    dout(10) << "activate - no missing, moving last_complete " << info.last_complete 
	     << " -> " << info.last_update << dendl;
    info.last_complete = info.last_update;
    pg_log.reset_recovery_pointers();
  } else {
    dout(10) << "activate - not complete, " << missing << dendl;
    pg_log.activate_not_complete(info);
    if (is_primary()) {
      dout(10) << "activate - starting recovery" << dendl;
      osd->queue_for_recovery(this);
      if (have_unfound())
	discover_all_missing(query_map);
    }
  }
    
  log_weirdness();

  // if primary..
  if (is_primary()) {
    assert(ctx);
    // start up replicas

    assert(!actingbackfill.empty());
    for (set<pg_shard_t>::iterator i = actingbackfill.begin();
	 i != actingbackfill.end();
	 ++i) {
      if (*i == pg_whoami) continue;
      pg_shard_t peer = *i;
      assert(peer_info.count(peer));
      pg_info_t& pi = peer_info[peer];

      dout(10) << "activate peer osd." << peer << " " << pi << dendl;

      MOSDPGLog *m = 0;
      pg_missing_t& pm = peer_missing[peer];

      bool needs_past_intervals = pi.dne();

      if (pi.last_update == info.last_update) {
        // empty log
	if (!pi.is_empty() && activator_map) {
	  dout(10) << "activate peer osd." << peer << " is up to date, queueing in pending_activators" << dendl;
	  (*activator_map)[peer.osd].push_back(
	    make_pair(
	      pg_notify_t(
		peer.shard, pg_whoami.shard,
		get_osdmap()->get_epoch(),
		get_osdmap()->get_epoch(),
		info),
	      past_intervals));
	} else {
	  dout(10) << "activate peer osd." << peer << " is up to date, but sending pg_log anyway" << dendl;
	  m = new MOSDPGLog(
	    i->shard, pg_whoami.shard,
	    get_osdmap()->get_epoch(), info);
	}
      } else if (
	pg_log.get_tail() > pi.last_update ||
	pi.last_backfill == hobject_t() ||
	(backfill_targets.count(*i) && pi.last_backfill.is_max())) {
	/* This last case covers a situation where a replica is not contiguous
	 * with the auth_log, but is contiguous with this replica.  Reshuffling
	 * the active set to handle this would be tricky, so instead we just go
	 * ahead and backfill it anyway.  This is probably preferrable in any
	 * case since the replica in question would have to be significantly
	 * behind.
	 */
	// backfill
	osd->clog.info() << info.pgid << " restarting backfill on osd." << peer
			 << " from (" << pi.log_tail << "," << pi.last_update << "] " << pi.last_backfill
			 << " to " << info.last_update;

	pi.last_update = info.last_update;
	pi.last_complete = info.last_update;
	pi.last_backfill = hobject_t();
	pi.history = info.history;
	pi.stats.stats.clear();

	m = new MOSDPGLog(
	  i->shard, pg_whoami.shard,
	  get_osdmap()->get_epoch(), pi);

	// send some recent log, so that op dup detection works well.
	m->log.copy_up_to(pg_log.get_log(), cct->_conf->osd_min_pg_log_entries);
	m->info.log_tail = m->log.tail;
	pi.log_tail = m->log.tail;  // sigh...

	pm.clear();
      } else {
	// catch up
	assert(pg_log.get_tail() <= pi.last_update);
	m = new MOSDPGLog(
	  i->shard, pg_whoami.shard,
	  get_osdmap()->get_epoch(), info);
	// send new stuff to append to replicas log
	m->log.copy_after(pg_log.get_log(), pi.last_update);
      }

      // share past_intervals if we are creating the pg on the replica
      // based on whether our info for that peer was dne() *before*
      // updating pi.history in the backfill block above.
      if (needs_past_intervals)
	m->past_intervals = past_intervals;

      // update local version of peer's missing list!
      if (m && pi.last_backfill != hobject_t()) {
        for (list<pg_log_entry_t>::iterator p = m->log.log.begin();
             p != m->log.log.end();
             ++p)
	  if (p->soid <= pi.last_backfill)
	    pm.add_next_event(*p);
      }
      
      if (m) {
	dout(10) << "activate peer osd." << peer << " sending " << m->log << dendl;
	//m->log.print(cout);
	osd->send_message_osd_cluster(peer.osd, m, get_osdmap()->get_epoch());
      }

      // peer now has 
      pi.last_update = info.last_update;

      // update our missing
      if (pm.num_missing() == 0) {
	pi.last_complete = pi.last_update;
        dout(10) << "activate peer osd." << peer << " " << pi << " uptodate" << dendl;
      } else {
        dout(10) << "activate peer osd." << peer << " " << pi << " missing " << pm << dendl;
      }
    }

    // Set up missing_loc
    for (set<pg_shard_t>::iterator i = actingbackfill.begin();
	 i != actingbackfill.end();
	 ++i) {
      if (*i == get_primary()) {
	missing_loc.add_active_missing(pg_log.get_missing());
      } else {
	assert(peer_missing.count(*i));
	missing_loc.add_active_missing(peer_missing[*i]);
      }
    }
    // If necessary, create might_have_unfound to help us find our unfound objects.
    // NOTE: It's important that we build might_have_unfound before trimming the
    // past intervals.
    might_have_unfound.clear();
    if (needs_recovery()) {
      missing_loc.add_source_info(pg_whoami, info, pg_log.get_missing());
      for (set<pg_shard_t>::iterator i = actingbackfill.begin();
	   i != actingbackfill.end();
	   ++i) {
	if (*i == pg_whoami) continue;
	dout(10) << __func__ << ": adding " << *i << " as a source" << dendl;
	assert(peer_missing.count(*i));
	assert(peer_info.count(*i));
	missing_loc.add_source_info(
	  *i,
	  peer_info[*i],
	  peer_missing[*i]);
      }
      for (map<pg_shard_t, pg_missing_t>::iterator i = peer_missing.begin();
	   i != peer_missing.end();
	   ++i) {
	if (is_actingbackfill(i->first))
	  continue;
	assert(peer_info.count(i->first));
	search_for_missing(
	  peer_info[i->first],
	  i->second,
	  i->first,
	  ctx);
      }

      build_might_have_unfound();
    }

    // degraded?
    if (get_osdmap()->get_pg_size(info.pgid.pgid) > acting.size())
      state_set(PG_STATE_DEGRADED);

  }
}

bool PG::op_has_sufficient_caps(OpRequestRef op)
{
  // only check MOSDOp
  if (op->get_req()->get_type() != CEPH_MSG_OSD_OP)
    return true;

  MOSDOp *req = static_cast<MOSDOp*>(op->get_req());

  OSD::Session *session = (OSD::Session *)req->get_connection()->get_priv();
  if (!session) {
    dout(0) << "op_has_sufficient_caps: no session for op " << *req << dendl;
    return false;
  }
  OSDCap& caps = session->caps;
  session->put();

  string key = req->get_object_locator().key;
  if (key.length() == 0)
    key = req->get_oid().name;

  bool cap = caps.is_capable(pool.name, req->get_object_locator().nspace,
                             pool.auid, key,
			     op->need_read_cap(),
			     op->need_write_cap(),
			     op->need_class_read_cap(),
			     op->need_class_write_cap());

  dout(20) << "op_has_sufficient_caps pool=" << pool.id << " (" << pool.name
		   << " " << req->get_object_locator().nspace
	   << ") owner=" << pool.auid
	   << " need_read_cap=" << op->need_read_cap()
	   << " need_write_cap=" << op->need_write_cap()
	   << " need_class_read_cap=" << op->need_class_read_cap()
	   << " need_class_write_cap=" << op->need_class_write_cap()
	   << " -> " << (cap ? "yes" : "NO")
	   << dendl;
  return cap;
}

void PG::take_op_map_waiters()
{
  Mutex::Locker l(map_lock);
  for (list<OpRequestRef>::iterator i = waiting_for_map.begin();
       i != waiting_for_map.end();
       ) {
    if (op_must_wait_for_map(get_osdmap_with_maplock(), *i)) {
      break;
    } else {
      osd->op_wq.queue(make_pair(PGRef(this), *i));
      waiting_for_map.erase(i++);
    }
  }
}

void PG::queue_op(OpRequestRef op)
{
  Mutex::Locker l(map_lock);
  if (!waiting_for_map.empty()) {
    // preserve ordering
    waiting_for_map.push_back(op);
    return;
  }
  if (op_must_wait_for_map(get_osdmap_with_maplock(), op)) {
    waiting_for_map.push_back(op);
    return;
  }
  osd->op_wq.queue(make_pair(PGRef(this), op));
}

void PG::replay_queued_ops()
{
  assert(is_replay());
  eversion_t c = info.last_update;
  list<OpRequestRef> replay;
  dout(10) << "replay_queued_ops" << dendl;
  state_clear(PG_STATE_REPLAY);

  for (map<eversion_t,OpRequestRef>::iterator p = replay_queue.begin();
       p != replay_queue.end();
       ++p) {
    if (p->first.version != c.version+1) {
      dout(10) << "activate replay " << p->first
	       << " skipping " << c.version+1 - p->first.version 
	       << " ops"
	       << dendl;      
      c = p->first;
    }
    dout(10) << "activate replay " << p->first << " "
             << *p->second->get_req() << dendl;
    replay.push_back(p->second);
  }
  replay_queue.clear();
  requeue_ops(replay);
  requeue_ops(waiting_for_active);

  publish_stats_to_osd();
}

void PG::_activate_committed(epoch_t e)
{
  lock();
  if (pg_has_reset_since(e)) {
    dout(10) << "_activate_committed " << e << ", that was an old interval" << dendl;
  } else if (is_primary()) {
    peer_activated.insert(pg_whoami);
    dout(10) << "_activate_committed " << e << " peer_activated now " << peer_activated 
	     << " last_epoch_started " << info.history.last_epoch_started
	     << " same_interval_since " << info.history.same_interval_since << dendl;
    assert(!actingbackfill.empty());
    if (peer_activated.size() == actingbackfill.size())
      all_activated_and_committed();
  } else {
    dout(10) << "_activate_committed " << e << " telling primary" << dendl;
    MOSDPGInfo *m = new MOSDPGInfo(e);
    pg_notify_t i = pg_notify_t(
      get_primary().shard, pg_whoami.shard,
      get_osdmap()->get_epoch(),
      get_osdmap()->get_epoch(),
      info);
    i.info.history.last_epoch_started = e;
    m->pg_list.push_back(make_pair(i, pg_interval_map_t()));
    osd->send_message_osd_cluster(get_primary().osd, m, get_osdmap()->get_epoch());

    state_set(PG_STATE_ACTIVE);
    // waiters
    if (flushes_in_progress == 0) {
      requeue_ops(waiting_for_active);
    }
  }

  if (dirty_info) {
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    write_if_dirty(*t);
    int tr = osd->store->queue_transaction_and_cleanup(osr.get(), t);
    assert(tr == 0);
  }

  unlock();
}

/*
 * update info.history.last_epoch_started ONLY after we and all
 * replicas have activated AND committed the activate transaction
 * (i.e. the peering results are stable on disk).
 */
void PG::all_activated_and_committed()
{
  dout(10) << "all_activated_and_committed" << dendl;
  assert(is_primary());
  assert(peer_activated.size() == actingbackfill.size());
  assert(!actingbackfill.empty());

  // info.last_epoch_started is set during activate()
  info.history.last_epoch_started = info.last_epoch_started;

  share_pg_info();
  publish_stats_to_osd();

  queue_peering_event(
    CephPeeringEvtRef(
      new CephPeeringEvt(
        get_osdmap()->get_epoch(),
        get_osdmap()->get_epoch(),
        AllReplicasActivated())));
}

void PG::queue_snap_trim()
{
  if (osd->queue_for_snap_trim(this))
    dout(10) << "queue_snap_trim -- queuing" << dendl;
  else
    dout(10) << "queue_snap_trim -- already trimming" << dendl;
}

bool PG::queue_scrub()
{
  assert(_lock.is_locked());
  if (is_scrubbing()) {
    return false;
  }
  scrubber.must_scrub = false;
  state_set(PG_STATE_SCRUBBING);
  if (scrubber.must_deep_scrub) {
    state_set(PG_STATE_DEEP_SCRUB);
    scrubber.must_deep_scrub = false;
  }
  if (scrubber.must_repair) {
    state_set(PG_STATE_REPAIR);
    scrubber.must_repair = false;
  }
  osd->queue_for_scrub(this);
  return true;
}

struct C_PG_FinishRecovery : public Context {
  PGRef pg;
  C_PG_FinishRecovery(PG *p) : pg(p) {}
  void finish(int r) {
    pg->_finish_recovery(this);
  }
};

void PG::mark_clean()
{
  // only mark CLEAN if we have the desired number of replicas AND we
  // are not remapped.
  if (acting.size() == get_osdmap()->get_pg_size(info.pgid.pgid) &&
      up == acting)
    state_set(PG_STATE_CLEAN);

  // NOTE: this is actually a bit premature: we haven't purged the
  // strays yet.
  info.history.last_epoch_clean = get_osdmap()->get_epoch();

  trim_past_intervals();

  if (is_clean() && !snap_trimq.empty())
    queue_snap_trim();

  dirty_info = true;
}

void PG::finish_recovery(list<Context*>& tfin)
{
  dout(10) << "finish_recovery" << dendl;
  assert(info.last_complete == info.last_update);

  clear_recovery_state();

  /*
   * sync all this before purging strays.  but don't block!
   */
  finish_sync_event = new C_PG_FinishRecovery(this);
  tfin.push_back(finish_sync_event);
}

void PG::_finish_recovery(Context *c)
{
  lock();
  if (deleting) {
    unlock();
    return;
  }
  if (c == finish_sync_event) {
    dout(10) << "_finish_recovery" << dendl;
    finish_sync_event = 0;
    purge_strays();

    publish_stats_to_osd();

    if (scrub_after_recovery) {
      dout(10) << "_finish_recovery requeueing for scrub" << dendl;
      scrub_after_recovery = false;
      scrubber.must_deep_scrub = true;
      queue_scrub();
    }
  } else {
    dout(10) << "_finish_recovery -- stale" << dendl;
  }
  unlock();
}

void PG::start_recovery_op(const hobject_t& soid)
{
  dout(10) << "start_recovery_op " << soid
#ifdef DEBUG_RECOVERY_OIDS
	   << " (" << recovering_oids << ")"
#endif
	   << dendl;
  assert(recovery_ops_active >= 0);
  recovery_ops_active++;
#ifdef DEBUG_RECOVERY_OIDS
  assert(recovering_oids.count(soid) == 0);
  recovering_oids.insert(soid);
#endif
  // TODOSAM: osd->osd-> not good
  osd->osd->start_recovery_op(this, soid);
}

void PG::finish_recovery_op(const hobject_t& soid, bool dequeue)
{
  dout(10) << "finish_recovery_op " << soid
#ifdef DEBUG_RECOVERY_OIDS
	   << " (" << recovering_oids << ")" 
#endif
	   << dendl;
  assert(recovery_ops_active > 0);
  recovery_ops_active--;
#ifdef DEBUG_RECOVERY_OIDS
  assert(recovering_oids.count(soid));
  recovering_oids.erase(soid);
#endif
  // TODOSAM: osd->osd-> not good
  osd->osd->finish_recovery_op(this, soid, dequeue);
}

static void split_list(
  list<OpRequestRef> *from,
  list<OpRequestRef> *to,
  unsigned match,
  unsigned bits)
{
  for (list<OpRequestRef>::iterator i = from->begin();
       i != from->end();
    ) {
    if (PG::split_request(*i, match, bits)) {
      to->push_back(*i);
      from->erase(i++);
    } else {
      ++i;
    }
  }
}

static void split_replay_queue(
  map<eversion_t, OpRequestRef> *from,
  map<eversion_t, OpRequestRef> *to,
  unsigned match,
  unsigned bits)
{
  for (map<eversion_t, OpRequestRef>::iterator i = from->begin();
       i != from->end();
       ) {
    if (PG::split_request(i->second, match, bits)) {
      to->insert(*i);
      from->erase(i++);
    } else {
      ++i;
    }
  }
}

void PG::split_ops(PG *child, unsigned split_bits) {
  unsigned match = child->info.pgid.ps();
  assert(waiting_for_all_missing.empty());
  assert(waiting_for_cache_not_full.empty());
  assert(waiting_for_unreadable_object.empty());
  assert(waiting_for_degraded_object.empty());
  assert(waiting_for_ack.empty());
  assert(waiting_for_ondisk.empty());
  split_replay_queue(&replay_queue, &(child->replay_queue), match, split_bits);

  osd->dequeue_pg(this, &waiting_for_active);
  split_list(&waiting_for_active, &(child->waiting_for_active), match, split_bits);
  {
    Mutex::Locker l(map_lock); // to avoid a race with the osd dispatch
    split_list(&waiting_for_map, &(child->waiting_for_map), match, split_bits);
  }
}

void PG::split_into(pg_t child_pgid, PG *child, unsigned split_bits)
{
  child->update_snap_mapper_bits(split_bits);
  child->update_osdmap_ref(get_osdmap());

  child->pool = pool;

  // Log
  pg_log.split_into(child_pgid, split_bits, &(child->pg_log));
  child->info.last_complete = info.last_complete;

  info.last_update = pg_log.get_head();
  child->info.last_update = child->pg_log.get_head();

  child->info.last_user_version = info.last_user_version;

  info.log_tail = pg_log.get_tail();
  child->info.log_tail = child->pg_log.get_tail();

  if (info.last_complete < pg_log.get_tail())
    info.last_complete = pg_log.get_tail();
  if (child->info.last_complete < child->pg_log.get_tail())
    child->info.last_complete = child->pg_log.get_tail();

  // Info
  child->info.history = info.history;
  child->info.purged_snaps = info.purged_snaps;
  child->info.last_backfill = info.last_backfill;

  child->info.stats = info.stats;
  info.stats.stats_invalid = true;
  child->info.stats.stats_invalid = true;
  child->info.last_epoch_started = info.last_epoch_started;

  child->snap_trimq = snap_trimq;

  // There can't be recovery/backfill going on now
  int primary, up_primary;
  vector<int> newup, newacting;
  get_osdmap()->pg_to_up_acting_osds(
    child->info.pgid.pgid, &newup, &up_primary, &newacting, &primary);
  child->init_primary_up_acting(
    newup,
    newacting,
    up_primary,
    primary);
  child->role = OSDMap::calc_pg_role(osd->whoami, child->acting);

  // this comparison includes primary rank via pg_shard_t
  if (get_primary() != child->get_primary())
    child->info.history.same_primary_since = get_osdmap()->get_epoch();

  // History
  child->past_intervals = past_intervals;

  split_ops(child, split_bits);
  _split_into(child_pgid, child, split_bits);

  child->dirty_info = true;
  child->dirty_big_info = true;
  dirty_info = true;
  dirty_big_info = true;
}

void PG::clear_recovery_state() 
{
  dout(10) << "clear_recovery_state" << dendl;

  pg_log.reset_recovery_pointers();
  finish_sync_event = 0;

  hobject_t soid;
  while (recovery_ops_active > 0) {
#ifdef DEBUG_RECOVERY_OIDS
    soid = *recovering_oids.begin();
#endif
    finish_recovery_op(soid, true);
  }

  backfill_targets.clear();
  backfill_info.clear();
  peer_backfill_info.clear();
  waiting_on_backfill.clear();
  _clear_recovery_state();  // pg impl specific hook
}

void PG::cancel_recovery()
{
  dout(10) << "cancel_recovery" << dendl;
  clear_recovery_state();
}


void PG::purge_strays()
{
  dout(10) << "purge_strays " << stray_set << dendl;
  
  bool removed = false;
  for (set<pg_shard_t>::iterator p = stray_set.begin();
       p != stray_set.end();
       ++p) {
    assert(!is_actingbackfill(*p));
    if (get_osdmap()->is_up(p->osd)) {
      dout(10) << "sending PGRemove to osd." << *p << dendl;
      vector<spg_t> to_remove;
      to_remove.push_back(spg_t(info.pgid.pgid, p->shard));
      MOSDPGRemove *m = new MOSDPGRemove(
	get_osdmap()->get_epoch(),
	to_remove);
      osd->send_message_osd_cluster(p->osd, m, get_osdmap()->get_epoch());
      stray_purged.insert(*p);
    } else {
      dout(10) << "not sending PGRemove to down osd." << *p << dendl;
    }
    peer_info.erase(*p);
    peer_purged.insert(*p);
    removed = true;
  }

  // if we removed anyone, update peers (which include peer_info)
  if (removed)
    update_heartbeat_peers();

  stray_set.clear();

  // clear _requested maps; we may have to peer() again if we discover
  // (more) stray content
  peer_log_requested.clear();
  peer_missing_requested.clear();
}

void PG::set_probe_targets(const set<pg_shard_t> &probe_set)
{
  Mutex::Locker l(heartbeat_peer_lock);
  probe_targets.clear();
  for (set<pg_shard_t>::iterator i = probe_set.begin();
       i != probe_set.end();
       ++i) {
    probe_targets.insert(i->osd);
  }
}

void PG::clear_probe_targets()
{
  Mutex::Locker l(heartbeat_peer_lock);
  probe_targets.clear();
}

void PG::update_heartbeat_peers()
{
  assert(is_locked());

  set<int> new_peers;
  if (is_primary()) {
    for (unsigned i=0; i<acting.size(); i++) {
      if (acting[i] != CRUSH_ITEM_NONE)
	new_peers.insert(acting[i]);
    }
    for (unsigned i=0; i<up.size(); i++) {
      if (up[i] != CRUSH_ITEM_NONE)
	new_peers.insert(up[i]);
    }
    for (map<pg_shard_t,pg_info_t>::iterator p = peer_info.begin();
	 p != peer_info.end();
	 ++p)
      new_peers.insert(p->first.osd);
  }

  bool need_update = false;
  heartbeat_peer_lock.Lock();
  if (new_peers == heartbeat_peers) {
    dout(10) << "update_heartbeat_peers " << heartbeat_peers << " unchanged" << dendl;
  } else {
    dout(10) << "update_heartbeat_peers " << heartbeat_peers << " -> " << new_peers << dendl;
    heartbeat_peers.swap(new_peers);
    need_update = true;
  }
  heartbeat_peer_lock.Unlock();

  if (need_update)
    osd->need_heartbeat_peer_update();
}

void PG::_update_calc_stats()
{
  info.stats.version = info.last_update;
  info.stats.created = info.history.epoch_created;
  info.stats.last_scrub = info.history.last_scrub;
  info.stats.last_scrub_stamp = info.history.last_scrub_stamp;
  info.stats.last_deep_scrub = info.history.last_deep_scrub;
  info.stats.last_deep_scrub_stamp = info.history.last_deep_scrub_stamp;
  info.stats.last_clean_scrub_stamp = info.history.last_clean_scrub_stamp;
  info.stats.last_epoch_clean = info.history.last_epoch_clean;

  info.stats.log_size = pg_log.get_head().version - pg_log.get_tail().version;
  info.stats.ondisk_log_size =
    pg_log.get_head().version - pg_log.get_tail().version;
  info.stats.log_start = pg_log.get_tail();
  info.stats.ondisk_log_start = pg_log.get_tail();

  // calc copies, degraded
  unsigned target = MAX(
    get_osdmap()->get_pg_size(info.pgid.pgid), actingbackfill.size());
  info.stats.stats.calc_copies(target);
  info.stats.stats.sum.num_objects_degraded = 0;
  if ((is_degraded() || !is_clean()) && is_active()) {
    // NOTE: we only generate copies, degraded, unfound values for
    // the summation, not individual stat categories.
    uint64_t num_objects = info.stats.stats.sum.num_objects;

    uint64_t degraded = 0;

    // if the actingbackfill set is smaller than we want, add in those missing replicas
    if (actingbackfill.size() < target)
      degraded += (target - actingbackfill.size()) * num_objects;

    // missing on primary
    info.stats.stats.sum.num_objects_missing_on_primary =
      pg_log.get_missing().num_missing();
    degraded += pg_log.get_missing().num_missing();

    assert(!actingbackfill.empty());
    for (set<pg_shard_t>::iterator i = actingbackfill.begin();
	 i != actingbackfill.end();
	 ++i) {
      if (*i == pg_whoami) continue;
      assert(peer_missing.count(*i));

      // in missing set
      degraded += peer_missing[*i].num_missing();

      // not yet backfilled
      degraded += num_objects - peer_info[*i].stats.stats.sum.num_objects;
    }
    info.stats.stats.sum.num_objects_degraded = degraded;
    info.stats.stats.sum.num_objects_unfound = get_num_unfound();
  }
}

void PG::publish_stats_to_osd()
{
  pg_stats_publish_lock.Lock();
  if (is_primary()) {
    // update our stat summary
    info.stats.reported_epoch = get_osdmap()->get_epoch();
    ++info.stats.reported_seq;

    if (info.stats.stats.sum.num_scrub_errors)
      state_set(PG_STATE_INCONSISTENT);
    else
      state_clear(PG_STATE_INCONSISTENT);

    utime_t now = ceph_clock_now(cct);
    info.stats.last_fresh = now;
    if (info.stats.state != state) {
      info.stats.state = state;
      info.stats.last_change = now;
      if ((state & PG_STATE_ACTIVE) &&
	  !(info.stats.state & PG_STATE_ACTIVE))
	info.stats.last_became_active = now;
    }
    if (info.stats.state & PG_STATE_CLEAN)
      info.stats.last_clean = now;
    if (info.stats.state & PG_STATE_ACTIVE)
      info.stats.last_active = now;
    info.stats.last_unstale = now;

    _update_calc_stats();

    pg_stats_publish_valid = true;
    pg_stats_publish = info.stats;
    pg_stats_publish.stats.add(unstable_stats);

    dout(15) << "publish_stats_to_osd " << pg_stats_publish.reported_epoch
	     << ":" << pg_stats_publish.reported_seq << dendl;
  } else {
    pg_stats_publish_valid = false;
    dout(15) << "publish_stats_to_osd -- not primary" << dendl;
  }
  pg_stats_publish_lock.Unlock();

  if (is_primary())
    osd->pg_stat_queue_enqueue(this);
}

void PG::clear_publish_stats()
{
  dout(15) << "clear_stats" << dendl;
  pg_stats_publish_lock.Lock();
  pg_stats_publish_valid = false;
  pg_stats_publish_lock.Unlock();

  osd->pg_stat_queue_dequeue(this);
}

/**
 * initialize a newly instantiated pg
 *
 * Initialize PG state, as when a PG is initially created, or when it
 * is first instantiated on the current node.
 *
 * @param role our role/rank
 * @param newup up set
 * @param newacting acting set
 * @param history pg history
 * @param pi past_intervals
 * @param backfill true if info should be marked as backfill
 * @param t transaction to write out our new state in
 */
void PG::init(
  int role,
  vector<int>& newup, int new_up_primary,
  vector<int>& newacting, int new_acting_primary,
  pg_history_t& history,
  pg_interval_map_t& pi,
  bool backfill,
  ObjectStore::Transaction *t)
{
  dout(10) << "init role " << role << " up " << newup << " acting " << newacting
	   << " history " << history
	   << " " << pi.size() << " past_intervals"
	   << dendl;

  set_role(role);
  acting = newacting;
  up = newup;
  init_primary_up_acting(
    newup,
    newacting,
    new_up_primary,
    new_acting_primary);

  info.history = history;
  past_intervals.swap(pi);

  info.stats.up = up;
  info.stats.up_primary = new_up_primary;
  info.stats.acting = acting;
  info.stats.acting_primary = new_acting_primary;
  info.stats.mapping_epoch = info.history.same_interval_since;

  if (backfill) {
    dout(10) << __func__ << ": Setting backfill" << dendl;
    info.last_backfill = hobject_t();
    info.last_complete = info.last_update;
  }

  reg_next_scrub();

  dirty_info = true;
  dirty_big_info = true;
  write_if_dirty(*t);
}

void PG::upgrade(ObjectStore *store, const interval_set<snapid_t> &snapcolls)
{
  unsigned removed = 0;
  for (interval_set<snapid_t>::const_iterator i = snapcolls.begin();
       i != snapcolls.end();
       ++i) {
    for (snapid_t next_dir = i.get_start();
	 next_dir != i.get_start() + i.get_len();
	 ++next_dir) {
      ++removed;
      coll_t cid(info.pgid, next_dir);
      dout(1) << "Removing collection " << cid
	      << " (" << removed << "/" << snapcolls.size()
	      << ")" << dendl;

      hobject_t cur;
      vector<hobject_t> objects;
      while (1) {
	int r = get_pgbackend()->objects_list_partial(
	  cur,
	  store->get_ideal_list_min(),
	  store->get_ideal_list_max(),
	  0,
	  &objects,
	  &cur);
	if (r != 0) {
	  derr << __func__ << ": collection_list_partial returned "
	       << cpp_strerror(r) << dendl;
	  assert(0);
	}
	if (objects.empty()) {
	  assert(cur.is_max());
	  break;
	}
	ObjectStore::Transaction t;
	for (vector<hobject_t>::iterator j = objects.begin();
	     j != objects.end();
	     ++j) {
	  t.remove(cid, *j);
	}
	r = store->apply_transaction(t);
	if (r != 0) {
	  derr << __func__ << ": apply_transaction returned "
	       << cpp_strerror(r) << dendl;
	  assert(0);
	}
	objects.clear();
      }
      ObjectStore::Transaction t;
      t.remove_collection(cid);
      int r = store->apply_transaction(t);
      if (r != 0) {
	derr << __func__ << ": apply_transaction returned "
	     << cpp_strerror(r) << dendl;
	assert(0);
      }
    }
  }

  hobject_t cur;
  coll_t cid(info.pgid);
  unsigned done = 0;
  vector<hobject_t> objects;
  while (1) {
    dout(1) << "Updating snap_mapper from main collection, "
	    << done << " objects done" << dendl;
    int r = get_pgbackend()->objects_list_partial(
      cur,
      store->get_ideal_list_min(),
      store->get_ideal_list_max(),
      0,
      &objects,
      &cur);
    if (r != 0) {
      derr << __func__ << ": collection_list_partial returned "
	   << cpp_strerror(r) << dendl;
      assert(0);
    }
    if (objects.empty()) {
      assert(cur.is_max());
      break;
    }
    done += objects.size();
    ObjectStore::Transaction t;
    for (vector<hobject_t>::iterator j = objects.begin();
	 j != objects.end();
	 ++j) {
      if (j->snap < CEPH_MAXSNAP) {
	OSDriver::OSTransaction _t(osdriver.get_transaction(&t));
	bufferlist bl;
	r = get_pgbackend()->objects_get_attr(
	  *j,
	  OI_ATTR,
	  &bl);
	if (r < 0) {
	  derr << __func__ << ": getattr returned "
	       << cpp_strerror(r) << dendl;
	  assert(0);
	}
	object_info_t oi(bl);
	set<snapid_t> oi_snaps(oi.snaps.begin(), oi.snaps.end());
	set<snapid_t> cur_snaps;
	r = snap_mapper.get_snaps(*j, &cur_snaps);
	if (r == 0) {
	  assert(cur_snaps == oi_snaps);
	} else if (r == -ENOENT) {
	  snap_mapper.add_oid(*j, oi_snaps, &_t);
	} else {
	  derr << __func__ << ": get_snaps returned "
	       << cpp_strerror(r) << dendl;
	  assert(0);
	}
      }
    }
    r = store->apply_transaction(t);
    if (r != 0) {
      derr << __func__ << ": apply_transaction returned "
	   << cpp_strerror(r) << dendl;
      assert(0);
    }
    objects.clear();
  }
  ObjectStore::Transaction t;
  snap_collections.clear();
  dirty_info = true;
  write_if_dirty(t);
  int r = store->apply_transaction(t);
  if (r != 0) {
    derr << __func__ << ": apply_transaction returned "
	 << cpp_strerror(r) << dendl;
    assert(0);
  }
  assert(r == 0);
}

int PG::_write_info(ObjectStore::Transaction& t, epoch_t epoch,
    pg_info_t &info, coll_t coll,
    map<epoch_t,pg_interval_t> &past_intervals,
    interval_set<snapid_t> &snap_collections,
    hobject_t &infos_oid,
    __u8 info_struct_v, bool dirty_big_info, bool force_ver)
{
  // pg state

  if (info_struct_v > cur_struct_v)
    return -EINVAL;

  // Only need to write struct_v to attr when upgrading
  if (force_ver || info_struct_v < cur_struct_v) {
    bufferlist attrbl;
    info_struct_v = cur_struct_v;
    ::encode(info_struct_v, attrbl);
    t.collection_setattr(coll, "info", attrbl);
    dirty_big_info = true;
  }

  // info.  store purged_snaps separately.
  interval_set<snapid_t> purged_snaps;
  map<string,bufferlist> v;
  ::encode(epoch, v[get_epoch_key(info.pgid)]);
  purged_snaps.swap(info.purged_snaps);
  ::encode(info, v[get_info_key(info.pgid)]);
  purged_snaps.swap(info.purged_snaps);

  if (dirty_big_info) {
    // potentially big stuff
    bufferlist& bigbl = v[get_biginfo_key(info.pgid)];
    ::encode(past_intervals, bigbl);
    ::encode(snap_collections, bigbl);
    ::encode(info.purged_snaps, bigbl);
    //dout(20) << "write_info bigbl " << bigbl.length() << dendl;
  }

  t.omap_setkeys(coll_t::META_COLL, infos_oid, v);

  return 0;
}

void PG::write_info(ObjectStore::Transaction& t)
{
  info.stats.stats.add(unstable_stats);
  unstable_stats.clear();

  int ret = _write_info(t, get_osdmap()->get_epoch(), info, coll,
     past_intervals, snap_collections, osd->infos_oid,
     info_struct_v, dirty_big_info);
  assert(ret == 0);
  last_persisted_osdmap_ref = osdmap_ref;

  dirty_info = false;
  dirty_big_info = false;
}

epoch_t PG::peek_map_epoch(ObjectStore *store, coll_t coll, hobject_t &infos_oid, bufferlist *bl)
{
  assert(bl);
  spg_t pgid;
  snapid_t snap;
  bool ok = coll.is_pg(pgid, snap);
  assert(ok);
  int r = store->collection_getattr(coll, "info", *bl);
  assert(r > 0);
  bufferlist::iterator bp = bl->begin();
  __u8 struct_v = 0;
  ::decode(struct_v, bp);
  if (struct_v < 5)
    return 0;
  epoch_t cur_epoch = 0;
  if (struct_v < 6) {
    ::decode(cur_epoch, bp);
  } else {
    // get epoch out of leveldb
    bufferlist tmpbl;
    string ek = get_epoch_key(pgid);
    set<string> keys;
    keys.insert(get_epoch_key(pgid));
    map<string,bufferlist> values;
    store->omap_get_values(coll_t::META_COLL, infos_oid, keys, &values);
    assert(values.size() == 1);
    tmpbl = values[ek];
    bufferlist::iterator p = tmpbl.begin();
    ::decode(cur_epoch, p);
  }
  return cur_epoch;
}

void PG::write_if_dirty(ObjectStore::Transaction& t)
{
  if (dirty_big_info || dirty_info)
    write_info(t);
  pg_log.write_log(t, log_oid);
}

void PG::trim_peers()
{
  assert(is_primary());
  calc_trim_to();
  dout(10) << "trim_peers " << pg_trim_to << dendl;
  if (pg_trim_to != eversion_t()) {
    assert(!actingbackfill.empty());
    for (set<pg_shard_t>::iterator i = actingbackfill.begin();
	 i != actingbackfill.end();
	 ++i) {
      if (*i == pg_whoami) continue;
      osd->send_message_osd_cluster(
	i->osd,
	new MOSDPGTrim(
	  get_osdmap()->get_epoch(),
	  spg_t(info.pgid.pgid, i->shard),
	  pg_trim_to),
	get_osdmap()->get_epoch());
    }
  }
}

void PG::add_log_entry(pg_log_entry_t& e, bufferlist& log_bl)
{
  // raise last_complete only if we were previously up to date
  if (info.last_complete == info.last_update)
    info.last_complete = e.version;
  
  // raise last_update.
  assert(e.version > info.last_update);
  info.last_update = e.version;

  // raise user_version, if it increased (it may have not get bumped
  // by all logged updates)
  if (e.user_version > info.last_user_version)
    info.last_user_version = e.user_version;

  /**
   * Make sure we don't keep around more than we need to in the
   * in-memory log
   */
  e.mod_desc.trim_bl();

  // log mutation
  pg_log.add(e);
  dout(10) << "add_log_entry " << e << dendl;

  e.encode_with_checksum(log_bl);
}


void PG::append_log(
  vector<pg_log_entry_t>& logv, eversion_t trim_to, ObjectStore::Transaction &t,
  bool transaction_applied)
{
  if (transaction_applied)
    update_snap_map(logv, t);
  dout(10) << "append_log " << pg_log.get_log() << " " << logv << dendl;

  map<string,bufferlist> keys;
  for (vector<pg_log_entry_t>::iterator p = logv.begin();
       p != logv.end();
       ++p) {
    p->offset = 0;
    add_log_entry(*p, keys[p->get_key_name()]);
  }
  if (!transaction_applied)
    pg_log.clear_can_rollback_to();

  dout(10) << "append_log  adding " << keys.size() << " keys" << dendl;
  t.omap_setkeys(coll_t::META_COLL, log_oid, keys);
  PGLogEntryHandler handler;
  pg_log.trim(&handler, trim_to, info);
  handler.apply(this, &t);

  // update the local pg, pg log
  dirty_info = true;
  write_if_dirty(t);
}

bool PG::check_log_for_corruption(ObjectStore *store)
{
  /// TODO: this method needs to work with the omap log
  return true;
}

//! Get the name we're going to save our corrupt page log as
std::string PG::get_corrupt_pg_log_name() const
{
  const int MAX_BUF = 512;
  char buf[MAX_BUF];
  struct tm tm_buf;
  time_t my_time(time(NULL));
  const struct tm *t = localtime_r(&my_time, &tm_buf);
  int ret = strftime(buf, sizeof(buf), "corrupt_log_%Y-%m-%d_%k:%M_", t);
  if (ret == 0) {
    dout(0) << "strftime failed" << dendl;
    return "corrupt_log_unknown_time";
  }
  string out(buf);
  out += stringify(info.pgid);
  return out;
}

int PG::read_info(
  ObjectStore *store, const coll_t coll, bufferlist &bl,
  pg_info_t &info, map<epoch_t,pg_interval_t> &past_intervals,
  hobject_t &biginfo_oid, hobject_t &infos_oid,
  interval_set<snapid_t>  &snap_collections, __u8 &struct_v)
{
  bufferlist::iterator p = bl.begin();
  bufferlist lbl;

  // info
  ::decode(struct_v, p);
  if (struct_v < 4)
    ::decode(info, p);
  if (struct_v < 2) {
    ::decode(past_intervals, p);
  
    // snap_collections
    store->collection_getattr(coll, "snap_collections", lbl);
    p = lbl.begin();
    ::decode(struct_v, p);
  } else {
    if (struct_v < 6) {
      int r = store->read(coll_t::META_COLL, biginfo_oid, 0, 0, lbl);
      if (r < 0)
        return r;
      p = lbl.begin();
      ::decode(past_intervals, p);
    } else {
      // get info out of leveldb
      string k = get_info_key(info.pgid);
      string bk = get_biginfo_key(info.pgid);
      set<string> keys;
      keys.insert(k);
      keys.insert(bk);
      map<string,bufferlist> values;
      store->omap_get_values(coll_t::META_COLL, infos_oid, keys, &values);
      assert(values.size() == 2);
      lbl = values[k];
      p = lbl.begin();
      ::decode(info, p);

      lbl = values[bk];
      p = lbl.begin();
      ::decode(past_intervals, p);
    }
  }

  if (struct_v < 3) {
    set<snapid_t> snap_collections_temp;
    ::decode(snap_collections_temp, p);
    snap_collections.clear();
    for (set<snapid_t>::iterator i = snap_collections_temp.begin();
	 i != snap_collections_temp.end();
	 ++i) {
      snap_collections.insert(*i);
    }
  } else {
    ::decode(snap_collections, p);
    if (struct_v >= 4 && struct_v < 6)
      ::decode(info, p);
    else if (struct_v >= 6)
      ::decode(info.purged_snaps, p);
  }
  return 0;
}

void PG::read_state(ObjectStore *store, bufferlist &bl)
{
  int r = read_info(store, coll, bl, info, past_intervals, biginfo_oid,
    osd->infos_oid, snap_collections, info_struct_v);
  assert(r >= 0);

  ostringstream oss;
  if (pg_log.read_log(
      store, coll, log_oid, info,
      oss)) {
    /* We don't want to leave the old format around in case the next log
     * write happens to be an append_log()
     */
    pg_log.mark_log_for_rewrite();
    ObjectStore::Transaction t;
    t.remove(coll_t(), log_oid); // remove old version
    pg_log.write_log(t, log_oid);
    int r = osd->store->apply_transaction(t);
    assert(!r);
  }
  if (oss.str().length())
    osd->clog.error() << oss;

  // log any weirdness
  log_weirdness();
}

void PG::log_weirdness()
{
  if (pg_log.get_tail() != info.log_tail)
    osd->clog.error() << info.pgid
		      << " info mismatch, log.tail " << pg_log.get_tail()
		      << " != info.log_tail " << info.log_tail
		      << "\n";
  if (pg_log.get_head() != info.last_update)
    osd->clog.error() << info.pgid
		      << " info mismatch, log.head " << pg_log.get_head()
		      << " != info.last_update " << info.last_update
		      << "\n";

  if (!pg_log.get_log().empty()) {
    // sloppy check
    if ((pg_log.get_log().log.begin()->version <= pg_log.get_tail()))
      osd->clog.error() << info.pgid
			<< " log bound mismatch, info (" << pg_log.get_tail() << ","
			<< pg_log.get_head() << "]"
			<< " actual ["
			<< pg_log.get_log().log.begin()->version << ","
			<< pg_log.get_log().log.rbegin()->version << "]"
			<< "\n";
  }
  
  if (pg_log.get_log().caller_ops.size() > pg_log.get_log().log.size()) {
    osd->clog.error() << info.pgid
		      << " caller_ops.size " << pg_log.get_log().caller_ops.size()
		      << " > log size " << pg_log.get_log().log.size()
		      << "\n";
  }
}

void PG::update_snap_map(
  vector<pg_log_entry_t> &log_entries,
  ObjectStore::Transaction &t)
{
  for (vector<pg_log_entry_t>::iterator i = log_entries.begin();
       i != log_entries.end();
       ++i) {
    OSDriver::OSTransaction _t(osdriver.get_transaction(&t));
    if (i->soid.snap < CEPH_MAXSNAP) {
      if (i->is_delete()) {
	int r = snap_mapper.remove_oid(
	  i->soid,
	  &_t);
	assert(r == 0);
      } else {
	assert(i->snaps.length() > 0);
	vector<snapid_t> snaps;
	bufferlist::iterator p = i->snaps.begin();
	try {
	  ::decode(snaps, p);
	} catch (...) {
	  snaps.clear();
	}
	set<snapid_t> _snaps(snaps.begin(), snaps.end());

	if (i->is_clone() || i->is_promote()) {
	  snap_mapper.add_oid(
	    i->soid,
	    _snaps,
	    &_t);
	} else if (i->is_modify()) {
	  assert(i->is_modify());
	  int r = snap_mapper.update_snaps(
	    i->soid,
	    _snaps,
	    0,
	    &_t);
	  assert(r == 0);
	} else {
	  assert(i->is_clean());
	}
      }
    }
  }
}

/**
 * filter trimming|trimmed snaps out of snapcontext
 */
void PG::filter_snapc(SnapContext& snapc)
{
  bool filtering = false;
  vector<snapid_t> newsnaps;
  for (vector<snapid_t>::iterator p = snapc.snaps.begin();
       p != snapc.snaps.end();
       ++p) {
    if (snap_trimq.contains(*p) || info.purged_snaps.contains(*p)) {
      if (!filtering) {
	// start building a new vector with what we've seen so far
	dout(10) << "filter_snapc filtering " << snapc << dendl;
	newsnaps.insert(newsnaps.begin(), snapc.snaps.begin(), p);
	filtering = true;
      }
      dout(20) << "filter_snapc  removing trimq|purged snap " << *p << dendl;
    } else {
      if (filtering)
	newsnaps.push_back(*p);  // continue building new vector
    }
  }
  if (filtering) {
    snapc.snaps.swap(newsnaps);
    dout(10) << "filter_snapc  result " << snapc << dendl;
  }
}

void PG::requeue_object_waiters(map<hobject_t, list<OpRequestRef> >& m)
{
  for (map<hobject_t, list<OpRequestRef> >::iterator it = m.begin();
       it != m.end();
       ++it)
    requeue_ops(it->second);
  m.clear();
}

void PG::requeue_op(OpRequestRef op)
{
  osd->op_wq.queue_front(make_pair(PGRef(this), op));
}

void PG::requeue_ops(list<OpRequestRef> &ls)
{
  dout(15) << " requeue_ops " << ls << dendl;
  for (list<OpRequestRef>::reverse_iterator i = ls.rbegin();
       i != ls.rend();
       ++i) {
    osd->op_wq.queue_front(make_pair(PGRef(this), *i));
  }
  ls.clear();
}


// ==========================================================================================
// SCRUB

/*
 * when holding pg and sched_scrub_lock, then the states are:
 *   scheduling:
 *     scrubber.reserved = true
 *     scrub_rserved_peers includes whoami
 *     osd->scrub_pending++
 *   scheduling, replica declined:
 *     scrubber.reserved = true
 *     scrubber.reserved_peers includes -1
 *     osd->scrub_pending++
 *   pending:
 *     scrubber.reserved = true
 *     scrubber.reserved_peers.size() == acting.size();
 *     pg on scrub_wq
 *     osd->scrub_pending++
 *   scrubbing:
 *     scrubber.reserved = false;
 *     scrubber.reserved_peers empty
 *     osd->scrubber.active++
 */

// returns true if a scrub has been newly kicked off
bool PG::sched_scrub()
{
  assert(_lock.is_locked());
  if (!(is_primary() && is_active() && is_clean() && !is_scrubbing())) {
    return false;
  }

  bool time_for_deep = (ceph_clock_now(cct) >
    info.history.last_deep_scrub_stamp + cct->_conf->osd_deep_scrub_interval);
 
  //NODEEP_SCRUB so ignore time initiated deep-scrub
  if (osd->osd->get_osdmap()->test_flag(CEPH_OSDMAP_NODEEP_SCRUB))
    time_for_deep = false;

  if (!scrubber.must_scrub) {
    assert(!scrubber.must_deep_scrub);

    //NOSCRUB so skip regular scrubs
    if (osd->osd->get_osdmap()->test_flag(CEPH_OSDMAP_NOSCRUB) && !time_for_deep)
      return false;
  }

  bool ret = true;
  if (!scrubber.reserved) {
    assert(scrubber.reserved_peers.empty());
    if (osd->inc_scrubs_pending()) {
      dout(20) << "sched_scrub: reserved locally, reserving replicas" << dendl;
      scrubber.reserved = true;
      scrubber.reserved_peers.insert(pg_whoami);
      scrub_reserve_replicas();
    } else {
      dout(20) << "sched_scrub: failed to reserve locally" << dendl;
      ret = false;
    }
  }
  if (scrubber.reserved) {
    if (scrubber.reserve_failed) {
      dout(20) << "sched_scrub: failed, a peer declined" << dendl;
      clear_scrub_reserved();
      scrub_unreserve_replicas();
      ret = false;
    } else if (scrubber.reserved_peers.size() == acting.size()) {
      dout(20) << "sched_scrub: success, reserved self and replicas" << dendl;
      if (time_for_deep) {
	dout(10) << "sched_scrub: scrub will be deep" << dendl;
	state_set(PG_STATE_DEEP_SCRUB);
      }
      queue_scrub();
    } else {
      // none declined, since scrubber.reserved is set
      dout(20) << "sched_scrub: reserved " << scrubber.reserved_peers << ", waiting for replicas" << dendl;
    }
  }

  return ret;
}

void PG::reg_next_scrub()
{
  if (scrubber.must_scrub) {
    scrubber.scrub_reg_stamp = utime_t();
  } else {
    scrubber.scrub_reg_stamp = info.history.last_scrub_stamp;
  }
  if (is_primary())
    osd->reg_last_pg_scrub(info.pgid, scrubber.scrub_reg_stamp);
}

void PG::unreg_next_scrub()
{
  if (is_primary())
    osd->unreg_last_pg_scrub(info.pgid, scrubber.scrub_reg_stamp);
}

void PG::sub_op_scrub_map(OpRequestRef op)
{
  MOSDSubOp *m = static_cast<MOSDSubOp *>(op->get_req());
  assert(m->get_header().type == MSG_OSD_SUBOP);
  dout(7) << "sub_op_scrub_map" << dendl;

  if (m->map_epoch < info.history.same_interval_since) {
    dout(10) << "sub_op_scrub discarding old sub_op from "
	     << m->map_epoch << " < " << info.history.same_interval_since << dendl;
    return;
  }

  op->mark_started();

  dout(10) << " got " << m->from << " scrub map" << dendl;
  bufferlist::iterator p = m->get_data().begin();

  if (scrubber.is_chunky) { // chunky scrub
    scrubber.received_maps[m->from].decode(p, info.pgid.pool());
    dout(10) << "map version is "
	     << scrubber.received_maps[m->from].valid_through
	     << dendl;
  } else {               // classic scrub
    if (scrubber.received_maps.count(m->from)) {
      ScrubMap incoming;
      incoming.decode(p, info.pgid.pool());
      dout(10) << "from replica " << m->from << dendl;
      dout(10) << "map version is " << incoming.valid_through << dendl;
      scrubber.received_maps[m->from].merge_incr(incoming);
    } else {
      scrubber.received_maps[m->from].decode(p, info.pgid.pool());
    }
  }

  --scrubber.waiting_on;
  scrubber.waiting_on_whom.erase(m->from);

  if (scrubber.waiting_on == 0) {
    if (scrubber.is_chunky) { // chunky scrub
      osd->scrub_wq.queue(this);
    } else {                  // classic scrub
      if (scrubber.finalizing) { // incremental lists received
        osd->scrub_finalize_wq.queue(this);
      } else {                // initial lists received
        scrubber.block_writes = true;
        if (last_update_applied == info.last_update) {
          scrubber.finalizing = true;
          scrub_gather_replica_maps();
          ++scrubber.waiting_on;
          scrubber.waiting_on_whom.insert(pg_whoami);
          osd->scrub_wq.queue(this);
        }
      }
    }
  }
}

// send scrub v2-compatible messages (classic scrub)
void PG::_request_scrub_map_classic(pg_shard_t replica, eversion_t version)
{
  assert(replica != pg_whoami);
  dout(10) << "scrub  requesting scrubmap from osd." << replica << dendl;
  MOSDRepScrub *repscrubop =
    new MOSDRepScrub(
      spg_t(info.pgid.pgid, replica.shard), version,
      last_update_applied,
      get_osdmap()->get_epoch());
  osd->send_message_osd_cluster(
    replica.osd, repscrubop, get_osdmap()->get_epoch());
}

// send scrub v3 messages (chunky scrub)
void PG::_request_scrub_map(
  pg_shard_t replica, eversion_t version,
  hobject_t start, hobject_t end,
  bool deep)
{
  assert(replica != pg_whoami);
  dout(10) << "scrub  requesting scrubmap from osd." << replica << dendl;
  MOSDRepScrub *repscrubop = new MOSDRepScrub(
    spg_t(info.pgid.pgid, replica.shard), version,
    get_osdmap()->get_epoch(),
    start, end, deep);
  osd->send_message_osd_cluster(
    replica.osd, repscrubop, get_osdmap()->get_epoch());
}

void PG::sub_op_scrub_reserve(OpRequestRef op)
{
  MOSDSubOp *m = static_cast<MOSDSubOp*>(op->get_req());
  assert(m->get_header().type == MSG_OSD_SUBOP);
  dout(7) << "sub_op_scrub_reserve" << dendl;

  if (scrubber.reserved) {
    dout(10) << "Ignoring reserve request: Already reserved" << dendl;
    return;
  }

  op->mark_started();

  scrubber.reserved = osd->inc_scrubs_pending();

  MOSDSubOpReply *reply = new MOSDSubOpReply(
    m, pg_whoami, 0, get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ACK);
  ::encode(scrubber.reserved, reply->get_data());
  osd->send_message_osd_cluster(reply, m->get_connection());
}

void PG::sub_op_scrub_reserve_reply(OpRequestRef op)
{
  MOSDSubOpReply *reply = static_cast<MOSDSubOpReply*>(op->get_req());
  assert(reply->get_header().type == MSG_OSD_SUBOPREPLY);
  dout(7) << "sub_op_scrub_reserve_reply" << dendl;

  if (!scrubber.reserved) {
    dout(10) << "ignoring obsolete scrub reserve reply" << dendl;
    return;
  }

  op->mark_started();

  pg_shard_t from = reply->from;
  bufferlist::iterator p = reply->get_data().begin();
  bool reserved;
  ::decode(reserved, p);

  if (scrubber.reserved_peers.find(from) != scrubber.reserved_peers.end()) {
    dout(10) << " already had osd." << from << " reserved" << dendl;
  } else {
    if (reserved) {
      dout(10) << " osd." << from << " scrub reserve = success" << dendl;
      scrubber.reserved_peers.insert(from);
    } else {
      /* One decline stops this pg from being scheduled for scrubbing. */
      dout(10) << " osd." << from << " scrub reserve = fail" << dendl;
      scrubber.reserve_failed = true;
    }
    sched_scrub();
  }
}

void PG::sub_op_scrub_unreserve(OpRequestRef op)
{
  assert(op->get_req()->get_header().type == MSG_OSD_SUBOP);
  dout(7) << "sub_op_scrub_unreserve" << dendl;

  op->mark_started();

  clear_scrub_reserved();
}

void PG::sub_op_scrub_stop(OpRequestRef op)
{
  op->mark_started();

  MOSDSubOp *m = static_cast<MOSDSubOp*>(op->get_req());
  assert(m->get_header().type == MSG_OSD_SUBOP);
  dout(7) << "sub_op_scrub_stop" << dendl;

  // see comment in sub_op_scrub_reserve
  scrubber.reserved = false;

  MOSDSubOpReply *reply = new MOSDSubOpReply(
    m, pg_whoami, 0, get_osdmap()->get_epoch(), CEPH_OSD_FLAG_ACK);
  osd->send_message_osd_cluster(reply, m->get_connection());
}

void PG::reject_reservation()
{
  osd->send_message_osd_cluster(
    primary.osd,
    new MBackfillReserve(
      MBackfillReserve::REJECT,
      spg_t(info.pgid.pgid, primary.shard),
      get_osdmap()->get_epoch()),
    get_osdmap()->get_epoch());
}

void PG::schedule_backfill_full_retry()
{
  Mutex::Locker lock(osd->backfill_request_lock);
  osd->backfill_request_timer.add_event_after(
    cct->_conf->osd_backfill_retry_interval,
    new QueuePeeringEvt<RequestBackfill>(
      this, get_osdmap()->get_epoch(),
      RequestBackfill()));
}

void PG::clear_scrub_reserved()
{
  osd->scrub_wq.dequeue(this);
  scrubber.reserved_peers.clear();
  scrubber.reserve_failed = false;

  if (scrubber.reserved) {
    scrubber.reserved = false;
    osd->dec_scrubs_pending();
  }
}

void PG::scrub_reserve_replicas()
{
  assert(backfill_targets.empty());
  for (set<pg_shard_t>::iterator i = actingbackfill.begin();
       i != actingbackfill.end();
       ++i) {
    if (*i == pg_whoami) continue;
    dout(10) << "scrub requesting reserve from osd." << *i << dendl;
    vector<OSDOp> scrub(1);
    scrub[0].op.op = CEPH_OSD_OP_SCRUB_RESERVE;
    hobject_t poid;
    eversion_t v;
    osd_reqid_t reqid;
    MOSDSubOp *subop = new MOSDSubOp(
      reqid, pg_whoami, spg_t(info.pgid.pgid, i->shard), poid, false, 0,
      get_osdmap()->get_epoch(), osd->get_tid(), v);
    subop->ops = scrub;
    osd->send_message_osd_cluster(
      i->osd, subop, get_osdmap()->get_epoch());
  }
}

void PG::scrub_unreserve_replicas()
{
  assert(backfill_targets.empty());
  for (set<pg_shard_t>::iterator i = actingbackfill.begin();
       i != actingbackfill.end();
       ++i) {
    if (*i == pg_whoami) continue;
    dout(10) << "scrub requesting unreserve from osd." << *i << dendl;
    vector<OSDOp> scrub(1);
    scrub[0].op.op = CEPH_OSD_OP_SCRUB_UNRESERVE;
    hobject_t poid;
    eversion_t v;
    osd_reqid_t reqid;
    MOSDSubOp *subop = new MOSDSubOp(
      reqid, pg_whoami, spg_t(info.pgid.pgid, i->shard), poid, false, 0,
      get_osdmap()->get_epoch(), osd->get_tid(), v);
    subop->ops = scrub;
    osd->send_message_osd_cluster(i->osd, subop, get_osdmap()->get_epoch());
  }
}

void PG::_scan_snaps(ScrubMap &smap) 
{
  for (map<hobject_t, ScrubMap::object>::iterator i = smap.objects.begin();
       i != smap.objects.end();
       ++i) {
    const hobject_t &hoid = i->first;
    ScrubMap::object &o = i->second;

    if (hoid.snap < CEPH_MAXSNAP) {
      // fake nlinks for old primaries
      bufferlist bl;
      bl.push_back(o.attrs[OI_ATTR]);
      object_info_t oi(bl);
      if (oi.snaps.empty()) {
	// Just head
	o.nlinks = 1;
      } else if (oi.snaps.size() == 1) {
	// Just head + only snap
	o.nlinks = 2;
      } else {
	// Just head + 1st and last snaps
	o.nlinks = 3;
      }

      // check and if necessary fix snap_mapper
      set<snapid_t> oi_snaps(oi.snaps.begin(), oi.snaps.end());
      set<snapid_t> cur_snaps;
      int r = snap_mapper.get_snaps(hoid, &cur_snaps);
      if (r != 0 && r != -ENOENT) {
	derr << __func__ << ": get_snaps returned " << cpp_strerror(r) << dendl;
	assert(0);
      }
      if (r == -ENOENT || cur_snaps != oi_snaps) {
	ObjectStore::Transaction t;
	OSDriver::OSTransaction _t(osdriver.get_transaction(&t));
	if (r == 0) {
	  r = snap_mapper.remove_oid(hoid, &_t);
	  if (r != 0) {
	    derr << __func__ << ": remove_oid returned " << cpp_strerror(r)
		 << dendl;
	    assert(0);
	  }
	  osd->clog.error() << "osd." << osd->whoami
			    << " found snap mapper error on pg "
			    << info.pgid
			    << " oid " << hoid << " snaps in mapper: "
			    << cur_snaps << ", oi: "
			    << oi_snaps
			    << "...repaired";
	} else {
	  osd->clog.error() << "osd." << osd->whoami
			    << " found snap mapper error on pg "
			    << info.pgid
			    << " oid " << hoid << " snaps missing in mapper"
			    << ", should be: "
			    << oi_snaps
			    << "...repaired";
	}
	snap_mapper.add_oid(hoid, oi_snaps, &_t);
	r = osd->store->apply_transaction(t);
	if (r != 0) {
	  derr << __func__ << ": apply_transaction got " << cpp_strerror(r)
	       << dendl;
	}
      }
    } else {
      o.nlinks = 1;
    }
  }
}

/*
 * build a scrub map over a chunk without releasing the lock
 * only used by chunky scrub
 */
int PG::build_scrub_map_chunk(
  ScrubMap &map,
  hobject_t start, hobject_t end, bool deep,
  ThreadPool::TPHandle &handle)
{
  dout(10) << "build_scrub_map" << dendl;
  dout(20) << "scrub_map_chunk [" << start << "," << end << ")" << dendl;

  map.valid_through = info.last_update;

  // objects
  vector<hobject_t> ls;
  int ret = get_pgbackend()->objects_list_range(start, end, 0, &ls);
  if (ret < 0) {
    dout(5) << "objects_list_range error: " << ret << dendl;
    return ret;
  }

  get_pgbackend()->be_scan_list(map, ls, deep, handle);
  _scan_snaps(map);

  // pg attrs
  osd->store->collection_getattrs(coll, map.attrs);
  dout(10) << __func__ << " done." << dendl;

  return 0;
}

/*
 * build a (sorted) summary of pg content for purposes of scrubbing
 * called while holding pg lock
 */ 
void PG::build_scrub_map(ScrubMap &map, ThreadPool::TPHandle &handle)
{
  dout(10) << "build_scrub_map" << dendl;

  map.valid_through = info.last_update;
  epoch_t epoch = get_osdmap()->get_epoch();

  unlock();

  // wait for any writes on our pg to flush to disk first.  this avoids races
  // with scrub starting immediately after trim or recovery completion.
  osr->flush();

  // objects
  vector<hobject_t> ls;
  osd->store->collection_list(coll, ls);

  get_pgbackend()->be_scan_list(map, ls, false, handle);
  lock();
  _scan_snaps(map);

  if (pg_has_reset_since(epoch)) {
    dout(10) << "scrub  pg changed, aborting" << dendl;
    return;
  }


  dout(10) << "PG relocked, finalizing" << dendl;

  // pg attrs
  osd->store->collection_getattrs(coll, map.attrs);

  dout(10) << __func__ << " done." << dendl;
}


/* 
 * build a summary of pg content changed starting after v
 * called while holding pg lock
 */
void PG::build_inc_scrub_map(
  ScrubMap &map, eversion_t v,
  ThreadPool::TPHandle &handle)
{
  map.valid_through = last_update_applied;
  map.incr_since = v;
  vector<hobject_t> ls;
  list<pg_log_entry_t>::const_iterator p;
  if (v == pg_log.get_tail()) {
    p = pg_log.get_log().log.begin();
  } else if (v > pg_log.get_tail()) {
    p = pg_log.get_log().find_entry(v);
    ++p;
  } else {
    assert(0);
  }
  
  for (; p != pg_log.get_log().log.end(); ++p) {
    if (p->is_update()) {
      ls.push_back(p->soid);
      map.objects[p->soid].negative = false;
    } else if (p->is_delete()) {
      map.objects[p->soid].negative = true;
    }
  }

  get_pgbackend()->be_scan_list(map, ls, false, handle);
  // pg attrs
  osd->store->collection_getattrs(coll, map.attrs);
}

void PG::repair_object(
  const hobject_t& soid, ScrubMap::object *po,
  pg_shard_t bad_peer, pg_shard_t ok_peer)
{
  dout(10) << "repair_object " << soid << " bad_peer osd."
	   << bad_peer << " ok_peer osd." << ok_peer << dendl;
  eversion_t v;
  bufferlist bv;
  bv.push_back(po->attrs[OI_ATTR]);
  object_info_t oi(bv);
  if (bad_peer != primary) {
    peer_missing[bad_peer].add(soid, oi.version, eversion_t());
  } else {
    // We should only be scrubbing if the PG is clean.
    assert(waiting_for_unreadable_object.empty());

    pg_log.missing_add(soid, oi.version, eversion_t());
    missing_loc.add_location(soid, ok_peer);

    pg_log.set_last_requested(0);
  }
}

/* replica_scrub
 *
 * Classic behavior:
 *
 * If msg->scrub_from is not set, replica_scrub calls build_scrubmap to
 * build a complete map (with the pg lock dropped).
 *
 * If msg->scrub_from is set, replica_scrub sets scrubber.finalizing.
 * Similarly to scrub, if last_update_applied is behind info.last_update
 * replica_scrub returns to be requeued by sub_op_modify_applied.
 * replica_scrub then builds an incremental scrub map with the 
 * pg lock held.
 *
 * Chunky behavior:
 *
 * Wait for last_update_applied to match msg->scrub_to as above. Wait
 * for pushes to complete in case of recent recovery. Build a single
 * scrubmap of objects that are in the range [msg->start, msg->end).
 */
void PG::replica_scrub(
  MOSDRepScrub *msg,
  ThreadPool::TPHandle &handle)
{
  assert(!scrubber.active_rep_scrub);
  dout(7) << "replica_scrub" << dendl;

  if (msg->map_epoch < info.history.same_interval_since) {
    if (scrubber.finalizing) {
      dout(10) << "scrub  pg changed, aborting" << dendl;
      scrubber.finalizing = 0;
    } else {
      dout(10) << "replica_scrub discarding old replica_scrub from "
	       << msg->map_epoch << " < " << info.history.same_interval_since 
	       << dendl;
    }
    return;
  }

  ScrubMap map;

  if (msg->chunky) { // chunky scrub
    if (last_update_applied < msg->scrub_to) {
      dout(10) << "waiting for last_update_applied to catch up" << dendl;
      scrubber.active_rep_scrub = msg;
      msg->get();
      return;
    }

    if (active_pushes > 0) {
      dout(10) << "waiting for active pushes to finish" << dendl;
      scrubber.active_rep_scrub = msg;
      msg->get();
      return;
    }

    build_scrub_map_chunk(
      map, msg->start, msg->end, msg->deep,
      handle);

  } else {
    if (msg->scrub_from > eversion_t()) {
      if (scrubber.finalizing) {
        assert(last_update_applied == info.last_update);
        assert(last_update_applied == msg->scrub_to);
      } else {
        scrubber.finalizing = 1;
        if (last_update_applied != msg->scrub_to) {
          scrubber.active_rep_scrub = msg;
          msg->get();
          return;
        }
      }
      build_inc_scrub_map(map, msg->scrub_from, handle);
      scrubber.finalizing = 0;
    } else {
      build_scrub_map(map, handle);
    }

    if (msg->map_epoch < info.history.same_interval_since) {
      dout(10) << "scrub  pg changed, aborting" << dendl;
      return;
    }
  }

  vector<OSDOp> scrub(1);
  scrub[0].op.op = CEPH_OSD_OP_SCRUB_MAP;
  hobject_t poid;
  eversion_t v;
  osd_reqid_t reqid;
  MOSDSubOp *subop = new MOSDSubOp(
    reqid,
    pg_whoami,
    spg_t(info.pgid.pgid, get_primary().shard),
    poid,
    false,
    0,
    msg->map_epoch,
    osd->get_tid(),
    v);
  ::encode(map, subop->get_data());
  subop->ops = scrub;

  osd->send_message_osd_cluster(subop, msg->get_connection());
}

/* Scrub:
 * PG_STATE_SCRUBBING is set when the scrub is queued
 * 
 * scrub will be chunky if all OSDs in PG support chunky scrub
 * scrub will fall back to classic in any other case
 */
void PG::scrub(ThreadPool::TPHandle &handle)
{
  lock();
  if (deleting) {
    unlock();
    return;
  }

  if (!is_primary() || !is_active() || !is_clean() || !is_scrubbing()) {
    dout(10) << "scrub -- not primary or active or not clean" << dendl;
    state_clear(PG_STATE_SCRUBBING);
    state_clear(PG_STATE_REPAIR);
    state_clear(PG_STATE_DEEP_SCRUB);
    publish_stats_to_osd();
    unlock();
    return;
  }

  // when we're starting a scrub, we need to determine which type of scrub to do
  if (!scrubber.active) {
    OSDMapRef curmap = osd->get_osdmap();
    scrubber.is_chunky = true;
    assert(backfill_targets.empty());
    for (unsigned i=0; i<acting.size(); i++) {
      if (acting[i] == pg_whoami.osd)
	continue;
      if (acting[i] == CRUSH_ITEM_NONE)
	continue;
      ConnectionRef con = osd->get_con_osd_cluster(acting[i], get_osdmap()->get_epoch());
      if (!con)
	continue;
      if (!con->has_feature(CEPH_FEATURE_CHUNKY_SCRUB)) {
        dout(20) << "OSD " << acting[i]
                 << " does not support chunky scrubs, falling back to classic"
                 << dendl;
        scrubber.is_chunky = false;
        break;
      }
    }

    if (scrubber.is_chunky) {
      scrubber.deep = state_test(PG_STATE_DEEP_SCRUB);
    } else {
      state_clear(PG_STATE_DEEP_SCRUB);
    }

    dout(10) << "starting a new " << (scrubber.is_chunky ? "chunky" : "classic") << " scrub" << dendl;
  }

  if (scrubber.is_chunky) {
    chunky_scrub(handle);
  } else {
    classic_scrub(handle);
  }

  unlock();
}

/*
 * Classic scrub is a two stage scrub: an initial scrub with writes enabled
 * followed by a finalize with writes blocked.
 *
 * A request is sent out to all replicas for initial scrub maps. Once they reply
 * (sub_op_scrub_map) writes are blocked for all objects in the PG.
 *
 * Finalize: Primaries and replicas wait for all writes in the log to be applied
 * (op_applied), then builds an incremental scrub of all the changes since the
 * beginning of the scrub.
 *
 * Once the primary has received all maps, it compares them and performs
 * repairs.
 *
 * The initial stage of the scrub is handled by scrub_wq and the final stage by
 * scrub_finalize_wq.
 *
 * Relevant variables:
 *
 * scrubber.waiting_on (int)
 * scrubber.waiting_on_whom
 *    Number of people who still need to build an initial/incremental scrub map.
 *    This is decremented in sub_op_scrub_map.
 *
 * last_update_applied
 *    The last update that's hit the disk. In the finalize stage, we block
 *    writes and wait for all writes to flush by checking:
 *
 *      last_update_appied == info.last_update
 *
 *    This is checked in op_applied.
 *
 *  scrubber.block_writes
 *    Flag to determine if writes are blocked.
 *
 *  finalizing scrub
 *    Flag set when we're in the finalize stage.
 *
 */
void PG::classic_scrub(ThreadPool::TPHandle &handle)
{
  assert(pool.info.type == pg_pool_t::TYPE_REPLICATED);
  if (!scrubber.active) {
    dout(10) << "scrub start" << dendl;
    scrubber.active = true;
    scrubber.classic = true;

    publish_stats_to_osd();
    scrubber.received_maps.clear();
    scrubber.epoch_start = info.history.same_interval_since;

    osd->inc_scrubs_active(scrubber.reserved);
    if (scrubber.reserved) {
      scrubber.reserved = false;
      scrubber.reserved_peers.clear();
    }

    /* scrubber.waiting_on == 0 iff all replicas have sent the requested maps and
     * the primary has done a final scrub (which in turn can only happen if
     * last_update_applied == info.last_update)
     */
    scrubber.waiting_on = acting.size();
    scrubber.waiting_on_whom.insert(
      actingbackfill.begin(), actingbackfill.end());
    scrubber.waiting_on_whom.erase(pg_whoami);

    // request maps from replicas
    for (set<pg_shard_t>::iterator i = actingbackfill.begin();
	 i != actingbackfill.end();
	 ++i) {
      if (*i == pg_whoami) continue;
      _request_scrub_map_classic(*i, eversion_t());
    }

    // Unlocks and relocks...
    scrubber.primary_scrubmap = ScrubMap();
    build_scrub_map(scrubber.primary_scrubmap, handle);

    if (scrubber.epoch_start != info.history.same_interval_since) {
      dout(10) << "scrub  pg changed, aborting" << dendl;
      scrub_clear_state();
      scrub_unreserve_replicas();
      return;
    }

    --scrubber.waiting_on;
    scrubber.waiting_on_whom.erase(pg_whoami);

    if (scrubber.waiting_on == 0) {
      // the replicas have completed their scrub map, so lock out writes
      scrubber.block_writes = true;
    } else {
      dout(10) << "wait for replicas to build initial scrub map" << dendl;
      return;
    }

    if (last_update_applied != info.last_update) {
      dout(10) << "wait for cleanup" << dendl;
      return;
    }

    // fall through if last_update_applied == info.last_update and scrubber.waiting_on == 0

    // request incrementals from replicas
    scrub_gather_replica_maps();
    ++scrubber.waiting_on;
    scrubber.waiting_on_whom.insert(pg_whoami);
  }
    
  dout(10) << "clean up scrub" << dendl;
  assert(last_update_applied == info.last_update);

  scrubber.finalizing = true;

  if (scrubber.epoch_start != info.history.same_interval_since) {
    dout(10) << "scrub  pg changed, aborting" << dendl;
    scrub_clear_state();
    scrub_unreserve_replicas();
    return;
  }
  
  if (scrubber.primary_scrubmap.valid_through != pg_log.get_head()) {
    ScrubMap incr;
    build_inc_scrub_map(incr, scrubber.primary_scrubmap.valid_through, handle);
    scrubber.primary_scrubmap.merge_incr(incr);
  }
  
  --scrubber.waiting_on;
  scrubber.waiting_on_whom.erase(pg_whoami);
  if (scrubber.waiting_on == 0) {
    assert(last_update_applied == info.last_update);
    osd->scrub_finalize_wq.queue(this);
  }
}

/*
 * Chunky scrub scrubs objects one chunk at a time with writes blocked for that
 * chunk.
 *
 * The object store is partitioned into chunks which end on hash boundaries. For
 * each chunk, the following logic is performed:
 *
 *  (1) Block writes on the chunk
 *  (2) Request maps from replicas
 *  (3) Wait for pushes to be applied (after recovery)
 *  (4) Wait for writes to flush on the chunk
 *  (5) Wait for maps from replicas
 *  (6) Compare / repair all scrub maps
 *
 * This logic is encoded in the very linear state machine:
 *
 *           +------------------+
 *  _________v__________        |
 * |                    |       |
 * |      INACTIVE      |       |
 * |____________________|       |
 *           |                  |
 *           |   +----------+   |
 *  _________v___v______    |   |
 * |                    |   |   |
 * |      NEW_CHUNK     |   |   |
 * |____________________|   |   |
 *           |              |   |
 *  _________v__________    |   |
 * |                    |   |   |
 * |     WAIT_PUSHES    |   |   |
 * |____________________|   |   |
 *           |              |   |
 *  _________v__________    |   |
 * |                    |   |   |
 * |  WAIT_LAST_UPDATE  |   |   |
 * |____________________|   |   |
 *           |              |   |
 *  _________v__________    |   |
 * |                    |   |   |
 * |      BUILD_MAP     |   |   |
 * |____________________|   |   |
 *           |              |   |
 *  _________v__________    |   |
 * |                    |   |   |
 * |    WAIT_REPLICAS   |   |   |
 * |____________________|   |   |
 *           |              |   |
 *  _________v__________    |   |
 * |                    |   |   |
 * |    COMPARE_MAPS    |   |   |
 * |____________________|   |   |
 *           |   |          |   |
 *           |   +----------+   |
 *  _________v__________        |
 * |                    |       |
 * |       FINISH       |       |
 * |____________________|       |
 *           |                  |
 *           +------------------+
 *
 * The primary determines the last update from the subset by walking the log. If
 * it sees a log entry pertaining to a file in the chunk, it tells the replicas
 * to wait until that update is applied before building a scrub map. Both the
 * primary and replicas will wait for any active pushes to be applied.
 *
 * In contrast to classic_scrub, chunky_scrub is entirely handled by scrub_wq.
 *
 * scrubber.state encodes the current state of the scrub (refer to state diagram
 * for details).
 */
void PG::chunky_scrub(ThreadPool::TPHandle &handle)
{
  // check for map changes
  if (scrubber.is_chunky_scrub_active()) {
    if (scrubber.epoch_start != info.history.same_interval_since) {
      dout(10) << "scrub  pg changed, aborting" << dendl;
      scrub_clear_state();
      scrub_unreserve_replicas();
      return;
    }
  }

  bool done = false;
  int ret;

  while (!done) {
    dout(20) << "scrub state " << Scrubber::state_string(scrubber.state) << dendl;

    switch (scrubber.state) {
      case PG::Scrubber::INACTIVE:
        dout(10) << "scrub start" << dendl;

        publish_stats_to_osd();
        scrubber.epoch_start = info.history.same_interval_since;
        scrubber.active = true;

	osd->inc_scrubs_active(scrubber.reserved);
	if (scrubber.reserved) {
	  scrubber.reserved = false;
	  scrubber.reserved_peers.clear();
	}

        scrubber.start = hobject_t();
        scrubber.state = PG::Scrubber::NEW_CHUNK;

        break;

      case PG::Scrubber::NEW_CHUNK:
        scrubber.primary_scrubmap = ScrubMap();
        scrubber.received_maps.clear();

        {

          // get the start and end of our scrub chunk
          //
          // start and end need to lie on a hash boundary. We test for this by
          // requesting a list and searching backward from the end looking for a
          // boundary. If there's no boundary, we request a list after the first
          // list, and so forth.

          bool boundary_found = false;
          hobject_t start = scrubber.start;
          while (!boundary_found) {
            vector<hobject_t> objects;
            ret = get_pgbackend()->objects_list_partial(
	      start,
	      cct->_conf->osd_scrub_chunk_min,
	      cct->_conf->osd_scrub_chunk_max,
	      0,
	      &objects,
	      &scrubber.end);
            assert(ret >= 0);

            // in case we don't find a boundary: start again at the end
            start = scrubber.end;

            // special case: reached end of file store, implicitly a boundary
            if (objects.empty()) {
              break;
            }

            // search backward from the end looking for a boundary
            objects.push_back(scrubber.end);
            while (!boundary_found && objects.size() > 1) {
              hobject_t end = objects.back().get_boundary();
              objects.pop_back();

              if (objects.back().get_filestore_key() != end.get_filestore_key()) {
                scrubber.end = end;
                boundary_found = true;
              }
            }
          }
        }

        scrubber.block_writes = true;

        // walk the log to find the latest update that affects our chunk
        scrubber.subset_last_update = pg_log.get_tail();
        for (list<pg_log_entry_t>::const_iterator p = pg_log.get_log().log.begin();
             p != pg_log.get_log().log.end();
             ++p) {
          if (p->soid >= scrubber.start && p->soid < scrubber.end)
            scrubber.subset_last_update = p->version;
        }

        // ask replicas to wait until last_update_applied >= scrubber.subset_last_update and then scan
        scrubber.waiting_on_whom.insert(pg_whoami);
        ++scrubber.waiting_on;

        // request maps from replicas
	for (set<pg_shard_t>::iterator i = actingbackfill.begin();
	     i != actingbackfill.end();
	     ++i) {
	  if (*i == pg_whoami) continue;
          _request_scrub_map(*i, scrubber.subset_last_update,
                             scrubber.start, scrubber.end, scrubber.deep);
          scrubber.waiting_on_whom.insert(*i);
          ++scrubber.waiting_on;
        }

        scrubber.state = PG::Scrubber::WAIT_PUSHES;

        break;

      case PG::Scrubber::WAIT_PUSHES:
        if (active_pushes == 0) {
          scrubber.state = PG::Scrubber::WAIT_LAST_UPDATE;
        } else {
          dout(15) << "wait for pushes to apply" << dendl;
          done = true;
        }
        break;

      case PG::Scrubber::WAIT_LAST_UPDATE:
        if (last_update_applied >= scrubber.subset_last_update) {
          scrubber.state = PG::Scrubber::BUILD_MAP;
        } else {
          // will be requeued by op_applied
          dout(15) << "wait for writes to flush" << dendl;
          done = true;
        }
        break;

      case PG::Scrubber::BUILD_MAP:
        assert(last_update_applied >= scrubber.subset_last_update);

        // build my own scrub map
        ret = build_scrub_map_chunk(scrubber.primary_scrubmap,
                                    scrubber.start, scrubber.end,
                                    scrubber.deep,
				    handle);
        if (ret < 0) {
          dout(5) << "error building scrub map: " << ret << ", aborting" << dendl;
          scrub_clear_state();
          scrub_unreserve_replicas();
          return;
        }

        --scrubber.waiting_on;
        scrubber.waiting_on_whom.erase(pg_whoami);

        scrubber.state = PG::Scrubber::WAIT_REPLICAS;
        break;

      case PG::Scrubber::WAIT_REPLICAS:
        if (scrubber.waiting_on > 0) {
          // will be requeued by sub_op_scrub_map
          dout(10) << "wait for replicas to build scrub map" << dendl;
          done = true;
        } else {
          scrubber.state = PG::Scrubber::COMPARE_MAPS;
        }
        break;

      case PG::Scrubber::COMPARE_MAPS:
        assert(last_update_applied >= scrubber.subset_last_update);
        assert(scrubber.waiting_on == 0);

        scrub_compare_maps();
        scrubber.block_writes = false;
	scrubber.run_callbacks();

        // requeue the writes from the chunk that just finished
        requeue_ops(waiting_for_active);

        if (scrubber.end < hobject_t::get_max()) {
          // schedule another leg of the scrub
          scrubber.start = scrubber.end;

          scrubber.state = PG::Scrubber::NEW_CHUNK;
          osd->scrub_wq.queue(this);
          done = true;
        } else {
          scrubber.state = PG::Scrubber::FINISH;
        }

        break;

      case PG::Scrubber::FINISH:
        scrub_finish();
        scrubber.state = PG::Scrubber::INACTIVE;
        done = true;

        break;

      default:
        assert(0);
    }
  }
}

void PG::scrub_clear_state()
{
  assert(_lock.is_locked());
  state_clear(PG_STATE_SCRUBBING);
  state_clear(PG_STATE_REPAIR);
  state_clear(PG_STATE_DEEP_SCRUB);
  publish_stats_to_osd();

  // active -> nothing.
  if (scrubber.active)
    osd->dec_scrubs_active();

  requeue_ops(waiting_for_active);

  if (scrubber.queue_snap_trim) {
    dout(10) << "scrub finished, requeuing snap_trimmer" << dendl;
    queue_snap_trim();
  }

  scrubber.reset();

  // type-specific state clear
  _scrub_clear_state();
}

bool PG::scrub_gather_replica_maps()
{
  assert(scrubber.waiting_on == 0);
  assert(_lock.is_locked());

  for (map<pg_shard_t, ScrubMap>::iterator p = scrubber.received_maps.begin();
       p != scrubber.received_maps.end();
       ++p) {
    
    if (scrubber.received_maps[p->first].valid_through != pg_log.get_head()) {
      scrubber.waiting_on++;
      scrubber.waiting_on_whom.insert(p->first);
      // Need to request another incremental map
      _request_scrub_map_classic(p->first, p->second.valid_through);
    }
  }
  
  if (scrubber.waiting_on > 0) {
    return false;
  } else {
    return true;
  }
}

void PG::scrub_compare_maps() 
{
  dout(10) << "scrub_compare_maps has maps, analyzing" << dendl;

  // construct authoritative scrub map for type specific scrubbing
  ScrubMap authmap(scrubber.primary_scrubmap);

  if (acting.size() > 1) {
    dout(10) << "scrub  comparing replica scrub maps" << dendl;

    stringstream ss;

    // Map from object with errors to good peer
    map<hobject_t, pg_shard_t> authoritative;
    map<pg_shard_t, ScrubMap *> maps;

    dout(2) << "scrub   osd." << acting[0] << " has " 
	    << scrubber.primary_scrubmap.objects.size() << " items" << dendl;
    maps[pg_whoami] = &scrubber.primary_scrubmap;

    for (set<pg_shard_t>::iterator i = actingbackfill.begin();
	 i != actingbackfill.end();
	 ++i) {
      if (*i == pg_whoami) continue;
      dout(2) << "scrub replica " << *i << " has "
	      << scrubber.received_maps[*i].objects.size()
	      << " items" << dendl;
      maps[*i] = &scrubber.received_maps[*i];
    }

    get_pgbackend()->be_compare_scrubmaps(
      maps,
      scrubber.missing,
      scrubber.inconsistent,
      authoritative,
      scrubber.inconsistent_snapcolls,
      scrubber.shallow_errors,
      scrubber.deep_errors,
      info.pgid, acting,
      ss);
    dout(2) << ss.str() << dendl;

    if (!authoritative.empty() || !scrubber.inconsistent_snapcolls.empty()) {
      osd->clog.error(ss);
    }

    for (map<hobject_t, pg_shard_t>::iterator i = authoritative.begin();
	 i != authoritative.end();
	 ++i) {
      scrubber.authoritative.insert(
	make_pair(
	  i->first,
	  make_pair(maps[i->second]->objects[i->first], i->second)));
    }

    for (map<hobject_t, pg_shard_t>::iterator i = authoritative.begin();
	 i != authoritative.end();
	 ++i) {
      authmap.objects.erase(i->first);
      authmap.objects.insert(*(maps[i->second]->objects.find(i->first)));
    }
  }

  // ok, do the pg-type specific scrubbing
  _scrub(authmap);
}

void PG::scrub_process_inconsistent()
{
  dout(10) << "process_inconsistent() checking authoritative" << dendl;
  bool repair = state_test(PG_STATE_REPAIR);
  bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
  const char *mode = (repair ? "repair": (deep_scrub ? "deep-scrub" : "scrub"));

  if (!scrubber.authoritative.empty() || !scrubber.inconsistent.empty()) {
    stringstream ss;
    for (map<hobject_t, set<pg_shard_t> >::iterator obj =
	   scrubber.inconsistent_snapcolls.begin();
	 obj != scrubber.inconsistent_snapcolls.end();
	 ++obj) {
      for (set<pg_shard_t>::iterator j = obj->second.begin();
	   j != obj->second.end();
	   ++j) {
	++scrubber.shallow_errors;
	ss << info.pgid << " " << mode << " " << " object " << obj->first
	   << " has inconsistent snapcolls on " << *j << std::endl;
      }
    }

    ss << info.pgid << " " << mode << " "
       << scrubber.missing.size() << " missing, "
       << scrubber.inconsistent.size() << " inconsistent objects\n";
    dout(2) << ss.str() << dendl;
    osd->clog.error(ss);
    if (repair) {
      state_clear(PG_STATE_CLEAN);
      for (map<hobject_t, pair<ScrubMap::object, pg_shard_t> >::iterator i =
	     scrubber.authoritative.begin();
	   i != scrubber.authoritative.end();
	   ++i) {
	set<pg_shard_t>::iterator j;
	
	if (scrubber.missing.count(i->first)) {
	  for (j = scrubber.missing[i->first].begin();
	       j != scrubber.missing[i->first].end(); 
	       ++j) {
	    repair_object(
	      i->first,
	      &(i->second.first),
	      *j,
	      i->second.second);
	    ++scrubber.fixed;
	  }
	}
	if (scrubber.inconsistent.count(i->first)) {
	  for (j = scrubber.inconsistent[i->first].begin(); 
	       j != scrubber.inconsistent[i->first].end(); 
	       ++j) {
	    repair_object(i->first, 
	      &(i->second.first),
	      *j,
	      i->second.second);
	    ++scrubber.fixed;
	  }
	}
      }
    }
  }
}

void PG::scrub_finalize()
{
  lock();
  if (deleting) {
    unlock();
    return;
  }

  assert(last_update_applied == info.last_update);

  if (scrubber.epoch_start != info.history.same_interval_since) {
    dout(10) << "scrub  pg changed, aborting" << dendl;
    scrub_clear_state();
    scrub_unreserve_replicas();
    unlock();
    return;
  }

  if (!scrub_gather_replica_maps()) {
    dout(10) << "maps not yet up to date, sent out new requests" << dendl;
    unlock();
    return;
  }

  scrub_compare_maps();

  scrub_finish();

  dout(10) << "scrub done" << dendl;
  unlock();
}

// the part that actually finalizes a scrub
void PG::scrub_finish() 
{
  bool repair = state_test(PG_STATE_REPAIR);
  bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
  const char *mode = (repair ? "repair": (deep_scrub ? "deep-scrub" : "scrub"));

  // type-specific finish (can tally more errors)
  _scrub_finish();

  scrub_process_inconsistent();

  {
    stringstream oss;
    oss << info.pgid.pgid << " " << mode << " ";
    int total_errors = scrubber.shallow_errors + scrubber.deep_errors;
    if (total_errors)
      oss << total_errors << " errors";
    else
      oss << "ok";
    if (!deep_scrub && info.stats.stats.sum.num_deep_scrub_errors)
      oss << " ( " << info.stats.stats.sum.num_deep_scrub_errors
          << " remaining deep scrub error(s) )";
    if (repair)
      oss << ", " << scrubber.fixed << " fixed";
    oss << "\n";
    if (total_errors)
      osd->clog.error(oss);
    else
      osd->clog.info(oss);
  }

  // finish up
  unreg_next_scrub();
  utime_t now = ceph_clock_now(cct);
  info.history.last_scrub = info.last_update;
  info.history.last_scrub_stamp = now;
  if (scrubber.deep) {
    info.history.last_deep_scrub = info.last_update;
    info.history.last_deep_scrub_stamp = now;
  }
  // Since we don't know which errors were fixed, we can only clear them
  // when every one has been fixed.
  if (repair) {
    if (scrubber.fixed == scrubber.shallow_errors + scrubber.deep_errors) {
      assert(deep_scrub);
      scrubber.shallow_errors = scrubber.deep_errors = 0;
    } else {
      // Deep scrub in order to get corrected error counts
      scrub_after_recovery = true;
    }
  }
  if (deep_scrub) {
    if ((scrubber.shallow_errors == 0) && (scrubber.deep_errors == 0))
      info.history.last_clean_scrub_stamp = now;
    info.stats.stats.sum.num_shallow_scrub_errors = scrubber.shallow_errors;
    info.stats.stats.sum.num_deep_scrub_errors = scrubber.deep_errors;
  } else {
    info.stats.stats.sum.num_shallow_scrub_errors = scrubber.shallow_errors;
    // XXX: last_clean_scrub_stamp doesn't mean the pg is not inconsistent
    // because of deep-scrub errors
    if (scrubber.shallow_errors == 0)
      info.history.last_clean_scrub_stamp = now;
  }
  info.stats.stats.sum.num_scrub_errors = 
    info.stats.stats.sum.num_shallow_scrub_errors +
    info.stats.stats.sum.num_deep_scrub_errors;
  reg_next_scrub();

  {
    ObjectStore::Transaction *t = new ObjectStore::Transaction;
    dirty_info = true;
    write_if_dirty(*t);
    int tr = osd->store->queue_transaction_and_cleanup(osr.get(), t);
    assert(tr == 0);
  }


  if (repair) {
    queue_peering_event(
      CephPeeringEvtRef(
	new CephPeeringEvt(
	  get_osdmap()->get_epoch(),
	  get_osdmap()->get_epoch(),
	  DoRecovery())));
  }

  scrub_clear_state();
  scrub_unreserve_replicas();

  if (is_active() && is_primary()) {
    share_pg_info();
  }
}

void PG::share_pg_info()
{
  dout(10) << "share_pg_info" << dendl;

  // share new pg_info_t with replicas
  assert(!actingbackfill.empty());
  for (set<pg_shard_t>::iterator i = actingbackfill.begin();
       i != actingbackfill.end();
       ++i) {
    if (*i == pg_whoami) continue;
    pg_shard_t peer = *i;
    if (peer_info.count(peer)) {
      peer_info[peer].last_epoch_started = info.last_epoch_started;
      peer_info[peer].history.merge(info.history);
    }
    MOSDPGInfo *m = new MOSDPGInfo(get_osdmap()->get_epoch());
    m->pg_list.push_back(
      make_pair(
	pg_notify_t(
	  peer.shard, pg_whoami.shard,
	  get_osdmap()->get_epoch(),
	  get_osdmap()->get_epoch(),
	  info),
	pg_interval_map_t()));
    osd->send_message_osd_cluster(peer.osd, m, get_osdmap()->get_epoch());
  }
}

/*
 * Share a new segment of this PG's log with some replicas, after PG is active.
 *
 * Updates peer_missing and peer_info.
 */
void PG::share_pg_log()
{
  dout(10) << __func__ << dendl;
  assert(is_primary());

  set<pg_shard_t>::const_iterator a = actingbackfill.begin();
  assert(a != actingbackfill.end());
  set<pg_shard_t>::const_iterator end = actingbackfill.end();
  while (a != end) {
    pg_shard_t peer(*a);
    ++a;
    if (peer == pg_whoami) continue;
    pg_missing_t& pmissing(peer_missing[peer]);
    pg_info_t& pinfo(peer_info[peer]);

    MOSDPGLog *m = new MOSDPGLog(
      peer.shard, pg_whoami.shard,
      info.last_update.epoch, info);
    m->log.copy_after(pg_log.get_log(), pinfo.last_update);

    for (list<pg_log_entry_t>::const_iterator i = m->log.log.begin();
	 i != m->log.log.end();
	 ++i) {
      pmissing.add_next_event(*i);
    }
    pinfo.last_update = m->log.head;

    osd->send_message_osd_cluster(peer.osd, m, get_osdmap()->get_epoch());
  }
}

void PG::update_history_from_master(pg_history_t new_history)
{
  unreg_next_scrub();
  info.history.merge(new_history);
  reg_next_scrub();
}

void PG::fulfill_info(
  pg_shard_t from, const pg_query_t &query,
  pair<pg_shard_t, pg_info_t> &notify_info)
{
  assert(from == primary);
  assert(query.type == pg_query_t::INFO);

  // info
  dout(10) << "sending info" << dendl;
  notify_info = make_pair(from, info);
}

void PG::fulfill_log(
  pg_shard_t from, const pg_query_t &query, epoch_t query_epoch)
{
  dout(10) << "log request from " << from << dendl;
  assert(from == primary);
  assert(query.type != pg_query_t::INFO);

  MOSDPGLog *mlog = new MOSDPGLog(
    from.shard, pg_whoami.shard,
    get_osdmap()->get_epoch(),
    info, query_epoch);
  mlog->missing = pg_log.get_missing();

  // primary -> other, when building master log
  if (query.type == pg_query_t::LOG) {
    dout(10) << " sending info+missing+log since " << query.since
	     << dendl;
    if (query.since != eversion_t() && query.since < pg_log.get_tail()) {
      osd->clog.error() << info.pgid << " got broken pg_query_t::LOG since " << query.since
			<< " when my log.tail is " << pg_log.get_tail()
			<< ", sending full log instead\n";
      mlog->log = pg_log.get_log();           // primary should not have requested this!!
    } else
      mlog->log.copy_after(pg_log.get_log(), query.since);
  }
  else if (query.type == pg_query_t::FULLLOG) {
    dout(10) << " sending info+missing+full log" << dendl;
    mlog->log = pg_log.get_log();
  }

  dout(10) << " sending " << mlog->log << " " << mlog->missing << dendl;

  ConnectionRef con = osd->get_con_osd_cluster(
    from.osd, get_osdmap()->get_epoch());
  if (con) {
    osd->osd->_share_map_outgoing(from.osd, con.get(), get_osdmap());
    osd->send_message_osd_cluster(mlog, con.get());
  } else {
    mlog->put();
  }
}


// true if all OSDs in prior intervals may have crashed, and we need to replay
// false positives are okay, false negatives are not.
bool PG::may_need_replay(const OSDMapRef osdmap) const
{
  bool crashed = false;

  for (map<epoch_t,pg_interval_t>::const_reverse_iterator p = past_intervals.rbegin();
       p != past_intervals.rend();
       ++p) {
    const pg_interval_t &interval = p->second;
    dout(10) << "may_need_replay " << interval << dendl;

    if (interval.last < info.history.last_epoch_started)
      break;  // we don't care

    if (interval.acting.empty())
      continue;

    if (!interval.maybe_went_rw)
      continue;

    // look at whether any of the osds during this interval survived
    // past the end of the interval (i.e., didn't crash and
    // potentially fail to COMMIT a write that it ACKed).
    bool any_survived_interval = false;

    // consider ACTING osds
    for (unsigned i=0; i<interval.acting.size(); i++) {
      int o = interval.acting[i];
      if (o == CRUSH_ITEM_NONE)
	continue;

      const osd_info_t *pinfo = 0;
      if (osdmap->exists(o))
	pinfo = &osdmap->get_info(o);

      // does this osd appear to have survived through the end of the
      // interval?
      if (pinfo) {
	if (pinfo->up_from <= interval.first && pinfo->up_thru > interval.last) {
	  dout(10) << "may_need_replay  osd." << o
		   << " up_from " << pinfo->up_from << " up_thru " << pinfo->up_thru
		   << " survived the interval" << dendl;
	  any_survived_interval = true;
	}
	else if (pinfo->up_from <= interval.first &&
		 (std::find(acting.begin(), acting.end(), o) != acting.end() ||
		  std::find(up.begin(), up.end(), o) != up.end())) {
	  dout(10) << "may_need_replay  osd." << o
		   << " up_from " << pinfo->up_from << " and is in acting|up,"
		   << " assumed to have survived the interval" << dendl;
	  // (if it hasn't, we will rebuild PriorSet)
	  any_survived_interval = true;
	}
	else if (pinfo->up_from > interval.last &&
		 pinfo->last_clean_begin <= interval.first &&
		 pinfo->last_clean_end > interval.last) {
	  dout(10) << "may_need_replay  prior osd." << o
		   << " up_from " << pinfo->up_from
		   << " and last clean interval ["
		   << pinfo->last_clean_begin << "," << pinfo->last_clean_end
		   << ") survived the interval" << dendl;
	  any_survived_interval = true;
	}
      }
    }

    if (!any_survived_interval) {
      dout(3) << "may_need_replay  no known survivors of interval "
	      << interval.first << "-" << interval.last
	      << ", may need replay" << dendl;
      crashed = true;
      break;
    }
  }

  return crashed;
}

bool PG::is_split(OSDMapRef lastmap, OSDMapRef nextmap)
{
  return info.pgid.is_split(
    lastmap->get_pg_num(pool.id),
    nextmap->get_pg_num(pool.id),
    0);
}

bool PG::acting_up_affected(
  int newupprimary,
  int newactingprimary,
  const vector<int>& newup, const vector<int>& newacting)
{
  if (newupprimary != up_primary.osd ||
      newactingprimary != primary.osd ||
      acting != newacting ||
      up != newup) {
    dout(20) << "acting_up_affected newup " << newup
	     << " newacting " << newacting << dendl;
    return true;
  } else {
    return false;
  }
}

bool PG::old_peering_msg(epoch_t reply_epoch, epoch_t query_epoch)
{
  if (last_peering_reset > reply_epoch ||
      last_peering_reset > query_epoch) {
    dout(10) << "old_peering_msg reply_epoch " << reply_epoch << " query_epoch " << query_epoch
	     << " last_peering_reset " << last_peering_reset
	     << dendl;
    return true;
  }
  return false;
}

void PG::set_last_peering_reset()
{
  dout(20) << "set_last_peering_reset " << get_osdmap()->get_epoch() << dendl;
  last_peering_reset = get_osdmap()->get_epoch();
}

struct FlushState {
  PGRef pg;
  epoch_t epoch;
  FlushState(PG *pg, epoch_t epoch) : pg(pg), epoch(epoch) {}
  ~FlushState() {
    pg->lock();
    if (!pg->pg_has_reset_since(epoch))
      pg->queue_flushed(epoch);
    pg->unlock();
  }
};
typedef ceph::shared_ptr<FlushState> FlushStateRef;

void PG::start_flush(ObjectStore::Transaction *t,
		     list<Context *> *on_applied,
		     list<Context *> *on_safe)
{
  // flush in progress ops
  FlushStateRef flush_trigger(
    new FlushState(this, get_osdmap()->get_epoch()));
  t->nop();
  flushes_in_progress++;
  on_applied->push_back(new ContainerContext<FlushStateRef>(flush_trigger));
  on_safe->push_back(new ContainerContext<FlushStateRef>(flush_trigger));
}

/* Called before initializing peering during advance_map */
void PG::start_peering_interval(
  const OSDMapRef lastmap,
  const vector<int>& newup, int new_up_primary,
  const vector<int>& newacting, int new_acting_primary,
  ObjectStore::Transaction *t)
{
  const OSDMapRef osdmap = get_osdmap();

  set_last_peering_reset();

  vector<int> oldacting, oldup;
  int oldrole = get_role();

  unreg_next_scrub();

  pg_shard_t old_acting_primary = get_primary();
  pg_shard_t old_up_primary = up_primary;
  bool was_old_primary = is_primary();

  acting.swap(oldacting);
  up.swap(oldup);
  init_primary_up_acting(
    newup,
    newacting,
    new_up_primary,
    new_acting_primary);

  if (info.stats.up != up ||
      info.stats.acting != acting ||
      info.stats.up_primary != new_up_primary ||
      info.stats.acting_primary != new_acting_primary) {
    info.stats.up = up;
    info.stats.up_primary = new_up_primary;
    info.stats.acting = acting;
    info.stats.acting_primary = new_acting_primary;
    info.stats.mapping_epoch = info.history.same_interval_since;
  }

  // This will now be remapped during a backfill in cases
  // that it would not have been before.
  if (up != acting)
    state_set(PG_STATE_REMAPPED);
  else
    state_clear(PG_STATE_REMAPPED);

  int role = osdmap->calc_pg_role(osd->whoami, acting, acting.size());
  if (pool.info.is_replicated() || role == pg_whoami.shard)
    set_role(role);
  else
    set_role(-1);

  reg_next_scrub();

  // did acting, up, primary|acker change?
  if (!lastmap) {
    dout(10) << " no lastmap" << dendl;
    dirty_info = true;
    dirty_big_info = true;
  } else {
    std::stringstream debug;
    bool new_interval = pg_interval_t::check_new_interval(
      old_acting_primary.osd,
      new_acting_primary,
      oldacting, newacting,
      oldup, newup,
      info.history.same_interval_since,
      info.history.last_epoch_clean,
      osdmap,
      lastmap,
      info.pgid.pool(),
      info.pgid.pgid,
      &past_intervals,
      &debug);
    dout(10) << __func__ << ": check_new_interval output: "
	     << debug.str() << dendl;
    if (new_interval) {
      dout(10) << " noting past " << past_intervals.rbegin()->second << dendl;
      dirty_info = true;
      dirty_big_info = true;
    }
  }

  if (old_up_primary != up_primary ||
      old_acting_primary != primary ||
      oldacting != acting ||
      oldup != up ||
      is_split(lastmap, osdmap)) {
    info.history.same_interval_since = osdmap->get_epoch();
  }
  if (old_up_primary != up_primary ||
      oldup != up) {
    info.history.same_up_since = osdmap->get_epoch();
  }
  // this comparison includes primary rank via pg_shard_t
  if (old_acting_primary != get_primary()) {
    info.history.same_primary_since = osdmap->get_epoch();
  }

  dout(10) << " up " << oldup << " -> " << up 
	   << ", acting " << oldacting << " -> " << acting 
	   << ", acting_primary " << old_acting_primary << " -> " << new_acting_primary
	   << ", up_primary " << old_up_primary << " -> " << new_up_primary
	   << ", role " << oldrole << " -> " << role << dendl; 

  // deactivate.
  state_clear(PG_STATE_ACTIVE);
  state_clear(PG_STATE_DOWN);
  state_clear(PG_STATE_RECOVERY_WAIT);
  state_clear(PG_STATE_RECOVERING);

  peer_missing.clear();
  peer_purged.clear();
  actingbackfill.clear();

  // reset primary state?
  if (was_old_primary || is_primary())
    clear_primary_state();

    
  // pg->on_*
  on_change(t);

  assert(!deleting);

  // should we tell the primary we are here?
  send_notify = !is_primary();

  if (role != oldrole ||
      was_old_primary != is_primary()) {
    // did primary change?
    if (was_old_primary != is_primary()) {
      state_clear(PG_STATE_CLEAN);
      clear_publish_stats();
	
      // take replay queue waiters
      list<OpRequestRef> ls;
      for (map<eversion_t,OpRequestRef>::iterator it = replay_queue.begin();
	   it != replay_queue.end();
	   ++it)
	ls.push_back(it->second);
      replay_queue.clear();
      requeue_ops(ls);
    }

    on_role_change();

    // take active waiters
    requeue_ops(waiting_for_active);

  } else {
    // no role change.
    // did primary change?
    if (get_primary() != old_acting_primary) {    
      dout(10) << *this << " " << oldacting << " -> " << acting 
	       << ", acting primary " 
	       << old_acting_primary << " -> " << get_primary() 
	       << dendl;
    } else {
      // primary is the same.
      if (is_primary()) {
	// i am (still) primary. but my replica set changed.
	state_clear(PG_STATE_CLEAN);
	  
	dout(10) << oldacting << " -> " << acting
		 << ", replicas changed" << dendl;
      }
    }
  }
  cancel_recovery();

  if (acting.empty() && !up.empty() && up_primary == pg_whoami) {
    dout(10) << " acting empty, but i am up[0], clearing pg_temp" << dendl;
    osd->queue_want_pg_temp(info.pgid.pgid, acting);
  }
}

void PG::proc_primary_info(ObjectStore::Transaction &t, const pg_info_t &oinfo)
{
  assert(!is_primary());

  unreg_next_scrub();
  if (info.history.merge(oinfo.history))
    dirty_info = true;
  reg_next_scrub();

  if (last_complete_ondisk.epoch >= info.history.last_epoch_started) {
    // DEBUG: verify that the snaps are empty in snap_mapper
    if (cct->_conf->osd_debug_verify_snaps_on_info) {
      interval_set<snapid_t> p;
      p.union_of(oinfo.purged_snaps, info.purged_snaps);
      p.subtract(info.purged_snaps);
      if (!p.empty()) {
	for (interval_set<snapid_t>::iterator i = p.begin();
	     i != p.end();
	     ++i) {
	  for (snapid_t snap = i.get_start();
	       snap != i.get_len() + i.get_start();
	       ++snap) {
	    hobject_t hoid;
	    int r = snap_mapper.get_next_object_to_trim(snap, &hoid);
	    if (r != 0 && r != -ENOENT) {
	      derr << __func__ << ": snap_mapper get_next_object_to_trim returned "
		   << cpp_strerror(r) << dendl;
	      assert(0);
	    } else if (r != -ENOENT) {
	      derr << __func__ << ": snap_mapper get_next_object_to_trim returned "
		   << cpp_strerror(r) << " for object "
		   << hoid << " on snap " << snap
		   << " which should have been fully trimmed " << dendl;
	      assert(0);
	    }
	  }
	}
      }
    }
    info.purged_snaps = oinfo.purged_snaps;
    dirty_info = true;
    dirty_big_info = true;
  }
}

ostream& operator<<(ostream& out, const PG& pg)
{
  out << "pg[" << pg.info
      << " " << pg.up;
  if (pg.acting != pg.up)
    out << "/" << pg.acting;
  out << " r=" << pg.get_role();
  out << " lpr=" << pg.get_last_peering_reset();

  if (!pg.past_intervals.empty()) {
    out << " pi=" << pg.past_intervals.begin()->first << "-" << pg.past_intervals.rbegin()->second.last
	<< "/" << pg.past_intervals.size();
  }

  if (pg.is_active() &&
      pg.last_update_ondisk != pg.info.last_update)
    out << " luod=" << pg.last_update_ondisk;

  if (pg.recovery_ops_active)
    out << " rops=" << pg.recovery_ops_active;

  if (pg.pg_log.get_tail() != pg.info.log_tail ||
      pg.pg_log.get_head() != pg.info.last_update)
    out << " (info mismatch, " << pg.pg_log.get_log() << ")";

  if (!pg.pg_log.get_log().empty()) {
    if ((pg.pg_log.get_log().log.begin()->version <= pg.pg_log.get_tail())) {
      out << " (log bound mismatch, actual=["
	  << pg.pg_log.get_log().log.begin()->version << ","
	  << pg.pg_log.get_log().log.rbegin()->version << "]";
      out << ")";
    }
  }

  if (!pg.backfill_targets.empty())
    out << " bft=" << pg.backfill_targets;
  out << " crt=" << pg.pg_log.get_log().can_rollback_to;

  if (pg.last_complete_ondisk != pg.info.last_complete)
    out << " lcod " << pg.last_complete_ondisk;

  if (pg.is_primary()) {
    out << " mlcod " << pg.min_last_complete_ondisk;
  }

  out << " " << pg_state_string(pg.get_state());
  if (pg.should_send_notify())
    out << " NOTIFY";

  if (pg.scrubber.must_repair)
    out << " MUST_REPAIR";
  if (pg.scrubber.must_deep_scrub)
    out << " MUST_DEEP_SCRUB";
  if (pg.scrubber.must_scrub)
    out << " MUST_SCRUB";

  //out << " (" << pg.pg_log.get_tail() << "," << pg.pg_log.get_head() << "]";
  if (pg.pg_log.get_missing().num_missing()) {
    out << " m=" << pg.pg_log.get_missing().num_missing();
    if (pg.is_primary()) {
      int unfound = pg.get_num_unfound();
      if (unfound)
	out << " u=" << unfound;
    }
  }
  if (pg.snap_trimq.size())
    out << " snaptrimq=" << pg.snap_trimq;

  out << "]";


  return out;
}

bool PG::can_discard_op(OpRequestRef op)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->get_req());
  if (OSD::op_is_discardable(m)) {
    dout(20) << " discard " << *m << dendl;
    return true;
  } else if ((op->may_write() || op->may_cache()) &&
	     (!is_primary() ||
	      !same_for_modify_since(m->get_map_epoch()))) {
    osd->handle_misdirected_op(this, op);
    return true;
  } else if (op->may_read() &&
	     !same_for_read_since(m->get_map_epoch())) {
    osd->handle_misdirected_op(this, op);
    return true;
  } else if (is_replay()) {
    if (m->get_version().version > 0) {
      dout(7) << " queueing replay at " << m->get_version()
	      << " for " << *m << dendl;
      replay_queue[m->get_version()] = op;
      op->mark_delayed("waiting for replay");
      return true;
    }
  }
  return false;
}

template<typename T, int MSGTYPE>
bool PG::can_discard_replica_op(OpRequestRef op)
{
  T *m = static_cast<T *>(op->get_req());
  assert(m->get_header().type == MSGTYPE);

  /* Mostly, this overlaps with the old_peering_msg
   * condition.  An important exception is pushes
   * sent by replicas not in the acting set, since
   * if such a replica goes down it does not cause
   * a new interval. */
  int from = m->get_source().num();
  if (get_osdmap()->get_down_at(from) >= m->map_epoch)
    return true;

  // same pg?
  //  if pg changes _at all_, we reset and repeer!
  if (old_peering_msg(m->map_epoch, m->map_epoch)) {
    dout(10) << "can_discard_replica_op pg changed " << info.history
	     << " after " << m->map_epoch
	     << ", dropping" << dendl;
    return true;
  }
  return false;
}

bool PG::can_discard_scan(OpRequestRef op)
{
  MOSDPGScan *m = static_cast<MOSDPGScan *>(op->get_req());
  assert(m->get_header().type == MSG_OSD_PG_SCAN);

  if (old_peering_msg(m->map_epoch, m->query_epoch)) {
    dout(10) << " got old scan, ignoring" << dendl;
    return true;
  }
  return false;
}

bool PG::can_discard_backfill(OpRequestRef op)
{
  MOSDPGBackfill *m = static_cast<MOSDPGBackfill *>(op->get_req());
  assert(m->get_header().type == MSG_OSD_PG_BACKFILL);

  if (old_peering_msg(m->map_epoch, m->query_epoch)) {
    dout(10) << " got old backfill, ignoring" << dendl;
    return true;
  }

  return false;

}

bool PG::can_discard_request(OpRequestRef op)
{
  switch (op->get_req()->get_type()) {
  case CEPH_MSG_OSD_OP:
    return can_discard_op(op);
  case MSG_OSD_SUBOP:
    return can_discard_replica_op<MOSDSubOp, MSG_OSD_SUBOP>(op);
  case MSG_OSD_PG_PUSH:
    return can_discard_replica_op<MOSDPGPush, MSG_OSD_PG_PUSH>(op);
  case MSG_OSD_PG_PULL:
    return can_discard_replica_op<MOSDPGPull, MSG_OSD_PG_PULL>(op);
  case MSG_OSD_PG_PUSH_REPLY:
    return can_discard_replica_op<MOSDPGPushReply, MSG_OSD_PG_PUSH_REPLY>(op);
  case MSG_OSD_SUBOPREPLY:
    return false;

  case MSG_OSD_EC_WRITE:
    return can_discard_replica_op<MOSDECSubOpWrite, MSG_OSD_EC_WRITE>(op);
  case MSG_OSD_EC_WRITE_REPLY:
    return can_discard_replica_op<MOSDECSubOpWriteReply, MSG_OSD_EC_WRITE_REPLY>(op);
  case MSG_OSD_EC_READ:
    return can_discard_replica_op<MOSDECSubOpRead, MSG_OSD_EC_READ>(op);
  case MSG_OSD_EC_READ_REPLY:
    return can_discard_replica_op<MOSDECSubOpReadReply, MSG_OSD_EC_READ_REPLY>(op);

  case MSG_OSD_PG_SCAN:
    return can_discard_scan(op);
  case MSG_OSD_PG_BACKFILL:
    return can_discard_backfill(op);
  }
  return true;
}

bool PG::split_request(OpRequestRef op, unsigned match, unsigned bits)
{
  unsigned mask = ~((~0)<<bits);
  switch (op->get_req()->get_type()) {
  case CEPH_MSG_OSD_OP:
    return (static_cast<MOSDOp*>(op->get_req())->get_pg().m_seed & mask) == match;
  }
  return false;
}

bool PG::op_must_wait_for_map(OSDMapRef curmap, OpRequestRef op)
{
  switch (op->get_req()->get_type()) {
  case CEPH_MSG_OSD_OP:
    return !have_same_or_newer_map(
      curmap,
      static_cast<MOSDOp*>(op->get_req())->get_map_epoch());

  case MSG_OSD_SUBOP:
    return !have_same_or_newer_map(
      curmap,
      static_cast<MOSDSubOp*>(op->get_req())->map_epoch);

  case MSG_OSD_SUBOPREPLY:
    return !have_same_or_newer_map(
      curmap,
      static_cast<MOSDSubOpReply*>(op->get_req())->map_epoch);

  case MSG_OSD_PG_SCAN:
    return !have_same_or_newer_map(
      curmap,
      static_cast<MOSDPGScan*>(op->get_req())->map_epoch);

  case MSG_OSD_PG_BACKFILL:
    return !have_same_or_newer_map(
      curmap,
      static_cast<MOSDPGBackfill*>(op->get_req())->map_epoch);

  case MSG_OSD_PG_PUSH:
    return !have_same_or_newer_map(
      curmap,
      static_cast<MOSDPGPush*>(op->get_req())->map_epoch);

  case MSG_OSD_PG_PULL:
    return !have_same_or_newer_map(
      curmap,
      static_cast<MOSDPGPull*>(op->get_req())->map_epoch);

  case MSG_OSD_PG_PUSH_REPLY:
    return !have_same_or_newer_map(
      curmap,
      static_cast<MOSDPGPushReply*>(op->get_req())->map_epoch);

  case MSG_OSD_EC_WRITE:
    return !have_same_or_newer_map(
      curmap,
      static_cast<MOSDECSubOpWrite*>(op->get_req())->map_epoch);

  case MSG_OSD_EC_WRITE_REPLY:
    return !have_same_or_newer_map(
      curmap,
      static_cast<MOSDECSubOpWriteReply*>(op->get_req())->map_epoch);

  case MSG_OSD_EC_READ:
    return !have_same_or_newer_map(
      curmap,
      static_cast<MOSDECSubOpRead*>(op->get_req())->map_epoch);

  case MSG_OSD_EC_READ_REPLY:
    return !have_same_or_newer_map(
      curmap,
      static_cast<MOSDECSubOpReadReply*>(op->get_req())->map_epoch);
  }
  assert(0);
  return false;
}

void PG::take_waiters()
{
  dout(10) << "take_waiters" << dendl;
  take_op_map_waiters();
  for (list<CephPeeringEvtRef>::iterator i = peering_waiters.begin();
       i != peering_waiters.end();
       ++i) osd->queue_for_peering(this);
  peering_queue.splice(peering_queue.begin(), peering_waiters,
		       peering_waiters.begin(), peering_waiters.end());
}

void PG::handle_peering_event(CephPeeringEvtRef evt, RecoveryCtx *rctx)
{
  dout(10) << "handle_peering_event: " << evt->get_desc() << dendl;
  if (!have_same_or_newer_map(evt->get_epoch_sent())) {
    dout(10) << "deferring event " << evt->get_desc() << dendl;
    peering_waiters.push_back(evt);
    return;
  }
  if (old_peering_evt(evt))
    return;
  recovery_state.handle_event(evt, rctx);
}

void PG::queue_peering_event(CephPeeringEvtRef evt)
{
  if (old_peering_evt(evt))
    return;
  peering_queue.push_back(evt);
  osd->queue_for_peering(this);
}

void PG::queue_notify(epoch_t msg_epoch,
		      epoch_t query_epoch,
		      pg_shard_t from, pg_notify_t& i)
{
  dout(10) << "notify " << i << " from replica " << from << dendl;
  queue_peering_event(
    CephPeeringEvtRef(new CephPeeringEvt(msg_epoch, query_epoch,
					 MNotifyRec(from, i))));
}

void PG::queue_info(epoch_t msg_epoch,
		     epoch_t query_epoch,
		     pg_shard_t from, pg_info_t& i)
{
  dout(10) << "info " << i << " from replica " << from << dendl;
  queue_peering_event(
    CephPeeringEvtRef(new CephPeeringEvt(msg_epoch, query_epoch,
					 MInfoRec(from, i, msg_epoch))));
}

void PG::queue_log(epoch_t msg_epoch,
		   epoch_t query_epoch,
		   pg_shard_t from,
		   MOSDPGLog *msg)
{
  dout(10) << "log " << *msg << " from replica " << from << dendl;
  queue_peering_event(
    CephPeeringEvtRef(new CephPeeringEvt(msg_epoch, query_epoch,
					 MLogRec(from, msg))));
}

void PG::queue_null(epoch_t msg_epoch,
		    epoch_t query_epoch)
{
  dout(10) << "null" << dendl;
  queue_peering_event(
    CephPeeringEvtRef(new CephPeeringEvt(msg_epoch, query_epoch,
					 NullEvt())));
}

void PG::queue_flushed(epoch_t e)
{
  dout(10) << "flushed" << dendl;
  queue_peering_event(
    CephPeeringEvtRef(new CephPeeringEvt(e, e,
					 FlushedEvt())));
}

void PG::queue_query(epoch_t msg_epoch,
		     epoch_t query_epoch,
		     pg_shard_t from, const pg_query_t& q)
{
  dout(10) << "handle_query " << q << " from replica " << from << dendl;
  queue_peering_event(
    CephPeeringEvtRef(new CephPeeringEvt(msg_epoch, query_epoch,
					 MQuery(from, q, query_epoch))));
}

void PG::handle_advance_map(
  OSDMapRef osdmap, OSDMapRef lastmap,
  vector<int>& newup, int up_primary,
  vector<int>& newacting, int acting_primary,
  RecoveryCtx *rctx)
{
  assert(lastmap->get_epoch() == osdmap_ref->get_epoch());
  assert(lastmap == osdmap_ref);
  dout(10) << "handle_advance_map "
	   << newup << "/" << newacting
	   << " -- " << up_primary << "/" << acting_primary
	   << dendl;
  update_osdmap_ref(osdmap);
  pool.update(osdmap);
  if (pool.info.last_change == osdmap_ref->get_epoch())
    on_pool_change();
  AdvMap evt(
    osdmap, lastmap, newup, up_primary,
    newacting, acting_primary);
  recovery_state.handle_event(evt, rctx);
}

void PG::handle_activate_map(RecoveryCtx *rctx)
{
  dout(10) << "handle_activate_map " << dendl;
  ActMap evt;
  recovery_state.handle_event(evt, rctx);
  if (osdmap_ref->get_epoch() - last_persisted_osdmap_ref->get_epoch() >
    cct->_conf->osd_pg_epoch_persisted_max_stale) {
    dout(20) << __func__ << ": Dirtying info: last_persisted is "
	     << last_persisted_osdmap_ref->get_epoch()
	     << " while current is " << osdmap_ref->get_epoch() << dendl;
    dirty_info = true;
  } else {
    dout(20) << __func__ << ": Not dirtying info: last_persisted is "
	     << last_persisted_osdmap_ref->get_epoch()
	     << " while current is " << osdmap_ref->get_epoch() << dendl;
  }
  if (osdmap_ref->check_new_blacklist_entries()) check_blacklisted_watchers();
}

void PG::handle_loaded(RecoveryCtx *rctx)
{
  dout(10) << "handle_loaded" << dendl;
  Load evt;
  recovery_state.handle_event(evt, rctx);
}

void PG::handle_create(RecoveryCtx *rctx)
{
  dout(10) << "handle_create" << dendl;
  Initialize evt;
  recovery_state.handle_event(evt, rctx);
  ActMap evt2;
  recovery_state.handle_event(evt2, rctx);
}

void PG::handle_query_state(Formatter *f)
{
  dout(10) << "handle_query_state" << dendl;
  QueryState q(f);
  recovery_state.handle_event(q, 0);
}



std::ostream& operator<<(std::ostream& oss,
			 const struct PG::PriorSet &prior)
{
  oss << "PriorSet[probe=" << prior.probe << " "
      << "down=" << prior.down << " "
      << "blocked_by=" << prior.blocked_by << "]";
  return oss;
}

/*------------ Recovery State Machine----------------*/
#undef dout_prefix
#define dout_prefix (*_dout << context< RecoveryMachine >().pg->gen_prefix() \
		     << "state<" << get_state_name() << ">: ")

/*------Crashed-------*/
PG::RecoveryState::Crashed::Crashed(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Crashed")
{
  context< RecoveryMachine >().log_enter(state_name);
  assert(0 == "we got a bad state machine event");
}


/*------Initial-------*/
PG::RecoveryState::Initial::Initial(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Initial")
{
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result PG::RecoveryState::Initial::react(const Load& l)
{
  PG *pg = context< RecoveryMachine >().pg;

  // do we tell someone we're here?
  pg->send_notify = (!pg->is_primary());

  return transit< Reset >();
}

boost::statechart::result PG::RecoveryState::Initial::react(const MNotifyRec& notify)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->proc_replica_info(notify.from, notify.notify.info);
  pg->update_heartbeat_peers();
  pg->set_last_peering_reset();
  return transit< Primary >();
}

boost::statechart::result PG::RecoveryState::Initial::react(const MInfoRec& i)
{
  PG *pg = context< RecoveryMachine >().pg;
  assert(!pg->is_primary());
  post_event(i);
  return transit< Stray >();
}

boost::statechart::result PG::RecoveryState::Initial::react(const MLogRec& i)
{
  PG *pg = context< RecoveryMachine >().pg;
  assert(!pg->is_primary());
  post_event(i);
  return transit< Stray >();
}

void PG::RecoveryState::Initial::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_initial_latency, dur);
}

/*------Started-------*/
PG::RecoveryState::Started::Started(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started")
{
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result
PG::RecoveryState::Started::react(const FlushedEvt&)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->on_flushed();
  return discard_event();
}


boost::statechart::result PG::RecoveryState::Started::react(const AdvMap& advmap)
{
  dout(10) << "Started advmap" << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  if (pg->acting_up_affected(
	advmap.up_primary,
	advmap.acting_primary,
	advmap.newup,
	advmap.newacting) ||
      pg->is_split(advmap.lastmap, advmap.osdmap)) {
    dout(10) << "up or acting affected, transitioning to Reset" << dendl;
    post_event(advmap);
    return transit< Reset >();
  }
  pg->remove_down_peer_info(advmap.osdmap);
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Started::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->close_section();
  return discard_event();
}

void PG::RecoveryState::Started::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_started_latency, dur);
}

/*--------Reset---------*/
PG::RecoveryState::Reset::Reset(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Reset")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  pg->flushes_in_progress = 0;
  pg->set_last_peering_reset();
}

boost::statechart::result
PG::RecoveryState::Reset::react(const FlushedEvt&)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->on_flushed();
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Reset::react(const AdvMap& advmap)
{
  PG *pg = context< RecoveryMachine >().pg;
  dout(10) << "Reset advmap" << dendl;

  // make sure we have past_intervals filled in.  hopefully this will happen
  // _before_ we are active.
  pg->generate_past_intervals();

  if (pg->acting_up_affected(
	advmap.up_primary,
	advmap.acting_primary,
	advmap.newup,
	advmap.newacting) ||
      pg->is_split(advmap.lastmap, advmap.osdmap)) {
    dout(10) << "up or acting affected, calling start_peering_interval again"
	     << dendl;
    pg->start_peering_interval(
      advmap.lastmap,
      advmap.newup, advmap.up_primary,
      advmap.newacting, advmap.acting_primary,
      context< RecoveryMachine >().get_cur_transaction());
  }
  pg->remove_down_peer_info(advmap.osdmap);
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Reset::react(const ActMap&)
{
  PG *pg = context< RecoveryMachine >().pg;
  if (pg->should_send_notify() && pg->get_primary().osd >= 0) {
    context< RecoveryMachine >().send_notify(
      pg->get_primary(),
      pg_notify_t(
	pg->get_primary().shard, pg->pg_whoami.shard,
	pg->get_osdmap()->get_epoch(),
	pg->get_osdmap()->get_epoch(),
	pg->info),
      pg->past_intervals);
  }

  pg->update_heartbeat_peers();
  pg->take_waiters();

  return transit< Started >();
}

boost::statechart::result PG::RecoveryState::Reset::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->close_section();
  return discard_event();
}

void PG::RecoveryState::Reset::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_reset_latency, dur);
}

/*-------Start---------*/
PG::RecoveryState::Start::Start(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Start")
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  if (pg->is_primary()) {
    dout(1) << "transitioning to Primary" << dendl;
    post_event(MakePrimary());
  } else { //is_stray
    dout(1) << "transitioning to Stray" << dendl; 
    post_event(MakeStray());
  }
}

void PG::RecoveryState::Start::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_start_latency, dur);
}

/*---------Primary--------*/
PG::RecoveryState::Primary::Primary(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  assert(pg->want_acting.empty());
}

boost::statechart::result PG::RecoveryState::Primary::react(const MNotifyRec& notevt)
{
  dout(7) << "handle_pg_notify from osd." << notevt.from << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  if (pg->peer_info.count(notevt.from) &&
      pg->peer_info[notevt.from].last_update == notevt.notify.info.last_update) {
    dout(10) << *pg << " got dup osd." << notevt.from << " info " << notevt.notify.info
	     << ", identical to ours" << dendl;
  } else {
    pg->proc_replica_info(notevt.from, notevt.notify.info);
  }
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Primary::react(const ActMap&)
{
  dout(7) << "handle ActMap primary" << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  pg->publish_stats_to_osd();
  pg->take_waiters();
  return discard_event();
}

void PG::RecoveryState::Primary::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->want_acting.clear();
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_primary_latency, dur);
}

/*---------Peering--------*/
PG::RecoveryState::Peering::Peering(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Peering")
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  assert(!pg->is_active());
  assert(!pg->is_peering());
  assert(pg->is_primary());
  pg->state_set(PG_STATE_PEERING);
}

boost::statechart::result PG::RecoveryState::Peering::react(const AdvMap& advmap) 
{
  PG *pg = context< RecoveryMachine >().pg;
  dout(10) << "Peering advmap" << dendl;
  if (prior_set.get()->affected_by_map(advmap.osdmap, pg)) {
    dout(1) << "Peering, affected_by_map, going to Reset" << dendl;
    post_event(advmap);
    return transit< Reset >();
  }
  
  pg->adjust_need_up_thru(advmap.osdmap);
  
  return forward_event();
}

boost::statechart::result PG::RecoveryState::Peering::react(const QueryState& q)
{
  PG *pg = context< RecoveryMachine >().pg;

  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  q.f->open_array_section("past_intervals");
  for (map<epoch_t,pg_interval_t>::iterator p = pg->past_intervals.begin();
       p != pg->past_intervals.end();
       ++p) {
    q.f->open_object_section("past_interval");
    p->second.dump(q.f);
    q.f->close_section();
  }
  q.f->close_section();

  q.f->open_array_section("probing_osds");
  for (set<pg_shard_t>::iterator p = prior_set->probe.begin();
       p != prior_set->probe.end();
       ++p)
    q.f->dump_stream("osd") << *p;
  q.f->close_section();

  if (prior_set->pg_down)
    q.f->dump_string("blocked", "peering is blocked due to down osds");

  q.f->open_array_section("down_osds_we_would_probe");
  for (set<int>::iterator p = prior_set->down.begin();
       p != prior_set->down.end();
       ++p)
    q.f->dump_int("osd", *p);
  q.f->close_section();

  q.f->open_array_section("peering_blocked_by");
  for (map<int,epoch_t>::iterator p = prior_set->blocked_by.begin();
       p != prior_set->blocked_by.end();
       ++p) {
    q.f->open_object_section("osd");
    q.f->dump_int("osd", p->first);
    q.f->dump_int("current_lost_at", p->second);
    q.f->dump_string("comment", "starting or marking this osd lost may let us proceed");
    q.f->close_section();
  }
  q.f->close_section();

  q.f->close_section();
  return forward_event();
}

void PG::RecoveryState::Peering::exit()
{
  dout(10) << "Leaving Peering" << dendl;
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_PEERING);
  pg->clear_probe_targets();

  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_peering_latency, dur);
}


/*------Backfilling-------*/
PG::RecoveryState::Backfilling::Backfilling(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Active/Backfilling")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  pg->backfill_reserved = true;
  pg->osd->queue_for_recovery(pg);
  pg->state_clear(PG_STATE_BACKFILL_TOOFULL);
  pg->state_clear(PG_STATE_BACKFILL_WAIT);
  pg->state_set(PG_STATE_BACKFILL);
}

boost::statechart::result
PG::RecoveryState::Backfilling::react(const RemoteReservationRejected &)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->local_reserver.cancel_reservation(pg->info.pgid);
  pg->state_set(PG_STATE_BACKFILL_TOOFULL);

  pg->osd->recovery_wq.dequeue(pg);

  pg->schedule_backfill_full_retry();
  return transit<NotBackfilling>();
}

void PG::RecoveryState::Backfilling::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->backfill_reserved = false;
  pg->backfill_reserving = false;
  pg->state_clear(PG_STATE_BACKFILL);
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_backfilling_latency, dur);
}

/*--WaitRemoteBackfillReserved--*/

PG::RecoveryState::WaitRemoteBackfillReserved::WaitRemoteBackfillReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Active/WaitRemoteBackfillReserved"),
    backfill_osd_it(context< Active >().sorted_backfill_set.begin())
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_set(PG_STATE_BACKFILL_WAIT);
  post_event(RemoteBackfillReserved());
}

boost::statechart::result
PG::RecoveryState::WaitRemoteBackfillReserved::react(const RemoteBackfillReserved &evt)
{
  PG *pg = context< RecoveryMachine >().pg;

  if (backfill_osd_it != context< Active >().sorted_backfill_set.end()) {
    //The primary never backfills itself
    assert(*backfill_osd_it != pg->pg_whoami);
    ConnectionRef con = pg->osd->get_con_osd_cluster(
      backfill_osd_it->osd, pg->get_osdmap()->get_epoch());
    if (con) {
      if (con->has_feature(CEPH_FEATURE_BACKFILL_RESERVATION)) {
        unsigned priority = pg->is_degraded() ? OSDService::BACKFILL_HIGH
	  : OSDService::BACKFILL_LOW;
        pg->osd->send_message_osd_cluster(
          new MBackfillReserve(
	  MBackfillReserve::REQUEST,
	  spg_t(pg->info.pgid.pgid, backfill_osd_it->shard),
	  pg->get_osdmap()->get_epoch(), priority),
	con.get());
      } else {
        post_event(RemoteBackfillReserved());
      }
    }
    ++backfill_osd_it;
  } else {
    post_event(AllBackfillsReserved());
  }
  return discard_event();
}

void PG::RecoveryState::WaitRemoteBackfillReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_waitremotebackfillreserved_latency, dur);
}

boost::statechart::result
PG::RecoveryState::WaitRemoteBackfillReserved::react(const RemoteReservationRejected &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->local_reserver.cancel_reservation(pg->info.pgid);

  // Send REJECT to all previously acquired reservations
  set<pg_shard_t>::const_iterator it, begin, end, next;
  begin = context< Active >().sorted_backfill_set.begin();
  end = context< Active >().sorted_backfill_set.end();
  assert(begin != end);
  for (next = it = begin, ++next ; next != backfill_osd_it; ++it, ++next) {
    //The primary never backfills itself
    assert(*it != pg->pg_whoami);
    ConnectionRef con = pg->osd->get_con_osd_cluster(
      it->osd, pg->get_osdmap()->get_epoch());
    if (con) {
      if (con->has_feature(CEPH_FEATURE_BACKFILL_RESERVATION)) {
        pg->osd->send_message_osd_cluster(
          new MBackfillReserve(
	  MBackfillReserve::REJECT,
	  spg_t(pg->info.pgid.pgid, it->shard),
	  pg->get_osdmap()->get_epoch()),
	con.get());
      }
    }
  }

  pg->state_clear(PG_STATE_BACKFILL_WAIT);
  pg->state_set(PG_STATE_BACKFILL_TOOFULL);

  pg->schedule_backfill_full_retry();

  return transit<NotBackfilling>();
}

/*--WaitLocalBackfillReserved--*/
PG::RecoveryState::WaitLocalBackfillReserved::WaitLocalBackfillReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Active/WaitLocalBackfillReserved")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_set(PG_STATE_BACKFILL_WAIT);
  pg->osd->local_reserver.request_reservation(
    pg->info.pgid,
    new QueuePeeringEvt<LocalBackfillReserved>(
      pg, pg->get_osdmap()->get_epoch(),
      LocalBackfillReserved()), pg->is_degraded() ? OSDService::BACKFILL_HIGH
	 : OSDService::BACKFILL_LOW);
}

void PG::RecoveryState::WaitLocalBackfillReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_waitlocalbackfillreserved_latency, dur);
}

/*----NotBackfilling------*/
PG::RecoveryState::NotBackfilling::NotBackfilling(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Active/NotBackfilling")
{
  context< RecoveryMachine >().log_enter(state_name);
}

void PG::RecoveryState::NotBackfilling::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_notbackfilling_latency, dur);
}

/*---RepNotRecovering----*/
PG::RecoveryState::RepNotRecovering::RepNotRecovering(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/ReplicaActive/RepNotRecovering")
{
  context< RecoveryMachine >().log_enter(state_name);
}

void PG::RecoveryState::RepNotRecovering::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_repnotrecovering_latency, dur);
}

/*---RepWaitRecoveryReserved--*/
PG::RecoveryState::RepWaitRecoveryReserved::RepWaitRecoveryReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/ReplicaActive/RepWaitRecoveryReserved")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;

  pg->osd->remote_reserver.request_reservation(
    pg->info.pgid,
    new QueuePeeringEvt<RemoteRecoveryReserved>(
      pg, pg->get_osdmap()->get_epoch(),
      RemoteRecoveryReserved()), OSDService::RECOVERY);
}

boost::statechart::result
PG::RecoveryState::RepWaitRecoveryReserved::react(const RemoteRecoveryReserved &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->send_message_osd_cluster(
    pg->primary.osd,
    new MRecoveryReserve(
      MRecoveryReserve::GRANT,
      spg_t(pg->info.pgid.pgid, pg->primary.shard),
      pg->get_osdmap()->get_epoch()),
    pg->get_osdmap()->get_epoch());
  return transit<RepRecovering>();
}

void PG::RecoveryState::RepWaitRecoveryReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_repwaitrecoveryreserved_latency, dur);
}

/*-RepWaitBackfillReserved*/
PG::RecoveryState::RepWaitBackfillReserved::RepWaitBackfillReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/ReplicaActive/RepWaitBackfillReserved")
{
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result 
PG::RecoveryState::RepNotRecovering::react(const RequestBackfillPrio &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  double ratio, max_ratio;

  if (g_conf->osd_debug_reject_backfill_probability > 0 &&
      (rand()%1000 < (g_conf->osd_debug_reject_backfill_probability*1000.0))) {
    dout(10) << "backfill reservation rejected: failure injection" << dendl;
    post_event(RemoteReservationRejected());
  } else if (pg->osd->too_full_for_backfill(&ratio, &max_ratio) &&
      !pg->cct->_conf->osd_debug_skip_full_check_in_backfill_reservation) {
    dout(10) << "backfill reservation rejected: full ratio is "
	     << ratio << ", which is greater than max allowed ratio "
	     << max_ratio << dendl;
    post_event(RemoteReservationRejected());
  } else {
    pg->osd->remote_reserver.request_reservation(
      pg->info.pgid,
      new QueuePeeringEvt<RemoteBackfillReserved>(
        pg, pg->get_osdmap()->get_epoch(),
        RemoteBackfillReserved()), evt.priority);
  }
  return transit<RepWaitBackfillReserved>();
}

void PG::RecoveryState::RepWaitBackfillReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_repwaitbackfillreserved_latency, dur);
}

boost::statechart::result
PG::RecoveryState::RepWaitBackfillReserved::react(const RemoteBackfillReserved &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->send_message_osd_cluster(
    pg->primary.osd,
    new MBackfillReserve(
      MBackfillReserve::GRANT,
      spg_t(pg->info.pgid.pgid, pg->primary.shard),
      pg->get_osdmap()->get_epoch()),
    pg->get_osdmap()->get_epoch());
  return transit<RepRecovering>();
}

boost::statechart::result
PG::RecoveryState::RepWaitBackfillReserved::react(const RemoteReservationRejected &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->reject_reservation();
  return transit<RepNotRecovering>();
}

/*---RepRecovering-------*/
PG::RecoveryState::RepRecovering::RepRecovering(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/ReplicaActive/RepRecovering")
{
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result
PG::RecoveryState::RepRecovering::react(const BackfillTooFull &)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->reject_reservation();
  return transit<RepNotRecovering>();
}

void PG::RecoveryState::RepRecovering::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->remote_reserver.cancel_reservation(pg->info.pgid);
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_RepRecovering_latency, dur);
}

/*------Activating--------*/
PG::RecoveryState::Activating::Activating(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Active/Activating")
{
  context< RecoveryMachine >().log_enter(state_name);
}

void PG::RecoveryState::Activating::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_activating_latency, dur);
}

PG::RecoveryState::WaitLocalRecoveryReserved::WaitLocalRecoveryReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Active/WaitLocalRecoveryReserved")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_set(PG_STATE_RECOVERY_WAIT);
  pg->osd->local_reserver.request_reservation(
    pg->info.pgid,
    new QueuePeeringEvt<LocalRecoveryReserved>(
      pg, pg->get_osdmap()->get_epoch(),
      LocalRecoveryReserved()), OSDService::RECOVERY);
}

void PG::RecoveryState::WaitLocalRecoveryReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_waitlocalrecoveryreserved_latency, dur);
}

PG::RecoveryState::WaitRemoteRecoveryReserved::WaitRemoteRecoveryReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Active/WaitRemoteRecoveryReserved"),
    acting_osd_it(context< Active >().sorted_actingbackfill_set.begin())
{
  context< RecoveryMachine >().log_enter(state_name);
  post_event(RemoteRecoveryReserved());
}

boost::statechart::result
PG::RecoveryState::WaitRemoteRecoveryReserved::react(const RemoteRecoveryReserved &evt) {
  PG *pg = context< RecoveryMachine >().pg;

  if (acting_osd_it != context< Active >().sorted_actingbackfill_set.end()) {
    // skip myself
    if (*acting_osd_it == pg->pg_whoami)
      ++acting_osd_it;
  }

  if (acting_osd_it != context< Active >().sorted_actingbackfill_set.end()) {
    ConnectionRef con = pg->osd->get_con_osd_cluster(
      acting_osd_it->osd, pg->get_osdmap()->get_epoch());
    if (con) {
      if (con->has_feature(CEPH_FEATURE_RECOVERY_RESERVATION)) {
	pg->osd->send_message_osd_cluster(
          new MRecoveryReserve(
	    MRecoveryReserve::REQUEST,
	    spg_t(pg->info.pgid.pgid, acting_osd_it->shard),
	    pg->get_osdmap()->get_epoch()),
	  con.get());
      } else {
	post_event(RemoteRecoveryReserved());
      }
    }
    ++acting_osd_it;
  } else {
    post_event(AllRemotesReserved());
  }
  return discard_event();
}

void PG::RecoveryState::WaitRemoteRecoveryReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_waitremoterecoveryreserved_latency, dur);
}

PG::RecoveryState::Recovering::Recovering(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Active/Recovering")
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_RECOVERY_WAIT);
  pg->state_set(PG_STATE_RECOVERING);
  pg->osd->queue_for_recovery(pg);
}

void PG::RecoveryState::Recovering::release_reservations()
{
  PG *pg = context< RecoveryMachine >().pg;
  assert(!pg->pg_log.get_missing().have_missing());

  // release remote reservations
  for (set<pg_shard_t>::const_iterator i =
	 context< Active >().sorted_actingbackfill_set.begin();
        i != context< Active >().sorted_actingbackfill_set.end();
        ++i) {
    if (*i == pg->pg_whoami) // skip myself
      continue;
    ConnectionRef con = pg->osd->get_con_osd_cluster(
      i->osd, pg->get_osdmap()->get_epoch());
    if (con) {
      if (con->has_feature(CEPH_FEATURE_RECOVERY_RESERVATION)) {
	pg->osd->send_message_osd_cluster(
          new MRecoveryReserve(
	    MRecoveryReserve::RELEASE,
	    spg_t(pg->info.pgid.pgid, i->shard),
	    pg->get_osdmap()->get_epoch()),
	  con.get());
      }
    }
  }
}

boost::statechart::result
PG::RecoveryState::Recovering::react(const AllReplicasRecovered &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_RECOVERING);
  release_reservations();
  return transit<Recovered>();
}

boost::statechart::result
PG::RecoveryState::Recovering::react(const RequestBackfill &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_RECOVERING);
  release_reservations();
  return transit<WaitRemoteBackfillReserved>();
}

void PG::RecoveryState::Recovering::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_recovering_latency, dur);
}

PG::RecoveryState::Recovered::Recovered(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Active/Recovered")
{
  pg_shard_t auth_log_shard;

  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->local_reserver.cancel_reservation(pg->info.pgid);

  // if we finished backfill, all acting are active; recheck if
  // DEGRADED is appropriate.
  assert(!pg->actingbackfill.empty());
  if (pg->get_osdmap()->get_pg_size(pg->info.pgid.pgid) <=
      pg->actingbackfill.size())
    pg->state_clear(PG_STATE_DEGRADED);

  // adjust acting set?  (e.g. because backfill completed...)
  if (pg->acting != pg->up && !pg->choose_acting(auth_log_shard))
    assert(pg->want_acting.size());

  assert(!pg->needs_recovery());

  if (context< Active >().all_replicas_activated)
    post_event(GoClean());
}

void PG::RecoveryState::Recovered::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_recovered_latency, dur);
}

PG::RecoveryState::Clean::Clean(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Active/Clean")
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;

  if (pg->info.last_complete != pg->info.last_update) {
    assert(0);
  }
  pg->finish_recovery(*context< RecoveryMachine >().get_on_safe_context_list());
  pg->mark_clean();

  pg->share_pg_info();
  pg->publish_stats_to_osd();

}

void PG::RecoveryState::Clean::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_CLEAN);
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_clean_latency, dur);
}

/*---------Active---------*/
PG::RecoveryState::Active::Active(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Active"),
    sorted_actingbackfill_set(
      context< RecoveryMachine >().pg->actingbackfill.begin(),
      context< RecoveryMachine >().pg->actingbackfill.end()),
    sorted_backfill_set(
      context< RecoveryMachine >().pg->backfill_targets.begin(),
      context< RecoveryMachine >().pg->backfill_targets.end()),
    all_replicas_activated(false)
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;

  assert(!pg->backfill_reserving);
  assert(!pg->backfill_reserved);
  assert(pg->is_primary());
  dout(10) << "In Active, about to call activate" << dendl;
  pg->start_flush(
    context< RecoveryMachine >().get_cur_transaction(),
    context< RecoveryMachine >().get_on_applied_context_list(),
    context< RecoveryMachine >().get_on_safe_context_list());
  pg->activate(*context< RecoveryMachine >().get_cur_transaction(),
	       pg->get_osdmap()->get_epoch(),
	       *context< RecoveryMachine >().get_on_safe_context_list(),
	       *context< RecoveryMachine >().get_query_map(),
	       context< RecoveryMachine >().get_info_map(),
	       context< RecoveryMachine >().get_recovery_ctx());
  dout(10) << "Activate Finished" << dendl;
}

boost::statechart::result PG::RecoveryState::Active::react(const AdvMap& advmap)
{
  PG *pg = context< RecoveryMachine >().pg;
  dout(10) << "Active advmap" << dendl;
  if (!pg->pool.newly_removed_snaps.empty()) {
    pg->snap_trimq.union_of(pg->pool.newly_removed_snaps);
    dout(10) << *pg << " snap_trimq now " << pg->snap_trimq << dendl;
    pg->dirty_info = true;
    pg->dirty_big_info = true;
  }

  for (vector<int>::iterator p = pg->want_acting.begin();
       p != pg->want_acting.end(); ++p) {
    if (!advmap.osdmap->is_up(*p)) {
      assert((std::find(pg->acting.begin(), pg->acting.end(), *p) !=
	      pg->acting.end()) ||
	     (std::find(pg->up.begin(), pg->up.end(), *p) !=
	      pg->up.end()));
    }
  }

  /* Check for changes in pool size (if the acting set changed as a result,
   * this does not matter) */
  if (advmap.lastmap->get_pg_size(pg->info.pgid.pgid) !=
      pg->get_osdmap()->get_pg_size(pg->info.pgid.pgid)) {
    if (pg->get_osdmap()->get_pg_size(pg->info.pgid.pgid) <= pg->acting.size())
      pg->state_clear(PG_STATE_DEGRADED);
    else
      pg->state_set(PG_STATE_DEGRADED);
    pg->publish_stats_to_osd(); // degraded may have changed
  }

  // if we haven't reported our PG stats in a long time, do so now.
  if (pg->info.stats.reported_epoch + pg->cct->_conf->osd_pg_stat_report_interval_max < advmap.osdmap->get_epoch()) {
    dout(20) << "reporting stats to osd after " << (advmap.osdmap->get_epoch() - pg->info.stats.reported_epoch)
	     << " epochs" << dendl;
    pg->publish_stats_to_osd();
  }

  return forward_event();
}
    
boost::statechart::result PG::RecoveryState::Active::react(const ActMap&)
{
  PG *pg = context< RecoveryMachine >().pg;
  dout(10) << "Active: handling ActMap" << dendl;
  assert(pg->is_primary());

  if (pg->have_unfound()) {
    // object may have become unfound
    pg->discover_all_missing(*context< RecoveryMachine >().get_query_map());
  }

  if (pg->cct->_conf->osd_check_for_log_corruption)
    pg->check_log_for_corruption(pg->osd->store);

  int unfound = pg->missing_loc.num_unfound();
  if (unfound > 0 &&
      pg->all_unfound_are_queried_or_lost(pg->get_osdmap())) {
    if (pg->cct->_conf->osd_auto_mark_unfound_lost) {
      pg->osd->clog.error() << pg->info.pgid << " has " << unfound
			    << " objects unfound and apparently lost, would automatically marking lost but NOT IMPLEMENTED\n";
      //pg->mark_all_unfound_lost(*context< RecoveryMachine >().get_cur_transaction());
    } else
      pg->osd->clog.error() << pg->info.pgid << " has " << unfound << " objects unfound and apparently lost\n";
  }

  if (!pg->snap_trimq.empty() &&
      pg->is_clean()) {
    dout(10) << "Active: queuing snap trim" << dendl;
    pg->queue_snap_trim();
  }

  if (!pg->is_clean() &&
      !pg->get_osdmap()->test_flag(CEPH_OSDMAP_NOBACKFILL)) {
    pg->osd->queue_for_recovery(pg);
  }
  return forward_event();
}

boost::statechart::result PG::RecoveryState::Active::react(const MNotifyRec& notevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  assert(pg->is_primary());
  if (pg->peer_info.count(notevt.from)) {
    dout(10) << "Active: got notify from " << notevt.from 
	     << ", already have info from that osd, ignoring" 
	     << dendl;
  } else if (pg->peer_purged.count(notevt.from)) {
    dout(10) << "Active: got notify from " << notevt.from
	     << ", already purged that peer, ignoring"
	     << dendl;
  } else {
    dout(10) << "Active: got notify from " << notevt.from 
	     << ", calling proc_replica_info and discover_all_missing"
	     << dendl;
    pg->proc_replica_info(notevt.from, notevt.notify.info);
    if (pg->have_unfound()) {
      pg->discover_all_missing(*context< RecoveryMachine >().get_query_map());
    }
  }
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Active::react(const MInfoRec& infoevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  assert(pg->is_primary());

  assert(!pg->actingbackfill.empty());
  // don't update history (yet) if we are active and primary; the replica
  // may be telling us they have activated (and committed) but we can't
  // share that until _everyone_ does the same.
  if (pg->is_actingbackfill(infoevt.from)) {
    assert(pg->info.history.last_epoch_started < 
	   pg->info.history.same_interval_since);
    assert(infoevt.info.history.last_epoch_started >= 
	   pg->info.history.same_interval_since);
    dout(10) << " peer osd." << infoevt.from << " activated and committed" 
	     << dendl;
    pg->peer_activated.insert(infoevt.from);

    if (pg->peer_activated.size() == pg->actingbackfill.size()) {
      pg->all_activated_and_committed();
    }
  }
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Active::react(const MLogRec& logevt)
{
  dout(10) << "searching osd." << logevt.from
           << " log for unfound items" << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  pg->proc_replica_log(
    *context<RecoveryMachine>().get_cur_transaction(),
    logevt.msg->info, logevt.msg->log, logevt.msg->missing, logevt.from);
  bool got_missing = pg->search_for_missing(
    pg->peer_info[logevt.from],
    pg->peer_missing[logevt.from],
    logevt.from,
    context< RecoveryMachine >().get_recovery_ctx());
  if (got_missing)
    pg->osd->queue_for_recovery(pg);
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Active::react(const QueryState& q)
{
  PG *pg = context< RecoveryMachine >().pg;

  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  {
    q.f->open_array_section("might_have_unfound");
    for (set<pg_shard_t>::iterator p = pg->might_have_unfound.begin();
	 p != pg->might_have_unfound.end();
	 ++p) {
      q.f->open_object_section("osd");
      q.f->dump_stream("osd") << *p;
      if (pg->peer_missing.count(*p)) {
	q.f->dump_string("status", "already probed");
      } else if (pg->peer_missing_requested.count(*p)) {
	q.f->dump_string("status", "querying");
      } else if (!pg->get_osdmap()->is_up(p->osd)) {
	q.f->dump_string("status", "osd is down");
      } else {
	q.f->dump_string("status", "not queried");
      }
      q.f->close_section();
    }
    q.f->close_section();
  }
  {
    q.f->open_object_section("recovery_progress");
    pg->dump_recovery_info(q.f);
    q.f->close_section();
  }

  {
    q.f->open_object_section("scrub");
    q.f->dump_stream("scrubber.epoch_start") << pg->scrubber.epoch_start;
    q.f->dump_int("scrubber.active", pg->scrubber.active);
    q.f->dump_int("scrubber.block_writes", pg->scrubber.block_writes);
    q.f->dump_int("scrubber.finalizing", pg->scrubber.finalizing);
    q.f->dump_int("scrubber.waiting_on", pg->scrubber.waiting_on);
    {
      q.f->open_array_section("scrubber.waiting_on_whom");
      for (set<pg_shard_t>::iterator p = pg->scrubber.waiting_on_whom.begin();
	   p != pg->scrubber.waiting_on_whom.end();
	   ++p) {
	q.f->dump_stream("shard") << *p;
      }
      q.f->close_section();
    }
    q.f->close_section();
  }

  q.f->close_section();
  return forward_event();
}

boost::statechart::result PG::RecoveryState::Active::react(const AllReplicasActivated &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  all_replicas_activated = true;

  pg->state_set(PG_STATE_ACTIVE);

  pg->check_local();

  // waiters
  if (!pg->is_replay() && pg->flushes_in_progress == 0) {
    pg->requeue_ops(pg->waiting_for_active);
  }

  pg->on_activate();

  return discard_event();
}

void PG::RecoveryState::Active::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->local_reserver.cancel_reservation(pg->info.pgid);

  pg->backfill_reserved = false;
  pg->backfill_reserving = false;
  pg->state_clear(PG_STATE_DEGRADED);
  pg->state_clear(PG_STATE_BACKFILL_TOOFULL);
  pg->state_clear(PG_STATE_BACKFILL_WAIT);
  pg->state_clear(PG_STATE_RECOVERY_WAIT);
  pg->state_clear(PG_STATE_REPLAY);
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_active_latency, dur);
  pg->agent_stop();
}

/*------ReplicaActive-----*/
PG::RecoveryState::ReplicaActive::ReplicaActive(my_context ctx) 
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/ReplicaActive")
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  pg->start_flush(
    context< RecoveryMachine >().get_cur_transaction(),
    context< RecoveryMachine >().get_on_applied_context_list(),
    context< RecoveryMachine >().get_on_safe_context_list());
}


boost::statechart::result PG::RecoveryState::ReplicaActive::react(
  const Activate& actevt) {
  dout(10) << "In ReplicaActive, about to call activate" << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  map<int, map<spg_t, pg_query_t> > query_map;
  pg->activate(*context< RecoveryMachine >().get_cur_transaction(),
	       actevt.query_epoch,
	       *context< RecoveryMachine >().get_on_safe_context_list(),
	       query_map, NULL, NULL);
  dout(10) << "Activate Finished" << dendl;
  return discard_event();
}

boost::statechart::result PG::RecoveryState::ReplicaActive::react(const MInfoRec& infoevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->proc_primary_info(*context<RecoveryMachine>().get_cur_transaction(),
			infoevt.info);
  return discard_event();
}

boost::statechart::result PG::RecoveryState::ReplicaActive::react(const MLogRec& logevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  dout(10) << "received log from " << logevt.from << dendl;
  ObjectStore::Transaction* t = context<RecoveryMachine>().get_cur_transaction();
  pg->merge_log(*t,logevt.msg->info, logevt.msg->log, logevt.from);
  assert(pg->pg_log.get_head() == pg->info.last_update);

  return discard_event();
}

boost::statechart::result PG::RecoveryState::ReplicaActive::react(const ActMap&)
{
  PG *pg = context< RecoveryMachine >().pg;
  if (pg->should_send_notify() && pg->get_primary().osd >= 0) {
    context< RecoveryMachine >().send_notify(
      pg->get_primary(),
      pg_notify_t(
	pg->get_primary().shard, pg->pg_whoami.shard,
	pg->get_osdmap()->get_epoch(),
	pg->get_osdmap()->get_epoch(),
	pg->info),
      pg->past_intervals);
  }
  pg->take_waiters();
  return discard_event();
}

boost::statechart::result PG::RecoveryState::ReplicaActive::react(const MQuery& query)
{
  PG *pg = context< RecoveryMachine >().pg;
  if (query.query.type == pg_query_t::MISSING) {
    pg->update_history_from_master(query.query.history);
    pg->fulfill_log(query.from, query.query, query.query_epoch);
  } // else: from prior to activation, safe to ignore
  return discard_event();
}

boost::statechart::result PG::RecoveryState::ReplicaActive::react(const QueryState& q)
{
  PG *pg = context< RecoveryMachine >().pg;

  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_int("scrubber.finalizing", pg->scrubber.finalizing);
  q.f->close_section();
  return forward_event();
}

void PG::RecoveryState::ReplicaActive::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->remote_reserver.cancel_reservation(pg->info.pgid);
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_replicaactive_latency, dur);
}

/*-------Stray---*/
PG::RecoveryState::Stray::Stray(my_context ctx) 
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Stray")
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  assert(!pg->is_active());
  assert(!pg->is_peering());
  assert(!pg->is_primary());
  pg->start_flush(
    context< RecoveryMachine >().get_cur_transaction(),
    context< RecoveryMachine >().get_on_applied_context_list(),
    context< RecoveryMachine >().get_on_safe_context_list());
}

boost::statechart::result PG::RecoveryState::Stray::react(const MLogRec& logevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  MOSDPGLog *msg = logevt.msg.get();
  dout(10) << "got info+log from osd." << logevt.from << " " << msg->info << " " << msg->log << dendl;

  if (msg->info.last_backfill == hobject_t()) {
    // restart backfill
    pg->unreg_next_scrub();
    pg->info = msg->info;
    pg->reg_next_scrub();
    pg->dirty_info = true;
    pg->dirty_big_info = true;  // maybe.
    pg->pg_log.claim_log(msg->log);
    pg->pg_log.reset_backfill();
  } else {
    ObjectStore::Transaction* t = context<RecoveryMachine>().get_cur_transaction();
    pg->merge_log(*t, msg->info, msg->log, logevt.from);
  }

  assert(pg->pg_log.get_head() == pg->info.last_update);

  post_event(Activate(logevt.msg->get_epoch()));
  return transit<ReplicaActive>();
}

boost::statechart::result PG::RecoveryState::Stray::react(const MInfoRec& infoevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  dout(10) << "got info from osd." << infoevt.from << " " << infoevt.info << dendl;

  if (pg->info.last_update > infoevt.info.last_update) {
    // rewind divergent log entries
    ObjectStore::Transaction* t = context<RecoveryMachine>().get_cur_transaction();
    pg->rewind_divergent_log(*t, infoevt.info.last_update);
    pg->info.stats = infoevt.info.stats;
  }
  
  assert(infoevt.info.last_update == pg->info.last_update);
  assert(pg->pg_log.get_head() == pg->info.last_update);

  post_event(Activate(infoevt.msg_epoch));
  return transit<ReplicaActive>();
}

boost::statechart::result PG::RecoveryState::Stray::react(const MQuery& query)
{
  PG *pg = context< RecoveryMachine >().pg;
  if (query.query.type == pg_query_t::INFO) {
    pair<pg_shard_t, pg_info_t> notify_info;
    pg->update_history_from_master(query.query.history);
    pg->fulfill_info(query.from, query.query, notify_info);
    context< RecoveryMachine >().send_notify(
      notify_info.first,
      pg_notify_t(
	notify_info.first.shard, pg->pg_whoami.shard,
	query.query_epoch,
	pg->get_osdmap()->get_epoch(),
	notify_info.second),
      pg->past_intervals);
  } else {
    pg->fulfill_log(query.from, query.query, query.query_epoch);
  }
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Stray::react(const ActMap&)
{
  PG *pg = context< RecoveryMachine >().pg;
  if (pg->should_send_notify() && pg->get_primary().osd >= 0) {
    context< RecoveryMachine >().send_notify(
      pg->get_primary(),
      pg_notify_t(
	pg->get_primary().shard, pg->pg_whoami.shard,
	pg->get_osdmap()->get_epoch(),
	pg->get_osdmap()->get_epoch(),
	pg->info),
      pg->past_intervals);
  }
  pg->take_waiters();
  return discard_event();
}

void PG::RecoveryState::Stray::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_stray_latency, dur);
}

/*--------GetInfo---------*/
PG::RecoveryState::GetInfo::GetInfo(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Peering/GetInfo")
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  pg->generate_past_intervals();
  auto_ptr<PriorSet> &prior_set = context< Peering >().prior_set;

  if (!prior_set.get())
    pg->build_prior(prior_set);

  pg->publish_stats_to_osd();

  get_infos();
  if (peer_info_requested.empty() && !prior_set->pg_down) {
    post_event(GotInfo());
  }
}

void PG::RecoveryState::GetInfo::get_infos()
{
  PG *pg = context< RecoveryMachine >().pg;
  auto_ptr<PriorSet> &prior_set = context< Peering >().prior_set;

  for (set<pg_shard_t>::const_iterator it = prior_set->probe.begin();
       it != prior_set->probe.end();
       ++it) {
    pg_shard_t peer = *it;
    if (peer == pg->pg_whoami) {
      continue;
    }
    if (pg->peer_info.count(peer)) {
      dout(10) << " have osd." << peer << " info " << pg->peer_info[peer] << dendl;
      continue;
    }
    if (peer_info_requested.count(peer)) {
      dout(10) << " already requested info from osd." << peer << dendl;
    } else if (!pg->get_osdmap()->is_up(peer.osd)) {
      dout(10) << " not querying info from down osd." << peer << dendl;
    } else {
      dout(10) << " querying info from osd." << peer << dendl;
      context< RecoveryMachine >().send_query(
	peer, pg_query_t(pg_query_t::INFO,
			 it->shard, pg->pg_whoami.shard,
			 pg->info.history,
			 pg->get_osdmap()->get_epoch()));
      peer_info_requested.insert(peer);
    }
  }
}

boost::statechart::result PG::RecoveryState::GetInfo::react(const MNotifyRec& infoevt) 
{
  set<pg_shard_t>::iterator p = peer_info_requested.find(infoevt.from);
  if (p != peer_info_requested.end())
    peer_info_requested.erase(p);

  PG *pg = context< RecoveryMachine >().pg;
  epoch_t old_start = pg->info.history.last_epoch_started;
  if (pg->proc_replica_info(infoevt.from, infoevt.notify.info)) {
    // we got something new ...
    auto_ptr<PriorSet> &prior_set = context< Peering >().prior_set;
    if (old_start < pg->info.history.last_epoch_started) {
      dout(10) << " last_epoch_started moved forward, rebuilding prior" << dendl;
      pg->build_prior(prior_set);

      // filter out any osds that got dropped from the probe set from
      // peer_info_requested.  this is less expensive than restarting
      // peering (which would re-probe everyone).
      set<pg_shard_t>::iterator p = peer_info_requested.begin();
      while (p != peer_info_requested.end()) {
	if (prior_set->probe.count(*p) == 0) {
	  dout(20) << " dropping osd." << *p << " from info_requested, no longer in probe set" << dendl;
	  peer_info_requested.erase(p++);
	} else {
	  ++p;
	}
      }
      get_infos();
    }

    // are we done getting everything?
    if (peer_info_requested.empty() && !prior_set->pg_down) {
      /*
       * make sure we have at least one !incomplete() osd from the
       * last rw interval.  the incomplete (backfilling) replicas
       * get a copy of the log, but they don't get all the object
       * updates, so they are insufficient to recover changes during
       * that interval.
       */
      if (pg->info.history.last_epoch_started) {
	for (map<epoch_t,pg_interval_t>::reverse_iterator p = pg->past_intervals.rbegin();
	     p != pg->past_intervals.rend();
	     ++p) {
	  if (p->first < pg->info.history.last_epoch_started)
	    break;
	  if (!p->second.maybe_went_rw)
	    continue;
	  pg_interval_t& interval = p->second;
	  dout(10) << " last maybe_went_rw interval was " << interval << dendl;
	  OSDMapRef osdmap = pg->get_osdmap();

	  /*
	   * this mirrors the PriorSet calculation: we wait if we
	   * don't have an up (AND !incomplete) node AND there are
	   * nodes down that might be usable.
	   */
	  bool any_up_complete_now = false;
	  bool any_down_now = false;
	  for (unsigned i=0; i<interval.acting.size(); i++) {
	    int o = interval.acting[i];
	    if (o == CRUSH_ITEM_NONE)
	      continue;
	    pg_shard_t so(o, pg->pool.info.ec_pool() ? i : ghobject_t::NO_SHARD);
	    if (!osdmap->exists(o) || osdmap->get_info(o).lost_at > interval.first)
	      continue;  // dne or lost
	    if (osdmap->is_up(o)) {
	      pg_info_t *pinfo;
	      if (so == pg->pg_whoami) {
		pinfo = &pg->info;
	      } else {
		assert(pg->peer_info.count(so));
		pinfo = &pg->peer_info[so];
	      }
	      if (!pinfo->is_incomplete())
		any_up_complete_now = true;
	    } else {
	      any_down_now = true;
	    }
	  }
	  if (!any_up_complete_now && any_down_now) {
	    dout(10) << " no osds up+complete from interval " << interval << dendl;
	    pg->state_set(PG_STATE_DOWN);
	    return discard_event();
	  }
	  break;
	}
      }
      post_event(GotInfo());
    }
  }
  return discard_event();
}

boost::statechart::result PG::RecoveryState::GetInfo::react(const QueryState& q)
{
  PG *pg = context< RecoveryMachine >().pg;
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  q.f->open_array_section("requested_info_from");
  for (set<pg_shard_t>::iterator p = peer_info_requested.begin();
       p != peer_info_requested.end();
       ++p) {
    q.f->open_object_section("osd");
    q.f->dump_stream("osd") << *p;
    if (pg->peer_info.count(*p)) {
      q.f->open_object_section("got_info");
      pg->peer_info[*p].dump(q.f);
      q.f->close_section();
    }
    q.f->close_section();
  }
  q.f->close_section();

  q.f->close_section();
  return forward_event();
}

void PG::RecoveryState::GetInfo::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_getinfo_latency, dur);
}

/*------GetLog------------*/
PG::RecoveryState::GetLog::GetLog(my_context ctx)
  : my_base(ctx),
    NamedState(
      context< RecoveryMachine >().pg->cct, "Started/Primary/Peering/GetLog"),
    msg(0)
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;

  // adjust acting?
  if (!pg->choose_acting(auth_log_shard)) {
    if (!pg->want_acting.empty()) {
      post_event(NeedActingChange());
    } else {
      post_event(IsIncomplete());
    }
    return;
  }

  // am i the best?
  if (auth_log_shard == pg->pg_whoami) {
    post_event(GotLog());
    return;
  }

  const pg_info_t& best = pg->peer_info[auth_log_shard];

  // am i broken?
  if (pg->info.last_update < best.log_tail) {
    dout(10) << " not contiguous with osd." << auth_log_shard << ", down" << dendl;
    post_event(IsIncomplete());
    return;
  }

  // how much log to request?
  eversion_t request_log_from = pg->info.last_update;
  assert(!pg->actingbackfill.empty());
  for (set<pg_shard_t>::iterator p = pg->actingbackfill.begin();
       p != pg->actingbackfill.end();
       ++p) {
    if (*p == pg->pg_whoami) continue;
    pg_info_t& ri = pg->peer_info[*p];
    if (ri.last_update >= best.log_tail && ri.last_update < request_log_from)
      request_log_from = ri.last_update;
  }

  // how much?
  dout(10) << " requesting log from osd." << auth_log_shard << dendl;
  context<RecoveryMachine>().send_query(
    auth_log_shard,
    pg_query_t(
      pg_query_t::LOG,
      auth_log_shard.shard, pg->pg_whoami.shard,
      request_log_from, pg->info.history,
      pg->get_osdmap()->get_epoch()));
}

boost::statechart::result PG::RecoveryState::GetLog::react(const AdvMap& advmap)
{
  // make sure our log source didn't go down.  we need to check
  // explicitly because it may not be part of the prior set, which
  // means the Peering state check won't catch it going down.
  if (!advmap.osdmap->is_up(auth_log_shard.osd)) {
    dout(10) << "GetLog: auth_log_shard osd."
	     << auth_log_shard.osd << " went down" << dendl;
    post_event(advmap);
    return transit< Reset >();
  }

  // let the Peering state do its checks.
  return forward_event();
}

boost::statechart::result PG::RecoveryState::GetLog::react(const MLogRec& logevt)
{
  assert(!msg);
  if (logevt.from != auth_log_shard) {
    dout(10) << "GetLog: discarding log from "
	     << "non-auth_log_shard osd." << logevt.from << dendl;
    return discard_event();
  }
  dout(10) << "GetLog: recieved master log from osd" 
	   << logevt.from << dendl;
  msg = logevt.msg;
  post_event(GotLog());
  return discard_event();
}

boost::statechart::result PG::RecoveryState::GetLog::react(const GotLog&)
{
  dout(10) << "leaving GetLog" << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  if (msg) {
    dout(10) << "processing master log" << dendl;
    pg->proc_master_log(*context<RecoveryMachine>().get_cur_transaction(),
			msg->info, msg->log, msg->missing, 
			auth_log_shard);
  }
  pg->start_flush(
    context< RecoveryMachine >().get_cur_transaction(),
    context< RecoveryMachine >().get_on_applied_context_list(),
    context< RecoveryMachine >().get_on_safe_context_list());
  return transit< GetMissing >();
}

boost::statechart::result PG::RecoveryState::GetLog::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_stream("auth_log_shard") << auth_log_shard;
  q.f->close_section();
  return forward_event();
}

void PG::RecoveryState::GetLog::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_getlog_latency, dur);
}

/*------WaitActingChange--------*/
PG::RecoveryState::WaitActingChange::WaitActingChange(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Peering/WaitActingChange")
{
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result PG::RecoveryState::WaitActingChange::react(const AdvMap& advmap)
{
  PG *pg = context< RecoveryMachine >().pg;
  OSDMapRef osdmap = advmap.osdmap;

  dout(10) << "verifying no want_acting " << pg->want_acting << " targets didn't go down" << dendl;
  for (vector<int>::iterator p = pg->want_acting.begin(); p != pg->want_acting.end(); ++p) {
    if (!osdmap->is_up(*p)) {
      dout(10) << " want_acting target osd." << *p << " went down, resetting" << dendl;
      post_event(advmap);
      return transit< Reset >();
    }
  }
  return forward_event();
}

boost::statechart::result PG::RecoveryState::WaitActingChange::react(const MLogRec& logevt)
{
  dout(10) << "In WaitActingChange, ignoring MLocRec" << dendl;
  return discard_event();
}

boost::statechart::result PG::RecoveryState::WaitActingChange::react(const MInfoRec& evt)
{
  dout(10) << "In WaitActingChange, ignoring MInfoRec" << dendl;
  return discard_event();
}

boost::statechart::result PG::RecoveryState::WaitActingChange::react(const MNotifyRec& evt)
{
  dout(10) << "In WaitActingChange, ignoring MNotifyRec" << dendl;
  return discard_event();
}

boost::statechart::result PG::RecoveryState::WaitActingChange::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment", "waiting for pg acting set to change");
  q.f->close_section();
  return forward_event();
}

void PG::RecoveryState::WaitActingChange::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_waitactingchange_latency, dur);
}

/*------Incomplete--------*/
PG::RecoveryState::Incomplete::Incomplete(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Peering/Incomplete")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;

  pg->state_clear(PG_STATE_PEERING);
  pg->state_set(PG_STATE_INCOMPLETE);
  pg->publish_stats_to_osd();
}

boost::statechart::result PG::RecoveryState::Incomplete::react(const AdvMap &advmap) {
  PG *pg = context< RecoveryMachine >().pg;
  int64_t poolnum = pg->info.pgid.pool();

  // Reset if min_size changed, pg might now be able to go active
  if (advmap.lastmap->get_pools().find(poolnum)->second.min_size !=
      advmap.osdmap->get_pools().find(poolnum)->second.min_size) {
    post_event(advmap);
    return transit< Reset >();
  }

  return forward_event();
}

boost::statechart::result PG::RecoveryState::Incomplete::react(const MNotifyRec& notevt) {
  dout(7) << "handle_pg_notify from osd." << notevt.from << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  if (pg->peer_info.count(notevt.from) &&
      pg->peer_info[notevt.from].last_update == notevt.notify.info.last_update) {
    dout(10) << *pg << " got dup osd." << notevt.from << " info " << notevt.notify.info
	     << ", identical to ours" << dendl;
    return discard_event();
  } else {
    pg->proc_replica_info(notevt.from, notevt.notify.info);
    // try again!
    return transit< GetLog >();
  }
}

void PG::RecoveryState::Incomplete::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;

  pg->state_clear(PG_STATE_INCOMPLETE);
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_incomplete_latency, dur);
}

/*------GetMissing--------*/
PG::RecoveryState::GetMissing::GetMissing(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Peering/GetMissing")
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  assert(!pg->actingbackfill.empty());
  for (set<pg_shard_t>::iterator i = pg->actingbackfill.begin();
       i != pg->actingbackfill.end();
       ++i) {
    if (*i == pg->get_primary()) continue;
    const pg_info_t& pi = pg->peer_info[*i];

    if (pi.is_empty())
      continue;                                // no pg data, nothing divergent

    if (pi.last_update < pg->pg_log.get_tail()) {
      dout(10) << " osd." << *i << " is not contiguous, will restart backfill" << dendl;
      pg->peer_missing[*i];
      continue;
    }
    if (pi.last_backfill == hobject_t()) {
      dout(10) << " osd." << *i << " will fully backfill; can infer empty missing set" << dendl;
      pg->peer_missing[*i];
      continue;
    }

    if (pi.last_update == pi.last_complete &&  // peer has no missing
	pi.last_update == pg->info.last_update) {  // peer is up to date
      // replica has no missing and identical log as us.  no need to
      // pull anything.
      // FIXME: we can do better here.  if last_update==last_complete we
      //        can infer the rest!
      dout(10) << " osd." << *i << " has no missing, identical log" << dendl;
      pg->peer_missing[*i];
      continue;
    }

    // We pull the log from the peer's last_epoch_started to ensure we
    // get enough log to detect divergent updates.
    eversion_t since(pi.last_epoch_started, 0);
    assert(pi.last_update >= pg->info.log_tail);  // or else choose_acting() did a bad thing
    if (pi.log_tail <= since) {
      dout(10) << " requesting log+missing since " << since << " from osd." << *i << dendl;
      context< RecoveryMachine >().send_query(
	*i,
	pg_query_t(
	  pg_query_t::LOG,
	  i->shard, pg->pg_whoami.shard,
	  since, pg->info.history,
	  pg->get_osdmap()->get_epoch()));
    } else {
      dout(10) << " requesting fulllog+missing from osd." << *i
	       << " (want since " << since << " < log.tail " << pi.log_tail << ")"
	       << dendl;
      context< RecoveryMachine >().send_query(
	*i, pg_query_t(
	  pg_query_t::FULLLOG,
	  i->shard, pg->pg_whoami.shard,
	  pg->info.history, pg->get_osdmap()->get_epoch()));
    }
    peer_missing_requested.insert(*i);
  }

  if (peer_missing_requested.empty()) {
    if (pg->need_up_thru) {
      dout(10) << " still need up_thru update before going active" << dendl;
      post_event(NeedUpThru());
      return;
    }

    // all good!
    post_event(Activate(pg->get_osdmap()->get_epoch()));
  }
}

boost::statechart::result PG::RecoveryState::GetMissing::react(const MLogRec& logevt)
{
  PG *pg = context< RecoveryMachine >().pg;

  peer_missing_requested.erase(logevt.from);
  pg->proc_replica_log(*context<RecoveryMachine>().get_cur_transaction(),
		       logevt.msg->info, logevt.msg->log, logevt.msg->missing, logevt.from);
  
  if (peer_missing_requested.empty()) {
    if (pg->need_up_thru) {
      dout(10) << " still need up_thru update before going active" << dendl;
      post_event(NeedUpThru());
    } else {
      dout(10) << "Got last missing, don't need missing "
	       << "posting CheckRepops" << dendl;
      post_event(Activate(pg->get_osdmap()->get_epoch()));
    }
  }
  return discard_event();
};

boost::statechart::result PG::RecoveryState::GetMissing::react(const QueryState& q)
{
  PG *pg = context< RecoveryMachine >().pg;
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;

  q.f->open_array_section("peer_missing_requested");
  for (set<pg_shard_t>::iterator p = peer_missing_requested.begin();
       p != peer_missing_requested.end();
       ++p) {
    q.f->open_object_section("osd");
    q.f->dump_stream("osd") << *p;
    if (pg->peer_missing.count(*p)) {
      q.f->open_object_section("got_missing");
      pg->peer_missing[*p].dump(q.f);
      q.f->close_section();
    }
    q.f->close_section();
  }
  q.f->close_section();

  q.f->close_section();
  return forward_event();
}

void PG::RecoveryState::GetMissing::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_getmissing_latency, dur);
}

/*------WaitUpThru--------*/
PG::RecoveryState::WaitUpThru::WaitUpThru(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg->cct, "Started/Primary/Peering/WaitUpThru")
{
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result PG::RecoveryState::WaitUpThru::react(const ActMap& am)
{
  PG *pg = context< RecoveryMachine >().pg;
  if (!pg->need_up_thru) {
    post_event(Activate(pg->get_osdmap()->get_epoch()));
  }
  return forward_event();
}

boost::statechart::result PG::RecoveryState::WaitUpThru::react(const MLogRec& logevt)
{
  dout(10) << "Noting missing from osd." << logevt.from << dendl;
  PG *pg = context< RecoveryMachine >().pg;
  pg->peer_missing[logevt.from].swap(logevt.msg->missing);
  pg->peer_info[logevt.from] = logevt.msg->info;
  return discard_event();
}

boost::statechart::result PG::RecoveryState::WaitUpThru::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment", "waiting for osdmap to reflect a new up_thru for this osd");
  q.f->close_section();
  return forward_event();
}

void PG::RecoveryState::WaitUpThru::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_waitupthru_latency, dur);
}

/*----RecoveryState::RecoveryMachine Methods-----*/
#undef dout_prefix
#define dout_prefix *_dout << pg->gen_prefix() 

void PG::RecoveryState::RecoveryMachine::log_enter(const char *state_name)
{
  dout(5) << "enter " << state_name << dendl;
  pg->osd->pg_recovery_stats.log_enter(state_name);
}

void PG::RecoveryState::RecoveryMachine::log_exit(const char *state_name, utime_t enter_time)
{
  utime_t dur = ceph_clock_now(pg->cct) - enter_time;
  dout(5) << "exit " << state_name << " " << dur << " " << event_count << " " << event_time << dendl;
  pg->osd->pg_recovery_stats.log_exit(state_name, ceph_clock_now(pg->cct) - enter_time,
				      event_count, event_time);
  event_count = 0;
  event_time = utime_t();
}


/*---------------------------------------------------*/
#undef dout_prefix
#define dout_prefix (*_dout << (debug_pg ? debug_pg->gen_prefix() : string()) << " PriorSet: ")

PG::PriorSet::PriorSet(bool ec_pool,
		       PGBackend::IsRecoverablePredicate *c,
		       const OSDMap &osdmap,
		       const map<epoch_t, pg_interval_t> &past_intervals,
		       const vector<int> &up,
		       const vector<int> &acting,
		       const pg_info_t &info,
		       const PG *debug_pg)
  : ec_pool(ec_pool), pg_down(false), pcontdec(c)
{
  /*
   * We have to be careful to gracefully deal with situations like
   * so. Say we have a power outage or something that takes out both
   * OSDs, but the monitor doesn't mark them down in the same epoch.
   * The history may look like
   *
   *  1: A B
   *  2:   B
   *  3:       let's say B dies for good, too (say, from the power spike) 
   *  4: A
   *
   * which makes it look like B may have applied updates to the PG
   * that we need in order to proceed.  This sucks...
   *
   * To minimize the risk of this happening, we CANNOT go active if
   * _any_ OSDs in the prior set are down until we send an MOSDAlive
   * to the monitor such that the OSDMap sets osd_up_thru to an epoch.
   * Then, we have something like
   *
   *  1: A B
   *  2:   B   up_thru[B]=0
   *  3:
   *  4: A
   *
   * -> we can ignore B, bc it couldn't have gone active (alive_thru
   *    still 0).
   *
   * or,
   *
   *  1: A B
   *  2:   B   up_thru[B]=0
   *  3:   B   up_thru[B]=2
   *  4:
   *  5: A    
   *
   * -> we must wait for B, bc it was alive through 2, and could have
   *    written to the pg.
   *
   * If B is really dead, then an administrator will need to manually
   * intervene by marking the OSD as "lost."
   */

  // Include current acting and up nodes... not because they may
  // contain old data (this interval hasn't gone active, obviously),
  // but because we want their pg_info to inform choose_acting(), and
  // so that we know what they do/do not have explicitly before
  // sending them any new info/logs/whatever.
  for (unsigned i=0; i<acting.size(); i++) {
    if (acting[i] != CRUSH_ITEM_NONE)
      probe.insert(pg_shard_t(acting[i], ec_pool ? i : ghobject_t::NO_SHARD));
  }
  // It may be possible to exlude the up nodes, but let's keep them in
  // there for now.
  for (unsigned i=0; i<up.size(); i++) {
    if (up[i] != CRUSH_ITEM_NONE)
      probe.insert(pg_shard_t(up[i], ec_pool ? i : ghobject_t::NO_SHARD));
  }

  for (map<epoch_t,pg_interval_t>::const_reverse_iterator p = past_intervals.rbegin();
       p != past_intervals.rend();
       ++p) {
    const pg_interval_t &interval = p->second;
    dout(10) << "build_prior " << interval << dendl;

    if (interval.last < info.history.last_epoch_started)
      break;  // we don't care

    if (interval.acting.empty())
      continue;

    if (!interval.maybe_went_rw)
      continue;

    // look at candidate osds during this interval.  each falls into
    // one of three categories: up, down (but potentially
    // interesting), or lost (down, but we won't wait for it).
    set<pg_shard_t> up_now;
    bool any_down_now = false;  // any candidates down now (that might have useful data)

    // consider ACTING osds
    for (unsigned i=0; i<interval.acting.size(); i++) {
      int o = interval.acting[i];
      if (o == CRUSH_ITEM_NONE)
	continue;
      pg_shard_t so(o, ec_pool ? i : ghobject_t::NO_SHARD);

      const osd_info_t *pinfo = 0;
      if (osdmap.exists(o))
	pinfo = &osdmap.get_info(o);

      if (osdmap.is_up(o)) {
	// include past acting osds if they are up.
	probe.insert(so);
	up_now.insert(so);
      } else if (!pinfo) {
	dout(10) << "build_prior  prior osd." << o << " no longer exists" << dendl;
	down.insert(o);
      } else if (pinfo->lost_at > interval.first) {
	dout(10) << "build_prior  prior osd." << o << " is down, but lost_at " << pinfo->lost_at << dendl;
	down.insert(o);
      } else {
	dout(10) << "build_prior  prior osd." << o << " is down" << dendl;
	down.insert(o);
	any_down_now = true;
      }
    }

    // if not enough osds survived this interval, and we may have gone rw,
    // then we need to wait for one of those osds to recover to
    // ensure that we haven't lost any information.
    if (!(*pcontdec)(up_now) && any_down_now) {
      // fixme: how do we identify a "clean" shutdown anyway?
      dout(10) << "build_prior  possibly went active+rw, insufficient up;"
	       << " including down osds" << dendl;
      for (vector<int>::const_iterator i = interval.acting.begin();
	   i != interval.acting.end();
	   ++i) {
	if (osdmap.exists(*i) &&   // if it doesn't exist, we already consider it lost.
	    osdmap.is_down(*i)) {
	  pg_down = true;

	  // make note of when any down osd in the cur set was lost, so that
	  // we can notice changes in prior_set_affected.
	  blocked_by[*i] = osdmap.get_info(*i).lost_at;
	}
      }
    }
  }

  dout(10) << "build_prior final: probe " << probe
	   << " down " << down
	   << " blocked_by " << blocked_by
	   << (pg_down ? " pg_down":"")
	   << dendl;
}

// true if the given map affects the prior set
bool PG::PriorSet::affected_by_map(const OSDMapRef osdmap, const PG *debug_pg) const
{
  for (set<pg_shard_t>::iterator p = probe.begin();
       p != probe.end();
       ++p) {
    int o = p->osd;

    // did someone in the prior set go down?
    if (osdmap->is_down(o) && down.count(o) == 0) {
      dout(10) << "affected_by_map osd." << o << " now down" << dendl;
      return true;
    }

    // did a down osd in cur get (re)marked as lost?
    map<int, epoch_t>::const_iterator r = blocked_by.find(o);
    if (r != blocked_by.end()) {
      if (!osdmap->exists(o)) {
	dout(10) << "affected_by_map osd." << o << " no longer exists" << dendl;
	return true;
      }
      if (osdmap->get_info(o).lost_at != r->second) {
	dout(10) << "affected_by_map osd." << o << " (re)marked as lost" << dendl;
	return true;
      }
    }
  }

  // did someone in the prior down set go up?
  for (set<int>::const_iterator p = down.begin();
       p != down.end();
       ++p) {
    int o = *p;

    if (osdmap->is_up(o)) {
      dout(10) << "affected_by_map osd." << *p << " now up" << dendl;
      return true;
    }

    // did someone in the prior set get lost or destroyed?
    if (!osdmap->exists(o)) {
      dout(10) << "affected_by_map osd." << o << " no longer exists" << dendl;
      return true;
    }
  }

  return false;
}

void PG::RecoveryState::start_handle(RecoveryCtx *new_ctx) {
  assert(!rctx);
  rctx = new_ctx;
  if (rctx)
    rctx->start_time = ceph_clock_now(pg->cct);
}

void PG::RecoveryState::end_handle() {
  if (rctx) {
    utime_t dur = ceph_clock_now(pg->cct) - rctx->start_time;
    machine.event_time += dur;
  }
  machine.event_count++;
  rctx = 0;
}

void intrusive_ptr_add_ref(PG *pg) { pg->get("intptr"); }
void intrusive_ptr_release(PG *pg) { pg->put("intptr"); }

#ifdef PG_DEBUG_REFS
  uint64_t get_with_id(PG *pg) { return pg->get_with_id(); }
  void put_with_id(PG *pg, uint64_t id) { return pg->put_with_id(id); }
#endif
