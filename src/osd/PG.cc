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
// #include "msg/Messenger.h"
#include "messages/MOSDRepScrub.h"
// #include "common/cmdparse.h"
// #include "common/ceph_context.h"

#include "common/errno.h"
#include "common/config.h"
#include "OSD.h"
#include "OpRequest.h"
#include "ScrubStore.h"
#include "Session.h"

#include "common/Timer.h"
#include "common/perf_counters.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDPGNotify.h"
// #include "messages/MOSDPGLog.h"
#include "messages/MOSDPGRemove.h"
#include "messages/MOSDPGInfo.h"
#include "messages/MOSDPGTrim.h"
#include "messages/MOSDPGScan.h"
#include "messages/MOSDPGBackfill.h"
#include "messages/MOSDPGBackfillRemove.h"
#include "messages/MBackfillReserve.h"
#include "messages/MRecoveryReserve.h"
#include "messages/MOSDPGPush.h"
#include "messages/MOSDPGPushReply.h"
#include "messages/MOSDPGPull.h"
#include "messages/MOSDECSubOpWrite.h"
#include "messages/MOSDECSubOpWriteReply.h"
#include "messages/MOSDECSubOpRead.h"
#include "messages/MOSDECSubOpReadReply.h"
#include "messages/MOSDPGUpdateLogMissing.h"
#include "messages/MOSDPGUpdateLogMissingReply.h"
#include "messages/MOSDBackoff.h"
#include "messages/MOSDScrubReserve.h"
#include "messages/MOSDRepOp.h"
#include "messages/MOSDRepOpReply.h"
#include "messages/MOSDRepScrubMap.h"
#include "messages/MOSDPGRecoveryDelete.h"
#include "messages/MOSDPGRecoveryDeleteReply.h"

#include "common/BackTrace.h"
#include "common/EventTrace.h"

#ifdef WITH_LTTNG
#define TRACEPOINT_DEFINE
#define TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#include "tracing/pg.h"
#undef TRACEPOINT_PROBE_DYNAMIC_LINKAGE
#undef TRACEPOINT_DEFINE
#else
#define tracepoint(...)
#endif

#include <sstream>

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

// prefix pgmeta_oid keys with _ so that PGLog::read_log_and_missing() can
// easily skip them
const string infover_key("_infover");
const string info_key("_info");
const string biginfo_key("_biginfo");
const string epoch_key("_epoch");
const string fastinfo_key("_fastinfo");

template <class T>
static ostream& _prefix(std::ostream *_dout, T *t)
{
  return t->gen_prefix(*_dout);
}

void PGStateHistory::enter(PG* pg, const utime_t entime, const char* state)
{
  // Ignore trimming state machine for now
  if (::strstr(state, "Trimming") != NULL) {
    return;
  } else if (pi != nullptr) {
    pi->enter_state(entime, state);
  } else {
    // Store current state since we can't reliably take the PG lock here
    if ( tmppi == nullptr) {
      tmppi = std::unique_ptr<PGStateInstance>(new PGStateInstance);
    }

    thispg = pg;
    tmppi->enter_state(entime, state);
  }
}

void PGStateHistory::exit(const char* state) {
  // Ignore trimming state machine for now
  // Do nothing if PG is being destroyed!
  if (::strstr(state, "Trimming") != NULL || pg_in_destructor) {
    return;
  } else {
    bool ilocked = false;
    if(!thispg->is_locked()) {
      thispg->lock();
      ilocked = true;
    }
    if (pi == nullptr) {
      buffer.push_back(std::unique_ptr<PGStateInstance>(tmppi.release()));
      pi = buffer.back().get();
      pi->setepoch(thispg->get_osdmap_epoch());
    }

    pi->exit_state(ceph_clock_now());
    if (::strcmp(state, "Reset") == 0) {
      this->reset();
    }
    if(ilocked) {
      thispg->unlock();
    }
  }
}

void PGStateHistory::dump(Formatter* f) const {
  f->open_array_section("history");
  for (auto pi = buffer.begin(); pi != buffer.end(); ++pi) {
    f->open_object_section("states");
    f->dump_stream("epoch") << (*pi)->this_epoch;
    for (auto she : (*pi)->state_history) {
      f->dump_string("state", std::get<2>(she));
      f->dump_stream("enter") << std::get<0>(she);
      f->dump_stream("exit") << std::get<1>(she);
    }
    f->close_section();
  }
  f->close_section();
}

void PG::get(const char* tag)
{
  int after = ++ref;
  lgeneric_subdout(cct, refs, 5) << "PG::get " << this << " "
				 << "tag " << (tag ? tag : "(none") << " "
				 << (after - 1) << " -> " << after << dendl;
#ifdef PG_DEBUG_REFS
  std::lock_guard l(_ref_id_lock);
  _tag_counts[tag]++;
#endif
}

void PG::put(const char* tag)
{
#ifdef PG_DEBUG_REFS
  {
    std::lock_guard l(_ref_id_lock);
    auto tag_counts_entry = _tag_counts.find(tag);
    ceph_assert(tag_counts_entry != _tag_counts.end());
    --tag_counts_entry->second;
    if (tag_counts_entry->second == 0) {
      _tag_counts.erase(tag_counts_entry);
    }
  }
#endif
  auto local_cct = cct;
  int after = --ref;
  lgeneric_subdout(local_cct, refs, 5) << "PG::put " << this << " "
				       << "tag " << (tag ? tag : "(none") << " "
				       << (after + 1) << " -> " << after
				       << dendl;
  if (after == 0)
    delete this;
}

#ifdef PG_DEBUG_REFS
uint64_t PG::get_with_id()
{
  ref++;
  std::lock_guard l(_ref_id_lock);
  uint64_t id = ++_ref_id;
  BackTrace bt(0);
  stringstream ss;
  bt.print(ss);
  lgeneric_subdout(cct, refs, 5) << "PG::get " << this << " " << info.pgid
				 << " got id " << id << " "
				 << (ref - 1) << " -> " << ref
				 << dendl;
  ceph_assert(!_live_ids.count(id));
  _live_ids.insert(make_pair(id, ss.str()));
  return id;
}

void PG::put_with_id(uint64_t id)
{
  int newref = --ref;
  lgeneric_subdout(cct, refs, 5) << "PG::put " << this << " " << info.pgid
				 << " put id " << id << " "
				 << (newref + 1) << " -> " << newref
				 << dendl;
  {
    std::lock_guard l(_ref_id_lock);
    ceph_assert(_live_ids.count(id));
    _live_ids.erase(id);
  }
  if (newref)
    delete this;
}

void PG::dump_live_ids()
{
  std::lock_guard l(_ref_id_lock);
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


void PGPool::update(CephContext *cct, OSDMapRef map)
{
  const pg_pool_t *pi = map->get_pg_pool(id);
  if (!pi) {
    return; // pool has been deleted
  }
  info = *pi;
  name = map->get_pool_name(id);

  bool updated = false;
  if ((map->get_epoch() != cached_epoch + 1) ||
      (pi->get_snap_epoch() == map->get_epoch())) {
    updated = true;
  }

  if (map->require_osd_release >= CEPH_RELEASE_MIMIC) {
    // mimic tracks removed_snaps_queue in the OSDmap and purged_snaps
    // in the pg_info_t, with deltas for both in each OSDMap.  we don't
    // need to (and can't) track it here.
    cached_removed_snaps.clear();
    newly_removed_snaps.clear();
  } else {
    // legacy (<= luminous) removed_snaps tracking
    if (updated) {
      if (pi->maybe_updated_removed_snaps(cached_removed_snaps)) {
	pi->build_removed_snaps(newly_removed_snaps);
	if (cached_removed_snaps.subset_of(newly_removed_snaps)) {
          interval_set<snapid_t> removed_snaps = newly_removed_snaps;
          newly_removed_snaps.subtract(cached_removed_snaps);
          cached_removed_snaps.swap(removed_snaps);
	} else {
          lgeneric_subdout(cct, osd, 0) << __func__
		<< " cached_removed_snaps shrank from " << cached_removed_snaps
		<< " to " << newly_removed_snaps << dendl;
          cached_removed_snaps.swap(newly_removed_snaps);
          newly_removed_snaps.clear();
	}
      } else {
	newly_removed_snaps.clear();
      }
    } else {
      /* 1) map->get_epoch() == cached_epoch + 1 &&
       * 2) pi->get_snap_epoch() != map->get_epoch()
       *
       * From the if branch, 1 && 2 must be true.  From 2, we know that
       * this map didn't change the set of removed snaps.  From 1, we
       * know that our cached_removed_snaps matches the previous map.
       * Thus, from 1 && 2, cached_removed snaps matches the current
       * set of removed snaps and all we have to do is clear
       * newly_removed_snaps.
       */
      newly_removed_snaps.clear();
    }
    lgeneric_subdout(cct, osd, 20)
      << "PGPool::update cached_removed_snaps "
      << cached_removed_snaps
      << " newly_removed_snaps "
      << newly_removed_snaps
      << " snapc " << snapc
      << (updated ? " (updated)":" (no change)")
      << dendl;
    if (cct->_conf->osd_debug_verify_cached_snaps) {
      interval_set<snapid_t> actual_removed_snaps;
      pi->build_removed_snaps(actual_removed_snaps);
      if (!(actual_removed_snaps == cached_removed_snaps)) {
	lgeneric_derr(cct) << __func__
		   << ": mismatch between the actual removed snaps "
		   << actual_removed_snaps
		   << " and pool.cached_removed_snaps "
		   << " pool.cached_removed_snaps " << cached_removed_snaps
		   << dendl;
      }
      ceph_assert(actual_removed_snaps == cached_removed_snaps);
    }
  }
  if (info.is_pool_snaps_mode() && updated) {
    snapc = pi->get_snap_context();
  }
  cached_epoch = map->get_epoch();
}

PG::PG(OSDService *o, OSDMapRef curmap,
       const PGPool &_pool, spg_t p) :
  pg_id(p),
  coll(p),
  osd(o),
  cct(o->cct),
  osdmap_ref(curmap),
  pool(_pool),
  osdriver(osd->store, coll_t(), OSD::make_snapmapper_oid()),
  snap_mapper(
    cct,
    &osdriver,
    p.ps(),
    p.get_split_bits(_pool.info.get_pg_num()),
    _pool.id,
    p.shard),
  last_persisted_osdmap(curmap->get_epoch()),
  deleting(false),
  trace_endpoint("0.0.0.0", 0, "PG"),
  dirty_info(false), dirty_big_info(false),
  info(p),
  info_struct_v(0),
  pg_log(cct),
  pgmeta_oid(p.make_pgmeta_oid()),
  missing_loc(this),
  stat_queue_item(this),
  scrub_queued(false),
  recovery_queued(false),
  recovery_ops_active(0),
  role(-1),
  state(0),
  send_notify(false),
  pg_whoami(osd->whoami, p.shard),
  need_up_thru(false),
  last_peering_reset(0),
  heartbeat_peer_lock("PG::heartbeat_peer_lock"),
  backfill_reserved(false),
  backfill_reserving(false),
  flushes_in_progress(0),
  pg_stats_publish_lock("PG::pg_stats_publish_lock"),
  pg_stats_publish_valid(false),
  finish_sync_event(NULL),
  backoff_lock("PG::backoff_lock"),
  scrub_after_recovery(false),
  active_pushes(0),
  recovery_state(this),
  peer_features(CEPH_FEATURES_SUPPORTED_DEFAULT),
  acting_features(CEPH_FEATURES_SUPPORTED_DEFAULT),
  upacting_features(CEPH_FEATURES_SUPPORTED_DEFAULT),
  last_epoch(0),
  last_require_osd_release(curmap->require_osd_release)
{
#ifdef PG_DEBUG_REFS
  osd->add_pgid(p, this);
#endif
#ifdef WITH_BLKIN
  std::stringstream ss;
  ss << "PG " << info.pgid;
  trace_endpoint.copy_name(ss.str());
#endif
}

PG::~PG()
{
  pgstate_history.set_pg_in_destructor();
#ifdef PG_DEBUG_REFS
  osd->remove_pgid(info.pgid, this);
#endif
}

void PG::lock(bool no_lockdep) const
{
  _lock.Lock(no_lockdep);
  // if we have unrecorded dirty state with the lock dropped, there is a bug
  ceph_assert(!dirty_info);
  ceph_assert(!dirty_big_info);

  dout(30) << "lock" << dendl;
}

std::ostream& PG::gen_prefix(std::ostream& out) const
{
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
  return out;
}
  
/********* PG **********/

void PG::proc_master_log(
  ObjectStore::Transaction& t, pg_info_t &oinfo,
  pg_log_t &olog, pg_missing_t& omissing, pg_shard_t from)
{
  dout(10) << "proc_master_log for osd." << from << ": "
	   << olog << " " << omissing << dendl;
  ceph_assert(!is_peered() && is_primary());

  // merge log into our own log to build master log.  no need to
  // make any adjustments to their missing map; we are taking their
  // log to be authoritative (i.e., their entries are by definitely
  // non-divergent).
  merge_log(t, oinfo, olog, from);
  peer_info[from] = oinfo;
  dout(10) << " peer osd." << from << " now " << oinfo << " " << omissing << dendl;
  might_have_unfound.insert(from);

  // See doc/dev/osd_internals/last_epoch_started
  if (oinfo.last_epoch_started > info.last_epoch_started) {
    info.last_epoch_started = oinfo.last_epoch_started;
    dirty_info = true;
  }
  if (oinfo.last_interval_started > info.last_interval_started) {
    info.last_interval_started = oinfo.last_interval_started;
    dirty_info = true;
  }
  update_history(oinfo.history);
  ceph_assert(cct->_conf->osd_find_best_info_ignore_history_les ||
	 info.last_epoch_started >= info.history.last_epoch_started);

  peer_missing[from].claim(omissing);
}
    
void PG::proc_replica_log(
  pg_info_t &oinfo,
  const pg_log_t &olog,
  pg_missing_t& omissing,
  pg_shard_t from)
{
  dout(10) << "proc_replica_log for osd." << from << ": "
	   << oinfo << " " << olog << " " << omissing << dendl;

  pg_log.proc_replica_log(oinfo, olog, omissing, from);

  peer_info[from] = oinfo;
  dout(10) << " peer osd." << from << " now " << oinfo << " " << omissing << dendl;
  might_have_unfound.insert(from);

  for (map<hobject_t, pg_missing_item>::const_iterator i =
	 omissing.get_items().begin();
       i != omissing.get_items().end();
       ++i) {
    dout(20) << " after missing " << i->first << " need " << i->second.need
	     << " have " << i->second.have << dendl;
  }
  peer_missing[from].claim(omissing);
}

bool PG::proc_replica_info(
  pg_shard_t from, const pg_info_t &oinfo, epoch_t send_epoch)
{
  map<pg_shard_t, pg_info_t>::iterator p = peer_info.find(from);
  if (p != peer_info.end() && p->second.last_update == oinfo.last_update) {
    dout(10) << " got dup osd." << from << " info " << oinfo << ", identical to ours" << dendl;
    return false;
  }

  if (!get_osdmap()->has_been_up_since(from.osd, send_epoch)) {
    dout(10) << " got info " << oinfo << " from down osd." << from
	     << " discarding" << dendl;
    return false;
  }

  dout(10) << " got osd." << from << " " << oinfo << dendl;
  ceph_assert(is_primary());
  peer_info[from] = oinfo;
  might_have_unfound.insert(from);

  update_history(oinfo.history);
  
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
      ceph_abort();
    }
  }
}

void PG::update_object_snap_mapping(
  ObjectStore::Transaction *t, const hobject_t &soid, const set<snapid_t> &snaps)
{
  OSDriver::OSTransaction _t(osdriver.get_transaction(t));
  ceph_assert(soid.snap < CEPH_MAXSNAP);
  int r = snap_mapper.remove_oid(
    soid,
    &_t);
  if (!(r == 0 || r == -ENOENT)) {
    derr << __func__ << ": remove_oid returned " << cpp_strerror(r) << dendl;
    ceph_abort();
  }
  snap_mapper.add_oid(
    soid,
    snaps,
    &_t);
}

void PG::merge_log(
  ObjectStore::Transaction& t, pg_info_t &oinfo, pg_log_t &olog, pg_shard_t from)
{
  PGLogEntryHandler rollbacker{this, &t};
  pg_log.merge_log(
    oinfo, olog, from, info, &rollbacker, dirty_info, dirty_big_info);
}

void PG::rewind_divergent_log(ObjectStore::Transaction& t, eversion_t newhead)
{
  PGLogEntryHandler rollbacker{this, &t};
  pg_log.rewind_divergent_log(
    newhead, info, &rollbacker, dirty_info, dirty_big_info);
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
  uint64_t num_unfound_before = missing_loc.num_unfound();
  bool found_missing = missing_loc.add_source_info(
    from, oinfo, omissing, ctx->handle);
  if (found_missing && num_unfound_before != missing_loc.num_unfound())
    publish_stats_to_osd();
  // avoid doing this if the peer is empty.  This is abit of paranoia
  // to avoid doing something rash if add_source_info() above
  // incorrectly decided we found something new. (if the peer has
  // last_update=0'0 that's impossible.)
  if (found_missing &&
      oinfo.last_update != eversion_t()) {
    pg_info_t tinfo(oinfo);
    tinfo.pgid.shard = pg_whoami.shard;
    (*(ctx->info_map))[from.osd].emplace_back(
	pg_notify_t(
	  from.shard, pg_whoami.shard,
	  get_osdmap_epoch(),
	  get_osdmap_epoch(),
	  tinfo),
	past_intervals);
  }
  return found_missing;
}


// MissingLoc

bool PG::MissingLoc::readable_with_acting(
  const hobject_t &hoid,
  const set<pg_shard_t> &acting) const {
  if (!needs_recovery(hoid))
    return true;
  if (is_deleted(hoid))
    return false;
  auto missing_loc_entry = missing_loc.find(hoid);
  if (missing_loc_entry == missing_loc.end())
    return false;
  const set<pg_shard_t> &locs = missing_loc_entry->second;
  ldout(pg->cct, 10) << __func__ << ": locs:" << locs << dendl;
  set<pg_shard_t> have_acting;
  for (set<pg_shard_t>::const_iterator i = locs.begin();
       i != locs.end();
       ++i) {
    if (acting.count(*i))
      have_acting.insert(*i);
  }
  return (*is_readable)(have_acting);
}

void PG::MissingLoc::add_batch_sources_info(
  const set<pg_shard_t> &sources, ThreadPool::TPHandle* handle)
{
  ldout(pg->cct, 10) << __func__ << ": adding sources in batch "
		     << sources.size() << dendl;
  unsigned loop = 0;
  bool sources_updated = false;
  for (map<hobject_t, pg_missing_item>::const_iterator i = needs_recovery_map.begin();
      i != needs_recovery_map.end();
      ++i) {
    if (handle && ++loop >= pg->cct->_conf->osd_loop_before_reset_tphandle) {
      handle->reset_tp_timeout();
      loop = 0;
    }
    if (i->second.is_delete())
      continue;

    auto p = missing_loc.find(i->first);
    if (p == missing_loc.end()) {
      p = missing_loc.emplace(i->first, set<pg_shard_t>()).first;
    } else {
      _dec_count(p->second);
    }
    missing_loc[i->first].insert(sources.begin(), sources.end());
    _inc_count(p->second);

    if (!sources_updated) {
      missing_loc_sources.insert(sources.begin(), sources.end());
      sources_updated = true;
    }
  }
}

bool PG::MissingLoc::add_source_info(
  pg_shard_t fromosd,
  const pg_info_t &oinfo,
  const pg_missing_t &omissing,
  ThreadPool::TPHandle* handle)
{
  bool found_missing = false;
  unsigned loop = 0;
  bool sources_updated = false;
  // found items?
  for (map<hobject_t,pg_missing_item>::const_iterator p = needs_recovery_map.begin();
       p != needs_recovery_map.end();
       ++p) {
    const hobject_t &soid(p->first);
    eversion_t need = p->second.need;
    if (handle && ++loop >= pg->cct->_conf->osd_loop_before_reset_tphandle) {
      handle->reset_tp_timeout();
      loop = 0;
    }
    if (p->second.is_delete()) {
      ldout(pg->cct, 10) << __func__ << " " << soid
			 << " delete, ignoring source" << dendl;
      continue;
    }
    if (oinfo.last_update < need) {
      ldout(pg->cct, 10) << "search_for_missing " << soid << " " << need
			 << " also missing on osd." << fromosd
			 << " (last_update " << oinfo.last_update
			 << " < needed " << need << ")" << dendl;
      continue;
    }
    if (!oinfo.last_backfill.is_max() &&
	!oinfo.last_backfill_bitwise) {
      ldout(pg->cct, 10) << "search_for_missing " << soid << " " << need
			 << " also missing on osd." << fromosd
			 << " (last_backfill " << oinfo.last_backfill
			 << " but with wrong sort order)"
			 << dendl;
      continue;
    }
    if (p->first >= oinfo.last_backfill) {
      // FIXME: this is _probably_ true, although it could conceivably
      // be in the undefined region!  Hmm!
      ldout(pg->cct, 10) << "search_for_missing " << soid << " " << need
			 << " also missing on osd." << fromosd
			 << " (past last_backfill " << oinfo.last_backfill
			 << ")" << dendl;
      continue;
    }
    if (omissing.is_missing(soid)) {
      ldout(pg->cct, 10) << "search_for_missing " << soid << " " << need
			 << " also missing on osd." << fromosd << dendl;
      continue;
    }

    ldout(pg->cct, 10) << "search_for_missing " << soid << " " << need
		       << " is on osd." << fromosd << dendl;

    {
      auto p = missing_loc.find(soid);
      if (p == missing_loc.end()) {
	p = missing_loc.emplace(soid, set<pg_shard_t>()).first;
      } else {
	_dec_count(p->second);
      }
      p->second.insert(fromosd);
      _inc_count(p->second);
    }

    if (!sources_updated) {
      missing_loc_sources.insert(fromosd);
      sources_updated = true;
    }
    found_missing = true;
  }

  ldout(pg->cct, 20) << "needs_recovery_map missing " << needs_recovery_map
		     << dendl;
  return found_missing;
}

void PG::MissingLoc::check_recovery_sources(const OSDMapRef& osdmap)
{
  set<pg_shard_t> now_down;
  for (set<pg_shard_t>::iterator p = missing_loc_sources.begin();
       p != missing_loc_sources.end();
       ) {
    if (osdmap->is_up(p->osd)) {
      ++p;
      continue;
    }
    ldout(pg->cct, 10) << __func__ << " source osd." << *p << " now down" << dendl;
    now_down.insert(*p);
    missing_loc_sources.erase(p++);
  }

  if (now_down.empty()) {
    ldout(pg->cct, 10) << __func__ << " no source osds (" << missing_loc_sources << ") went down" << dendl;
  } else {
    ldout(pg->cct, 10) << __func__ << " sources osds " << now_down << " now down, remaining sources are "
		       << missing_loc_sources << dendl;
    
    // filter missing_loc
    map<hobject_t, set<pg_shard_t>>::iterator p = missing_loc.begin();
    while (p != missing_loc.end()) {
      set<pg_shard_t>::iterator q = p->second.begin();
      bool changed = false;
      while (q != p->second.end()) {
	if (now_down.count(*q)) {
	  if (!changed) {
	    changed = true;
	    _dec_count(p->second);
	  }
	  p->second.erase(q++);
	} else {
	  ++q;
	}
      }
      if (p->second.empty()) {
	missing_loc.erase(p++);
      } else {
	if (changed) {
	  _inc_count(p->second);
	}
	++p;
      }
    }
  }
}
  
void PG::discover_all_missing(map<int, map<spg_t,pg_query_t> > &query_map)
{
  auto &missing = pg_log.get_missing();
  uint64_t unfound = get_num_unfound();

  dout(10) << __func__ << " "
	   << missing.num_missing() << " missing, "
	   << unfound << " unfound"
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
	info.history, get_osdmap_epoch());
  }
}

/******* PG ***********/
bool PG::needs_recovery() const
{
  ceph_assert(is_primary());

  auto &missing = pg_log.get_missing();

  if (missing.num_missing()) {
    dout(10) << __func__ << " primary has " << missing.num_missing()
      << " missing" << dendl;
    return true;
  }

  ceph_assert(!acting_recovery_backfill.empty());
  set<pg_shard_t>::const_iterator end = acting_recovery_backfill.end();
  set<pg_shard_t>::const_iterator a = acting_recovery_backfill.begin();
  for (; a != end; ++a) {
    if (*a == get_primary()) continue;
    pg_shard_t peer = *a;
    map<pg_shard_t, pg_missing_t>::const_iterator pm = peer_missing.find(peer);
    if (pm == peer_missing.end()) {
      dout(10) << __func__ << " osd." << peer << " doesn't have missing set"
        << dendl;
      continue;
    }
    if (pm->second.num_missing()) {
      dout(10) << __func__ << " osd." << peer << " has "
        << pm->second.num_missing() << " missing" << dendl;
      return true;
    }
  }

  dout(10) << __func__ << " is recovered" << dendl;
  return false;
}

bool PG::needs_backfill() const
{
  ceph_assert(is_primary());

  // We can assume that only possible osds that need backfill
  // are on the backfill_targets vector nodes.
  set<pg_shard_t>::const_iterator end = backfill_targets.end();
  set<pg_shard_t>::const_iterator a = backfill_targets.begin();
  for (; a != end; ++a) {
    pg_shard_t peer = *a;
    map<pg_shard_t, pg_info_t>::const_iterator pi = peer_info.find(peer);
    if (!pi->second.last_backfill.is_max()) {
      dout(10) << __func__ << " osd." << peer << " has last_backfill " << pi->second.last_backfill << dendl;
      return true;
    }
  }

  dout(10) << __func__ << " does not need backfill" << dendl;
  return false;
}


void PG::check_past_interval_bounds() const
{
  auto rpib = get_required_past_interval_bounds(
    info,
    osd->get_superblock().oldest_map);
  if (rpib.first >= rpib.second) {
    if (!past_intervals.empty()) {
      osd->clog->error() << info.pgid << " required past_interval bounds are"
			 << " empty [" << rpib << ") but past_intervals is not: "
			 << past_intervals;
      derr << info.pgid << " required past_interval bounds are"
	   << " empty [" << rpib << ") but past_intervals is not: "
	   << past_intervals << dendl;
    }
  } else {
    if (past_intervals.empty()) {
      osd->clog->error() << info.pgid << " required past_interval bounds are"
			 << " not empty [" << rpib << ") but past_intervals "
			 << past_intervals << " is empty";
      derr << info.pgid << " required past_interval bounds are"
	   << " not empty [" << rpib << ") but past_intervals "
	   << past_intervals << " is empty" << dendl;
      ceph_assert(!past_intervals.empty());
    }

    auto apib = past_intervals.get_bounds();
    if (apib.first > rpib.first) {
      osd->clog->error() << info.pgid << " past_intervals [" << apib
			 << ") start interval does not contain the required"
			 << " bound [" << rpib << ") start";
      derr << info.pgid << " past_intervals [" << apib
	   << ") start interval does not contain the required"
	   << " bound [" << rpib << ") start" << dendl;
      ceph_abort_msg("past_interval start interval mismatch");
    }
    if (apib.second != rpib.second) {
      osd->clog->error() << info.pgid << " past_interal bound [" << apib
			 << ") end does not match required [" << rpib
			 << ") end";
      derr << info.pgid << " past_interal bound [" << apib
	   << ") end does not match required [" << rpib
	   << ") end" << dendl;
      ceph_abort_msg("past_interval end mismatch");
    }
  }
}

bool PG::adjust_need_up_thru(const OSDMapRef osdmap)
{
  epoch_t up_thru = osdmap->get_up_thru(osd->whoami);
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
      peer_purged.erase(p->first); // so we can re-purge if necessary
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
  ceph_assert(is_primary());

  set<pg_shard_t>::const_iterator peer = might_have_unfound.begin();
  set<pg_shard_t>::const_iterator mend = might_have_unfound.end();
  for (; peer != mend; ++peer) {
    if (peer_missing.count(*peer))
      continue;
    map<pg_shard_t, pg_info_t>::const_iterator iter = peer_info.find(*peer);
    if (iter != peer_info.end() &&
        (iter->second.is_empty() || iter->second.dne()))
      continue;
    if (!osdmap->exists(peer->osd))
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

PastIntervals::PriorSet PG::build_prior()
{
  if (1) {
    // sanity check
    for (map<pg_shard_t,pg_info_t>::iterator it = peer_info.begin();
	 it != peer_info.end();
	 ++it) {
      ceph_assert(info.history.last_epoch_started >= it->second.history.last_epoch_started);
    }
  }

  const OSDMap &osdmap = *get_osdmap();
  PastIntervals::PriorSet prior = past_intervals.get_prior_set(
    pool.info.is_erasure(),
    info.history.last_epoch_started,
    get_pgbackend()->get_is_recoverable_predicate(),
    [&](epoch_t start, int osd, epoch_t *lost_at) {
      const osd_info_t *pinfo = 0;
      if (osdmap.exists(osd)) {
	pinfo = &osdmap.get_info(osd);
	if (lost_at)
	  *lost_at = pinfo->lost_at;
      }

      if (osdmap.is_up(osd)) {
	return PastIntervals::UP;
      } else if (!pinfo) {
	return PastIntervals::DNE;
      } else if (pinfo->lost_at > start) {
	return PastIntervals::LOST;
      } else {
	return PastIntervals::DOWN;
      }
    },
    up,
    acting,
    this);
				 
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
  set_probe_targets(prior.probe);
  return prior;
}

void PG::clear_primary_state()
{
  dout(10) << "clear_primary_state" << dendl;

  // clear peering state
  stray_set.clear();
  peer_log_requested.clear();
  peer_missing_requested.clear();
  peer_info.clear();
  peer_bytes.clear();
  peer_missing.clear();
  need_up_thru = false;
  peer_last_complete_ondisk.clear();
  peer_activated.clear();
  min_last_complete_ondisk = eversion_t();
  pg_trim_to = eversion_t();
  might_have_unfound.clear();
  projected_log = PGLog::IndexedLog();

  last_update_ondisk = eversion_t();

  snap_trimq.clear();

  finish_sync_event = 0;  // so that _finish_recovery doesn't go off in another thread

  missing_loc.clear();

  release_pg_backoffs();

  pg_log.reset_recovery_pointers();

  scrubber.reserved_peers.clear();
  scrub_after_recovery = false;

  agent_clear();
}

PG::Scrubber::Scrubber()
 : reserved(false), reserve_failed(false),
   epoch_start(0),
   active(false),
   shallow_errors(0), deep_errors(0), fixed(0),
   must_scrub(false), must_deep_scrub(false), must_repair(false),
   auto_repair(false),
   check_repair(false),
   deep_scrub_on_error(false),
   num_digest_updates_pending(0),
   state(INACTIVE),
   deep(false)
{}

PG::Scrubber::~Scrubber() {}

/**
 * find_best_info
 *
 * Returns an iterator to the best info in infos sorted by:
 *  1) Prefer newer last_update
 *  2) Prefer longer tail if it brings another info into contiguity
 *  3) Prefer current primary
 */
map<pg_shard_t, pg_info_t>::const_iterator PG::find_best_info(
  const map<pg_shard_t, pg_info_t> &infos,
  bool restrict_to_up_acting,
  bool *history_les_bound) const
{
  ceph_assert(history_les_bound);
  /* See doc/dev/osd_internals/last_epoch_started.rst before attempting
   * to make changes to this process.  Also, make sure to update it
   * when you find bugs! */
  eversion_t min_last_update_acceptable = eversion_t::max();
  epoch_t max_last_epoch_started_found = 0;
  for (map<pg_shard_t, pg_info_t>::const_iterator i = infos.begin();
       i != infos.end();
       ++i) {
    if (!cct->_conf->osd_find_best_info_ignore_history_les &&
	max_last_epoch_started_found < i->second.history.last_epoch_started) {
      *history_les_bound = true;
      max_last_epoch_started_found = i->second.history.last_epoch_started;
    }
    if (!i->second.is_incomplete() &&
	max_last_epoch_started_found < i->second.last_epoch_started) {
      *history_les_bound = false;
      max_last_epoch_started_found = i->second.last_epoch_started;
    }
  }
  for (map<pg_shard_t, pg_info_t>::const_iterator i = infos.begin();
       i != infos.end();
       ++i) {
    if (max_last_epoch_started_found <= i->second.last_epoch_started) {
      if (min_last_update_acceptable > i->second.last_update)
	min_last_update_acceptable = i->second.last_update;
    }
  }
  if (min_last_update_acceptable == eversion_t::max())
    return infos.end();

  map<pg_shard_t, pg_info_t>::const_iterator best = infos.end();
  // find osd with newest last_update (oldest for ec_pool).
  // if there are multiples, prefer
  //  - a longer tail, if it brings another peer into log contiguity
  //  - the current primary
  for (map<pg_shard_t, pg_info_t>::const_iterator p = infos.begin();
       p != infos.end();
       ++p) {
    if (restrict_to_up_acting && !is_up(p->first) &&
	!is_acting(p->first))
      continue;
    // Only consider peers with last_update >= min_last_update_acceptable
    if (p->second.last_update < min_last_update_acceptable)
      continue;
    // Disqualify anyone with a too old last_epoch_started
    if (p->second.last_epoch_started < max_last_epoch_started_found)
      continue;
    // Disqualify anyone who is incomplete (not fully backfilled)
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

    if (!p->second.has_missing() && best->second.has_missing()) {
      dout(10) << __func__ << " prefer osd." << p->first
               << " because it is complete while best has missing"
               << dendl;
      best = p;
      continue;
    } else if (p->second.has_missing() && !best->second.has_missing()) {
      dout(10) << __func__ << " skipping osd." << p->first
               << " because it has missing while best is complete"
               << dendl;
      continue;
    } else {
      // both are complete or have missing
      // fall through
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
  const vector<int> &up,
  const map<pg_shard_t, pg_info_t> &all_info,
  bool restrict_to_up_acting,
  vector<int> *_want,
  set<pg_shard_t> *backfill,
  set<pg_shard_t> *acting_backfill,
  ostream &ss)
{
  vector<int> want(size, CRUSH_ITEM_NONE);
  map<shard_id_t, set<pg_shard_t> > all_info_by_shard;
  for (map<pg_shard_t, pg_info_t>::const_iterator i = all_info.begin();
       i != all_info.end();
       ++i) {
    all_info_by_shard[i->first.shard].insert(i->first);
  }
  for (uint8_t i = 0; i < want.size(); ++i) {
    ss << "For position " << (unsigned)i << ": ";
    if (up.size() > (unsigned)i && up[i] != CRUSH_ITEM_NONE &&
	!all_info.find(pg_shard_t(up[i], shard_id_t(i)))->second.is_incomplete() &&
	all_info.find(pg_shard_t(up[i], shard_id_t(i)))->second.last_update >=
	auth_log_shard->second.log_tail) {
      ss << " selecting up[i]: " << pg_shard_t(up[i], shard_id_t(i)) << std::endl;
      want[i] = up[i];
      continue;
    }
    if (up.size() > (unsigned)i && up[i] != CRUSH_ITEM_NONE) {
      ss << " backfilling up[i]: " << pg_shard_t(up[i], shard_id_t(i))
	 << " and ";
      backfill->insert(pg_shard_t(up[i], shard_id_t(i)));
    }

    if (acting.size() > (unsigned)i && acting[i] != CRUSH_ITEM_NONE &&
	!all_info.find(pg_shard_t(acting[i], shard_id_t(i)))->second.is_incomplete() &&
	all_info.find(pg_shard_t(acting[i], shard_id_t(i)))->second.last_update >=
	auth_log_shard->second.log_tail) {
      ss << " selecting acting[i]: " << pg_shard_t(acting[i], shard_id_t(i)) << std::endl;
      want[i] = acting[i];
    } else if (!restrict_to_up_acting) {
      for (set<pg_shard_t>::iterator j = all_info_by_shard[shard_id_t(i)].begin();
	   j != all_info_by_shard[shard_id_t(i)].end();
	   ++j) {
	ceph_assert(j->shard == i);
	if (!all_info.find(*j)->second.is_incomplete() &&
	    all_info.find(*j)->second.last_update >=
	    auth_log_shard->second.log_tail) {
	  ss << " selecting stray: " << *j << std::endl;
	  want[i] = j->osd;
	  break;
	}
      }
      if (want[i] == CRUSH_ITEM_NONE)
	ss << " failed to fill position " << (int)i << std::endl;
    }
  }

  for (uint8_t i = 0; i < want.size(); ++i) {
    if (want[i] != CRUSH_ITEM_NONE) {
      acting_backfill->insert(pg_shard_t(want[i], shard_id_t(i)));
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
  uint64_t force_auth_primary_missing_objects,
  unsigned size,
  const vector<int> &acting,
  const vector<int> &up,
  pg_shard_t up_primary,
  const map<pg_shard_t, pg_info_t> &all_info,
  bool restrict_to_up_acting,
  vector<int> *want,
  set<pg_shard_t> *backfill,
  set<pg_shard_t> *acting_backfill,
  const OSDMapRef osdmap,
  ostream &ss)
{
  pg_shard_t auth_log_shard_id = auth_log_shard->first;

  ss << __func__ << " newest update on osd." << auth_log_shard_id
     << " with " << auth_log_shard->second
     << (restrict_to_up_acting ? " restrict_to_up_acting" : "") << std::endl;
  
  // select primary
  auto primary = all_info.find(up_primary);
  if (up.size() &&
      !primary->second.is_incomplete() &&
      primary->second.last_update >=
        auth_log_shard->second.log_tail) {
    if (HAVE_FEATURE(osdmap->get_up_osd_features(), SERVER_NAUTILUS)) {
      auto approx_missing_objects =
        primary->second.stats.stats.sum.num_objects_missing;
      auto auth_version = auth_log_shard->second.last_update.version;
      auto primary_version = primary->second.last_update.version;
      if (auth_version > primary_version) {
        approx_missing_objects += auth_version - primary_version;
      } else {
        approx_missing_objects += primary_version - auth_version;
      }
      if ((uint64_t)approx_missing_objects >
          force_auth_primary_missing_objects) {
        primary = auth_log_shard;
        ss << "up_primary: " << up_primary << ") has approximate "
           << approx_missing_objects
           << "(>" << force_auth_primary_missing_objects <<") "
           << "missing objects, osd." << auth_log_shard_id
           << " selected as primary instead"
           << std::endl;
      } else {
        ss << "up_primary: " << up_primary << ") selected as primary"
           << std::endl;
      }
    } else {
      ss << "up_primary: " << up_primary << ") selected as primary" << std::endl;
    }
  } else {
    ceph_assert(!auth_log_shard->second.is_incomplete());
    ss << "up[0] needs backfill, osd." << auth_log_shard_id
       << " selected as primary instead" << std::endl;
    primary = auth_log_shard;
  }

  ss << __func__ << " primary is osd." << primary->first
     << " with " << primary->second << std::endl;
  want->push_back(primary->first.osd);
  acting_backfill->insert(primary->first);

  /* We include auth_log_shard->second.log_tail because in GetLog,
   * we will request logs back to the min last_update over our
   * acting_backfill set, which will result in our log being extended
   * as far backwards as necessary to pick up any peers which can
   * be log recovered by auth_log_shard's log */
  eversion_t oldest_auth_log_entry =
    std::min(primary->second.log_tail, auth_log_shard->second.log_tail);

  // select replicas that have log contiguity with primary.
  // prefer up, then acting, then any peer_info osds
  for (auto i : up) {
    pg_shard_t up_cand = pg_shard_t(i, shard_id_t::NO_SHARD);
    if (up_cand == primary->first)
      continue;
    const pg_info_t &cur_info = all_info.find(up_cand)->second;
    if (cur_info.is_incomplete() ||
        cur_info.last_update < oldest_auth_log_entry) {
      ss << " shard " << up_cand << " (up) backfill " << cur_info << std::endl;
      backfill->insert(up_cand);
      acting_backfill->insert(up_cand);
    } else {
      want->push_back(i);
      acting_backfill->insert(up_cand);
      ss << " osd." << i << " (up) accepted " << cur_info << std::endl;
    }
    if (want->size() >= size) {
      break;
    }
  }

  if (want->size() >= size) {
    return;
  }

  std::vector<std::pair<eversion_t, int>> candidate_by_last_update;
  candidate_by_last_update.reserve(acting.size());
  // This no longer has backfill OSDs, but they are covered above.
  for (auto i : acting) {
    pg_shard_t acting_cand(i, shard_id_t::NO_SHARD);
    // skip up osds we already considered above
    if (acting_cand == primary->first)
      continue;
    vector<int>::const_iterator up_it = find(up.begin(), up.end(), i);
    if (up_it != up.end())
      continue;

    const pg_info_t &cur_info = all_info.find(acting_cand)->second;
    if (cur_info.is_incomplete() ||
	cur_info.last_update < oldest_auth_log_entry) {
      ss << " shard " << acting_cand << " (acting) REJECTED "
	 << cur_info << std::endl;
    } else {
      candidate_by_last_update.emplace_back(cur_info.last_update, i);
    }
  }

  auto sort_by_eversion =[](const std::pair<eversion_t, int> &lhs,
                            const std::pair<eversion_t, int> &rhs) {
    return lhs.first > rhs.first;
  };
  // sort by last_update, in descending order.
  std::sort(candidate_by_last_update.begin(),
            candidate_by_last_update.end(), sort_by_eversion);
  for (auto &p: candidate_by_last_update) {
    ceph_assert(want->size() < size);
    want->push_back(p.second);
    pg_shard_t s = pg_shard_t(p.second, shard_id_t::NO_SHARD);
    acting_backfill->insert(s);
    ss << " shard " << s << " (acting) accepted "
       << all_info.find(s)->second << std::endl;
    if (want->size() >= size) {
      return;
    }
  }

  if (restrict_to_up_acting) {
    return;
  }
  candidate_by_last_update.clear();
  candidate_by_last_update.reserve(all_info.size()); // overestimate but fine
  // continue to search stray to find more suitable peers
  for (auto &i : all_info) {
    // skip up osds we already considered above
    if (i.first == primary->first)
      continue;
    vector<int>::const_iterator up_it = find(up.begin(), up.end(), i.first.osd);
    if (up_it != up.end())
      continue;
    vector<int>::const_iterator acting_it = find(
      acting.begin(), acting.end(), i.first.osd);
    if (acting_it != acting.end())
      continue;

    if (i.second.is_incomplete() ||
	i.second.last_update < oldest_auth_log_entry) {
      ss << " shard " << i.first << " (stray) REJECTED " << i.second
         << std::endl;
    } else {
      candidate_by_last_update.emplace_back(i.second.last_update, i.first.osd);
    }
  }

  if (candidate_by_last_update.empty()) {
    // save us some effort
    return;
  }

  // sort by last_update, in descending order.
  std::sort(candidate_by_last_update.begin(),
            candidate_by_last_update.end(), sort_by_eversion);

  for (auto &p: candidate_by_last_update) {
    ceph_assert(want->size() < size);
    want->push_back(p.second);
    pg_shard_t s = pg_shard_t(p.second, shard_id_t::NO_SHARD);
    acting_backfill->insert(s);
    ss << " shard " << s << " (stray) accepted "
       << all_info.find(s)->second << std::endl;
    if (want->size() >= size) {
      return;
    }
  }
}

bool PG::recoverable_and_ge_min_size(const vector<int> &want) const
{
  unsigned num_want_acting = 0;
  set<pg_shard_t> have;
  for (int i = 0; i < (int)want.size(); ++i) {
    if (want[i] != CRUSH_ITEM_NONE) {
      ++num_want_acting;
      have.insert(
        pg_shard_t(
          want[i],
          pool.info.is_erasure() ? shard_id_t(i) : shard_id_t::NO_SHARD));
    }
  }
  // We go incomplete if below min_size for ec_pools since backfill
  // does not currently maintain rollbackability
  // Otherwise, we will go "peered", but not "active"
  if (num_want_acting < pool.info.min_size &&
      (pool.info.is_erasure() ||
       !cct->_conf->osd_allow_recovery_below_min_size)) {
    dout(10) << __func__ << " failed, below min size" << dendl;
    return false;
  }

  /* Check whether we have enough acting shards to later perform recovery */
  boost::scoped_ptr<IsPGRecoverablePredicate> recoverable_predicate(
      get_pgbackend()->get_is_recoverable_predicate());
  if (!(*recoverable_predicate)(have)) {
    dout(10) << __func__ << " failed, not recoverable" << dendl;
    return false;
  }

  return true;
}

void PG::choose_async_recovery_ec(const map<pg_shard_t, pg_info_t> &all_info,
                                  const pg_info_t &auth_info,
                                  vector<int> *want,
                                  set<pg_shard_t> *async_recovery) const
{
  set<pair<int, pg_shard_t> > candidates_by_cost;
  for (uint8_t i = 0; i < want->size(); ++i) {
    if ((*want)[i] == CRUSH_ITEM_NONE)
      continue;

    // Considering log entries to recover is accurate enough for
    // now. We could use minimum_to_decode_with_cost() later if
    // necessary.
    pg_shard_t shard_i((*want)[i], shard_id_t(i));
    // do not include strays
    if (stray_set.find(shard_i) != stray_set.end())
      continue;
    // Do not include an osd that is not up, since choosing it as
    // an async_recovery_target will move it out of the acting set.
    // This results in it being identified as a stray during peering,
    // because it is no longer in the up or acting set.
    if (!is_up(shard_i))
      continue;
    auto shard_info = all_info.find(shard_i)->second;
    // for ec pools we rollback all entries past the authoritative
    // last_update *before* activation. This is relatively inexpensive
    // compared to recovery, since it is purely local, so treat shards
    // past the authoritative last_update the same as those equal to it.
    version_t auth_version = auth_info.last_update.version;
    version_t candidate_version = shard_info.last_update.version;
    auto approx_missing_objects =
      shard_info.stats.stats.sum.num_objects_missing;
    if (auth_version > candidate_version) {
      approx_missing_objects += auth_version - candidate_version;
    }
    if (static_cast<uint64_t>(approx_missing_objects) >
	cct->_conf.get_val<uint64_t>("osd_async_recovery_min_cost")) {
      candidates_by_cost.emplace(approx_missing_objects, shard_i);
    }
  }

  dout(20) << __func__ << " candidates by cost are: " << candidates_by_cost
           << dendl;

  // take out as many osds as we can for async recovery, in order of cost
  for (auto rit = candidates_by_cost.rbegin();
       rit != candidates_by_cost.rend(); ++rit) {
    pg_shard_t cur_shard = rit->second;
    vector<int> candidate_want(*want);
    candidate_want[cur_shard.shard.id] = CRUSH_ITEM_NONE;
    if (recoverable_and_ge_min_size(candidate_want)) {
      want->swap(candidate_want);
      async_recovery->insert(cur_shard);
    }
  }
  dout(20) << __func__ << " result want=" << *want
           << " async_recovery=" << *async_recovery << dendl;
}

void PG::choose_async_recovery_replicated(const map<pg_shard_t, pg_info_t> &all_info,
                                          const pg_info_t &auth_info,
                                          vector<int> *want,
                                          set<pg_shard_t> *async_recovery) const
{
  set<pair<int, pg_shard_t> > candidates_by_cost;
  for (auto osd_num : *want) {
    pg_shard_t shard_i(osd_num, shard_id_t::NO_SHARD);
    // do not include strays
    if (stray_set.find(shard_i) != stray_set.end())
      continue;
    // Do not include an osd that is not up, since choosing it as
    // an async_recovery_target will move it out of the acting set.
    // This results in it being identified as a stray during peering,
    // because it is no longer in the up or acting set.
    if (!is_up(shard_i))
      continue;
    auto shard_info = all_info.find(shard_i)->second;
    // use the approximate magnitude of the difference in length of
    // logs plus historical missing objects as the cost of recovery
    version_t auth_version = auth_info.last_update.version;
    version_t candidate_version = shard_info.last_update.version;
    auto approx_missing_objects =
      shard_info.stats.stats.sum.num_objects_missing;
    if (auth_version > candidate_version) {
      approx_missing_objects += auth_version - candidate_version;
    } else {
      approx_missing_objects += candidate_version - auth_version;
    }
    if (static_cast<uint64_t>(approx_missing_objects)  >
	cct->_conf.get_val<uint64_t>("osd_async_recovery_min_cost")) {
      candidates_by_cost.emplace(approx_missing_objects, shard_i);
    }
  }

  dout(20) << __func__ << " candidates by cost are: " << candidates_by_cost
           << dendl;
  // take out as many osds as we can for async recovery, in order of cost
  for (auto rit = candidates_by_cost.rbegin();
       rit != candidates_by_cost.rend(); ++rit) {
    if (want->size() <= pool.info.min_size) {
      break;
    }
    pg_shard_t cur_shard = rit->second;
    vector<int> candidate_want(*want);
    for (auto it = candidate_want.begin(); it != candidate_want.end(); ++it) {
      if (*it == cur_shard.osd) {
        candidate_want.erase(it);
	want->swap(candidate_want);
	async_recovery->insert(cur_shard);
        break;
      }
    }
  }
  dout(20) << __func__ << " result want=" << *want
           << " async_recovery=" << *async_recovery << dendl;
}

/**
 * choose acting
 *
 * calculate the desired acting, and request a change with the monitor
 * if it differs from the current acting.
 *
 * if restrict_to_up_acting=true, we filter out anything that's not in
 * up/acting.  in order to lift this restriction, we need to
 *  1) check whether it's worth switching the acting set any time we get
 *     a new pg info (not just here, when recovery finishes)
 *  2) check whether anything in want_acting went down on each new map
 *     (and, if so, calculate a new want_acting)
 *  3) remove the assertion in PG::RecoveryState::Active::react(const AdvMap)
 * TODO!
 */
bool PG::choose_acting(pg_shard_t &auth_log_shard_id,
		       bool restrict_to_up_acting,
		       bool *history_les_bound)
{
  map<pg_shard_t, pg_info_t> all_info(peer_info.begin(), peer_info.end());
  all_info[pg_whoami] = info;

  if (cct->_conf->subsys.should_gather<dout_subsys, 10>()) {
    for (map<pg_shard_t, pg_info_t>::iterator p = all_info.begin();
         p != all_info.end();
         ++p) {
      dout(10) << __func__ << " all_info osd." << p->first << " " << p->second << dendl;
    }
  }

  map<pg_shard_t, pg_info_t>::const_iterator auth_log_shard =
    find_best_info(all_info, restrict_to_up_acting, history_les_bound);

  if (auth_log_shard == all_info.end()) {
    if (up != acting) {
      dout(10) << __func__ << " no suitable info found (incomplete backfills?),"
	       << " reverting to up" << dendl;
      want_acting = up;
      vector<int> empty;
      osd->queue_want_pg_temp(info.pgid.pgid, empty);
    } else {
      dout(10) << __func__ << " failed" << dendl;
      ceph_assert(want_acting.empty());
    }
    return false;
  }

  ceph_assert(!auth_log_shard->second.is_incomplete());
  auth_log_shard_id = auth_log_shard->first;

  set<pg_shard_t> want_backfill, want_acting_backfill;
  vector<int> want;
  stringstream ss;
  if (pool.info.is_replicated())
    calc_replicated_acting(
      auth_log_shard,
      cct->_conf.get_val<uint64_t>(
        "osd_force_auth_primary_missing_objects"),
      get_osdmap()->get_pg_size(info.pgid.pgid),
      acting,
      up,
      up_primary,
      all_info,
      restrict_to_up_acting,
      &want,
      &want_backfill,
      &want_acting_backfill,
      get_osdmap(),
      ss);
  else
    calc_ec_acting(
      auth_log_shard,
      get_osdmap()->get_pg_size(info.pgid.pgid),
      acting,
      up,
      all_info,
      restrict_to_up_acting,
      &want,
      &want_backfill,
      &want_acting_backfill,
      ss);
  dout(10) << ss.str() << dendl;

  if (!recoverable_and_ge_min_size(want)) {
    want_acting.clear();
    return false;
  }

  set<pg_shard_t> want_async_recovery;
  if (HAVE_FEATURE(get_osdmap()->get_up_osd_features(), SERVER_MIMIC)) {
    if (pool.info.is_erasure()) {
      choose_async_recovery_ec(all_info, auth_log_shard->second, &want, &want_async_recovery);
    } else {
      choose_async_recovery_replicated(all_info, auth_log_shard->second, &want, &want_async_recovery);
    }
  }
  if (want != acting) {
    dout(10) << __func__ << " want " << want << " != acting " << acting
	     << ", requesting pg_temp change" << dendl;
    want_acting = want;

    if (!cct->_conf->osd_debug_no_acting_change) {
      if (want_acting == up) {
	// There can't be any pending backfill if
	// want is the same as crush map up OSDs.
	ceph_assert(want_backfill.empty());
	vector<int> empty;
	osd->queue_want_pg_temp(info.pgid.pgid, empty);
      } else
	osd->queue_want_pg_temp(info.pgid.pgid, want);
    }
    return false;
  }
  want_acting.clear();
  acting_recovery_backfill = want_acting_backfill;
  dout(10) << "acting_recovery_backfill is " << acting_recovery_backfill << dendl;
  ceph_assert(backfill_targets.empty() || backfill_targets == want_backfill);
  if (backfill_targets.empty()) {
    // Caller is GetInfo
    backfill_targets = want_backfill;
  }
  // Adding !needs_recovery() to let the async_recovery_targets reset after recovery is complete
  ceph_assert(async_recovery_targets.empty() || async_recovery_targets == want_async_recovery || !needs_recovery());
  if (async_recovery_targets.empty() || !needs_recovery()) {
    async_recovery_targets = want_async_recovery;
  }
  // Will not change if already set because up would have had to change
  // Verify that nothing in backfill is in stray_set
  for (set<pg_shard_t>::iterator i = want_backfill.begin();
      i != want_backfill.end();
      ++i) {
    ceph_assert(stray_set.find(*i) == stray_set.end());
  }
  dout(10) << "choose_acting want=" << want << " backfill_targets="
           << want_backfill << " async_recovery_targets="
           << async_recovery_targets << dendl;
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
  ceph_assert(might_have_unfound.empty());
  ceph_assert(is_primary());

  dout(10) << __func__ << dendl;

  check_past_interval_bounds();

  might_have_unfound = past_intervals.get_might_have_unfound(
    pg_whoami,
    pool.info.is_erasure());

  // include any (stray) peers
  for (map<pg_shard_t, pg_info_t>::iterator p = peer_info.begin();
       p != peer_info.end();
       ++p)
    might_have_unfound.insert(p->first);

  dout(15) << __func__ << ": built " << might_have_unfound << dendl;
}

void PG::activate(ObjectStore::Transaction& t,
		  epoch_t activation_epoch,
		  map<int, map<spg_t,pg_query_t> >& query_map,
		  map<int,
		      vector<
			pair<pg_notify_t,
			     PastIntervals> > > *activator_map,
                  RecoveryCtx *ctx)
{
  ceph_assert(!is_peered());
  ceph_assert(scrubber.callbacks.empty());
  ceph_assert(callbacks_for_degraded_object.empty());

  // twiddle pg state
  state_clear(PG_STATE_DOWN);

  send_notify = false;

  if (is_primary()) {
    // only update primary last_epoch_started if we will go active
    if (acting.size() >= pool.info.min_size) {
      ceph_assert(cct->_conf->osd_find_best_info_ignore_history_les ||
	     info.last_epoch_started <= activation_epoch);
      info.last_epoch_started = activation_epoch;
      info.last_interval_started = info.history.same_interval_since;
    }
  } else if (is_acting(pg_whoami)) {
    /* update last_epoch_started on acting replica to whatever the primary sent
     * unless it's smaller (could happen if we are going peered rather than
     * active, see doc/dev/osd_internals/last_epoch_started.rst) */
    if (info.last_epoch_started < activation_epoch) {
      info.last_epoch_started = activation_epoch;
      info.last_interval_started = info.history.same_interval_since;
    }
  }

  auto &missing = pg_log.get_missing();

  if (is_primary()) {
    last_update_ondisk = info.last_update;
    min_last_complete_ondisk = eversion_t(0,0);  // we don't know (yet)!
  }
  last_update_applied = info.last_update;
  last_rollback_info_trimmed_to_applied = pg_log.get_can_rollback_to();

  need_up_thru = false;

  // write pg info, log
  dirty_info = true;
  dirty_big_info = true; // maybe

  // find out when we commit
  t.register_on_complete(
    new C_PG_ActivateCommitted(
      this,
      get_osdmap_epoch(),
      activation_epoch));
  
  if (is_primary()) {
    // initialize snap_trimq
    if (get_osdmap()->require_osd_release < CEPH_RELEASE_MIMIC) {
      dout(20) << "activate - purged_snaps " << info.purged_snaps
	       << " cached_removed_snaps " << pool.cached_removed_snaps
	       << dendl;
      snap_trimq = pool.cached_removed_snaps;
    } else {
      auto& removed_snaps_queue = get_osdmap()->get_removed_snaps_queue();
      auto p = removed_snaps_queue.find(info.pgid.pgid.pool());
      snap_trimq.clear();
      if (p != removed_snaps_queue.end()) {
	dout(20) << "activate - purged_snaps " << info.purged_snaps
		 << " removed_snaps " << p->second
		 << dendl;
	for (auto q : p->second) {
	  snap_trimq.insert(q.first, q.second);
	}
      }
    }
    interval_set<snapid_t> purged;
    purged.intersection_of(snap_trimq, info.purged_snaps);
    snap_trimq.subtract(purged);

    if (get_osdmap()->require_osd_release >= CEPH_RELEASE_MIMIC) {
      // adjust purged_snaps: PG may have been inactive while snaps were pruned
      // from the removed_snaps_queue in the osdmap.  update local purged_snaps
      // reflect only those snaps that we thought were pruned and were still in
      // the queue.
      info.purged_snaps.swap(purged);
    }
  }

  // init complete pointer
  if (missing.num_missing() == 0) {
    dout(10) << "activate - no missing, moving last_complete " << info.last_complete 
	     << " -> " << info.last_update << dendl;
    info.last_complete = info.last_update;
    info.stats.stats.sum.num_objects_missing = 0;
    pg_log.reset_recovery_pointers();
  } else {
    dout(10) << "activate - not complete, " << missing << dendl;
    info.stats.stats.sum.num_objects_missing = missing.num_missing();
    pg_log.activate_not_complete(info);
  }
    
  log_weirdness();

  // if primary..
  if (is_primary()) {
    ceph_assert(ctx);
    // start up replicas

    ceph_assert(!acting_recovery_backfill.empty());
    for (set<pg_shard_t>::iterator i = acting_recovery_backfill.begin();
	 i != acting_recovery_backfill.end();
	 ++i) {
      if (*i == pg_whoami) continue;
      pg_shard_t peer = *i;
      ceph_assert(peer_info.count(peer));
      pg_info_t& pi = peer_info[peer];

      dout(10) << "activate peer osd." << peer << " " << pi << dendl;

      MOSDPGLog *m = 0;
      ceph_assert(peer_missing.count(peer));
      pg_missing_t& pm = peer_missing[peer];

      bool needs_past_intervals = pi.dne();

      /*
       * cover case where peer sort order was different and
       * last_backfill cannot be interpreted
       */
      bool force_restart_backfill =
	!pi.last_backfill.is_max() &&
	!pi.last_backfill_bitwise;

      if (pi.last_update == info.last_update && !force_restart_backfill) {
        // empty log
	if (!pi.last_backfill.is_max())
	  osd->clog->info() << info.pgid << " continuing backfill to osd."
			    << peer
			    << " from (" << pi.log_tail << "," << pi.last_update
			    << "] " << pi.last_backfill
			    << " to " << info.last_update;
	if (!pi.is_empty() && activator_map) {
	  dout(10) << "activate peer osd." << peer << " is up to date, queueing in pending_activators" << dendl;
	  (*activator_map)[peer.osd].emplace_back(
	      pg_notify_t(
		peer.shard, pg_whoami.shard,
		get_osdmap_epoch(),
		get_osdmap_epoch(),
		info),
	      past_intervals);
	} else {
	  dout(10) << "activate peer osd." << peer << " is up to date, but sending pg_log anyway" << dendl;
	  m = new MOSDPGLog(
	    i->shard, pg_whoami.shard,
	    get_osdmap_epoch(), info,
	    last_peering_reset);
	}
      } else if (
	pg_log.get_tail() > pi.last_update ||
	pi.last_backfill == hobject_t() ||
	force_restart_backfill ||
	(backfill_targets.count(*i) && pi.last_backfill.is_max())) {
	/* ^ This last case covers a situation where a replica is not contiguous
	 * with the auth_log, but is contiguous with this replica.  Reshuffling
	 * the active set to handle this would be tricky, so instead we just go
	 * ahead and backfill it anyway.  This is probably preferrable in any
	 * case since the replica in question would have to be significantly
	 * behind.
	 */
	// backfill
	osd->clog->debug() << info.pgid << " starting backfill to osd." << peer
			 << " from (" << pi.log_tail << "," << pi.last_update
			  << "] " << pi.last_backfill
			 << " to " << info.last_update;

	pi.last_update = info.last_update;
	pi.last_complete = info.last_update;
	pi.set_last_backfill(hobject_t());
	pi.last_epoch_started = info.last_epoch_started;
	pi.last_interval_started = info.last_interval_started;
	pi.history = info.history;
	pi.hit_set = info.hit_set;
        // Save num_bytes for reservation request, can't be negative
        peer_bytes[peer] = std::max<int64_t>(0, pi.stats.stats.sum.num_bytes);
        pi.stats.stats.clear();

	// initialize peer with our purged_snaps.
	pi.purged_snaps = info.purged_snaps;

	m = new MOSDPGLog(
	  i->shard, pg_whoami.shard,
	  get_osdmap_epoch(), pi,
	  last_peering_reset /* epoch to create pg at */);

	// send some recent log, so that op dup detection works well.
	m->log.copy_up_to(pg_log.get_log(), cct->_conf->osd_min_pg_log_entries);
	m->info.log_tail = m->log.tail;
	pi.log_tail = m->log.tail;  // sigh...

	pm.clear();
      } else {
	// catch up
	ceph_assert(pg_log.get_tail() <= pi.last_update);
	m = new MOSDPGLog(
	  i->shard, pg_whoami.shard,
	  get_osdmap_epoch(), info,
	  last_peering_reset /* epoch to create pg at */);
	// send new stuff to append to replicas log
	m->log.copy_after(pg_log.get_log(), pi.last_update);
      }

      // share past_intervals if we are creating the pg on the replica
      // based on whether our info for that peer was dne() *before*
      // updating pi.history in the backfill block above.
      if (m && needs_past_intervals)
	m->past_intervals = past_intervals;

      // update local version of peer's missing list!
      if (m && pi.last_backfill != hobject_t()) {
        for (list<pg_log_entry_t>::iterator p = m->log.log.begin();
             p != m->log.log.end();
             ++p) {
	  if (p->soid <= pi.last_backfill &&
	      !p->is_error()) {
	    if (perform_deletes_during_peering() && p->is_delete()) {
	      pm.rm(p->soid, p->version);
	    } else {
	      pm.add_next_event(*p);
	    }
	  }
	}
      }

      if (m) {
	dout(10) << "activate peer osd." << peer << " sending " << m->log << dendl;
	//m->log.print(cout);
	osd->send_message_osd_cluster(peer.osd, m, get_osdmap_epoch());
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
    set<pg_shard_t> complete_shards;
    for (set<pg_shard_t>::iterator i = acting_recovery_backfill.begin();
	 i != acting_recovery_backfill.end();
	 ++i) {
      dout(20) << __func__ << " setting up missing_loc from shard " << *i << " " << dendl;
      if (*i == get_primary()) {
	missing_loc.add_active_missing(missing);
        if (!missing.have_missing())
          complete_shards.insert(*i);
      } else {
	auto peer_missing_entry = peer_missing.find(*i);
	ceph_assert(peer_missing_entry != peer_missing.end());
	missing_loc.add_active_missing(peer_missing_entry->second);
        if (!peer_missing_entry->second.have_missing() &&
	    peer_info[*i].last_backfill.is_max())
	  complete_shards.insert(*i);
      }
    }

    // If necessary, create might_have_unfound to help us find our unfound objects.
    // NOTE: It's important that we build might_have_unfound before trimming the
    // past intervals.
    might_have_unfound.clear();
    if (needs_recovery()) {
      // If only one shard has missing, we do a trick to add all others as recovery
      // source, this is considered safe since the PGLogs have been merged locally,
      // and covers vast majority of the use cases, like one OSD/host is down for
      // a while for hardware repairing
      if (complete_shards.size() + 1 == acting_recovery_backfill.size()) {
        missing_loc.add_batch_sources_info(complete_shards, ctx->handle);
      } else {
        missing_loc.add_source_info(pg_whoami, info, pg_log.get_missing(),
				    ctx->handle);
        for (set<pg_shard_t>::iterator i = acting_recovery_backfill.begin();
	     i != acting_recovery_backfill.end();
	     ++i) {
	  if (*i == pg_whoami) continue;
	  dout(10) << __func__ << ": adding " << *i << " as a source" << dendl;
	  ceph_assert(peer_missing.count(*i));
	  ceph_assert(peer_info.count(*i));
	  missing_loc.add_source_info(
	    *i,
	    peer_info[*i],
	    peer_missing[*i],
            ctx->handle);
        }
      }
      for (map<pg_shard_t, pg_missing_t>::iterator i = peer_missing.begin();
	   i != peer_missing.end();
	   ++i) {
	if (is_acting_recovery_backfill(i->first))
	  continue;
	ceph_assert(peer_info.count(i->first));
	search_for_missing(
	  peer_info[i->first],
	  i->second,
	  i->first,
	  ctx);
      }

      build_might_have_unfound();

      // Always call now so _update_calc_stats() will be accurate
      discover_all_missing(query_map);
    }

    // num_objects_degraded if calculated should reflect this too, unless no
    // missing and we are about to go clean.
    if (get_osdmap()->get_pg_size(info.pgid.pgid) > actingset.size()) {
      state_set(PG_STATE_UNDERSIZED);
    }

    state_set(PG_STATE_ACTIVATING);
    release_pg_backoffs();
    projected_last_update = info.last_update;
  }
  if (acting.size() >= pool.info.min_size) {
    PGLogEntryHandler handler{this, &t};
    pg_log.roll_forward(&handler);
  }
}

bool PG::op_has_sufficient_caps(OpRequestRef& op)
{
  // only check MOSDOp
  if (op->get_req()->get_type() != CEPH_MSG_OSD_OP)
    return true;

  const MOSDOp *req = static_cast<const MOSDOp*>(op->get_req());

  auto priv = req->get_connection()->get_priv();
  auto session = static_cast<Session*>(priv.get());
  if (!session) {
    dout(0) << "op_has_sufficient_caps: no session for op " << *req << dendl;
    return false;
  }
  OSDCap& caps = session->caps;
  priv.reset();

  const string &key = req->get_hobj().get_key().empty() ?
    req->get_oid().name :
    req->get_hobj().get_key();

  bool cap = caps.is_capable(pool.name, req->get_hobj().nspace,
			     pool.info.application_metadata,
			     key,
			     op->need_read_cap(),
			     op->need_write_cap(),
			     op->classes(),
			     session->get_peer_socket_addr());

  dout(20) << "op_has_sufficient_caps "
           << "session=" << session
           << " pool=" << pool.id << " (" << pool.name
           << " " << req->get_hobj().nspace
	   << ")"
	   << " pool_app_metadata=" << pool.info.application_metadata
	   << " need_read_cap=" << op->need_read_cap()
	   << " need_write_cap=" << op->need_write_cap()
	   << " classes=" << op->classes()
	   << " -> " << (cap ? "yes" : "NO")
	   << dendl;
  return cap;
}

void PG::_activate_committed(epoch_t epoch, epoch_t activation_epoch)
{
  lock();
  if (pg_has_reset_since(epoch)) {
    dout(10) << "_activate_committed " << epoch
	     << ", that was an old interval" << dendl;
  } else if (is_primary()) {
    ceph_assert(!peer_activated.count(pg_whoami));
    peer_activated.insert(pg_whoami);
    dout(10) << "_activate_committed " << epoch
	     << " peer_activated now " << peer_activated
	     << " last_interval_started " << info.history.last_interval_started
	     << " last_epoch_started " << info.history.last_epoch_started
	     << " same_interval_since " << info.history.same_interval_since << dendl;
    ceph_assert(!acting_recovery_backfill.empty());
    if (peer_activated.size() == acting_recovery_backfill.size())
      all_activated_and_committed();
  } else {
    dout(10) << "_activate_committed " << epoch << " telling primary" << dendl;
    MOSDPGInfo *m = new MOSDPGInfo(epoch);
    pg_notify_t i = pg_notify_t(
      get_primary().shard, pg_whoami.shard,
      get_osdmap_epoch(),
      get_osdmap_epoch(),
      info);

    i.info.history.last_epoch_started = activation_epoch;
    i.info.history.last_interval_started = i.info.history.same_interval_since;
    if (acting.size() >= pool.info.min_size) {
      state_set(PG_STATE_ACTIVE);
    } else {
      state_set(PG_STATE_PEERED);
    }

    m->pg_list.emplace_back(i, PastIntervals());
    osd->send_message_osd_cluster(get_primary().osd, m, get_osdmap_epoch());

    // waiters
    if (flushes_in_progress == 0) {
      requeue_ops(waiting_for_peered);
    } else if (!waiting_for_peered.empty()) {
      dout(10) << __func__ << " flushes in progress, moving "
	       << waiting_for_peered.size() << " items to waiting_for_flush"
	       << dendl;
      ceph_assert(waiting_for_flush.empty());
      waiting_for_flush.swap(waiting_for_peered);
    }
  }

  ceph_assert(!dirty_info);

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
  ceph_assert(is_primary());
  ceph_assert(peer_activated.size() == acting_recovery_backfill.size());
  ceph_assert(!acting_recovery_backfill.empty());
  ceph_assert(blocked_by.empty());

  // Degraded?
  _update_calc_stats();
  if (info.stats.stats.sum.num_objects_degraded) {
    state_set(PG_STATE_DEGRADED);
  } else {
    state_clear(PG_STATE_DEGRADED);
  }

  queue_peering_event(
    PGPeeringEventRef(
      std::make_shared<PGPeeringEvent>(
        get_osdmap_epoch(),
        get_osdmap_epoch(),
        AllReplicasActivated())));
}

bool PG::requeue_scrub(bool high_priority)
{
  ceph_assert(is_locked());
  if (scrub_queued) {
    dout(10) << __func__ << ": already queued" << dendl;
    return false;
  } else {
    dout(10) << __func__ << ": queueing" << dendl;
    scrub_queued = true;
    osd->queue_for_scrub(this, high_priority);
    return true;
  }
}

void PG::queue_recovery()
{
  if (!is_primary() || !is_peered()) {
    dout(10) << "queue_recovery -- not primary or not peered " << dendl;
    ceph_assert(!recovery_queued);
  } else if (recovery_queued) {
    dout(10) << "queue_recovery -- already queued" << dendl;
  } else {
    dout(10) << "queue_recovery -- queuing" << dendl;
    recovery_queued = true;
    osd->queue_for_recovery(this);
  }
}

bool PG::queue_scrub()
{
  ceph_assert(is_locked());
  if (is_scrubbing()) {
    return false;
  }
  // An interrupted recovery repair could leave this set.
  state_clear(PG_STATE_REPAIR);
  scrubber.priority = scrubber.must_scrub ?
         cct->_conf->osd_requested_scrub_priority : get_scrub_priority();
  scrubber.must_scrub = false;
  state_set(PG_STATE_SCRUBBING);
  if (scrubber.must_deep_scrub) {
    state_set(PG_STATE_DEEP_SCRUB);
    scrubber.must_deep_scrub = false;
  }
  if (scrubber.must_repair || scrubber.auto_repair) {
    state_set(PG_STATE_REPAIR);
    scrubber.must_repair = false;
  }
  requeue_scrub();
  return true;
}

unsigned PG::get_scrub_priority()
{
  // a higher value -> a higher priority
  int64_t pool_scrub_priority = 0;
  pool.info.opts.get(pool_opts_t::SCRUB_PRIORITY, &pool_scrub_priority);
  return pool_scrub_priority > 0 ? pool_scrub_priority : cct->_conf->osd_scrub_priority;
}

void PG::try_mark_clean()
{
  if (actingset.size() == get_osdmap()->get_pg_size(info.pgid.pgid)) {
    state_clear(PG_STATE_FORCED_BACKFILL | PG_STATE_FORCED_RECOVERY);
    state_set(PG_STATE_CLEAN);
    info.history.last_epoch_clean = get_osdmap_epoch();
    info.history.last_interval_clean = info.history.same_interval_since;
    past_intervals.clear();
    dirty_big_info = true;
    dirty_info = true;
  }

  if (is_active()) {
    kick_snap_trim();
  } else if (is_peered()) {
    if (is_clean()) {
      bool target;
      if (pool.info.is_pending_merge(info.pgid.pgid, &target)) {
	if (target) {
	  ldout(cct, 10) << "ready to merge (target)" << dendl;
	  osd->set_ready_to_merge_target(this,
					 info.last_update,
					 info.history.last_epoch_started,
					 info.history.last_epoch_clean);
	} else {
	  ldout(cct, 10) << "ready to merge (source)" << dendl;
	  osd->set_ready_to_merge_source(this, info.last_update);
	}
      }
    } else {
      ldout(cct, 10) << "not clean, not ready to merge" << dendl;
      // we should have notified OSD in Active state entry point
    }
  }

  state_clear(PG_STATE_FORCED_RECOVERY | PG_STATE_FORCED_BACKFILL);

  share_pg_info();
  publish_stats_to_osd();
  requeue_ops(waiting_for_clean_to_primary_repair);
}

bool PG::set_force_recovery(bool b)
{
  bool did = false;
  if (b) {
    if (!(state & PG_STATE_FORCED_RECOVERY) &&
	(state & (PG_STATE_DEGRADED |
		  PG_STATE_RECOVERY_WAIT |
		  PG_STATE_RECOVERING))) {
      dout(20) << __func__ << " set" << dendl;
      state_set(PG_STATE_FORCED_RECOVERY);
      publish_stats_to_osd();
      did = true;
    }
  } else if (state & PG_STATE_FORCED_RECOVERY) {
    dout(20) << __func__ << " clear" << dendl;
    state_clear(PG_STATE_FORCED_RECOVERY);
    publish_stats_to_osd();
    did = true;
  }
  if (did) {
    dout(20) << __func__ << " state " << pgstate_history.get_current_state() << dendl;
    osd->local_reserver.update_priority(info.pgid, get_recovery_priority());
  }
  return did;
}

bool PG::set_force_backfill(bool b)
{
  bool did = false;
  if (b) {
    if (!(state & PG_STATE_FORCED_BACKFILL) &&
	(state & (PG_STATE_DEGRADED |
		  PG_STATE_BACKFILL_WAIT |
		  PG_STATE_BACKFILLING))) {
      dout(10) << __func__ << " set" << dendl;
      state_set(PG_STATE_FORCED_BACKFILL);
      publish_stats_to_osd();
      did = true;
    }
  } else if (state & PG_STATE_FORCED_BACKFILL) {
    dout(10) << __func__ << " clear" << dendl;
    state_clear(PG_STATE_FORCED_BACKFILL);
    publish_stats_to_osd();
    did = true;
  }
  if (did) {
    dout(20) << __func__ << " state " << pgstate_history.get_current_state() << dendl;
    osd->local_reserver.update_priority(info.pgid, get_backfill_priority());
  }
  return did;
}

inline int PG::clamp_recovery_priority(int priority)
{
  static_assert(OSD_RECOVERY_PRIORITY_MIN < OSD_RECOVERY_PRIORITY_MAX, "Invalid priority range");
  static_assert(OSD_RECOVERY_PRIORITY_MIN >= 0, "Priority range must match unsigned type");

  // Clamp to valid range
  if (priority > OSD_RECOVERY_PRIORITY_MAX) {
    return OSD_RECOVERY_PRIORITY_MAX;
  } else if (priority < OSD_RECOVERY_PRIORITY_MIN) {
    return OSD_RECOVERY_PRIORITY_MIN;
  } else {
    return priority;
  }
}

unsigned PG::get_recovery_priority()
{
  // a higher value -> a higher priority
  int64_t ret = 0;

  if (state & PG_STATE_FORCED_RECOVERY) {
    ret = OSD_RECOVERY_PRIORITY_FORCED;
  } else {
    pool.info.opts.get(pool_opts_t::RECOVERY_PRIORITY, &ret);
    ret = clamp_recovery_priority(OSD_RECOVERY_PRIORITY_BASE + ret);
  }
  dout(20) << __func__ << " recovery priority for " << *this << " is " << ret << ", state is " << state << dendl;
  return static_cast<unsigned>(ret);
}

unsigned PG::get_backfill_priority()
{
  // a higher value -> a higher priority
  int ret = OSD_BACKFILL_PRIORITY_BASE;
  if (state & PG_STATE_FORCED_BACKFILL) {
    ret = OSD_BACKFILL_PRIORITY_FORCED;
  } else {
    if (acting.size() < pool.info.min_size) {
      // inactive: no. of replicas < min_size, highest priority since it blocks IO
      ret = OSD_BACKFILL_INACTIVE_PRIORITY_BASE + (pool.info.min_size - acting.size());

    } else if (is_undersized()) {
      // undersized: OSD_BACKFILL_DEGRADED_PRIORITY_BASE + num missing replicas
      ceph_assert(pool.info.size > actingset.size());
      ret = OSD_BACKFILL_DEGRADED_PRIORITY_BASE + (pool.info.size - actingset.size());

    } else if (is_degraded()) {
      // degraded: baseline degraded
      ret = OSD_BACKFILL_DEGRADED_PRIORITY_BASE;
    }

    // Adjust with pool's recovery priority
    int64_t pool_recovery_priority = 0;
    pool.info.opts.get(pool_opts_t::RECOVERY_PRIORITY, &pool_recovery_priority);

    ret = clamp_recovery_priority(pool_recovery_priority + ret);
  }

  return static_cast<unsigned>(ret);
}

unsigned PG::get_delete_priority()
{
  auto state = get_osdmap()->get_state(osd->whoami);
  if (state & (CEPH_OSD_BACKFILLFULL |
               CEPH_OSD_FULL)) {
    return OSD_DELETE_PRIORITY_FULL;
  } else if (state & CEPH_OSD_NEARFULL) {
    return OSD_DELETE_PRIORITY_FULLISH;
  } else {
    return OSD_DELETE_PRIORITY_NORMAL;
  }
}

Context *PG::finish_recovery()
{
  dout(10) << "finish_recovery" << dendl;
  ceph_assert(info.last_complete == info.last_update);

  clear_recovery_state();

  /*
   * sync all this before purging strays.  but don't block!
   */
  finish_sync_event = new C_PG_FinishRecovery(this);
  return finish_sync_event;
}

void PG::_finish_recovery(Context *c)
{
  lock();
  // When recovery is initiated by a repair, that flag is left on
  state_clear(PG_STATE_REPAIR);
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
      scrubber.check_repair = true;
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
  ceph_assert(recovery_ops_active >= 0);
  recovery_ops_active++;
#ifdef DEBUG_RECOVERY_OIDS
  recovering_oids.insert(soid);
#endif
  osd->start_recovery_op(this, soid);
}

void PG::finish_recovery_op(const hobject_t& soid, bool dequeue)
{
  dout(10) << "finish_recovery_op " << soid
#ifdef DEBUG_RECOVERY_OIDS
	   << " (" << recovering_oids << ")" 
#endif
	   << dendl;
  ceph_assert(recovery_ops_active > 0);
  recovery_ops_active--;
#ifdef DEBUG_RECOVERY_OIDS
  ceph_assert(recovering_oids.count(soid));
  recovering_oids.erase(recovering_oids.find(soid));
#endif
  osd->finish_recovery_op(this, soid, dequeue);

  if (!dequeue) {
    queue_recovery();
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
  child->info.history.epoch_created = get_osdmap_epoch();
  child->info.purged_snaps = info.purged_snaps;

  if (info.last_backfill.is_max()) {
    child->info.set_last_backfill(hobject_t::get_max());
  } else {
    // restart backfill on parent and child to be safe.  we could
    // probably do better in the bitwise sort case, but it's more
    // fragile (there may be special work to do on backfill completion
    // in the future).
    info.set_last_backfill(hobject_t());
    child->info.set_last_backfill(hobject_t());
    // restarting backfill implies that the missing set is empty,
    // since it is only used for objects prior to last_backfill
    pg_log.reset_backfill();
    child->pg_log.reset_backfill();
  }

  child->info.stats = info.stats;
  child->info.stats.parent_split_bits = split_bits;
  info.stats.stats_invalid = true;
  child->info.stats.stats_invalid = true;
  child->info.last_epoch_started = info.last_epoch_started;
  child->info.last_interval_started = info.last_interval_started;

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
    child->info.history.same_primary_since = get_osdmap_epoch();

  child->info.stats.up = up;
  child->info.stats.up_primary = up_primary;
  child->info.stats.acting = acting;
  child->info.stats.acting_primary = primary;
  child->info.stats.mapping_epoch = get_osdmap_epoch();

  // History
  child->past_intervals = past_intervals;

  _split_into(child_pgid, child, split_bits);

  // release all backoffs for simplicity
  release_backoffs(hobject_t(), hobject_t::get_max());

  child->on_new_interval();

  child->send_notify = !child->is_primary();

  child->dirty_info = true;
  child->dirty_big_info = true;
  dirty_info = true;
  dirty_big_info = true;
}

void PG::start_split_stats(const set<spg_t>& childpgs, vector<object_stat_sum_t> *out)
{
  out->resize(childpgs.size() + 1);
  info.stats.stats.sum.split(*out);
}

void PG::finish_split_stats(const object_stat_sum_t& stats, ObjectStore::Transaction *t)
{
  info.stats.stats.sum = stats;
  write_if_dirty(*t);
}

void PG::merge_from(map<spg_t,PGRef>& sources, RecoveryCtx *rctx,
		    unsigned split_bits,
		    const pg_merge_meta_t& last_pg_merge_meta)
{
  dout(10) << __func__ << " from " << sources << " split_bits " << split_bits
	   << dendl;
  bool incomplete = false;
  if (info.last_complete != info.last_update ||
      info.is_incomplete() ||
      info.dne()) {
    dout(10) << __func__ << " target incomplete" << dendl;
    incomplete = true;
  }
  if (last_pg_merge_meta.source_pgid != pg_t()) {
    if (info.pgid.pgid != last_pg_merge_meta.source_pgid.get_parent()) {
      dout(10) << __func__ << " target doesn't match expected parent "
	       << last_pg_merge_meta.source_pgid.get_parent()
	       << " of source_pgid " << last_pg_merge_meta.source_pgid
	       << dendl;
      incomplete = true;
    }
    if (info.last_update != last_pg_merge_meta.target_version) {
      dout(10) << __func__ << " target version doesn't match expected "
	       << last_pg_merge_meta.target_version << dendl;
      incomplete = true;
    }
  }

  PGLogEntryHandler handler{this, rctx->transaction};
  pg_log.roll_forward(&handler);

  info.last_complete = info.last_update;  // to fake out trim()
  pg_log.reset_recovery_pointers();
  pg_log.trim(info.last_update, info);

  vector<PGLog*> log_from;
  for (auto& i : sources) {
    auto& source = i.second;
    if (!source) {
      dout(10) << __func__ << " source " << i.first << " missing" << dendl;
      incomplete = true;
      continue;
    }
    if (source->info.last_complete != source->info.last_update ||
	source->info.is_incomplete() ||
	source->info.dne()) {
      dout(10) << __func__ << " source " << source->pg_id << " incomplete"
	       << dendl;
      incomplete = true;
    }
    if (last_pg_merge_meta.source_pgid != pg_t()) {
      if (source->info.pgid.pgid != last_pg_merge_meta.source_pgid) {
	dout(10) << __func__ << " source " << source->info.pgid.pgid
		 << " doesn't match expected source pgid "
		 << last_pg_merge_meta.source_pgid << dendl;
	incomplete = true;
      }
      if (source->info.last_update != last_pg_merge_meta.source_version) {
	dout(10) << __func__ << " source version doesn't match expected "
		 << last_pg_merge_meta.target_version << dendl;
	incomplete = true;
      }
    }

    // prepare log
    PGLogEntryHandler handler{source.get(), rctx->transaction};
    source->pg_log.roll_forward(&handler);
    source->info.last_complete = source->info.last_update;  // to fake out trim()
    source->pg_log.reset_recovery_pointers();
    source->pg_log.trim(source->info.last_update, source->info);
    log_from.push_back(&source->pg_log);

    // wipe out source's pgmeta
    rctx->transaction->remove(source->coll, source->pgmeta_oid);

    // merge (and destroy source collection)
    rctx->transaction->merge_collection(source->coll, coll, split_bits);

    // combine stats
    info.stats.add(source->info.stats);

    // pull up last_update
    info.last_update = std::max(info.last_update, source->info.last_update);

    // adopt source's PastIntervals if target has none.  we can do this since
    // pgp_num has been reduced prior to the merge, so the OSD mappings for
    // the PGs are identical.
    if (past_intervals.empty() && !source->past_intervals.empty()) {
      dout(10) << __func__ << " taking source's past_intervals" << dendl;
      past_intervals = source->past_intervals;
    }
  }

  // merge_collection does this, but maybe all of our sources were missing.
  rctx->transaction->collection_set_bits(coll, split_bits);

  info.last_complete = info.last_update;
  info.log_tail = info.last_update;
  if (incomplete) {
    info.last_backfill = hobject_t();
  }

  snap_mapper.update_bits(split_bits);

  // merge logs
  pg_log.merge_from(log_from, info.last_update);

  // make sure we have a meaningful last_epoch_started/clean (if we were a
  // placeholder)
  if (info.last_epoch_started == 0) {
    // start with (a) source's history, since these PGs *should* have been
    // remapped in concert with each other...
    info.history = sources.begin()->second->info.history;

    // we use the last_epoch_{started,clean} we got from
    // the caller, which are the epochs that were reported by the PGs were
    // found to be ready for merge.
    info.history.last_epoch_clean = last_pg_merge_meta.last_epoch_clean;
    info.history.last_epoch_started = last_pg_merge_meta.last_epoch_started;
    info.last_epoch_started = last_pg_merge_meta.last_epoch_started;
    dout(10) << __func__
	     << " set les/c to " << last_pg_merge_meta.last_epoch_started << "/"
	     << last_pg_merge_meta.last_epoch_clean
	     << " from pool last_dec_*, source pg history was "
	     << sources.begin()->second->info.history
	     << dendl;

    // if the past_intervals start is later than last_epoch_clean, it
    // implies the source repeered again but the target didn't, or
    // that the source became clean in a later epoch than the target.
    // avoid the discrepancy but adjusting the interval start
    // backwards to match so that check_past_interval_bounds() will
    // not complain.
    auto pib = past_intervals.get_bounds();
    if (info.history.last_epoch_clean < pib.first) {
      dout(10) << __func__ << " last_epoch_clean "
	       << info.history.last_epoch_clean << " < past_interval start "
	       << pib.first << ", adjusting start backwards" << dendl;
      past_intervals.adjust_start_backwards(info.history.last_epoch_clean);
    }

    // Similarly, if the same_interval_since value is later than
    // last_epoch_clean, the next interval change will result in a
    // past_interval start that is later than last_epoch_clean.  This
    // can happen if we use the pg_history values from the merge
    // source.  Adjust the same_interval_since value backwards if that
    // happens.  (We trust the les and lec values more because they came from
    // the real target, whereas the history value we stole from the source.)
    if (info.history.last_epoch_started < info.history.same_interval_since) {
      dout(10) << __func__ << " last_epoch_started "
	       << info.history.last_epoch_started << " < same_interval_since "
	       << info.history.same_interval_since
	       << ", adjusting pg_history backwards" << dendl;
      info.history.same_interval_since = info.history.last_epoch_clean;
      // make sure same_{up,primary}_since are <= same_interval_since
      info.history.same_up_since = std::min(
	info.history.same_up_since, info.history.same_interval_since);
      info.history.same_primary_since = std::min(
	info.history.same_primary_since, info.history.same_interval_since);
    }
  }

  dirty_info = true;
  dirty_big_info = true;
}

void PG::add_backoff(SessionRef s, const hobject_t& begin, const hobject_t& end)
{
  ConnectionRef con = s->con;
  if (!con)   // OSD::ms_handle_reset clears s->con without a lock
    return;
  BackoffRef b(s->have_backoff(info.pgid, begin));
  if (b) {
    derr << __func__ << " already have backoff for " << s << " begin " << begin
	 << " " << *b << dendl;
    ceph_abort();
  }
  std::lock_guard l(backoff_lock);
  {
    b = new Backoff(info.pgid, this, s, ++s->backoff_seq, begin, end);
    backoffs[begin].insert(b);
    s->add_backoff(b);
    dout(10) << __func__ << " session " << s << " added " << *b << dendl;
  }
  con->send_message(
    new MOSDBackoff(
      info.pgid,
      get_osdmap_epoch(),
      CEPH_OSD_BACKOFF_OP_BLOCK,
      b->id,
      begin,
      end));
}

void PG::release_backoffs(const hobject_t& begin, const hobject_t& end)
{
  dout(10) << __func__ << " [" << begin << "," << end << ")" << dendl;
  vector<BackoffRef> bv;
  {
    std::lock_guard l(backoff_lock);
    auto p = backoffs.lower_bound(begin);
    while (p != backoffs.end()) {
      int r = cmp(p->first, end);
      dout(20) << __func__ << " ? " << r << " " << p->first
	       << " " << p->second << dendl;
      // note: must still examine begin=end=p->first case
      if (r > 0 || (r == 0 && begin < end)) {
	break;
      }
      dout(20) << __func__ << " checking " << p->first
	       << " " << p->second << dendl;
      auto q = p->second.begin();
      while (q != p->second.end()) {
	dout(20) << __func__ << " checking  " << *q << dendl;
	int r = cmp((*q)->begin, begin);
	if (r == 0 || (r > 0 && (*q)->end < end)) {
	  bv.push_back(*q);
	  q = p->second.erase(q);
	} else {
	  ++q;
	}
      }
      if (p->second.empty()) {
	p = backoffs.erase(p);
      } else {
	++p;
      }
    }
  }
  for (auto b : bv) {
    std::lock_guard l(b->lock);
    dout(10) << __func__ << " " << *b << dendl;
    if (b->session) {
      ceph_assert(b->pg == this);
      ConnectionRef con = b->session->con;
      if (con) {   // OSD::ms_handle_reset clears s->con without a lock
	con->send_message(
	  new MOSDBackoff(
	    info.pgid,
	    get_osdmap_epoch(),
	    CEPH_OSD_BACKOFF_OP_UNBLOCK,
	    b->id,
	    b->begin,
	    b->end));
      }
      if (b->is_new()) {
	b->state = Backoff::STATE_DELETING;
      } else {
	b->session->rm_backoff(b);
	b->session.reset();
      }
      b->pg.reset();
    }
  }
}

void PG::clear_backoffs()
{
  dout(10) << __func__ << " " << dendl;
  map<hobject_t,set<BackoffRef>> ls;
  {
    std::lock_guard l(backoff_lock);
    ls.swap(backoffs);
  }
  for (auto& p : ls) {
    for (auto& b : p.second) {
      std::lock_guard l(b->lock);
      dout(10) << __func__ << " " << *b << dendl;
      if (b->session) {
	ceph_assert(b->pg == this);
	if (b->is_new()) {
	  b->state = Backoff::STATE_DELETING;
	} else {
	  b->session->rm_backoff(b);
	  b->session.reset();
	}
	b->pg.reset();
      }
    }
  }
}

// called by Session::clear_backoffs()
void PG::rm_backoff(BackoffRef b)
{
  dout(10) << __func__ << " " << *b << dendl;
  std::lock_guard l(backoff_lock);
  ceph_assert(b->lock.is_locked_by_me());
  ceph_assert(b->pg == this);
  auto p = backoffs.find(b->begin);
  // may race with release_backoffs()
  if (p != backoffs.end()) {
    auto q = p->second.find(b);
    if (q != p->second.end()) {
      p->second.erase(q);
      if (p->second.empty()) {
	backoffs.erase(p);
      }
    }
  }
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

  async_recovery_targets.clear();
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
  if (is_premerge()) {
    dout(10) << "purge_strays " << stray_set << " but premerge, doing nothing"
	     << dendl;
    return;
  }
  if (cct->_conf.get_val<bool>("osd_debug_no_purge_strays")) {
    return;
  }
  dout(10) << "purge_strays " << stray_set << dendl;
  
  bool removed = false;
  for (set<pg_shard_t>::iterator p = stray_set.begin();
       p != stray_set.end();
       ++p) {
    ceph_assert(!is_acting_recovery_backfill(*p));
    if (get_osdmap()->is_up(p->osd)) {
      dout(10) << "sending PGRemove to osd." << *p << dendl;
      vector<spg_t> to_remove;
      to_remove.push_back(spg_t(info.pgid.pgid, p->shard));
      MOSDPGRemove *m = new MOSDPGRemove(
	get_osdmap_epoch(),
	to_remove);
      osd->send_message_osd_cluster(p->osd, m, get_osdmap_epoch());
    } else {
      dout(10) << "not sending PGRemove to down osd." << *p << dendl;
    }
    peer_missing.erase(*p);
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
  std::lock_guard l(heartbeat_peer_lock);
  probe_targets.clear();
  for (set<pg_shard_t>::iterator i = probe_set.begin();
       i != probe_set.end();
       ++i) {
    probe_targets.insert(i->osd);
  }
}

void PG::clear_probe_targets()
{
  std::lock_guard l(heartbeat_peer_lock);
  probe_targets.clear();
}

void PG::update_heartbeat_peers()
{
  ceph_assert(is_locked());

  if (!is_primary())
    return;

  set<int> new_peers;
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


bool PG::check_in_progress_op(
  const osd_reqid_t &r,
  eversion_t *version,
  version_t *user_version,
  int *return_code) const
{
  return (
    projected_log.get_request(r, version, user_version, return_code) ||
    pg_log.get_log().get_request(r, version, user_version, return_code));
}

static bool find_shard(const set<pg_shard_t> & pgs, shard_id_t shard)
{
    for (auto&p : pgs)
      if (p.shard == shard)
        return true;
    return false;
}

static pg_shard_t get_another_shard(const set<pg_shard_t> & pgs, pg_shard_t skip, shard_id_t shard)
{
    for (auto&p : pgs) {
      if (p == skip)
        continue;
      if (p.shard == shard)
        return p;
    }
    return pg_shard_t();
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
  info.stats.ondisk_log_size = info.stats.log_size;
  info.stats.log_start = pg_log.get_tail();
  info.stats.ondisk_log_start = pg_log.get_tail();
  info.stats.snaptrimq_len = snap_trimq.size();

  unsigned num_shards = get_osdmap()->get_pg_size(info.pgid.pgid);

  // In rare case that upset is too large (usually transient), use as target
  // for calculations below.
  unsigned target = std::max(num_shards, (unsigned)upset.size());
  // For undersized actingset may be larger with OSDs out
  unsigned nrep = std::max(actingset.size(), upset.size());
  // calc num_object_copies
  info.stats.stats.calc_copies(std::max(target, nrep));
  info.stats.stats.sum.num_objects_degraded = 0;
  info.stats.stats.sum.num_objects_unfound = 0;
  info.stats.stats.sum.num_objects_misplaced = 0;

  if ((is_remapped() || is_undersized() || !is_clean()) && (is_peered() || is_activating())) {
    dout(20) << __func__ << " actingset " << actingset << " upset "
             << upset << " acting_recovery_backfill " << acting_recovery_backfill << dendl;
    dout(20) << __func__ << " acting " << acting << " up " << up << dendl;

    ceph_assert(!acting_recovery_backfill.empty());

    bool estimate = false;

    // NOTE: we only generate degraded, misplaced and unfound
    // values for the summation, not individual stat categories.
    int64_t num_objects = info.stats.stats.sum.num_objects;

    // Objects missing from up nodes, sorted by # objects.
    boost::container::flat_set<pair<int64_t,pg_shard_t>> missing_target_objects;
    // Objects missing from nodes not in up, sort by # objects
    boost::container::flat_set<pair<int64_t,pg_shard_t>> acting_source_objects;

    // Fill missing_target_objects/acting_source_objects

    {
      int64_t missing;

      // Primary first
      missing = pg_log.get_missing().num_missing();
      ceph_assert(acting_recovery_backfill.count(pg_whoami));
      if (upset.count(pg_whoami)) {
        missing_target_objects.emplace(missing, pg_whoami);
      } else {
        acting_source_objects.emplace(missing, pg_whoami);
      }
      info.stats.stats.sum.num_objects_missing_on_primary = missing;
      dout(20) << __func__ << " shard " << pg_whoami
               << " primary objects " << num_objects
               << " missing " << missing
               << dendl;
    }

    // All other peers
    for (auto& peer : peer_info) {
      // Primary should not be in the peer_info, skip if it is.
      if (peer.first == pg_whoami) continue;
      int64_t missing = 0;
      int64_t peer_num_objects = peer.second.stats.stats.sum.num_objects;
      // Backfill targets always track num_objects accurately
      // all other peers track missing accurately.
      if (is_backfill_targets(peer.first)) {
        missing = std::max((int64_t)0, num_objects - peer_num_objects);
      } else {
        if (peer_missing.count(peer.first)) {
          missing = peer_missing[peer.first].num_missing();
        } else {
          dout(20) << __func__ << " no peer_missing found for " << peer.first << dendl;
          if (is_recovering()) {
            estimate = true;
          }
          missing = std::max((int64_t)0, num_objects - peer_num_objects);
        }
      }
      if (upset.count(peer.first)) {
	missing_target_objects.emplace(missing, peer.first);
      } else if (actingset.count(peer.first)) {
	acting_source_objects.emplace(missing, peer.first);
      }
      peer.second.stats.stats.sum.num_objects_missing = missing;
      dout(20) << __func__ << " shard " << peer.first
               << " objects " << peer_num_objects
               << " missing " << missing
               << dendl;
    }

    // A misplaced object is not stored on the correct OSD
    int64_t misplaced = 0;
    // a degraded objects has fewer replicas or EC shards than the pool specifies.
    int64_t degraded = 0;

    if (is_recovering()) {
      for (auto& sml: missing_loc.get_missing_by_count()) {
        for (auto& ml: sml.second) {
          int missing_shards;
          if (sml.first == shard_id_t::NO_SHARD) {
            dout(20) << __func__ << " ml " << ml.second << " upset size " << upset.size() << " up " << ml.first.up << dendl;
            missing_shards = (int)upset.size() - ml.first.up;
          } else {
	    // Handle shards not even in upset below
            if (!find_shard(upset, sml.first))
	      continue;
	    missing_shards = std::max(0, 1 - ml.first.up);
            dout(20) << __func__ << " shard " << sml.first << " ml " << ml.second << " missing shards " << missing_shards << dendl;
          }
          int odegraded = ml.second * missing_shards;
          // Copies on other osds but limited to the possible degraded
          int more_osds = std::min(missing_shards, ml.first.other);
          int omisplaced = ml.second * more_osds;
          ceph_assert(omisplaced <= odegraded);
          odegraded -= omisplaced;

          misplaced += omisplaced;
          degraded += odegraded;
        }
      }

      dout(20) << __func__ << " missing based degraded " << degraded << dendl;
      dout(20) << __func__ << " missing based misplaced " << misplaced << dendl;

      // Handle undersized case
      if (pool.info.is_replicated()) {
        // Add degraded for missing targets (num_objects missing)
        ceph_assert(target >= upset.size());
        unsigned needed = target - upset.size();
        degraded += num_objects * needed;
      } else {
        for (unsigned i = 0 ; i < num_shards; ++i) {
          shard_id_t shard(i);

          if (!find_shard(upset, shard)) {
            pg_shard_t pgs = get_another_shard(actingset, pg_shard_t(), shard);

            if (pgs != pg_shard_t()) {
              int64_t missing;

              if (pgs == pg_whoami)
                missing = info.stats.stats.sum.num_objects_missing_on_primary;
              else
                missing = peer_info[pgs].stats.stats.sum.num_objects_missing;

              degraded += missing;
              misplaced += std::max((int64_t)0, num_objects - missing);
            } else {
              // No shard anywhere
              degraded += num_objects;
            }
          }
        }
      }
      goto out;
    }

    // Handle undersized case
    if (pool.info.is_replicated()) {
      // Add to missing_target_objects
      ceph_assert(target >= missing_target_objects.size());
      unsigned needed = target - missing_target_objects.size();
      if (needed)
        missing_target_objects.emplace(num_objects * needed, pg_shard_t(pg_shard_t::NO_OSD));
    } else {
      for (unsigned i = 0 ; i < num_shards; ++i) {
        shard_id_t shard(i);
	bool found = false;
	for (const auto& t : missing_target_objects) {
	  if (std::get<1>(t).shard == shard) {
	    found = true;
	    break;
	  }
	}
	if (!found)
	  missing_target_objects.emplace(num_objects, pg_shard_t(pg_shard_t::NO_OSD,shard));
      }
    }

    for (const auto& item : missing_target_objects)
      dout(20) << __func__ << " missing shard " << std::get<1>(item) << " missing= " << std::get<0>(item) << dendl;
    for (const auto& item : acting_source_objects)
      dout(20) << __func__ << " acting shard " << std::get<1>(item) << " missing= " << std::get<0>(item) << dendl;

    // Handle all objects not in missing for remapped
    // or backfill
    for (auto m = missing_target_objects.rbegin();
        m != missing_target_objects.rend(); ++m) {

      int64_t extra_missing = -1;

      if (pool.info.is_replicated()) {
	if (!acting_source_objects.empty()) {
	  auto extra_copy = acting_source_objects.begin();
	  extra_missing = std::get<0>(*extra_copy);
          acting_source_objects.erase(extra_copy);
	}
      } else {	// Erasure coded
	// Use corresponding shard
	for (const auto& a : acting_source_objects) {
	  if (std::get<1>(a).shard == std::get<1>(*m).shard) {
	    extra_missing = std::get<0>(a);
	    acting_source_objects.erase(a);
	    break;
	  }
	}
      }

      if (extra_missing >= 0 && std::get<0>(*m) >= extra_missing) {
	// We don't know which of the objects on the target
	// are part of extra_missing so assume are all degraded.
	misplaced += std::get<0>(*m) - extra_missing;
	degraded += extra_missing;
      } else {
	// 1. extra_missing == -1, more targets than sources so degraded
	// 2. extra_missing > std::get<0>(m), so that we know that some extra_missing
	//    previously degraded are now present on the target.
	degraded += std::get<0>(*m);
      }
    }
    // If there are still acting that haven't been accounted for
    // then they are misplaced
    for (const auto& a : acting_source_objects) {
      int64_t extra_misplaced = std::max((int64_t)0, num_objects - std::get<0>(a));
      dout(20) << __func__ << " extra acting misplaced " << extra_misplaced << dendl;
      misplaced += extra_misplaced;
    }
out:
    // NOTE: Tests use these messages to verify this code
    dout(20) << __func__ << " degraded " << degraded << (estimate ? " (est)": "") << dendl;
    dout(20) << __func__ << " misplaced " << misplaced << (estimate ? " (est)": "")<< dendl;

    info.stats.stats.sum.num_objects_degraded = degraded;
    info.stats.stats.sum.num_objects_unfound = get_num_unfound();
    info.stats.stats.sum.num_objects_misplaced = misplaced;
  }
}

void PG::_update_blocked_by()
{
  // set a max on the number of blocking peers we report. if we go
  // over, report a random subset.  keep the result sorted.
  unsigned keep = std::min<unsigned>(blocked_by.size(), cct->_conf->osd_max_pg_blocked_by);
  unsigned skip = blocked_by.size() - keep;
  info.stats.blocked_by.clear();
  info.stats.blocked_by.resize(keep);
  unsigned pos = 0;
  for (set<int>::iterator p = blocked_by.begin();
       p != blocked_by.end() && keep > 0;
       ++p) {
    if (skip > 0 && (rand() % (skip + keep) < skip)) {
      --skip;
    } else {
      info.stats.blocked_by[pos++] = *p;
      --keep;
    }
  }
}

void PG::publish_stats_to_osd()
{
  if (!is_primary())
    return;

  pg_stats_publish_lock.Lock();

  if (info.stats.stats.sum.num_scrub_errors)
    state_set(PG_STATE_INCONSISTENT);
  else {
    state_clear(PG_STATE_INCONSISTENT);
    state_clear(PG_STATE_FAILED_REPAIR);
  }

  utime_t now = ceph_clock_now();
  if (info.stats.state != state) {
    info.stats.last_change = now;
    // Optimistic estimation, if we just find out an inactive PG,
    // assumt it is active till now.
    if (!(state & PG_STATE_ACTIVE) &&
	(info.stats.state & PG_STATE_ACTIVE))
      info.stats.last_active = now;

    if ((state & PG_STATE_ACTIVE) &&
	!(info.stats.state & PG_STATE_ACTIVE))
      info.stats.last_became_active = now;
    if ((state & (PG_STATE_ACTIVE|PG_STATE_PEERED)) &&
	!(info.stats.state & (PG_STATE_ACTIVE|PG_STATE_PEERED)))
      info.stats.last_became_peered = now;
    info.stats.state = state;
  }

  _update_calc_stats();
  if (info.stats.stats.sum.num_objects_degraded) {
    state_set(PG_STATE_DEGRADED);
  } else {
    state_clear(PG_STATE_DEGRADED);
  }
  _update_blocked_by();

  pg_stat_t pre_publish = info.stats;
  pre_publish.stats.add(unstable_stats);
  utime_t cutoff = now;
  cutoff -= cct->_conf->osd_pg_stat_report_interval_max;

  if (get_osdmap()->require_osd_release >= CEPH_RELEASE_MIMIC) {
    // share (some of) our purged_snaps via the pg_stats. limit # of intervals
    // because we don't want to make the pg_stat_t structures too expensive.
    unsigned max = cct->_conf->osd_max_snap_prune_intervals_per_epoch;
    unsigned num = 0;
    auto i = info.purged_snaps.begin();
    while (num < max && i != info.purged_snaps.end()) {
      pre_publish.purged_snaps.insert(i.get_start(), i.get_len());
      ++num;
      ++i;
    }
    dout(20) << __func__ << " reporting purged_snaps "
	     << pre_publish.purged_snaps << dendl;
  }

  if (pg_stats_publish_valid && pre_publish == pg_stats_publish &&
      info.stats.last_fresh > cutoff) {
    dout(15) << "publish_stats_to_osd " << pg_stats_publish.reported_epoch
	     << ": no change since " << info.stats.last_fresh << dendl;
  } else {
    // update our stat summary and timestamps
    info.stats.reported_epoch = get_osdmap_epoch();
    ++info.stats.reported_seq;

    info.stats.last_fresh = now;

    if (info.stats.state & PG_STATE_CLEAN)
      info.stats.last_clean = now;
    if (info.stats.state & PG_STATE_ACTIVE)
      info.stats.last_active = now;
    if (info.stats.state & (PG_STATE_ACTIVE|PG_STATE_PEERED))
      info.stats.last_peered = now;
    info.stats.last_unstale = now;
    if ((info.stats.state & PG_STATE_DEGRADED) == 0)
      info.stats.last_undegraded = now;
    if ((info.stats.state & PG_STATE_UNDERSIZED) == 0)
      info.stats.last_fullsized = now;

    pg_stats_publish_valid = true;
    pg_stats_publish = pre_publish;

    dout(15) << "publish_stats_to_osd " << pg_stats_publish.reported_epoch
	     << ":" << pg_stats_publish.reported_seq << dendl;
  }
  pg_stats_publish_lock.Unlock();
}

void PG::clear_publish_stats()
{
  dout(15) << "clear_stats" << dendl;
  pg_stats_publish_lock.Lock();
  pg_stats_publish_valid = false;
  pg_stats_publish_lock.Unlock();
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
  const vector<int>& newup, int new_up_primary,
  const vector<int>& newacting, int new_acting_primary,
  const pg_history_t& history,
  const PastIntervals& pi,
  bool backfill,
  ObjectStore::Transaction *t)
{
  dout(10) << "init role " << role << " up " << newup << " acting " << newacting
	   << " history " << history
	   << " past_intervals " << pi
	   << dendl;

  set_role(role);
  init_primary_up_acting(
    newup,
    newacting,
    new_up_primary,
    new_acting_primary);

  info.history = history;
  past_intervals = pi;

  info.stats.up = up;
  info.stats.up_primary = new_up_primary;
  info.stats.acting = acting;
  info.stats.acting_primary = new_acting_primary;
  info.stats.mapping_epoch = info.history.same_interval_since;

  if (backfill) {
    dout(10) << __func__ << ": Setting backfill" << dendl;
    info.set_last_backfill(hobject_t());
    info.last_complete = info.last_update;
    pg_log.mark_log_for_rewrite();
  }

  on_new_interval();

  dirty_info = true;
  dirty_big_info = true;
  write_if_dirty(*t);
}

void PG::shutdown()
{
  ch->flush();
  lock();
  on_shutdown();
  unlock();
}

#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

void PG::upgrade(ObjectStore *store)
{
  dout(0) << __func__ << " " << info_struct_v << " -> " << latest_struct_v
	  << dendl;
  ceph_assert(info_struct_v <= 10);
  ObjectStore::Transaction t;

  // <do upgrade steps here>

  // finished upgrade!
  ceph_assert(info_struct_v == 10);

  // update infover_key
  if (info_struct_v < latest_struct_v) {
    map<string,bufferlist> v;
    __u8 ver = latest_struct_v;
    encode(ver, v[infover_key]);
    t.omap_setkeys(coll, pgmeta_oid, v);
  }

  dirty_info = true;
  dirty_big_info = true;
  write_if_dirty(t);

  ObjectStore::CollectionHandle ch = store->open_collection(coll);
  int r = store->queue_transaction(ch, std::move(t));
  if (r != 0) {
    derr << __func__ << ": queue_transaction returned "
	 << cpp_strerror(r) << dendl;
    ceph_abort();
  }
  ceph_assert(r == 0);

  C_SaferCond waiter;
  if (!ch->flush_commit(&waiter)) {
    waiter.wait();
  }
}

#pragma GCC diagnostic pop
#pragma GCC diagnostic warning "-Wpragmas"

int PG::_prepare_write_info(CephContext* cct,
			    map<string,bufferlist> *km,
			    epoch_t epoch,
			    pg_info_t &info, pg_info_t &last_written_info,
			    PastIntervals &past_intervals,
			    bool dirty_big_info,
			    bool dirty_epoch,
			    bool try_fast_info,
			    PerfCounters *logger)
{
  if (dirty_epoch) {
    encode(epoch, (*km)[epoch_key]);
  }

  if (logger)
    logger->inc(l_osd_pg_info);

  // try to do info efficiently?
  if (!dirty_big_info && try_fast_info &&
      info.last_update > last_written_info.last_update) {
    pg_fast_info_t fast;
    fast.populate_from(info);
    bool did = fast.try_apply_to(&last_written_info);
    ceph_assert(did);  // we verified last_update increased above
    if (info == last_written_info) {
      encode(fast, (*km)[fastinfo_key]);
      if (logger)
	logger->inc(l_osd_pg_fastinfo);
      return 0;
    }
    generic_dout(30) << __func__ << " fastinfo failed, info:\n";
    {
      JSONFormatter jf(true);
      jf.dump_object("info", info);
      jf.flush(*_dout);
    }
    {
      *_dout << "\nlast_written_info:\n";
      JSONFormatter jf(true);
      jf.dump_object("last_written_info", last_written_info);
      jf.flush(*_dout);
    }
    *_dout << dendl;
  }
  last_written_info = info;

  // info.  store purged_snaps separately.
  interval_set<snapid_t> purged_snaps;
  purged_snaps.swap(info.purged_snaps);
  encode(info, (*km)[info_key]);
  purged_snaps.swap(info.purged_snaps);

  if (dirty_big_info) {
    // potentially big stuff
    bufferlist& bigbl = (*km)[biginfo_key];
    encode(past_intervals, bigbl);
    encode(info.purged_snaps, bigbl);
    //dout(20) << "write_info bigbl " << bigbl.length() << dendl;
    if (logger)
      logger->inc(l_osd_pg_biginfo);
  }

  return 0;
}

void PG::_create(ObjectStore::Transaction& t, spg_t pgid, int bits)
{
  coll_t coll(pgid);
  t.create_collection(coll, bits);
}

void PG::_init(ObjectStore::Transaction& t, spg_t pgid, const pg_pool_t *pool)
{
  coll_t coll(pgid);

  if (pool) {
    // Give a hint to the PG collection
    bufferlist hint;
    uint32_t pg_num = pool->get_pg_num();
    uint64_t expected_num_objects_pg = pool->expected_num_objects / pg_num;
    encode(pg_num, hint);
    encode(expected_num_objects_pg, hint);
    uint32_t hint_type = ObjectStore::Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS;
    t.collection_hint(coll, hint_type, hint);
  }

  ghobject_t pgmeta_oid(pgid.make_pgmeta_oid());
  t.touch(coll, pgmeta_oid);
  map<string,bufferlist> values;
  __u8 struct_v = latest_struct_v;
  encode(struct_v, values[infover_key]);
  t.omap_setkeys(coll, pgmeta_oid, values);
}

void PG::prepare_write_info(map<string,bufferlist> *km)
{
  info.stats.stats.add(unstable_stats);
  unstable_stats.clear();

  bool need_update_epoch = last_epoch < get_osdmap_epoch();
  int ret = _prepare_write_info(cct, km, get_osdmap_epoch(),
				info,
				last_written_info,
				past_intervals,
				dirty_big_info, need_update_epoch,
				cct->_conf->osd_fast_info,
				osd->logger);
  ceph_assert(ret == 0);
  if (need_update_epoch)
    last_epoch = get_osdmap_epoch();
  last_persisted_osdmap = last_epoch;

  dirty_info = false;
  dirty_big_info = false;
}

#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

bool PG::_has_removal_flag(ObjectStore *store,
			   spg_t pgid)
{
  coll_t coll(pgid);
  ghobject_t pgmeta_oid(pgid.make_pgmeta_oid());

  // first try new way
  set<string> keys;
  keys.insert("_remove");
  map<string,bufferlist> values;
  auto ch = store->open_collection(coll);
  ceph_assert(ch);
  if (store->omap_get_values(ch, pgmeta_oid, keys, &values) == 0 &&
      values.size() == 1)
    return true;

  return false;
}

int PG::peek_map_epoch(ObjectStore *store,
		       spg_t pgid,
		       epoch_t *pepoch)
{
  coll_t coll(pgid);
  ghobject_t legacy_infos_oid(OSD::make_infos_oid());
  ghobject_t pgmeta_oid(pgid.make_pgmeta_oid());
  epoch_t cur_epoch = 0;

  // validate collection name
  ceph_assert(coll.is_pg());

  // try for v8
  set<string> keys;
  keys.insert(infover_key);
  keys.insert(epoch_key);
  map<string,bufferlist> values;
  auto ch = store->open_collection(coll);
  ceph_assert(ch);
  int r = store->omap_get_values(ch, pgmeta_oid, keys, &values);
  if (r == 0) {
    ceph_assert(values.size() == 2);

    // sanity check version
    auto bp = values[infover_key].cbegin();
    __u8 struct_v = 0;
    decode(struct_v, bp);
    ceph_assert(struct_v >= 8);

    // get epoch
    bp = values[epoch_key].begin();
    decode(cur_epoch, bp);
  } else {
    // probably bug 10617; see OSD::load_pgs()
    return -1;
  }

  *pepoch = cur_epoch;
  return 0;
}

#pragma GCC diagnostic pop
#pragma GCC diagnostic warning "-Wpragmas"

void PG::write_if_dirty(ObjectStore::Transaction& t)
{
  map<string,bufferlist> km;
  if (dirty_big_info || dirty_info)
    prepare_write_info(&km);
  pg_log.write_log_and_missing(t, &km, coll, pgmeta_oid, pool.info.require_rollback());
  if (!km.empty())
    t.omap_setkeys(coll, pgmeta_oid, km);
}

void PG::add_log_entry(const pg_log_entry_t& e, bool applied)
{
  // raise last_complete only if we were previously up to date
  if (info.last_complete == info.last_update)
    info.last_complete = e.version;
  
  // raise last_update.
  ceph_assert(e.version > info.last_update);
  info.last_update = e.version;

  // raise user_version, if it increased (it may have not get bumped
  // by all logged updates)
  if (e.user_version > info.last_user_version)
    info.last_user_version = e.user_version;

  // log mutation
  pg_log.add(e, applied);
  dout(10) << "add_log_entry " << e << dendl;
}


void PG::append_log(
  const vector<pg_log_entry_t>& logv,
  eversion_t trim_to,
  eversion_t roll_forward_to,
  ObjectStore::Transaction &t,
  bool transaction_applied,
  bool async)
{
  if (transaction_applied)
    update_snap_map(logv, t);

  /* The primary has sent an info updating the history, but it may not
   * have arrived yet.  We want to make sure that we cannot remember this
   * write without remembering that it happened in an interval which went
   * active in epoch history.last_epoch_started.
   */
  if (info.last_epoch_started != info.history.last_epoch_started) {
    info.history.last_epoch_started = info.last_epoch_started;
  }
  if (info.last_interval_started != info.history.last_interval_started) {
    info.history.last_interval_started = info.last_interval_started;
  }
  dout(10) << "append_log " << pg_log.get_log() << " " << logv << dendl;

  PGLogEntryHandler handler{this, &t};
  if (!transaction_applied) {
     /* We must be a backfill or async recovery peer, so it's ok if we apply
      * out-of-turn since we won't be considered when
      * determining a min possible last_update.
      *
      * We skip_rollforward() here, which advances the crt, without
      * doing an actual rollforward. This avoids cleaning up entries
      * from the backend and we do not end up in a situation, where the
      * object is deleted before we can _merge_object_divergent_entries().
      */
    pg_log.skip_rollforward();
  }

  for (vector<pg_log_entry_t>::const_iterator p = logv.begin();
       p != logv.end();
       ++p) {
    add_log_entry(*p, transaction_applied);

    /* We don't want to leave the rollforward artifacts around
     * here past last_backfill.  It's ok for the same reason as
     * above */
    if (transaction_applied &&
	p->soid > info.last_backfill) {
      pg_log.roll_forward(&handler);
    }
  }
  auto last = logv.rbegin();
  if (is_primary() && last != logv.rend()) {
    projected_log.skip_can_rollback_to_to_head();
    projected_log.trim(cct, last->version, nullptr, nullptr, nullptr);
  }

  if (transaction_applied && roll_forward_to > pg_log.get_can_rollback_to()) {
    pg_log.roll_forward_to(
      roll_forward_to,
      &handler);
    last_rollback_info_trimmed_to_applied = roll_forward_to;
  }

  dout(10) << __func__ << " approx pg log length =  "
           << pg_log.get_log().approx_size() << dendl;
  dout(10) << __func__ << " transaction_applied = "
           << transaction_applied << dendl;
  if (!transaction_applied || async)
    dout(10) << __func__ << " " << pg_whoami
             << " is async_recovery or backfill target" << dendl;
  pg_log.trim(trim_to, info, transaction_applied, async);

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
  ObjectStore *store, spg_t pgid, const coll_t &coll,
  pg_info_t &info, PastIntervals &past_intervals,
  __u8 &struct_v)
{
  set<string> keys;
  keys.insert(infover_key);
  keys.insert(info_key);
  keys.insert(biginfo_key);
  keys.insert(fastinfo_key);
  ghobject_t pgmeta_oid(pgid.make_pgmeta_oid());
  map<string,bufferlist> values;
  auto ch = store->open_collection(coll);
  ceph_assert(ch);
  int r = store->omap_get_values(ch, pgmeta_oid, keys, &values);
  ceph_assert(r == 0);
  ceph_assert(values.size() == 3 ||
	 values.size() == 4);

  auto p = values[infover_key].cbegin();
  decode(struct_v, p);
  ceph_assert(struct_v >= 10);

  p = values[info_key].begin();
  decode(info, p);

  p = values[biginfo_key].begin();
  decode(past_intervals, p);
  decode(info.purged_snaps, p);

  p = values[fastinfo_key].begin();
  if (!p.end()) {
    pg_fast_info_t fast;
    decode(fast, p);
    fast.try_apply_to(&info);
  }
  return 0;
}

void PG::read_state(ObjectStore *store)
{
  int r = read_info(store, pg_id, coll, info, past_intervals,
		    info_struct_v);
  ceph_assert(r >= 0);

  if (info_struct_v < compat_struct_v) {
    derr << "PG needs upgrade, but on-disk data is too old; upgrade to"
	 << " an older version first." << dendl;
    ceph_abort_msg("PG too old to upgrade");
  }

  last_written_info = info;

  ostringstream oss;
  pg_log.read_log_and_missing(
    store,
    ch,
    pgmeta_oid,
    info,
    oss,
    cct->_conf->osd_ignore_stale_divergent_priors,
    cct->_conf->osd_debug_verify_missing_on_start);
  if (oss.tellp())
    osd->clog->error() << oss.str();

  // log any weirdness
  log_weirdness();

  if (info_struct_v < latest_struct_v) {
    upgrade(store);
  }

  // initialize current mapping
  {
    int primary, up_primary;
    vector<int> acting, up;
    get_osdmap()->pg_to_up_acting_osds(
      pg_id.pgid, &up, &up_primary, &acting, &primary);
    init_primary_up_acting(
      up,
      acting,
      up_primary,
      primary);
    int rr = OSDMap::calc_pg_role(osd->whoami, acting);
    if (pool.info.is_replicated() || rr == pg_whoami.shard)
      set_role(rr);
    else
      set_role(-1);
  }

  PG::RecoveryCtx rctx(0, 0, 0, new ObjectStore::Transaction);
  handle_initialize(&rctx);
  // note: we don't activate here because we know the OSD will advance maps
  // during boot.
  write_if_dirty(*rctx.transaction);
  store->queue_transaction(ch, std::move(*rctx.transaction));
  delete rctx.transaction;
}

void PG::log_weirdness()
{
  if (pg_log.get_tail() != info.log_tail)
    osd->clog->error() << info.pgid
		       << " info mismatch, log.tail " << pg_log.get_tail()
		       << " != info.log_tail " << info.log_tail;
  if (pg_log.get_head() != info.last_update)
    osd->clog->error() << info.pgid
		       << " info mismatch, log.head " << pg_log.get_head()
		       << " != info.last_update " << info.last_update;

  if (!pg_log.get_log().empty()) {
    // sloppy check
    if ((pg_log.get_log().log.begin()->version <= pg_log.get_tail()))
      osd->clog->error() << info.pgid
			<< " log bound mismatch, info (tail,head] ("
			<< pg_log.get_tail() << "," << pg_log.get_head() << "]"
			<< " actual ["
			<< pg_log.get_log().log.begin()->version << ","
			 << pg_log.get_log().log.rbegin()->version << "]";
  }
  
  if (pg_log.get_log().caller_ops.size() > pg_log.get_log().log.size()) {
    osd->clog->error() << info.pgid
		      << " caller_ops.size " << pg_log.get_log().caller_ops.size()
		       << " > log size " << pg_log.get_log().log.size();
  }
}

void PG::update_snap_map(
  const vector<pg_log_entry_t> &log_entries,
  ObjectStore::Transaction &t)
{
  for (vector<pg_log_entry_t>::const_iterator i = log_entries.begin();
       i != log_entries.end();
       ++i) {
    OSDriver::OSTransaction _t(osdriver.get_transaction(&t));
    if (i->soid.snap < CEPH_MAXSNAP) {
      if (i->is_delete()) {
	int r = snap_mapper.remove_oid(
	  i->soid,
	  &_t);
	if (r != 0)
	  derr << __func__ << " remove_oid " << i->soid << " failed with " << r << dendl;
        // On removal tolerate missing key corruption
        ceph_assert(r == 0 || r == -ENOENT);
      } else if (i->is_update()) {
	ceph_assert(i->snaps.length() > 0);
	vector<snapid_t> snaps;
	bufferlist snapbl = i->snaps;
	auto p = snapbl.cbegin();
	try {
	  decode(snaps, p);
	} catch (...) {
	  derr << __func__ << " decode snaps failure on " << *i << dendl;
	  snaps.clear();
	}
	set<snapid_t> _snaps(snaps.begin(), snaps.end());

	if (i->is_clone() || i->is_promote()) {
	  snap_mapper.add_oid(
	    i->soid,
	    _snaps,
	    &_t);
	} else if (i->is_modify()) {
	  int r = snap_mapper.update_snaps(
	    i->soid,
	    _snaps,
	    0,
	    &_t);
	  ceph_assert(r == 0);
	} else {
	  ceph_assert(i->is_clean());
	}
      }
    }
  }
}

/**
 * filter trimming|trimmed snaps out of snapcontext
 */
void PG::filter_snapc(vector<snapid_t> &snaps)
{
  // nothing needs to trim, we can return immediately
  if (snap_trimq.empty() && info.purged_snaps.empty())
    return;

  bool filtering = false;
  vector<snapid_t> newsnaps;
  for (vector<snapid_t>::iterator p = snaps.begin();
       p != snaps.end();
       ++p) {
    if (snap_trimq.contains(*p) || info.purged_snaps.contains(*p)) {
      if (!filtering) {
	// start building a new vector with what we've seen so far
	dout(10) << "filter_snapc filtering " << snaps << dendl;
	newsnaps.insert(newsnaps.begin(), snaps.begin(), p);
	filtering = true;
      }
      dout(20) << "filter_snapc  removing trimq|purged snap " << *p << dendl;
    } else {
      if (filtering)
	newsnaps.push_back(*p);  // continue building new vector
    }
  }
  if (filtering) {
    snaps.swap(newsnaps);
    dout(10) << "filter_snapc  result " << snaps << dendl;
  }
}

void PG::requeue_object_waiters(map<hobject_t, list<OpRequestRef>>& m)
{
  for (map<hobject_t, list<OpRequestRef>>::iterator it = m.begin();
       it != m.end();
       ++it)
    requeue_ops(it->second);
  m.clear();
}

void PG::requeue_op(OpRequestRef op)
{
  auto p = waiting_for_map.find(op->get_source());
  if (p != waiting_for_map.end()) {
    dout(20) << __func__ << " " << op << " (waiting_for_map " << p->first << ")"
	     << dendl;
    p->second.push_front(op);
  } else {
    dout(20) << __func__ << " " << op << dendl;
    osd->enqueue_front(
      OpQueueItem(
        unique_ptr<OpQueueItem::OpQueueable>(new PGOpItem(info.pgid, op)),
	op->get_req()->get_cost(),
	op->get_req()->get_priority(),
	op->get_req()->get_recv_stamp(),
	op->get_req()->get_source().num(),
	get_osdmap_epoch()));
  }
}

void PG::requeue_ops(list<OpRequestRef> &ls)
{
  for (list<OpRequestRef>::reverse_iterator i = ls.rbegin();
       i != ls.rend();
       ++i) {
    requeue_op(*i);
  }
  ls.clear();
}

void PG::requeue_map_waiters()
{
  epoch_t epoch = get_osdmap_epoch();
  auto p = waiting_for_map.begin();
  while (p != waiting_for_map.end()) {
    if (epoch < p->second.front()->min_epoch) {
      dout(20) << __func__ << " " << p->first << " front op "
	       << p->second.front() << " must still wait, doing nothing"
	       << dendl;
      ++p;
    } else {
      dout(20) << __func__ << " " << p->first << " " << p->second << dendl;
      for (auto q = p->second.rbegin(); q != p->second.rend(); ++q) {
	auto req = *q;
	osd->enqueue_front(OpQueueItem(
          unique_ptr<OpQueueItem::OpQueueable>(new PGOpItem(info.pgid, req)),
	  req->get_req()->get_cost(),
	  req->get_req()->get_priority(),
	  req->get_req()->get_recv_stamp(),
	  req->get_req()->get_source().num(),
	  epoch));
      }
      p = waiting_for_map.erase(p);
    }
  }
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
  bool nodeep_scrub = false;
  ceph_assert(is_locked());
  if (!(is_primary() && is_active() && is_clean() && !is_scrubbing())) {
    return false;
  }

  double deep_scrub_interval = 0;
  pool.info.opts.get(pool_opts_t::DEEP_SCRUB_INTERVAL, &deep_scrub_interval);
  if (deep_scrub_interval <= 0) {
    deep_scrub_interval = cct->_conf->osd_deep_scrub_interval;
  }
  bool time_for_deep = ceph_clock_now() >=
    info.history.last_deep_scrub_stamp + deep_scrub_interval;

  bool deep_coin_flip = false;
  // Only add random deep scrubs when NOT user initiated scrub
  if (!scrubber.must_scrub)
      deep_coin_flip = (rand() % 100) < cct->_conf->osd_deep_scrub_randomize_ratio * 100;
  dout(20) << __func__ << ": time_for_deep=" << time_for_deep << " deep_coin_flip=" << deep_coin_flip << dendl;

  time_for_deep = (time_for_deep || deep_coin_flip);

  //NODEEP_SCRUB so ignore time initiated deep-scrub
  if (osd->osd->get_osdmap()->test_flag(CEPH_OSDMAP_NODEEP_SCRUB) ||
      pool.info.has_flag(pg_pool_t::FLAG_NODEEP_SCRUB)) {
    time_for_deep = false;
    nodeep_scrub = true;
  }

  if (!scrubber.must_scrub) {
    ceph_assert(!scrubber.must_deep_scrub);

    //NOSCRUB so skip regular scrubs
    if ((osd->osd->get_osdmap()->test_flag(CEPH_OSDMAP_NOSCRUB) ||
	 pool.info.has_flag(pg_pool_t::FLAG_NOSCRUB)) && !time_for_deep) {
      if (scrubber.reserved) {
        // cancel scrub if it is still in scheduling,
        // so pgs from other pools where scrub are still legal
        // have a chance to go ahead with scrubbing.
        clear_scrub_reserved();
        scrub_unreserve_replicas();
      }
      return false;
    }
  }

  // Clear these in case user issues the scrub/repair command during
  // the scheduling of the scrub/repair (e.g. request reservation)
  scrubber.deep_scrub_on_error = false;
  scrubber.auto_repair = false;
  if (cct->_conf->osd_scrub_auto_repair
      && get_pgbackend()->auto_repair_supported()
      // respect the command from user, and not do auto-repair
      && !scrubber.must_repair
      && !scrubber.must_scrub
      && !scrubber.must_deep_scrub) {
    if (time_for_deep) {
      dout(20) << __func__ << ": auto repair with deep scrubbing" << dendl;
      scrubber.auto_repair = true;
    } else {
      dout(20) << __func__ << ": auto repair with scrubbing, rescrub if errors found" << dendl;
      scrubber.deep_scrub_on_error = true;
    }
  }

  bool ret = true;
  if (!scrubber.reserved) {
    ceph_assert(scrubber.reserved_peers.empty());
    if ((cct->_conf->osd_scrub_during_recovery || !osd->is_recovery_active()) &&
         osd->inc_scrubs_pending()) {
      dout(20) << __func__ << ": reserved locally, reserving replicas" << dendl;
      scrubber.reserved = true;
      scrubber.reserved_peers.insert(pg_whoami);
      scrub_reserve_replicas();
    } else {
      dout(20) << __func__ << ": failed to reserve locally" << dendl;
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
      } else if (!scrubber.must_deep_scrub && info.stats.stats.sum.num_deep_scrub_errors) {
	if (!nodeep_scrub) {
	  osd->clog->info() << "osd." << osd->whoami
			    << " pg " << info.pgid
			    << " Deep scrub errors, upgrading scrub to deep-scrub";
	  state_set(PG_STATE_DEEP_SCRUB);
	} else if (!scrubber.must_scrub) {
	  osd->clog->error() << "osd." << osd->whoami
			     << " pg " << info.pgid
			     << " Regular scrub skipped due to deep-scrub errors and nodeep-scrub set";
	  clear_scrub_reserved();
	  scrub_unreserve_replicas();
	  return false;
	} else {
	  osd->clog->error() << "osd." << osd->whoami
			     << " pg " << info.pgid
			     << " Regular scrub request, deep-scrub details will be lost";
	}
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
  if (!is_primary())
    return;

  utime_t reg_stamp;
  bool must = false;
  if (scrubber.must_scrub) {
    // Set the smallest time that isn't utime_t()
    reg_stamp = utime_t(0,1);
    must = true;
  } else if (info.stats.stats_invalid && cct->_conf->osd_scrub_invalid_stats) {
    reg_stamp = ceph_clock_now();
    must = true;
  } else {
    reg_stamp = info.history.last_scrub_stamp;
  }
  // note down the sched_time, so we can locate this scrub, and remove it
  // later on.
  double scrub_min_interval = 0, scrub_max_interval = 0;
  pool.info.opts.get(pool_opts_t::SCRUB_MIN_INTERVAL, &scrub_min_interval);
  pool.info.opts.get(pool_opts_t::SCRUB_MAX_INTERVAL, &scrub_max_interval);
  ceph_assert(scrubber.scrub_reg_stamp == utime_t());
  scrubber.scrub_reg_stamp = osd->reg_pg_scrub(info.pgid,
					       reg_stamp,
					       scrub_min_interval,
					       scrub_max_interval,
					       must);
  dout(10) << __func__ << " pg " << pg_id << " register next scrub, scrub time "
      << scrubber.scrub_reg_stamp << ", must = " << (int)must << dendl;
}

void PG::unreg_next_scrub()
{
  if (is_primary()) {
    osd->unreg_pg_scrub(info.pgid, scrubber.scrub_reg_stamp);
    scrubber.scrub_reg_stamp = utime_t();
  }
}

void PG::do_replica_scrub_map(OpRequestRef op)
{
  const MOSDRepScrubMap *m = static_cast<const MOSDRepScrubMap*>(op->get_req());
  dout(7) << __func__ << " " << *m << dendl;
  if (m->map_epoch < info.history.same_interval_since) {
    dout(10) << __func__ << " discarding old from "
	     << m->map_epoch << " < " << info.history.same_interval_since
	     << dendl;
    return;
  }
  if (!scrubber.is_chunky_scrub_active()) {
    dout(10) << __func__ << " scrub isn't active" << dendl;
    return;
  }

  op->mark_started();

  auto p = const_cast<bufferlist&>(m->get_data()).cbegin();
  scrubber.received_maps[m->from].decode(p, info.pgid.pool());
  dout(10) << "map version is "
	   << scrubber.received_maps[m->from].valid_through
	   << dendl;

  dout(10) << __func__ << " waiting_on_whom was " << scrubber.waiting_on_whom
	   << dendl;
  ceph_assert(scrubber.waiting_on_whom.count(m->from));
  scrubber.waiting_on_whom.erase(m->from);
  if (m->preempted) {
    dout(10) << __func__ << " replica was preempted, setting flag" << dendl;
    scrub_preempted = true;
  }
  if (scrubber.waiting_on_whom.empty()) {
    requeue_scrub(ops_blocked_by_scrub());
  }
}

// send scrub v3 messages (chunky scrub)
void PG::_request_scrub_map(
  pg_shard_t replica, eversion_t version,
  hobject_t start, hobject_t end,
  bool deep,
  bool allow_preemption)
{
  ceph_assert(replica != pg_whoami);
  dout(10) << "scrub  requesting scrubmap from osd." << replica
	   << " deep " << (int)deep << dendl;
  MOSDRepScrub *repscrubop = new MOSDRepScrub(
    spg_t(info.pgid.pgid, replica.shard), version,
    get_osdmap_epoch(),
    get_last_peering_reset(),
    start, end, deep,
    allow_preemption,
    scrubber.priority,
    ops_blocked_by_scrub());
  // default priority, we want the rep scrub processed prior to any recovery
  // or client io messages (we are holding a lock!)
  osd->send_message_osd_cluster(
    replica.osd, repscrubop, get_osdmap_epoch());
}

void PG::handle_scrub_reserve_request(OpRequestRef op)
{
  dout(7) << __func__ << " " << *op->get_req() << dendl;
  op->mark_started();
  if (scrubber.reserved) {
    dout(10) << __func__ << " ignoring reserve request: Already reserved"
	     << dendl;
    return;
  }
  if ((cct->_conf->osd_scrub_during_recovery || !osd->is_recovery_active()) &&
      osd->inc_scrubs_pending()) {
    scrubber.reserved = true;
  } else {
    dout(20) << __func__ << ": failed to reserve remotely" << dendl;
    scrubber.reserved = false;
  }
  const MOSDScrubReserve *m =
    static_cast<const MOSDScrubReserve*>(op->get_req());
  Message *reply = new MOSDScrubReserve(
    spg_t(info.pgid.pgid, primary.shard),
    m->map_epoch,
    scrubber.reserved ? MOSDScrubReserve::GRANT : MOSDScrubReserve::REJECT,
    pg_whoami);
  osd->send_message_osd_cluster(reply, op->get_req()->get_connection());
}

void PG::handle_scrub_reserve_grant(OpRequestRef op, pg_shard_t from)
{
  dout(7) << __func__ << " " << *op->get_req() << dendl;
  op->mark_started();
  if (!scrubber.reserved) {
    dout(10) << "ignoring obsolete scrub reserve reply" << dendl;
    return;
  }
  if (scrubber.reserved_peers.find(from) != scrubber.reserved_peers.end()) {
    dout(10) << " already had osd." << from << " reserved" << dendl;
  } else {
    dout(10) << " osd." << from << " scrub reserve = success" << dendl;
    scrubber.reserved_peers.insert(from);
    sched_scrub();
  }
}

void PG::handle_scrub_reserve_reject(OpRequestRef op, pg_shard_t from)
{
  dout(7) << __func__ << " " << *op->get_req() << dendl;
  op->mark_started();
  if (!scrubber.reserved) {
    dout(10) << "ignoring obsolete scrub reserve reply" << dendl;
    return;
  }
  if (scrubber.reserved_peers.find(from) != scrubber.reserved_peers.end()) {
    dout(10) << " already had osd." << from << " reserved" << dendl;
  } else {
    /* One decline stops this pg from being scheduled for scrubbing. */
    dout(10) << " osd." << from << " scrub reserve = fail" << dendl;
    scrubber.reserve_failed = true;
    sched_scrub();
  }
}

void PG::handle_scrub_reserve_release(OpRequestRef op)
{
  dout(7) << __func__ << " " << *op->get_req() << dendl;
  op->mark_started();
  clear_scrub_reserved();
}

// We can zero the value of primary num_bytes as just an atomic.
// However, setting above zero reserves space for backfill and requires
// the OSDService::stat_lock which protects all OSD usage
void PG::set_reserved_num_bytes(int64_t primary, int64_t local) {
  ceph_assert(osd->stat_lock.is_locked_by_me());
  primary_num_bytes.store(primary);
  local_num_bytes.store(local);
  return;
}

void PG::clear_reserved_num_bytes() {
  primary_num_bytes.store(0);
  local_num_bytes.store(0);
  return;
}

void PG::reject_reservation()
{
  clear_reserved_num_bytes();
  osd->send_message_osd_cluster(
    primary.osd,
    new MBackfillReserve(
      MBackfillReserve::REJECT,
      spg_t(info.pgid.pgid, primary.shard),
      get_osdmap_epoch()),
    get_osdmap_epoch());
}

void PG::schedule_backfill_retry(float delay)
{
  std::lock_guard lock(osd->recovery_request_lock);
  osd->recovery_request_timer.add_event_after(
    delay,
    new QueuePeeringEvt<RequestBackfill>(
      this, get_osdmap_epoch(),
      RequestBackfill()));
}

void PG::schedule_recovery_retry(float delay)
{
  std::lock_guard lock(osd->recovery_request_lock);
  osd->recovery_request_timer.add_event_after(
    delay,
    new QueuePeeringEvt<DoRecovery>(
      this, get_osdmap_epoch(),
      DoRecovery()));
}

void PG::clear_scrub_reserved()
{
  scrubber.reserved_peers.clear();
  scrubber.reserve_failed = false;

  if (scrubber.reserved) {
    scrubber.reserved = false;
    osd->dec_scrubs_pending();
  }
}

void PG::scrub_reserve_replicas()
{
  ceph_assert(backfill_targets.empty());
  for (set<pg_shard_t>::iterator i = acting_recovery_backfill.begin();
       i != acting_recovery_backfill.end();
       ++i) {
    if (*i == pg_whoami) continue;
    dout(10) << "scrub requesting reserve from osd." << *i << dendl;
    osd->send_message_osd_cluster(
      i->osd,
      new MOSDScrubReserve(spg_t(info.pgid.pgid, i->shard),
			   get_osdmap_epoch(),
			   MOSDScrubReserve::REQUEST, pg_whoami),
      get_osdmap_epoch());
  }
}

void PG::scrub_unreserve_replicas()
{
  ceph_assert(backfill_targets.empty());
  for (set<pg_shard_t>::iterator i = acting_recovery_backfill.begin();
       i != acting_recovery_backfill.end();
       ++i) {
    if (*i == pg_whoami) continue;
    dout(10) << "scrub requesting unreserve from osd." << *i << dendl;
    osd->send_message_osd_cluster(
      i->osd,
      new MOSDScrubReserve(spg_t(info.pgid.pgid, i->shard),
			   get_osdmap_epoch(),
			   MOSDScrubReserve::RELEASE, pg_whoami),
      get_osdmap_epoch());
  }
}

void PG::_scan_rollback_obs(const vector<ghobject_t> &rollback_obs)
{
  ObjectStore::Transaction t;
  eversion_t trimmed_to = last_rollback_info_trimmed_to_applied;
  for (vector<ghobject_t>::const_iterator i = rollback_obs.begin();
       i != rollback_obs.end();
       ++i) {
    if (i->generation < trimmed_to.version) {
      dout(10) << __func__ << "osd." << osd->whoami
	       << " pg " << info.pgid
	       << " found obsolete rollback obj "
	       << *i << " generation < trimmed_to "
	       << trimmed_to
	       << "...repaired" << dendl;
      t.remove(coll, *i);
    }
  }
  if (!t.empty()) {
    derr << __func__ << ": queueing trans to clean up obsolete rollback objs"
	 << dendl;
    osd->store->queue_transaction(ch, std::move(t), NULL);
  }
}

void PG::_scan_snaps(ScrubMap &smap) 
{
  hobject_t head;
  SnapSet snapset;

  // Test qa/standalone/scrub/osd-scrub-snaps.sh uses this message to verify 
  // caller using clean_meta_map(), and it works properly.
  dout(20) << __func__ << " start" << dendl;

  for (map<hobject_t, ScrubMap::object>::reverse_iterator i = smap.objects.rbegin();
       i != smap.objects.rend();
       ++i) {
    const hobject_t &hoid = i->first;
    ScrubMap::object &o = i->second;

    dout(20) << __func__ << " " << hoid << dendl;

    ceph_assert(!hoid.is_snapdir());
    if (hoid.is_head()) {
      // parse the SnapSet
      bufferlist bl;
      if (o.attrs.find(SS_ATTR) == o.attrs.end()) {
	continue;
      }
      bl.push_back(o.attrs[SS_ATTR]);
      auto p = bl.cbegin();
      try {
	decode(snapset, p);
      } catch(...) {
	continue;
      }
      head = hoid.get_head();
      continue;
    }
    if (hoid.snap < CEPH_MAXSNAP) {
      // check and if necessary fix snap_mapper
      if (hoid.get_head() != head) {
	derr << __func__ << " no head for " << hoid << " (have " << head << ")"
	     << dendl;
	continue;
      }
      set<snapid_t> obj_snaps;
      auto p = snapset.clone_snaps.find(hoid.snap);
      if (p == snapset.clone_snaps.end()) {
	derr << __func__ << " no clone_snaps for " << hoid << " in " << snapset
	     << dendl;
	continue;
      }
      obj_snaps.insert(p->second.begin(), p->second.end());
      set<snapid_t> cur_snaps;
      int r = snap_mapper.get_snaps(hoid, &cur_snaps);
      if (r != 0 && r != -ENOENT) {
	derr << __func__ << ": get_snaps returned " << cpp_strerror(r) << dendl;
	ceph_abort();
      }
      if (r == -ENOENT || cur_snaps != obj_snaps) {
	ObjectStore::Transaction t;
	OSDriver::OSTransaction _t(osdriver.get_transaction(&t));
	if (r == 0) {
	  r = snap_mapper.remove_oid(hoid, &_t);
	  if (r != 0) {
	    derr << __func__ << ": remove_oid returned " << cpp_strerror(r)
		 << dendl;
	    ceph_abort();
	  }
	  osd->clog->error() << "osd." << osd->whoami
			    << " found snap mapper error on pg "
			    << info.pgid
			    << " oid " << hoid << " snaps in mapper: "
			    << cur_snaps << ", oi: "
			    << obj_snaps
			    << "...repaired";
	} else {
	  osd->clog->error() << "osd." << osd->whoami
			    << " found snap mapper error on pg "
			    << info.pgid
			    << " oid " << hoid << " snaps missing in mapper"
			    << ", should be: "
			    << obj_snaps
			     << " was " << cur_snaps << " r " << r
			    << "...repaired";
	}
	snap_mapper.add_oid(hoid, obj_snaps, &_t);

	// wait for repair to apply to avoid confusing other bits of the system.
	{
	  Cond my_cond;
	  Mutex my_lock("PG::_scan_snaps my_lock");
	  int r = 0;
	  bool done;
	  t.register_on_applied_sync(
	    new C_SafeCond(&my_lock, &my_cond, &done, &r));
	  r = osd->store->queue_transaction(ch, std::move(t));
	  if (r != 0) {
	    derr << __func__ << ": queue_transaction got " << cpp_strerror(r)
		 << dendl;
	  } else {
	    my_lock.Lock();
	    while (!done)
	      my_cond.Wait(my_lock);
	    my_lock.Unlock();
	  }
	}
      }
    }
  }
}

void PG::_repair_oinfo_oid(ScrubMap &smap)
{
  for (map<hobject_t, ScrubMap::object>::reverse_iterator i = smap.objects.rbegin();
       i != smap.objects.rend();
       ++i) {
    const hobject_t &hoid = i->first;
    ScrubMap::object &o = i->second;

    bufferlist bl;
    if (o.attrs.find(OI_ATTR) == o.attrs.end()) {
      continue;
    }
    bl.push_back(o.attrs[OI_ATTR]);
    object_info_t oi;
    try {
      oi.decode(bl);
    } catch(...) {
      continue;
    }
    if (oi.soid != hoid) {
      ObjectStore::Transaction t;
      OSDriver::OSTransaction _t(osdriver.get_transaction(&t));
      osd->clog->error() << "osd." << osd->whoami
			    << " found object info error on pg "
			    << info.pgid
			    << " oid " << hoid << " oid in object info: "
			    << oi.soid
			    << "...repaired";
      // Fix object info
      oi.soid = hoid;
      bl.clear();
      encode(oi, bl, get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));

      bufferptr bp(bl.c_str(), bl.length());
      o.attrs[OI_ATTR] = bp;

      t.setattr(coll, ghobject_t(hoid), OI_ATTR, bl);
      int r = osd->store->queue_transaction(ch, std::move(t));
      if (r != 0) {
	derr << __func__ << ": queue_transaction got " << cpp_strerror(r)
	     << dendl;
      }
    }
  }
}
int PG::build_scrub_map_chunk(
  ScrubMap &map,
  ScrubMapBuilder &pos,
  hobject_t start,
  hobject_t end,
  bool deep,
  ThreadPool::TPHandle &handle)
{
  dout(10) << __func__ << " [" << start << "," << end << ") "
	   << " pos " << pos
	   << dendl;

  // start
  while (pos.empty()) {
    pos.deep = deep;
    map.valid_through = info.last_update;

    // objects
    vector<ghobject_t> rollback_obs;
    pos.ret = get_pgbackend()->objects_list_range(
      start,
      end,
      &pos.ls,
      &rollback_obs);
    if (pos.ret < 0) {
      dout(5) << "objects_list_range error: " << pos.ret << dendl;
      return pos.ret;
    }
    if (pos.ls.empty()) {
      break;
    }
    _scan_rollback_obs(rollback_obs);
    pos.pos = 0;
    return -EINPROGRESS;
  }

  // scan objects
  while (!pos.done()) {
    int r = get_pgbackend()->be_scan_list(map, pos);
    if (r == -EINPROGRESS) {
      return r;
    }
  }

  // finish
  dout(20) << __func__ << " finishing" << dendl;
  ceph_assert(pos.done());
  _repair_oinfo_oid(map);
  if (!is_primary()) {
    ScrubMap for_meta_scrub;
    // In case we restarted smaller chunk, clear old data
    scrubber.cleaned_meta_map.clear_from(scrubber.start);
    scrubber.cleaned_meta_map.insert(map);
    scrubber.clean_meta_map(for_meta_scrub);
    _scan_snaps(for_meta_scrub);
  }

  dout(20) << __func__ << " done, got " << map.objects.size() << " items"
	   << dendl;
  return 0;
}

void PG::Scrubber::cleanup_store(ObjectStore::Transaction *t) {
  if (!store)
    return;
  struct OnComplete : Context {
    std::unique_ptr<Scrub::Store> store;
    explicit OnComplete(
      std::unique_ptr<Scrub::Store> &&store)
      : store(std::move(store)) {}
    void finish(int) override {}
  };
  store->cleanup(t);
  t->register_on_complete(new OnComplete(std::move(store)));
  ceph_assert(!store);
}

void PG::repair_object(
  const hobject_t& soid, list<pair<ScrubMap::object, pg_shard_t> > *ok_peers,
  pg_shard_t bad_peer)
{
  list<pg_shard_t> op_shards;
  for (auto i : *ok_peers) {
    op_shards.push_back(i.second);
  }
  dout(10) << "repair_object " << soid << " bad_peer osd."
	   << bad_peer << " ok_peers osd.{" << op_shards << "}" << dendl;
  ScrubMap::object &po = ok_peers->back().first;
  eversion_t v;
  bufferlist bv;
  bv.push_back(po.attrs[OI_ATTR]);
  object_info_t oi;
  try {
    auto bliter = bv.cbegin();
    decode(oi, bliter);
  } catch (...) {
    dout(0) << __func__ << ": Need version of replica, bad object_info_t: " << soid << dendl;
    ceph_abort();
  }
  if (bad_peer != primary) {
    peer_missing[bad_peer].add(soid, oi.version, eversion_t(), false);
  } else {
    // We should only be scrubbing if the PG is clean.
    ceph_assert(waiting_for_unreadable_object.empty());

    pg_log.missing_add(soid, oi.version, eversion_t());

    pg_log.set_last_requested(0);
    dout(10) << __func__ << ": primary = " << primary << dendl;
  }

  if (is_ec_pg() || bad_peer == primary) {
    // we'd better collect all shard for EC pg, and prepare good peers as the
    // source of pull in the case of replicated pg.
    missing_loc.add_missing(soid, oi.version, eversion_t());
    list<pair<ScrubMap::object, pg_shard_t> >::iterator i;
    for (i = ok_peers->begin();
	i != ok_peers->end();
	++i)
      missing_loc.add_location(soid, i->second);
  }
}

/* replica_scrub
 *
 * Wait for last_update_applied to match msg->scrub_to as above. Wait
 * for pushes to complete in case of recent recovery. Build a single
 * scrubmap of objects that are in the range [msg->start, msg->end).
 */
void PG::replica_scrub(
  OpRequestRef op,
  ThreadPool::TPHandle &handle)
{
  const MOSDRepScrub *msg = static_cast<const MOSDRepScrub *>(op->get_req());
  ceph_assert(!scrubber.active_rep_scrub);
  dout(7) << "replica_scrub" << dendl;

  if (msg->map_epoch < info.history.same_interval_since) {
    dout(10) << "replica_scrub discarding old replica_scrub from "
	     << msg->map_epoch << " < " << info.history.same_interval_since 
	     << dendl;
    return;
  }

  ceph_assert(msg->chunky);
  if (active_pushes > 0) {
    dout(10) << "waiting for active pushes to finish" << dendl;
    scrubber.active_rep_scrub = op;
    return;
  }

  scrubber.state = Scrubber::BUILD_MAP_REPLICA;
  scrubber.replica_scrub_start = msg->min_epoch;
  scrubber.start = msg->start;
  scrubber.end = msg->end;
  scrubber.max_end = msg->end;
  scrubber.deep = msg->deep;
  scrubber.epoch_start = info.history.same_interval_since;
  if (msg->priority) {
    scrubber.priority = msg->priority;
  } else {
    scrubber.priority = get_scrub_priority();
  }

  scrub_can_preempt = msg->allow_preemption;
  scrub_preempted = false;
  scrubber.replica_scrubmap_pos.reset();

  requeue_scrub(msg->high_priority);
}

/* Scrub:
 * PG_STATE_SCRUBBING is set when the scrub is queued
 * 
 * scrub will be chunky if all OSDs in PG support chunky scrub
 * scrub will fail if OSDs are too old.
 */
void PG::scrub(epoch_t queued, ThreadPool::TPHandle &handle)
{
  if (cct->_conf->osd_scrub_sleep > 0 &&
      (scrubber.state == PG::Scrubber::NEW_CHUNK ||
       scrubber.state == PG::Scrubber::INACTIVE) &&
       scrubber.needs_sleep) {
    ceph_assert(!scrubber.sleeping);
    dout(20) << __func__ << " state is INACTIVE|NEW_CHUNK, sleeping" << dendl;

    // Do an async sleep so we don't block the op queue
    OSDService *osds = osd;
    spg_t pgid = get_pgid();
    int state = scrubber.state;
    auto scrub_requeue_callback =
        new FunctionContext([osds, pgid, state](int r) {
          PGRef pg = osds->osd->lookup_lock_pg(pgid);
          if (pg == nullptr) {
            lgeneric_dout(osds->osd->cct, 20)
                << "scrub_requeue_callback: Could not find "
                << "PG " << pgid << " can't complete scrub requeue after sleep"
                << dendl;
            return;
          }
          pg->scrubber.sleeping = false;
          pg->scrubber.needs_sleep = false;
          lgeneric_dout(pg->cct, 20)
              << "scrub_requeue_callback: slept for "
              << ceph_clock_now() - pg->scrubber.sleep_start
              << ", re-queuing scrub with state " << state << dendl;
          pg->scrub_queued = false;
          pg->requeue_scrub();
          pg->scrubber.sleep_start = utime_t();
          pg->unlock();
        });
    std::lock_guard l(osd->sleep_lock);
    osd->sleep_timer.add_event_after(cct->_conf->osd_scrub_sleep,
                                           scrub_requeue_callback);
    scrubber.sleeping = true;
    scrubber.sleep_start = ceph_clock_now();
    return;
  }
  if (pg_has_reset_since(queued)) {
    return;
  }
  ceph_assert(scrub_queued);
  scrub_queued = false;
  scrubber.needs_sleep = true;

  // for the replica
  if (!is_primary() &&
      scrubber.state == PG::Scrubber::BUILD_MAP_REPLICA) {
    chunky_scrub(handle);
    return;
  }

  if (!is_primary() || !is_active() || !is_clean() || !is_scrubbing()) {
    dout(10) << "scrub -- not primary or active or not clean" << dendl;
    state_clear(PG_STATE_SCRUBBING);
    state_clear(PG_STATE_REPAIR);
    state_clear(PG_STATE_DEEP_SCRUB);
    publish_stats_to_osd();
    return;
  }

  if (!scrubber.active) {
    ceph_assert(backfill_targets.empty());

    scrubber.deep = state_test(PG_STATE_DEEP_SCRUB);

    dout(10) << "starting a new chunky scrub" << dendl;
  }

  chunky_scrub(handle);
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
 *  (7) Wait for digest updates to apply
 *
 * This logic is encoded in the mostly linear state machine:
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
 *           |              |   |
 *           |              |   |
 *  _________v__________    |   |
 * |                    |   |   |
 * |WAIT_DIGEST_UPDATES |   |   |
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
    dout(20) << "scrub state " << Scrubber::state_string(scrubber.state)
	     << " [" << scrubber.start << "," << scrubber.end << ")"
	     << " max_end " << scrubber.max_end << dendl;

    switch (scrubber.state) {
      case PG::Scrubber::INACTIVE:
        dout(10) << "scrub start" << dendl;
	ceph_assert(is_primary());

        publish_stats_to_osd();
        scrubber.epoch_start = info.history.same_interval_since;
        scrubber.active = true;

	osd->inc_scrubs_active(scrubber.reserved);
	if (scrubber.reserved) {
	  scrubber.reserved = false;
	  scrubber.reserved_peers.clear();
	}

	{
	  ObjectStore::Transaction t;
	  scrubber.cleanup_store(&t);
	  scrubber.store.reset(Scrub::Store::create(osd->store, &t,
						    info.pgid, coll));
	  osd->store->queue_transaction(ch, std::move(t), nullptr);
	}

        // Don't include temporary objects when scrubbing
        scrubber.start = info.pgid.pgid.get_hobj_start();
        scrubber.state = PG::Scrubber::NEW_CHUNK;

	{
	  bool repair = state_test(PG_STATE_REPAIR);
	  bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
	  const char *mode = (repair ? "repair": (deep_scrub ? "deep-scrub" : "scrub"));
	  stringstream oss;
	  oss << info.pgid.pgid << " " << mode << " starts" << std::endl;
	  osd->clog->debug(oss);
	}

	scrubber.preempt_left = cct->_conf.get_val<uint64_t>(
	  "osd_scrub_max_preemptions");
	scrubber.preempt_divisor = 1;
        break;

      case PG::Scrubber::NEW_CHUNK:
        scrubber.primary_scrubmap = ScrubMap();
        scrubber.received_maps.clear();

	// begin (possible) preemption window
	if (scrub_preempted) {
	  scrubber.preempt_left--;
	  scrubber.preempt_divisor *= 2;
	  dout(10) << __func__ << " preempted, " << scrubber.preempt_left
		   << " left" << dendl;
	  scrub_preempted = false;
	}
	scrub_can_preempt = scrubber.preempt_left > 0;

        {
          /* get the start and end of our scrub chunk
	   *
	   * Our scrub chunk has an important restriction we're going to need to
	   * respect. We can't let head be start or end.
	   * Using a half-open interval means that if end == head,
	   * we'd scrub/lock head and the clone right next to head in different
	   * chunks which would allow us to miss clones created between
	   * scrubbing that chunk and scrubbing the chunk including head.
	   * This isn't true for any of the other clones since clones can
	   * only be created "just to the left of" head.  There is one exception
	   * to this: promotion of clones which always happens to the left of the
	   * left-most clone, but promote_object checks the scrubber in that
	   * case, so it should be ok.  Also, it's ok to "miss" clones at the
	   * left end of the range if we are a tier because they may legitimately
	   * not exist (see _scrub).
	   */
	  int min = std::max<int64_t>(3, cct->_conf->osd_scrub_chunk_min /
				      scrubber.preempt_divisor);
	  int max = std::max<int64_t>(min, cct->_conf->osd_scrub_chunk_max /
                                      scrubber.preempt_divisor);
          hobject_t start = scrubber.start;
	  hobject_t candidate_end;
	  vector<hobject_t> objects;
	  ret = get_pgbackend()->objects_list_partial(
	    start,
	    min,
	    max,
	    &objects,
	    &candidate_end);
	  ceph_assert(ret >= 0);

	  if (!objects.empty()) {
	    hobject_t back = objects.back();
	    while (candidate_end.is_head() &&
		   candidate_end == back.get_head()) {
	      candidate_end = back;
	      objects.pop_back();
	      if (objects.empty()) {
		ceph_assert(0 ==
		       "Somehow we got more than 2 objects which"
		       "have the same head but are not clones");
	      }
	      back = objects.back();
	    }
	    if (candidate_end.is_head()) {
	      ceph_assert(candidate_end != back.get_head());
	      candidate_end = candidate_end.get_object_boundary();
	    }
	  } else {
	    ceph_assert(candidate_end.is_max());
	  }

	  if (!_range_available_for_scrub(scrubber.start, candidate_end)) {
	    // we'll be requeued by whatever made us unavailable for scrub
	    dout(10) << __func__ << ": scrub blocked somewhere in range "
		     << "[" << scrubber.start << ", " << candidate_end << ")"
		     << dendl;
	    done = true;
	    break;
	  }
	  scrubber.end = candidate_end;
	  if (scrubber.end > scrubber.max_end)
	    scrubber.max_end = scrubber.end;
        }

        // walk the log to find the latest update that affects our chunk
        scrubber.subset_last_update = eversion_t();
	for (auto p = projected_log.log.rbegin();
	     p != projected_log.log.rend();
	     ++p) {
          if (p->soid >= scrubber.start &&
	      p->soid < scrubber.end) {
            scrubber.subset_last_update = p->version;
            break;
	  }
	}
	if (scrubber.subset_last_update == eversion_t()) {
	  for (list<pg_log_entry_t>::const_reverse_iterator p =
		 pg_log.get_log().log.rbegin();
	       p != pg_log.get_log().log.rend();
	       ++p) {
	    if (p->soid >= scrubber.start &&
		p->soid < scrubber.end) {
	      scrubber.subset_last_update = p->version;
	      break;
	    }
	  }
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
        if (last_update_applied < scrubber.subset_last_update) {
          // will be requeued by op_applied
          dout(15) << "wait for EC read/modify/writes to queue" << dendl;
          done = true;
	  break;
	}

        // ask replicas to scan
        scrubber.waiting_on_whom.insert(pg_whoami);

        // request maps from replicas
	for (set<pg_shard_t>::iterator i = acting_recovery_backfill.begin();
	     i != acting_recovery_backfill.end();
	     ++i) {
	  if (*i == pg_whoami) continue;
          _request_scrub_map(*i, scrubber.subset_last_update,
                             scrubber.start, scrubber.end, scrubber.deep,
			     scrubber.preempt_left > 0);
          scrubber.waiting_on_whom.insert(*i);
        }
	dout(10) << __func__ << " waiting_on_whom " << scrubber.waiting_on_whom
		 << dendl;

	scrubber.state = PG::Scrubber::BUILD_MAP;
	scrubber.primary_scrubmap_pos.reset();
        break;

      case PG::Scrubber::BUILD_MAP:
        ceph_assert(last_update_applied >= scrubber.subset_last_update);

        // build my own scrub map
	if (scrub_preempted) {
	  dout(10) << __func__ << " preempted" << dendl;
	  scrubber.state = PG::Scrubber::BUILD_MAP_DONE;
	  break;
	}
	ret = build_scrub_map_chunk(
	  scrubber.primary_scrubmap,
	  scrubber.primary_scrubmap_pos,
	  scrubber.start, scrubber.end,
	  scrubber.deep,
	  handle);
	if (ret == -EINPROGRESS) {
	  requeue_scrub();
	  done = true;
	  break;
	}
	scrubber.state = PG::Scrubber::BUILD_MAP_DONE;
	break;

      case PG::Scrubber::BUILD_MAP_DONE:
	if (scrubber.primary_scrubmap_pos.ret < 0) {
	  dout(5) << "error: " << scrubber.primary_scrubmap_pos.ret
		  << ", aborting" << dendl;
          scrub_clear_state();
          scrub_unreserve_replicas();
          return;
        }
	dout(10) << __func__ << " waiting_on_whom was "
		 << scrubber.waiting_on_whom << dendl;
	ceph_assert(scrubber.waiting_on_whom.count(pg_whoami));
        scrubber.waiting_on_whom.erase(pg_whoami);

        scrubber.state = PG::Scrubber::WAIT_REPLICAS;
        break;

      case PG::Scrubber::WAIT_REPLICAS:
        if (!scrubber.waiting_on_whom.empty()) {
          // will be requeued by sub_op_scrub_map
          dout(10) << "wait for replicas to build scrub map" << dendl;
          done = true;
	  break;
	}
	// end (possible) preemption window
	scrub_can_preempt = false;
	if (scrub_preempted) {
	  dout(10) << __func__ << " preempted, restarting chunk" << dendl;
	  scrubber.state = PG::Scrubber::NEW_CHUNK;
	} else {
          scrubber.state = PG::Scrubber::COMPARE_MAPS;
        }
        break;

      case PG::Scrubber::COMPARE_MAPS:
        ceph_assert(last_update_applied >= scrubber.subset_last_update);
        ceph_assert(scrubber.waiting_on_whom.empty());

        scrub_compare_maps();
	scrubber.start = scrubber.end;
	scrubber.run_callbacks();

        // requeue the writes from the chunk that just finished
        requeue_ops(waiting_for_scrub);

	scrubber.state = PG::Scrubber::WAIT_DIGEST_UPDATES;

	// fall-thru

      case PG::Scrubber::WAIT_DIGEST_UPDATES:
	if (scrubber.num_digest_updates_pending) {
	  dout(10) << __func__ << " waiting on "
		   << scrubber.num_digest_updates_pending
		   << " digest updates" << dendl;
	  done = true;
	  break;
	}

	scrubber.preempt_left = cct->_conf.get_val<uint64_t>(
	  "osd_scrub_max_preemptions");
	scrubber.preempt_divisor = 1;

	if (!(scrubber.end.is_max())) {
	  scrubber.state = PG::Scrubber::NEW_CHUNK;
	  requeue_scrub();
          done = true;
        } else {
          scrubber.state = PG::Scrubber::FINISH;
        }

	break;

      case PG::Scrubber::FINISH:
        scrub_finish();
        scrubber.state = PG::Scrubber::INACTIVE;
        done = true;

	if (!snap_trimq.empty()) {
	  dout(10) << "scrub finished, requeuing snap_trimmer" << dendl;
	  snap_trimmer_scrub_complete();
	}

        break;

      case PG::Scrubber::BUILD_MAP_REPLICA:
        // build my own scrub map
	if (scrub_preempted) {
	  dout(10) << __func__ << " preempted" << dendl;
	  ret = 0;
	} else {
	  ret = build_scrub_map_chunk(
	    scrubber.replica_scrubmap,
	    scrubber.replica_scrubmap_pos,
	    scrubber.start, scrubber.end,
	    scrubber.deep,
	    handle);
	}
	if (ret == -EINPROGRESS) {
	  requeue_scrub();
	  done = true;
	  break;
	}
	// reply
	{
	  MOSDRepScrubMap *reply = new MOSDRepScrubMap(
	    spg_t(info.pgid.pgid, get_primary().shard),
	    scrubber.replica_scrub_start,
	    pg_whoami);
	  reply->preempted = scrub_preempted;
	  ::encode(scrubber.replica_scrubmap, reply->get_data());
	  osd->send_message_osd_cluster(
	    get_primary().osd, reply,
	    scrubber.replica_scrub_start);
	}
	scrub_preempted = false;
	scrub_can_preempt = false;
	scrubber.state = PG::Scrubber::INACTIVE;
	scrubber.replica_scrubmap = ScrubMap();
	scrubber.replica_scrubmap_pos = ScrubMapBuilder();
	scrubber.start = hobject_t();
	scrubber.end = hobject_t();
	scrubber.max_end = hobject_t();
	done = true;
	break;

      default:
        ceph_abort();
    }
  }
  dout(20) << "scrub final state " << Scrubber::state_string(scrubber.state)
	   << " [" << scrubber.start << "," << scrubber.end << ")"
	   << " max_end " << scrubber.max_end << dendl;
}

bool PG::write_blocked_by_scrub(const hobject_t& soid)
{
  if (soid < scrubber.start || soid >= scrubber.end) {
    return false;
  }
  if (scrub_can_preempt) {
    if (!scrub_preempted) {
      dout(10) << __func__ << " " << soid << " preempted" << dendl;
      scrub_preempted = true;
    } else {
      dout(10) << __func__ << " " << soid << " already preempted" << dendl;
    }
    return false;
  }
  return true;
}

bool PG::range_intersects_scrub(const hobject_t &start, const hobject_t& end)
{
  // does [start, end] intersect [scrubber.start, scrubber.max_end)
  return (start < scrubber.max_end &&
	  end >= scrubber.start);
}

void PG::scrub_clear_state(bool has_error)
{
  ceph_assert(is_locked());
  state_clear(PG_STATE_SCRUBBING);
  if (!has_error)
    state_clear(PG_STATE_REPAIR);
  state_clear(PG_STATE_DEEP_SCRUB);
  publish_stats_to_osd();

  // active -> nothing.
  if (scrubber.active)
    osd->dec_scrubs_active();

  requeue_ops(waiting_for_scrub);

  scrubber.reset();

  // type-specific state clear
  _scrub_clear_state();
}

void PG::scrub_compare_maps() 
{
  dout(10) << __func__ << " has maps, analyzing" << dendl;

  // construct authoritative scrub map for type specific scrubbing
  scrubber.cleaned_meta_map.insert(scrubber.primary_scrubmap);
  map<hobject_t,
      pair<boost::optional<uint32_t>,
           boost::optional<uint32_t>>> missing_digest;

  map<pg_shard_t, ScrubMap *> maps;
  maps[pg_whoami] = &scrubber.primary_scrubmap;

  for (const auto& i : acting_recovery_backfill) {
    if (i == pg_whoami) continue;
    dout(2) << __func__ << " replica " << i << " has "
            << scrubber.received_maps[i].objects.size()
            << " items" << dendl;
    maps[i] = &scrubber.received_maps[i];
  }

  set<hobject_t> master_set;

  // Construct master set
  for (const auto map : maps) {
    for (const auto i : map.second->objects) {
      master_set.insert(i.first);
    }
  }

  stringstream ss;
  get_pgbackend()->be_omap_checks(maps, master_set,
                                  scrubber.omap_stats, ss);

  if (!ss.str().empty()) {
    osd->clog->warn(ss);
  }

  if (acting.size() > 1) {
    dout(10) << __func__ << "  comparing replica scrub maps" << dendl;

    // Map from object with errors to good peer
    map<hobject_t, list<pg_shard_t>> authoritative;

    dout(2) << __func__ << "   osd." << acting[0] << " has "
	    << scrubber.primary_scrubmap.objects.size() << " items" << dendl;

    ss.str("");
    ss.clear();

    get_pgbackend()->be_compare_scrubmaps(
      maps,
      master_set,
      state_test(PG_STATE_REPAIR),
      scrubber.missing,
      scrubber.inconsistent,
      authoritative,
      missing_digest,
      scrubber.shallow_errors,
      scrubber.deep_errors,
      scrubber.store.get(),
      info.pgid, acting,
      ss);
    dout(2) << ss.str() << dendl;

    if (!ss.str().empty()) {
      osd->clog->error(ss);
    }

    for (map<hobject_t, list<pg_shard_t>>::iterator i = authoritative.begin();
	 i != authoritative.end();
	 ++i) {
      list<pair<ScrubMap::object, pg_shard_t> > good_peers;
      for (list<pg_shard_t>::const_iterator j = i->second.begin();
	   j != i->second.end();
	   ++j) {
	good_peers.emplace_back(maps[*j]->objects[i->first], *j);
      }
      scrubber.authoritative.emplace(i->first, good_peers);
    }

    for (map<hobject_t, list<pg_shard_t>>::iterator i = authoritative.begin();
	 i != authoritative.end();
	 ++i) {
      scrubber.cleaned_meta_map.objects.erase(i->first);
      scrubber.cleaned_meta_map.objects.insert(
	*(maps[i->second.back()]->objects.find(i->first))
	);
    }
  }

  ScrubMap for_meta_scrub;
  scrubber.clean_meta_map(for_meta_scrub);

  // ok, do the pg-type specific scrubbing
  scrub_snapshot_metadata(for_meta_scrub, missing_digest);
  // Called here on the primary can use an authoritative map if it isn't the primary
  _scan_snaps(for_meta_scrub);
  if (!scrubber.store->empty()) {
    if (state_test(PG_STATE_REPAIR)) {
      dout(10) << __func__ << ": discarding scrub results" << dendl;
      scrubber.store->flush(nullptr);
    } else {
      dout(10) << __func__ << ": updating scrub object" << dendl;
      ObjectStore::Transaction t;
      scrubber.store->flush(&t);
      osd->store->queue_transaction(ch, std::move(t), nullptr);
    }
  }
}

bool PG::scrub_process_inconsistent()
{
  dout(10) << __func__ << ": checking authoritative" << dendl;
  bool repair = state_test(PG_STATE_REPAIR);
  bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
  const char *mode = (repair ? "repair": (deep_scrub ? "deep-scrub" : "scrub"));
  
  // authoriative only store objects which missing or inconsistent.
  if (!scrubber.authoritative.empty()) {
    stringstream ss;
    ss << info.pgid << " " << mode << " "
       << scrubber.missing.size() << " missing, "
       << scrubber.inconsistent.size() << " inconsistent objects";
    dout(2) << ss.str() << dendl;
    osd->clog->error(ss);
    if (repair) {
      state_clear(PG_STATE_CLEAN);
      for (map<hobject_t, list<pair<ScrubMap::object, pg_shard_t> >>::iterator i =
	     scrubber.authoritative.begin();
	   i != scrubber.authoritative.end();
	   ++i) {
	set<pg_shard_t>::iterator j;

	auto missing_entry = scrubber.missing.find(i->first);
	if (missing_entry != scrubber.missing.end()) {
	  for (j = missing_entry->second.begin();
	       j != missing_entry->second.end();
	       ++j) {
	    repair_object(
	      i->first,
	      &(i->second),
	      *j);
	    ++scrubber.fixed;
	  }
	}
	if (scrubber.inconsistent.count(i->first)) {
	  for (j = scrubber.inconsistent[i->first].begin(); 
	       j != scrubber.inconsistent[i->first].end(); 
	       ++j) {
	    repair_object(i->first, 
	      &(i->second),
	      *j);
	    ++scrubber.fixed;
	  }
	}
      }
    }
  }
  return (!scrubber.authoritative.empty() && repair);
}

bool PG::ops_blocked_by_scrub() const {
  return (waiting_for_scrub.size() != 0);
}

// the part that actually finalizes a scrub
void PG::scrub_finish() 
{
  dout(20) << __func__ << dendl;
  bool repair = state_test(PG_STATE_REPAIR);
  bool do_deep_scrub = false;
  // if the repair request comes from auto-repair and large number of errors,
  // we would like to cancel auto-repair
  if (repair && scrubber.auto_repair
      && scrubber.authoritative.size() > cct->_conf->osd_scrub_auto_repair_num_errors) {
    state_clear(PG_STATE_REPAIR);
    repair = false;
  }
  bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
  const char *mode = (repair ? "repair": (deep_scrub ? "deep-scrub" : "scrub"));

  // if a regular scrub had errors within the limit, do a deep scrub to auto repair.
  if (scrubber.deep_scrub_on_error
      && scrubber.authoritative.size() <= cct->_conf->osd_scrub_auto_repair_num_errors) {
    ceph_assert(!deep_scrub);
    scrubber.deep_scrub_on_error = false;
    do_deep_scrub = true;
    dout(20) << __func__ << " Try to auto repair after scrub errors" << dendl;
  }

  // type-specific finish (can tally more errors)
  _scrub_finish();

  bool has_error = scrub_process_inconsistent();

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
          << " remaining deep scrub error details lost)";
    if (repair)
      oss << ", " << scrubber.fixed << " fixed";
    if (total_errors)
      osd->clog->error(oss);
    else
      osd->clog->debug(oss);
  }

  // finish up
  unreg_next_scrub();
  utime_t now = ceph_clock_now();
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
      ceph_assert(deep_scrub);
      scrubber.shallow_errors = scrubber.deep_errors = 0;
      dout(20) << __func__ << " All may be fixed" << dendl;
    } else if (has_error) {
      // Deep scrub in order to get corrected error counts
      scrub_after_recovery = true;
      dout(20) << __func__ << " Set scrub_after_recovery" << dendl;
    } else if (scrubber.shallow_errors || scrubber.deep_errors) {
      // We have errors but nothing can be fixed, so there is no repair
      // possible.
      state_set(PG_STATE_FAILED_REPAIR);
      dout(10) << __func__ << " " << (scrubber.shallow_errors + scrubber.deep_errors)
	       << " error(s) present with no repair possible" << dendl;
    }
  }
  if (deep_scrub) {
    if ((scrubber.shallow_errors == 0) && (scrubber.deep_errors == 0))
      info.history.last_clean_scrub_stamp = now;
    info.stats.stats.sum.num_shallow_scrub_errors = scrubber.shallow_errors;
    info.stats.stats.sum.num_deep_scrub_errors = scrubber.deep_errors;
    info.stats.stats.sum.num_large_omap_objects = scrubber.omap_stats.large_omap_objects;
    info.stats.stats.sum.num_omap_bytes = scrubber.omap_stats.omap_bytes;
    info.stats.stats.sum.num_omap_keys = scrubber.omap_stats.omap_keys;
    dout(25) << __func__ << " shard " << pg_whoami << " num_omap_bytes = "
             << info.stats.stats.sum.num_omap_bytes << " num_omap_keys = "
             << info.stats.stats.sum.num_omap_keys << dendl;
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
  if (scrubber.check_repair) {
    scrubber.check_repair = false;
    if (info.stats.stats.sum.num_scrub_errors) {
      state_set(PG_STATE_FAILED_REPAIR);
      dout(10) << __func__ << " " << info.stats.stats.sum.num_scrub_errors
	       << " error(s) still present after re-scrub" << dendl;
    }
  }
  publish_stats_to_osd();
  if (do_deep_scrub) {
    // XXX: Auto scrub won't activate if must_scrub is set, but
    // setting the scrub stamps affects what users see.
    utime_t stamp = utime_t(0,1);
    set_last_scrub_stamp(stamp);
    set_last_deep_scrub_stamp(stamp);
  }
  reg_next_scrub();

  {
    ObjectStore::Transaction t;
    dirty_info = true;
    write_if_dirty(t);
    int tr = osd->store->queue_transaction(ch, std::move(t), NULL);
    ceph_assert(tr == 0);
  }


  if (has_error) {
    queue_peering_event(
      PGPeeringEventRef(
	std::make_shared<PGPeeringEvent>(
	  get_osdmap_epoch(),
	  get_osdmap_epoch(),
	  DoRecovery())));
  }

  scrub_clear_state(has_error);
  scrub_unreserve_replicas();

  if (is_active() && is_primary()) {
    share_pg_info();
  }
}

void PG::share_pg_info()
{
  dout(10) << "share_pg_info" << dendl;

  // share new pg_info_t with replicas
  ceph_assert(!acting_recovery_backfill.empty());
  for (set<pg_shard_t>::iterator i = acting_recovery_backfill.begin();
       i != acting_recovery_backfill.end();
       ++i) {
    if (*i == pg_whoami) continue;
    auto pg_shard = *i;
    auto peer = peer_info.find(pg_shard);
    if (peer != peer_info.end()) {
      peer->second.last_epoch_started = info.last_epoch_started;
      peer->second.last_interval_started = info.last_interval_started;
      peer->second.history.merge(info.history);
    }
    MOSDPGInfo *m = new MOSDPGInfo(get_osdmap_epoch());
    m->pg_list.emplace_back(
	pg_notify_t(
	  pg_shard.shard, pg_whoami.shard,
	  get_osdmap_epoch(),
	  get_osdmap_epoch(),
	  info),
	past_intervals);
    osd->send_message_osd_cluster(pg_shard.osd, m, get_osdmap_epoch());
  }
}

bool PG::append_log_entries_update_missing(
  const mempool::osd_pglog::list<pg_log_entry_t> &entries,
  ObjectStore::Transaction &t, boost::optional<eversion_t> trim_to,
  boost::optional<eversion_t> roll_forward_to)
{
  ceph_assert(!entries.empty());
  ceph_assert(entries.begin()->version > info.last_update);

  PGLogEntryHandler rollbacker{this, &t};
  bool invalidate_stats =
    pg_log.append_new_log_entries(info.last_backfill,
				  info.last_backfill_bitwise,
				  entries,
				  &rollbacker);

  if (roll_forward_to && entries.rbegin()->soid > info.last_backfill) {
    pg_log.roll_forward(&rollbacker);
  }
  if (roll_forward_to && *roll_forward_to > pg_log.get_can_rollback_to()) {
    pg_log.roll_forward_to(*roll_forward_to, &rollbacker);
    last_rollback_info_trimmed_to_applied = *roll_forward_to;
  }

  info.last_update = pg_log.get_head();

  if (pg_log.get_missing().num_missing() == 0) {
    // advance last_complete since nothing else is missing!
    info.last_complete = info.last_update;
  }
  info.stats.stats_invalid = info.stats.stats_invalid || invalidate_stats;

  dout(20) << __func__ << " trim_to bool = " << bool(trim_to) << " trim_to = " << (trim_to ? *trim_to : eversion_t()) << dendl;
  if (trim_to)
    pg_log.trim(*trim_to, info);
  dirty_info = true;
  write_if_dirty(t);
  return invalidate_stats;
}


void PG::merge_new_log_entries(
  const mempool::osd_pglog::list<pg_log_entry_t> &entries,
  ObjectStore::Transaction &t,
  boost::optional<eversion_t> trim_to,
  boost::optional<eversion_t> roll_forward_to)
{
  dout(10) << __func__ << " " << entries << dendl;
  ceph_assert(is_primary());

  bool rebuild_missing = append_log_entries_update_missing(entries, t, trim_to, roll_forward_to);
  for (set<pg_shard_t>::const_iterator i = acting_recovery_backfill.begin();
       i != acting_recovery_backfill.end();
       ++i) {
    pg_shard_t peer(*i);
    if (peer == pg_whoami) continue;
    ceph_assert(peer_missing.count(peer));
    ceph_assert(peer_info.count(peer));
    pg_missing_t& pmissing(peer_missing[peer]);
    dout(20) << __func__ << " peer_missing for " << peer << " = " << pmissing << dendl;
    pg_info_t& pinfo(peer_info[peer]);
    bool invalidate_stats = PGLog::append_log_entries_update_missing(
      pinfo.last_backfill,
      info.last_backfill_bitwise,
      entries,
      true,
      NULL,
      pmissing,
      NULL,
      this);
    pinfo.last_update = info.last_update;
    pinfo.stats.stats_invalid = pinfo.stats.stats_invalid || invalidate_stats;
    rebuild_missing = rebuild_missing || invalidate_stats;
  }

  if (!rebuild_missing) {
    return;
  }

  for (auto &&i: entries) {
    missing_loc.rebuild(
      i.soid,
      pg_whoami,
      acting_recovery_backfill,
      info,
      pg_log.get_missing(),
      peer_missing,
      peer_info);
  }
}

void PG::update_history(const pg_history_t& new_history)
{
  unreg_next_scrub();
  if (info.history.merge(new_history)) {
    dout(20) << __func__ << " advanced history from " << new_history << dendl;
    dirty_info = true;
    if (info.history.last_epoch_clean >= info.history.same_interval_since) {
      dout(20) << __func__ << " clearing past_intervals" << dendl;
      past_intervals.clear();
      dirty_big_info = true;
    }
  }
  reg_next_scrub();
}

void PG::fulfill_info(
  pg_shard_t from, const pg_query_t &query,
  pair<pg_shard_t, pg_info_t> &notify_info)
{
  ceph_assert(from == primary);
  ceph_assert(query.type == pg_query_t::INFO);

  // info
  dout(10) << "sending info" << dendl;
  notify_info = make_pair(from, info);
}

void PG::fulfill_log(
  pg_shard_t from, const pg_query_t &query, epoch_t query_epoch)
{
  dout(10) << "log request from " << from << dendl;
  ceph_assert(from == primary);
  ceph_assert(query.type != pg_query_t::INFO);
  ConnectionRef con = osd->get_con_osd_cluster(
    from.osd, get_osdmap_epoch());
  if (!con) return;

  MOSDPGLog *mlog = new MOSDPGLog(
    from.shard, pg_whoami.shard,
    get_osdmap_epoch(),
    info, query_epoch);
  mlog->missing = pg_log.get_missing();

  // primary -> other, when building master log
  if (query.type == pg_query_t::LOG) {
    dout(10) << " sending info+missing+log since " << query.since
	     << dendl;
    if (query.since != eversion_t() && query.since < pg_log.get_tail()) {
      osd->clog->error() << info.pgid << " got broken pg_query_t::LOG since " << query.since
			<< " when my log.tail is " << pg_log.get_tail()
			<< ", sending full log instead";
      mlog->log = pg_log.get_log();           // primary should not have requested this!!
    } else
      mlog->log.copy_after(pg_log.get_log(), query.since);
  }
  else if (query.type == pg_query_t::FULLLOG) {
    dout(10) << " sending info+missing+full log" << dendl;
    mlog->log = pg_log.get_log();
  }

  dout(10) << " sending " << mlog->log << " " << mlog->missing << dendl;

  osd->share_map_peer(from.osd, con.get(), get_osdmap());
  osd->send_message_osd_cluster(mlog, con.get());
}

void PG::fulfill_query(const MQuery& query, RecoveryCtx *rctx)
{
  if (query.query.type == pg_query_t::INFO) {
    pair<pg_shard_t, pg_info_t> notify_info;
    update_history(query.query.history);
    fulfill_info(query.from, query.query, notify_info);
    rctx->send_notify(
      notify_info.first,
      pg_notify_t(
	notify_info.first.shard, pg_whoami.shard,
	query.query_epoch,
	get_osdmap_epoch(),
	notify_info.second),
      past_intervals);
  } else {
    update_history(query.query.history);
    fulfill_log(query.from, query.query, query.query_epoch);
  }
}

void PG::check_full_transition(OSDMapRef lastmap, OSDMapRef osdmap)
{
  bool changed = false;
  if (osdmap->test_flag(CEPH_OSDMAP_FULL) &&
      !lastmap->test_flag(CEPH_OSDMAP_FULL)) {
    dout(10) << " cluster was marked full in " << osdmap->get_epoch() << dendl;
    changed = true;
  }
  const pg_pool_t *pi = osdmap->get_pg_pool(info.pgid.pool());
  if (!pi) {
    return; // pool deleted
  }
  if (pi->has_flag(pg_pool_t::FLAG_FULL)) {
    const pg_pool_t *opi = lastmap->get_pg_pool(info.pgid.pool());
    if (!opi || !opi->has_flag(pg_pool_t::FLAG_FULL)) {
      dout(10) << " pool was marked full in " << osdmap->get_epoch() << dendl;
      changed = true;
    }
  }
  if (changed) {
    info.history.last_epoch_marked_full = osdmap->get_epoch();
    dirty_info = true;
  }
}

bool PG::should_restart_peering(
  int newupprimary,
  int newactingprimary,
  const vector<int>& newup,
  const vector<int>& newacting,
  OSDMapRef lastmap,
  OSDMapRef osdmap)
{
  if (PastIntervals::is_new_interval(
	primary.osd,
	newactingprimary,
	acting,
	newacting,
	up_primary.osd,
	newupprimary,
	up,
	newup,
	osdmap,
	lastmap,
	info.pgid.pgid)) {
    dout(20) << "new interval newup " << newup
	     << " newacting " << newacting << dendl;
    return true;
  }
  if (!lastmap->is_up(osd->whoami) && osdmap->is_up(osd->whoami)) {
    dout(10) << __func__ << " osd transitioned from down -> up" << dendl;
    return true;
  }
  return false;
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
  dout(20) << "set_last_peering_reset " << get_osdmap_epoch() << dendl;
  if (last_peering_reset != get_osdmap_epoch()) {
    last_peering_reset = get_osdmap_epoch();
    reset_interval_flush();
  }
}

struct FlushState {
  PGRef pg;
  epoch_t epoch;
  FlushState(PG *pg, epoch_t epoch) : pg(pg), epoch(epoch) {}
  ~FlushState() {
    pg->lock();
    if (!pg->pg_has_reset_since(epoch))
      pg->on_flushed();
    pg->unlock();
  }
};
typedef std::shared_ptr<FlushState> FlushStateRef;

void PG::start_flush(ObjectStore::Transaction *t)
{
  // flush in progress ops
  FlushStateRef flush_trigger (std::make_shared<FlushState>(
                               this, get_osdmap_epoch()));
  flushes_in_progress++;
  t->register_on_applied(new ContainerContext<FlushStateRef>(flush_trigger));
  t->register_on_commit(new ContainerContext<FlushStateRef>(flush_trigger));
}

void PG::reset_interval_flush()
{
  dout(10) << "Clearing blocked outgoing recovery messages" << dendl;
  recovery_state.clear_blocked_outgoing();
  
  Context *c = new QueuePeeringEvt<IntervalFlush>(
    this, get_osdmap_epoch(), IntervalFlush());
  if (!ch->flush_commit(c)) {
    dout(10) << "Beginning to block outgoing recovery messages" << dendl;
    recovery_state.begin_block_outgoing();
  } else {
    dout(10) << "Not blocking outgoing recovery messages" << dendl;
    delete c;
  }
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

  if (is_primary()) {
    osd->clear_ready_to_merge(this);
  }

  pg_shard_t old_acting_primary = get_primary();
  pg_shard_t old_up_primary = up_primary;
  bool was_old_primary = is_primary();
  bool was_old_replica = is_replica();

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
    info.stats.mapping_epoch = osdmap->get_epoch();
  }

  pg_stats_publish_lock.Lock();
  pg_stats_publish_valid = false;
  pg_stats_publish_lock.Unlock();

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

  // did acting, up, primary|acker change?
  if (!lastmap) {
    dout(10) << " no lastmap" << dendl;
    dirty_info = true;
    dirty_big_info = true;
    info.history.same_interval_since = osdmap->get_epoch();
  } else {
    std::stringstream debug;
    ceph_assert(info.history.same_interval_since != 0);
    boost::scoped_ptr<IsPGRecoverablePredicate> recoverable(
      get_is_recoverable_predicate());
    bool new_interval = PastIntervals::check_new_interval(
      old_acting_primary.osd,
      new_acting_primary,
      oldacting, newacting,
      old_up_primary.osd,
      new_up_primary,
      oldup, newup,
      info.history.same_interval_since,
      info.history.last_epoch_clean,
      osdmap,
      lastmap,
      info.pgid.pgid,
      recoverable.get(),
      &past_intervals,
      &debug);
    dout(10) << __func__ << ": check_new_interval output: "
	     << debug.str() << dendl;
    if (new_interval) {
      if (osdmap->get_epoch() == osd->get_superblock().oldest_map &&
	  info.history.last_epoch_clean < osdmap->get_epoch()) {
	dout(10) << " map gap, clearing past_intervals and faking" << dendl;
	// our information is incomplete and useless; someone else was clean
	// after everything we know if osdmaps were trimmed.
	past_intervals.clear();
      } else {
	dout(10) << " noting past " << past_intervals << dendl;
      }
      dirty_info = true;
      dirty_big_info = true;
      info.history.same_interval_since = osdmap->get_epoch();
      if (osdmap->have_pg_pool(info.pgid.pgid.pool()) &&
	  info.pgid.pgid.is_split(lastmap->get_pg_num(info.pgid.pgid.pool()),
				  osdmap->get_pg_num(info.pgid.pgid.pool()),
				  nullptr)) {
	info.history.last_epoch_split = osdmap->get_epoch();
      }
    }
  }

  if (old_up_primary != up_primary ||
      oldup != up) {
    info.history.same_up_since = osdmap->get_epoch();
  }
  // this comparison includes primary rank via pg_shard_t
  if (old_acting_primary != get_primary()) {
    info.history.same_primary_since = osdmap->get_epoch();
  }

  on_new_interval();

  dout(1) << __func__ << " up " << oldup << " -> " << up
	   << ", acting " << oldacting << " -> " << acting 
	   << ", acting_primary " << old_acting_primary << " -> " << new_acting_primary
	   << ", up_primary " << old_up_primary << " -> " << new_up_primary
	   << ", role " << oldrole << " -> " << role
	   << ", features acting " << acting_features
	   << " upacting " << upacting_features
	   << dendl;

  // deactivate.
  state_clear(PG_STATE_ACTIVE);
  state_clear(PG_STATE_PEERED);
  state_clear(PG_STATE_PREMERGE);
  state_clear(PG_STATE_DOWN);
  state_clear(PG_STATE_RECOVERY_WAIT);
  state_clear(PG_STATE_RECOVERY_TOOFULL);
  state_clear(PG_STATE_RECOVERING);

  peer_purged.clear();
  acting_recovery_backfill.clear();
  scrub_queued = false;

  // reset primary/replica state?
  if (was_old_primary || is_primary()) {
    osd->remove_want_pg_temp(info.pgid.pgid);
  } else if (was_old_replica || is_replica()) {
    osd->remove_want_pg_temp(info.pgid.pgid);
  }
  clear_primary_state();

    
  // pg->on_*
  on_change(t);

  projected_last_update = eversion_t();

  ceph_assert(!deleting);

  // should we tell the primary we are here?
  send_notify = !is_primary();

  if (role != oldrole ||
      was_old_primary != is_primary()) {
    // did primary change?
    if (was_old_primary != is_primary()) {
      state_clear(PG_STATE_CLEAN);
      clear_publish_stats();
    }

    on_role_change();

    // take active waiters
    requeue_ops(waiting_for_peered);

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

void PG::on_new_interval()
{
  const OSDMapRef osdmap = get_osdmap();

  reg_next_scrub();

  // initialize features
  acting_features = CEPH_FEATURES_SUPPORTED_DEFAULT;
  upacting_features = CEPH_FEATURES_SUPPORTED_DEFAULT;
  for (vector<int>::iterator p = acting.begin(); p != acting.end(); ++p) {
    if (*p == CRUSH_ITEM_NONE)
      continue;
    uint64_t f = osdmap->get_xinfo(*p).features;
    acting_features &= f;
    upacting_features &= f;
  }
  for (vector<int>::iterator p = up.begin(); p != up.end(); ++p) {
    if (*p == CRUSH_ITEM_NONE)
      continue;
    upacting_features &= osdmap->get_xinfo(*p).features;
  }

  _on_new_interval();
}

void PG::proc_primary_info(ObjectStore::Transaction &t, const pg_info_t &oinfo)
{
  ceph_assert(!is_primary());

  update_history(oinfo.history);
  if (!info.stats.stats_invalid && info.stats.stats.sum.num_scrub_errors) {
    info.stats.stats.sum.num_scrub_errors = 0;
    info.stats.stats.sum.num_shallow_scrub_errors = 0;
    info.stats.stats.sum.num_deep_scrub_errors = 0;
    dirty_info = true;
  }

  if (!(info.purged_snaps == oinfo.purged_snaps)) {
    dout(10) << __func__ << " updating purged_snaps to " << oinfo.purged_snaps
	     << dendl;
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
  if (pg.is_ec_pg())
    out << "p" << pg.get_primary();
  if (!pg.async_recovery_targets.empty())
    out << " async=[" << pg.async_recovery_targets << "]";
  if (!pg.backfill_targets.empty())
    out << " backfill=[" << pg.backfill_targets << "]";
  out << " r=" << pg.get_role();
  out << " lpr=" << pg.get_last_peering_reset();

  if (pg.deleting)
    out << " DELETING";

  if (!pg.past_intervals.empty()) {
    out << " pi=[" << pg.past_intervals.get_bounds()
	<< ")/" << pg.past_intervals.size();
  }

  if (pg.is_peered()) {
    if (pg.last_update_ondisk != pg.info.last_update)
      out << " luod=" << pg.last_update_ondisk;
    if (pg.last_update_applied != pg.info.last_update)
      out << " lua=" << pg.last_update_applied;
  }

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

  out << " crt=" << pg.pg_log.get_can_rollback_to();

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
  if (pg.scrubber.auto_repair)
    out << " AUTO_REPAIR";
  if (pg.scrubber.check_repair)
    out << " CHECK_REPAIR";
  if (pg.scrubber.deep_scrub_on_error)
    out << " DEEP_SCRUB_ON_ERROR";
  if (pg.scrubber.must_deep_scrub)
    out << " MUST_DEEP_SCRUB";
  if (pg.scrubber.must_scrub)
    out << " MUST_SCRUB";

  //out << " (" << pg.pg_log.get_tail() << "," << pg.pg_log.get_head() << "]";
  if (pg.pg_log.get_missing().num_missing()) {
    out << " m=" << pg.pg_log.get_missing().num_missing();
    if (pg.is_primary()) {
      uint64_t unfound = pg.get_num_unfound();
      if (unfound)
	out << " u=" << unfound;
    }
  }
  if (!pg.is_clean()) {
    out << " mbc=" << pg.missing_loc.get_missing_by_count();
  }
  if (!pg.snap_trimq.empty()) {
    out << " trimq=";
    // only show a count if the set is large
    if (pg.snap_trimq.num_intervals() > 16) {
      out << pg.snap_trimq.size();
    } else {
      out << pg.snap_trimq;
    }
  }
  if (!pg.info.purged_snaps.empty()) {
    out << " ps="; // snap trim queue / purged snaps
    if (pg.info.purged_snaps.num_intervals() > 16) {
      out << pg.info.purged_snaps.size();
    } else {
      out << pg.info.purged_snaps;
    }
  }

  out << "]";


  return out;
}

bool PG::can_discard_op(OpRequestRef& op)
{
  const MOSDOp *m = static_cast<const MOSDOp*>(op->get_req());
  if (cct->_conf->osd_discard_disconnected_ops && OSD::op_is_discardable(m)) {
    dout(20) << " discard " << *m << dendl;
    return true;
  }

  if (m->get_map_epoch() < info.history.same_primary_since) {
    dout(7) << " changed after " << m->get_map_epoch()
	    << ", dropping " << *m << dendl;
    return true;
  }

  if (m->get_connection()->has_feature(CEPH_FEATURE_RESEND_ON_SPLIT)) {
    // >= luminous client
    if (m->get_connection()->has_feature(CEPH_FEATURE_SERVER_NAUTILUS)) {
      // >= nautilus client
      if (m->get_map_epoch() < pool.info.get_last_force_op_resend()) {
	dout(7) << __func__ << " sent before last_force_op_resend "
		<< pool.info.last_force_op_resend
		<< ", dropping" << *m << dendl;
	return true;
      }
    } else {
      // == < nautilus client (luminous or mimic)
      if (m->get_map_epoch() < pool.info.get_last_force_op_resend_prenautilus()) {
	dout(7) << __func__ << " sent before last_force_op_resend_prenautilus "
		<< pool.info.last_force_op_resend_prenautilus
		<< ", dropping" << *m << dendl;
	return true;
      }
    }
    if (m->get_map_epoch() < info.history.last_epoch_split) {
      dout(7) << __func__ << " pg split in "
	      << info.history.last_epoch_split << ", dropping" << dendl;
      return true;
    }
  } else if (m->get_connection()->has_feature(CEPH_FEATURE_OSD_POOLRESEND)) {
    // < luminous client
    if (m->get_map_epoch() < pool.info.get_last_force_op_resend_preluminous()) {
      dout(7) << __func__ << " sent before last_force_op_resend_preluminous "
	      << pool.info.last_force_op_resend_preluminous
	      << ", dropping" << *m << dendl;
      return true;
    }
  }

  return false;
}

template<typename T, int MSGTYPE>
bool PG::can_discard_replica_op(OpRequestRef& op)
{
  const T *m = static_cast<const T *>(op->get_req());
  ceph_assert(m->get_type() == MSGTYPE);

  int from = m->get_source().num();

  // if a repop is replied after a replica goes down in a new osdmap, and
  // before the pg advances to this new osdmap, the repop replies before this
  // repop can be discarded by that replica OSD, because the primary resets the
  // connection to it when handling the new osdmap marking it down, and also
  // resets the messenger sesssion when the replica reconnects. to avoid the
  // out-of-order replies, the messages from that replica should be discarded.
  OSDMapRef next_map = osd->get_next_osdmap();
  if (next_map->is_down(from))
    return true;
  /* Mostly, this overlaps with the old_peering_msg
   * condition.  An important exception is pushes
   * sent by replicas not in the acting set, since
   * if such a replica goes down it does not cause
   * a new interval. */
  if (next_map->get_down_at(from) >= m->map_epoch)
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
  const MOSDPGScan *m = static_cast<const MOSDPGScan *>(op->get_req());
  ceph_assert(m->get_type() == MSG_OSD_PG_SCAN);

  if (old_peering_msg(m->map_epoch, m->query_epoch)) {
    dout(10) << " got old scan, ignoring" << dendl;
    return true;
  }
  return false;
}

bool PG::can_discard_backfill(OpRequestRef op)
{
  const MOSDPGBackfill *m = static_cast<const MOSDPGBackfill *>(op->get_req());
  ceph_assert(m->get_type() == MSG_OSD_PG_BACKFILL);

  if (old_peering_msg(m->map_epoch, m->query_epoch)) {
    dout(10) << " got old backfill, ignoring" << dendl;
    return true;
  }

  return false;

}

bool PG::can_discard_request(OpRequestRef& op)
{
  switch (op->get_req()->get_type()) {
  case CEPH_MSG_OSD_OP:
    return can_discard_op(op);
  case CEPH_MSG_OSD_BACKOFF:
    return false; // never discard
  case MSG_OSD_REPOP:
    return can_discard_replica_op<MOSDRepOp, MSG_OSD_REPOP>(op);
  case MSG_OSD_PG_PUSH:
    return can_discard_replica_op<MOSDPGPush, MSG_OSD_PG_PUSH>(op);
  case MSG_OSD_PG_PULL:
    return can_discard_replica_op<MOSDPGPull, MSG_OSD_PG_PULL>(op);
  case MSG_OSD_PG_PUSH_REPLY:
    return can_discard_replica_op<MOSDPGPushReply, MSG_OSD_PG_PUSH_REPLY>(op);
  case MSG_OSD_REPOPREPLY:
    return can_discard_replica_op<MOSDRepOpReply, MSG_OSD_REPOPREPLY>(op);
  case MSG_OSD_PG_RECOVERY_DELETE:
    return can_discard_replica_op<MOSDPGRecoveryDelete, MSG_OSD_PG_RECOVERY_DELETE>(op);

  case MSG_OSD_PG_RECOVERY_DELETE_REPLY:
    return can_discard_replica_op<MOSDPGRecoveryDeleteReply, MSG_OSD_PG_RECOVERY_DELETE_REPLY>(op);

  case MSG_OSD_EC_WRITE:
    return can_discard_replica_op<MOSDECSubOpWrite, MSG_OSD_EC_WRITE>(op);
  case MSG_OSD_EC_WRITE_REPLY:
    return can_discard_replica_op<MOSDECSubOpWriteReply, MSG_OSD_EC_WRITE_REPLY>(op);
  case MSG_OSD_EC_READ:
    return can_discard_replica_op<MOSDECSubOpRead, MSG_OSD_EC_READ>(op);
  case MSG_OSD_EC_READ_REPLY:
    return can_discard_replica_op<MOSDECSubOpReadReply, MSG_OSD_EC_READ_REPLY>(op);
  case MSG_OSD_REP_SCRUB:
    return can_discard_replica_op<MOSDRepScrub, MSG_OSD_REP_SCRUB>(op);
  case MSG_OSD_SCRUB_RESERVE:
    return can_discard_replica_op<MOSDScrubReserve, MSG_OSD_SCRUB_RESERVE>(op);
  case MSG_OSD_REP_SCRUBMAP:
    return can_discard_replica_op<MOSDRepScrubMap, MSG_OSD_REP_SCRUBMAP>(op);
  case MSG_OSD_PG_UPDATE_LOG_MISSING:
    return can_discard_replica_op<
      MOSDPGUpdateLogMissing, MSG_OSD_PG_UPDATE_LOG_MISSING>(op);
  case MSG_OSD_PG_UPDATE_LOG_MISSING_REPLY:
    return can_discard_replica_op<
      MOSDPGUpdateLogMissingReply, MSG_OSD_PG_UPDATE_LOG_MISSING_REPLY>(op);

  case MSG_OSD_PG_SCAN:
    return can_discard_scan(op);
  case MSG_OSD_PG_BACKFILL:
    return can_discard_backfill(op);
  case MSG_OSD_PG_BACKFILL_REMOVE:
    return can_discard_replica_op<MOSDPGBackfillRemove,
				  MSG_OSD_PG_BACKFILL_REMOVE>(op);
  }
  return true;
}

void PG::take_waiters()
{
  dout(10) << "take_waiters" << dendl;
  requeue_map_waiters();
}

void PG::do_peering_event(PGPeeringEventRef evt, RecoveryCtx *rctx)
{
  dout(10) << __func__ << ": " << evt->get_desc() << dendl;
  ceph_assert(have_same_or_newer_map(evt->get_epoch_sent()));
  if (old_peering_evt(evt)) {
    dout(10) << "discard old " << evt->get_desc() << dendl;
  } else {
    recovery_state.handle_event(evt, rctx);
  }
  // write_if_dirty regardless of path above to ensure we capture any work
  // done by OSD::advance_pg().
  write_if_dirty(*rctx->transaction);
}

void PG::queue_peering_event(PGPeeringEventRef evt)
{
  if (old_peering_evt(evt))
    return;
  osd->osd->enqueue_peering_evt(info.pgid, evt);
}

void PG::queue_null(epoch_t msg_epoch,
		    epoch_t query_epoch)
{
  dout(10) << "null" << dendl;
  queue_peering_event(
    PGPeeringEventRef(std::make_shared<PGPeeringEvent>(msg_epoch, query_epoch,
					 NullEvt())));
}

void PG::find_unfound(epoch_t queued, RecoveryCtx *rctx)
{
  /*
    * if we couldn't start any recovery ops and things are still
    * unfound, see if we can discover more missing object locations.
    * It may be that our initial locations were bad and we errored
    * out while trying to pull.
    */
  discover_all_missing(*rctx->query_map);
  if (rctx->query_map->empty()) {
    string action;
    if (state_test(PG_STATE_BACKFILLING)) {
      auto evt = PGPeeringEventRef(
	new PGPeeringEvent(
	  queued,
	  queued,
	  PG::UnfoundBackfill()));
      queue_peering_event(evt);
      action = "in backfill";
    } else if (state_test(PG_STATE_RECOVERING)) {
      auto evt = PGPeeringEventRef(
	new PGPeeringEvent(
	  queued,
	  queued,
	  PG::UnfoundRecovery()));
      queue_peering_event(evt);
      action = "in recovery";
    } else {
      action = "already out of recovery/backfill";
    }
    dout(10) << __func__ << ": no luck, giving up on this pg for now (" << action << ")" << dendl;
  } else {
    dout(10) << __func__ << ": no luck, giving up on this pg for now (queue_recovery)" << dendl;
    queue_recovery();
  }
}

void PG::handle_advance_map(
  OSDMapRef osdmap, OSDMapRef lastmap,
  vector<int>& newup, int up_primary,
  vector<int>& newacting, int acting_primary,
  RecoveryCtx *rctx)
{
  ceph_assert(lastmap->get_epoch() == osdmap_ref->get_epoch());
  ceph_assert(lastmap == osdmap_ref);
  dout(10) << "handle_advance_map "
	   << newup << "/" << newacting
	   << " -- " << up_primary << "/" << acting_primary
	   << dendl;
  update_osdmap_ref(osdmap);
  osd_shard->update_pg_epoch(pg_slot, osdmap->get_epoch());

  pool.update(cct, osdmap);

  AdvMap evt(
    osdmap, lastmap, newup, up_primary,
    newacting, acting_primary);
  recovery_state.handle_event(evt, rctx);
  if (pool.info.last_change == osdmap_ref->get_epoch()) {
    on_pool_change();
    update_store_with_options();
  }
  last_require_osd_release = osdmap->require_osd_release;
}

void PG::handle_activate_map(RecoveryCtx *rctx)
{
  dout(10) << "handle_activate_map " << dendl;
  ActMap evt;
  recovery_state.handle_event(evt, rctx);
  if (osdmap_ref->get_epoch() - last_persisted_osdmap >
    cct->_conf->osd_pg_epoch_persisted_max_stale) {
    dout(20) << __func__ << ": Dirtying info: last_persisted is "
	     << last_persisted_osdmap
	     << " while current is " << osdmap_ref->get_epoch() << dendl;
    dirty_info = true;
  } else {
    dout(20) << __func__ << ": Not dirtying info: last_persisted is "
	     << last_persisted_osdmap
	     << " while current is " << osdmap_ref->get_epoch() << dendl;
  }
  if (osdmap_ref->check_new_blacklist_entries()) {
    check_blacklisted_watchers();
  }
  write_if_dirty(*rctx->transaction);
}

void PG::handle_initialize(RecoveryCtx *rctx)
{
  dout(10) << __func__ << dendl;
  Initialize evt;
  recovery_state.handle_event(evt, rctx);
}

void PG::handle_query_state(Formatter *f)
{
  dout(10) << "handle_query_state" << dendl;
  QueryState q(f);
  recovery_state.handle_event(q, 0);
}

void PG::update_store_with_options()
{
  auto r = osd->store->set_collection_opts(ch, pool.info.opts);
  if(r < 0 && r != -EOPNOTSUPP) {
    derr << __func__ << " set_collection_opts returns error:" << r << dendl;
  }
}

struct C_DeleteMore : public Context {
  PGRef pg;
  epoch_t epoch;
  C_DeleteMore(PG *p, epoch_t e) : pg(p), epoch(e) {}
  void finish(int r) override {
    ceph_abort();
  }
  void complete(int r) override {
    ceph_assert(r == 0);
    pg->lock();
    if (!pg->pg_has_reset_since(epoch)) {
      pg->osd->queue_for_pg_delete(pg->get_pgid(), epoch);
    }
    pg->unlock();
    delete this;
  }
};

void PG::_delete_some(ObjectStore::Transaction *t)
{
  dout(10) << __func__ << dendl;

  {
    float osd_delete_sleep = osd->osd->get_osd_delete_sleep();
    if (osd_delete_sleep > 0 && delete_needs_sleep) {
      epoch_t e = get_osdmap()->get_epoch();
      PGRef pgref(this);
      auto delete_requeue_callback = new FunctionContext([this, pgref, e](int r) {
        dout(20) << __func__ << " wake up at "
                 << ceph_clock_now()
	         << ", re-queuing delete" << dendl;
        lock();
        delete_needs_sleep = false;
        if (!pg_has_reset_since(e)) {
          osd->queue_for_pg_delete(get_pgid(), e);
        }
        unlock();
      });

      utime_t delete_schedule_time = ceph_clock_now();
      delete_schedule_time += osd_delete_sleep;
      Mutex::Locker l(osd->sleep_lock);
      osd->sleep_timer.add_event_at(delete_schedule_time,
	                                        delete_requeue_callback);
      dout(20) << __func__ << " Delete scheduled at " << delete_schedule_time << dendl;
      return;
    }
  }

  delete_needs_sleep = true;

  vector<ghobject_t> olist;
  int max = std::min(osd->store->get_ideal_list_max(),
		     (int)cct->_conf->osd_target_transaction_size);
  ghobject_t next;
  osd->store->collection_list(
    ch,
    next,
    ghobject_t::get_max(),
    max,
    &olist,
    &next);
  dout(20) << __func__ << " " << olist << dendl;

  OSDriver::OSTransaction _t(osdriver.get_transaction(t));
  int64_t num = 0;
  for (auto& oid : olist) {
    if (oid.is_pgmeta()) {
      continue;
    }
    int r = snap_mapper.remove_oid(oid.hobj, &_t);
    if (r != 0 && r != -ENOENT) {
      ceph_abort();
    }
    t->remove(coll, oid);
    ++num;
  }
  if (num) {
    dout(20) << __func__ << " deleting " << num << " objects" << dendl;
    Context *fin = new C_DeleteMore(this, get_osdmap_epoch());
    t->register_on_commit(fin);
  } else {
    dout(20) << __func__ << " finished" << dendl;
    if (cct->_conf->osd_inject_failure_on_pg_removal) {
      _exit(1);
    }

    // final flush here to ensure completions drop refs.  Of particular concern
    // are the SnapMapper ContainerContexts.
    {
      PGRef pgref(this);
      PGLog::clear_info_log(info.pgid, t);
      t->remove_collection(coll);
      t->register_on_commit(new ContainerContext<PGRef>(pgref));
      t->register_on_applied(new ContainerContext<PGRef>(pgref));
      osd->store->queue_transaction(ch, std::move(*t));
    }
    ch->flush();

    if (!osd->try_finish_pg_delete(this, pool.info.get_pg_num())) {
      dout(1) << __func__ << " raced with merge, reinstantiating" << dendl;
      ch = osd->store->create_new_collection(coll);
      _create(*t,
	      info.pgid,
	      info.pgid.get_split_bits(pool.info.get_pg_num()));
      _init(*t, info.pgid, &pool.info);
      last_epoch = 0;  // to ensure pg epoch is also written
      dirty_info = true;
      dirty_big_info = true;
    } else {
      deleted = true;

      // cancel reserver here, since the PG is about to get deleted and the
      // exit() methods don't run when that happens.
      osd->local_reserver.cancel_reservation(info.pgid);

      osd->logger->dec(l_osd_pg_removing);
    }
  }
}

// Compute pending backfill data
static int64_t pending_backfill(CephContext *cct, int64_t bf_bytes, int64_t local_bytes)
{
    lgeneric_dout(cct, 20) << __func__ << " Adjust local usage " << (local_bytes >> 10) << "KiB"
		               << " primary usage " << (bf_bytes >> 10) << "KiB" << dendl;
    return std::max((int64_t)0, bf_bytes - local_bytes);
}

int PG::pg_stat_adjust(osd_stat_t *ns)
{
  osd_stat_t &new_stat = *ns;
  if (is_primary()) {
    return 0;
  }
  // Adjust the kb_used by adding pending backfill data
  uint64_t reserved_num_bytes = get_reserved_num_bytes();

  // For now we don't consider projected space gains here
  // I suggest we have an optional 2 pass backfill that frees up
  // space in a first pass.  This could be triggered when at nearfull
  // or near to backfillfull.
  if (reserved_num_bytes > 0) {
    // TODO: Handle compression by adjusting by the PGs average
    // compression precentage.
    dout(20) << __func__ << " reserved_num_bytes " << (reserved_num_bytes >> 10) << "KiB"
             << " Before kb_used " << new_stat.statfs.kb_used() << "KiB" << dendl;
    if (new_stat.statfs.available > reserved_num_bytes)
      new_stat.statfs.available -= reserved_num_bytes;
    else
      new_stat.statfs.available = 0;
    dout(20) << __func__ << " After kb_used " << new_stat.statfs.kb_used() << "KiB" << dendl;
    return 1;
  }
  return 0;
}


/*------------ Recovery State Machine----------------*/
#undef dout_prefix
#define dout_prefix (context< RecoveryMachine >().pg->gen_prefix(*_dout) \
		     << "state<" << get_state_name() << ">: ")

/*------Crashed-------*/
PG::RecoveryState::Crashed::Crashed(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Crashed")
{
  context< RecoveryMachine >().log_enter(state_name);
  ceph_abort_msg("we got a bad state machine event");
}


/*------Initial-------*/
PG::RecoveryState::Initial::Initial(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Initial")
{
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result PG::RecoveryState::Initial::react(const MNotifyRec& notify)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->proc_replica_info(
    notify.from, notify.notify.info, notify.notify.epoch_sent);
  pg->set_last_peering_reset();
  return transit< Primary >();
}

boost::statechart::result PG::RecoveryState::Initial::react(const MInfoRec& i)
{
  PG *pg = context< RecoveryMachine >().pg;
  ceph_assert(!pg->is_primary());
  post_event(i);
  return transit< Stray >();
}

boost::statechart::result PG::RecoveryState::Initial::react(const MLogRec& i)
{
  PG *pg = context< RecoveryMachine >().pg;
  ceph_assert(!pg->is_primary());
  post_event(i);
  return transit< Stray >();
}

void PG::RecoveryState::Initial::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_initial_latency, dur);
}

/*------Started-------*/
PG::RecoveryState::Started::Started(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started")
{
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result
PG::RecoveryState::Started::react(const IntervalFlush&)
{
  PG *pg = context< RecoveryMachine >().pg;
  ldout(pg->cct, 10) << "Ending blocked outgoing recovery messages" << dendl;
  context< RecoveryMachine >().pg->recovery_state.end_block_outgoing();
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Started::react(const AdvMap& advmap)
{
  PG *pg = context< RecoveryMachine >().pg;
  ldout(pg->cct, 10) << "Started advmap" << dendl;
  pg->check_full_transition(advmap.lastmap, advmap.osdmap);
  if (pg->should_restart_peering(
	advmap.up_primary,
	advmap.acting_primary,
	advmap.newup,
	advmap.newacting,
	advmap.lastmap,
	advmap.osdmap)) {
    ldout(pg->cct, 10) << "should_restart_peering, transitioning to Reset"
		       << dendl;
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
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_started_latency, dur);
}

/*--------Reset---------*/
PG::RecoveryState::Reset::Reset(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Reset")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;

  pg->flushes_in_progress = 0;
  pg->set_last_peering_reset();
}

boost::statechart::result
PG::RecoveryState::Reset::react(const IntervalFlush&)
{
  PG *pg = context< RecoveryMachine >().pg;
  ldout(pg->cct, 10) << "Ending blocked outgoing recovery messages" << dendl;
  context< RecoveryMachine >().pg->recovery_state.end_block_outgoing();
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Reset::react(const AdvMap& advmap)
{
  PG *pg = context< RecoveryMachine >().pg;
  ldout(pg->cct, 10) << "Reset advmap" << dendl;

  pg->check_full_transition(advmap.lastmap, advmap.osdmap);

  if (pg->should_restart_peering(
	advmap.up_primary,
	advmap.acting_primary,
	advmap.newup,
	advmap.newacting,
	advmap.lastmap,
	advmap.osdmap)) {
    ldout(pg->cct, 10) << "should restart peering, calling start_peering_interval again"
		       << dendl;
    pg->start_peering_interval(
      advmap.lastmap,
      advmap.newup, advmap.up_primary,
      advmap.newacting, advmap.acting_primary,
      context< RecoveryMachine >().get_cur_transaction());
  }
  pg->remove_down_peer_info(advmap.osdmap);
  pg->check_past_interval_bounds();
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
	pg->get_osdmap_epoch(),
	pg->get_osdmap_epoch(),
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
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_reset_latency, dur);
}

/*-------Start---------*/
PG::RecoveryState::Start::Start(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Start")
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  if (pg->is_primary()) {
    ldout(pg->cct, 1) << "transitioning to Primary" << dendl;
    post_event(MakePrimary());
  } else { //is_stray
    ldout(pg->cct, 1) << "transitioning to Stray" << dendl;
    post_event(MakeStray());
  }
}

void PG::RecoveryState::Start::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_start_latency, dur);
}

/*---------Primary--------*/
PG::RecoveryState::Primary::Primary(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/Primary")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  ceph_assert(pg->want_acting.empty());

  // set CREATING bit until we have peered for the first time.
  if (pg->info.history.last_epoch_started == 0) {
    pg->state_set(PG_STATE_CREATING);
    // use the history timestamp, which ultimately comes from the
    // monitor in the create case.
    utime_t t = pg->info.history.last_scrub_stamp;
    pg->info.stats.last_fresh = t;
    pg->info.stats.last_active = t;
    pg->info.stats.last_change = t;
    pg->info.stats.last_peered = t;
    pg->info.stats.last_clean = t;
    pg->info.stats.last_unstale = t;
    pg->info.stats.last_undegraded = t;
    pg->info.stats.last_fullsized = t;
    pg->info.stats.last_scrub_stamp = t;
    pg->info.stats.last_deep_scrub_stamp = t;
    pg->info.stats.last_clean_scrub_stamp = t;
  }
}

boost::statechart::result PG::RecoveryState::Primary::react(const MNotifyRec& notevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  ldout(pg->cct, 7) << "handle_pg_notify from osd." << notevt.from << dendl;
  pg->proc_replica_info(
    notevt.from, notevt.notify.info, notevt.notify.epoch_sent);
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Primary::react(const ActMap&)
{
  PG *pg = context< RecoveryMachine >().pg;
  ldout(pg->cct, 7) << "handle ActMap primary" << dendl;
  pg->publish_stats_to_osd();
  pg->take_waiters();
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Primary::react(
  const SetForceRecovery&)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->set_force_recovery(true);
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Primary::react(
  const UnsetForceRecovery&)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->set_force_recovery(false);
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Primary::react(
  const RequestScrub& evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  if (pg->is_primary()) {
    pg->unreg_next_scrub();
    pg->scrubber.must_scrub = true;
    pg->scrubber.must_deep_scrub = evt.deep || evt.repair;
    pg->scrubber.must_repair = evt.repair;
    pg->reg_next_scrub();
    ldout(pg->cct,10) << "marking for scrub" << dendl;
  }
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Primary::react(
  const SetForceBackfill&)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->set_force_backfill(true);
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Primary::react(
  const UnsetForceBackfill&)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->set_force_backfill(false);
  return discard_event();
}

void PG::RecoveryState::Primary::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->want_acting.clear();
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_primary_latency, dur);
  pg->clear_primary_state();
  pg->state_clear(PG_STATE_CREATING);
}

/*---------Peering--------*/
PG::RecoveryState::Peering::Peering(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/Primary/Peering"),
    history_les_bound(false)
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  ceph_assert(!pg->is_peered());
  ceph_assert(!pg->is_peering());
  ceph_assert(pg->is_primary());
  pg->state_set(PG_STATE_PEERING);
}

boost::statechart::result PG::RecoveryState::Peering::react(const AdvMap& advmap) 
{
  PG *pg = context< RecoveryMachine >().pg;
  ldout(pg->cct, 10) << "Peering advmap" << dendl;
  if (prior_set.affected_by_map(*(advmap.osdmap), pg)) {
    ldout(pg->cct, 1) << "Peering, affected_by_map, going to Reset" << dendl;
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
  pg->past_intervals.dump(q.f);
  q.f->close_section();

  q.f->open_array_section("probing_osds");
  for (set<pg_shard_t>::iterator p = prior_set.probe.begin();
       p != prior_set.probe.end();
       ++p)
    q.f->dump_stream("osd") << *p;
  q.f->close_section();

  if (prior_set.pg_down)
    q.f->dump_string("blocked", "peering is blocked due to down osds");

  q.f->open_array_section("down_osds_we_would_probe");
  for (set<int>::iterator p = prior_set.down.begin();
       p != prior_set.down.end();
       ++p)
    q.f->dump_int("osd", *p);
  q.f->close_section();

  q.f->open_array_section("peering_blocked_by");
  for (map<int,epoch_t>::iterator p = prior_set.blocked_by.begin();
       p != prior_set.blocked_by.end();
       ++p) {
    q.f->open_object_section("osd");
    q.f->dump_int("osd", p->first);
    q.f->dump_int("current_lost_at", p->second);
    q.f->dump_string("comment", "starting or marking this osd lost may let us proceed");
    q.f->close_section();
  }
  q.f->close_section();

  if (history_les_bound) {
    q.f->open_array_section("peering_blocked_by_detail");
    q.f->open_object_section("item");
    q.f->dump_string("detail","peering_blocked_by_history_les_bound");
    q.f->close_section();
    q.f->close_section();
  }

  q.f->close_section();
  return forward_event();
}

void PG::RecoveryState::Peering::exit()
{
  PG *pg = context< RecoveryMachine >().pg;
  ldout(pg->cct, 10) << "Leaving Peering" << dendl;
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  pg->state_clear(PG_STATE_PEERING);
  pg->clear_probe_targets();

  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_peering_latency, dur);
}


/*------Backfilling-------*/
PG::RecoveryState::Backfilling::Backfilling(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/Primary/Active/Backfilling")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  pg->backfill_reserved = true;
  pg->queue_recovery();
  pg->state_clear(PG_STATE_BACKFILL_TOOFULL);
  pg->state_clear(PG_STATE_BACKFILL_WAIT);
  pg->state_set(PG_STATE_BACKFILLING);
  pg->publish_stats_to_osd();
}

void PG::RecoveryState::Backfilling::backfill_release_reservations()
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->local_reserver.cancel_reservation(pg->info.pgid);
  for (set<pg_shard_t>::iterator it = pg->backfill_targets.begin();
       it != pg->backfill_targets.end();
       ++it) {
    ceph_assert(*it != pg->pg_whoami);
    ConnectionRef con = pg->osd->get_con_osd_cluster(
      it->osd, pg->get_osdmap_epoch());
    if (con) {
      pg->osd->send_message_osd_cluster(
        new MBackfillReserve(
	  MBackfillReserve::RELEASE,
	  spg_t(pg->info.pgid.pgid, it->shard),
	  pg->get_osdmap_epoch()),
	con.get());
    }
  }
}

void PG::RecoveryState::Backfilling::cancel_backfill()
{
  PG *pg = context< RecoveryMachine >().pg;
  backfill_release_reservations();
  if (!pg->waiting_on_backfill.empty()) {
    pg->waiting_on_backfill.clear();
    pg->finish_recovery_op(hobject_t::get_max());
  }
}

boost::statechart::result
PG::RecoveryState::Backfilling::react(const Backfilled &c)
{
  backfill_release_reservations();
  return transit<Recovered>();
}

boost::statechart::result
PG::RecoveryState::Backfilling::react(const DeferBackfill &c)
{
  PG *pg = context< RecoveryMachine >().pg;
  ldout(pg->cct, 10) << "defer backfill, retry delay " << c.delay << dendl;
  pg->state_set(PG_STATE_BACKFILL_WAIT);
  pg->state_clear(PG_STATE_BACKFILLING);
  cancel_backfill();
  pg->schedule_backfill_retry(c.delay);
  return transit<NotBackfilling>();
}

boost::statechart::result
PG::RecoveryState::Backfilling::react(const UnfoundBackfill &c)
{
  PG *pg = context< RecoveryMachine >().pg;
  ldout(pg->cct, 10) << "backfill has unfound, can't continue" << dendl;
  pg->state_set(PG_STATE_BACKFILL_UNFOUND);
  pg->state_clear(PG_STATE_BACKFILLING);
  cancel_backfill();
  return transit<NotBackfilling>();
}

boost::statechart::result
PG::RecoveryState::Backfilling::react(const RemoteReservationRevokedTooFull &)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_set(PG_STATE_BACKFILL_TOOFULL);
  pg->state_clear(PG_STATE_BACKFILLING);
  cancel_backfill();
  pg->schedule_backfill_retry(pg->cct->_conf->osd_backfill_retry_interval);
  return transit<NotBackfilling>();
}

boost::statechart::result
PG::RecoveryState::Backfilling::react(const RemoteReservationRevoked &)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_set(PG_STATE_BACKFILL_WAIT);
  cancel_backfill();
  if (pg->needs_backfill()) {
    return transit<WaitLocalBackfillReserved>();
  } else {
    // raced with MOSDPGBackfill::OP_BACKFILL_FINISH, ignore
    return discard_event();
  }
}

void PG::RecoveryState::Backfilling::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->backfill_reserved = false;
  pg->backfill_reserving = false;
  pg->state_clear(PG_STATE_BACKFILLING);
  pg->state_clear(PG_STATE_FORCED_BACKFILL | PG_STATE_FORCED_RECOVERY);
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_backfilling_latency, dur);
}

/*--WaitRemoteBackfillReserved--*/

PG::RecoveryState::WaitRemoteBackfillReserved::WaitRemoteBackfillReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/Primary/Active/WaitRemoteBackfillReserved"),
    backfill_osd_it(context< Active >().remote_shards_to_reserve_backfill.begin())
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_set(PG_STATE_BACKFILL_WAIT);
  pg->publish_stats_to_osd();
  post_event(RemoteBackfillReserved());
}

boost::statechart::result
PG::RecoveryState::WaitRemoteBackfillReserved::react(const RemoteBackfillReserved &evt)
{
  PG *pg = context< RecoveryMachine >().pg;

  int64_t num_bytes = pg->info.stats.stats.sum.num_bytes;
  ldout(pg->cct, 10) << __func__ << " num_bytes " << num_bytes << dendl;
  if (backfill_osd_it != context< Active >().remote_shards_to_reserve_backfill.end()) {
    //The primary never backfills itself
    ceph_assert(*backfill_osd_it != pg->pg_whoami);
    ConnectionRef con = pg->osd->get_con_osd_cluster(
      backfill_osd_it->osd, pg->get_osdmap_epoch());
    if (con) {
      pg->osd->send_message_osd_cluster(
        new MBackfillReserve(
	MBackfillReserve::REQUEST,
	spg_t(pg->info.pgid.pgid, backfill_osd_it->shard),
	pg->get_osdmap_epoch(),
	pg->get_backfill_priority(),
        num_bytes,
        pg->peer_bytes[*backfill_osd_it]),
      con.get());
    }
    ++backfill_osd_it;
  } else {
    pg->peer_bytes.clear();
    post_event(AllBackfillsReserved());
  }
  return discard_event();
}

void PG::RecoveryState::WaitRemoteBackfillReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_waitremotebackfillreserved_latency, dur);
}

void PG::RecoveryState::WaitRemoteBackfillReserved::retry()
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->local_reserver.cancel_reservation(pg->info.pgid);

  // Send CANCEL to all previously acquired reservations
  set<pg_shard_t>::const_iterator it, begin, end;
  begin = context< Active >().remote_shards_to_reserve_backfill.begin();
  end = context< Active >().remote_shards_to_reserve_backfill.end();
  ceph_assert(begin != end);
  for (it = begin; it != backfill_osd_it; ++it) {
    //The primary never backfills itself
    ceph_assert(*it != pg->pg_whoami);
    ConnectionRef con = pg->osd->get_con_osd_cluster(
      it->osd, pg->get_osdmap_epoch());
    if (con) {
      pg->osd->send_message_osd_cluster(
        new MBackfillReserve(
	MBackfillReserve::RELEASE,
	spg_t(pg->info.pgid.pgid, it->shard),
	pg->get_osdmap_epoch()),
      con.get());
    }
  }

  pg->state_clear(PG_STATE_BACKFILL_WAIT);
  pg->state_set(PG_STATE_BACKFILL_TOOFULL);
  pg->publish_stats_to_osd();

  pg->schedule_backfill_retry(pg->cct->_conf->osd_backfill_retry_interval);
}

boost::statechart::result
PG::RecoveryState::WaitRemoteBackfillReserved::react(const RemoteReservationRejected &evt)
{
  retry();
  return transit<NotBackfilling>();
}

boost::statechart::result
PG::RecoveryState::WaitRemoteBackfillReserved::react(const RemoteReservationRevoked &evt)
{
  retry();
  return transit<NotBackfilling>();
}

/*--WaitLocalBackfillReserved--*/
PG::RecoveryState::WaitLocalBackfillReserved::WaitLocalBackfillReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/Primary/Active/WaitLocalBackfillReserved")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_set(PG_STATE_BACKFILL_WAIT);
  pg->osd->local_reserver.request_reservation(
    pg->info.pgid,
    new QueuePeeringEvt<LocalBackfillReserved>(
      pg, pg->get_osdmap_epoch(),
      LocalBackfillReserved()),
    pg->get_backfill_priority(),
    new QueuePeeringEvt<DeferBackfill>(
      pg, pg->get_osdmap_epoch(),
      DeferBackfill(0.0)));
  pg->publish_stats_to_osd();
}

void PG::RecoveryState::WaitLocalBackfillReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_waitlocalbackfillreserved_latency, dur);
}

/*----NotBackfilling------*/
PG::RecoveryState::NotBackfilling::NotBackfilling(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/Primary/Active/NotBackfilling")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_REPAIR);
  pg->publish_stats_to_osd();
}

boost::statechart::result
PG::RecoveryState::NotBackfilling::react(const RemoteBackfillReserved &evt)
{
  return discard_event();
}

boost::statechart::result
PG::RecoveryState::NotBackfilling::react(const RemoteReservationRejected &evt)
{
  return discard_event();
}

void PG::RecoveryState::NotBackfilling::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_BACKFILL_UNFOUND);
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_notbackfilling_latency, dur);
}

/*----NotRecovering------*/
PG::RecoveryState::NotRecovering::NotRecovering(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/Primary/Active/NotRecovering")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  pg->publish_stats_to_osd();
}

void PG::RecoveryState::NotRecovering::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_RECOVERY_UNFOUND);
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_notrecovering_latency, dur);
}

/*---RepNotRecovering----*/
PG::RecoveryState::RepNotRecovering::RepNotRecovering(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/ReplicaActive/RepNotRecovering")
{
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result
PG::RecoveryState::RepNotRecovering::react(const RejectRemoteReservation &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->reject_reservation();
  post_event(RemoteReservationRejected());
  return discard_event();
}

void PG::RecoveryState::RepNotRecovering::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_repnotrecovering_latency, dur);
}

/*---RepWaitRecoveryReserved--*/
PG::RecoveryState::RepWaitRecoveryReserved::RepWaitRecoveryReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/ReplicaActive/RepWaitRecoveryReserved")
{
  context< RecoveryMachine >().log_enter(state_name);
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
      pg->get_osdmap_epoch()),
    pg->get_osdmap_epoch());
  return transit<RepRecovering>();
}

boost::statechart::result
PG::RecoveryState::RepWaitRecoveryReserved::react(
  const RemoteReservationCanceled &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->clear_reserved_num_bytes();
  pg->osd->remote_reserver.cancel_reservation(pg->info.pgid);
  return transit<RepNotRecovering>();
}

void PG::RecoveryState::RepWaitRecoveryReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_repwaitrecoveryreserved_latency, dur);
}

/*-RepWaitBackfillReserved*/
PG::RecoveryState::RepWaitBackfillReserved::RepWaitBackfillReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/ReplicaActive/RepWaitBackfillReserved")
{
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result
PG::RecoveryState::RepNotRecovering::react(const RequestBackfillPrio &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  // Use tentative_bacfill_full() to make sure enough
  // space is available to handle target bytes from primary.

  // TODO: If we passed num_objects from primary we could account for
  // an estimate of the metadata overhead.

  // TODO: If we had compressed_allocated and compressed_original from primary
  // we could compute compression ratio and adjust accordingly.

  // XXX: There is no way to get omap overhead and this would only apply
  // to whatever possibly different partition that is storing the database.

  // update_osd_stat() from heartbeat will do this on a new
  // statfs using pg->primary_num_bytes.
  uint64_t pending_adjustment = 0;
  int64_t primary_num_bytes = evt.primary_num_bytes;
  int64_t local_num_bytes = evt.local_num_bytes;
  if (primary_num_bytes) {
    // For erasure coded pool overestimate by a full stripe per object
    // because we don't know how each objected rounded to the nearest stripe
    if (pg->pool.info.is_erasure()) {
      primary_num_bytes /= (int)pg->get_pgbackend()->get_ec_data_chunk_count();
      primary_num_bytes += pg->get_pgbackend()->get_ec_stripe_chunk_size() * pg->info.stats.stats.sum.num_objects;
      local_num_bytes /= (int)pg->get_pgbackend()->get_ec_data_chunk_count();
      local_num_bytes += pg->get_pgbackend()->get_ec_stripe_chunk_size() * pg->info.stats.stats.sum.num_objects;
    }
    pending_adjustment = pending_backfill(pg->cct, primary_num_bytes, local_num_bytes);
    ldout(pg->cct, 10) << __func__ << " primary_num_bytes " << (primary_num_bytes >> 10) << "KiB"
                       << " local " << (local_num_bytes >> 10) << "KiB"
                       << " pending_adjustments " << (pending_adjustment >> 10) << "KiB"
                       << dendl;
  }
  // This lock protects not only the stats OSDService but also setting the pg primary_num_bytes
  // That's why we don't immediately unlock
  Mutex::Locker l(pg->osd->stat_lock);
  osd_stat_t cur_stat = pg->osd->osd_stat;
  if (pg->cct->_conf->osd_debug_reject_backfill_probability > 0 &&
      (rand()%1000 < (pg->cct->_conf->osd_debug_reject_backfill_probability*1000.0))) {
    ldout(pg->cct, 10) << "backfill reservation rejected: failure injection"
		       << dendl;
    post_event(RejectRemoteReservation());
  } else if (!pg->cct->_conf->osd_debug_skip_full_check_in_backfill_reservation &&
      pg->osd->tentative_backfill_full(pg, pending_adjustment, cur_stat)) {
    ldout(pg->cct, 10) << "backfill reservation rejected: backfill full"
		       << dendl;
    post_event(RejectRemoteReservation());
  } else {
    Context *preempt = nullptr;
    // Don't reserve space if skipped reservation check, this is used
    // to test the other backfill full check AND in case a corruption
    // of num_bytes requires ignoring that value and trying the
    // backfill anyway.
    if (primary_num_bytes && !pg->cct->_conf->osd_debug_skip_full_check_in_backfill_reservation)
      pg->set_reserved_num_bytes(primary_num_bytes, local_num_bytes);
    else
      pg->clear_reserved_num_bytes();
    // Use un-ec-adjusted bytes for stats.
    pg->info.stats.stats.sum.num_bytes = evt.local_num_bytes;
    if (HAVE_FEATURE(pg->upacting_features, RECOVERY_RESERVATION_2)) {
      // older peers will interpret preemption as TOOFULL
      preempt = new QueuePeeringEvt<RemoteBackfillPreempted>(
	pg, pg->get_osdmap_epoch(),
	RemoteBackfillPreempted());
    }
    pg->osd->remote_reserver.request_reservation(
      pg->info.pgid,
      new QueuePeeringEvt<RemoteBackfillReserved>(
        pg, pg->get_osdmap_epoch(),
        RemoteBackfillReserved()),
      evt.priority,
      preempt);
  }
  return transit<RepWaitBackfillReserved>();
}

boost::statechart::result
PG::RecoveryState::RepNotRecovering::react(const RequestRecoveryPrio &evt)
{
  PG *pg = context< RecoveryMachine >().pg;

  // fall back to a local reckoning of priority of primary doesn't pass one
  // (pre-mimic compat)
  int prio = evt.priority ? evt.priority : pg->get_recovery_priority();

  Context *preempt = nullptr;
  if (HAVE_FEATURE(pg->upacting_features, RECOVERY_RESERVATION_2)) {
    // older peers can't handle this
    preempt = new QueuePeeringEvt<RemoteRecoveryPreempted>(
      pg, pg->get_osdmap_epoch(),
      RemoteRecoveryPreempted());
  }

  pg->osd->remote_reserver.request_reservation(
    pg->info.pgid,
    new QueuePeeringEvt<RemoteRecoveryReserved>(
      pg, pg->get_osdmap_epoch(),
      RemoteRecoveryReserved()),
    prio,
    preempt);
  return transit<RepWaitRecoveryReserved>();
}

void PG::RecoveryState::RepWaitBackfillReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now() - enter_time;
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
	pg->get_osdmap_epoch()),
      pg->get_osdmap_epoch());
  return transit<RepRecovering>();
}

boost::statechart::result
PG::RecoveryState::RepWaitBackfillReserved::react(
  const RejectRemoteReservation &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->reject_reservation();
  post_event(RemoteReservationRejected());
  return discard_event();
}

boost::statechart::result
PG::RecoveryState::RepWaitBackfillReserved::react(
  const RemoteReservationRejected &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->clear_reserved_num_bytes();
  pg->osd->remote_reserver.cancel_reservation(pg->info.pgid);
  return transit<RepNotRecovering>();
}

boost::statechart::result
PG::RecoveryState::RepWaitBackfillReserved::react(
  const RemoteReservationCanceled &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->clear_reserved_num_bytes();
  pg->osd->remote_reserver.cancel_reservation(pg->info.pgid);
  return transit<RepNotRecovering>();
}

/*---RepRecovering-------*/
PG::RecoveryState::RepRecovering::RepRecovering(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/ReplicaActive/RepRecovering")
{
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result
PG::RecoveryState::RepRecovering::react(const RemoteRecoveryPreempted &)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->clear_reserved_num_bytes();
  pg->osd->send_message_osd_cluster(
    pg->primary.osd,
    new MRecoveryReserve(
      MRecoveryReserve::REVOKE,
      spg_t(pg->info.pgid.pgid, pg->primary.shard),
      pg->get_osdmap_epoch()),
    pg->get_osdmap_epoch());
  return discard_event();
}

boost::statechart::result
PG::RecoveryState::RepRecovering::react(const BackfillTooFull &)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->clear_reserved_num_bytes();
  pg->osd->send_message_osd_cluster(
    pg->primary.osd,
    new MBackfillReserve(
      MBackfillReserve::TOOFULL,
      spg_t(pg->info.pgid.pgid, pg->primary.shard),
      pg->get_osdmap_epoch()),
    pg->get_osdmap_epoch());
  return discard_event();
}

boost::statechart::result
PG::RecoveryState::RepRecovering::react(const RemoteBackfillPreempted &)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->clear_reserved_num_bytes();
  pg->osd->send_message_osd_cluster(
    pg->primary.osd,
    new MBackfillReserve(
      MBackfillReserve::REVOKE,
      spg_t(pg->info.pgid.pgid, pg->primary.shard),
      pg->get_osdmap_epoch()),
    pg->get_osdmap_epoch());
  return discard_event();
}

void PG::RecoveryState::RepRecovering::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->clear_reserved_num_bytes();
  pg->osd->remote_reserver.cancel_reservation(pg->info.pgid);
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_reprecovering_latency, dur);
}

/*------Activating--------*/
PG::RecoveryState::Activating::Activating(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/Primary/Active/Activating")
{
  context< RecoveryMachine >().log_enter(state_name);
}

void PG::RecoveryState::Activating::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_activating_latency, dur);
}

PG::RecoveryState::WaitLocalRecoveryReserved::WaitLocalRecoveryReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/Primary/Active/WaitLocalRecoveryReserved")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;

  // Make sure all nodes that part of the recovery aren't full
  if (!pg->cct->_conf->osd_debug_skip_full_check_in_recovery &&
      pg->osd->check_osdmap_full(pg->acting_recovery_backfill)) {
    post_event(RecoveryTooFull());
    return;
  }

  pg->state_clear(PG_STATE_RECOVERY_TOOFULL);
  pg->state_set(PG_STATE_RECOVERY_WAIT);
  pg->osd->local_reserver.request_reservation(
    pg->info.pgid,
    new QueuePeeringEvt<LocalRecoveryReserved>(
      pg, pg->get_osdmap_epoch(),
      LocalRecoveryReserved()),
    pg->get_recovery_priority(),
    new QueuePeeringEvt<DeferRecovery>(
      pg, pg->get_osdmap_epoch(),
      DeferRecovery(0.0)));
  pg->publish_stats_to_osd();
}

boost::statechart::result
PG::RecoveryState::WaitLocalRecoveryReserved::react(const RecoveryTooFull &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_set(PG_STATE_RECOVERY_TOOFULL);
  pg->schedule_recovery_retry(pg->cct->_conf->osd_recovery_retry_interval);
  return transit<NotRecovering>();
}

void PG::RecoveryState::WaitLocalRecoveryReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_waitlocalrecoveryreserved_latency, dur);
}

PG::RecoveryState::WaitRemoteRecoveryReserved::WaitRemoteRecoveryReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/Primary/Active/WaitRemoteRecoveryReserved"),
    remote_recovery_reservation_it(context< Active >().remote_shards_to_reserve_recovery.begin())
{
  context< RecoveryMachine >().log_enter(state_name);
  post_event(RemoteRecoveryReserved());
}

boost::statechart::result
PG::RecoveryState::WaitRemoteRecoveryReserved::react(const RemoteRecoveryReserved &evt) {
  PG *pg = context< RecoveryMachine >().pg;

  if (remote_recovery_reservation_it != context< Active >().remote_shards_to_reserve_recovery.end()) {
    ceph_assert(*remote_recovery_reservation_it != pg->pg_whoami);
    ConnectionRef con = pg->osd->get_con_osd_cluster(
      remote_recovery_reservation_it->osd, pg->get_osdmap_epoch());
    if (con) {
      pg->osd->send_message_osd_cluster(
        new MRecoveryReserve(
	  MRecoveryReserve::REQUEST,
	  spg_t(pg->info.pgid.pgid, remote_recovery_reservation_it->shard),
	  pg->get_osdmap_epoch(),
	  pg->get_recovery_priority()),
	con.get());
    }
    ++remote_recovery_reservation_it;
  } else {
    post_event(AllRemotesReserved());
  }
  return discard_event();
}

void PG::RecoveryState::WaitRemoteRecoveryReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_waitremoterecoveryreserved_latency, dur);
}

PG::RecoveryState::Recovering::Recovering(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/Primary/Active/Recovering")
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_RECOVERY_WAIT);
  pg->state_clear(PG_STATE_RECOVERY_TOOFULL);
  pg->state_set(PG_STATE_RECOVERING);
  ceph_assert(!pg->state_test(PG_STATE_ACTIVATING));
  pg->publish_stats_to_osd();
  pg->queue_recovery();
}

void PG::RecoveryState::Recovering::release_reservations(bool cancel)
{
  PG *pg = context< RecoveryMachine >().pg;
  ceph_assert(cancel || !pg->pg_log.get_missing().have_missing());

  // release remote reservations
  for (set<pg_shard_t>::const_iterator i =
	 context< Active >().remote_shards_to_reserve_recovery.begin();
        i != context< Active >().remote_shards_to_reserve_recovery.end();
        ++i) {
    if (*i == pg->pg_whoami) // skip myself
      continue;
    ConnectionRef con = pg->osd->get_con_osd_cluster(
      i->osd, pg->get_osdmap_epoch());
    if (con) {
      pg->osd->send_message_osd_cluster(
        new MRecoveryReserve(
	  MRecoveryReserve::RELEASE,
	  spg_t(pg->info.pgid.pgid, i->shard),
	  pg->get_osdmap_epoch()),
	con.get());
    }
  }
}

boost::statechart::result
PG::RecoveryState::Recovering::react(const AllReplicasRecovered &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_FORCED_RECOVERY);
  release_reservations();
  pg->osd->local_reserver.cancel_reservation(pg->info.pgid);
  return transit<Recovered>();
}

boost::statechart::result
PG::RecoveryState::Recovering::react(const RequestBackfill &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_FORCED_RECOVERY);
  release_reservations();
  pg->osd->local_reserver.cancel_reservation(pg->info.pgid);
  // XXX: Is this needed?
  pg->publish_stats_to_osd();
  return transit<WaitLocalBackfillReserved>();
}

boost::statechart::result
PG::RecoveryState::Recovering::react(const DeferRecovery &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  if (!pg->state_test(PG_STATE_RECOVERING)) {
    // we may have finished recovery and have an AllReplicasRecovered
    // event queued to move us to the next state.
    ldout(pg->cct, 10) << "got defer recovery but not recovering" << dendl;
    return discard_event();
  }
  ldout(pg->cct, 10) << "defer recovery, retry delay " << evt.delay << dendl;
  pg->state_set(PG_STATE_RECOVERY_WAIT);
  pg->osd->local_reserver.cancel_reservation(pg->info.pgid);
  release_reservations(true);
  pg->schedule_recovery_retry(evt.delay);
  return transit<NotRecovering>();
}

boost::statechart::result
PG::RecoveryState::Recovering::react(const UnfoundRecovery &evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  ldout(pg->cct, 10) << "recovery has unfound, can't continue" << dendl;
  pg->state_set(PG_STATE_RECOVERY_UNFOUND);
  pg->osd->local_reserver.cancel_reservation(pg->info.pgid);
  release_reservations(true);
  return transit<NotRecovering>();
}

void PG::RecoveryState::Recovering::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now() - enter_time;
  pg->state_clear(PG_STATE_RECOVERING);
  pg->osd->recoverystate_perf->tinc(rs_recovering_latency, dur);
}

PG::RecoveryState::Recovered::Recovered(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/Primary/Active/Recovered")
{
  pg_shard_t auth_log_shard;

  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;

  ceph_assert(!pg->needs_recovery());

  // if we finished backfill, all acting are active; recheck if
  // DEGRADED | UNDERSIZED is appropriate.
  ceph_assert(!pg->acting_recovery_backfill.empty());
  if (pg->get_osdmap()->get_pg_size(pg->info.pgid.pgid) <=
      pg->acting_recovery_backfill.size()) {
    pg->state_clear(PG_STATE_FORCED_BACKFILL | PG_STATE_FORCED_RECOVERY);
    pg->publish_stats_to_osd();
  }

  // adjust acting set?  (e.g. because backfill completed...)
  bool history_les_bound = false;
  if (pg->acting != pg->up && !pg->choose_acting(auth_log_shard,
						 true, &history_les_bound)) {
    ceph_assert(pg->want_acting.size());
  } else if (!pg->async_recovery_targets.empty()) {
    pg->choose_acting(auth_log_shard, true, &history_les_bound);
  }

  if (context< Active >().all_replicas_activated  &&
      pg->async_recovery_targets.empty())
    post_event(GoClean());
}

void PG::RecoveryState::Recovered::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_recovered_latency, dur);
}

PG::RecoveryState::Clean::Clean(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/Primary/Active/Clean")
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;

  if (pg->info.last_complete != pg->info.last_update) {
    ceph_abort();
  }
  Context *c = pg->finish_recovery();
  context< RecoveryMachine >().get_cur_transaction()->register_on_commit(c);

  pg->try_mark_clean();
}

void PG::RecoveryState::Clean::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->state_clear(PG_STATE_CLEAN);
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_clean_latency, dur);
}

template <typename T>
set<pg_shard_t> unique_osd_shard_set(const pg_shard_t & skip, const T &in)
{
  set<int> osds_found;
  set<pg_shard_t> out;
  for (typename T::const_iterator i = in.begin();
       i != in.end();
       ++i) {
    if (*i != skip && !osds_found.count(i->osd)) {
      osds_found.insert(i->osd);
      out.insert(*i);
    }
  }
  return out;
}

/*---------Active---------*/
PG::RecoveryState::Active::Active(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/Primary/Active"),
    remote_shards_to_reserve_recovery(
      unique_osd_shard_set(
	context< RecoveryMachine >().pg->pg_whoami,
	context< RecoveryMachine >().pg->acting_recovery_backfill)),
    remote_shards_to_reserve_backfill(
      unique_osd_shard_set(
	context< RecoveryMachine >().pg->pg_whoami,
	context< RecoveryMachine >().pg->backfill_targets)),
    all_replicas_activated(false)
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;

  ceph_assert(!pg->backfill_reserving);
  ceph_assert(!pg->backfill_reserved);
  ceph_assert(pg->is_primary());
  ldout(pg->cct, 10) << "In Active, about to call activate" << dendl;
  pg->start_flush(context< RecoveryMachine >().get_cur_transaction());
  pg->activate(*context< RecoveryMachine >().get_cur_transaction(),
	       pg->get_osdmap_epoch(),
	       *context< RecoveryMachine >().get_query_map(),
	       context< RecoveryMachine >().get_info_map(),
	       context< RecoveryMachine >().get_recovery_ctx());

  // everyone has to commit/ack before we are truly active
  pg->blocked_by.clear();
  for (set<pg_shard_t>::iterator p = pg->acting_recovery_backfill.begin();
       p != pg->acting_recovery_backfill.end();
       ++p) {
    if (p->shard != pg->pg_whoami.shard) {
      pg->blocked_by.insert(p->shard);
    }
  }
  pg->publish_stats_to_osd();
  ldout(pg->cct, 10) << "Activate Finished" << dendl;
}

boost::statechart::result PG::RecoveryState::Active::react(const AdvMap& advmap)
{
  PG *pg = context< RecoveryMachine >().pg;
  if (pg->should_restart_peering(
	advmap.up_primary,
	advmap.acting_primary,
	advmap.newup,
	advmap.newacting,
	advmap.lastmap,
	advmap.osdmap)) {
    ldout(pg->cct, 10) << "Active advmap interval change, fast return" << dendl;
    return forward_event();
  }
  ldout(pg->cct, 10) << "Active advmap" << dendl;
  bool need_publish = false;

  if (advmap.osdmap->require_osd_release >= CEPH_RELEASE_MIMIC) {
    const auto& new_removed_snaps = advmap.osdmap->get_new_removed_snaps();
    auto i = new_removed_snaps.find(pg->info.pgid.pool());
    if (i != new_removed_snaps.end()) {
      bool bad = false;
      for (auto j : i->second) {
	if (pg->snap_trimq.intersects(j.first, j.second)) {
	  decltype(pg->snap_trimq) added, overlap;
	  added.insert(j.first, j.second);
	  overlap.intersection_of(pg->snap_trimq, added);
	  if (pg->last_require_osd_release < CEPH_RELEASE_MIMIC) {
	    lderr(pg->cct) << __func__ << " removed_snaps already contains "
			   << overlap << ", but this is the first mimic+ osdmap,"
			   << " so it's expected" << dendl;
	  } else {
	    lderr(pg->cct) << __func__ << " removed_snaps already contains "
			   << overlap << dendl;
	    bad = true;
	  }
	  pg->snap_trimq.union_of(added);
	} else {
	  pg->snap_trimq.insert(j.first, j.second);
	}
      }
      if (pg->last_require_osd_release < CEPH_RELEASE_MIMIC) {
	// at upgrade, we report *all* previously removed snaps as removed in
	// the first mimic epoch.  remove the ones we previously divined were
	// removed (and subsequently purged) from the trimq.
	lderr(pg->cct) << __func__ << " first mimic map, filtering purged_snaps"
		       << " from new removed_snaps" << dendl;
	pg->snap_trimq.subtract(pg->info.purged_snaps);
      }
      ldout(pg->cct,10) << __func__ << " new removed_snaps " << i->second
			<< ", snap_trimq now " << pg->snap_trimq << dendl;
      ceph_assert(!bad || !pg->cct->_conf->osd_debug_verify_cached_snaps);
      pg->dirty_info = true;
      pg->dirty_big_info = true;
    }

    const auto& new_purged_snaps = advmap.osdmap->get_new_purged_snaps();
    auto j = new_purged_snaps.find(pg->info.pgid.pool());
    if (j != new_purged_snaps.end()) {
      bool bad = false;
      for (auto k : j->second) {
	if (!pg->info.purged_snaps.contains(k.first, k.second)) {
	  decltype(pg->info.purged_snaps) rm, overlap;
	  rm.insert(k.first, k.second);
	  overlap.intersection_of(pg->info.purged_snaps, rm);
	  lderr(pg->cct) << __func__ << " purged_snaps does not contain "
			 << rm << ", only " << overlap << dendl;
	  pg->info.purged_snaps.subtract(overlap);
	  // This can currently happen in the normal (if unlikely) course of
	  // events.  Because adding snaps to purged_snaps does not increase
	  // the pg version or add a pg log entry, we don't reliably propagate
	  // purged_snaps additions to other OSDs.
	  // One example:
	  //  - purge S
	  //  - primary and replicas update purged_snaps
	  //  - no object updates
	  //  - pg mapping changes, new primary on different node
	  //  - new primary pg version == eversion_t(), so info is not
	  //    propagated.
     	  //bad = true;
	} else {
	  pg->info.purged_snaps.erase(k.first, k.second);
	}
      }
      ldout(pg->cct,10) << __func__ << " new purged_snaps " << j->second
			<< ", now " << pg->info.purged_snaps << dendl;
      ceph_assert(!bad || !pg->cct->_conf->osd_debug_verify_cached_snaps);
      pg->dirty_info = true;
      pg->dirty_big_info = true;
    }
    if (pg->dirty_big_info) {
      // share updated purged_snaps to mgr/mon so that we (a) stop reporting
      // purged snaps and (b) perhaps share more snaps that we have purged
      // but didn't fit in pg_stat_t.
      need_publish = true;
      pg->share_pg_info();
    }
  } else if (!pg->pool.newly_removed_snaps.empty()) {
    pg->snap_trimq.union_of(pg->pool.newly_removed_snaps);
    ldout(pg->cct, 10) << *pg << " snap_trimq now " << pg->snap_trimq << dendl;
    pg->dirty_info = true;
    pg->dirty_big_info = true;
  }

  for (size_t i = 0; i < pg->want_acting.size(); i++) {
    int osd = pg->want_acting[i];
    if (!advmap.osdmap->is_up(osd)) {
      pg_shard_t osd_with_shard(osd, shard_id_t(i));
      ceph_assert(pg->is_acting(osd_with_shard) || pg->is_up(osd_with_shard));
    }
  }

  /* Check for changes in pool size (if the acting set changed as a result,
   * this does not matter) */
  if (advmap.lastmap->get_pg_size(pg->info.pgid.pgid) !=
      pg->get_osdmap()->get_pg_size(pg->info.pgid.pgid)) {
    if (pg->get_osdmap()->get_pg_size(pg->info.pgid.pgid) <= pg->actingset.size()) {
      pg->state_clear(PG_STATE_UNDERSIZED);
    } else {
      pg->state_set(PG_STATE_UNDERSIZED);
    }
    // degraded changes will be detected by call from publish_stats_to_osd()
    need_publish = true;
  }

  // if we haven't reported our PG stats in a long time, do so now.
  if (pg->info.stats.reported_epoch + pg->cct->_conf->osd_pg_stat_report_interval_max < advmap.osdmap->get_epoch()) {
    ldout(pg->cct, 20) << "reporting stats to osd after " << (advmap.osdmap->get_epoch() - pg->info.stats.reported_epoch)
		       << " epochs" << dendl;
    need_publish = true;
  }

  if (need_publish)
    pg->publish_stats_to_osd();

  return forward_event();
}
    
boost::statechart::result PG::RecoveryState::Active::react(const ActMap&)
{
  PG *pg = context< RecoveryMachine >().pg;
  ldout(pg->cct, 10) << "Active: handling ActMap" << dendl;
  ceph_assert(pg->is_primary());

  if (pg->have_unfound()) {
    // object may have become unfound
    pg->discover_all_missing(*context< RecoveryMachine >().get_query_map());
  }

  if (pg->cct->_conf->osd_check_for_log_corruption)
    pg->check_log_for_corruption(pg->osd->store);

  uint64_t unfound = pg->missing_loc.num_unfound();
  if (unfound > 0 &&
      pg->all_unfound_are_queried_or_lost(pg->get_osdmap())) {
    if (pg->cct->_conf->osd_auto_mark_unfound_lost) {
      pg->osd->clog->error() << pg->info.pgid.pgid << " has " << unfound
			    << " objects unfound and apparently lost, would automatically "
			    << "mark these objects lost but this feature is not yet implemented "
			    << "(osd_auto_mark_unfound_lost)";
    } else
      pg->osd->clog->error() << pg->info.pgid.pgid << " has "
                             << unfound << " objects unfound and apparently lost";
  }

  if (pg->is_active()) {
    ldout(pg->cct, 10) << "Active: kicking snap trim" << dendl;
    pg->kick_snap_trim();
  }

  if (pg->is_peered() &&
      !pg->is_clean() &&
      !pg->get_osdmap()->test_flag(CEPH_OSDMAP_NOBACKFILL) &&
      (!pg->get_osdmap()->test_flag(CEPH_OSDMAP_NOREBALANCE) || pg->is_degraded())) {
    pg->queue_recovery();
  }
  return forward_event();
}

boost::statechart::result PG::RecoveryState::Active::react(const MNotifyRec& notevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  ceph_assert(pg->is_primary());
  if (pg->peer_info.count(notevt.from)) {
    ldout(pg->cct, 10) << "Active: got notify from " << notevt.from
		       << ", already have info from that osd, ignoring"
		       << dendl;
  } else if (pg->peer_purged.count(notevt.from)) {
    ldout(pg->cct, 10) << "Active: got notify from " << notevt.from
		       << ", already purged that peer, ignoring"
		       << dendl;
  } else {
    ldout(pg->cct, 10) << "Active: got notify from " << notevt.from
		       << ", calling proc_replica_info and discover_all_missing"
		       << dendl;
    pg->proc_replica_info(
      notevt.from, notevt.notify.info, notevt.notify.epoch_sent);
    if (pg->have_unfound()) {
      pg->discover_all_missing(*context< RecoveryMachine >().get_query_map());
    }
  }
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Active::react(const MTrim& trim)
{
  PG *pg = context< RecoveryMachine >().pg;
  ceph_assert(pg->is_primary());

  // peer is informing us of their last_complete_ondisk
  ldout(pg->cct,10) << " replica osd." << trim.from << " lcod " << trim.trim_to << dendl;
  pg->peer_last_complete_ondisk[pg_shard_t(trim.from, trim.shard)] = trim.trim_to;

  // trim log when the pg is recovered
  pg->calc_min_last_complete_ondisk();
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Active::react(const MInfoRec& infoevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  ceph_assert(pg->is_primary());

  ceph_assert(!pg->acting_recovery_backfill.empty());
  // don't update history (yet) if we are active and primary; the replica
  // may be telling us they have activated (and committed) but we can't
  // share that until _everyone_ does the same.
  if (pg->is_acting_recovery_backfill(infoevt.from) &&
      pg->peer_activated.count(infoevt.from) == 0) {
    ldout(pg->cct, 10) << " peer osd." << infoevt.from
		       << " activated and committed" << dendl;
    pg->peer_activated.insert(infoevt.from);
    pg->blocked_by.erase(infoevt.from.shard);
    pg->publish_stats_to_osd();
    if (pg->peer_activated.size() == pg->acting_recovery_backfill.size()) {
      pg->all_activated_and_committed();
    }
  }
  return discard_event();
}

boost::statechart::result PG::RecoveryState::Active::react(const MLogRec& logevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  ldout(pg->cct, 10) << "searching osd." << logevt.from
		     << " log for unfound items" << dendl;
  pg->proc_replica_log(
    logevt.msg->info, logevt.msg->log, logevt.msg->missing, logevt.from);
  bool got_missing = pg->search_for_missing(
    pg->peer_info[logevt.from],
    pg->peer_missing[logevt.from],
    logevt.from,
    context< RecoveryMachine >().get_recovery_ctx());
  // If there are missing AND we are "fully" active then start recovery now
  if (got_missing && pg->state_test(PG_STATE_ACTIVE)) {
    post_event(DoRecovery());
  }
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
    q.f->dump_bool("scrubber.active", pg->scrubber.active);
    q.f->dump_string("scrubber.state", Scrubber::state_string(pg->scrubber.state));
    q.f->dump_stream("scrubber.start") << pg->scrubber.start;
    q.f->dump_stream("scrubber.end") << pg->scrubber.end;
    q.f->dump_stream("scrubber.max_end") << pg->scrubber.max_end;
    q.f->dump_stream("scrubber.subset_last_update") << pg->scrubber.subset_last_update;
    q.f->dump_bool("scrubber.deep", pg->scrubber.deep);
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
  pg_t pgid = pg->info.pgid.pgid;

  all_replicas_activated = true;

  pg->state_clear(PG_STATE_ACTIVATING);
  pg->state_clear(PG_STATE_CREATING);
  pg->state_clear(PG_STATE_PREMERGE);

  bool merge_target;
  if (pg->pool.info.is_pending_merge(pgid, &merge_target)) {
    pg->state_set(PG_STATE_PEERED);
    pg->state_set(PG_STATE_PREMERGE);

    if (pg->actingset.size() != pg->get_osdmap()->get_pg_size(pgid)) {
      if (merge_target) {
	pg_t src = pgid;
	src.set_ps(pg->pool.info.get_pg_num_pending());
	assert(src.get_parent() == pgid);
	pg->osd->set_not_ready_to_merge_target(pgid, src);
      } else {
	pg->osd->set_not_ready_to_merge_source(pgid);
      }
    }
  } else if (pg->acting.size() < pg->pool.info.min_size) {
    pg->state_set(PG_STATE_PEERED);
  } else {
    pg->state_set(PG_STATE_ACTIVE);
  }

  if (pg->pool.info.has_flag(pg_pool_t::FLAG_CREATING)) {
    pg->osd->send_pg_created(pgid);
  }

  pg->info.history.last_epoch_started = pg->info.last_epoch_started;
  pg->info.history.last_interval_started = pg->info.last_interval_started;
  pg->dirty_info = true;

  pg->share_pg_info();
  pg->publish_stats_to_osd();

  pg->check_local();

  // waiters
  if (pg->flushes_in_progress == 0) {
    pg->requeue_ops(pg->waiting_for_peered);
  } else if (!pg->waiting_for_peered.empty()) {
    ldout(pg->cct, 10) << __func__ << " flushes in progress, moving "
		       << pg->waiting_for_peered.size()
		       << " items to waiting_for_flush"
		       << dendl;
    ceph_assert(pg->waiting_for_flush.empty());
    pg->waiting_for_flush.swap(pg->waiting_for_peered);
  }

  pg->on_activate();

  return discard_event();
}

void PG::RecoveryState::Active::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->local_reserver.cancel_reservation(pg->info.pgid);

  pg->blocked_by.clear();
  pg->backfill_reserved = false;
  pg->backfill_reserving = false;
  pg->state_clear(PG_STATE_ACTIVATING);
  pg->state_clear(PG_STATE_DEGRADED);
  pg->state_clear(PG_STATE_UNDERSIZED);
  pg->state_clear(PG_STATE_BACKFILL_TOOFULL);
  pg->state_clear(PG_STATE_BACKFILL_WAIT);
  pg->state_clear(PG_STATE_RECOVERY_WAIT);
  pg->state_clear(PG_STATE_RECOVERY_TOOFULL);
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_active_latency, dur);
  pg->agent_stop();
}

/*------ReplicaActive-----*/
PG::RecoveryState::ReplicaActive::ReplicaActive(my_context ctx) 
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/ReplicaActive")
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  pg->start_flush(context< RecoveryMachine >().get_cur_transaction());
}


boost::statechart::result PG::RecoveryState::ReplicaActive::react(
  const Activate& actevt) {
  PG *pg = context< RecoveryMachine >().pg;
  ldout(pg->cct, 10) << "In ReplicaActive, about to call activate" << dendl;
  map<int, map<spg_t, pg_query_t> > query_map;
  pg->activate(*context< RecoveryMachine >().get_cur_transaction(),
	       actevt.activation_epoch,
	       query_map, NULL, NULL);
  ldout(pg->cct, 10) << "Activate Finished" << dendl;
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
  ldout(pg->cct, 10) << "received log from " << logevt.from << dendl;
  ObjectStore::Transaction* t = context<RecoveryMachine>().get_cur_transaction();
  pg->merge_log(*t, logevt.msg->info, logevt.msg->log, logevt.from);
  ceph_assert(pg->pg_log.get_head() == pg->info.last_update);

  return discard_event();
}

boost::statechart::result PG::RecoveryState::ReplicaActive::react(const MTrim& trim)
{
  PG *pg = context< RecoveryMachine >().pg;
  // primary is instructing us to trim
  pg->pg_log.trim(trim.trim_to, pg->info);
  pg->dirty_info = true;
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
	pg->get_osdmap_epoch(),
	pg->get_osdmap_epoch(),
	pg->info),
      pg->past_intervals);
  }
  pg->take_waiters();
  return discard_event();
}

boost::statechart::result PG::RecoveryState::ReplicaActive::react(
  const MQuery& query)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->fulfill_query(query, context<RecoveryMachine>().get_recovery_ctx());
  return discard_event();
}

boost::statechart::result PG::RecoveryState::ReplicaActive::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->close_section();
  return forward_event();
}

void PG::RecoveryState::ReplicaActive::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->clear_reserved_num_bytes();
  pg->osd->remote_reserver.cancel_reservation(pg->info.pgid);
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_replicaactive_latency, dur);
}

/*-------Stray---*/
PG::RecoveryState::Stray::Stray(my_context ctx) 
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/Stray")
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  ceph_assert(!pg->is_peered());
  ceph_assert(!pg->is_peering());
  ceph_assert(!pg->is_primary());

  if (!pg->get_osdmap()->have_pg_pool(pg->get_pgid().pool())) {
    ldout(pg->cct,10) << __func__ << " pool is deleted" << dendl;
    post_event(DeleteStart());
  } else {
    pg->start_flush(context< RecoveryMachine >().get_cur_transaction());
  }
}

boost::statechart::result PG::RecoveryState::Stray::react(const MLogRec& logevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  MOSDPGLog *msg = logevt.msg.get();
  ldout(pg->cct, 10) << "got info+log from osd." << logevt.from << " " << msg->info << " " << msg->log << dendl;

  ObjectStore::Transaction* t = context<RecoveryMachine>().get_cur_transaction();
  if (msg->info.last_backfill == hobject_t()) {
    // restart backfill
    pg->unreg_next_scrub();
    pg->info = msg->info;
    pg->reg_next_scrub();
    pg->dirty_info = true;
    pg->dirty_big_info = true;  // maybe.

    PGLogEntryHandler rollbacker{pg, t};
    pg->pg_log.reset_backfill_claim_log(msg->log, &rollbacker);

    pg->pg_log.reset_backfill();
  } else {
    pg->merge_log(*t, msg->info, msg->log, logevt.from);
  }

  ceph_assert(pg->pg_log.get_head() == pg->info.last_update);

  post_event(Activate(logevt.msg->info.last_epoch_started));
  return transit<ReplicaActive>();
}

boost::statechart::result PG::RecoveryState::Stray::react(const MInfoRec& infoevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  ldout(pg->cct, 10) << "got info from osd." << infoevt.from << " " << infoevt.info << dendl;

  if (pg->info.last_update > infoevt.info.last_update) {
    // rewind divergent log entries
    ObjectStore::Transaction* t = context<RecoveryMachine>().get_cur_transaction();
    pg->rewind_divergent_log(*t, infoevt.info.last_update);
    pg->info.stats = infoevt.info.stats;
    pg->info.hit_set = infoevt.info.hit_set;
  }

  ceph_assert(infoevt.info.last_update == pg->info.last_update);
  ceph_assert(pg->pg_log.get_head() == pg->info.last_update);

  post_event(Activate(infoevt.info.last_epoch_started));
  return transit<ReplicaActive>();
}

boost::statechart::result PG::RecoveryState::Stray::react(const MQuery& query)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->fulfill_query(query, context<RecoveryMachine>().get_recovery_ctx());
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
	pg->get_osdmap_epoch(),
	pg->get_osdmap_epoch(),
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
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_stray_latency, dur);
}


/*--------ToDelete----------*/
PG::RecoveryState::ToDelete::ToDelete(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/ToDelete")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  pg->osd->logger->inc(l_osd_pg_removing);
}

void PG::RecoveryState::ToDelete::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  // note: on a successful removal, this path doesn't execute. see
  // _delete_some().
  pg->osd->logger->dec(l_osd_pg_removing);
  pg->osd->local_reserver.cancel_reservation(pg->info.pgid);
}

/*----WaitDeleteReserved----*/
PG::RecoveryState::WaitDeleteReserved::WaitDeleteReserved(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg,
	       "Started/ToDelete/WaitDeleteReseved")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  context<ToDelete>().priority = pg->get_delete_priority();
  pg->osd->local_reserver.cancel_reservation(pg->info.pgid);
  pg->osd->local_reserver.request_reservation(
    pg->info.pgid,
    new QueuePeeringEvt<DeleteReserved>(
      pg, pg->get_osdmap_epoch(),
      DeleteReserved()),
    context<ToDelete>().priority,
    new QueuePeeringEvt<DeleteInterrupted>(
      pg, pg->get_osdmap_epoch(),
      DeleteInterrupted()));
}

boost::statechart::result PG::RecoveryState::ToDelete::react(
  const ActMap& evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  if (pg->get_delete_priority() != priority) {
    ldout(pg->cct,10) << __func__ << " delete priority changed, resetting"
		      << dendl;
    return transit<ToDelete>();
  }
  return discard_event();
}

void PG::RecoveryState::WaitDeleteReserved::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
}

/*----Deleting-----*/
PG::RecoveryState::Deleting::Deleting(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/ToDelete/Deleting")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;
  pg->deleting = true;
  ObjectStore::Transaction* t = context<RecoveryMachine>().get_cur_transaction();
  pg->on_removal(t);
  t->register_on_commit(new C_DeleteMore(pg, pg->get_osdmap_epoch()));
}

boost::statechart::result PG::RecoveryState::Deleting::react(
  const DeleteSome& evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  pg->_delete_some(context<RecoveryMachine>().get_cur_transaction());
  return discard_event();
}

void PG::RecoveryState::Deleting::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;
  pg->deleting = false;
  pg->osd->local_reserver.cancel_reservation(pg->info.pgid);
}

/*--------GetInfo---------*/
PG::RecoveryState::GetInfo::GetInfo(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/Primary/Peering/GetInfo")
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  pg->check_past_interval_bounds();
  PastIntervals::PriorSet &prior_set = context< Peering >().prior_set;

  ceph_assert(pg->blocked_by.empty());

  prior_set = pg->build_prior();

  pg->reset_min_peer_features();
  get_infos();
  if (prior_set.pg_down) {
    post_event(IsDown());
  } else if (peer_info_requested.empty()) {
    post_event(GotInfo());
  }
}

void PG::RecoveryState::GetInfo::get_infos()
{
  PG *pg = context< RecoveryMachine >().pg;
  PastIntervals::PriorSet &prior_set = context< Peering >().prior_set;

  pg->blocked_by.clear();
  for (set<pg_shard_t>::const_iterator it = prior_set.probe.begin();
       it != prior_set.probe.end();
       ++it) {
    pg_shard_t peer = *it;
    if (peer == pg->pg_whoami) {
      continue;
    }
    if (pg->peer_info.count(peer)) {
      ldout(pg->cct, 10) << " have osd." << peer << " info " << pg->peer_info[peer] << dendl;
      continue;
    }
    if (peer_info_requested.count(peer)) {
      ldout(pg->cct, 10) << " already requested info from osd." << peer << dendl;
      pg->blocked_by.insert(peer.osd);
    } else if (!pg->get_osdmap()->is_up(peer.osd)) {
      ldout(pg->cct, 10) << " not querying info from down osd." << peer << dendl;
    } else {
      ldout(pg->cct, 10) << " querying info from osd." << peer << dendl;
      context< RecoveryMachine >().send_query(
	peer, pg_query_t(pg_query_t::INFO,
			 it->shard, pg->pg_whoami.shard,
			 pg->info.history,
			 pg->get_osdmap_epoch()));
      peer_info_requested.insert(peer);
      pg->blocked_by.insert(peer.osd);
    }
  }

  pg->publish_stats_to_osd();
}

boost::statechart::result PG::RecoveryState::GetInfo::react(const MNotifyRec& infoevt) 
{
  PG *pg = context< RecoveryMachine >().pg;

  set<pg_shard_t>::iterator p = peer_info_requested.find(infoevt.from);
  if (p != peer_info_requested.end()) {
    peer_info_requested.erase(p);
    pg->blocked_by.erase(infoevt.from.osd);
  }

  epoch_t old_start = pg->info.history.last_epoch_started;
  if (pg->proc_replica_info(
	infoevt.from, infoevt.notify.info, infoevt.notify.epoch_sent)) {
    // we got something new ...
    PastIntervals::PriorSet &prior_set = context< Peering >().prior_set;
    if (old_start < pg->info.history.last_epoch_started) {
      ldout(pg->cct, 10) << " last_epoch_started moved forward, rebuilding prior" << dendl;
      prior_set = pg->build_prior();

      // filter out any osds that got dropped from the probe set from
      // peer_info_requested.  this is less expensive than restarting
      // peering (which would re-probe everyone).
      set<pg_shard_t>::iterator p = peer_info_requested.begin();
      while (p != peer_info_requested.end()) {
	if (prior_set.probe.count(*p) == 0) {
	  ldout(pg->cct, 20) << " dropping osd." << *p << " from info_requested, no longer in probe set" << dendl;
	  peer_info_requested.erase(p++);
	} else {
	  ++p;
	}
      }
      get_infos();
    }
    ldout(pg->cct, 20) << "Adding osd: " << infoevt.from.osd << " peer features: "
		       << hex << infoevt.features << dec << dendl;
    pg->apply_peer_features(infoevt.features);

    // are we done getting everything?
    if (peer_info_requested.empty() && !prior_set.pg_down) {
      ldout(pg->cct, 20) << "Common peer features: " << hex << pg->get_min_peer_features() << dec << dendl;
      ldout(pg->cct, 20) << "Common acting features: " << hex << pg->get_min_acting_features() << dec << dendl;
      ldout(pg->cct, 20) << "Common upacting features: " << hex << pg->get_min_upacting_features() << dec << dendl;
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
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_getinfo_latency, dur);
  pg->blocked_by.clear();
}

/*------GetLog------------*/
PG::RecoveryState::GetLog::GetLog(my_context ctx)
  : my_base(ctx),
    NamedState(
      context< RecoveryMachine >().pg, "Started/Primary/Peering/GetLog"),
    msg(0)
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;

  // adjust acting?
  if (!pg->choose_acting(auth_log_shard, false,
			 &context< Peering >().history_les_bound)) {
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
    ldout(pg->cct, 10) << " not contiguous with osd." << auth_log_shard << ", down" << dendl;
    post_event(IsIncomplete());
    return;
  }

  // how much log to request?
  eversion_t request_log_from = pg->info.last_update;
  ceph_assert(!pg->acting_recovery_backfill.empty());
  for (set<pg_shard_t>::iterator p = pg->acting_recovery_backfill.begin();
       p != pg->acting_recovery_backfill.end();
       ++p) {
    if (*p == pg->pg_whoami) continue;
    pg_info_t& ri = pg->peer_info[*p];
    if (ri.last_update < pg->info.log_tail && ri.last_update >= best.log_tail &&
        ri.last_update < request_log_from)
      request_log_from = ri.last_update;
  }

  // how much?
  ldout(pg->cct, 10) << " requesting log from osd." << auth_log_shard << dendl;
  context<RecoveryMachine>().send_query(
    auth_log_shard,
    pg_query_t(
      pg_query_t::LOG,
      auth_log_shard.shard, pg->pg_whoami.shard,
      request_log_from, pg->info.history,
      pg->get_osdmap_epoch()));

  ceph_assert(pg->blocked_by.empty());
  pg->blocked_by.insert(auth_log_shard.osd);
  pg->publish_stats_to_osd();
}

boost::statechart::result PG::RecoveryState::GetLog::react(const AdvMap& advmap)
{
  PG *pg = context< RecoveryMachine >().pg;
  // make sure our log source didn't go down.  we need to check
  // explicitly because it may not be part of the prior set, which
  // means the Peering state check won't catch it going down.
  if (!advmap.osdmap->is_up(auth_log_shard.osd)) {
    ldout(pg->cct, 10) << "GetLog: auth_log_shard osd."
		       << auth_log_shard.osd << " went down" << dendl;
    post_event(advmap);
    return transit< Reset >();
  }

  // let the Peering state do its checks.
  return forward_event();
}

boost::statechart::result PG::RecoveryState::GetLog::react(const MLogRec& logevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  ceph_assert(!msg);
  if (logevt.from != auth_log_shard) {
    ldout(pg->cct, 10) << "GetLog: discarding log from "
		       << "non-auth_log_shard osd." << logevt.from << dendl;
    return discard_event();
  }
  ldout(pg->cct, 10) << "GetLog: received master log from osd."
		     << logevt.from << dendl;
  msg = logevt.msg;
  post_event(GotLog());
  return discard_event();
}

boost::statechart::result PG::RecoveryState::GetLog::react(const GotLog&)
{
  PG *pg = context< RecoveryMachine >().pg;
  ldout(pg->cct, 10) << "leaving GetLog" << dendl;
  if (msg) {
    ldout(pg->cct, 10) << "processing master log" << dendl;
    pg->proc_master_log(*context<RecoveryMachine>().get_cur_transaction(),
			msg->info, msg->log, msg->missing,
			auth_log_shard);
  }
  pg->start_flush(context< RecoveryMachine >().get_cur_transaction());
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
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_getlog_latency, dur);
  pg->blocked_by.clear();
}

/*------WaitActingChange--------*/
PG::RecoveryState::WaitActingChange::WaitActingChange(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/Primary/WaitActingChange")
{
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result PG::RecoveryState::WaitActingChange::react(const AdvMap& advmap)
{
  PG *pg = context< RecoveryMachine >().pg;
  OSDMapRef osdmap = advmap.osdmap;

  ldout(pg->cct, 10) << "verifying no want_acting " << pg->want_acting << " targets didn't go down" << dendl;
  for (vector<int>::iterator p = pg->want_acting.begin(); p != pg->want_acting.end(); ++p) {
    if (!osdmap->is_up(*p)) {
      ldout(pg->cct, 10) << " want_acting target osd." << *p << " went down, resetting" << dendl;
      post_event(advmap);
      return transit< Reset >();
    }
  }
  return forward_event();
}

boost::statechart::result PG::RecoveryState::WaitActingChange::react(const MLogRec& logevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  ldout(pg->cct, 10) << "In WaitActingChange, ignoring MLocRec" << dendl;
  return discard_event();
}

boost::statechart::result PG::RecoveryState::WaitActingChange::react(const MInfoRec& evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  ldout(pg->cct, 10) << "In WaitActingChange, ignoring MInfoRec" << dendl;
  return discard_event();
}

boost::statechart::result PG::RecoveryState::WaitActingChange::react(const MNotifyRec& evt)
{
  PG *pg = context< RecoveryMachine >().pg;
  ldout(pg->cct, 10) << "In WaitActingChange, ignoring MNotifyRec" << dendl;
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
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_waitactingchange_latency, dur);
}

/*------Down--------*/
PG::RecoveryState::Down::Down(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/Primary/Peering/Down")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;

  pg->state_clear(PG_STATE_PEERING);
  pg->state_set(PG_STATE_DOWN);

  auto &prior_set = context< Peering >().prior_set;
  ceph_assert(pg->blocked_by.empty());
  pg->blocked_by.insert(prior_set.down.begin(), prior_set.down.end());
  pg->publish_stats_to_osd();
}

void PG::RecoveryState::Down::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;

  pg->state_clear(PG_STATE_DOWN);
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_down_latency, dur);

  pg->blocked_by.clear();
}

boost::statechart::result PG::RecoveryState::Down::react(const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment",
		   "not enough up instances of this PG to go active");
  q.f->close_section();
  return forward_event();
}

boost::statechart::result PG::RecoveryState::Down::react(const MNotifyRec& infoevt)
{
  PG *pg = context< RecoveryMachine >().pg;

  ceph_assert(pg->is_primary());
  epoch_t old_start = pg->info.history.last_epoch_started;
  if (!pg->peer_info.count(infoevt.from) &&
      pg->get_osdmap()->has_been_up_since(infoevt.from.osd, infoevt.notify.epoch_sent)) {
    pg->update_history(infoevt.notify.info.history);
  }
  // if we got something new to make pg escape down state
  if (pg->info.history.last_epoch_started > old_start) {
      ldout(pg->cct, 10) << " last_epoch_started moved forward, re-enter getinfo" << dendl;
    pg->state_clear(PG_STATE_DOWN);
    pg->state_set(PG_STATE_PEERING);
    return transit< GetInfo >();
  }

  return discard_event();
}


/*------Incomplete--------*/
PG::RecoveryState::Incomplete::Incomplete(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/Primary/Peering/Incomplete")
{
  context< RecoveryMachine >().log_enter(state_name);
  PG *pg = context< RecoveryMachine >().pg;

  pg->state_clear(PG_STATE_PEERING);
  pg->state_set(PG_STATE_INCOMPLETE);

  PastIntervals::PriorSet &prior_set = context< Peering >().prior_set;
  ceph_assert(pg->blocked_by.empty());
  pg->blocked_by.insert(prior_set.down.begin(), prior_set.down.end());
  pg->publish_stats_to_osd();
}

boost::statechart::result PG::RecoveryState::Incomplete::react(const AdvMap &advmap) {
  PG *pg = context< RecoveryMachine >().pg;
  int64_t poolnum = pg->info.pgid.pool();

  // Reset if min_size turn smaller than previous value, pg might now be able to go active
  if (!advmap.osdmap->have_pg_pool(poolnum) ||
      advmap.lastmap->get_pools().find(poolnum)->second.min_size >
      advmap.osdmap->get_pools().find(poolnum)->second.min_size) {
    post_event(advmap);
    return transit< Reset >();
  }

  return forward_event();
}

boost::statechart::result PG::RecoveryState::Incomplete::react(const MNotifyRec& notevt) {
  PG *pg = context< RecoveryMachine >().pg;
  ldout(pg->cct, 7) << "handle_pg_notify from osd." << notevt.from << dendl;
  if (pg->proc_replica_info(
    notevt.from, notevt.notify.info, notevt.notify.epoch_sent)) {
    // We got something new, try again!
    return transit< GetLog >();
  } else {
    return discard_event();
  }
}

boost::statechart::result PG::RecoveryState::Incomplete::react(
  const QueryState& q)
{
  q.f->open_object_section("state");
  q.f->dump_string("name", state_name);
  q.f->dump_stream("enter_time") << enter_time;
  q.f->dump_string("comment", "not enough complete instances of this PG");
  q.f->close_section();
  return forward_event();
}

void PG::RecoveryState::Incomplete::exit()
{
  context< RecoveryMachine >().log_exit(state_name, enter_time);
  PG *pg = context< RecoveryMachine >().pg;

  pg->state_clear(PG_STATE_INCOMPLETE);
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_incomplete_latency, dur);

  pg->blocked_by.clear();
}

/*------GetMissing--------*/
PG::RecoveryState::GetMissing::GetMissing(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/Primary/Peering/GetMissing")
{
  context< RecoveryMachine >().log_enter(state_name);

  PG *pg = context< RecoveryMachine >().pg;
  ceph_assert(!pg->acting_recovery_backfill.empty());
  eversion_t since;
  for (set<pg_shard_t>::iterator i = pg->acting_recovery_backfill.begin();
       i != pg->acting_recovery_backfill.end();
       ++i) {
    if (*i == pg->get_primary()) continue;
    const pg_info_t& pi = pg->peer_info[*i];
    // reset this so to make sure the pg_missing_t is initialized and
    // has the correct semantics even if we don't need to get a
    // missing set from a shard. This way later additions due to
    // lost+unfound delete work properly.
    pg->peer_missing[*i].may_include_deletes = !pg->perform_deletes_during_peering();

    if (pi.is_empty())
      continue;                                // no pg data, nothing divergent

    if (pi.last_update < pg->pg_log.get_tail()) {
      ldout(pg->cct, 10) << " osd." << *i << " is not contiguous, will restart backfill" << dendl;
      pg->peer_missing[*i].clear();
      continue;
    }
    if (pi.last_backfill == hobject_t()) {
      ldout(pg->cct, 10) << " osd." << *i << " will fully backfill; can infer empty missing set" << dendl;
      pg->peer_missing[*i].clear();
      continue;
    }

    if (pi.last_update == pi.last_complete &&  // peer has no missing
	pi.last_update == pg->info.last_update) {  // peer is up to date
      // replica has no missing and identical log as us.  no need to
      // pull anything.
      // FIXME: we can do better here.  if last_update==last_complete we
      //        can infer the rest!
      ldout(pg->cct, 10) << " osd." << *i << " has no missing, identical log" << dendl;
      pg->peer_missing[*i].clear();
      continue;
    }

    // We pull the log from the peer's last_epoch_started to ensure we
    // get enough log to detect divergent updates.
    since.epoch = pi.last_epoch_started;
    ceph_assert(pi.last_update >= pg->info.log_tail);  // or else choose_acting() did a bad thing
    if (pi.log_tail <= since) {
      ldout(pg->cct, 10) << " requesting log+missing since " << since << " from osd." << *i << dendl;
      context< RecoveryMachine >().send_query(
	*i,
	pg_query_t(
	  pg_query_t::LOG,
	  i->shard, pg->pg_whoami.shard,
	  since, pg->info.history,
	  pg->get_osdmap_epoch()));
    } else {
      ldout(pg->cct, 10) << " requesting fulllog+missing from osd." << *i
			 << " (want since " << since << " < log.tail "
			 << pi.log_tail << ")" << dendl;
      context< RecoveryMachine >().send_query(
	*i, pg_query_t(
	  pg_query_t::FULLLOG,
	  i->shard, pg->pg_whoami.shard,
	  pg->info.history, pg->get_osdmap_epoch()));
    }
    peer_missing_requested.insert(*i);
    pg->blocked_by.insert(i->osd);
  }

  if (peer_missing_requested.empty()) {
    if (pg->need_up_thru) {
      ldout(pg->cct, 10) << " still need up_thru update before going active"
			 << dendl;
      post_event(NeedUpThru());
      return;
    }

    // all good!
    post_event(Activate(pg->get_osdmap_epoch()));
  } else {
    pg->publish_stats_to_osd();
  }
}

boost::statechart::result PG::RecoveryState::GetMissing::react(const MLogRec& logevt)
{
  PG *pg = context< RecoveryMachine >().pg;

  peer_missing_requested.erase(logevt.from);
  pg->proc_replica_log(logevt.msg->info, logevt.msg->log, logevt.msg->missing, logevt.from);
  
  if (peer_missing_requested.empty()) {
    if (pg->need_up_thru) {
      ldout(pg->cct, 10) << " still need up_thru update before going active"
			 << dendl;
      post_event(NeedUpThru());
    } else {
      ldout(pg->cct, 10) << "Got last missing, don't need missing "
			 << "posting Activate" << dendl;
      post_event(Activate(pg->get_osdmap_epoch()));
    }
  }
  return discard_event();
}

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
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_getmissing_latency, dur);
  pg->blocked_by.clear();
}

/*------WaitUpThru--------*/
PG::RecoveryState::WaitUpThru::WaitUpThru(my_context ctx)
  : my_base(ctx),
    NamedState(context< RecoveryMachine >().pg, "Started/Primary/Peering/WaitUpThru")
{
  context< RecoveryMachine >().log_enter(state_name);
}

boost::statechart::result PG::RecoveryState::WaitUpThru::react(const ActMap& am)
{
  PG *pg = context< RecoveryMachine >().pg;
  if (!pg->need_up_thru) {
    post_event(Activate(pg->get_osdmap_epoch()));
  }
  return forward_event();
}

boost::statechart::result PG::RecoveryState::WaitUpThru::react(const MLogRec& logevt)
{
  PG *pg = context< RecoveryMachine >().pg;
  ldout(pg->cct, 10) << "Noting missing from osd." << logevt.from << dendl;
  pg->peer_missing[logevt.from].claim(logevt.msg->missing);
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
  utime_t dur = ceph_clock_now() - enter_time;
  pg->osd->recoverystate_perf->tinc(rs_waitupthru_latency, dur);
}

/*----RecoveryState::RecoveryMachine Methods-----*/
#undef dout_prefix
#define dout_prefix pg->gen_prefix(*_dout)

void PG::RecoveryState::RecoveryMachine::log_enter(const char *state_name)
{
  PG *pg = context< RecoveryMachine >().pg;
  ldout(pg->cct, 5) << "enter " << state_name << dendl;
  pg->osd->pg_recovery_stats.log_enter(state_name);
}

void PG::RecoveryState::RecoveryMachine::log_exit(const char *state_name, utime_t enter_time)
{
  utime_t dur = ceph_clock_now() - enter_time;
  PG *pg = context< RecoveryMachine >().pg;
  ldout(pg->cct, 5) << "exit " << state_name << " " << dur << " " << event_count << " " << event_time << dendl;
  pg->osd->pg_recovery_stats.log_exit(state_name, ceph_clock_now() - enter_time,
				      event_count, event_time);
  event_count = 0;
  event_time = utime_t();
}


/*---------------------------------------------------*/
#undef dout_prefix
#define dout_prefix ((debug_pg ? debug_pg->gen_prefix(*_dout) : *_dout) << " PriorSet: ")

void PG::RecoveryState::start_handle(RecoveryCtx *new_ctx) {
  ceph_assert(!rctx);
  ceph_assert(!orig_ctx);
  orig_ctx = new_ctx;
  if (new_ctx) {
    if (messages_pending_flush) {
      rctx = RecoveryCtx(*messages_pending_flush, *new_ctx);
    } else {
      rctx = *new_ctx;
    }
    rctx->start_time = ceph_clock_now();
  }
}

void PG::RecoveryState::begin_block_outgoing() {
  ceph_assert(!messages_pending_flush);
  ceph_assert(orig_ctx);
  ceph_assert(rctx);
  messages_pending_flush = BufferedRecoveryMessages();
  rctx = RecoveryCtx(*messages_pending_flush, *orig_ctx);
}

void PG::RecoveryState::clear_blocked_outgoing() {
  ceph_assert(orig_ctx);
  ceph_assert(rctx);
  messages_pending_flush = boost::optional<BufferedRecoveryMessages>();
}

void PG::RecoveryState::end_block_outgoing() {
  ceph_assert(messages_pending_flush);
  ceph_assert(orig_ctx);
  ceph_assert(rctx);

  rctx = RecoveryCtx(*orig_ctx);
  rctx->accept_buffered_messages(*messages_pending_flush);
  messages_pending_flush = boost::optional<BufferedRecoveryMessages>();
}

void PG::RecoveryState::end_handle() {
  if (rctx) {
    utime_t dur = ceph_clock_now() - rctx->start_time;
    machine.event_time += dur;
  }

  machine.event_count++;
  rctx = boost::optional<RecoveryCtx>();
  orig_ctx = NULL;
}

ostream& operator<<(ostream& out, const PG::BackfillInterval& bi)
{
  out << "BackfillInfo(" << bi.begin << "-" << bi.end
      << " " << bi.objects.size() << " objects";
  if (!bi.objects.empty())
    out << " " << bi.objects;
  out << ")";
  return out;
}

void PG::dump_pgstate_history(Formatter *f)
{
  lock();
  pgstate_history.dump(f);
  unlock();
}

void PG::dump_missing(Formatter *f)
{
  for (auto& i : pg_log.get_missing().get_items()) {
    f->open_object_section("object");
    f->dump_object("oid", i.first);
    f->dump_object("missing_info", i.second);
    if (missing_loc.needs_recovery(i.first)) {
      f->dump_bool("unfound", missing_loc.is_unfound(i.first));
      f->open_array_section("locations");
      for (auto l : missing_loc.get_locations(i.first)) {
	f->dump_object("shard", l);
      }
      f->close_section();
    }
    f->close_section();
  }
}

void PG::get_pg_stats(std::function<void(const pg_stat_t&, epoch_t lec)> f)
{
  pg_stats_publish_lock.Lock();
  if (pg_stats_publish_valid) {
    f(pg_stats_publish, pg_stats_publish.get_effective_last_epoch_clean());
  }
  pg_stats_publish_lock.Unlock();
}

void PG::with_heartbeat_peers(std::function<void(int)> f)
{
  heartbeat_peer_lock.Lock();
  for (auto p : heartbeat_peers) {
    f(p);
  }
  for (auto p : probe_targets) {
    f(p);
  }
  heartbeat_peer_lock.Unlock();
}
