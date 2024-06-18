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
#include "messages/MOSDRepScrub.h"

#include "common/errno.h"
#include "common/ceph_releases.h"
#include "common/config.h"
#include "OSD.h"
#include "OpRequest.h"
#include "osd/scrubber/ScrubStore.h"
#include "osd/scrubber/pg_scrubber.h"
#include "osd/scheduler/OpSchedulerItem.h"
#include "Session.h"

#include "common/Timer.h"
#include "common/perf_counters.h"

#include "messages/MOSDOp.h"
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

using std::list;
using std::map;
using std::ostringstream;
using std::pair;
using std::set;
using std::string;
using std::stringstream;
using std::unique_ptr;
using std::vector;

using ceph::bufferlist;
using ceph::bufferptr;
using ceph::decode;
using ceph::encode;
using ceph::Formatter;

using namespace ceph::osd::scheduler;

template <class T>
static ostream& _prefix(std::ostream *_dout, T *t)
{
  return t->gen_prefix(*_dout);
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
  ClibBackTrace bt(0);
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

PG::PG(OSDService *o, OSDMapRef curmap,
       const PGPool &_pool, spg_t p) :
  pg_whoami(o->whoami, p.shard),
  pg_id(p),
  coll(p),
  osd(o),
  cct(o->cct),
  osdriver(osd->store, coll_t(), OSD::make_snapmapper_oid()),
  snap_mapper(
    cct,
    &osdriver,
    p.ps(),
    p.get_split_bits(_pool.info.get_pg_num()),
    _pool.id,
    p.shard),
  trace_endpoint("0.0.0.0", 0, "PG"),
  info_struct_v(0),
  pgmeta_oid(p.make_pgmeta_oid()),
  stat_queue_item(this),
  recovery_queued(false),
  recovery_ops_active(0),
  backfill_reserving(false),
  finish_sync_event(NULL),
  scrub_after_recovery(false),
  active_pushes(0),
  recovery_state(
    o->cct,
    pg_whoami,
    p,
    _pool,
    curmap,
    this,
    this),
  pool(recovery_state.get_pgpool()),
  info(recovery_state.get_info())
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
#ifdef PG_DEBUG_REFS
  osd->remove_pgid(info.pgid, this);
#endif
}

void PG::lock(bool no_lockdep) const
{
#ifdef CEPH_DEBUG_MUTEX
  _lock.lock(no_lockdep);
#else
  _lock.lock();
  locked_by = std::this_thread::get_id();
#endif
  // if we have unrecorded dirty state with the lock dropped, there is a bug
  ceph_assert(!recovery_state.debug_has_dirty_state());

  dout(30) << "lock" << dendl;
}

bool PG::is_locked() const
{
  return ceph_mutex_is_locked(_lock);
}

void PG::unlock() const
{
  //generic_dout(0) << this << " " << info.pgid << " unlock" << dendl;
  ceph_assert(!recovery_state.debug_has_dirty_state());
#ifndef CEPH_DEBUG_MUTEX
  locked_by = {};
#endif
  _lock.unlock();
}

std::ostream& PG::gen_prefix(std::ostream& out) const
{
  OSDMapRef mapref = recovery_state.get_osdmap();
#ifdef CEPH_DEBUG_MUTEX
  if (_lock.is_locked_by_me()) {
#else
  if (locked_by == std::this_thread::get_id()) {
#endif
    out << "osd." << osd->whoami
	<< " pg_epoch: " << (mapref ? mapref->get_epoch():0)
	<< " " << *this << " ";
  } else {
    out << "osd." << osd->whoami
	<< " pg_epoch: " << (mapref ? mapref->get_epoch():0)
	<< " pg[" << pg_id.pgid << "(unlocked)] ";
  }
  return out;
}

PerfCounters &PG::get_peering_perf() {
  return *(osd->recoverystate_perf);
}

PerfCounters &PG::get_perf_logger() {
  return *(osd->logger);
}

void PG::log_state_enter(const char *state) {
  osd->pg_recovery_stats.log_enter(state);
}

void PG::log_state_exit(
  const char *state_name, utime_t enter_time,
  uint64_t events, utime_t event_dur) {
  osd->pg_recovery_stats.log_exit(
    state_name, ceph_clock_now() - enter_time, events, event_dur);
}

/********* PG **********/

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

/******* PG ***********/
void PG::clear_primary_state()
{
  dout(20) << __func__ << dendl;

  projected_log = PGLog::IndexedLog();

  snap_trimq.clear();
  snap_trimq_repeat.clear();
  finish_sync_event = 0;  // so that _finish_recovery doesn't go off in another thread
  release_pg_backoffs();

  if (m_scrubber) {
    m_scrubber->on_new_interval();
  }
  scrub_after_recovery = false;

  agent_clear();
}


bool PG::op_has_sufficient_caps(OpRequestRef& op)
{
  // only check MOSDOp
  if (op->get_req()->get_type() != CEPH_MSG_OSD_OP)
    return true;

  auto req = op->get_req<MOSDOp>();
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
    // Let cost per object be the average object size
    uint64_t cost_per_object = get_average_object_size();
    osd->queue_for_recovery(
      this, cost_per_object, recovery_state.get_recovery_op_priority()
    );
  }
}

void PG::queue_scrub_after_repair()
{
  dout(10) << __func__ << dendl;
  ceph_assert(ceph_mutex_is_locked(_lock));

  m_planned_scrub.must_deep_scrub = true;
  m_planned_scrub.check_repair = true;
  m_planned_scrub.must_scrub = true;
  m_planned_scrub.calculated_to_deep = true;

  if (is_scrub_queued_or_active()) {
    dout(10) << __func__ << ": scrubbing already ("
             << (is_scrubbing() ? "active)" : "queued)") << dendl;
    return;
  }

  m_scrubber->set_op_parameters(m_planned_scrub);
  dout(15) << __func__ << ": queueing" << dendl;

  osd->queue_scrub_after_repair(this, Scrub::scrub_prio_t::high_priority);
}

unsigned PG::get_scrub_priority()
{
  // a higher value -> a higher priority
  int64_t pool_scrub_priority =
    pool.info.opts.value_or(pool_opts_t::SCRUB_PRIORITY, (int64_t)0);
  return pool_scrub_priority > 0 ? pool_scrub_priority : cct->_conf->osd_scrub_priority;
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

void PG::_finish_recovery(Context* c)
{
  dout(15) << __func__ << " finish_sync_event? " << finish_sync_event << " clean? "
		 << is_clean() << dendl;

  std::scoped_lock locker{*this};
  if (recovery_state.is_deleting() || !is_clean()) {
    dout(10) << __func__ << " raced with delete or repair" << dendl;
    return;
  }
  // When recovery is initiated by a repair, that flag is left on
  state_clear(PG_STATE_REPAIR);
  if (c == finish_sync_event) {
    dout(15) << __func__ << " scrub_after_recovery? " << scrub_after_recovery << dendl;
    finish_sync_event = 0;
    recovery_state.purge_strays();

    publish_stats_to_osd();

    if (scrub_after_recovery) {
      dout(10) << "_finish_recovery requeueing for scrub" << dendl;
      scrub_after_recovery = false;
      queue_scrub_after_repair();
    }
  } else {
    dout(10) << "_finish_recovery -- stale" << dendl;
  }
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
  recovery_state.split_into(child_pgid, &child->recovery_state, split_bits);

  child->update_snap_mapper_bits(split_bits);

  child->snap_trimq = snap_trimq;
  child->snap_trimq_repeat = snap_trimq_repeat;

  _split_into(child_pgid, child, split_bits);

  // release all backoffs for simplicity
  release_backoffs(hobject_t(), hobject_t::get_max());
}

void PG::start_split_stats(const set<spg_t>& childpgs, vector<object_stat_sum_t> *out)
{
  recovery_state.start_split_stats(childpgs, out);
}

void PG::finish_split_stats(const object_stat_sum_t& stats, ObjectStore::Transaction &t)
{
  recovery_state.finish_split_stats(stats, t);
}

void PG::merge_from(map<spg_t,PGRef>& sources, PeeringCtx &rctx,
		    unsigned split_bits,
		    const pg_merge_meta_t& last_pg_merge_meta)
{
  dout(10) << __func__ << " from " << sources << " split_bits " << split_bits
	   << dendl;
  map<spg_t, PeeringState*> source_ps;
  for (auto &&source : sources) {
    source_ps.emplace(source.first, &source.second->recovery_state);
  }
  recovery_state.merge_from(source_ps, rctx, split_bits, last_pg_merge_meta);

  for (auto& i : sources) {
    auto& source = i.second;
    // wipe out source's pgmeta
    rctx.transaction.remove(source->coll, source->pgmeta_oid);

    // merge (and destroy source collection)
    rctx.transaction.merge_collection(source->coll, coll, split_bits);
  }

  // merge_collection does this, but maybe all of our sources were missing.
  rctx.transaction.collection_set_bits(coll, split_bits);

  snap_mapper.update_bits(split_bits);
}

void PG::add_backoff(const ceph::ref_t<Session>& s, const hobject_t& begin, const hobject_t& end)
{
  auto con = s->con;
  if (!con)   // OSD::ms_handle_reset clears s->con without a lock
    return;
  auto b = s->have_backoff(info.pgid, begin);
  if (b) {
    derr << __func__ << " already have backoff for " << s << " begin " << begin
	 << " " << *b << dendl;
    ceph_abort();
  }
  std::lock_guard l(backoff_lock);
  b = ceph::make_ref<Backoff>(info.pgid, this, s, ++s->backoff_seq, begin, end);
  backoffs[begin].insert(b);
  s->add_backoff(b);
  dout(10) << __func__ << " session " << s << " added " << *b << dendl;
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
  vector<ceph::ref_t<Backoff>> bv;
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
	int rr = cmp((*q)->begin, begin);
	if (rr == 0 || (rr > 0 && (*q)->end < end)) {
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
  map<hobject_t,set<ceph::ref_t<Backoff>>> ls;
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
void PG::rm_backoff(const ceph::ref_t<Backoff>& b)
{
  dout(10) << __func__ << " " << *b << dendl;
  std::lock_guard l(backoff_lock);
  ceph_assert(ceph_mutex_is_locked_by_me(b->lock));
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

  finish_sync_event = 0;

  hobject_t soid;
  while (recovery_ops_active > 0) {
#ifdef DEBUG_RECOVERY_OIDS
    soid = *recovering_oids.begin();
#endif
    finish_recovery_op(soid, true);
  }

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

void PG::send_cluster_message(
  int target, MessageRef m,
  epoch_t epoch, bool share_map_update)
{
  ConnectionRef con = osd->get_con_osd_cluster(
    target, get_osdmap_epoch());
  if (!con) {
    return;
  }

  if (share_map_update) {
    osd->maybe_share_map(con.get(), get_osdmap());
  }
  osd->send_message_osd_cluster(m, con.get());
}

void PG::clear_probe_targets()
{
  std::lock_guard l(heartbeat_peer_lock);
  probe_targets.clear();
}

void PG::update_heartbeat_peers(set<int> new_peers)
{
  bool need_update = false;
  heartbeat_peer_lock.lock();
  if (new_peers == heartbeat_peers) {
    dout(10) << "update_heartbeat_peers " << heartbeat_peers << " unchanged" << dendl;
  } else {
    dout(10) << "update_heartbeat_peers " << heartbeat_peers << " -> " << new_peers << dendl;
    heartbeat_peers.swap(new_peers);
    need_update = true;
  }
  heartbeat_peer_lock.unlock();

  if (need_update)
    osd->need_heartbeat_peer_update();
}


bool PG::check_in_progress_op(
  const osd_reqid_t &r,
  eversion_t *version,
  version_t *user_version,
  int *return_code,
  vector<pg_log_op_return_item_t> *op_returns
  ) const
{
  return (
    projected_log.get_request(r, version, user_version, return_code,
			      op_returns) ||
    recovery_state.get_pg_log().get_log().get_request(
      r, version, user_version, return_code, op_returns));
}

void PG::publish_stats_to_osd()
{
  if (!is_primary())
    return;

  ceph_assert(m_scrubber);
  recovery_state.update_stats_wo_resched(
    [scrubber = m_scrubber.get()](pg_history_t& hist,
                                  pg_stat_t& info) mutable -> void {
      info.scrub_sched_status = scrubber->get_schedule();
    });

  std::lock_guard l{pg_stats_publish_lock};
  auto stats =
    recovery_state.prepare_stats_for_publish(pg_stats_publish, unstable_stats);
  if (stats) {
    pg_stats_publish = std::move(stats);
  }
}

unsigned PG::get_target_pg_log_entries() const
{
  return osd->get_target_pg_log_entries();
}

void PG::clear_publish_stats()
{
  dout(15) << "clear_stats" << dendl;
  std::lock_guard l{pg_stats_publish_lock};
  pg_stats_publish.reset();
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
  ObjectStore::Transaction &t)
{
  recovery_state.init(
    role, newup, new_up_primary, newacting,
    new_acting_primary, history, pi, t);
}

void PG::shutdown()
{
  ch->flush();
  std::scoped_lock l{*this};
  recovery_state.shutdown();
  on_shutdown();
}

#pragma GCC diagnostic ignored "-Wpragmas"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

void PG::upgrade(ObjectStore *store)
{
  dout(0) << __func__ << " " << info_struct_v << " -> " << pg_latest_struct_v
	  << dendl;
  ceph_assert(info_struct_v <= 10);
  ObjectStore::Transaction t;

  // <do upgrade steps here>

  // finished upgrade!
  ceph_assert(info_struct_v == 10);

  // update infover_key
  if (info_struct_v < pg_latest_struct_v) {
    map<string,bufferlist> v;
    __u8 ver = pg_latest_struct_v;
    encode(ver, v[string(infover_key)]);
    t.omap_setkeys(coll, pgmeta_oid, v);
  }

  recovery_state.force_write_state(t);

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

void PG::prepare_write(
  pg_info_t &info,
  pg_info_t &last_written_info,
  PastIntervals &past_intervals,
  PGLog &pglog,
  bool dirty_info,
  bool dirty_big_info,
  bool need_write_epoch,
  ObjectStore::Transaction &t)
{
  info.stats.stats.add(unstable_stats);
  unstable_stats.clear();
  map<string,bufferlist> km;
  string key_to_remove;
  if (dirty_big_info || dirty_info) {
    int ret = prepare_info_keymap(
      cct,
      &km,
      &key_to_remove,
      get_osdmap_epoch(),
      info,
      last_written_info,
      past_intervals,
      dirty_big_info,
      need_write_epoch,
      cct->_conf->osd_fast_info,
      osd->logger,
      this);
    ceph_assert(ret == 0);
  }
  pglog.write_log_and_missing(
    t, &km, coll, pgmeta_oid, pool.info.require_rollback());
  if (!km.empty())
    t.omap_setkeys(coll, pgmeta_oid, km);
  if (!key_to_remove.empty())
    t.omap_rmkey(coll, pgmeta_oid, key_to_remove);
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
  ghobject_t pgmeta_oid(pgid.make_pgmeta_oid());
  epoch_t cur_epoch = 0;

  // validate collection name
  ceph_assert(coll.is_pg());

  // try for v8
  set<string> keys;
  keys.insert(string(infover_key));
  keys.insert(string(epoch_key));
  map<string,bufferlist> values;
  auto ch = store->open_collection(coll);
  ceph_assert(ch);
  int r = store->omap_get_values(ch, pgmeta_oid, keys, &values);
  if (r == 0) {
    ceph_assert(values.size() == 2);

    // sanity check version
    auto bp = values[string(infover_key)].cbegin();
    __u8 struct_v = 0;
    decode(struct_v, bp);
    ceph_assert(struct_v >= 8);

    // get epoch
    bp = values[string(epoch_key)].begin();
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
  keys.insert(string(infover_key));
  keys.insert(string(info_key));
  keys.insert(string(biginfo_key));
  keys.insert(string(fastinfo_key));
  ghobject_t pgmeta_oid(pgid.make_pgmeta_oid());
  map<string,bufferlist> values;
  auto ch = store->open_collection(coll);
  ceph_assert(ch);
  int r = store->omap_get_values(ch, pgmeta_oid, keys, &values);
  ceph_assert(r == 0);
  ceph_assert(values.size() == 3 ||
	 values.size() == 4);

  auto p = values[string(infover_key)].cbegin();
  decode(struct_v, p);
  ceph_assert(struct_v >= 10);

  p = values[string(info_key)].begin();
  decode(info, p);

  p = values[string(biginfo_key)].begin();
  decode(past_intervals, p);
  decode(info.purged_snaps, p);

  p = values[string(fastinfo_key)].begin();
  if (!p.end()) {
    pg_fast_info_t fast;
    decode(fast, p);
    fast.try_apply_to(&info);
  }
  return 0;
}

void PG::read_state(ObjectStore *store)
{
  PastIntervals past_intervals_from_disk;
  pg_info_t info_from_disk;
  int r = read_info(
    store,
    pg_id,
    coll,
    info_from_disk,
    past_intervals_from_disk,
    info_struct_v);
  ceph_assert(r >= 0);

  if (info_struct_v < pg_compat_struct_v) {
    derr << "PG needs upgrade, but on-disk data is too old; upgrade to"
	 << " an older version first." << dendl;
    ceph_abort_msg("PG too old to upgrade");
  }

  recovery_state.init_from_disk_state(
    std::move(info_from_disk),
    std::move(past_intervals_from_disk),
    [this, store] (PGLog &pglog) {
      ostringstream oss;
      pglog.read_log_and_missing(
	store,
	ch,
	pgmeta_oid,
	info,
	oss,
	cct->_conf->osd_ignore_stale_divergent_priors,
	cct->_conf->osd_debug_verify_missing_on_start);

      if (oss.tellp())
	osd->clog->error() << oss.str();
      return 0;
    });

  if (info_struct_v < pg_latest_struct_v) {
    upgrade(store);
  }

  // initialize current mapping
  {
    int primary, up_primary;
    vector<int> acting, up;
    get_osdmap()->pg_to_up_acting_osds(
      pg_id.pgid, &up, &up_primary, &acting, &primary);
    recovery_state.init_primary_up_acting(
      up,
      acting,
      up_primary,
      primary);
    recovery_state.set_role(OSDMap::calc_pg_role(pg_whoami, acting));
  }

  // init pool options
  store->set_collection_opts(ch, pool.info.opts);

  PeeringCtx rctx;
  handle_initialize(rctx);
  // note: we don't activate here because we know the OSD will advance maps
  // during boot.
  write_if_dirty(rctx.transaction);
  store->queue_transaction(ch, std::move(rctx.transaction));
}

void PG::update_snap_map(
  const vector<pg_log_entry_t> &log_entries,
  ObjectStore::Transaction &t)
{
  for (auto i = log_entries.cbegin(); i != log_entries.cend(); ++i) {
    OSDriver::OSTransaction _t(osdriver.get_transaction(&t));
    if (i->soid.snap < CEPH_MAXSNAP) {
      if (i->is_delete()) {
	int r = snap_mapper.remove_oid(
	  i->soid,
	  &_t);
	if (r)
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
  for (auto it = m.begin(); it != m.end(); ++it)
    requeue_ops(it->second);
  m.clear();
}

void PG::requeue_op(OpRequestRef op)
{
  auto p = waiting_for_map.find(op->get_source());
  if (p != waiting_for_map.end()) {
    dout(20) << __func__ << " " << *op->get_req()
             << " (waiting_for_map " << p->first << ")"
	     << dendl;
    p->second.push_front(op);
  } else {
    dout(20) << __func__ << " " << *op->get_req() << dendl;
    osd->enqueue_front(
      OpSchedulerItem(
        unique_ptr<OpSchedulerItem::OpQueueable>(new PGOpItem(info.pgid, op)),
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
	osd->enqueue_front(OpSchedulerItem(
          unique_ptr<OpSchedulerItem::OpQueueable>(new PGOpItem(info.pgid, req)),
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

bool PG::get_must_scrub() const
{
  dout(20) << __func__ << " must_scrub? " << (m_planned_scrub.must_scrub ? "true" : "false") << dendl;
  return m_planned_scrub.must_scrub;
}

unsigned int PG::scrub_requeue_priority(Scrub::scrub_prio_t with_priority) const
{
  return m_scrubber->scrub_requeue_priority(with_priority);
}

unsigned int PG::scrub_requeue_priority(Scrub::scrub_prio_t with_priority, unsigned int suggested_priority) const
{
  return m_scrubber->scrub_requeue_priority(with_priority, suggested_priority);
}

// ==========================================================================================
// SCRUB


Scrub::schedule_result_t PG::start_scrubbing(
    Scrub::OSDRestrictions osd_restrictions)
{
  dout(10) << fmt::format(
		  "{}: {}+{} (env restrictions:{})", __func__,
		  (is_active() ? "<active>" : "<not-active>"),
		  (is_clean() ? "<clean>" : "<not-clean>"), osd_restrictions)
	   << dendl;
  ceph_assert(ceph_mutex_is_locked(_lock));
  ceph_assert(m_scrubber);

  Scrub::ScrubPGPreconds pg_cond{};
  pg_cond.allow_shallow =
      !(get_osdmap()->test_flag(CEPH_OSDMAP_NOSCRUB) ||
	pool.info.has_flag(pg_pool_t::FLAG_NOSCRUB));
  pg_cond.allow_deep =
      !(get_osdmap()->test_flag(CEPH_OSDMAP_NODEEP_SCRUB) ||
	pool.info.has_flag(pg_pool_t::FLAG_NODEEP_SCRUB));
  pg_cond.has_deep_errors = (info.stats.stats.sum.num_deep_scrub_errors > 0);
  pg_cond.can_autorepair =
      (cct->_conf->osd_scrub_auto_repair &&
       get_pgbackend()->auto_repair_supported());

  return m_scrubber->start_scrub_session(
      osd_restrictions, pg_cond, m_planned_scrub);
}


double PG::next_deepscrub_interval() const
{
  double deep_scrub_interval =
    pool.info.opts.value_or(pool_opts_t::DEEP_SCRUB_INTERVAL, 0.0);
  if (deep_scrub_interval <= 0.0)
    deep_scrub_interval = cct->_conf->osd_deep_scrub_interval;
  return info.history.last_deep_scrub_stamp + deep_scrub_interval;
}
void PG::on_scrub_schedule_input_change()
{
  if (is_active() && is_primary()) {
    dout(20) << __func__ << ": active/primary" << dendl;
    ceph_assert(m_scrubber);
    m_scrubber->update_scrub_job(m_planned_scrub);
  } else {
    dout(20) << __func__ << ": inactive or non-primary" << dendl;
  }
}

void PG::scrub_requested(scrub_level_t scrub_level, scrub_type_t scrub_type)
{
  ceph_assert(m_scrubber);
  std::ignore =
      m_scrubber->scrub_requested(scrub_level, scrub_type, m_planned_scrub);
}

void PG::clear_ready_to_merge() {
  osd->clear_ready_to_merge(this);
}

void PG::queue_want_pg_temp(const vector<int> &wanted) {
  osd->queue_want_pg_temp(get_pgid().pgid, wanted);
}

void PG::clear_want_pg_temp() {
  osd->remove_want_pg_temp(get_pgid().pgid);
}

void PG::on_role_change() {
  requeue_ops(waiting_for_peered);
  plpg_on_role_change();
}

void PG::on_new_interval()
{
  projected_last_update = eversion_t();
  cancel_recovery();
  m_scrubber->on_new_interval();
}

epoch_t PG::cluster_osdmap_trim_lower_bound() {
  return osd->get_superblock().cluster_osdmap_trim_lower_bound;
}

OstreamTemp PG::get_clog_info() {
  return osd->clog->info();
}

OstreamTemp PG::get_clog_debug() {
  return osd->clog->debug();
}

OstreamTemp PG::get_clog_error() {
  return osd->clog->error();
}

void PG::schedule_event_after(
  PGPeeringEventRef event,
  float delay) {
  std::lock_guard lock(osd->recovery_request_lock);
  osd->recovery_request_timer.add_event_after(
    delay,
    new QueuePeeringEvt(
      this,
      std::move(event)));
}

void PG::request_local_background_io_reservation(
  unsigned priority,
  PGPeeringEventURef on_grant,
  PGPeeringEventURef on_preempt) {
  osd->local_reserver.request_reservation(
    pg_id,
    on_grant ? new QueuePeeringEvt(
      this, std::move(on_grant)) : nullptr,
    priority,
    on_preempt ? new QueuePeeringEvt(
      this, std::move(on_preempt)) : nullptr);
}

void PG::update_local_background_io_priority(
  unsigned priority) {
  osd->local_reserver.update_priority(
    pg_id,
    priority);
}

void PG::cancel_local_background_io_reservation() {
  osd->local_reserver.cancel_reservation(
    pg_id);
}

void PG::request_remote_recovery_reservation(
  unsigned priority,
  PGPeeringEventURef on_grant,
  PGPeeringEventURef on_preempt) {
  osd->remote_reserver.request_reservation(
    pg_id,
    on_grant ? new QueuePeeringEvt(
      this, std::move(on_grant)) : nullptr,
    priority,
    on_preempt ? new QueuePeeringEvt(
      this, std::move(on_preempt)) : nullptr);
}

void PG::cancel_remote_recovery_reservation() {
  osd->remote_reserver.cancel_reservation(
    pg_id);
}

void PG::schedule_event_on_commit(
  ObjectStore::Transaction &t,
  PGPeeringEventRef on_commit)
{
  t.register_on_commit(new QueuePeeringEvt(this, on_commit));
}

void PG::on_activate(interval_set<snapid_t> snaps)
{
  ceph_assert(!m_scrubber->are_callbacks_pending());
  ceph_assert(callbacks_for_degraded_object.empty());
  snap_trimq = snaps;
  release_pg_backoffs();
  projected_last_update = info.last_update;
}

void PG::on_replica_activate()
{
  m_scrubber->on_replica_activate();
}

void PG::on_active_exit()
{
  backfill_reserving = false;
  agent_stop();
}

Context* PG::on_clean()
{
  if (is_active()) {
    kick_snap_trim();
  }
  m_scrubber->on_primary_active_clean();
  requeue_ops(waiting_for_clean_to_primary_repair);
  return finish_recovery();
}

void PG::on_active_advmap(const OSDMapRef &osdmap)
{
  const auto& new_removed_snaps = osdmap->get_new_removed_snaps();
  auto i = new_removed_snaps.find(get_pgid().pool());
  if (i != new_removed_snaps.end()) {
    bool bad = false;
    for (auto j : i->second) {
      if (snap_trimq.intersects(j.first, j.second)) {
	decltype(snap_trimq) added, overlap;
	added.insert(j.first, j.second);
	overlap.intersection_of(snap_trimq, added);
	derr << __func__ << " removed_snaps already contains "
	     << overlap << dendl;
	bad = true;
	snap_trimq.union_of(added);
      } else {
	snap_trimq.insert(j.first, j.second);
      }
    }
    dout(10) << __func__ << " new removed_snaps " << i->second
	     << ", snap_trimq now " << snap_trimq << dendl;
    ceph_assert(!bad || !cct->_conf->osd_debug_verify_cached_snaps);
  }

  const auto& new_purged_snaps = osdmap->get_new_purged_snaps();
  auto j = new_purged_snaps.find(get_pgid().pgid.pool());
  if (j != new_purged_snaps.end()) {
    bool bad = false;
    for (auto k : j->second) {
      if (!recovery_state.get_info().purged_snaps.contains(k.first, k.second)) {
	interval_set<snapid_t> rm, overlap;
	rm.insert(k.first, k.second);
	overlap.intersection_of(recovery_state.get_info().purged_snaps, rm);
	derr << __func__ << " purged_snaps does not contain "
	     << rm << ", only " << overlap << dendl;
	recovery_state.adjust_purged_snaps(
	  [&overlap](auto &purged_snaps) {
	    purged_snaps.subtract(overlap);
	  });
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
	recovery_state.adjust_purged_snaps(
	  [&k](auto &purged_snaps) {
	    purged_snaps.erase(k.first, k.second);
	  });
      }
    }
    dout(10) << __func__ << " new purged_snaps " << j->second
	     << ", now " << recovery_state.get_info().purged_snaps << dendl;
    ceph_assert(!bad || !cct->_conf->osd_debug_verify_cached_snaps);
  }
}

void PG::queue_snap_retrim(snapid_t snap)
{
  if (!is_active() ||
      !is_primary()) {
    dout(10) << __func__ << " snap " << snap << " - not active and primary"
	     << dendl;
    return;
  }
  if (!snap_trimq.contains(snap)) {
    snap_trimq.insert(snap);
    snap_trimq_repeat.insert(snap);
    dout(20) << __func__ << " snap " << snap
	     << ", trimq now " << snap_trimq
	     << ", repeat " << snap_trimq_repeat << dendl;
    kick_snap_trim();
  } else {
    dout(20) << __func__ << " snap " << snap
	     << " already in trimq " << snap_trimq << dendl;
  }
}

void PG::on_active_actmap()
{
  if (cct->_conf->osd_check_for_log_corruption)
    check_log_for_corruption(osd->store);


  if (recovery_state.is_active()) {
    dout(10) << "Active: kicking snap trim" << dendl;
    kick_snap_trim();
  }

  if (recovery_state.is_peered() &&
      !recovery_state.is_clean() &&
      !recovery_state.get_osdmap()->test_flag(CEPH_OSDMAP_NOBACKFILL) &&
      (!recovery_state.get_osdmap()->test_flag(CEPH_OSDMAP_NOREBALANCE) ||
       recovery_state.is_degraded())) {
    queue_recovery();
  }
}

void PG::on_backfill_reserved()
{
  backfill_reserving = false;
  queue_recovery();
}

void PG::on_backfill_canceled()
{
  if (!waiting_on_backfill.empty()) {
    waiting_on_backfill.clear();
    finish_recovery_op(hobject_t::get_max());
  }
}

void PG::on_recovery_reserved()
{
  queue_recovery();
}

void PG::set_not_ready_to_merge_target(pg_t pgid, pg_t src)
{
  osd->set_not_ready_to_merge_target(pgid, src);
}

void PG::set_not_ready_to_merge_source(pg_t pgid)
{
  osd->set_not_ready_to_merge_source(pgid);
}

void PG::set_ready_to_merge_target(eversion_t lu, epoch_t les, epoch_t lec)
{
  osd->set_ready_to_merge_target(this, lu, les, lec);
}

void PG::set_ready_to_merge_source(eversion_t lu)
{
  osd->set_ready_to_merge_source(this, lu);
}

void PG::send_pg_created(pg_t pgid)
{
  osd->send_pg_created(pgid);
}

ceph::signedspan PG::get_mnow() const
{
  return osd->get_mnow();
}

HeartbeatStampsRef PG::get_hb_stamps(int peer)
{
  return osd->get_hb_stamps(peer);
}

void PG::schedule_renew_lease(epoch_t lpr, ceph::timespan delay)
{
  auto spgid = info.pgid;
  auto o = osd;
  osd->mono_timer.add_event(
    delay,
    [o, lpr, spgid]() {
      o->queue_renew_lease(lpr, spgid);
    });
}

void PG::queue_check_readable(epoch_t lpr, ceph::timespan delay)
{
  osd->queue_check_readable(info.pgid, lpr, delay);
}

void PG::rebuild_missing_set_with_deletes(PGLog &pglog)
{
  pglog.rebuild_missing_set_with_deletes(
    osd->store,
    ch,
    recovery_state.get_info());
}

void PG::on_activate_committed()
{
  if (!is_primary()) {
    // waiters
    if (recovery_state.needs_flush() == 0) {
      requeue_ops(waiting_for_peered);
    } else if (!waiting_for_peered.empty()) {
      dout(10) << __func__ << " flushes in progress, moving "
	       << waiting_for_peered.size() << " items to waiting_for_flush"
	       << dendl;
      ceph_assert(waiting_for_flush.empty());
      waiting_for_flush.swap(waiting_for_peered);
    }
  }
}

// Compute pending backfill data
static int64_t pending_backfill(CephContext *cct, int64_t bf_bytes, int64_t local_bytes)
{
  lgeneric_dout(cct, 20) << __func__ << " Adjust local usage "
			 << (local_bytes >> 10) << "KiB"
			 << " primary usage " << (bf_bytes >> 10)
			 << "KiB" << dendl;

  return std::max((int64_t)0, bf_bytes - local_bytes);
}


// We can zero the value of primary num_bytes as just an atomic.
// However, setting above zero reserves space for backfill and requires
// the OSDService::stat_lock which protects all OSD usage
bool PG::try_reserve_recovery_space(
  int64_t primary_bytes, int64_t local_bytes) {
  // Use tentative_bacfill_full() to make sure enough
  // space is available to handle target bytes from primary.

  // TODO: If we passed num_objects from primary we could account for
  // an estimate of the metadata overhead.

  // TODO: If we had compressed_allocated and compressed_original from primary
  // we could compute compression ratio and adjust accordingly.

  // XXX: There is no way to get omap overhead and this would only apply
  // to whatever possibly different partition that is storing the database.

  // update_osd_stat() from heartbeat will do this on a new
  // statfs using ps->primary_bytes.
  uint64_t pending_adjustment = 0;
  if (primary_bytes) {
    // For erasure coded pool overestimate by a full stripe per object
    // because we don't know how each objected rounded to the nearest stripe
    if (pool.info.is_erasure()) {
      primary_bytes /= (int)get_pgbackend()->get_ec_data_chunk_count();
      primary_bytes += get_pgbackend()->get_ec_stripe_chunk_size() *
	info.stats.stats.sum.num_objects;
      local_bytes /= (int)get_pgbackend()->get_ec_data_chunk_count();
      local_bytes += get_pgbackend()->get_ec_stripe_chunk_size() *
	info.stats.stats.sum.num_objects;
    }
    pending_adjustment = pending_backfill(
      cct,
      primary_bytes,
      local_bytes);
    dout(10) << __func__ << " primary_bytes " << (primary_bytes >> 10)
	     << "KiB"
	     << " local " << (local_bytes >> 10) << "KiB"
	     << " pending_adjustments " << (pending_adjustment >> 10) << "KiB"
	     << dendl;
  }

  // This lock protects not only the stats OSDService but also setting the
  // pg primary_bytes.  That's why we don't immediately unlock
  std::lock_guard l{osd->stat_lock};
  osd_stat_t cur_stat = osd->osd_stat;
  if (cct->_conf->osd_debug_reject_backfill_probability > 0 &&
      (rand()%1000 < (cct->_conf->osd_debug_reject_backfill_probability*1000.0))) {
    dout(10) << "backfill reservation rejected: failure injection"
	     << dendl;
    return false;
  } else if (!cct->_conf->osd_debug_skip_full_check_in_backfill_reservation &&
      osd->tentative_backfill_full(this, pending_adjustment, cur_stat)) {
    dout(10) << "backfill reservation rejected: backfill full"
	     << dendl;
    return false;
  } else {
    // Don't reserve space if skipped reservation check, this is used
    // to test the other backfill full check AND in case a corruption
    // of num_bytes requires ignoring that value and trying the
    // backfill anyway.
    if (primary_bytes &&
	!cct->_conf->osd_debug_skip_full_check_in_backfill_reservation) {
      primary_num_bytes.store(primary_bytes);
      local_num_bytes.store(local_bytes);
    } else {
      unreserve_recovery_space();
    }
    return true;
  }
}

void PG::unreserve_recovery_space() {
  primary_num_bytes.store(0);
  local_num_bytes.store(0);
}

void PG::_scan_rollback_obs(const vector<ghobject_t> &rollback_obs)
{
  ObjectStore::Transaction t;
  eversion_t trimmed_to = recovery_state.get_last_rollback_info_trimmed_to_applied();
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


void PG::forward_scrub_event(ScrubAPI fn, epoch_t epoch_queued, std::string_view desc)
{
  dout(20) << __func__ << ": " << desc << " queued at: " << epoch_queued << dendl;
  ceph_assert(m_scrubber);
  if (is_active()) {
    ((*m_scrubber).*fn)(epoch_queued);
  } else {
    // pg might be in the process of being deleted
    dout(5) << __func__ << " refusing to forward. " << (is_clean() ? "(clean) " : "(not clean) ") <<
	      (is_active() ? "(active) " : "(not active) ") <<  dendl;
  }
}

void PG::forward_scrub_event(ScrubSafeAPI fn,
			     epoch_t epoch_queued,
			     Scrub::act_token_t act_token,
			     std::string_view desc)
{
  dout(20) << __func__ << ": " << desc << " queued: " << epoch_queued
	   << " token: " << act_token << dendl;
  ceph_assert(m_scrubber);
  if (is_active()) {
    ((*m_scrubber).*fn)(epoch_queued, act_token);
  } else {
    // pg might be in the process of being deleted
    dout(5) << __func__ << " refusing to forward. "
	    << (is_clean() ? "(clean) " : "(not clean) ")
	    << (is_active() ? "(active) " : "(not active) ") << dendl;
  }
}

void PG::replica_scrub(OpRequestRef op, ThreadPool::TPHandle& handle)
{
  dout(10) << __func__ << " (op)" << dendl;
  ceph_assert(m_scrubber);
  m_scrubber->replica_scrub_op(op);
}

void PG::replica_scrub(epoch_t epoch_queued,
		       Scrub::act_token_t act_token,
		       [[maybe_unused]] ThreadPool::TPHandle& handle)
{
  dout(10) << __func__ << " queued at: " << epoch_queued
	   << (is_primary() ? " (primary)" : " (replica)") << dendl;
  forward_scrub_event(&ScrubPgIF::send_start_replica, epoch_queued, act_token,
		      "StartReplica/nw");
}

bool PG::ops_blocked_by_scrub() const
{
  return !waiting_for_scrub.empty();
}

Scrub::scrub_prio_t PG::is_scrub_blocking_ops() const
{
  return waiting_for_scrub.empty() ? Scrub::scrub_prio_t::low_priority
				   : Scrub::scrub_prio_t::high_priority;
}

bool PG::old_peering_msg(epoch_t reply_epoch, epoch_t query_epoch)
{
  if (auto last_reset = get_last_peering_reset();
      last_reset > reply_epoch || last_reset > query_epoch) {
    dout(10) << "old_peering_msg reply_epoch " << reply_epoch << " query_epoch "
	     << query_epoch << " last_peering_reset " << last_reset << dendl;
    return true;
  }
  return false;
}

struct FlushState {
  PGRef pg;
  epoch_t epoch;
  FlushState(PG *pg, epoch_t epoch) : pg(pg), epoch(epoch) {}
  ~FlushState() {
    std::scoped_lock l{*pg};
    if (!pg->pg_has_reset_since(epoch)) {
      pg->recovery_state.complete_flush();
    }
  }
};
typedef std::shared_ptr<FlushState> FlushStateRef;

void PG::start_flush_on_transaction(ObjectStore::Transaction &t)
{
  // flush in progress ops
  FlushStateRef flush_trigger (std::make_shared<FlushState>(
                               this, get_osdmap_epoch()));
  t.register_on_applied(new ContainerContext<FlushStateRef>(flush_trigger));
  t.register_on_commit(new ContainerContext<FlushStateRef>(flush_trigger));
}

bool PG::try_flush_or_schedule_async()
{
  Context *c = new QueuePeeringEvt(
    this, get_osdmap_epoch(), PeeringState::IntervalFlush());
  if (!ch->flush_commit(c)) {
    return false;
  } else {
    delete c;
    return true;
  }
}

ostream& operator<<(ostream& out, const PG& pg)
{
  out << pg.recovery_state;

  // listing all scrub-related flags - both current and "planned next scrub"
  if (pg.is_scrubbing()) {
    out << *pg.m_scrubber;
  }
  out << pg.m_planned_scrub;

  if (pg.recovery_ops_active)
    out << " rops=" << pg.recovery_ops_active;

  //out << " (" << pg.pg_log.get_tail() << "," << pg.pg_log.get_head() << "]";
  if (pg.recovery_state.have_missing()) {
    out << " m=" << pg.recovery_state.get_num_missing();
    if (pg.is_primary()) {
      uint64_t unfound = pg.recovery_state.get_num_unfound();
      if (unfound)
	out << " u=" << unfound;
    }
  }
  if (!pg.is_clean()) {
    out << " mbc=" << pg.recovery_state.get_missing_by_count();
  }
  if (!pg.snap_trimq.empty()) {
    out << " trimq=";
    // only show a count if the set is large
    if (pg.snap_trimq.num_intervals() > 16) {
      out << pg.snap_trimq.size();
      if (!pg.snap_trimq_repeat.empty()) {
	out << "(" << pg.snap_trimq_repeat.size() << ")";
      }
    } else {
      out << pg.snap_trimq;
      if (!pg.snap_trimq_repeat.empty()) {
	out << "(" << pg.snap_trimq_repeat << ")";
      }
    }
  }
  if (!pg.recovery_state.get_info().purged_snaps.empty()) {
    out << " ps="; // snap trim queue / purged snaps
    if (pg.recovery_state.get_info().purged_snaps.num_intervals() > 16) {
      out << pg.recovery_state.get_info().purged_snaps.size();
    } else {
      out << pg.recovery_state.get_info().purged_snaps;
    }
  }

  out << "]";
  return out;
}

bool PG::can_discard_op(OpRequestRef& op)
{
  auto m = op->get_req<MOSDOp>();
  if (cct->_conf->osd_discard_disconnected_ops && OSD::op_is_discardable(m)) {
    dout(20) << " discard " << *m << dendl;
    return true;
  }

  if (m->get_map_epoch() < info.history.same_primary_since) {
    dout(7) << " changed after " << m->get_map_epoch()
	    << ", dropping " << *m << dendl;
    return true;
  }

  if ((m->get_flags() & (CEPH_OSD_FLAG_BALANCE_READS |
			 CEPH_OSD_FLAG_LOCALIZE_READS)) &&
      !is_primary() &&
      m->get_map_epoch() < info.history.same_interval_since) {
    // Note: the Objecter will resend on interval change without the primary
    // changing if it actually sent to a replica.  If the primary hasn't
    // changed since the send epoch, we got it, and we're primary, it won't
    // have resent even if the interval did change as it sent it to the primary
    // (us).
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
  auto m = op->get_req<T>();
  ceph_assert(m->get_type() == MSGTYPE);

  int from = m->get_source().num();

  // if a repop is replied after a replica goes down in a new osdmap, and
  // before the pg advances to this new osdmap, the repop replies before this
  // repop can be discarded by that replica OSD, because the primary resets the
  // connection to it when handling the new osdmap marking it down, and also
  // resets the messenger sesssion when the replica reconnects. to avoid the
  // out-of-order replies, the messages from that replica should be discarded.
  OSDMapRef next_map = osd->get_next_osdmap();
  if (next_map->is_down(from)) {
    dout(20) << " " << __func__ << " dead for nextmap is down " << from << dendl;
    return true;
  }
  /* Mostly, this overlaps with the old_peering_msg
   * condition.  An important exception is pushes
   * sent by replicas not in the acting set, since
   * if such a replica goes down it does not cause
   * a new interval. */
  if (next_map->get_down_at(from) >= m->map_epoch) {
    dout(20) << " " << __func__ << " dead for 'get_down_at' " << from << dendl;
    return true;
  }

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
  auto m = op->get_req<MOSDPGScan>();
  ceph_assert(m->get_type() == MSG_OSD_PG_SCAN);

  if (old_peering_msg(m->map_epoch, m->query_epoch)) {
    dout(10) << " got old scan, ignoring" << dendl;
    return true;
  }
  return false;
}

bool PG::can_discard_backfill(OpRequestRef op)
{
  auto m = op->get_req<MOSDPGBackfill>();
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

void PG::do_peering_event(PGPeeringEventRef evt, PeeringCtx &rctx)
{
  dout(10) << __func__ << ": " << evt->get_desc() << dendl;
  ceph_assert(have_same_or_newer_map(evt->get_epoch_sent()));
  if (old_peering_evt(evt)) {
    dout(10) << "discard old " << evt->get_desc() << dendl;
  } else {
    recovery_state.handle_event(evt, &rctx);
  }
  // write_if_dirty regardless of path above to ensure we capture any work
  // done by OSD::advance_pg().
  write_if_dirty(rctx.transaction);
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

void PG::find_unfound(epoch_t queued, PeeringCtx &rctx)
{
  /*
    * if we couldn't start any recovery ops and things are still
    * unfound, see if we can discover more missing object locations.
    * It may be that our initial locations were bad and we errored
    * out while trying to pull.
    */
  if (!recovery_state.discover_all_missing(rctx)) {
    string action;
    if (state_test(PG_STATE_BACKFILLING)) {
      auto evt = PGPeeringEventRef(
	new PGPeeringEvent(
	  queued,
	  queued,
	  PeeringState::UnfoundBackfill()));
      queue_peering_event(evt);
      action = "in backfill";
    } else if (state_test(PG_STATE_RECOVERING)) {
      auto evt = PGPeeringEventRef(
	new PGPeeringEvent(
	  queued,
	  queued,
	  PeeringState::UnfoundRecovery()));
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
  PeeringCtx &rctx)
{
  dout(10) << __func__ << ": " << osdmap->get_epoch() << dendl;
  osd_shard->update_pg_epoch(pg_slot, osdmap->get_epoch());
  recovery_state.advance_map(
    osdmap,
    lastmap,
    newup,
    up_primary,
    newacting,
    acting_primary,
    rctx);
}

void PG::handle_activate_map(PeeringCtx &rctx, epoch_t range_starts_at)
{
  dout(10) << fmt::format("{}: epoch range: {}..{}", __func__, range_starts_at,
                          get_osdmap()->get_epoch())
           << dendl;
  recovery_state.activate_map(rctx);
  requeue_map_waiters();

  // If pool.info changed during this sequence of map updates, invoke
  // on_scrub_schedule_input_change() as pool.info contains scrub scheduling
  // parameters.
  if (pool.info.last_change >= range_starts_at) {
    on_scrub_schedule_input_change();
  }
}

void PG::handle_initialize(PeeringCtx &rctx)
{
  dout(10) << __func__ << dendl;
  PeeringState::Initialize evt;
  recovery_state.handle_event(evt, &rctx);
}


void PG::handle_query_state(Formatter *f)
{
  dout(10) << "handle_query_state" << dendl;
  PeeringState::QueryState q(f);
  recovery_state.handle_event(q, 0);
}

void PG::init_collection_pool_opts()
{
  auto r = osd->store->set_collection_opts(ch, pool.info.opts);
  if (r < 0 && r != -EOPNOTSUPP) {
    derr << __func__ << " set_collection_opts returns error:" << r << dendl;
  }
}

void PG::on_pool_change()
{
  init_collection_pool_opts();
  plpg_on_pool_change();
}

void PG::C_DeleteMore::complete(int r) {
  ceph_assert(r == 0);
  pg->lock();
  if (!pg->pg_has_reset_since(epoch)) {
    pg->osd->queue_for_pg_delete(pg->get_pgid(), epoch,
	                         num_objects);
  }
  pg->unlock();
  delete this;
}

std::pair<ghobject_t, bool> PG::do_delete_work(
  ObjectStore::Transaction &t,
  ghobject_t _next)
{
  dout(10) << __func__ << dendl;

  {
    float osd_delete_sleep = osd->osd->get_osd_delete_sleep();
    if (osd_delete_sleep > 0 && delete_needs_sleep) {
      epoch_t e = get_osdmap()->get_epoch();
      PGRef pgref(this);
      auto delete_requeue_callback = new LambdaContext([this, pgref, e](int r) {
        dout(20) << "do_delete_work() [cb] wake up at "
                 << ceph_clock_now()
	         << ", re-queuing delete" << dendl;
        std::scoped_lock locker{*this};
        delete_needs_sleep = false;
        if (!pg_has_reset_since(e)) {
	  // We pass 1 for num_objects here as only wpq uses this code path
	  // and it will be ignored
          osd->queue_for_pg_delete(get_pgid(), e, 1);
        }
      });

      auto delete_schedule_time = ceph::real_clock::now();
      delete_schedule_time += ceph::make_timespan(osd_delete_sleep);
      std::lock_guard l{osd->sleep_lock};
      osd->sleep_timer.add_event_at(delete_schedule_time,
				    delete_requeue_callback);
      dout(20) << __func__ << " Delete scheduled at " << delete_schedule_time << dendl;
      return std::make_pair(_next, true);
    }
  }

  delete_needs_sleep = true;

  ghobject_t next;

  vector<ghobject_t> olist;
  int max = std::min(osd->store->get_ideal_list_max(),
		     (int)cct->_conf->osd_target_transaction_size);

  osd->store->collection_list(
    ch,
    _next,
    ghobject_t::get_max(),
    max,
    &olist,
    &next);
  dout(20) << __func__ << " " << olist << dendl;

  // make sure we've removed everything
  // by one more listing from the beginning
  if (_next != ghobject_t() && olist.empty()) {
    next = ghobject_t();
    osd->store->collection_list(
      ch,
      next,
      ghobject_t::get_max(),
      max,
      &olist,
      &next);
    for (auto& oid : olist) {
      if (oid == pgmeta_oid) {
        dout(20) << __func__ << " removing pgmeta object " << oid << dendl;
      } else {
        dout(0) << __func__ << " additional unexpected onode"
                <<" new onode has appeared since PG removal started"
                << oid << dendl;
      }
    }
  }

  OSDriver::OSTransaction _t(osdriver.get_transaction(&t));
  int64_t num = 0;
  for (auto& oid : olist) {
    if (oid == pgmeta_oid) {
      continue;
    }
    if (oid.is_pgmeta()) {
      osd->clog->warn() << info.pgid << " found stray pgmeta-like " << oid
			<< " during PG removal";
    }
    int r = snap_mapper.remove_oid(oid.hobj, &_t);
    if (r != 0 && r != -ENOENT) {
      ceph_abort();
    }
    t.remove(coll, oid);
    ++num;
  }
  bool running = true;
  if (num) {
    dout(20) << __func__ << " deleting " << num << " objects" << dendl;
    Context *fin = new C_DeleteMore(this, get_osdmap_epoch(), num);
    t.register_on_commit(fin);
  } else {
    if (cct->_conf->osd_inject_failure_on_pg_removal) {
      _exit(1);
    }

    // final flush here to ensure completions drop refs.  Of particular concern
    // are the SnapMapper ContainerContexts.
    {
      PGRef pgref(this);
      PGLog::clear_info_log(info.pgid, &t);
      t.remove_collection(coll);
      t.register_on_commit(new ContainerContext<PGRef>(pgref));
      t.register_on_applied(new ContainerContext<PGRef>(pgref));
      osd->store->queue_transaction(ch, std::move(t));
    }
    ch->flush();

    if (!osd->try_finish_pg_delete(this, pool.info.get_pg_num())) {
      dout(1) << __func__ << " raced with merge, reinstantiating" << dendl;
      ch = osd->store->create_new_collection(coll);
      create_pg_collection(t,
	      info.pgid,
	      info.pgid.get_split_bits(pool.info.get_pg_num()));
      init_pg_ondisk(t, info.pgid, &pool.info);
      recovery_state.reset_last_persisted();
    } else {
      recovery_state.set_delete_complete();

      // cancel reserver here, since the PG is about to get deleted and the
      // exit() methods don't run when that happens.
      osd->local_reserver.cancel_reservation(info.pgid);

      running = false;
    }
  }
  return {next, running};
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

void PG::dump_pgstate_history(Formatter *f)
{
  std::scoped_lock l{*this};
  recovery_state.dump_history(f);
}

void PG::dump_missing(Formatter *f)
{
  for (auto& i : recovery_state.get_pg_log().get_missing().get_items()) {
    f->open_object_section("object");
    f->dump_object("oid", i.first);
    f->dump_object("missing_info", i.second);
    if (recovery_state.get_missing_loc().needs_recovery(i.first)) {
      f->dump_bool(
	"unfound",
	recovery_state.get_missing_loc().is_unfound(i.first));
      f->open_array_section("locations");
      for (auto l : recovery_state.get_missing_loc().get_locations(i.first)) {
	f->dump_object("shard", l);
      }
      f->close_section();
    }
    f->close_section();
  }
}

void PG::with_pg_stats(ceph::coarse_real_clock::time_point now_is,
		       std::function<void(const pg_stat_t&, epoch_t lec)>&& f)
{
  dout(30) << __func__ << dendl;
  // possibly update the scrub state & timers
  lock();
  if (m_scrubber) {
    m_scrubber->update_scrub_stats(now_is);
  }
  unlock();

  // now - the actual publishing
  std::lock_guard l{pg_stats_publish_lock};
  if (pg_stats_publish) {
    f(*pg_stats_publish, pg_stats_publish->get_effective_last_epoch_clean());
  }
}

void PG::with_heartbeat_peers(std::function<void(int)>&& f)
{
  std::lock_guard l{heartbeat_peer_lock};
  for (auto p : heartbeat_peers) {
    f(p);
  }
  for (auto p : probe_targets) {
    f(p);
  }
}

uint64_t PG::get_min_alloc_size() const {
  return osd->store->get_min_alloc_size();
}

PGLockWrapper::~PGLockWrapper()
{
  if (m_pg) {
    // otherwise - we were 'moved from'
    m_pg->unlock();
  }
}
