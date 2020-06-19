// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "boost/tuple/tuple.hpp"
#include "boost/intrusive_ptr.hpp"
#include "PG.h"
#include "PrimaryLogPG.h"
#include "OSD.h"
#include "OpRequest.h"
#include "ScrubStore.h"
#include "Session.h"
#include "objclass/objclass.h"

#include "cls/cas/cls_cas_ops.h"
#include "common/ceph_crypto.h"
#include "common/errno.h"
#include "common/scrub_types.h"
#include "common/perf_counters.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDBackoff.h"
#include "messages/MOSDPGTrim.h"
#include "messages/MOSDPGScan.h"
#include "messages/MOSDRepScrub.h"
#include "messages/MOSDPGBackfill.h"
#include "messages/MOSDPGBackfillRemove.h"
#include "messages/MOSDPGUpdateLogMissing.h"
#include "messages/MOSDPGUpdateLogMissingReply.h"
#include "messages/MCommandReply.h"
#include "messages/MOSDScrubReserve.h"
#include "common/EventTrace.h"

#include "common/config.h"
#include "include/compat.h"
#include "mon/MonClient.h"
#include "osdc/Objecter.h"
#include "json_spirit/json_spirit_value.h"
#include "json_spirit/json_spirit_reader.h"
#include "include/ceph_assert.h"  // json_spirit clobbers it
#include "include/rados/rados_types.hpp"

#ifdef WITH_LTTNG
#include "tracing/osd.h"
#else
#define tracepoint(...)
#endif

#define dout_context cct
#define dout_subsys ceph_subsys_osd
#define DOUT_PREFIX_ARGS this, osd->whoami, get_osdmap()
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

#include <sstream>
#include <utility>

#include <errno.h>

MEMPOOL_DEFINE_OBJECT_FACTORY(PrimaryLogPG, replicatedpg, osd);

using std::list;
using std::ostream;
using std::pair;
using std::make_pair;
using std::map;
using std::ostringstream;
using std::set;
using std::string;
using std::string_view;
using std::stringstream;
using std::unique_ptr;
using std::vector;

using ceph::bufferlist;
using ceph::bufferptr;
using ceph::Formatter;
using ceph::decode;
using ceph::decode_noclear;
using ceph::encode;
using ceph::encode_destructively;

using namespace ceph::osd::scheduler;
using TOPNSPC::common::cmd_getval;

template <typename T>
static ostream& _prefix(std::ostream *_dout, T *pg) {
  return pg->gen_prefix(*_dout);
}

/**
 * The CopyCallback class defines an interface for completions to the
 * copy_start code. Users of the copy infrastructure must implement
 * one and give an instance of the class to start_copy.
 *
 * The implementer is responsible for making sure that the CopyCallback
 * can associate itself with the correct copy operation.
 */
class PrimaryLogPG::CopyCallback : public GenContext<CopyCallbackResults> {
protected:
  CopyCallback() {}
  /**
   * results.get<0>() is the return code: 0 for success; -ECANCELED if
   * the operation was cancelled by the local OSD; -errno for other issues.
   * results.get<1>() is a pointer to a CopyResults object, which you are
   * responsible for deleting.
   */
  void finish(CopyCallbackResults results_) override = 0;

public:
  /// Provide the final size of the copied object to the CopyCallback
  ~CopyCallback() override {}
};

template <typename T>
class PrimaryLogPG::BlessedGenContext : public GenContext<T> {
  PrimaryLogPGRef pg;
  unique_ptr<GenContext<T>> c;
  epoch_t e;
public:
  BlessedGenContext(PrimaryLogPG *pg, GenContext<T> *c, epoch_t e)
    : pg(pg), c(c), e(e) {}
  void finish(T t) override {
    std::scoped_lock locker{*pg};
    if (pg->pg_has_reset_since(e))
      c.reset();
    else
      c.release()->complete(t);
  }
  bool sync_finish(T t) {
    // we assume here all blessed/wrapped Contexts can complete synchronously.
    c.release()->complete(t);
    return true;
  }
};

GenContext<ThreadPool::TPHandle&> *PrimaryLogPG::bless_gencontext(
  GenContext<ThreadPool::TPHandle&> *c) {
  return new BlessedGenContext<ThreadPool::TPHandle&>(
    this, c, get_osdmap_epoch());
}

template <typename T>
class PrimaryLogPG::UnlockedBlessedGenContext : public GenContext<T> {
  PrimaryLogPGRef pg;
  unique_ptr<GenContext<T>> c;
  epoch_t e;
public:
  UnlockedBlessedGenContext(PrimaryLogPG *pg, GenContext<T> *c, epoch_t e)
    : pg(pg), c(c), e(e) {}
  void finish(T t) override {
    if (pg->pg_has_reset_since(e))
      c.reset();
    else
      c.release()->complete(t);
  }
  bool sync_finish(T t) {
    // we assume here all blessed/wrapped Contexts can complete synchronously.
    c.release()->complete(t);
    return true;
  }
};

GenContext<ThreadPool::TPHandle&> *PrimaryLogPG::bless_unlocked_gencontext(
  GenContext<ThreadPool::TPHandle&> *c) {
  return new UnlockedBlessedGenContext<ThreadPool::TPHandle&>(
    this, c, get_osdmap_epoch());
}

class PrimaryLogPG::BlessedContext : public Context {
  PrimaryLogPGRef pg;
  unique_ptr<Context> c;
  epoch_t e;
public:
  BlessedContext(PrimaryLogPG *pg, Context *c, epoch_t e)
    : pg(pg), c(c), e(e) {}
  void finish(int r) override {
    std::scoped_lock locker{*pg};
    if (pg->pg_has_reset_since(e))
      c.reset();
    else
      c.release()->complete(r);
  }
  bool sync_finish(int r) override {
    // we assume here all blessed/wrapped Contexts can complete synchronously.
    c.release()->complete(r);
    return true;
  }
};

Context *PrimaryLogPG::bless_context(Context *c) {
  return new BlessedContext(this, c, get_osdmap_epoch());
}

class PrimaryLogPG::C_PG_ObjectContext : public Context {
  PrimaryLogPGRef pg;
  ObjectContext *obc;
  public:
  C_PG_ObjectContext(PrimaryLogPG *p, ObjectContext *o) :
    pg(p), obc(o) {}
  void finish(int r) override {
    pg->object_context_destructor_callback(obc);
  }
};

struct OnReadComplete : public Context {
  PrimaryLogPG *pg;
  PrimaryLogPG::OpContext *opcontext;
  OnReadComplete(
    PrimaryLogPG *pg,
    PrimaryLogPG::OpContext *ctx) : pg(pg), opcontext(ctx) {}
  void finish(int r) override {
    opcontext->finish_read(pg);
  }
  ~OnReadComplete() override {}
};

class PrimaryLogPG::C_OSD_AppliedRecoveredObject : public Context {
  PrimaryLogPGRef pg;
  ObjectContextRef obc;
  public:
  C_OSD_AppliedRecoveredObject(PrimaryLogPG *p, ObjectContextRef o) :
    pg(p), obc(o) {}
  bool sync_finish(int r) override {
    pg->_applied_recovered_object(obc);
    return true;
  }
  void finish(int r) override {
    std::scoped_lock locker{*pg};
    pg->_applied_recovered_object(obc);
  }
};

class PrimaryLogPG::C_OSD_CommittedPushedObject : public Context {
  PrimaryLogPGRef pg;
  epoch_t epoch;
  eversion_t last_complete;
  public:
  C_OSD_CommittedPushedObject(
    PrimaryLogPG *p, epoch_t epoch, eversion_t lc) :
    pg(p), epoch(epoch), last_complete(lc) {
  }
  void finish(int r) override {
    pg->_committed_pushed_object(epoch, last_complete);
  }
};

class PrimaryLogPG::C_OSD_AppliedRecoveredObjectReplica : public Context {
  PrimaryLogPGRef pg;
  public:
  explicit C_OSD_AppliedRecoveredObjectReplica(PrimaryLogPG *p) :
    pg(p) {}
  bool sync_finish(int r) override {
    pg->_applied_recovered_object_replica();
    return true;
  }
  void finish(int r) override {
    std::scoped_lock locker{*pg};
    pg->_applied_recovered_object_replica();
  }
};

// OpContext
void PrimaryLogPG::OpContext::start_async_reads(PrimaryLogPG *pg)
{
  inflightreads = 1;
  list<pair<boost::tuple<uint64_t, uint64_t, unsigned>,
	    pair<bufferlist*, Context*> > > in;
  in.swap(pending_async_reads);
  pg->pgbackend->objects_read_async(
    obc->obs.oi.soid,
    in,
    new OnReadComplete(pg, this), pg->get_pool().fast_read);
}
void PrimaryLogPG::OpContext::finish_read(PrimaryLogPG *pg)
{
  ceph_assert(inflightreads > 0);
  --inflightreads;
  if (async_reads_complete()) {
    ceph_assert(pg->in_progress_async_reads.size());
    ceph_assert(pg->in_progress_async_reads.front().second == this);
    pg->in_progress_async_reads.pop_front();

    // Restart the op context now that all reads have been
    // completed. Read failures will be handled by the op finisher
    pg->execute_ctx(this);
  }
}

class CopyFromCallback : public PrimaryLogPG::CopyCallback {
public:
  PrimaryLogPG::CopyResults *results = nullptr;
  PrimaryLogPG::OpContext *ctx;
  OSDOp &osd_op;
  uint32_t truncate_seq;
  uint64_t truncate_size;
  bool have_truncate = false;

  CopyFromCallback(PrimaryLogPG::OpContext *ctx, OSDOp &osd_op)
    : ctx(ctx), osd_op(osd_op) {
  }
  ~CopyFromCallback() override {}

  void finish(PrimaryLogPG::CopyCallbackResults results_) override {
    results = results_.get<1>();
    int r = results_.get<0>();

    // Only use truncate_{seq,size} from the original object if the client
    // did not sent us these parameters
    if (!have_truncate) {
      truncate_seq = results->truncate_seq;
      truncate_size = results->truncate_size;
    }

    // for finish_copyfrom
    ctx->user_at_version = results->user_version;

    if (r >= 0) {
      ctx->pg->execute_ctx(ctx);
    } else {
      if (r != -ECANCELED) { // on cancel just toss it out; client resends
	if (ctx->op)
	  ctx->pg->osd->reply_op_error(ctx->op, r);
      } else if (results->should_requeue) {
	if (ctx->op)
	  ctx->pg->requeue_op(ctx->op);
      }
      ctx->pg->close_op_ctx(ctx);
    }
  }

  bool is_temp_obj_used() {
    return results->started_temp_obj;
  }
  uint64_t get_data_size() {
    return results->object_size;
  }
  void set_truncate(uint32_t seq, uint64_t size) {
    truncate_seq = seq;
    truncate_size = size;
    have_truncate = true;
  }
};

struct CopyFromFinisher : public PrimaryLogPG::OpFinisher {
  CopyFromCallback *copy_from_callback;

  explicit CopyFromFinisher(CopyFromCallback *copy_from_callback)
    : copy_from_callback(copy_from_callback) {
  }

  int execute() override {
    // instance will be destructed after this method completes
    copy_from_callback->ctx->pg->finish_copyfrom(copy_from_callback);
    return 0;
  }
};

// ======================
// PGBackend::Listener

void PrimaryLogPG::on_local_recover(
  const hobject_t &hoid,
  const ObjectRecoveryInfo &_recovery_info,
  ObjectContextRef obc,
  bool is_delete,
  ObjectStore::Transaction *t
  )
{
  dout(10) << __func__ << ": " << hoid << dendl;

  ObjectRecoveryInfo recovery_info(_recovery_info);
  clear_object_snap_mapping(t, hoid);
  if (!is_delete && recovery_info.soid.is_snap()) {
    OSDriver::OSTransaction _t(osdriver.get_transaction(t));
    set<snapid_t> snaps;
    dout(20) << " snapset " << recovery_info.ss << dendl;
    auto p = recovery_info.ss.clone_snaps.find(hoid.snap);
    if (p != recovery_info.ss.clone_snaps.end()) {
      snaps.insert(p->second.begin(), p->second.end());
      dout(20) << " snaps " << snaps << dendl;
      snap_mapper.add_oid(
	recovery_info.soid,
	snaps,
	&_t);
    } else {
      derr << __func__ << " " << hoid << " had no clone_snaps" << dendl;
    }
  }
  if (!is_delete && recovery_state.get_pg_log().get_missing().is_missing(recovery_info.soid) &&
      recovery_state.get_pg_log().get_missing().get_items().find(recovery_info.soid)->second.need > recovery_info.version) {
    ceph_assert(is_primary());
    const pg_log_entry_t *latest = recovery_state.get_pg_log().get_log().objects.find(recovery_info.soid)->second;
    if (latest->op == pg_log_entry_t::LOST_REVERT &&
	latest->reverting_to == recovery_info.version) {
      dout(10) << " got old revert version " << recovery_info.version
	       << " for " << *latest << dendl;
      recovery_info.version = latest->version;
      // update the attr to the revert event version
      recovery_info.oi.prior_version = recovery_info.oi.version;
      recovery_info.oi.version = latest->version;
      bufferlist bl;
      encode(recovery_info.oi, bl,
	       get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
      ceph_assert(!pool.info.is_erasure());
      t->setattr(coll, ghobject_t(recovery_info.soid), OI_ATTR, bl);
      if (obc)
	obc->attr_cache[OI_ATTR] = bl;
    }
  }

  // keep track of active pushes for scrub
  ++active_pushes;

  recovery_state.recover_got(
    recovery_info.soid,
    recovery_info.version,
    is_delete,
    *t);

  if (is_primary()) {
    if (!is_delete) {
      obc->obs.exists = true;

      bool got = obc->get_recovery_read();
      ceph_assert(got);

      ceph_assert(recovering.count(obc->obs.oi.soid));
      recovering[obc->obs.oi.soid] = obc;
      obc->obs.oi = recovery_info.oi;  // may have been updated above
    }

    t->register_on_applied(new C_OSD_AppliedRecoveredObject(this, obc));

    publish_stats_to_osd();
    release_backoffs(hoid);
    if (!is_unreadable_object(hoid)) {
      auto unreadable_object_entry = waiting_for_unreadable_object.find(hoid);
      if (unreadable_object_entry != waiting_for_unreadable_object.end()) {
	dout(20) << " kicking unreadable waiters on " << hoid << dendl;
	requeue_ops(unreadable_object_entry->second);
	waiting_for_unreadable_object.erase(unreadable_object_entry);
      }
    }
  } else {
    t->register_on_applied(
      new C_OSD_AppliedRecoveredObjectReplica(this));

  }

  t->register_on_commit(
    new C_OSD_CommittedPushedObject(
      this,
      get_osdmap_epoch(),
      info.last_complete));
}

void PrimaryLogPG::on_global_recover(
  const hobject_t &soid,
  const object_stat_sum_t &stat_diff,
  bool is_delete)
{
  recovery_state.object_recovered(soid, stat_diff);
  publish_stats_to_osd();
  dout(10) << "pushed " << soid << " to all replicas" << dendl;
  auto i = recovering.find(soid);
  ceph_assert(i != recovering.end());

  if (i->second && i->second->rwstate.recovery_read_marker) {
    // recover missing won't have had an obc, but it gets filled in
    // during on_local_recover
    ceph_assert(i->second);
    list<OpRequestRef> requeue_list;
    i->second->drop_recovery_read(&requeue_list);
    requeue_ops(requeue_list);
  }

  backfills_in_flight.erase(soid);

  recovering.erase(i);
  finish_recovery_op(soid);
  release_backoffs(soid);
  auto degraded_object_entry = waiting_for_degraded_object.find(soid);
  if (degraded_object_entry != waiting_for_degraded_object.end()) {
    dout(20) << " kicking degraded waiters on " << soid << dendl;
    requeue_ops(degraded_object_entry->second);
    waiting_for_degraded_object.erase(degraded_object_entry);
  }
  auto unreadable_object_entry = waiting_for_unreadable_object.find(soid);
  if (unreadable_object_entry != waiting_for_unreadable_object.end()) {
    dout(20) << " kicking unreadable waiters on " << soid << dendl;
    requeue_ops(unreadable_object_entry->second);
    waiting_for_unreadable_object.erase(unreadable_object_entry);
  }
  finish_degraded_object(soid);
}

void PrimaryLogPG::schedule_recovery_work(
  GenContext<ThreadPool::TPHandle&> *c)
{
  osd->queue_recovery_context(this, c);
}

void PrimaryLogPG::replica_clear_repop_obc(
  const vector<pg_log_entry_t> &logv,
  ObjectStore::Transaction &t)
{
  for (auto &&e: logv) {
    /* Have to blast all clones, they share a snapset */
    object_contexts.clear_range(
      e.soid.get_object_boundary(), e.soid.get_head());
    ceph_assert(
      snapset_contexts.find(e.soid.get_head()) ==
      snapset_contexts.end());
  }
}

bool PrimaryLogPG::should_send_op(
  pg_shard_t peer,
  const hobject_t &hoid) {
  if (peer == get_primary())
    return true;
  ceph_assert(recovery_state.has_peer_info(peer));
  bool should_send =
      hoid.pool != (int64_t)info.pgid.pool() ||
      hoid <= last_backfill_started ||
      hoid <= recovery_state.get_peer_info(peer).last_backfill;
  if (!should_send) {
    ceph_assert(is_backfill_target(peer));
    dout(10) << __func__ << " issue_repop shipping empty opt to osd." << peer
             << ", object " << hoid
             << " beyond std::max(last_backfill_started "
             << ", peer_info[peer].last_backfill "
             << recovery_state.get_peer_info(peer).last_backfill
	     << ")" << dendl;
    return should_send;
  }
  if (is_async_recovery_target(peer) &&
      recovery_state.get_peer_missing(peer).is_missing(hoid)) {
    should_send = false;
    dout(10) << __func__ << " issue_repop shipping empty opt to osd." << peer
             << ", object " << hoid
             << " which is pending recovery in async_recovery_targets" << dendl;
  }
  return should_send;
}


ConnectionRef PrimaryLogPG::get_con_osd_cluster(
  int peer, epoch_t from_epoch)
{
  return osd->get_con_osd_cluster(peer, from_epoch);
}

PerfCounters *PrimaryLogPG::get_logger()
{
  return osd->logger;
}


// ====================
// missing objects

bool PrimaryLogPG::is_missing_object(const hobject_t& soid) const
{
  return recovery_state.get_pg_log().get_missing().get_items().count(soid);
}

void PrimaryLogPG::maybe_kick_recovery(
  const hobject_t &soid)
{
  eversion_t v;
  bool work_started = false;
  if (!recovery_state.get_missing_loc().needs_recovery(soid, &v))
    return;

  map<hobject_t, ObjectContextRef>::const_iterator p = recovering.find(soid);
  if (p != recovering.end()) {
    dout(7) << "object " << soid << " v " << v << ", already recovering." << dendl;
  } else if (recovery_state.get_missing_loc().is_unfound(soid)) {
    dout(7) << "object " << soid << " v " << v << ", is unfound." << dendl;
  } else {
    dout(7) << "object " << soid << " v " << v << ", recovering." << dendl;
    PGBackend::RecoveryHandle *h = pgbackend->open_recovery_op();
    if (is_missing_object(soid)) {
      recover_missing(soid, v, CEPH_MSG_PRIO_HIGH, h);
    } else if (recovery_state.get_missing_loc().is_deleted(soid)) {
      prep_object_replica_deletes(soid, v, h, &work_started);
    } else {
      prep_object_replica_pushes(soid, v, h, &work_started);
    }
    pgbackend->run_recovery_op(h, CEPH_MSG_PRIO_HIGH);
  }
}

void PrimaryLogPG::wait_for_unreadable_object(
  const hobject_t& soid, OpRequestRef op)
{
  ceph_assert(is_unreadable_object(soid));
  maybe_kick_recovery(soid);
  waiting_for_unreadable_object[soid].push_back(op);
  op->mark_delayed("waiting for missing object");
}

bool PrimaryLogPG::is_degraded_or_backfilling_object(const hobject_t& soid)
{
  /* The conditions below may clear (on_local_recover, before we queue
   * the transaction) before we actually requeue the degraded waiters
   * in on_global_recover after the transaction completes.
   */
  if (waiting_for_degraded_object.count(soid))
    return true;
  if (recovery_state.get_pg_log().get_missing().get_items().count(soid))
    return true;
  ceph_assert(!get_acting_recovery_backfill().empty());
  for (set<pg_shard_t>::iterator i = get_acting_recovery_backfill().begin();
       i != get_acting_recovery_backfill().end();
       ++i) {
    if (*i == get_primary()) continue;
    pg_shard_t peer = *i;
    auto peer_missing_entry = recovery_state.get_peer_missing().find(peer);
    // If an object is missing on an async_recovery_target, return false.
    // This will not block the op and the object is async recovered later.
    if (peer_missing_entry != recovery_state.get_peer_missing().end() &&
	peer_missing_entry->second.get_items().count(soid)) {
      if (is_async_recovery_target(peer))
	continue;
      else
	return true;
    }
    // Object is degraded if after last_backfill AND
    // we are backfilling it
    if (is_backfill_target(peer) &&
        recovery_state.get_peer_info(peer).last_backfill <= soid &&
	last_backfill_started >= soid &&
	backfills_in_flight.count(soid))
      return true;
  }
  return false;
}

bool PrimaryLogPG::is_degraded_on_async_recovery_target(const hobject_t& soid)
{
  for (auto &i: get_async_recovery_targets()) {
    auto peer_missing_entry = recovery_state.get_peer_missing().find(i);
    if (peer_missing_entry != recovery_state.get_peer_missing().end() &&
        peer_missing_entry->second.get_items().count(soid)) {
      dout(30) << __func__ << " " << soid << dendl;
      return true;
    }
  }
  return false;
}

void PrimaryLogPG::wait_for_degraded_object(const hobject_t& soid, OpRequestRef op)
{
  ceph_assert(is_degraded_or_backfilling_object(soid) || is_degraded_on_async_recovery_target(soid));

  maybe_kick_recovery(soid);
  waiting_for_degraded_object[soid].push_back(op);
  op->mark_delayed("waiting for degraded object");
}

void PrimaryLogPG::block_write_on_full_cache(
  const hobject_t& _oid, OpRequestRef op)
{
  const hobject_t oid = _oid.get_head();
  dout(20) << __func__ << ": blocking object " << oid
	   << " on full cache" << dendl;
  objects_blocked_on_cache_full.insert(oid);
  waiting_for_cache_not_full.push_back(op);
  op->mark_delayed("waiting for cache not full");
}

void PrimaryLogPG::block_for_clean(
  const hobject_t& oid, OpRequestRef op)
{
  dout(20) << __func__ << ": blocking object " << oid
	   << " on primary repair" << dendl;
  waiting_for_clean_to_primary_repair.push_back(op);
  op->mark_delayed("waiting for clean to repair");
}

void PrimaryLogPG::block_write_on_snap_rollback(
  const hobject_t& oid, ObjectContextRef obc, OpRequestRef op)
{
  dout(20) << __func__ << ": blocking object " << oid.get_head()
	   << " on snap promotion " << obc->obs.oi.soid << dendl;
  // otherwise, we'd have blocked in do_op
  ceph_assert(oid.is_head());
  ceph_assert(objects_blocked_on_snap_promotion.count(oid) == 0);
  objects_blocked_on_snap_promotion[oid] = obc;
  wait_for_blocked_object(obc->obs.oi.soid, op);
}

void PrimaryLogPG::block_write_on_degraded_snap(
  const hobject_t& snap, OpRequestRef op)
{
  dout(20) << __func__ << ": blocking object " << snap.get_head()
	   << " on degraded snap " << snap << dendl;
  // otherwise, we'd have blocked in do_op
  ceph_assert(objects_blocked_on_degraded_snap.count(snap.get_head()) == 0);
  objects_blocked_on_degraded_snap[snap.get_head()] = snap.snap;
  wait_for_degraded_object(snap, op);
}

bool PrimaryLogPG::maybe_await_blocked_head(
  const hobject_t &hoid,
  OpRequestRef op)
{
  ObjectContextRef obc;
  obc = object_contexts.lookup(hoid.get_head());
  if (obc) {
    if (obc->is_blocked()) {
      wait_for_blocked_object(obc->obs.oi.soid, op);
      return true;
    } else {
      return false;
    }
  }
  return false;
}

void PrimaryLogPG::wait_for_blocked_object(const hobject_t& soid, OpRequestRef op)
{
  dout(10) << __func__ << " " << soid << " " << op << dendl;
  waiting_for_blocked_object[soid].push_back(op);
  op->mark_delayed("waiting for blocked object");
}

void PrimaryLogPG::maybe_force_recovery()
{
  // no force if not in degraded/recovery/backfill states
  if (!is_degraded() &&
      !state_test(PG_STATE_RECOVERING |
                  PG_STATE_RECOVERY_WAIT |
		  PG_STATE_BACKFILLING |
		  PG_STATE_BACKFILL_WAIT |
		  PG_STATE_BACKFILL_TOOFULL))
    return;

  if (recovery_state.get_pg_log().get_log().approx_size() <
      cct->_conf->osd_max_pg_log_entries *
        cct->_conf->osd_force_recovery_pg_log_entries_factor)
    return;

  // find the oldest missing object
  version_t min_version = recovery_state.get_pg_log().get_log().head.version;
  hobject_t soid;
  if (!recovery_state.get_pg_log().get_missing().get_rmissing().empty()) {
    min_version = recovery_state.get_pg_log().get_missing().get_rmissing().begin()->first;
    soid = recovery_state.get_pg_log().get_missing().get_rmissing().begin()->second;
  }
  ceph_assert(!get_acting_recovery_backfill().empty());
  for (set<pg_shard_t>::iterator it = get_acting_recovery_backfill().begin();
       it != get_acting_recovery_backfill().end();
       ++it) {
    if (*it == get_primary()) continue;
    pg_shard_t peer = *it;
    auto it_missing = recovery_state.get_peer_missing().find(peer);
    if (it_missing != recovery_state.get_peer_missing().end() &&
	!it_missing->second.get_rmissing().empty()) {
      const auto& min_obj = recovery_state.get_peer_missing(peer).get_rmissing().begin();
      dout(20) << __func__ << " peer " << peer << " min_version " << min_obj->first
               << " oid " << min_obj->second << dendl;
      if (min_version > min_obj->first) {
        min_version = min_obj->first;
        soid = min_obj->second;
      }
    }
  }

  // recover it
  if (soid != hobject_t())
    maybe_kick_recovery(soid);
}

bool PrimaryLogPG::check_laggy(OpRequestRef& op)
{
  if (!HAVE_FEATURE(recovery_state.get_min_upacting_features(),
		    SERVER_OCTOPUS)) {
    dout(20) << __func__ << " not all upacting has SERVER_OCTOPUS" << dendl;
    return true;
  }
  if (state_test(PG_STATE_WAIT)) {
    dout(10) << __func__ << " PG is WAIT state" << dendl;
  } else if (!state_test(PG_STATE_LAGGY)) {
    auto mnow = osd->get_mnow();
    auto ru = recovery_state.get_readable_until();
    if (mnow <= ru) {
      // not laggy
      return true;
    }
    dout(10) << __func__
	     << " mnow " << mnow
	     << " > readable_until " << ru << dendl;

    if (!is_primary()) {
      osd->reply_op_error(op, -EAGAIN);
      return false;
    }

    // go to laggy state
    state_set(PG_STATE_LAGGY);
    publish_stats_to_osd();
  }
  dout(10) << __func__ << " not readable" << dendl;
  waiting_for_readable.push_back(op);
  op->mark_delayed("waiting for readable");
  return false;
}

bool PrimaryLogPG::check_laggy_requeue(OpRequestRef& op)
{
  if (!HAVE_FEATURE(recovery_state.get_min_upacting_features(),
		    SERVER_OCTOPUS)) {
    return true;
  }
  if (!state_test(PG_STATE_WAIT) && !state_test(PG_STATE_LAGGY)) {
    return true; // not laggy
  }
  dout(10) << __func__ << " not readable" << dendl;
  waiting_for_readable.push_front(op);
  op->mark_delayed("waiting for readable");
  return false;
}

void PrimaryLogPG::recheck_readable()
{
  if (!is_wait() && !is_laggy()) {
    dout(20) << __func__ << " wasn't wait or laggy" << dendl;
    return;
  }
  auto mnow = osd->get_mnow();
  bool pub = false;
  if (is_wait()) {
    auto prior_readable_until_ub = recovery_state.get_prior_readable_until_ub();
    if (mnow < prior_readable_until_ub) {
      dout(10) << __func__ << " still wait (mnow " << mnow
	       << " < prior_readable_until_ub " << prior_readable_until_ub
	       << ")" << dendl;
    } else {
      dout(10) << __func__ << " no longer wait (mnow " << mnow
	       << " >= prior_readable_until_ub " << prior_readable_until_ub
	       << ")" << dendl;
      state_clear(PG_STATE_WAIT);
      recovery_state.clear_prior_readable_until_ub();
      pub = true;
    }
  }
  if (is_laggy()) {
    auto ru = recovery_state.get_readable_until();
    if (ru == ceph::signedspan::zero()) {
      dout(10) << __func__ << " still laggy (mnow " << mnow
	       << ", readable_until zero)" << dendl;
    } else if (mnow >= ru) {
      dout(10) << __func__ << " still laggy (mnow " << mnow
	       << " >= readable_until " << ru << ")" << dendl;
    } else {
      dout(10) << __func__ << " no longer laggy (mnow " << mnow
	       << " < readable_until " << ru << ")" << dendl;
      state_clear(PG_STATE_LAGGY);
      pub = true;
    }
  }
  if (pub) {
    publish_stats_to_osd();
  }
  if (!is_laggy() && !is_wait()) {
    requeue_ops(waiting_for_readable);
  }
}

bool PrimaryLogPG::pgls_filter(const PGLSFilter& filter, const hobject_t& sobj)
{
  bufferlist bl;

  // If filter has expressed an interest in an xattr, load it.
  if (!filter.get_xattr().empty()) {
    int ret = pgbackend->objects_get_attr(
      sobj,
      filter.get_xattr(),
      &bl);
    dout(0) << "getattr (sobj=" << sobj << ", attr=" << filter.get_xattr() << ") returned " << ret << dendl;
    if (ret < 0) {
      if (ret != -ENODATA || filter.reject_empty_xattr()) {
        return false;
      }
    }
  }

  return filter.filter(sobj, bl);
}

std::pair<int, std::unique_ptr<const PGLSFilter>>
PrimaryLogPG::get_pgls_filter(bufferlist::const_iterator& iter)
{
  string type;
  // storing non-const PGLSFilter for the sake of ::init()
  std::unique_ptr<PGLSFilter> filter;

  try {
    decode(type, iter);
  }
  catch (ceph::buffer::error& e) {
    return { -EINVAL, nullptr };
  }

  if (type.compare("plain") == 0) {
    filter = std::make_unique<PGLSPlainFilter>();
  } else {
    std::size_t dot = type.find(".");
    if (dot == std::string::npos || dot == 0 || dot == type.size() - 1) {
      return { -EINVAL, nullptr };
    }

    const std::string class_name = type.substr(0, dot);
    const std::string filter_name = type.substr(dot + 1);
    ClassHandler::ClassData *cls = NULL;
    int r = ClassHandler::get_instance().open_class(class_name, &cls);
    if (r != 0) {
      derr << "Error opening class '" << class_name << "': "
           << cpp_strerror(r) << dendl;
      if (r != -EPERM) // propogate permission error
        r = -EINVAL;
      return { r, nullptr };
    } else {
      ceph_assert(cls);
    }

    ClassHandler::ClassFilter *class_filter = cls->get_filter(filter_name);
    if (class_filter == NULL) {
      derr << "Error finding filter '" << filter_name << "' in class "
           << class_name << dendl;
      return { -EINVAL, nullptr };
    }
    filter.reset(class_filter->fn());
    if (!filter) {
      // Object classes are obliged to return us something, but let's
      // give an error rather than asserting out.
      derr << "Buggy class " << class_name << " failed to construct "
              "filter " << filter_name << dendl;
      return { -EINVAL, nullptr };
    }
  }

  ceph_assert(filter);
  int r = filter->init(iter);
  if (r < 0) {
    derr << "Error initializing filter " << type << ": "
         << cpp_strerror(r) << dendl;
    return { -EINVAL, nullptr };
  } else {
    // Successfully constructed and initialized, return it.
    return std::make_pair(0, std::move(filter));
  }
}


// ==========================================================

void PrimaryLogPG::do_command(
  const string_view& orig_prefix,
  const cmdmap_t& cmdmap,
  const bufferlist& idata,
  std::function<void(int,const std::string&,bufferlist&)> on_finish)
{
  string format;
  cmd_getval(cmdmap, "format", format);
  std::unique_ptr<Formatter> f(Formatter::create(
				 format, "json-pretty", "json-pretty"));
  int ret = 0;
  stringstream ss;   // stderr error message stream
  bufferlist outbl;  // if empty at end, we'll dump formatter as output

  // get final prefix:
  // - ceph pg <pgid> foo -> prefix=pg, cmd=foo
  // - ceph tell <pgid> foo -> prefix=foo
  string prefix(orig_prefix);
  string command;
  cmd_getval(cmdmap, "cmd", command);
  if (command.size()) {
    prefix = command;
  }

  if (prefix == "query") {
    f->open_object_section("pg");
    f->dump_stream("snap_trimq") << snap_trimq;
    f->dump_unsigned("snap_trimq_len", snap_trimq.size());
    recovery_state.dump_peering_state(f.get());
    f->close_section();

    f->open_array_section("recovery_state");
    handle_query_state(f.get());
    f->close_section();

    f->open_object_section("agent_state");
    if (agent_state)
      agent_state->dump(f.get());
    f->close_section();

    f->close_section();
  }

  else if (prefix == "mark_unfound_lost") {
    string mulcmd;
    cmd_getval(cmdmap, "mulcmd", mulcmd);
    int mode = -1;
    if (mulcmd == "revert") {
      if (pool.info.is_erasure()) {
	ss << "mode must be 'delete' for ec pool";
	ret = -EINVAL;
	goto out;
      }
      mode = pg_log_entry_t::LOST_REVERT;
    } else if (mulcmd == "delete") {
      mode = pg_log_entry_t::LOST_DELETE;
    } else {
      ss << "mode must be 'revert' or 'delete'; mark not yet implemented";
      ret = -EINVAL;
      goto out;
    }
    ceph_assert(mode == pg_log_entry_t::LOST_REVERT ||
		mode == pg_log_entry_t::LOST_DELETE);

    if (!is_primary()) {
      ss << "not primary";
      ret = -EROFS;
      goto out;
    }

    uint64_t unfound = recovery_state.get_missing_loc().num_unfound();
    if (!unfound) {
      ss << "pg has no unfound objects";
      goto out;  // make command idempotent
    }

    if (!recovery_state.all_unfound_are_queried_or_lost(get_osdmap())) {
      ss << "pg has " << unfound
	 << " unfound objects but we haven't probed all sources, not marking lost";
      ret = -EINVAL;
      goto out;
    }

    mark_all_unfound_lost(mode, on_finish);
    return;
  }

  else if (prefix == "list_unfound") {
    hobject_t offset;
    string offset_json;
    bool show_offset = false;
    if (cmd_getval(cmdmap, "offset", offset_json)) {
      json_spirit::Value v;
      try {
	if (!json_spirit::read(offset_json, v))
	  throw std::runtime_error("bad json");
	offset.decode(v);
      } catch (std::runtime_error& e) {
	ss << "error parsing offset: " << e.what();
	ret = -EINVAL;
	goto out;
      }
      show_offset = true;
    }
    f->open_object_section("missing");
    if (show_offset) {
      f->open_object_section("offset");
      offset.dump(f.get());
      f->close_section();
    }
    auto &needs_recovery_map = recovery_state.get_missing_loc()
      .get_needs_recovery();
    f->dump_int("num_missing", needs_recovery_map.size());
    f->dump_int("num_unfound", get_num_unfound());
    map<hobject_t, pg_missing_item>::const_iterator p =
      needs_recovery_map.upper_bound(offset);
    {
      f->open_array_section("objects");
      int32_t num = 0;
      for (; p != needs_recovery_map.end() &&
	     num < cct->_conf->osd_command_max_records;
	   ++p) {
        if (recovery_state.get_missing_loc().is_unfound(p->first)) {
	  f->open_object_section("object");
	  {
	    f->open_object_section("oid");
	    p->first.dump(f.get());
	    f->close_section();
	  }
          p->second.dump(f.get()); // have, need keys
	  {
	    f->open_array_section("locations");
            for (auto &&r : recovery_state.get_missing_loc().get_locations(
		   p->first)) {
              f->dump_stream("shard") << r;
	    }
	    f->close_section();
	  }
	  f->close_section();
	  num++;
        }
      }
      f->close_section();
    }
    f->dump_bool("more", p != needs_recovery_map.end());
    f->close_section();
  }

  else if (prefix == "scrub" ||
	   prefix == "deep_scrub") {
    bool deep = (prefix == "deep_scrub");
    int64_t time;
    cmd_getval(cmdmap, "time", time, (int64_t)0);

    if (is_primary()) {
      const pg_pool_t *p = &pool.info;
      double pool_scrub_max_interval = 0;
      double scrub_max_interval;
      if (deep) {
        p->opts.get(pool_opts_t::DEEP_SCRUB_INTERVAL, &pool_scrub_max_interval);
        scrub_max_interval = pool_scrub_max_interval > 0 ?
          pool_scrub_max_interval : g_conf()->osd_deep_scrub_interval;
      } else {
        p->opts.get(pool_opts_t::SCRUB_MAX_INTERVAL, &pool_scrub_max_interval);
        scrub_max_interval = pool_scrub_max_interval > 0 ?
          pool_scrub_max_interval : g_conf()->osd_scrub_max_interval;
      }
      // Instead of marking must_scrub force a schedule scrub
      utime_t stamp = ceph_clock_now();
      if (time == 0)
        stamp -= scrub_max_interval;
      else
        stamp -=  (float)time;
      stamp -= 100.0;  // push back last scrub more for good measure
      if (deep) {
        set_last_deep_scrub_stamp(stamp);
      } else {
        set_last_scrub_stamp(stamp);
      }
      f->open_object_section("result");
      f->dump_bool("deep", deep);
      f->dump_stream("stamp") << stamp;
      f->close_section();
    } else {
      ss << "Not primary";
      ret = -EPERM;
    }
    outbl.append(ss.str());
  }

  else {
    ret = -ENOSYS;
    ss << "prefix '" << prefix << "' not implemented";
  }

 out:
  if (ret >= 0 && outbl.length() == 0) {
    f->flush(outbl);
  }
  on_finish(ret, ss.str(), outbl);
}


// ==========================================================

void PrimaryLogPG::do_pg_op(OpRequestRef op)
{
  const MOSDOp *m = static_cast<const MOSDOp *>(op->get_req());
  ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);
  dout(10) << "do_pg_op " << *m << dendl;

  op->mark_started();

  int result = 0;
  string cname, mname;

  snapid_t snapid = m->get_snapid();

  vector<OSDOp> ops = m->ops;

  for (vector<OSDOp>::iterator p = ops.begin(); p != ops.end(); ++p) {
    std::unique_ptr<const PGLSFilter> filter;
    OSDOp& osd_op = *p;
    auto bp = p->indata.cbegin();
    switch (p->op.op) {
    case CEPH_OSD_OP_PGNLS_FILTER:
      try {
	decode(cname, bp);
	decode(mname, bp);
      }
      catch (const ceph::buffer::error& e) {
	dout(0) << "unable to decode PGLS_FILTER description in " << *m << dendl;
	result = -EINVAL;
	break;
      }
      std::tie(result, filter) = get_pgls_filter(bp);
      if (result < 0)
        break;

      ceph_assert(filter);

      // fall through

    case CEPH_OSD_OP_PGNLS:
      if (snapid != CEPH_NOSNAP) {
	result = -EINVAL;
	break;
      }
      if (get_osdmap()->raw_pg_to_pg(m->get_pg()) != info.pgid.pgid) {
        dout(10) << " pgnls pg=" << m->get_pg()
		 << " " << get_osdmap()->raw_pg_to_pg(m->get_pg())
		 << " != " << info.pgid << dendl;
	result = 0; // hmm?
      } else {
	unsigned list_size = std::min<uint64_t>(cct->_conf->osd_max_pgls,
						p->op.pgls.count);

        dout(10) << " pgnls pg=" << m->get_pg() << " count " << list_size
		 << dendl;
	// read into a buffer
        vector<hobject_t> sentries;
        pg_nls_response_t response;
	try {
	  decode(response.handle, bp);
	}
	catch (const ceph::buffer::error& e) {
	  dout(0) << "unable to decode PGNLS handle in " << *m << dendl;
	  result = -EINVAL;
	  break;
	}

	hobject_t next;
	hobject_t lower_bound = response.handle;
	hobject_t pg_start = info.pgid.pgid.get_hobj_start();
	hobject_t pg_end = info.pgid.pgid.get_hobj_end(pool.info.get_pg_num());
        dout(10) << " pgnls lower_bound " << lower_bound
		 << " pg_end " << pg_end << dendl;
	if (((!lower_bound.is_max() && lower_bound >= pg_end) ||
	     (lower_bound != hobject_t() && lower_bound < pg_start))) {
	  // this should only happen with a buggy client.
	  dout(10) << "outside of PG bounds " << pg_start << " .. "
		   << pg_end << dendl;
	  result = -EINVAL;
	  break;
	}

	hobject_t current = lower_bound;
	int r = pgbackend->objects_list_partial(
	  current,
	  list_size,
	  list_size,
	  &sentries,
	  &next);
	if (r != 0) {
	  result = -EINVAL;
	  break;
	}

	map<hobject_t, pg_missing_item>::const_iterator missing_iter =
	  recovery_state.get_pg_log().get_missing().get_items().lower_bound(current);
	vector<hobject_t>::iterator ls_iter = sentries.begin();
	hobject_t _max = hobject_t::get_max();
	while (1) {
	  const hobject_t &mcand =
	    missing_iter == recovery_state.get_pg_log().get_missing().get_items().end() ?
	    _max :
	    missing_iter->first;
	  const hobject_t &lcand =
	    ls_iter == sentries.end() ?
	    _max :
	    *ls_iter;

	  hobject_t candidate;
	  if (mcand == lcand) {
	    candidate = mcand;
	    if (!mcand.is_max()) {
	      ++ls_iter;
	      ++missing_iter;
	    }
	  } else if (mcand < lcand) {
	    candidate = mcand;
	    ceph_assert(!mcand.is_max());
	    ++missing_iter;
	  } else {
	    candidate = lcand;
	    ceph_assert(!lcand.is_max());
	    ++ls_iter;
	  }

          dout(10) << " pgnls candidate 0x" << std::hex << candidate.get_hash()
		   << " vs lower bound 0x" << lower_bound.get_hash()
		   << std::dec << dendl;

	  if (candidate >= next) {
	    break;
	  }

	  if (response.entries.size() == list_size) {
	    next = candidate;
	    break;
	  }

	  if (candidate.snap != CEPH_NOSNAP)
	    continue;

	  // skip internal namespace
	  if (candidate.get_namespace() == cct->_conf->osd_hit_set_namespace)
	    continue;

	  if (recovery_state.get_missing_loc().is_deleted(candidate))
	    continue;

	  // skip wrong namespace
	  if (m->get_hobj().nspace != librados::all_nspaces &&
               candidate.get_namespace() != m->get_hobj().nspace)
	    continue;

	  if (filter && !pgls_filter(*filter, candidate))
	    continue;

          dout(20) << "pgnls item 0x" << std::hex
            << candidate.get_hash()
            << ", rev 0x" << hobject_t::_reverse_bits(candidate.get_hash())
            << std::dec << " "
            << candidate.oid.name << dendl;

	  librados::ListObjectImpl item;
	  item.nspace = candidate.get_namespace();
	  item.oid = candidate.oid.name;
	  item.locator = candidate.get_key();
	  response.entries.push_back(item);
	}

	if (next.is_max() &&
	    missing_iter == recovery_state.get_pg_log().get_missing().get_items().end() &&
	    ls_iter == sentries.end()) {
	  result = 1;

	  // Set response.handle to the start of the next PG according
	  // to the object sort order.
	  response.handle = info.pgid.pgid.get_hobj_end(pool.info.get_pg_num());
	} else {
          response.handle = next;
        }
        dout(10) << "pgnls handle=" << response.handle << dendl;
	encode(response, osd_op.outdata);
	dout(10) << " pgnls result=" << result << " outdata.length()="
		 << osd_op.outdata.length() << dendl;
      }
      break;

    case CEPH_OSD_OP_PGLS_FILTER:
      try {
	decode(cname, bp);
	decode(mname, bp);
      }
      catch (const ceph::buffer::error& e) {
	dout(0) << "unable to decode PGLS_FILTER description in " << *m << dendl;
	result = -EINVAL;
	break;
      }
      std::tie(result, filter) = get_pgls_filter(bp);
      if (result < 0)
        break;

      ceph_assert(filter);

      // fall through

    case CEPH_OSD_OP_PGLS:
      if (snapid != CEPH_NOSNAP) {
	result = -EINVAL;
	break;
      }
      if (get_osdmap()->raw_pg_to_pg(m->get_pg()) != info.pgid.pgid) {
        dout(10) << " pgls pg=" << m->get_pg()
		 << " " << get_osdmap()->raw_pg_to_pg(m->get_pg())
		 << " != " << info.pgid << dendl;
	result = 0; // hmm?
      } else {
	unsigned list_size = std::min<uint64_t>(cct->_conf->osd_max_pgls,
						p->op.pgls.count);

        dout(10) << " pgls pg=" << m->get_pg() << " count " << list_size << dendl;
	// read into a buffer
        vector<hobject_t> sentries;
        pg_ls_response_t response;
	try {
	  decode(response.handle, bp);
	}
	catch (const ceph::buffer::error& e) {
	  dout(0) << "unable to decode PGLS handle in " << *m << dendl;
	  result = -EINVAL;
	  break;
	}

	hobject_t next;
	hobject_t current = response.handle;
	int r = pgbackend->objects_list_partial(
	  current,
	  list_size,
	  list_size,
	  &sentries,
	  &next);
	if (r != 0) {
	  result = -EINVAL;
	  break;
	}

	ceph_assert(snapid == CEPH_NOSNAP || recovery_state.get_pg_log().get_missing().get_items().empty());

	map<hobject_t, pg_missing_item>::const_iterator missing_iter =
	  recovery_state.get_pg_log().get_missing().get_items().lower_bound(current);
	vector<hobject_t>::iterator ls_iter = sentries.begin();
	hobject_t _max = hobject_t::get_max();
	while (1) {
	  const hobject_t &mcand =
	    missing_iter == recovery_state.get_pg_log().get_missing().get_items().end() ?
	    _max :
	    missing_iter->first;
	  const hobject_t &lcand =
	    ls_iter == sentries.end() ?
	    _max :
	    *ls_iter;

	  hobject_t candidate;
	  if (mcand == lcand) {
	    candidate = mcand;
	    if (!mcand.is_max()) {
	      ++ls_iter;
	      ++missing_iter;
	    }
	  } else if (mcand < lcand) {
	    candidate = mcand;
	    ceph_assert(!mcand.is_max());
	    ++missing_iter;
	  } else {
	    candidate = lcand;
	    ceph_assert(!lcand.is_max());
	    ++ls_iter;
	  }

	  if (candidate >= next) {
	    break;
	  }
	    
	  if (response.entries.size() == list_size) {
	    next = candidate;
	    break;
	  }

	  if (candidate.snap != CEPH_NOSNAP)
	    continue;

	  // skip wrong namespace
	  if (candidate.get_namespace() != m->get_hobj().nspace)
	    continue;

	  if (recovery_state.get_missing_loc().is_deleted(candidate))
	    continue;

	  if (filter && !pgls_filter(*filter, candidate))
	    continue;

	  response.entries.push_back(make_pair(candidate.oid,
					       candidate.get_key()));
	}
	if (next.is_max() &&
	    missing_iter == recovery_state.get_pg_log().get_missing().get_items().end() &&
	    ls_iter == sentries.end()) {
	  result = 1;
	}
	response.handle = next;
	encode(response, osd_op.outdata);
	dout(10) << " pgls result=" << result << " outdata.length()="
		 << osd_op.outdata.length() << dendl;
      }
      break;

    case CEPH_OSD_OP_PG_HITSET_LS:
      {
	list< pair<utime_t,utime_t> > ls;
	for (list<pg_hit_set_info_t>::const_iterator p = info.hit_set.history.begin();
	     p != info.hit_set.history.end();
	     ++p)
	  ls.push_back(make_pair(p->begin, p->end));
	if (hit_set)
	  ls.push_back(make_pair(hit_set_start_stamp, utime_t()));
	encode(ls, osd_op.outdata);
      }
      break;

    case CEPH_OSD_OP_PG_HITSET_GET:
      {
	utime_t stamp(osd_op.op.hit_set_get.stamp);
	if (hit_set_start_stamp && stamp >= hit_set_start_stamp) {
	  // read the current in-memory HitSet, not the version we've
	  // checkpointed.
	  if (!hit_set) {
	    result= -ENOENT;
	    break;
	  }
	  encode(*hit_set, osd_op.outdata);
	  result = osd_op.outdata.length();
	} else {
	  // read an archived HitSet.
	  hobject_t oid;
	  for (list<pg_hit_set_info_t>::const_iterator p = info.hit_set.history.begin();
	       p != info.hit_set.history.end();
	       ++p) {
	    if (stamp >= p->begin && stamp <= p->end) {
	      oid = get_hit_set_archive_object(p->begin, p->end, p->using_gmt);
	      break;
	    }
	  }
	  if (oid == hobject_t()) {
	    result = -ENOENT;
	    break;
	  }
	  if (!pool.info.is_replicated()) {
	    // FIXME: EC not supported yet
	    result = -EOPNOTSUPP;
	    break;
	  }
	  if (is_unreadable_object(oid)) {
	    wait_for_unreadable_object(oid, op);
	    return;
	  }
	  result = osd->store->read(ch, ghobject_t(oid), 0, 0, osd_op.outdata);
	}
      }
      break;

   case CEPH_OSD_OP_SCRUBLS:
      result = do_scrub_ls(m, &osd_op);
      break;

    default:
      result = -EINVAL;
      break;
    }

    if (result < 0)
      break;
  }

  // reply
  MOSDOpReply *reply = new MOSDOpReply(m, 0, get_osdmap_epoch(),
				       CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK,
				       false);
  reply->claim_op_out_data(ops);
  reply->set_result(result);
  reply->set_reply_versions(info.last_update, info.last_user_version);
  osd->send_message_osd_client(reply, m->get_connection());
}

int PrimaryLogPG::do_scrub_ls(const MOSDOp *m, OSDOp *osd_op)
{
  if (m->get_pg() != info.pgid.pgid) {
    dout(10) << " scrubls pg=" << m->get_pg() << " != " << info.pgid << dendl;
    return -EINVAL; // hmm?
  }
  auto bp = osd_op->indata.cbegin();
  scrub_ls_arg_t arg;
  try {
    arg.decode(bp);
  } catch (ceph::buffer::error&) {
    dout(10) << " corrupted scrub_ls_arg_t" << dendl;
    return -EINVAL;
  }
  int r = 0;
  scrub_ls_result_t result = {.interval = info.history.same_interval_since};
  if (arg.interval != 0 && arg.interval != info.history.same_interval_since) {
    r = -EAGAIN;
  } else if (!scrubber.store) {
    r = -ENOENT;
  } else if (arg.get_snapsets) {
    result.vals = scrubber.store->get_snap_errors(osd->store,
						  get_pgid().pool(),
						  arg.start_after,
						  arg.max_return);
  } else {
    result.vals = scrubber.store->get_object_errors(osd->store,
						    get_pgid().pool(),
						    arg.start_after,
						    arg.max_return);
  }
  encode(result, osd_op->outdata);
  return r;
}

PrimaryLogPG::PrimaryLogPG(OSDService *o, OSDMapRef curmap,
			   const PGPool &_pool,
			   const map<string,string>& ec_profile, spg_t p) :
  PG(o, curmap, _pool, p),
  pgbackend(
    PGBackend::build_pg_backend(
      _pool.info, ec_profile, this, coll_t(p), ch, o->store, cct)),
  object_contexts(o->cct, o->cct->_conf->osd_pg_object_context_cache_count),
  new_backfill(false),
  temp_seq(0),
  snap_trimmer_machine(this)
{ 
  recovery_state.set_backend_predicates(
    pgbackend->get_is_readable_predicate(),
    pgbackend->get_is_recoverable_predicate());
  snap_trimmer_machine.initiate();
}

void PrimaryLogPG::get_src_oloc(const object_t& oid, const object_locator_t& oloc, object_locator_t& src_oloc)
{
  src_oloc = oloc;
  if (oloc.key.empty())
    src_oloc.key = oid.name;
}

void PrimaryLogPG::handle_backoff(OpRequestRef& op)
{
  auto m = op->get_req<MOSDBackoff>();
  auto session = ceph::ref_cast<Session>(m->get_connection()->get_priv());
  if (!session)
    return;  // drop it.
  hobject_t begin = info.pgid.pgid.get_hobj_start();
  hobject_t end = info.pgid.pgid.get_hobj_end(pool.info.get_pg_num());
  if (begin < m->begin) {
    begin = m->begin;
  }
  if (end > m->end) {
    end = m->end;
  }
  dout(10) << __func__ << " backoff ack id " << m->id
	   << " [" << begin << "," << end << ")" << dendl;
  session->ack_backoff(cct, m->pgid, m->id, begin, end);
}

void PrimaryLogPG::do_request(
  OpRequestRef& op,
  ThreadPool::TPHandle &handle)
{
  if (op->osd_trace) {
    op->pg_trace.init("pg op", &trace_endpoint, &op->osd_trace);
    op->pg_trace.event("do request");
  }
  // make sure we have a new enough map
  auto p = waiting_for_map.find(op->get_source());
  if (p != waiting_for_map.end()) {
    // preserve ordering
    dout(20) << __func__ << " waiting_for_map "
	     << p->first << " not empty, queueing" << dendl;
    p->second.push_back(op);
    op->mark_delayed("waiting_for_map not empty");
    return;
  }
  if (!have_same_or_newer_map(op->min_epoch)) {
    dout(20) << __func__ << " min " << op->min_epoch
	     << ", queue on waiting_for_map " << op->get_source() << dendl;
    waiting_for_map[op->get_source()].push_back(op);
    op->mark_delayed("op must wait for map");
    osd->request_osdmap_update(op->min_epoch);
    return;
  }

  if (can_discard_request(op)) {
    return;
  }

  // pg-wide backoffs
  const Message *m = op->get_req();
  int msg_type = m->get_type();
  if (m->get_connection()->has_feature(CEPH_FEATURE_RADOS_BACKOFF)) {
    auto session = ceph::ref_cast<Session>(m->get_connection()->get_priv());
    if (!session)
      return;  // drop it.

    if (msg_type == CEPH_MSG_OSD_OP) {
      if (session->check_backoff(cct, info.pgid,
				 info.pgid.pgid.get_hobj_start(), m)) {
	return;
      }

      bool backoff =
	is_down() ||
	is_incomplete() ||
	(!is_active() && is_peered());
      if (g_conf()->osd_backoff_on_peering && !backoff) {
	if (is_peering()) {
	  backoff = true;
	}
      }
      if (backoff) {
	add_pg_backoff(session);
	return;
      }
    }
    // pg backoff acks at pg-level
    if (msg_type == CEPH_MSG_OSD_BACKOFF) {
      const MOSDBackoff *ba = static_cast<const MOSDBackoff*>(m);
      if (ba->begin != ba->end) {
	handle_backoff(op);
	return;
      }
    }
  }

  if (!is_peered()) {
    // Delay unless PGBackend says it's ok
    if (pgbackend->can_handle_while_inactive(op)) {
      bool handled = pgbackend->handle_message(op);
      ceph_assert(handled);
      return;
    } else {
      waiting_for_peered.push_back(op);
      op->mark_delayed("waiting for peered");
      return;
    }
  }

  if (recovery_state.needs_flush()) {
    dout(20) << "waiting for flush on " << op << dendl;
    waiting_for_flush.push_back(op);
    op->mark_delayed("waiting for flush");
    return;
  }

  ceph_assert(is_peered() && !recovery_state.needs_flush());
  if (pgbackend->handle_message(op))
    return;

  switch (msg_type) {
  case CEPH_MSG_OSD_OP:
  case CEPH_MSG_OSD_BACKOFF:
    if (!is_active()) {
      dout(20) << " peered, not active, waiting for active on " << op << dendl;
      waiting_for_active.push_back(op);
      op->mark_delayed("waiting for active");
      return;
    }
    switch (msg_type) {
    case CEPH_MSG_OSD_OP:
      // verify client features
      if ((pool.info.has_tiers() || pool.info.is_tier()) &&
	  !op->has_feature(CEPH_FEATURE_OSD_CACHEPOOL)) {
	osd->reply_op_error(op, -EOPNOTSUPP);
	return;
      }
      do_op(op);
      break;
    case CEPH_MSG_OSD_BACKOFF:
      // object-level backoff acks handled in osdop context
      handle_backoff(op);
      break;
    }
    break;

  case MSG_OSD_PG_SCAN:
    do_scan(op, handle);
    break;

  case MSG_OSD_PG_BACKFILL:
    do_backfill(op);
    break;

  case MSG_OSD_PG_BACKFILL_REMOVE:
    do_backfill_remove(op);
    break;

  case MSG_OSD_SCRUB_RESERVE:
    {
      auto m = op->get_req<MOSDScrubReserve>();
      switch (m->type) {
      case MOSDScrubReserve::REQUEST:
	handle_scrub_reserve_request(op);
	break;
      case MOSDScrubReserve::GRANT:
	handle_scrub_reserve_grant(op, m->from);
	break;
      case MOSDScrubReserve::REJECT:
	handle_scrub_reserve_reject(op, m->from);
	break;
      case MOSDScrubReserve::RELEASE:
	handle_scrub_reserve_release(op);
	break;
      }
    }
    break;

  case MSG_OSD_REP_SCRUB:
    replica_scrub(op, handle);
    break;

  case MSG_OSD_REP_SCRUBMAP:
    do_replica_scrub_map(op);
    break;

  case MSG_OSD_PG_UPDATE_LOG_MISSING:
    do_update_log_missing(op);
    break;

  case MSG_OSD_PG_UPDATE_LOG_MISSING_REPLY:
    do_update_log_missing_reply(op);
    break;

  default:
    ceph_abort_msg("bad message type in do_request");
  }
}

/** do_op - do an op
 * pg lock will be held (if multithreaded)
 * osd_lock NOT held.
 */
void PrimaryLogPG::do_op(OpRequestRef& op)
{
  FUNCTRACE(cct);
  // NOTE: take a non-const pointer here; we must be careful not to
  // change anything that will break other reads on m (operator<<).
  MOSDOp *m = static_cast<MOSDOp*>(op->get_nonconst_req());
  ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);
  if (m->finish_decode()) {
    op->reset_desc();   // for TrackedOp
    m->clear_payload();
  }

  dout(20) << __func__ << ": op " << *m << dendl;

  const hobject_t head = m->get_hobj().get_head();

  if (!info.pgid.pgid.contains(
	info.pgid.pgid.get_split_bits(pool.info.get_pg_num()), head)) {
    derr << __func__ << " " << info.pgid.pgid << " does not contain "
	 << head << " pg_num " << pool.info.get_pg_num() << " hash "
	 << std::hex << head.get_hash() << std::dec << dendl;
    osd->clog->warn() << info.pgid.pgid << " does not contain " << head
		      << " op " << *m;
    ceph_assert(!cct->_conf->osd_debug_misdirected_ops);
    return;
  }

  bool can_backoff =
    m->get_connection()->has_feature(CEPH_FEATURE_RADOS_BACKOFF);
  ceph::ref_t<Session> session;
  if (can_backoff) {
    session = static_cast<Session*>(m->get_connection()->get_priv().get());
    if (!session.get()) {
      dout(10) << __func__ << " no session" << dendl;
      return;
    }

    if (session->check_backoff(cct, info.pgid, head, m)) {
      return;
    }
  }

  if (m->has_flag(CEPH_OSD_FLAG_PARALLELEXEC)) {
    // not implemented.
    dout(20) << __func__ << ": PARALLELEXEC not implemented " << *m << dendl;
    osd->reply_op_error(op, -EINVAL);
    return;
  }

  {
    int r = op->maybe_init_op_info(*get_osdmap());
    if (r) {
      osd->reply_op_error(op, r);
      return;
    }
  }

  if ((m->get_flags() & (CEPH_OSD_FLAG_BALANCE_READS |
			 CEPH_OSD_FLAG_LOCALIZE_READS)) &&
      op->may_read() &&
      !(op->may_write() || op->may_cache())) {
    // balanced reads; any replica will do
    if (!(is_primary() || is_nonprimary())) {
      osd->handle_misdirected_op(this, op);
      return;
    }
  } else {
    // normal case; must be primary
    if (!is_primary()) {
      osd->handle_misdirected_op(this, op);
      return;
    }
  }

  if (!check_laggy(op)) {
    return;
  }

  if (!op_has_sufficient_caps(op)) {
    osd->reply_op_error(op, -EPERM);
    return;
  }

  if (op->includes_pg_op()) {
    return do_pg_op(op);
  }

  // object name too long?
  if (m->get_oid().name.size() > cct->_conf->osd_max_object_name_len) {
    dout(4) << "do_op name is longer than "
	    << cct->_conf->osd_max_object_name_len
	    << " bytes" << dendl;
    osd->reply_op_error(op, -ENAMETOOLONG);
    return;
  }
  if (m->get_hobj().get_key().size() > cct->_conf->osd_max_object_name_len) {
    dout(4) << "do_op locator is longer than "
	    << cct->_conf->osd_max_object_name_len
	    << " bytes" << dendl;
    osd->reply_op_error(op, -ENAMETOOLONG);
    return;
  }
  if (m->get_hobj().nspace.size() > cct->_conf->osd_max_object_namespace_len) {
    dout(4) << "do_op namespace is longer than "
	    << cct->_conf->osd_max_object_namespace_len
	    << " bytes" << dendl;
    osd->reply_op_error(op, -ENAMETOOLONG);
    return;
  }
  if (m->get_hobj().oid.name.empty()) {
    dout(4) << "do_op empty oid name is not allowed" << dendl;
    osd->reply_op_error(op, -EINVAL);
    return;
  }

  if (int r = osd->store->validate_hobject_key(head)) {
    dout(4) << "do_op object " << head << " invalid for backing store: "
	    << r << dendl;
    osd->reply_op_error(op, r);
    return;
  }

  // blacklisted?
  if (get_osdmap()->is_blacklisted(m->get_source_addr())) {
    dout(10) << "do_op " << m->get_source_addr() << " is blacklisted" << dendl;
    osd->reply_op_error(op, -EBLACKLISTED);
    return;
  }

  // order this op as a write?
  bool write_ordered = op->rwordered();

  // discard due to cluster full transition?  (we discard any op that
  // originates before the cluster or pool is marked full; the client
  // will resend after the full flag is removed or if they expect the
  // op to succeed despite being full).  The except is FULL_FORCE and
  // FULL_TRY ops, which there is no reason to discard because they
  // bypass all full checks anyway.  If this op isn't write or
  // read-ordered, we skip.
  // FIXME: we exclude mds writes for now.
  if (write_ordered && !(m->get_source().is_mds() ||
			 m->has_flag(CEPH_OSD_FLAG_FULL_TRY) ||
			 m->has_flag(CEPH_OSD_FLAG_FULL_FORCE)) &&
      info.history.last_epoch_marked_full > m->get_map_epoch()) {
    dout(10) << __func__ << " discarding op sent before full " << m << " "
	     << *m << dendl;
    return;
  }
  // mds should have stopped writing before this point.
  // We can't allow OSD to become non-startable even if mds
  // could be writing as part of file removals.
  if (write_ordered && osd->check_failsafe_full(get_dpp()) && 
      !m->has_flag(CEPH_OSD_FLAG_FULL_TRY)) {
    dout(10) << __func__ << " fail-safe full check failed, dropping request." << dendl;
    return;
  }
  int64_t poolid = get_pgid().pool();
  if (op->may_write()) {

    const pg_pool_t *pi = get_osdmap()->get_pg_pool(poolid);
    if (!pi) {
      return;
    }

    // invalid?
    if (m->get_snapid() != CEPH_NOSNAP) {
      dout(20) << __func__ << ": write to clone not valid " << *m << dendl;
      osd->reply_op_error(op, -EINVAL);
      return;
    }

    // too big?
    if (cct->_conf->osd_max_write_size &&
        m->get_data_len() > cct->_conf->osd_max_write_size << 20) {
      // journal can't hold commit!
      derr << "do_op msg data len " << m->get_data_len()
           << " > osd_max_write_size " << (cct->_conf->osd_max_write_size << 20)
           << " on " << *m << dendl;
      osd->reply_op_error(op, -OSD_WRITETOOBIG);
      return;
    }
  }

  dout(10) << "do_op " << *m
	   << (op->may_write() ? " may_write" : "")
	   << (op->may_read() ? " may_read" : "")
	   << (op->may_cache() ? " may_cache" : "")
	   << " -> " << (write_ordered ? "write-ordered" : "read-ordered")
	   << " flags " << ceph_osd_flag_string(m->get_flags())
	   << dendl;

  // missing object?
  if (is_unreadable_object(head)) {
    if (!is_primary()) {
      osd->reply_op_error(op, -EAGAIN);
      return;
    }
    if (can_backoff &&
	(g_conf()->osd_backoff_on_degraded ||
	 (g_conf()->osd_backoff_on_unfound &&
	  recovery_state.get_missing_loc().is_unfound(head)))) {
      add_backoff(session, head, head);
      maybe_kick_recovery(head);
    } else {
      wait_for_unreadable_object(head, op);
    }
    return;
  }

  if (write_ordered) {
    // degraded object?
    if (is_degraded_or_backfilling_object(head)) {
      if (can_backoff && g_conf()->osd_backoff_on_degraded) {
        add_backoff(session, head, head);
        maybe_kick_recovery(head);
      } else {
        wait_for_degraded_object(head, op);
      }
      return;
    }

    if (scrubber.is_chunky_scrub_active() && write_blocked_by_scrub(head)) {
      dout(20) << __func__ << ": waiting for scrub" << dendl;
      waiting_for_scrub.push_back(op);
      op->mark_delayed("waiting for scrub");
      return;
    }
    if (!check_laggy_requeue(op)) {
      return;
    }

    // blocked on snap?
    if (auto blocked_iter = objects_blocked_on_degraded_snap.find(head);
	blocked_iter != std::end(objects_blocked_on_degraded_snap)) {
      hobject_t to_wait_on(head);
      to_wait_on.snap = blocked_iter->second;
      wait_for_degraded_object(to_wait_on, op);
      return;
    }
    if (auto blocked_snap_promote_iter = objects_blocked_on_snap_promotion.find(head);
	blocked_snap_promote_iter != std::end(objects_blocked_on_snap_promotion)) {
      wait_for_blocked_object(blocked_snap_promote_iter->second->obs.oi.soid, op);
      return;
    }
    if (objects_blocked_on_cache_full.count(head)) {
      block_write_on_full_cache(head, op);
      return;
    }
  }

  // dup/resent?
  if (op->may_write() || op->may_cache()) {
    // warning: we will get back *a* request for this reqid, but not
    // necessarily the most recent.  this happens with flush and
    // promote ops, but we can't possible have both in our log where
    // the original request is still not stable on disk, so for our
    // purposes here it doesn't matter which one we get.
    eversion_t version;
    version_t user_version;
    int return_code = 0;
    vector<pg_log_op_return_item_t> op_returns;
    bool got = check_in_progress_op(
      m->get_reqid(), &version, &user_version, &return_code, &op_returns);
    if (got) {
      dout(3) << __func__ << " dup " << m->get_reqid()
	      << " version " << version << dendl;
      if (already_complete(version)) {
	osd->reply_op_error(op, return_code, version, user_version, op_returns);
      } else {
	dout(10) << " waiting for " << version << " to commit" << dendl;
        // always queue ondisk waiters, so that we can requeue if needed
	waiting_for_ondisk[version].emplace_back(op, user_version, return_code,
						 op_returns);
	op->mark_delayed("waiting for ondisk");
      }
      return;
    }
  }

  ObjectContextRef obc;
  bool can_create = op->may_write();
  hobject_t missing_oid;

  // kludge around the fact that LIST_SNAPS sets CEPH_SNAPDIR for LIST_SNAPS
  const hobject_t& oid =
    m->get_snapid() == CEPH_SNAPDIR ? head : m->get_hobj();

  // make sure LIST_SNAPS is on CEPH_SNAPDIR and nothing else
  for (vector<OSDOp>::iterator p = m->ops.begin(); p != m->ops.end(); ++p) {
    OSDOp& osd_op = *p;

    if (osd_op.op.op == CEPH_OSD_OP_LIST_SNAPS) {
      if (m->get_snapid() != CEPH_SNAPDIR) {
	dout(10) << "LIST_SNAPS with incorrect context" << dendl;
	osd->reply_op_error(op, -EINVAL);
	return;
      }
    } else {
      if (m->get_snapid() == CEPH_SNAPDIR) {
	dout(10) << "non-LIST_SNAPS on snapdir" << dendl;
	osd->reply_op_error(op, -EINVAL);
	return;
      }
    }
  }

  // io blocked on obc?
  if (!m->has_flag(CEPH_OSD_FLAG_FLUSH) &&
      maybe_await_blocked_head(oid, op)) {
    return;
  }

  if (!is_primary()) {
    if (!recovery_state.can_serve_replica_read(oid)) {
      dout(20) << __func__ << ": oid " << oid
	       << " unstable write on replica, bouncing to primary."
	       << *m << dendl;
      osd->reply_op_error(op, -EAGAIN);
      return;
    } else {
      dout(20) << __func__ << ": serving replica read on oid" << oid
	       << dendl;
    }
  }

  int r = find_object_context(
    oid, &obc, can_create,
    m->has_flag(CEPH_OSD_FLAG_MAP_SNAP_CLONE),
    &missing_oid);

  // LIST_SNAPS needs the ssc too
  if (obc &&
      m->get_snapid() == CEPH_SNAPDIR &&
      !obc->ssc) {
    obc->ssc = get_snapset_context(oid, true);
  }

  if (r == -EAGAIN) {
    // If we're not the primary of this OSD, we just return -EAGAIN. Otherwise,
    // we have to wait for the object.
    if (is_primary()) {
      // missing the specific snap we need; requeue and wait.
      ceph_assert(!op->may_write()); // only happens on a read/cache
      wait_for_unreadable_object(missing_oid, op);
      return;
    }
  } else if (r == 0) {
    if (is_unreadable_object(obc->obs.oi.soid)) {
      dout(10) << __func__ << ": clone " << obc->obs.oi.soid
	       << " is unreadable, waiting" << dendl;
      wait_for_unreadable_object(obc->obs.oi.soid, op);
      return;
    }

    // degraded object?  (the check above was for head; this could be a clone)
    if (write_ordered &&
	obc->obs.oi.soid.snap != CEPH_NOSNAP &&
	is_degraded_or_backfilling_object(obc->obs.oi.soid)) {
      dout(10) << __func__ << ": clone " << obc->obs.oi.soid
	       << " is degraded, waiting" << dendl;
      wait_for_degraded_object(obc->obs.oi.soid, op);
      return;
    }
  }

  bool in_hit_set = false;
  if (hit_set) {
    if (obc.get()) {
      if (obc->obs.oi.soid != hobject_t() && hit_set->contains(obc->obs.oi.soid))
	in_hit_set = true;
    } else {
      if (missing_oid != hobject_t() && hit_set->contains(missing_oid))
        in_hit_set = true;
    }
    if (!op->hitset_inserted) {
      hit_set->insert(oid);
      op->hitset_inserted = true;
      if (hit_set->is_full() ||
          hit_set_start_stamp + pool.info.hit_set_period <= m->get_recv_stamp()) {
        hit_set_persist();
      }
    }
  }

  if (agent_state) {
    if (agent_choose_mode(false, op))
      return;
  }

  if (obc.get() && obc->obs.exists && obc->obs.oi.has_manifest()) {
    if (maybe_handle_manifest(op,
			       write_ordered,
			       obc))
    return;
  }

  if (maybe_handle_cache(op,
			 write_ordered,
			 obc,
			 r,
			 missing_oid,
			 false,
			 in_hit_set))
    return;

  if (r && (r != -ENOENT || !obc)) {
    // copy the reqids for copy get on ENOENT
    if (r == -ENOENT &&
	(m->ops[0].op.op == CEPH_OSD_OP_COPY_GET)) {
      fill_in_copy_get_noent(op, oid, m->ops[0]);
      return;
    }
    dout(20) << __func__ << ": find_object_context got error " << r << dendl;
    if (op->may_write() &&
	get_osdmap()->require_osd_release >= ceph_release_t::kraken) {
      record_write_error(op, oid, nullptr, r);
    } else {
      osd->reply_op_error(op, r);
    }
    return;
  }

  // make sure locator is consistent
  object_locator_t oloc(obc->obs.oi.soid);
  if (m->get_object_locator() != oloc) {
    dout(10) << " provided locator " << m->get_object_locator() 
	     << " != object's " << obc->obs.oi.soid << dendl;
    osd->clog->warn() << "bad locator " << m->get_object_locator() 
		     << " on object " << oloc
		      << " op " << *m;
  }

  // io blocked on obc?
  if (obc->is_blocked() &&
      !m->has_flag(CEPH_OSD_FLAG_FLUSH)) {
    wait_for_blocked_object(obc->obs.oi.soid, op);
    return;
  }

  dout(25) << __func__ << " oi " << obc->obs.oi << dendl;

  OpContext *ctx = new OpContext(op, m->get_reqid(), &m->ops, obc, this);

  if (m->has_flag(CEPH_OSD_FLAG_SKIPRWLOCKS)) {
    dout(20) << __func__ << ": skipping rw locks" << dendl;
  } else if (m->get_flags() & CEPH_OSD_FLAG_FLUSH) {
    dout(20) << __func__ << ": part of flush, will ignore write lock" << dendl;

    // verify there is in fact a flush in progress
    // FIXME: we could make this a stronger test.
    map<hobject_t,FlushOpRef>::iterator p = flush_ops.find(obc->obs.oi.soid);
    if (p == flush_ops.end()) {
      dout(10) << __func__ << " no flush in progress, aborting" << dendl;
      reply_ctx(ctx, -EINVAL);
      return;
    }
  } else if (!get_rw_locks(write_ordered, ctx)) {
    dout(20) << __func__ << " waiting for rw locks " << dendl;
    op->mark_delayed("waiting for rw locks");
    close_op_ctx(ctx);
    return;
  }
  dout(20) << __func__ << " obc " << *obc << dendl;

  if (r) {
    dout(20) << __func__ << " returned an error: " << r << dendl;
    if (op->may_write() &&
	get_osdmap()->require_osd_release >= ceph_release_t::kraken) {
      record_write_error(op, oid, nullptr, r,
			 ctx->op->allows_returnvec() ? ctx : nullptr);
    } else {
      osd->reply_op_error(op, r);
    }
    close_op_ctx(ctx);
    return;
  }

  if (m->has_flag(CEPH_OSD_FLAG_IGNORE_CACHE)) {
    ctx->ignore_cache = true;
  }

  if ((op->may_read()) && (obc->obs.oi.is_lost())) {
    // This object is lost. Reading from it returns an error.
    dout(20) << __func__ << ": object " << obc->obs.oi.soid
	     << " is lost" << dendl;
    reply_ctx(ctx, -ENFILE);
    return;
  }
  if (!op->may_write() &&
      !op->may_cache() &&
      (!obc->obs.exists ||
       ((m->get_snapid() != CEPH_SNAPDIR) &&
	obc->obs.oi.is_whiteout()))) {
    // copy the reqids for copy get on ENOENT
    if (m->ops[0].op.op == CEPH_OSD_OP_COPY_GET) {
      fill_in_copy_get_noent(op, oid, m->ops[0]);
      close_op_ctx(ctx);
      return;
    }
    reply_ctx(ctx, -ENOENT);
    return;
  }

  op->mark_started();

  execute_ctx(ctx);
  utime_t prepare_latency = ceph_clock_now();
  prepare_latency -= op->get_dequeued_time();
  osd->logger->tinc(l_osd_op_prepare_lat, prepare_latency);
  if (op->may_read() && op->may_write()) {
    osd->logger->tinc(l_osd_op_rw_prepare_lat, prepare_latency);
  } else if (op->may_read()) {
    osd->logger->tinc(l_osd_op_r_prepare_lat, prepare_latency);
  } else if (op->may_write() || op->may_cache()) {
    osd->logger->tinc(l_osd_op_w_prepare_lat, prepare_latency);
  }

  // force recovery of the oldest missing object if too many logs
  maybe_force_recovery();
}

PrimaryLogPG::cache_result_t PrimaryLogPG::maybe_handle_manifest_detail(
  OpRequestRef op,
  bool write_ordered,
  ObjectContextRef obc)
{
  ceph_assert(obc);
  if (op->get_req<MOSDOp>()->get_flags() & CEPH_OSD_FLAG_IGNORE_REDIRECT) {
    dout(20) << __func__ << ": ignoring redirect due to flag" << dendl;
    return cache_result_t::NOOP;
  }

  // if it is write-ordered and blocked, stop now
  if (obc->is_blocked() && write_ordered) {
    // we're already doing something with this object
    dout(20) << __func__ << " blocked on " << obc->obs.oi.soid << dendl;
    return cache_result_t::NOOP;
  }

  vector<OSDOp> ops = op->get_req<MOSDOp>()->ops;
  for (vector<OSDOp>::iterator p = ops.begin(); p != ops.end(); ++p) {
    OSDOp& osd_op = *p;
    ceph_osd_op& op = osd_op.op;
    if (op.op == CEPH_OSD_OP_SET_REDIRECT ||
	op.op == CEPH_OSD_OP_SET_CHUNK || 
	op.op == CEPH_OSD_OP_UNSET_MANIFEST ||
	op.op == CEPH_OSD_OP_TIER_FLUSH) {
      return cache_result_t::NOOP;
    } else if (op.op == CEPH_OSD_OP_TIER_PROMOTE) {
      bool is_dirty = false;
      for (auto& p : obc->obs.oi.manifest.chunk_map) {
	if (p.second.is_dirty()) {
	  is_dirty = true;
	}
      }
      if (is_dirty) {
	start_flush(OpRequestRef(), obc, true, NULL, std::nullopt);
      }
      return cache_result_t::NOOP;
    }
  }

  switch (obc->obs.oi.manifest.type) {
  case object_manifest_t::TYPE_REDIRECT:
    if (op->may_write() || write_ordered) {
      do_proxy_write(op, obc);
    } else {
      // promoted object 
      if (obc->obs.oi.size != 0) {
	return cache_result_t::NOOP;
      }
      do_proxy_read(op, obc);
    }
    return cache_result_t::HANDLED_PROXY;
  case object_manifest_t::TYPE_CHUNKED: 
    {
      if (can_proxy_chunked_read(op, obc)) {
	map<hobject_t,FlushOpRef>::iterator p = flush_ops.find(obc->obs.oi.soid);
        if (p != flush_ops.end()) {
          do_proxy_chunked_op(op, obc->obs.oi.soid, obc, true);
          return cache_result_t::HANDLED_PROXY;
        }
	do_proxy_chunked_op(op, obc->obs.oi.soid, obc, write_ordered);
	return cache_result_t::HANDLED_PROXY;
      }

      MOSDOp *m = static_cast<MOSDOp*>(op->get_nonconst_req());
      ceph_assert(m->get_type() == CEPH_MSG_OSD_OP);
      hobject_t head = m->get_hobj();

      if (is_degraded_or_backfilling_object(head)) {
	dout(20) << __func__ << ": " << head << " is degraded, waiting" << dendl;
	wait_for_degraded_object(head, op);
	return cache_result_t::BLOCKED_RECOVERY;
      }

      if (write_blocked_by_scrub(head)) {
	dout(20) << __func__ << ": waiting for scrub" << dendl;
	waiting_for_scrub.push_back(op);
	op->mark_delayed("waiting for scrub");
	return cache_result_t::BLOCKED_RECOVERY;
      }
      if (!check_laggy_requeue(op)) {
	return cache_result_t::BLOCKED_RECOVERY;
      }
      
      for (auto& p : obc->obs.oi.manifest.chunk_map) {
	if (p.second.is_missing()) {
	  auto m = op->get_req<MOSDOp>();
	  const object_locator_t oloc = m->get_object_locator();
	  promote_object(obc, obc->obs.oi.soid, oloc, op, NULL);
	  return cache_result_t::BLOCKED_PROMOTE;
	}
      }

      bool all_dirty = true;
      for (auto& p : obc->obs.oi.manifest.chunk_map) {
	if (!p.second.is_dirty()) {
	  all_dirty = false;
	}
      }
      if (all_dirty) {
	start_flush(OpRequestRef(), obc, true, NULL, std::nullopt);
      }
      return cache_result_t::NOOP;
    }
  default:
    ceph_abort_msg("unrecognized manifest type");
  }

  return cache_result_t::NOOP;
}

struct C_ManifestFlush : public Context {
  PrimaryLogPGRef pg;
  hobject_t oid;
  epoch_t lpr;
  ceph_tid_t tid;
  utime_t start;
  uint64_t offset;
  uint64_t last_offset;
  C_ManifestFlush(PrimaryLogPG *p, hobject_t o, epoch_t e)
    : pg(p), oid(o), lpr(e),
      tid(0), start(ceph_clock_now())
  {}
  void finish(int r) override {
    if (r == -ECANCELED)
      return;
    std::scoped_lock locker{*pg};
    pg->handle_manifest_flush(oid, tid, r, offset, last_offset, lpr);
    pg->osd->logger->tinc(l_osd_tier_flush_lat, ceph_clock_now() - start);
  }
};

void PrimaryLogPG::handle_manifest_flush(hobject_t oid, ceph_tid_t tid, int r,
                                         uint64_t offset, uint64_t last_offset,
                                         epoch_t lpr)
{
  map<hobject_t,FlushOpRef>::iterator p = flush_ops.find(oid);
  if (p == flush_ops.end()) {
    dout(10) << __func__ << " no flush_op found" << dendl;
    return;
  }
  if (p->second->rval < 0) {
    return;
  }
  p->second->io_results[offset] = r;
  for (auto &ior: p->second->io_results) {
    if (ior.second < 0) {
      finish_manifest_flush(oid, tid, r, p->second->obc, last_offset);
      p->second->rval = r;
      return;
    }
  }
  if (p->second->chunks == p->second->io_results.size()) {
    if (lpr == get_last_peering_reset()) {
      ceph_assert(p->second->obc);
      finish_manifest_flush(oid, tid, r, p->second->obc, last_offset);
    }
  }
}

int PrimaryLogPG::start_manifest_flush(OpRequestRef op, ObjectContextRef obc, bool blocking,
				       std::optional<std::function<void()>> &&on_flush)
{
  auto p = obc->obs.oi.manifest.chunk_map.begin();
  FlushOpRef manifest_fop(std::make_shared<FlushOp>());
  manifest_fop->op = op;
  manifest_fop->obc = obc;
  manifest_fop->flushed_version = obc->obs.oi.user_version;
  manifest_fop->blocking = blocking;
  manifest_fop->on_flush = std::move(on_flush);
  int r = do_manifest_flush(op, obc, manifest_fop, p->first, blocking);
  if (r < 0) {
    return r;
  }

  flush_ops[obc->obs.oi.soid] = manifest_fop;
  return -EINPROGRESS;
}

int PrimaryLogPG::do_manifest_flush(OpRequestRef op, ObjectContextRef obc, FlushOpRef manifest_fop,
				    uint64_t start_offset, bool block)
{
  struct object_manifest_t &manifest = obc->obs.oi.manifest;
  hobject_t soid = obc->obs.oi.soid;
  ceph_tid_t tid;
  SnapContext snapc;
  uint64_t max_copy_size = 0, last_offset = 0;

  map<uint64_t, chunk_info_t>::iterator iter = manifest.chunk_map.find(start_offset); 
  ceph_assert(iter != manifest.chunk_map.end());
  for (;iter != manifest.chunk_map.end(); ++iter) {
    if (iter->second.is_dirty()) {
      last_offset = iter->first;
      max_copy_size += iter->second.length;
    }
    if (get_copy_chunk_size() < max_copy_size) {
      break;
    }
  }

  iter = manifest.chunk_map.find(start_offset);
  for (;iter != manifest.chunk_map.end(); ++iter) {
    if (!iter->second.is_dirty()) {
      continue;
    }
    uint64_t tgt_length = iter->second.length;
    uint64_t tgt_offset= iter->second.offset;
    hobject_t tgt_soid = iter->second.oid;
    object_locator_t oloc(tgt_soid);
    ObjectOperation obj_op;
    bufferlist chunk_data;
    int r = pgbackend->objects_read_sync(
	soid, iter->first, tgt_length, 0, &chunk_data);
    if (r < 0) {
      dout(0) << __func__ << " read fail " << " offset: " << tgt_offset
	      << " len: " << tgt_length << " r: " << r << dendl;
      return r;
    }
    if (!chunk_data.length()) {
      return -ENODATA;
    }

    unsigned flags = CEPH_OSD_FLAG_IGNORE_CACHE | CEPH_OSD_FLAG_IGNORE_OVERLAY |
		     CEPH_OSD_FLAG_RWORDERED;
    tgt_length = chunk_data.length();
    if (pg_pool_t::fingerprint_t fp_algo = pool.info.get_fingerprint_type();
	iter->second.has_reference() &&
	fp_algo != pg_pool_t::TYPE_FINGERPRINT_NONE) {
      object_t fp_oid = [fp_algo, &chunk_data]() -> string {
        switch (fp_algo) {
	case pg_pool_t::TYPE_FINGERPRINT_SHA1:
	  return ceph::crypto::digest<ceph::crypto::SHA1>(chunk_data).to_str();
	case pg_pool_t::TYPE_FINGERPRINT_SHA256:
	  return ceph::crypto::digest<ceph::crypto::SHA256>(chunk_data).to_str();
	case pg_pool_t::TYPE_FINGERPRINT_SHA512:
	  return ceph::crypto::digest<ceph::crypto::SHA512>(chunk_data).to_str();
	default:
	  assert(0 == "unrecognized fingerprint type");
	  return {};
	}
      }();
      bufferlist in;
      if (fp_oid != tgt_soid.oid && (!obc->ssc && !obc->ssc->snapset.clones.size())) {
	// TODO: don't retain reference to dirty extents
	// decrement old chunk's reference count
	ObjectOperation dec_op;
	cls_cas_chunk_put_ref_op put_call;
	put_call.source = soid.get_head();
	::encode(put_call, in);
	dec_op.call("cas", "chunk_put_ref", in);
	// we don't care dec_op's completion. scrub for dedup will fix this.
	tid = osd->objecter->mutate(
	  tgt_soid.oid, oloc, dec_op, snapc,
	  ceph::real_clock::from_ceph_timespec(obc->obs.oi.mtime),
	  flags, NULL);
	in.clear();
      }
      tgt_soid.oid = fp_oid;
      iter->second.oid = tgt_soid;
      {
	bufferlist t;
	cls_cas_chunk_create_or_get_ref_op get_call;
	get_call.source = soid.get_head();
	get_call.data = chunk_data;
	::encode(get_call, t);
	obj_op.call("cas", "chunk_create_or_get_ref", t);
      }
    } else {
      obj_op.add_data(CEPH_OSD_OP_WRITE, tgt_offset, tgt_length, chunk_data);
    }

    C_ManifestFlush *fin = new C_ManifestFlush(this, soid, get_last_peering_reset());
    fin->offset = iter->first;
    fin->last_offset = last_offset;
    manifest_fop->chunks++;

    tid = osd->objecter->mutate(
      tgt_soid.oid, oloc, obj_op, snapc,
      ceph::real_clock::from_ceph_timespec(obc->obs.oi.mtime),
      flags, new C_OnFinisher(fin, osd->get_objecter_finisher(get_pg_shard())));
    fin->tid = tid;
    manifest_fop->io_tids[iter->first] = tid;

    dout(20) << __func__ << " offset: " << tgt_offset << " len: " << tgt_length 
	    << " oid: " << tgt_soid.oid << " ori oid: " << soid.oid.name 
	    << " tid: " << tid << dendl;
    if (last_offset < iter->first) {
      break;
    }
  }

  return 0;
}

void PrimaryLogPG::finish_manifest_flush(hobject_t oid, ceph_tid_t tid, int r,
					 ObjectContextRef obc, uint64_t last_offset)
{
  dout(10) << __func__ << " " << oid << " tid " << tid
	   << " " << cpp_strerror(r) << " last_offset: " << last_offset << dendl;
  map<hobject_t,FlushOpRef>::iterator p = flush_ops.find(oid);
  if (p == flush_ops.end()) {
    dout(10) << __func__ << " no flush_op found" << dendl;
    return;
  }
  map<uint64_t, chunk_info_t>::iterator iter = 
      obc->obs.oi.manifest.chunk_map.find(last_offset); 
  ceph_assert(iter != obc->obs.oi.manifest.chunk_map.end());
  for (;iter != obc->obs.oi.manifest.chunk_map.end(); ++iter) {
    if (iter->second.is_dirty() && last_offset < iter->first) {
      do_manifest_flush(p->second->op, obc, p->second, iter->first, p->second->blocking);
      return;
    }
  }
  finish_flush(oid, tid, r);
}

void PrimaryLogPG::record_write_error(OpRequestRef op, const hobject_t &soid,
				      MOSDOpReply *orig_reply, int r,
				      OpContext *ctx_for_op_returns)
{
  dout(20) << __func__ << " r=" << r << dendl;
  ceph_assert(op->may_write());
  const osd_reqid_t &reqid = op->get_req<MOSDOp>()->get_reqid();
  mempool::osd_pglog::list<pg_log_entry_t> entries;
  entries.push_back(pg_log_entry_t(pg_log_entry_t::ERROR, soid,
				   get_next_version(), eversion_t(), 0,
				   reqid, utime_t(), r));
  if (ctx_for_op_returns) {
    entries.back().set_op_returns(*ctx_for_op_returns->ops);
    dout(20) << __func__ << " op_returns=" << entries.back().op_returns << dendl;
  }

  struct OnComplete {
    PrimaryLogPG *pg;
    OpRequestRef op;
    boost::intrusive_ptr<MOSDOpReply> orig_reply;
    int r;
    OnComplete(
      PrimaryLogPG *pg,
      OpRequestRef op,
      MOSDOpReply *orig_reply,
      int r)
      : pg(pg), op(op),
	orig_reply(orig_reply, false /* take over ref */), r(r)
      {}
    void operator()() {
      ldpp_dout(pg, 20) << "finished " << __func__ << " r=" << r << dendl;
      auto m = op->get_req<MOSDOp>();
      MOSDOpReply *reply = orig_reply.detach();
      ldpp_dout(pg, 10) << " sending commit on " << *m << " " << reply << dendl;
      pg->osd->send_message_osd_client(reply, m->get_connection());
    }
  };

  ObcLockManager lock_manager;
  submit_log_entries(
    entries,
    std::move(lock_manager),
    std::optional<std::function<void(void)> >(
      OnComplete(this, op, orig_reply, r)),
    op,
    r);
}

PrimaryLogPG::cache_result_t PrimaryLogPG::maybe_handle_cache_detail(
  OpRequestRef op,
  bool write_ordered,
  ObjectContextRef obc,
  int r, hobject_t missing_oid,
  bool must_promote,
  bool in_hit_set,
  ObjectContextRef *promote_obc)
{
  // return quickly if caching is not enabled
  if (pool.info.cache_mode == pg_pool_t::CACHEMODE_NONE)
    return cache_result_t::NOOP;

  if (op &&
      op->get_req() &&
      op->get_req()->get_type() == CEPH_MSG_OSD_OP &&
      (op->get_req<MOSDOp>()->get_flags() &
       CEPH_OSD_FLAG_IGNORE_CACHE)) {
    dout(20) << __func__ << ": ignoring cache due to flag" << dendl;
    return cache_result_t::NOOP;
  }

  must_promote = must_promote || op->need_promote();

  if (obc)
    dout(25) << __func__ << " " << obc->obs.oi << " "
	     << (obc->obs.exists ? "exists" : "DNE")
	     << " missing_oid " << missing_oid
	     << " must_promote " << (int)must_promote
	     << " in_hit_set " << (int)in_hit_set
	     << dendl;
  else
    dout(25) << __func__ << " (no obc)"
	     << " missing_oid " << missing_oid
	     << " must_promote " << (int)must_promote
	     << " in_hit_set " << (int)in_hit_set
	     << dendl;

  // if it is write-ordered and blocked, stop now
  if (obc.get() && obc->is_blocked() && write_ordered) {
    // we're already doing something with this object
    dout(20) << __func__ << " blocked on " << obc->obs.oi.soid << dendl;
    return cache_result_t::NOOP;
  }

  if (r == -ENOENT && missing_oid == hobject_t()) {
    // we know this object is logically absent (e.g., an undefined clone)
    return cache_result_t::NOOP;
  }

  if (obc.get() && obc->obs.exists) {
    osd->logger->inc(l_osd_op_cache_hit);
    return cache_result_t::NOOP;
  }
  if (!is_primary()) {
    dout(20) << __func__ << " cache miss; ask the primary" << dendl;
    osd->reply_op_error(op, -EAGAIN);
    return cache_result_t::REPLIED_WITH_EAGAIN;
  }

  if (missing_oid == hobject_t() && obc.get()) {
    missing_oid = obc->obs.oi.soid;
  }

  auto m = op->get_req<MOSDOp>();
  const object_locator_t oloc = m->get_object_locator();

  if (op->need_skip_handle_cache()) {
    return cache_result_t::NOOP;
  }

  OpRequestRef promote_op;

  switch (pool.info.cache_mode) {
  case pg_pool_t::CACHEMODE_WRITEBACK:
    if (agent_state &&
	agent_state->evict_mode == TierAgentState::EVICT_MODE_FULL) {
      if (!op->may_write() && !op->may_cache() &&
	  !write_ordered && !must_promote) {
	dout(20) << __func__ << " cache pool full, proxying read" << dendl;
	do_proxy_read(op);
	return cache_result_t::HANDLED_PROXY;
      }
      dout(20) << __func__ << " cache pool full, waiting" << dendl;
      block_write_on_full_cache(missing_oid, op);
      return cache_result_t::BLOCKED_FULL;
    }

    if (must_promote || (!hit_set && !op->need_skip_promote())) {
      promote_object(obc, missing_oid, oloc, op, promote_obc);
      return cache_result_t::BLOCKED_PROMOTE;
    }

    if (op->may_write() || op->may_cache()) {
      do_proxy_write(op);

      // Promote too?
      if (!op->need_skip_promote() && 
          maybe_promote(obc, missing_oid, oloc, in_hit_set,
	              pool.info.min_write_recency_for_promote,
		      OpRequestRef(),
		      promote_obc)) {
	return cache_result_t::BLOCKED_PROMOTE;
      }
      return cache_result_t::HANDLED_PROXY;
    } else {
      do_proxy_read(op);

      // Avoid duplicate promotion
      if (obc.get() && obc->is_blocked()) {
	if (promote_obc)
	  *promote_obc = obc;
        return cache_result_t::BLOCKED_PROMOTE;
      }

      // Promote too?
      if (!op->need_skip_promote()) {
        (void)maybe_promote(obc, missing_oid, oloc, in_hit_set,
                            pool.info.min_read_recency_for_promote,
                            promote_op, promote_obc);
      }

      return cache_result_t::HANDLED_PROXY;
    }
    ceph_abort_msg("unreachable");
    return cache_result_t::NOOP;

  case pg_pool_t::CACHEMODE_READONLY:
    // TODO: clean this case up
    if (!obc.get() && r == -ENOENT) {
      // we don't have the object and op's a read
      promote_object(obc, missing_oid, oloc, op, promote_obc);
      return cache_result_t::BLOCKED_PROMOTE;
    }
    if (!r) { // it must be a write
      do_cache_redirect(op);
      return cache_result_t::HANDLED_REDIRECT;
    }
    // crap, there was a failure of some kind
    return cache_result_t::NOOP;

  case pg_pool_t::CACHEMODE_FORWARD:
    // this mode is deprecated; proxy instead
  case pg_pool_t::CACHEMODE_PROXY:
    if (!must_promote) {
      if (op->may_write() || op->may_cache() || write_ordered) {
	do_proxy_write(op);
	return cache_result_t::HANDLED_PROXY;
      } else {
	do_proxy_read(op);
	return cache_result_t::HANDLED_PROXY;
      }
    }
    // ugh, we're forced to promote.
    if (agent_state &&
	agent_state->evict_mode == TierAgentState::EVICT_MODE_FULL) {
      dout(20) << __func__ << " cache pool full, waiting" << dendl;
      block_write_on_full_cache(missing_oid, op);
      return cache_result_t::BLOCKED_FULL;
    }
    promote_object(obc, missing_oid, oloc, op, promote_obc);
    return cache_result_t::BLOCKED_PROMOTE;

  case pg_pool_t::CACHEMODE_READFORWARD:
    // this mode is deprecated; proxy instead
  case pg_pool_t::CACHEMODE_READPROXY:
    // Do writeback to the cache tier for writes
    if (op->may_write() || write_ordered || must_promote) {
      if (agent_state &&
	  agent_state->evict_mode == TierAgentState::EVICT_MODE_FULL) {
	dout(20) << __func__ << " cache pool full, waiting" << dendl;
	block_write_on_full_cache(missing_oid, op);
	return cache_result_t::BLOCKED_FULL;
      }
      promote_object(obc, missing_oid, oloc, op, promote_obc);
      return cache_result_t::BLOCKED_PROMOTE;
    }

    // If it is a read, we can read, we need to proxy it
    do_proxy_read(op);
    return cache_result_t::HANDLED_PROXY;

  default:
    ceph_abort_msg("unrecognized cache_mode");
  }
  return cache_result_t::NOOP;
}

bool PrimaryLogPG::maybe_promote(ObjectContextRef obc,
				 const hobject_t& missing_oid,
				 const object_locator_t& oloc,
				 bool in_hit_set,
				 uint32_t recency,
				 OpRequestRef promote_op,
				 ObjectContextRef *promote_obc)
{
  dout(20) << __func__ << " missing_oid " << missing_oid
	   << "  in_hit_set " << in_hit_set << dendl;

  switch (recency) {
  case 0:
    break;
  case 1:
    // Check if in the current hit set
    if (in_hit_set) {
      break;
    } else {
      // not promoting
      return false;
    }
    break;
  default:
    {
      unsigned count = (int)in_hit_set;
      if (count) {
	// Check if in other hit sets
	const hobject_t& oid = obc.get() ? obc->obs.oi.soid : missing_oid;
	for (map<time_t,HitSetRef>::reverse_iterator itor =
	       agent_state->hit_set_map.rbegin();
	     itor != agent_state->hit_set_map.rend();
	     ++itor) {
	  if (!itor->second->contains(oid)) {
	    break;
	  }
	  ++count;
	  if (count >= recency) {
	    break;
	  }
	}
      }
      if (count >= recency) {
	break;
      }
      return false;	// not promoting
    }
    break;
  }

  if (osd->promote_throttle()) {
    dout(10) << __func__ << " promote throttled" << dendl;
    return false;
  }
  promote_object(obc, missing_oid, oloc, promote_op, promote_obc);
  return true;
}

void PrimaryLogPG::do_cache_redirect(OpRequestRef op)
{
  auto m = op->get_req<MOSDOp>();
  int flags = m->get_flags() & (CEPH_OSD_FLAG_ACK|CEPH_OSD_FLAG_ONDISK);
  MOSDOpReply *reply = new MOSDOpReply(m, -ENOENT, get_osdmap_epoch(),
                                       flags, false);
  request_redirect_t redir(m->get_object_locator(), pool.info.tier_of);
  reply->set_redirect(redir);
  dout(10) << "sending redirect to pool " << pool.info.tier_of << " for op "
	   << op << dendl;
  m->get_connection()->send_message(reply);
  return;
}

struct C_ProxyRead : public Context {
  PrimaryLogPGRef pg;
  hobject_t oid;
  epoch_t last_peering_reset;
  ceph_tid_t tid;
  PrimaryLogPG::ProxyReadOpRef prdop;
  utime_t start;
  C_ProxyRead(PrimaryLogPG *p, hobject_t o, epoch_t lpr,
	     const PrimaryLogPG::ProxyReadOpRef& prd)
    : pg(p), oid(o), last_peering_reset(lpr),
      tid(0), prdop(prd), start(ceph_clock_now())
  {}
  void finish(int r) override {
    if (prdop->canceled)
      return;
    std::scoped_lock locker{*pg};
    if (prdop->canceled) {
      return;
    }
    if (last_peering_reset == pg->get_last_peering_reset()) {
      pg->finish_proxy_read(oid, tid, r);
      pg->osd->logger->tinc(l_osd_tier_r_lat, ceph_clock_now() - start);
    }
  }
};

struct C_ProxyChunkRead : public Context {
  PrimaryLogPGRef pg;
  hobject_t oid;
  epoch_t last_peering_reset;
  ceph_tid_t tid;
  PrimaryLogPG::ProxyReadOpRef prdop;
  utime_t start;
  ObjectOperation *obj_op;
  int op_index = 0;
  uint64_t req_offset = 0;
  ObjectContextRef obc;
  uint64_t req_total_len = 0;
  C_ProxyChunkRead(PrimaryLogPG *p, hobject_t o, epoch_t lpr,
		   const PrimaryLogPG::ProxyReadOpRef& prd)
    : pg(p), oid(o), last_peering_reset(lpr),
      tid(0), prdop(prd), start(ceph_clock_now()), obj_op(NULL)
  {}
  void finish(int r) override {
    if (prdop->canceled)
      return;
    std::scoped_lock locker{*pg};
    if (prdop->canceled) {
      return;
    }
    if (last_peering_reset == pg->get_last_peering_reset()) {
      if (r >= 0) {
	if (!prdop->ops[op_index].outdata.length()) {
	  ceph_assert(req_total_len);
	  bufferlist list;
	  bufferptr bptr(req_total_len);
	  list.push_back(std::move(bptr));
	  prdop->ops[op_index].outdata.append(list);
	}
	ceph_assert(obj_op);
	uint64_t copy_offset;
	if (req_offset >= prdop->ops[op_index].op.extent.offset) {
	  copy_offset = req_offset - prdop->ops[op_index].op.extent.offset;
	} else {
	  copy_offset = 0;
	}
	prdop->ops[op_index].outdata.begin(copy_offset).copy_in(
          obj_op->ops[0].outdata.length(),
          obj_op->ops[0].outdata.c_str());
      } 	
      
      pg->finish_proxy_read(oid, tid, r);
      pg->osd->logger->tinc(l_osd_tier_r_lat, ceph_clock_now() - start);
      if (obj_op) {
	delete obj_op;
      }
    }
  }
};

void PrimaryLogPG::do_proxy_read(OpRequestRef op, ObjectContextRef obc)
{
  // NOTE: non-const here because the ProxyReadOp needs mutable refs to
  // stash the result in the request's OSDOp vector
  MOSDOp *m = static_cast<MOSDOp*>(op->get_nonconst_req());
  object_locator_t oloc;
  hobject_t soid;
  /* extensible tier */
  if (obc && obc->obs.exists && obc->obs.oi.has_manifest()) {
    switch (obc->obs.oi.manifest.type) {
      case object_manifest_t::TYPE_REDIRECT:
	  oloc = object_locator_t(obc->obs.oi.manifest.redirect_target);
	  soid = obc->obs.oi.manifest.redirect_target;  
	  break;
      default:
	ceph_abort_msg("unrecognized manifest type");
    }
  } else {
  /* proxy */
    soid = m->get_hobj();
    oloc = object_locator_t(m->get_object_locator());
    oloc.pool = pool.info.tier_of;
  }
  unsigned flags = CEPH_OSD_FLAG_IGNORE_CACHE | CEPH_OSD_FLAG_IGNORE_OVERLAY;

  // pass through some original flags that make sense.
  //  - leave out redirection and balancing flags since we are
  //    already proxying through the primary
  //  - leave off read/write/exec flags that are derived from the op
  flags |= m->get_flags() & (CEPH_OSD_FLAG_RWORDERED |
			     CEPH_OSD_FLAG_ORDERSNAP |
			     CEPH_OSD_FLAG_ENFORCE_SNAPC |
			     CEPH_OSD_FLAG_MAP_SNAP_CLONE);

  dout(10) << __func__ << " Start proxy read for " << *m << dendl;

  ProxyReadOpRef prdop(std::make_shared<ProxyReadOp>(op, soid, m->ops));

  ObjectOperation obj_op;
  obj_op.dup(prdop->ops);

  if (pool.info.cache_mode == pg_pool_t::CACHEMODE_WRITEBACK &&
      (agent_state && agent_state->evict_mode != TierAgentState::EVICT_MODE_FULL)) {
    for (unsigned i = 0; i < obj_op.ops.size(); i++) {
      ceph_osd_op op = obj_op.ops[i].op;
      switch (op.op) {
	case CEPH_OSD_OP_READ:
	case CEPH_OSD_OP_SYNC_READ:
	case CEPH_OSD_OP_SPARSE_READ:
	case CEPH_OSD_OP_CHECKSUM:
	case CEPH_OSD_OP_CMPEXT:
	  op.flags = (op.flags | CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL) &
		       ~(CEPH_OSD_OP_FLAG_FADVISE_DONTNEED | CEPH_OSD_OP_FLAG_FADVISE_NOCACHE);
      }
    }
  }

  C_ProxyRead *fin = new C_ProxyRead(this, soid, get_last_peering_reset(),
				     prdop);
  ceph_tid_t tid = osd->objecter->read(
    soid.oid, oloc, obj_op,
    m->get_snapid(), NULL,
    flags, new C_OnFinisher(fin, osd->get_objecter_finisher(get_pg_shard())),
    &prdop->user_version,
    &prdop->data_offset,
    m->get_features());
  fin->tid = tid;
  prdop->objecter_tid = tid;
  proxyread_ops[tid] = prdop;
  in_progress_proxy_ops[soid].push_back(op);
}

void PrimaryLogPG::finish_proxy_read(hobject_t oid, ceph_tid_t tid, int r)
{
  dout(10) << __func__ << " " << oid << " tid " << tid
	   << " " << cpp_strerror(r) << dendl;

  map<ceph_tid_t, ProxyReadOpRef>::iterator p = proxyread_ops.find(tid);
  if (p == proxyread_ops.end()) {
    dout(10) << __func__ << " no proxyread_op found" << dendl;
    return;
  }
  ProxyReadOpRef prdop = p->second;
  if (tid != prdop->objecter_tid) {
    dout(10) << __func__ << " tid " << tid << " != prdop " << prdop
	     << " tid " << prdop->objecter_tid << dendl;
    return;
  }
  if (oid != prdop->soid) {
    dout(10) << __func__ << " oid " << oid << " != prdop " << prdop
	     << " soid " << prdop->soid << dendl;
    return;
  }
  proxyread_ops.erase(tid);

  map<hobject_t, list<OpRequestRef>>::iterator q = in_progress_proxy_ops.find(oid);
  if (q == in_progress_proxy_ops.end()) {
    dout(10) << __func__ << " no in_progress_proxy_ops found" << dendl;
    return;
  }
  ceph_assert(q->second.size());
  list<OpRequestRef>::iterator it = std::find(q->second.begin(),
                                              q->second.end(),
					      prdop->op);
  ceph_assert(it != q->second.end());
  OpRequestRef op = *it;
  q->second.erase(it);
  if (q->second.size() == 0) {
    in_progress_proxy_ops.erase(oid);
  } else if (std::find(q->second.begin(),
                       q->second.end(),
                       prdop->op) != q->second.end()) {
    /* multiple read case */
    dout(20) << __func__ << " " << oid << " is not completed  " << dendl;
    return;
  }

  osd->logger->inc(l_osd_tier_proxy_read);

  auto m = op->get_req<MOSDOp>();
  OpContext *ctx = new OpContext(op, m->get_reqid(), &prdop->ops, this);
  ctx->reply = new MOSDOpReply(m, 0, get_osdmap_epoch(), 0, false);
  ctx->user_at_version = prdop->user_version;
  ctx->data_off = prdop->data_offset;
  ctx->ignore_log_op_stats = true;
  complete_read_ctx(r, ctx);
}

void PrimaryLogPG::kick_proxy_ops_blocked(hobject_t& soid)
{
  map<hobject_t, list<OpRequestRef>>::iterator p = in_progress_proxy_ops.find(soid);
  if (p == in_progress_proxy_ops.end())
    return;

  list<OpRequestRef>& ls = p->second;
  dout(10) << __func__ << " " << soid << " requeuing " << ls.size() << " requests" << dendl;
  requeue_ops(ls);
  in_progress_proxy_ops.erase(p);
}

void PrimaryLogPG::cancel_proxy_read(ProxyReadOpRef prdop,
				     vector<ceph_tid_t> *tids)
{
  dout(10) << __func__ << " " << prdop->soid << dendl;
  prdop->canceled = true;

  // cancel objecter op, if we can
  if (prdop->objecter_tid) {
    tids->push_back(prdop->objecter_tid);
    for (uint32_t i = 0; i < prdop->ops.size(); i++) {
      prdop->ops[i].outdata.clear();
    }
    proxyread_ops.erase(prdop->objecter_tid);
    prdop->objecter_tid = 0;
  }
}

void PrimaryLogPG::cancel_proxy_ops(bool requeue, vector<ceph_tid_t> *tids)
{
  dout(10) << __func__ << dendl;

  // cancel proxy reads
  map<ceph_tid_t, ProxyReadOpRef>::iterator p = proxyread_ops.begin();
  while (p != proxyread_ops.end()) {
    cancel_proxy_read((p++)->second, tids);
  }

  // cancel proxy writes
  map<ceph_tid_t, ProxyWriteOpRef>::iterator q = proxywrite_ops.begin();
  while (q != proxywrite_ops.end()) {
    cancel_proxy_write((q++)->second, tids);
  }

  if (requeue) {
    map<hobject_t, list<OpRequestRef>>::iterator p =
      in_progress_proxy_ops.begin();
    while (p != in_progress_proxy_ops.end()) {
      list<OpRequestRef>& ls = p->second;
      dout(10) << __func__ << " " << p->first << " requeuing " << ls.size()
	       << " requests" << dendl;
      requeue_ops(ls);
      in_progress_proxy_ops.erase(p++);
    }
  } else {
    in_progress_proxy_ops.clear();
  }
}

struct C_ProxyWrite_Commit : public Context {
  PrimaryLogPGRef pg;
  hobject_t oid;
  epoch_t last_peering_reset;
  ceph_tid_t tid;
  PrimaryLogPG::ProxyWriteOpRef pwop;
  C_ProxyWrite_Commit(PrimaryLogPG *p, hobject_t o, epoch_t lpr,
	              const PrimaryLogPG::ProxyWriteOpRef& pw)
    : pg(p), oid(o), last_peering_reset(lpr),
      tid(0), pwop(pw)
  {}
  void finish(int r) override {
    if (pwop->canceled)
      return;
    std::scoped_lock locker{*pg};
    if (pwop->canceled) {
      return;
    }
    if (last_peering_reset == pg->get_last_peering_reset()) {
      pg->finish_proxy_write(oid, tid, r);
    }
  }
};

void PrimaryLogPG::do_proxy_write(OpRequestRef op, ObjectContextRef obc)
{
  // NOTE: non-const because ProxyWriteOp takes a mutable ref
  MOSDOp *m = static_cast<MOSDOp*>(op->get_nonconst_req());
  object_locator_t oloc;
  SnapContext snapc(m->get_snap_seq(), m->get_snaps());
  hobject_t soid;
  /* extensible tier */
  if (obc && obc->obs.exists && obc->obs.oi.has_manifest()) {
    switch (obc->obs.oi.manifest.type) {
      case object_manifest_t::TYPE_REDIRECT:
	  oloc = object_locator_t(obc->obs.oi.manifest.redirect_target);
	  soid = obc->obs.oi.manifest.redirect_target;  
	  break;
      default:
	ceph_abort_msg("unrecognized manifest type");
    }
  } else {
  /* proxy */
    soid = m->get_hobj();
    oloc = object_locator_t(m->get_object_locator());
    oloc.pool = pool.info.tier_of;
  }

  unsigned flags = CEPH_OSD_FLAG_IGNORE_CACHE | CEPH_OSD_FLAG_IGNORE_OVERLAY;
  if (!(op->may_write() || op->may_cache())) {
    flags |= CEPH_OSD_FLAG_RWORDERED;
  }
  if (op->allows_returnvec()) {
    flags |= CEPH_OSD_FLAG_RETURNVEC;
  }

  dout(10) << __func__ << " Start proxy write for " << *m << dendl;

  ProxyWriteOpRef pwop(std::make_shared<ProxyWriteOp>(op, soid, m->ops, m->get_reqid()));
  pwop->ctx = new OpContext(op, m->get_reqid(), &pwop->ops, this);
  pwop->mtime = m->get_mtime();

  ObjectOperation obj_op;
  obj_op.dup(pwop->ops);

  C_ProxyWrite_Commit *fin = new C_ProxyWrite_Commit(
      this, soid, get_last_peering_reset(), pwop);
  ceph_tid_t tid = osd->objecter->mutate(
    soid.oid, oloc, obj_op, snapc,
    ceph::real_clock::from_ceph_timespec(pwop->mtime),
    flags, new C_OnFinisher(fin, osd->get_objecter_finisher(get_pg_shard())),
    &pwop->user_version, pwop->reqid);
  fin->tid = tid;
  pwop->objecter_tid = tid;
  proxywrite_ops[tid] = pwop;
  in_progress_proxy_ops[soid].push_back(op);
}

void PrimaryLogPG::do_proxy_chunked_op(OpRequestRef op, const hobject_t& missing_oid, 
				       ObjectContextRef obc, bool write_ordered)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->get_nonconst_req());
  OSDOp *osd_op = NULL;
  for (unsigned int i = 0; i < m->ops.size(); i++) {
    osd_op = &m->ops[i];
    uint64_t cursor = osd_op->op.extent.offset;
    uint64_t op_length = osd_op->op.extent.offset + osd_op->op.extent.length;
    uint64_t chunk_length = 0, chunk_index = 0, req_len = 0;
    object_manifest_t *manifest = &obc->obs.oi.manifest;
    map <uint64_t, map<uint64_t, uint64_t>> chunk_read;

    while (cursor < op_length) {
      chunk_index = 0;
      chunk_length = 0;
      /* find the right chunk position for cursor */
      for (auto &p : manifest->chunk_map) {                                                                        
	if (p.first <= cursor && p.first + p.second.length > cursor) {                                             
	  chunk_length = p.second.length;                                                                          
	  chunk_index = p.first;                                                                                   
	  break;
	}
      } 
      /* no index */
      if (!chunk_index && !chunk_length) {
	if (cursor == osd_op->op.extent.offset) {
	  OpContext *ctx = new OpContext(op, m->get_reqid(), &m->ops, this);                                        
	  ctx->reply = new MOSDOpReply(m, 0, get_osdmap_epoch(), 0, false);
	  ctx->data_off = osd_op->op.extent.offset;                                                                
	  ctx->ignore_log_op_stats = true;                                                                         
	  complete_read_ctx(0, ctx);                                                                               
	}
	break;
      }
      uint64_t next_length = chunk_length;
      /* the size to read -> | op length | */
      /*		     | 	 a chunk   | */
      if (cursor + next_length > op_length) {
	next_length = op_length - cursor;
      }
      /* the size to read -> |   op length   | */
      /*		     | 	 a chunk | */
      if (cursor + next_length > chunk_index + chunk_length) {                                                  
	next_length = chunk_index + chunk_length - cursor;                                                      
      } 

      chunk_read[cursor] = {{chunk_index, next_length}};
      cursor += next_length;
    }

    req_len = cursor - osd_op->op.extent.offset;
    for (auto &p : chunk_read) {
      auto chunks = p.second.begin();
      dout(20) << __func__ << " chunk_index: " << chunks->first 
	      << " next_length: " << chunks->second << " cursor: " 
	      << p.first << dendl;
      do_proxy_chunked_read(op, obc, i, chunks->first, p.first, chunks->second, req_len, write_ordered);
    }
  } 
}

struct RefCountCallback : public Context {
public:
  PrimaryLogPG::OpContext *ctx;
  OSDOp& osd_op;
  bool requeue = false;
    
  RefCountCallback(PrimaryLogPG::OpContext *ctx, OSDOp &osd_op)
    : ctx(ctx), osd_op(osd_op) {}
  void finish(int r) override {
    // NB: caller must already have pg->lock held
    ctx->obc->stop_block();
    ctx->pg->kick_object_context_blocked(ctx->obc);
    if (r >= 0) {
      osd_op.rval = 0;
      ctx->pg->execute_ctx(ctx);
    } else {
       // on cancel simply toss op out,
       // or requeue as requested
      if (r != -ECANCELED) {
        if (ctx->op)
          ctx->pg->osd->reply_op_error(ctx->op, r);
      } else if (requeue) {
        if (ctx->op)
          ctx->pg->requeue_op(ctx->op);
      }
      ctx->pg->close_op_ctx(ctx);
    }
  }
  void set_requeue(bool rq) {
    requeue = rq;
  }
};

struct SetManifestFinisher : public PrimaryLogPG::OpFinisher {
  OSDOp& osd_op;

  explicit SetManifestFinisher(OSDOp& osd_op) : osd_op(osd_op) {
  }

  int execute() override {
    return osd_op.rval;
  }
};

struct C_SetManifestRefCountDone : public Context {
  RefCountCallback* cb;
  hobject_t soid;
  C_SetManifestRefCountDone(
    RefCountCallback* cb, hobject_t soid) : cb(cb), soid(soid) {}
  void finish(int r) override {
    if (r == -ECANCELED)
      return;
    auto pg = cb->ctx->pg;
    std::scoped_lock locker{*pg};
    auto it = pg->manifest_ops.find(soid);
    if (it == pg->manifest_ops.end()) {
      // raced with cancel_manifest_ops
      return;
    }
    pg->manifest_ops.erase(it);
    cb->complete(r);
  }
};

void PrimaryLogPG::cancel_manifest_ops(bool requeue, vector<ceph_tid_t> *tids)
{
  dout(10) << __func__ << dendl;
  auto p = manifest_ops.begin();
  while (p != manifest_ops.end()) {
    auto mop = p->second;
    // cancel objecter op, if we can
    if (mop->objecter_tid) {
      tids->push_back(mop->objecter_tid);
      mop->objecter_tid = 0;
    }
    mop->cb->set_requeue(requeue);
    mop->cb->complete(-ECANCELED);
    manifest_ops.erase(p++);
  }
}

void PrimaryLogPG::dec_refcount(ObjectContextRef obc, const object_ref_delta_t& refs)
{
  for (auto p = refs.begin(); p != refs.end(); ++p) {
    int dec_ref_count = p->second;
    ceph_assert(dec_ref_count < 0);
    while (dec_ref_count < 0) {
      dout(10) << __func__ << ": decrement reference on offset oid: " << p->first << dendl;
      refcount_manifest(obc->obs.oi.soid, p->first, 
			refcount_t::DECREMENT_REF, NULL);
      dec_ref_count++;
    }
  }
}


void PrimaryLogPG::dec_all_refcount_manifest(const object_info_t& oi, OpContext* ctx)
{
  ceph_assert(oi.has_manifest());
  ceph_assert(ctx->obc->ssc);

  if (oi.manifest.is_chunked()) {
    const SnapSet& snapset = ctx->obc->ssc->snapset;

    auto get_context = [this, &oi, &snapset](auto iter)
      -> ObjectContextRef {
      hobject_t cid = oi.soid;
      cid.snap = (iter == snapset.clones.end()) ? snapid_t(CEPH_NOSNAP) : *iter;
      ObjectContextRef obc = get_object_context(cid, false, NULL);
      ceph_assert(obc);
      ceph_assert(obc->obs.oi.has_manifest());
      ceph_assert(obc->obs.oi.manifest.is_chunked());
      return obc;
    };
    ObjectContextRef obc_l, obc_g;

    // check adjacent clones
    auto s = std::find(snapset.clones.begin(), snapset.clones.end(), oi.soid.snap);

    // We *must* find the clone iff it's not head,
    // let s == snapset.clones.end() mean head
    ceph_assert((s == snapset.clones.end()) == oi.soid.is_head()); 

    if (s != snapset.clones.begin()) {
      obc_l = get_context(s - 1);
    }

    if (s != snapset.clones.end()) {
      obc_g = get_context(s + 1);
    }

    object_ref_delta_t refs;
    oi.manifest.calc_refs_to_drop_on_removal(
      obc_l ? &(obc_l->obs.oi.manifest) : nullptr,
      obc_g ? &(obc_g->obs.oi.manifest) : nullptr,
      refs);

    if (!refs.is_empty()) {
      ctx->register_on_commit(
	[ctx, this, refs](){
	  dec_refcount(ctx->obc, refs);
	});
    }
  } else if (oi.manifest.is_redirect()) {
    ceph_assert(oi.flags & object_info_t::FLAG_REDIRECT_HAS_REFERENCE);
    ctx->register_on_commit(
      [oi, this](){
	refcount_manifest(oi.soid, oi.manifest.redirect_target, 
			  refcount_t::DECREMENT_REF, NULL);
      });
  } else {
    ceph_abort_msg("unrecognized manifest type");
  }
}

void PrimaryLogPG::refcount_manifest(hobject_t src_soid, hobject_t tgt_soid, refcount_t type, 
				     RefCountCallback* cb)
{
  unsigned flags = CEPH_OSD_FLAG_IGNORE_CACHE | CEPH_OSD_FLAG_IGNORE_OVERLAY |
                   CEPH_OSD_FLAG_RWORDERED;                      

  dout(10) << __func__ << " Start refcount from " << src_soid 
	   << " to " << tgt_soid << dendl;
    
  ObjectOperation obj_op;
  bufferlist in;
  if (type == refcount_t::INCREMENT_REF) {             
    cls_cas_chunk_get_ref_op call;
    call.source = src_soid.get_head();
    ::encode(call, in);                             
    obj_op.call("cas", "chunk_get_ref", in);
  } else if (type == refcount_t::DECREMENT_REF) {                    
    cls_cas_chunk_put_ref_op call;
    call.source = src_soid.get_head();
    ::encode(call, in);          
    obj_op.call("cas", "chunk_put_ref", in);
  }                                                     
  
  Context *c = nullptr;
  if (cb) {
    C_SetManifestRefCountDone *fin =
      new C_SetManifestRefCountDone(cb, src_soid);
    c = new C_OnFinisher(fin, osd->get_objecter_finisher(get_pg_shard()));
  }

  object_locator_t oloc(tgt_soid);
  ObjectContextRef src_obc = get_object_context(src_soid, false, NULL);
  ceph_assert(src_obc);
  auto tid = osd->objecter->mutate(
    tgt_soid.oid, oloc, obj_op, SnapContext(),
    ceph::real_clock::from_ceph_timespec(src_obc->obs.oi.mtime),
    flags, c);
  if (cb) {
    manifest_ops[src_soid] = std::make_shared<ManifestOp>(cb, tid);
    src_obc->start_block();
  }
}  

void PrimaryLogPG::do_proxy_chunked_read(OpRequestRef op, ObjectContextRef obc, int op_index,
					 uint64_t chunk_index, uint64_t req_offset, uint64_t req_length,
					 uint64_t req_total_len, bool write_ordered)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->get_nonconst_req());
  object_manifest_t *manifest = &obc->obs.oi.manifest;
  if (!manifest->chunk_map.count(chunk_index)) {
    return;
  } 
  uint64_t chunk_length = manifest->chunk_map[chunk_index].length;
  hobject_t soid = manifest->chunk_map[chunk_index].oid;
  hobject_t ori_soid = m->get_hobj();
  object_locator_t oloc(soid);
  unsigned flags = CEPH_OSD_FLAG_IGNORE_CACHE | CEPH_OSD_FLAG_IGNORE_OVERLAY;
  if (write_ordered) {
    flags |= CEPH_OSD_FLAG_RWORDERED;
  }
  
  if (!chunk_length || soid == hobject_t()) {
    return;
  }

  /* same as do_proxy_read() */
  flags |= m->get_flags() & (CEPH_OSD_FLAG_RWORDERED |
			     CEPH_OSD_FLAG_ORDERSNAP |
			     CEPH_OSD_FLAG_ENFORCE_SNAPC |
			     CEPH_OSD_FLAG_MAP_SNAP_CLONE);

  dout(10) << __func__ << " Start do chunk proxy read for " << *m 
	   << " index: " << op_index << " oid: " << soid.oid.name << " req_offset: " << req_offset 
	   << " req_length: " << req_length << dendl;

  ProxyReadOpRef prdop(std::make_shared<ProxyReadOp>(op, ori_soid, m->ops));

  ObjectOperation *pobj_op = new ObjectOperation;
  OSDOp &osd_op = pobj_op->add_op(m->ops[op_index].op.op);

  if (chunk_index <= req_offset) {
    osd_op.op.extent.offset = manifest->chunk_map[chunk_index].offset + req_offset - chunk_index;
  } else {
    ceph_abort_msg("chunk_index > req_offset");
  } 
  osd_op.op.extent.length = req_length; 

  ObjectOperation obj_op;
  obj_op.dup(pobj_op->ops);

  C_ProxyChunkRead *fin = new C_ProxyChunkRead(this, ori_soid, get_last_peering_reset(),
					       prdop);
  fin->obj_op = pobj_op;
  fin->op_index = op_index;
  fin->req_offset = req_offset;
  fin->obc = obc;
  fin->req_total_len = req_total_len;

  ceph_tid_t tid = osd->objecter->read(
    soid.oid, oloc, obj_op,
    m->get_snapid(), NULL,
    flags, new C_OnFinisher(fin, osd->get_objecter_finisher(get_pg_shard())),
    &prdop->user_version,
    &prdop->data_offset,
    m->get_features());
  fin->tid = tid;
  prdop->objecter_tid = tid;
  proxyread_ops[tid] = prdop;
  in_progress_proxy_ops[ori_soid].push_back(op);
}

bool PrimaryLogPG::can_proxy_chunked_read(OpRequestRef op, ObjectContextRef obc)
{
  MOSDOp *m = static_cast<MOSDOp*>(op->get_nonconst_req());
  OSDOp *osd_op = NULL;
  bool ret = true;
  for (unsigned int i = 0; i < m->ops.size(); i++) {
    osd_op = &m->ops[i];
    ceph_osd_op op = osd_op->op;
    switch (op.op) {
      case CEPH_OSD_OP_READ: 
      case CEPH_OSD_OP_SYNC_READ: {
	uint64_t cursor = osd_op->op.extent.offset;
	uint64_t remain = osd_op->op.extent.length;

	/* requested chunks exist in chunk_map ? */
	for (auto &p : obc->obs.oi.manifest.chunk_map) {
	  if (p.first <= cursor && p.first + p.second.length > cursor) {
	    if (!p.second.is_missing()) {
	      return false;
	    }
	    if (p.second.length >= remain) {
	      remain = 0;
	      break; 
	    } else {
	      remain = remain - p.second.length;
	    }
	    cursor += p.second.length;
	  }
	}
	
	if (remain) {
	  dout(20) << __func__ << " requested chunks don't exist in chunk_map " << dendl;
	  return false;
	}
	continue;
      }
      default:
	return false;
    }
  }
  return ret;
}

void PrimaryLogPG::finish_proxy_write(hobject_t oid, ceph_tid_t tid, int r)
{
  dout(10) << __func__ << " " << oid << " tid " << tid
	   << " " << cpp_strerror(r) << dendl;

  map<ceph_tid_t, ProxyWriteOpRef>::iterator p = proxywrite_ops.find(tid);
  if (p == proxywrite_ops.end()) {
    dout(10) << __func__ << " no proxywrite_op found" << dendl;
    return;
  }
  ProxyWriteOpRef pwop = p->second;
  ceph_assert(tid == pwop->objecter_tid);
  ceph_assert(oid == pwop->soid);

  proxywrite_ops.erase(tid);

  map<hobject_t, list<OpRequestRef> >::iterator q = in_progress_proxy_ops.find(oid);
  if (q == in_progress_proxy_ops.end()) {
    dout(10) << __func__ << " no in_progress_proxy_ops found" << dendl;
    delete pwop->ctx;
    pwop->ctx = NULL;
    return;
  }
  list<OpRequestRef>& in_progress_op = q->second;
  ceph_assert(in_progress_op.size());
  list<OpRequestRef>::iterator it = std::find(in_progress_op.begin(),
                                              in_progress_op.end(),
					      pwop->op);
  ceph_assert(it != in_progress_op.end());
  in_progress_op.erase(it);
  if (in_progress_op.size() == 0) {
    in_progress_proxy_ops.erase(oid);
  } else if (std::find(in_progress_op.begin(),
                        in_progress_op.end(),
                        pwop->op) != in_progress_op.end()) {
    if (pwop->ctx)
      delete pwop->ctx;
    pwop->ctx = NULL;
    dout(20) << __func__ << " " << oid << " tid " << tid
            << " in_progress_op size: "
            << in_progress_op.size() << dendl;
    return;
  }

  osd->logger->inc(l_osd_tier_proxy_write);

  auto m = pwop->op->get_req<MOSDOp>();
  ceph_assert(m != NULL);

  if (!pwop->sent_reply) {
    // send commit.
    assert(pwop->ctx->reply == nullptr);
    MOSDOpReply *reply = new MOSDOpReply(m, r, get_osdmap_epoch(), 0,
					 true /* we claim it below */);
    reply->set_reply_versions(eversion_t(), pwop->user_version);
    reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
    reply->claim_op_out_data(pwop->ops);
    dout(10) << " sending commit on " << pwop << " " << reply << dendl;
    osd->send_message_osd_client(reply, m->get_connection());
    pwop->sent_reply = true;
    pwop->ctx->op->mark_commit_sent();
  }

  delete pwop->ctx;
  pwop->ctx = NULL;
}

void PrimaryLogPG::cancel_proxy_write(ProxyWriteOpRef pwop,
				      vector<ceph_tid_t> *tids)
{
  dout(10) << __func__ << " " << pwop->soid << dendl;
  pwop->canceled = true;

  // cancel objecter op, if we can
  if (pwop->objecter_tid) {
    tids->push_back(pwop->objecter_tid);
    delete pwop->ctx;
    pwop->ctx = NULL;
    proxywrite_ops.erase(pwop->objecter_tid);
    pwop->objecter_tid = 0;
  }
}

class PromoteCallback: public PrimaryLogPG::CopyCallback {
  ObjectContextRef obc;
  PrimaryLogPG *pg;
  utime_t start;
public:
  PromoteCallback(ObjectContextRef obc_, PrimaryLogPG *pg_)
    : obc(obc_),
      pg(pg_),
      start(ceph_clock_now()) {}

  void finish(PrimaryLogPG::CopyCallbackResults results) override {
    PrimaryLogPG::CopyResults *results_data = results.get<1>();
    int r = results.get<0>();
    pg->finish_promote(r, results_data, obc);
    pg->osd->logger->tinc(l_osd_tier_promote_lat, ceph_clock_now() - start);
  }
};

class PromoteManifestCallback: public PrimaryLogPG::CopyCallback {
  ObjectContextRef obc;
  PrimaryLogPG *pg;
  utime_t start;
  PrimaryLogPG::OpContext *ctx;
  PrimaryLogPG::CopyCallbackResults promote_results;
public:
  PromoteManifestCallback(ObjectContextRef obc_, PrimaryLogPG *pg_, PrimaryLogPG::OpContext *ctx = NULL)
    : obc(obc_),
      pg(pg_),
      start(ceph_clock_now()), ctx(ctx) {}

  void finish(PrimaryLogPG::CopyCallbackResults results) override {
    PrimaryLogPG::CopyResults *results_data = results.get<1>();
    int r = results.get<0>();
    if (ctx) {
      promote_results = results;
      pg->execute_ctx(ctx);
    } else {
      pg->finish_promote_manifest(r, results_data, obc);
    }
    pg->osd->logger->tinc(l_osd_tier_promote_lat, ceph_clock_now() - start);
  }
  friend struct PromoteFinisher;
};

struct PromoteFinisher : public PrimaryLogPG::OpFinisher {
  PromoteManifestCallback *promote_callback;

  explicit PromoteFinisher(PromoteManifestCallback *promote_callback)
    : promote_callback(promote_callback) {
  }

  int execute() override {
    if (promote_callback->ctx->obc->obs.oi.manifest.is_redirect()) {
      promote_callback->ctx->pg->finish_promote(promote_callback->promote_results.get<0>(),
						promote_callback->promote_results.get<1>(),
						promote_callback->obc);
    } else if (promote_callback->ctx->obc->obs.oi.manifest.is_chunked()) {
      promote_callback->ctx->pg->finish_promote_manifest(promote_callback->promote_results.get<0>(),
						promote_callback->promote_results.get<1>(),
						promote_callback->obc);
    } else {
      ceph_abort_msg("unrecognized manifest type");
    }
    return 0;
  }
};

void PrimaryLogPG::promote_object(ObjectContextRef obc,
				  const hobject_t& missing_oid,
				  const object_locator_t& oloc,
				  OpRequestRef op,
				  ObjectContextRef *promote_obc)
{
  hobject_t hoid = obc ? obc->obs.oi.soid : missing_oid;
  ceph_assert(hoid != hobject_t());
  if (write_blocked_by_scrub(hoid)) {
    dout(10) << __func__ << " " << hoid
	     << " blocked by scrub" << dendl;
    if (op) {
      waiting_for_scrub.push_back(op);
      op->mark_delayed("waiting for scrub");
      dout(10) << __func__ << " " << hoid
	       << " placing op in waiting_for_scrub" << dendl;
    } else {
      dout(10) << __func__ << " " << hoid
	       << " no op, dropping on the floor" << dendl;
    }
    return;
  }
  if (op && !check_laggy_requeue(op)) {
    return;
  }
  if (!obc) { // we need to create an ObjectContext
    ceph_assert(missing_oid != hobject_t());
    obc = get_object_context(missing_oid, true);
  }
  if (promote_obc)
    *promote_obc = obc;

  /*
   * Before promote complete, if there are  proxy-reads for the object,
   * for this case we don't use DONTNEED.
   */
  unsigned src_fadvise_flags = LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL;
  map<hobject_t, list<OpRequestRef>>::iterator q = in_progress_proxy_ops.find(obc->obs.oi.soid);
  if (q == in_progress_proxy_ops.end()) {
    src_fadvise_flags |= LIBRADOS_OP_FLAG_FADVISE_DONTNEED;
  }

  CopyCallback *cb;
  object_locator_t my_oloc;
  hobject_t src_hoid;
  if (!obc->obs.oi.has_manifest()) {
    my_oloc = oloc;
    my_oloc.pool = pool.info.tier_of;
    src_hoid = obc->obs.oi.soid;
    cb = new PromoteCallback(obc, this);
  } else {
    if (obc->obs.oi.manifest.is_chunked()) {
      src_hoid = obc->obs.oi.soid;
      cb = new PromoteManifestCallback(obc, this);
    } else if (obc->obs.oi.manifest.is_redirect()) {
      object_locator_t src_oloc(obc->obs.oi.manifest.redirect_target);
      my_oloc = src_oloc;
      src_hoid = obc->obs.oi.manifest.redirect_target;
      cb = new PromoteCallback(obc, this);
    } else {
      ceph_abort_msg("unrecognized manifest type");
    }
  }

  unsigned flags = CEPH_OSD_COPY_FROM_FLAG_IGNORE_OVERLAY |
                   CEPH_OSD_COPY_FROM_FLAG_IGNORE_CACHE |
                   CEPH_OSD_COPY_FROM_FLAG_MAP_SNAP_CLONE |
                   CEPH_OSD_COPY_FROM_FLAG_RWORDERED;
  start_copy(cb, obc, src_hoid, my_oloc, 0, flags,
	     obc->obs.oi.soid.snap == CEPH_NOSNAP,
	     src_fadvise_flags, 0);

  ceph_assert(obc->is_blocked());

  if (op)
    wait_for_blocked_object(obc->obs.oi.soid, op);

  recovery_state.update_stats(
    [](auto &history, auto &stats) {
      stats.stats.sum.num_promote++;
      return false;
    });
}

void PrimaryLogPG::execute_ctx(OpContext *ctx)
{
  FUNCTRACE(cct);
  dout(10) << __func__ << " " << ctx << dendl;
  ctx->reset_obs(ctx->obc);
  ctx->update_log_only = false; // reset in case finish_copyfrom() is re-running execute_ctx
  OpRequestRef op = ctx->op;
  auto m = op->get_req<MOSDOp>();
  ObjectContextRef obc = ctx->obc;
  const hobject_t& soid = obc->obs.oi.soid;

  // this method must be idempotent since we may call it several times
  // before we finally apply the resulting transaction.
  ctx->op_t.reset(new PGTransaction);

  if (op->may_write() || op->may_cache()) {
    // snap
    if (!(m->has_flag(CEPH_OSD_FLAG_ENFORCE_SNAPC)) &&
	pool.info.is_pool_snaps_mode()) {
      // use pool's snapc
      ctx->snapc = pool.snapc;
    } else {
      // client specified snapc
      ctx->snapc.seq = m->get_snap_seq();
      ctx->snapc.snaps = m->get_snaps();
      filter_snapc(ctx->snapc.snaps);
    }
    if ((m->has_flag(CEPH_OSD_FLAG_ORDERSNAP)) &&
	ctx->snapc.seq < obc->ssc->snapset.seq) {
      dout(10) << " ORDERSNAP flag set and snapc seq " << ctx->snapc.seq
	       << " < snapset seq " << obc->ssc->snapset.seq
	       << " on " << obc->obs.oi.soid << dendl;
      reply_ctx(ctx, -EOLDSNAPC);
      return;
    }

    // version
    ctx->at_version = get_next_version();
    ctx->mtime = m->get_mtime();

    dout(10) << __func__ << " " << soid << " " << *ctx->ops
	     << " ov " << obc->obs.oi.version << " av " << ctx->at_version 
	     << " snapc " << ctx->snapc
	     << " snapset " << obc->ssc->snapset
	     << dendl;  
  } else {
    dout(10) << __func__ << " " << soid << " " << *ctx->ops
	     << " ov " << obc->obs.oi.version
	     << dendl;  
  }

  if (!ctx->user_at_version)
    ctx->user_at_version = obc->obs.oi.user_version;
  dout(30) << __func__ << " user_at_version " << ctx->user_at_version << dendl;

  {
#ifdef WITH_LTTNG
    osd_reqid_t reqid = ctx->op->get_reqid();
#endif
    tracepoint(osd, prepare_tx_enter, reqid.name._type,
        reqid.name._num, reqid.tid, reqid.inc);
  }

  int result = prepare_transaction(ctx);

  {
#ifdef WITH_LTTNG
    osd_reqid_t reqid = ctx->op->get_reqid();
#endif
    tracepoint(osd, prepare_tx_exit, reqid.name._type,
        reqid.name._num, reqid.tid, reqid.inc);
  }

  bool pending_async_reads = !ctx->pending_async_reads.empty();
  if (result == -EINPROGRESS || pending_async_reads) {
    // come back later.
    if (pending_async_reads) {
      ceph_assert(pool.info.is_erasure());
      in_progress_async_reads.push_back(make_pair(op, ctx));
      ctx->start_async_reads(this);
    }
    return;
  }

  if (result == -EAGAIN) {
    // clean up after the ctx
    close_op_ctx(ctx);
    return;
  }

  bool ignore_out_data = false;
  if (!ctx->op_t->empty() &&
      op->may_write() &&
      result >= 0) {
    // successful update
    if (ctx->op->allows_returnvec()) {
      // enforce reasonable bound on the return buffer sizes
      for (auto& i : *ctx->ops) {
	if (i.outdata.length() > cct->_conf->osd_max_write_op_reply_len) {
	  dout(10) << __func__ << " op " << i << " outdata overflow" << dendl;
	  result = -EOVERFLOW;  // overall result is overflow
	  i.rval = -EOVERFLOW;
	  i.outdata.clear();
	}
      }
    } else {
      // legacy behavior -- zero result and return data etc.
      ignore_out_data = true;
      result = 0;
    }
  }

  // prepare the reply
  ctx->reply = new MOSDOpReply(m, result, get_osdmap_epoch(), 0,
			       ignore_out_data);
  dout(20) << __func__ << " alloc reply " << ctx->reply
	   << " result " << result << dendl;

  // read or error?
  if ((ctx->op_t->empty() || result < 0) && !ctx->update_log_only) {
    // finish side-effects
    if (result >= 0)
      do_osd_op_effects(ctx, m->get_connection());

    complete_read_ctx(result, ctx);
    return;
  }

  ctx->reply->set_reply_versions(ctx->at_version, ctx->user_at_version);

  ceph_assert(op->may_write() || op->may_cache());

  // trim log?
  recovery_state.update_trim_to();

  // verify that we are doing this in order?
  if (cct->_conf->osd_debug_op_order && m->get_source().is_client() &&
      !pool.info.is_tier() && !pool.info.has_tiers()) {
    map<client_t,ceph_tid_t>& cm = debug_op_order[obc->obs.oi.soid];
    ceph_tid_t t = m->get_tid();
    client_t n = m->get_source().num();
    map<client_t,ceph_tid_t>::iterator p = cm.find(n);
    if (p == cm.end()) {
      dout(20) << " op order client." << n << " tid " << t << " (first)" << dendl;
      cm[n] = t;
    } else {
      dout(20) << " op order client." << n << " tid " << t << " last was " << p->second << dendl;
      if (p->second > t) {
	derr << "bad op order, already applied " << p->second << " > this " << t << dendl;
	ceph_abort_msg("out of order op");
      }
      p->second = t;
    }
  }

  if (ctx->update_log_only) {
    if (result >= 0)
      do_osd_op_effects(ctx, m->get_connection());

    dout(20) << __func__ << " update_log_only -- result=" << result << dendl;
    // save just what we need from ctx
    MOSDOpReply *reply = ctx->reply;
    ctx->reply = nullptr;
    reply->get_header().data_off = (ctx->data_off ? *ctx->data_off : 0);

    if (result == -ENOENT) {
      reply->set_enoent_reply_versions(info.last_update,
				       info.last_user_version);
    }
    reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
    // append to pg log for dup detection - don't save buffers for now
    record_write_error(op, soid, reply, result,
		       ctx->op->allows_returnvec() ? ctx : nullptr);
    close_op_ctx(ctx);
    return;
  }

  // no need to capture PG ref, repop cancel will handle that
  // Can capture the ctx by pointer, it's owned by the repop
  ctx->register_on_commit(
    [m, ctx, this](){
      if (ctx->op)
	log_op_stats(*ctx->op, ctx->bytes_written, ctx->bytes_read);

      if (m && !ctx->sent_reply) {
	MOSDOpReply *reply = ctx->reply;
	ctx->reply = nullptr;
	reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
	dout(10) << " sending reply on " << *m << " " << reply << dendl;
	osd->send_message_osd_client(reply, m->get_connection());
	ctx->sent_reply = true;
	ctx->op->mark_commit_sent();
      }
    });
  ctx->register_on_success(
    [ctx, this]() {
      do_osd_op_effects(
	ctx,
	ctx->op ? ctx->op->get_req()->get_connection() :
	ConnectionRef());
    });
  ctx->register_on_finish(
    [ctx]() {
      delete ctx;
    });

  // issue replica writes
  ceph_tid_t rep_tid = osd->get_tid();

  RepGather *repop = new_repop(ctx, obc, rep_tid);

  issue_repop(repop, ctx);
  eval_repop(repop);
  repop->put();
}

void PrimaryLogPG::close_op_ctx(OpContext *ctx) {
  release_object_locks(ctx->lock_manager);

  ctx->op_t.reset();

  for (auto p = ctx->on_finish.begin(); p != ctx->on_finish.end();
       ctx->on_finish.erase(p++)) {
    (*p)();
  }
  delete ctx;
}

void PrimaryLogPG::reply_ctx(OpContext *ctx, int r)
{
  if (ctx->op)
    osd->reply_op_error(ctx->op, r);
  close_op_ctx(ctx);
}

void PrimaryLogPG::log_op_stats(const OpRequest& op,
				const uint64_t inb,
				const uint64_t outb)
{
  auto m = op.get_req<MOSDOp>();
  const utime_t now = ceph_clock_now();

  const utime_t latency = now - m->get_recv_stamp();
  const utime_t process_latency = now - op.get_dequeued_time();

  osd->logger->inc(l_osd_op);

  osd->logger->inc(l_osd_op_outb, outb);
  osd->logger->inc(l_osd_op_inb, inb);
  osd->logger->tinc(l_osd_op_lat, latency);
  osd->logger->tinc(l_osd_op_process_lat, process_latency);

  if (op.may_read() && op.may_write()) {
    osd->logger->inc(l_osd_op_rw);
    osd->logger->inc(l_osd_op_rw_inb, inb);
    osd->logger->inc(l_osd_op_rw_outb, outb);
    osd->logger->tinc(l_osd_op_rw_lat, latency);
    osd->logger->hinc(l_osd_op_rw_lat_inb_hist, latency.to_nsec(), inb);
    osd->logger->hinc(l_osd_op_rw_lat_outb_hist, latency.to_nsec(), outb);
    osd->logger->tinc(l_osd_op_rw_process_lat, process_latency);
  } else if (op.may_read()) {
    osd->logger->inc(l_osd_op_r);
    osd->logger->inc(l_osd_op_r_outb, outb);
    osd->logger->tinc(l_osd_op_r_lat, latency);
    osd->logger->hinc(l_osd_op_r_lat_outb_hist, latency.to_nsec(), outb);
    osd->logger->tinc(l_osd_op_r_process_lat, process_latency);
  } else if (op.may_write() || op.may_cache()) {
    osd->logger->inc(l_osd_op_w);
    osd->logger->inc(l_osd_op_w_inb, inb);
    osd->logger->tinc(l_osd_op_w_lat, latency);
    osd->logger->hinc(l_osd_op_w_lat_inb_hist, latency.to_nsec(), inb);
    osd->logger->tinc(l_osd_op_w_process_lat, process_latency);
  } else {
    ceph_abort();
  }

  dout(15) << "log_op_stats " << *m
	   << " inb " << inb
	   << " outb " << outb
	   << " lat " << latency << dendl;

  if (m_dynamic_perf_stats.is_enabled()) {
    m_dynamic_perf_stats.add(osd, info, op, inb, outb, latency);
  }
}

void PrimaryLogPG::set_dynamic_perf_stats_queries(
    const std::list<OSDPerfMetricQuery> &queries)
{
  m_dynamic_perf_stats.set_queries(queries);
}

void PrimaryLogPG::get_dynamic_perf_stats(DynamicPerfStats *stats)
{
  std::swap(m_dynamic_perf_stats, *stats);
}

void PrimaryLogPG::do_scan(
  OpRequestRef op,
  ThreadPool::TPHandle &handle)
{
  auto m = op->get_req<MOSDPGScan>();
  ceph_assert(m->get_type() == MSG_OSD_PG_SCAN);
  dout(10) << "do_scan " << *m << dendl;

  op->mark_started();

  switch (m->op) {
  case MOSDPGScan::OP_SCAN_GET_DIGEST:
    {
      auto dpp = get_dpp();
      if (osd->check_backfill_full(dpp)) {
	dout(1) << __func__ << ": Canceling backfill: Full." << dendl;
	queue_peering_event(
	  PGPeeringEventRef(
	    std::make_shared<PGPeeringEvent>(
	      get_osdmap_epoch(),
	      get_osdmap_epoch(),
	      PeeringState::BackfillTooFull())));
	return;
      }

      BackfillInterval bi;
      bi.begin = m->begin;
      // No need to flush, there won't be any in progress writes occuring
      // past m->begin
      scan_range(
	cct->_conf->osd_backfill_scan_min,
	cct->_conf->osd_backfill_scan_max,
	&bi,
	handle);
      MOSDPGScan *reply = new MOSDPGScan(
	MOSDPGScan::OP_SCAN_DIGEST,
	pg_whoami,
	get_osdmap_epoch(), m->query_epoch,
	spg_t(info.pgid.pgid, get_primary().shard), bi.begin, bi.end);
      encode(bi.objects, reply->get_data());
      osd->send_message_osd_cluster(reply, m->get_connection());
    }
    break;

  case MOSDPGScan::OP_SCAN_DIGEST:
    {
      pg_shard_t from = m->from;

      // Check that from is in backfill_targets vector
      ceph_assert(is_backfill_target(from));

      BackfillInterval& bi = peer_backfill_info[from];
      bi.begin = m->begin;
      bi.end = m->end;
      auto p = m->get_data().cbegin();

      // take care to preserve ordering!
      bi.clear_objects();
      decode_noclear(bi.objects, p);

      if (waiting_on_backfill.erase(from)) {
	if (waiting_on_backfill.empty()) {
	  ceph_assert(
	    peer_backfill_info.size() ==
	    get_backfill_targets().size());
	  finish_recovery_op(hobject_t::get_max());
	}
      } else {
	// we canceled backfill for a while due to a too full, and this
	// is an extra response from a non-too-full peer
	dout(20) << __func__ << " canceled backfill (too full?)" << dendl;
      }
    }
    break;
  }
}

void PrimaryLogPG::do_backfill(OpRequestRef op)
{
  auto m = op->get_req<MOSDPGBackfill>();
  ceph_assert(m->get_type() == MSG_OSD_PG_BACKFILL);
  dout(10) << "do_backfill " << *m << dendl;

  op->mark_started();

  switch (m->op) {
  case MOSDPGBackfill::OP_BACKFILL_FINISH:
    {
      ceph_assert(cct->_conf->osd_kill_backfill_at != 1);

      MOSDPGBackfill *reply = new MOSDPGBackfill(
	MOSDPGBackfill::OP_BACKFILL_FINISH_ACK,
	get_osdmap_epoch(),
	m->query_epoch,
	spg_t(info.pgid.pgid, get_primary().shard));
      reply->set_priority(get_recovery_op_priority());
      osd->send_message_osd_cluster(reply, m->get_connection());
      queue_peering_event(
	PGPeeringEventRef(
	  std::make_shared<PGPeeringEvent>(
	    get_osdmap_epoch(),
	    get_osdmap_epoch(),
	    RecoveryDone())));
    }
    // fall-thru

  case MOSDPGBackfill::OP_BACKFILL_PROGRESS:
    {
      ceph_assert(cct->_conf->osd_kill_backfill_at != 2);

      ObjectStore::Transaction t;
      recovery_state.update_backfill_progress(
	m->last_backfill,
	m->stats,
	m->op == MOSDPGBackfill::OP_BACKFILL_PROGRESS,
	t);

      int tr = osd->store->queue_transaction(ch, std::move(t), NULL);
      ceph_assert(tr == 0);
    }
    break;

  case MOSDPGBackfill::OP_BACKFILL_FINISH_ACK:
    {
      ceph_assert(is_primary());
      ceph_assert(cct->_conf->osd_kill_backfill_at != 3);
      finish_recovery_op(hobject_t::get_max());
    }
    break;
  }
}

void PrimaryLogPG::do_backfill_remove(OpRequestRef op)
{
  const MOSDPGBackfillRemove *m = static_cast<const MOSDPGBackfillRemove*>(
    op->get_req());
  ceph_assert(m->get_type() == MSG_OSD_PG_BACKFILL_REMOVE);
  dout(7) << __func__ << " " << m->ls << dendl;

  op->mark_started();

  ObjectStore::Transaction t;
  for (auto& p : m->ls) {
    if (is_remote_backfilling()) {
      struct stat st;
      int r = osd->store->stat(ch, ghobject_t(p.first, ghobject_t::NO_GEN,
                               pg_whoami.shard) , &st);
      if (r == 0) {
        sub_local_num_bytes(st.st_size);
        int64_t usersize;
        if (pool.info.is_erasure()) {
          bufferlist bv;
	  int r = osd->store->getattr(
	      ch,
              ghobject_t(p.first, ghobject_t::NO_GEN, pg_whoami.shard),
	      OI_ATTR,
	      bv);
	  if (r >= 0) {
	    object_info_t oi(bv);
            usersize = oi.size * pgbackend->get_ec_data_chunk_count();
          } else {
            dout(0) << __func__ << " " << ghobject_t(p.first, ghobject_t::NO_GEN, pg_whoami.shard)
                    << " can't get object info" << dendl;
            usersize = 0;
          }
        } else {
          usersize = st.st_size;
        }
        sub_num_bytes(usersize);
        dout(10) << __func__ << " " << ghobject_t(p.first, ghobject_t::NO_GEN, pg_whoami.shard)
                 << " sub actual data by " << st.st_size
                 << " sub num_bytes by " << usersize
                 << dendl;
      }
    }
    remove_snap_mapped_object(t, p.first);
  }
  int r = osd->store->queue_transaction(ch, std::move(t), NULL);
  ceph_assert(r == 0);
}

int PrimaryLogPG::trim_object(
  bool first, const hobject_t &coid, snapid_t snap_to_trim,
  PrimaryLogPG::OpContextUPtr *ctxp)
{
  *ctxp = NULL;

  // load clone info
  bufferlist bl;
  ObjectContextRef obc = get_object_context(coid, false, NULL);
  if (!obc || !obc->ssc || !obc->ssc->exists) {
    osd->clog->error() << __func__ << ": Can not trim " << coid
      << " repair needed " << (obc ? "(no obc->ssc or !exists)" : "(no obc)");
    return -ENOENT;
  }

  hobject_t head_oid = coid.get_head();
  ObjectContextRef head_obc = get_object_context(head_oid, false);
  if (!head_obc) {
    osd->clog->error() << __func__ << ": Can not trim " << coid
      << " repair needed, no snapset obc for " << head_oid;
    return -ENOENT;
  }

  SnapSet& snapset = obc->ssc->snapset;

  object_info_t &coi = obc->obs.oi;
  auto citer = snapset.clone_snaps.find(coid.snap);
  if (citer == snapset.clone_snaps.end()) {
    osd->clog->error() << "No clone_snaps in snapset " << snapset
		       << " for object " << coid << "\n";
    return -ENOENT;
  }
  set<snapid_t> old_snaps(citer->second.begin(), citer->second.end());
  if (old_snaps.empty()) {
    osd->clog->error() << "No object info snaps for object " << coid;
    return -ENOENT;
  }

  dout(10) << coid << " old_snaps " << old_snaps
	   << " old snapset " << snapset << dendl;
  if (snapset.seq == 0) {
    osd->clog->error() << "No snapset.seq for object " << coid;
    return -ENOENT;
  }

  set<snapid_t> new_snaps;
  const OSDMapRef& osdmap = get_osdmap();
  for (set<snapid_t>::iterator i = old_snaps.begin();
       i != old_snaps.end();
       ++i) {
    if (!osdmap->in_removed_snaps_queue(info.pgid.pgid.pool(), *i) &&
	*i != snap_to_trim) {
      new_snaps.insert(*i);
    }
  }

  vector<snapid_t>::iterator p = snapset.clones.end();

  if (new_snaps.empty()) {
    p = std::find(snapset.clones.begin(), snapset.clones.end(), coid.snap);
    if (p == snapset.clones.end()) {
      osd->clog->error() << "Snap " << coid.snap << " not in clones";
      return -ENOENT;
    }
  }

  OpContextUPtr ctx = simple_opc_create(obc);
  ctx->head_obc = head_obc;

  if (!ctx->lock_manager.get_snaptrimmer_write(
	coid,
	obc,
	first)) {
    close_op_ctx(ctx.release());
    dout(10) << __func__ << ": Unable to get a wlock on " << coid << dendl;
    return -ENOLCK;
  }

  if (!ctx->lock_manager.get_snaptrimmer_write(
	head_oid,
	head_obc,
	first)) {
    close_op_ctx(ctx.release());
    dout(10) << __func__ << ": Unable to get a wlock on " << head_oid << dendl;
    return -ENOLCK;
  }

  ctx->at_version = get_next_version();

  PGTransaction *t = ctx->op_t.get();
 
  if (new_snaps.empty()) {
    // remove clone
    dout(10) << coid << " snaps " << old_snaps << " -> "
	     << new_snaps << " ... deleting" << dendl;

    // ...from snapset
    ceph_assert(p != snapset.clones.end());
  
    snapid_t last = coid.snap;
    ctx->delta_stats.num_bytes -= snapset.get_clone_bytes(last);

    if (p != snapset.clones.begin()) {
      // not the oldest... merge overlap into next older clone
      vector<snapid_t>::iterator n = p - 1;
      hobject_t prev_coid = coid;
      prev_coid.snap = *n;
      bool adjust_prev_bytes = is_present_clone(prev_coid);

      if (adjust_prev_bytes)
	ctx->delta_stats.num_bytes -= snapset.get_clone_bytes(*n);

      snapset.clone_overlap[*n].intersection_of(
	snapset.clone_overlap[*p]);

      if (adjust_prev_bytes)
	ctx->delta_stats.num_bytes += snapset.get_clone_bytes(*n);
    }
    ctx->delta_stats.num_objects--;
    if (coi.is_dirty())
      ctx->delta_stats.num_objects_dirty--;
    if (coi.is_omap())
      ctx->delta_stats.num_objects_omap--;
    if (coi.is_whiteout()) {
      dout(20) << __func__ << " trimming whiteout on " << coid << dendl;
      ctx->delta_stats.num_whiteouts--;
    }
    ctx->delta_stats.num_object_clones--;
    if (coi.is_cache_pinned())
      ctx->delta_stats.num_objects_pinned--;
    if (coi.has_manifest()) {
      dec_all_refcount_manifest(coi, ctx.get());
      ctx->delta_stats.num_objects_manifest--;
    }
    obc->obs.exists = false;

    snapset.clones.erase(p);
    snapset.clone_overlap.erase(last);
    snapset.clone_size.erase(last);
    snapset.clone_snaps.erase(last);
	
    ctx->log.push_back(
      pg_log_entry_t(
	pg_log_entry_t::DELETE,
	coid,
	ctx->at_version,
	ctx->obs->oi.version,
	0,
	osd_reqid_t(),
	ctx->mtime,
	0)
      );
    t->remove(coid);
    t->update_snaps(
      coid,
      old_snaps,
      new_snaps);

    coi = object_info_t(coid);

    ctx->at_version.version++;
  } else {
    // save adjusted snaps for this object
    dout(10) << coid << " snaps " << old_snaps << " -> " << new_snaps << dendl;
    snapset.clone_snaps[coid.snap] =
      vector<snapid_t>(new_snaps.rbegin(), new_snaps.rend());
    // we still do a 'modify' event on this object just to trigger a
    // snapmapper.update ... :(

    coi.prior_version = coi.version;
    coi.version = ctx->at_version;
    bl.clear();
    encode(coi, bl, get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
    t->setattr(coid, OI_ATTR, bl);

    ctx->log.push_back(
      pg_log_entry_t(
	pg_log_entry_t::MODIFY,
	coid,
	coi.version,
	coi.prior_version,
	0,
	osd_reqid_t(),
	ctx->mtime,
	0)
      );
    ctx->at_version.version++;

    t->update_snaps(
      coid,
      old_snaps,
      new_snaps);
  }

  // save head snapset
  dout(10) << coid << " new snapset " << snapset << " on "
	   << head_obc->obs.oi << dendl;
  if (snapset.clones.empty() &&
      (head_obc->obs.oi.is_whiteout() &&
       !(head_obc->obs.oi.is_dirty() && pool.info.is_tier()) &&
       !head_obc->obs.oi.is_cache_pinned())) {
    // NOTE: this arguably constitutes minor interference with the
    // tiering agent if this is a cache tier since a snap trim event
    // is effectively evicting a whiteout we might otherwise want to
    // keep around.
    dout(10) << coid << " removing " << head_oid << dendl;
    ctx->log.push_back(
      pg_log_entry_t(
	pg_log_entry_t::DELETE,
	head_oid,
	ctx->at_version,
	head_obc->obs.oi.version,
	0,
	osd_reqid_t(),
	ctx->mtime,
	0)
      );
    derr << "removing snap head" << dendl;
    object_info_t& oi = head_obc->obs.oi;
    ctx->delta_stats.num_objects--;
    if (oi.is_dirty()) {
      ctx->delta_stats.num_objects_dirty--;
    }
    if (oi.is_omap())
      ctx->delta_stats.num_objects_omap--;
    if (oi.is_whiteout()) {
      dout(20) << __func__ << " trimming whiteout on " << oi.soid << dendl;
      ctx->delta_stats.num_whiteouts--;
    }
    if (oi.is_cache_pinned()) {
      ctx->delta_stats.num_objects_pinned--;
    }
    if (oi.has_manifest()) {
      ctx->delta_stats.num_objects_manifest--;
      dec_all_refcount_manifest(oi, ctx.get());
    }
    head_obc->obs.exists = false;
    head_obc->obs.oi = object_info_t(head_oid);
    t->remove(head_oid);
  } else {
    if (get_osdmap()->require_osd_release < ceph_release_t::octopus) {
      // filter SnapSet::snaps for the benefit of pre-octopus
      // peers. This is perhaps overly conservative in that I'm not
      // certain they need this, but let's be conservative here.
      dout(10) << coid << " filtering snapset on " << head_oid << dendl;
      snapset.filter(pool.info);
    } else {
      snapset.snaps.clear();
    }
    dout(10) << coid << " writing updated snapset on " << head_oid
	     << ", snapset is " << snapset << dendl;
    ctx->log.push_back(
      pg_log_entry_t(
	pg_log_entry_t::MODIFY,
	head_oid,
	ctx->at_version,
	head_obc->obs.oi.version,
	0,
	osd_reqid_t(),
	ctx->mtime,
	0)
      );

    head_obc->obs.oi.prior_version = head_obc->obs.oi.version;
    head_obc->obs.oi.version = ctx->at_version;

    map <string, bufferlist> attrs;
    bl.clear();
    encode(snapset, bl);
    attrs[SS_ATTR].claim(bl);

    bl.clear();
    encode(head_obc->obs.oi, bl,
	     get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
    attrs[OI_ATTR].claim(bl);
    t->setattrs(head_oid, attrs);
  }

  *ctxp = std::move(ctx);
  return 0;
}

void PrimaryLogPG::kick_snap_trim()
{
  ceph_assert(is_active());
  ceph_assert(is_primary());
  if (is_clean() &&
      !state_test(PG_STATE_PREMERGE) &&
      !snap_trimq.empty()) {
    if (get_osdmap()->test_flag(CEPH_OSDMAP_NOSNAPTRIM)) {
      dout(10) << __func__ << ": nosnaptrim set, not kicking" << dendl;
    } else {
      dout(10) << __func__ << ": clean and snaps to trim, kicking" << dendl;
      snap_trimmer_machine.process_event(KickTrim());
    }
  }
}

void PrimaryLogPG::snap_trimmer_scrub_complete()
{
  if (is_primary() && is_active() && is_clean()) {
    ceph_assert(!snap_trimq.empty());
    snap_trimmer_machine.process_event(ScrubComplete());
  }
}

void PrimaryLogPG::snap_trimmer(epoch_t queued)
{
  if (recovery_state.is_deleting() || pg_has_reset_since(queued)) {
    return;
  }

  ceph_assert(is_primary());

  dout(10) << "snap_trimmer posting" << dendl;
  snap_trimmer_machine.process_event(DoSnapWork());
  dout(10) << "snap_trimmer complete" << dendl;
  return;
}

int PrimaryLogPG::do_xattr_cmp_u64(int op, __u64 v1, bufferlist& xattr)
{
  __u64 v2;

  string v2s(xattr.c_str(), xattr.length());
  if (v2s.length())
    v2 = strtoull(v2s.c_str(), NULL, 10);
  else
    v2 = 0;

  dout(20) << "do_xattr_cmp_u64 '" << v1 << "' vs '" << v2 << "' op " << op << dendl;

  switch (op) {
  case CEPH_OSD_CMPXATTR_OP_EQ:
    return (v1 == v2);
  case CEPH_OSD_CMPXATTR_OP_NE:
    return (v1 != v2);
  case CEPH_OSD_CMPXATTR_OP_GT:
    return (v1 > v2);
  case CEPH_OSD_CMPXATTR_OP_GTE:
    return (v1 >= v2);
  case CEPH_OSD_CMPXATTR_OP_LT:
    return (v1 < v2);
  case CEPH_OSD_CMPXATTR_OP_LTE:
    return (v1 <= v2);
  default:
    return -EINVAL;
  }
}

int PrimaryLogPG::do_xattr_cmp_str(int op, string& v1s, bufferlist& xattr)
{
  string v2s(xattr.c_str(), xattr.length());

  dout(20) << "do_xattr_cmp_str '" << v1s << "' vs '" << v2s << "' op " << op << dendl;

  switch (op) {
  case CEPH_OSD_CMPXATTR_OP_EQ:
    return (v1s.compare(v2s) == 0);
  case CEPH_OSD_CMPXATTR_OP_NE:
    return (v1s.compare(v2s) != 0);
  case CEPH_OSD_CMPXATTR_OP_GT:
    return (v1s.compare(v2s) > 0);
  case CEPH_OSD_CMPXATTR_OP_GTE:
    return (v1s.compare(v2s) >= 0);
  case CEPH_OSD_CMPXATTR_OP_LT:
    return (v1s.compare(v2s) < 0);
  case CEPH_OSD_CMPXATTR_OP_LTE:
    return (v1s.compare(v2s) <= 0);
  default:
    return -EINVAL;
  }
}

int PrimaryLogPG::do_writesame(OpContext *ctx, OSDOp& osd_op)
{
  ceph_osd_op& op = osd_op.op;
  vector<OSDOp> write_ops(1);
  OSDOp& write_op = write_ops[0];
  uint64_t write_length = op.writesame.length;
  int result = 0;

  if (!write_length)
    return 0;

  if (!op.writesame.data_length || write_length % op.writesame.data_length)
    return -EINVAL;

  if (op.writesame.data_length != osd_op.indata.length()) {
    derr << "invalid length ws data length " << op.writesame.data_length << " actual len " << osd_op.indata.length() << dendl;
    return -EINVAL;
  }

  while (write_length) {
    write_op.indata.append(osd_op.indata);
    write_length -= op.writesame.data_length;
  }

  write_op.op.op = CEPH_OSD_OP_WRITE;
  write_op.op.extent.offset = op.writesame.offset;
  write_op.op.extent.length = op.writesame.length;
  result = do_osd_ops(ctx, write_ops);
  if (result < 0)
    derr << "do_writesame do_osd_ops failed " << result << dendl;

  return result;
}

// ========================================================================
// low level osd ops

int PrimaryLogPG::do_tmap2omap(OpContext *ctx, unsigned flags)
{
  dout(20) << " convert tmap to omap for " << ctx->new_obs.oi.soid << dendl;
  bufferlist header, vals;
  int r = _get_tmap(ctx, &header, &vals);
  if (r < 0) {
    if (r == -ENODATA && (flags & CEPH_OSD_TMAP2OMAP_NULLOK))
      r = 0;
    return r;
  }

  vector<OSDOp> ops(3);

  ops[0].op.op = CEPH_OSD_OP_TRUNCATE;
  ops[0].op.extent.offset = 0;
  ops[0].op.extent.length = 0;

  ops[1].op.op = CEPH_OSD_OP_OMAPSETHEADER;
  ops[1].indata.claim(header);

  ops[2].op.op = CEPH_OSD_OP_OMAPSETVALS;
  ops[2].indata.claim(vals);

  return do_osd_ops(ctx, ops);
}

int PrimaryLogPG::do_tmapup_slow(OpContext *ctx, bufferlist::const_iterator& bp,
				 OSDOp& osd_op, bufferlist& bl)
{
  // decode
  bufferlist header;
  map<string, bufferlist> m;
  if (bl.length()) {
    auto p = bl.cbegin();
    decode(header, p);
    decode(m, p);
    ceph_assert(p.end());
  }

  // do the update(s)
  while (!bp.end()) {
    __u8 op;
    string key;
    decode(op, bp);

    switch (op) {
    case CEPH_OSD_TMAP_SET: // insert key
      {
	decode(key, bp);
	bufferlist data;
	decode(data, bp);
	m[key] = data;
      }
      break;
    case CEPH_OSD_TMAP_RM: // remove key
      decode(key, bp);
      if (!m.count(key)) {
	return -ENOENT;
      }
      m.erase(key);
      break;
    case CEPH_OSD_TMAP_RMSLOPPY: // remove key
      decode(key, bp);
      m.erase(key);
      break;
    case CEPH_OSD_TMAP_HDR: // update header
      {
	decode(header, bp);
      }
      break;
    default:
      return -EINVAL;
    }
  }

  // reencode
  bufferlist obl;
  encode(header, obl);
  encode(m, obl);

  // write it out
  vector<OSDOp> nops(1);
  OSDOp& newop = nops[0];
  newop.op.op = CEPH_OSD_OP_WRITEFULL;
  newop.op.extent.offset = 0;
  newop.op.extent.length = obl.length();
  newop.indata = obl;
  do_osd_ops(ctx, nops);
  return 0;
}

int PrimaryLogPG::do_tmapup(OpContext *ctx, bufferlist::const_iterator& bp, OSDOp& osd_op)
{
  bufferlist::const_iterator orig_bp = bp;
  int result = 0;
  if (bp.end()) {
    dout(10) << "tmapup is a no-op" << dendl;
  } else {
    // read the whole object
    vector<OSDOp> nops(1);
    OSDOp& newop = nops[0];
    newop.op.op = CEPH_OSD_OP_READ;
    newop.op.extent.offset = 0;
    newop.op.extent.length = 0;
    result = do_osd_ops(ctx, nops);

    dout(10) << "tmapup read " << newop.outdata.length() << dendl;

    dout(30) << " starting is \n";
    newop.outdata.hexdump(*_dout);
    *_dout << dendl;

    auto ip = newop.outdata.cbegin();
    bufferlist obl;

    dout(30) << "the update command is: \n";
    osd_op.indata.hexdump(*_dout);
    *_dout << dendl;

    // header
    bufferlist header;
    __u32 nkeys = 0;
    if (newop.outdata.length()) {
      decode(header, ip);
      decode(nkeys, ip);
    }
    dout(10) << "tmapup header " << header.length() << dendl;

    if (!bp.end() && *bp == CEPH_OSD_TMAP_HDR) {
      ++bp;
      decode(header, bp);
      dout(10) << "tmapup new header " << header.length() << dendl;
    }

    encode(header, obl);

    dout(20) << "tmapup initial nkeys " << nkeys << dendl;

    // update keys
    bufferlist newkeydata;
    string nextkey, last_in_key;
    bufferlist nextval;
    bool have_next = false;
    if (!ip.end()) {
      have_next = true;
      decode(nextkey, ip);
      decode(nextval, ip);
    }
    while (!bp.end() && !result) {
      __u8 op;
      string key;
      try {
	decode(op, bp);
	decode(key, bp);
      }
      catch (ceph::buffer::error& e) {
	return -EINVAL;
      }
      if (key < last_in_key) {
	dout(5) << "tmapup warning: key '" << key << "' < previous key '" << last_in_key
		<< "', falling back to an inefficient (unsorted) update" << dendl;
	bp = orig_bp;
	return do_tmapup_slow(ctx, bp, osd_op, newop.outdata);
      }
      last_in_key = key;

      dout(10) << "tmapup op " << (int)op << " key " << key << dendl;

      // skip existing intervening keys
      bool key_exists = false;
      while (have_next && !key_exists) {
	dout(20) << "  (have_next=" << have_next << " nextkey=" << nextkey << ")" << dendl;
	if (nextkey > key)
	  break;
	if (nextkey < key) {
	  // copy untouched.
	  encode(nextkey, newkeydata);
	  encode(nextval, newkeydata);
	  dout(20) << "  keep " << nextkey << " " << nextval.length() << dendl;
	} else {
	  // don't copy; discard old value.  and stop.
	  dout(20) << "  drop " << nextkey << " " << nextval.length() << dendl;
	  key_exists = true;
	  nkeys--;
	}
	if (!ip.end()) {
	  decode(nextkey, ip);
	  decode(nextval, ip);
	} else {
	  have_next = false;
	}
      }

      if (op == CEPH_OSD_TMAP_SET) {
	bufferlist val;
	try {
	  decode(val, bp);
	}
	catch (ceph::buffer::error& e) {
	  return -EINVAL;
	}
	encode(key, newkeydata);
	encode(val, newkeydata);
	dout(20) << "   set " << key << " " << val.length() << dendl;
	nkeys++;
      } else if (op == CEPH_OSD_TMAP_CREATE) {
	if (key_exists) {
	  return -EEXIST;
	}
	bufferlist val;
	try {
	  decode(val, bp);
	}
	catch (ceph::buffer::error& e) {
	  return -EINVAL;
	}
	encode(key, newkeydata);
	encode(val, newkeydata);
	dout(20) << "   create " << key << " " << val.length() << dendl;
	nkeys++;
      } else if (op == CEPH_OSD_TMAP_RM) {
	// do nothing.
	if (!key_exists) {
	  return -ENOENT;
	}
      } else if (op == CEPH_OSD_TMAP_RMSLOPPY) {
	// do nothing
      } else {
	dout(10) << "  invalid tmap op " << (int)op << dendl;
	return -EINVAL;
      }
    }

    // copy remaining
    if (have_next) {
      encode(nextkey, newkeydata);
      encode(nextval, newkeydata);
      dout(20) << "  keep " << nextkey << " " << nextval.length() << dendl;
    }
    if (!ip.end()) {
      bufferlist rest;
      rest.substr_of(newop.outdata, ip.get_off(), newop.outdata.length() - ip.get_off());
      dout(20) << "  keep trailing " << rest.length()
	       << " at " << newkeydata.length() << dendl;
      newkeydata.claim_append(rest);
    }

    // encode final key count + key data
    dout(20) << "tmapup final nkeys " << nkeys << dendl;
    encode(nkeys, obl);
    obl.claim_append(newkeydata);

    if (0) {
      dout(30) << " final is \n";
      obl.hexdump(*_dout);
      *_dout << dendl;

      // sanity check
      auto tp = obl.cbegin();
      bufferlist h;
      decode(h, tp);
      map<string,bufferlist> d;
      decode(d, tp);
      ceph_assert(tp.end());
      dout(0) << " **** debug sanity check, looks ok ****" << dendl;
    }

    // write it out
    if (!result) {
      dout(20) << "tmapput write " << obl.length() << dendl;
      newop.op.op = CEPH_OSD_OP_WRITEFULL;
      newop.op.extent.offset = 0;
      newop.op.extent.length = obl.length();
      newop.indata = obl;
      do_osd_ops(ctx, nops);
    }
  }
  return result;
}

static int check_offset_and_length(uint64_t offset, uint64_t length,
  uint64_t max, DoutPrefixProvider *dpp)
{
  if (offset >= max ||
      length > max ||
      offset + length > max) {
    ldpp_dout(dpp, 10) << __func__ << " "
      << "osd_max_object_size: " << max
      << "; Hard limit of object size is 4GB." << dendl;
    return -EFBIG;
  }

  return 0;
}

struct FillInVerifyExtent : public Context {
  ceph_le64 *r;
  int32_t *rval;
  bufferlist *outdatap;
  std::optional<uint32_t> maybe_crc;
  uint64_t size;
  OSDService *osd;
  hobject_t soid;
  uint32_t flags;
  FillInVerifyExtent(ceph_le64 *r, int32_t *rv, bufferlist *blp,
		     std::optional<uint32_t> mc, uint64_t size,
		     OSDService *osd, hobject_t soid, uint32_t flags) :
    r(r), rval(rv), outdatap(blp), maybe_crc(mc),
    size(size), osd(osd), soid(soid), flags(flags) {}
  void finish(int len) override {
    *r = len;
    if (len < 0) {
      *rval = len;
      return;
    }
    *rval = 0;

    // whole object?  can we verify the checksum?
    if (maybe_crc && *r == size) {
      uint32_t crc = outdatap->crc32c(-1);
      if (maybe_crc != crc) {
        osd->clog->error() << std::hex << " full-object read crc 0x" << crc
			   << " != expected 0x" << *maybe_crc
			   << std::dec << " on " << soid;
        if (!(flags & CEPH_OSD_OP_FLAG_FAILOK)) {
	  *rval = -EIO;
	  *r = 0;
	}
      }
    }
  }
};

struct ToSparseReadResult : public Context {
  int* result;
  bufferlist* data_bl;
  uint64_t data_offset;
  ceph_le64* len;
  ToSparseReadResult(int* result, bufferlist* bl, uint64_t offset,
		     ceph_le64* len)
    : result(result), data_bl(bl), data_offset(offset),len(len) {}
  void finish(int r) override {
    if (r < 0) {
      *result = r;
      return;
    }
    *result = 0;
    *len = r;
    bufferlist outdata;
    map<uint64_t, uint64_t> extents = {{data_offset, r}};
    encode(extents, outdata);
    encode_destructively(*data_bl, outdata);
    data_bl->swap(outdata);
  }
};

template<typename V>
static string list_keys(const map<string, V>& m) {
  string s;
  for (typename map<string, V>::const_iterator itr = m.begin(); itr != m.end(); ++itr) {
    if (!s.empty()) {
      s.push_back(',');
    }
    s.append(itr->first);
  }
  return s;
}

template<typename T>
static string list_entries(const T& m) {
  string s;
  for (typename T::const_iterator itr = m.begin(); itr != m.end(); ++itr) {
    if (!s.empty()) {
      s.push_back(',');
    }
    s.append(*itr);
  }
  return s;
}

void PrimaryLogPG::maybe_create_new_object(
  OpContext *ctx,
  bool ignore_transaction)
{
  ObjectState& obs = ctx->new_obs;
  if (!obs.exists) {
    ctx->delta_stats.num_objects++;
    obs.exists = true;
    ceph_assert(!obs.oi.is_whiteout());
    obs.oi.new_object();
    if (!ignore_transaction)
      ctx->op_t->create(obs.oi.soid);
  } else if (obs.oi.is_whiteout()) {
    dout(10) << __func__ << " clearing whiteout on " << obs.oi.soid << dendl;
    ctx->new_obs.oi.clear_flag(object_info_t::FLAG_WHITEOUT);
    --ctx->delta_stats.num_whiteouts;
  }
}

struct ReadFinisher : public PrimaryLogPG::OpFinisher {
  OSDOp& osd_op;

  explicit ReadFinisher(OSDOp& osd_op) : osd_op(osd_op) {
  }

  int execute() override {
    return osd_op.rval;
  }
};

struct C_ChecksumRead : public Context {
  PrimaryLogPG *primary_log_pg;
  OSDOp &osd_op;
  Checksummer::CSumType csum_type;
  bufferlist init_value_bl;
  ceph_le64 read_length;
  bufferlist read_bl;
  Context *fill_extent_ctx;

  C_ChecksumRead(PrimaryLogPG *primary_log_pg, OSDOp &osd_op,
		 Checksummer::CSumType csum_type, bufferlist &&init_value_bl,
		 std::optional<uint32_t> maybe_crc, uint64_t size,
		 OSDService *osd, hobject_t soid, uint32_t flags)
    : primary_log_pg(primary_log_pg), osd_op(osd_op),
      csum_type(csum_type), init_value_bl(std::move(init_value_bl)),
      fill_extent_ctx(new FillInVerifyExtent(&read_length, &osd_op.rval,
					     &read_bl, maybe_crc, size,
					     osd, soid, flags)) {
  }
  ~C_ChecksumRead() override {
    delete fill_extent_ctx;
  }

  void finish(int r) override {
    fill_extent_ctx->complete(r);
    fill_extent_ctx = nullptr;

    if (osd_op.rval >= 0) {
      bufferlist::const_iterator init_value_bl_it = init_value_bl.begin();
      osd_op.rval = primary_log_pg->finish_checksum(osd_op, csum_type,
						    &init_value_bl_it, read_bl);
    }
  }
};

int PrimaryLogPG::do_checksum(OpContext *ctx, OSDOp& osd_op,
			      bufferlist::const_iterator *bl_it)
{
  dout(20) << __func__ << dendl;

  auto& op = osd_op.op;
  if (op.checksum.chunk_size > 0) {
    if (op.checksum.length == 0) {
      dout(10) << __func__ << ": length required when chunk size provided"
	       << dendl;
      return -EINVAL;
    }
    if (op.checksum.length % op.checksum.chunk_size != 0) {
      dout(10) << __func__ << ": length not aligned to chunk size" << dendl;
      return -EINVAL;
    }
  }

  auto& oi = ctx->new_obs.oi;
  if (op.checksum.offset == 0 && op.checksum.length == 0) {
    // zeroed offset+length implies checksum whole object
    op.checksum.length = oi.size;
  } else if (op.checksum.offset >= oi.size) {
    // read size was trimmed to zero, do nothing
    // see PrimaryLogPG::do_read
    return 0;
  } else if (op.extent.offset + op.extent.length > oi.size) {
    op.extent.length = oi.size - op.extent.offset;
    if (op.checksum.chunk_size > 0 &&
        op.checksum.length % op.checksum.chunk_size != 0) {
      dout(10) << __func__ << ": length (trimmed to 0x"
               << std::hex << op.checksum.length
               << ") not aligned to chunk size 0x"
               << op.checksum.chunk_size << std::dec
               << dendl;
      return -EINVAL;
    }
  }

  Checksummer::CSumType csum_type;
  switch (op.checksum.type) {
  case CEPH_OSD_CHECKSUM_OP_TYPE_XXHASH32:
    csum_type = Checksummer::CSUM_XXHASH32;
    break;
  case CEPH_OSD_CHECKSUM_OP_TYPE_XXHASH64:
    csum_type = Checksummer::CSUM_XXHASH64;
    break;
  case CEPH_OSD_CHECKSUM_OP_TYPE_CRC32C:
    csum_type = Checksummer::CSUM_CRC32C;
    break;
  default:
    dout(10) << __func__ << ": unknown crc type ("
	     << static_cast<uint32_t>(op.checksum.type) << ")" << dendl;
    return -EINVAL;
  }

  size_t csum_init_value_size = Checksummer::get_csum_init_value_size(csum_type);
  if (bl_it->get_remaining() < csum_init_value_size) {
    dout(10) << __func__ << ": init value not provided" << dendl;
    return -EINVAL;
  }

  bufferlist init_value_bl;
  init_value_bl.substr_of(bl_it->get_bl(), bl_it->get_off(),
			  csum_init_value_size);
  *bl_it += csum_init_value_size;

  if (pool.info.is_erasure() && op.checksum.length > 0) {
    // If there is a data digest and it is possible we are reading
    // entire object, pass the digest.
    std::optional<uint32_t> maybe_crc;
    if (oi.is_data_digest() && op.checksum.offset == 0 &&
        op.checksum.length >= oi.size) {
      maybe_crc = oi.data_digest;
    }

    // async read
    auto& soid = oi.soid;
    auto checksum_ctx = new C_ChecksumRead(this, osd_op, csum_type,
					   std::move(init_value_bl), maybe_crc,
					   oi.size, osd, soid, op.flags);

    ctx->pending_async_reads.push_back({
      {op.checksum.offset, op.checksum.length, op.flags},
      {&checksum_ctx->read_bl, checksum_ctx}});

    dout(10) << __func__ << ": async_read noted for " << soid << dendl;
    ctx->op_finishers[ctx->current_osd_subop_num].reset(
      new ReadFinisher(osd_op));
    return -EINPROGRESS;
  }

  // sync read
  std::vector<OSDOp> read_ops(1);
  auto& read_op = read_ops[0];
  if (op.checksum.length > 0) {
    read_op.op.op = CEPH_OSD_OP_READ;
    read_op.op.flags = op.flags;
    read_op.op.extent.offset = op.checksum.offset;
    read_op.op.extent.length = op.checksum.length;
    read_op.op.extent.truncate_size = 0;
    read_op.op.extent.truncate_seq = 0;

    int r = do_osd_ops(ctx, read_ops);
    if (r < 0) {
      derr << __func__ << ": do_osd_ops failed: " << cpp_strerror(r) << dendl;
      return r;
    }
  }

  bufferlist::const_iterator init_value_bl_it = init_value_bl.begin();
  return finish_checksum(osd_op, csum_type, &init_value_bl_it,
			 read_op.outdata);
}

int PrimaryLogPG::finish_checksum(OSDOp& osd_op,
				  Checksummer::CSumType csum_type,
				  bufferlist::const_iterator *init_value_bl_it,
				  const bufferlist &read_bl) {
  dout(20) << __func__ << dendl;

  auto& op = osd_op.op;

  if (op.checksum.length > 0 && read_bl.length() != op.checksum.length) {
    derr << __func__ << ": bytes read " << read_bl.length() << " != "
	 << op.checksum.length << dendl;
    return -EINVAL;
  }

  size_t csum_chunk_size = (op.checksum.chunk_size != 0 ?
			      op.checksum.chunk_size : read_bl.length());
  uint32_t csum_count = (csum_chunk_size > 0 ?
			   read_bl.length() / csum_chunk_size : 0);

  bufferlist csum;
  bufferptr csum_data;
  if (csum_count > 0) {
    size_t csum_value_size = Checksummer::get_csum_value_size(csum_type);
    csum_data = ceph::buffer::create(csum_value_size * csum_count);
    csum_data.zero();
    csum.append(csum_data);

    switch (csum_type) {
    case Checksummer::CSUM_XXHASH32:
      {
        Checksummer::xxhash32::init_value_t init_value;
        decode(init_value, *init_value_bl_it);
        Checksummer::calculate<Checksummer::xxhash32>(
	  init_value, csum_chunk_size, 0, read_bl.length(), read_bl,
	  &csum_data);
      }
      break;
    case Checksummer::CSUM_XXHASH64:
      {
        Checksummer::xxhash64::init_value_t init_value;
        decode(init_value, *init_value_bl_it);
        Checksummer::calculate<Checksummer::xxhash64>(
	  init_value, csum_chunk_size, 0, read_bl.length(), read_bl,
	  &csum_data);
      }
      break;
    case Checksummer::CSUM_CRC32C:
      {
        Checksummer::crc32c::init_value_t init_value;
        decode(init_value, *init_value_bl_it);
        Checksummer::calculate<Checksummer::crc32c>(
  	  init_value, csum_chunk_size, 0, read_bl.length(), read_bl,
	  &csum_data);
      }
      break;
    default:
      break;
    }
  }

  encode(csum_count, osd_op.outdata);
  osd_op.outdata.claim_append(csum);
  return 0;
}

struct C_ExtentCmpRead : public Context {
  PrimaryLogPG *primary_log_pg;
  OSDOp &osd_op;
  ceph_le64 read_length{};
  bufferlist read_bl;
  Context *fill_extent_ctx;

  C_ExtentCmpRead(PrimaryLogPG *primary_log_pg, OSDOp &osd_op,
		  std::optional<uint32_t> maybe_crc, uint64_t size,
		  OSDService *osd, hobject_t soid, uint32_t flags)
    : primary_log_pg(primary_log_pg), osd_op(osd_op),
      fill_extent_ctx(new FillInVerifyExtent(&read_length, &osd_op.rval,
					     &read_bl, maybe_crc, size,
					     osd, soid, flags)) {
  }
  ~C_ExtentCmpRead() override {
    delete fill_extent_ctx;
  }

  void finish(int r) override {
    if (r == -ENOENT) {
      osd_op.rval = 0;
      read_bl.clear();
      delete fill_extent_ctx;
    } else {
      fill_extent_ctx->complete(r);
    }
    fill_extent_ctx = nullptr;

    if (osd_op.rval >= 0) {
      osd_op.rval = primary_log_pg->finish_extent_cmp(osd_op, read_bl);
    }
  }
};

int PrimaryLogPG::do_extent_cmp(OpContext *ctx, OSDOp& osd_op)
{
  dout(20) << __func__ << dendl;
  ceph_osd_op& op = osd_op.op;

  auto& oi = ctx->new_obs.oi;
  uint64_t size = oi.size;
  if ((oi.truncate_seq < op.extent.truncate_seq) &&
      (op.extent.offset + op.extent.length > op.extent.truncate_size)) {
    size = op.extent.truncate_size;
  }

  if (op.extent.offset >= size) {
    op.extent.length = 0;
  } else if (op.extent.offset + op.extent.length > size) {
    op.extent.length = size - op.extent.offset;
  }

  if (op.extent.length == 0) {
    dout(20) << __func__ << " zero length extent" << dendl;
    return finish_extent_cmp(osd_op, bufferlist{});
  } else if (!ctx->obs->exists || ctx->obs->oi.is_whiteout()) {
    dout(20) << __func__ << " object DNE" << dendl;
    return finish_extent_cmp(osd_op, {});
  } else if (pool.info.is_erasure()) {
    // If there is a data digest and it is possible we are reading
    // entire object, pass the digest.
    std::optional<uint32_t> maybe_crc;
    if (oi.is_data_digest() && op.checksum.offset == 0 &&
        op.checksum.length >= oi.size) {
      maybe_crc = oi.data_digest;
    }

    // async read
    auto& soid = oi.soid;
    auto extent_cmp_ctx = new C_ExtentCmpRead(this, osd_op, maybe_crc, oi.size,
					      osd, soid, op.flags);
    ctx->pending_async_reads.push_back({
      {op.extent.offset, op.extent.length, op.flags},
      {&extent_cmp_ctx->read_bl, extent_cmp_ctx}});

    dout(10) << __func__ << ": async_read noted for " << soid << dendl;

    ctx->op_finishers[ctx->current_osd_subop_num].reset(
      new ReadFinisher(osd_op));
    return -EINPROGRESS;
  }

  // sync read
  vector<OSDOp> read_ops(1);
  OSDOp& read_op = read_ops[0];

  read_op.op.op = CEPH_OSD_OP_SYNC_READ;
  read_op.op.extent.offset = op.extent.offset;
  read_op.op.extent.length = op.extent.length;
  read_op.op.extent.truncate_seq = op.extent.truncate_seq;
  read_op.op.extent.truncate_size = op.extent.truncate_size;

  int result = do_osd_ops(ctx, read_ops);
  if (result < 0) {
    derr << __func__ << " failed " << result << dendl;
    return result;
  }
  return finish_extent_cmp(osd_op, read_op.outdata);
}

int PrimaryLogPG::finish_extent_cmp(OSDOp& osd_op, const bufferlist &read_bl)
{
  for (uint64_t idx = 0; idx < osd_op.indata.length(); ++idx) {
    char read_byte = (idx < read_bl.length() ? read_bl[idx] : 0);
    if (osd_op.indata[idx] != read_byte) {
        return (-MAX_ERRNO - idx);
    }
  }

  return 0;
}

int PrimaryLogPG::do_read(OpContext *ctx, OSDOp& osd_op) {
  dout(20) << __func__ << dendl;
  auto& op = osd_op.op;
  auto& oi = ctx->new_obs.oi;
  auto& soid = oi.soid;
  __u32 seq = oi.truncate_seq;
  uint64_t size = oi.size;
  bool trimmed_read = false;

  dout(30) << __func__ << " oi.size: " << oi.size << dendl;
  dout(30) << __func__ << " oi.truncate_seq: " << oi.truncate_seq << dendl;
  dout(30) << __func__ << " op.extent.truncate_seq: " << op.extent.truncate_seq << dendl;
  dout(30) << __func__ << " op.extent.truncate_size: " << op.extent.truncate_size << dendl;

  // are we beyond truncate_size?
  if ( (seq < op.extent.truncate_seq) &&
       (op.extent.offset + op.extent.length > op.extent.truncate_size) &&
       (size > op.extent.truncate_size) )
    size = op.extent.truncate_size;

  if (op.extent.length == 0) //length is zero mean read the whole object
    op.extent.length = size;

  if (op.extent.offset >= size) {
    op.extent.length = 0;
    trimmed_read = true;
  } else if (op.extent.offset + op.extent.length > size) {
    op.extent.length = size - op.extent.offset;
    trimmed_read = true;
  }

  dout(30) << __func__ << "op.extent.length is now " << op.extent.length << dendl;

  // read into a buffer
  int result = 0;
  if (trimmed_read && op.extent.length == 0) {
    // read size was trimmed to zero and it is expected to do nothing
    // a read operation of 0 bytes does *not* do nothing, this is why
    // the trimmed_read boolean is needed
  } else if (pool.info.is_erasure()) {
    // The initialisation below is required to silence a false positive
    // -Wmaybe-uninitialized warning
    std::optional<uint32_t> maybe_crc;
    // If there is a data digest and it is possible we are reading
    // entire object, pass the digest.  FillInVerifyExtent will
    // will check the oi.size again.
    if (oi.is_data_digest() && op.extent.offset == 0 &&
        op.extent.length >= oi.size)
      maybe_crc = oi.data_digest;
    ctx->pending_async_reads.push_back(
      make_pair(
        boost::make_tuple(op.extent.offset, op.extent.length, op.flags),
        make_pair(&osd_op.outdata,
		  new FillInVerifyExtent(&op.extent.length, &osd_op.rval,
					 &osd_op.outdata, maybe_crc, oi.size,
					 osd, soid, op.flags))));
    dout(10) << " async_read noted for " << soid << dendl;

    ctx->op_finishers[ctx->current_osd_subop_num].reset(
      new ReadFinisher(osd_op));
  } else {
    int r = pgbackend->objects_read_sync(
      soid, op.extent.offset, op.extent.length, op.flags, &osd_op.outdata);
    // whole object?  can we verify the checksum?
    if (r >= 0 && op.extent.offset == 0 &&
        (uint64_t)r == oi.size && oi.is_data_digest()) {
      uint32_t crc = osd_op.outdata.crc32c(-1);
      if (oi.data_digest != crc) {
        osd->clog->error() << info.pgid << std::hex
                           << " full-object read crc 0x" << crc
                           << " != expected 0x" << oi.data_digest
                           << std::dec << " on " << soid;
        r = -EIO; // try repair later
      }
    }
    if (r == -EIO) {
      r = rep_repair_primary_object(soid, ctx);
    }
    if (r >= 0)
      op.extent.length = r;
    else if (r == -EAGAIN) {
      result = -EAGAIN;
    } else {
      result = r;
      op.extent.length = 0;
    }
    dout(10) << " read got " << r << " / " << op.extent.length
	     << " bytes from obj " << soid << dendl;
  }
  if (result >= 0) {
    ctx->delta_stats.num_rd_kb += shift_round_up(op.extent.length, 10);
    ctx->delta_stats.num_rd++;
  }
  return result;
}

int PrimaryLogPG::do_sparse_read(OpContext *ctx, OSDOp& osd_op) {
  dout(20) << __func__ << dendl;
  auto& op = osd_op.op;
  auto& oi = ctx->new_obs.oi;
  auto& soid = oi.soid;

  if (op.extent.truncate_seq) {
    dout(0) << "sparse_read does not support truncation sequence " << dendl;
    return -EINVAL;
  }

  ++ctx->num_read;
  if (pool.info.is_erasure()) {
    // translate sparse read to a normal one if not supported
    uint64_t offset = op.extent.offset;
    uint64_t length = op.extent.length;
    if (offset > oi.size) {
      length = 0;
    } else if (offset + length > oi.size) {
      length = oi.size - offset;
    }

    if (length > 0) {
      ctx->pending_async_reads.push_back(
        make_pair(
          boost::make_tuple(offset, length, op.flags),
          make_pair(
	    &osd_op.outdata,
	    new ToSparseReadResult(&osd_op.rval, &osd_op.outdata, offset,
				   &op.extent.length))));
      dout(10) << " async_read (was sparse_read) noted for " << soid << dendl;

      ctx->op_finishers[ctx->current_osd_subop_num].reset(
        new ReadFinisher(osd_op));
    } else {
      dout(10) << " sparse read ended up empty for " << soid << dendl;
      map<uint64_t, uint64_t> extents;
      encode(extents, osd_op.outdata);
    }
  } else {
    // read into a buffer
    map<uint64_t, uint64_t> m;
    int r = osd->store->fiemap(ch, ghobject_t(soid, ghobject_t::NO_GEN,
					      info.pgid.shard),
			       op.extent.offset, op.extent.length, m);
    if (r < 0)  {
      return r;
    }

    bufferlist data_bl;
    r = pgbackend->objects_readv_sync(soid, std::move(m), op.flags, &data_bl);
    if (r == -EIO) {
      r = rep_repair_primary_object(soid, ctx);
    }
    if (r < 0) {
      return r;
    }

    // Why SPARSE_READ need checksum? In fact, librbd always use sparse-read.
    // Maybe at first, there is no much whole objects. With continued use, more
    // and more whole object exist. So from this point, for spare-read add
    // checksum make sense.
    if ((uint64_t)r == oi.size && oi.is_data_digest()) {
      uint32_t crc = data_bl.crc32c(-1);
      if (oi.data_digest != crc) {
        osd->clog->error() << info.pgid << std::hex
          << " full-object read crc 0x" << crc
          << " != expected 0x" << oi.data_digest
          << std::dec << " on " << soid;
        r = rep_repair_primary_object(soid, ctx);
	if (r < 0) {
	  return r;
	}
      }
    }

    op.extent.length = r;

    encode(m, osd_op.outdata); // re-encode since it might be modified
    ::encode_destructively(data_bl, osd_op.outdata);

    dout(10) << " sparse_read got " << r << " bytes from object "
	     << soid << dendl;
  }

  ctx->delta_stats.num_rd_kb += shift_round_up(op.extent.length, 10);
  ctx->delta_stats.num_rd++;
  return 0;
}

int PrimaryLogPG::do_osd_ops(OpContext *ctx, vector<OSDOp>& ops)
{
  int result = 0;
  SnapSetContext *ssc = ctx->obc->ssc;
  ObjectState& obs = ctx->new_obs;
  object_info_t& oi = obs.oi;
  const hobject_t& soid = oi.soid;
  const bool skip_data_digest = osd->store->has_builtin_csum() &&
    osd->osd_skip_data_digest;

  PGTransaction* t = ctx->op_t.get();

  dout(10) << "do_osd_op " << soid << " " << ops << dendl;

  ctx->current_osd_subop_num = 0;
  for (auto p = ops.begin(); p != ops.end(); ++p, ctx->current_osd_subop_num++, ctx->processed_subop_count++) {
    OSDOp& osd_op = *p;
    ceph_osd_op& op = osd_op.op;

    OpFinisher* op_finisher = nullptr;
    {
      auto op_finisher_it = ctx->op_finishers.find(ctx->current_osd_subop_num);
      if (op_finisher_it != ctx->op_finishers.end()) {
        op_finisher = op_finisher_it->second.get();
      }
    }

    // TODO: check endianness (ceph_le32 vs uint32_t, etc.)
    // The fields in ceph_osd_op are little-endian (according to the definition in rados.h),
    // but the code in this function seems to treat them as native-endian.  What should the
    // tracepoints do?
    tracepoint(osd, do_osd_op_pre, soid.oid.name.c_str(), soid.snap.val, op.op, ceph_osd_op_name(op.op), op.flags);

    dout(10) << "do_osd_op  " << osd_op << dendl;

    auto bp = osd_op.indata.cbegin();

    // user-visible modifcation?
    switch (op.op) {
      // non user-visible modifications
    case CEPH_OSD_OP_WATCH:
    case CEPH_OSD_OP_CACHE_EVICT:
    case CEPH_OSD_OP_CACHE_FLUSH:
    case CEPH_OSD_OP_CACHE_TRY_FLUSH:
    case CEPH_OSD_OP_UNDIRTY:
    case CEPH_OSD_OP_COPY_FROM:  // we handle user_version update explicitly
    case CEPH_OSD_OP_COPY_FROM2:
    case CEPH_OSD_OP_CACHE_PIN:
    case CEPH_OSD_OP_CACHE_UNPIN:
    case CEPH_OSD_OP_SET_REDIRECT:
    case CEPH_OSD_OP_TIER_PROMOTE:
    case CEPH_OSD_OP_TIER_FLUSH:
      break;
    default:
      if (op.op & CEPH_OSD_OP_MODE_WR)
	ctx->user_modify = true;
    }

    // munge -1 truncate to 0 truncate
    if (ceph_osd_op_uses_extent(op.op) &&
        op.extent.truncate_seq == 1 &&
        op.extent.truncate_size == (-1ULL)) {
      op.extent.truncate_size = 0;
      op.extent.truncate_seq = 0;
    }

    // munge ZERO -> TRUNCATE?  (don't munge to DELETE or we risk hosing attributes)
    if (op.op == CEPH_OSD_OP_ZERO &&
        obs.exists &&
        op.extent.offset < static_cast<Option::size_t>(osd->osd_max_object_size) &&
        op.extent.length >= 1 &&
        op.extent.length <= static_cast<Option::size_t>(osd->osd_max_object_size) &&
	op.extent.offset + op.extent.length >= oi.size) {
      if (op.extent.offset >= oi.size) {
        // no-op
	goto fail;
      }
      dout(10) << " munging ZERO " << op.extent.offset << "~" << op.extent.length
	       << " -> TRUNCATE " << op.extent.offset << " (old size is " << oi.size << ")" << dendl;
      op.op = CEPH_OSD_OP_TRUNCATE;
    }

    switch (op.op) {
      
      // --- READS ---

    case CEPH_OSD_OP_CMPEXT:
      ++ctx->num_read;
      tracepoint(osd, do_osd_op_pre_extent_cmp, soid.oid.name.c_str(),
		 soid.snap.val, oi.size, oi.truncate_seq, op.extent.offset,
		 op.extent.length, op.extent.truncate_size,
		 op.extent.truncate_seq);

      if (op_finisher == nullptr) {
	result = do_extent_cmp(ctx, osd_op);
      } else {
	result = op_finisher->execute();
      }
      break;

    case CEPH_OSD_OP_SYNC_READ:
      if (pool.info.is_erasure()) {
	result = -EOPNOTSUPP;
	break;
      }
      // fall through
    case CEPH_OSD_OP_READ:
      ++ctx->num_read;
      tracepoint(osd, do_osd_op_pre_read, soid.oid.name.c_str(),
		 soid.snap.val, oi.size, oi.truncate_seq, op.extent.offset,
		 op.extent.length, op.extent.truncate_size,
		 op.extent.truncate_seq);
      if (op_finisher == nullptr) {
	if (!ctx->data_off) {
	  ctx->data_off = op.extent.offset;
	}
	result = do_read(ctx, osd_op);
      } else {
	result = op_finisher->execute();
      }
      break;

    case CEPH_OSD_OP_CHECKSUM:
      ++ctx->num_read;
      {
	tracepoint(osd, do_osd_op_pre_checksum, soid.oid.name.c_str(),
		   soid.snap.val, oi.size, oi.truncate_seq, op.checksum.type,
		   op.checksum.offset, op.checksum.length,
		   op.checksum.chunk_size);

	if (op_finisher == nullptr) {
	  result = do_checksum(ctx, osd_op, &bp);
	} else {
	  result = op_finisher->execute();
	}
      }
      break;

    /* map extents */
    case CEPH_OSD_OP_MAPEXT:
      tracepoint(osd, do_osd_op_pre_mapext, soid.oid.name.c_str(), soid.snap.val, op.extent.offset, op.extent.length);
      if (pool.info.is_erasure()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_read;
      {
	// read into a buffer
	bufferlist bl;
	int r = osd->store->fiemap(ch, ghobject_t(soid, ghobject_t::NO_GEN,
						  info.pgid.shard),
				   op.extent.offset, op.extent.length, bl);
	osd_op.outdata.claim(bl);
	if (r < 0)
	  result = r;
	else
	  ctx->delta_stats.num_rd_kb += shift_round_up(bl.length(), 10);
	ctx->delta_stats.num_rd++;
	dout(10) << " map_extents done on object " << soid << dendl;
      }
      break;

    /* map extents */
    case CEPH_OSD_OP_SPARSE_READ:
      tracepoint(osd, do_osd_op_pre_sparse_read, soid.oid.name.c_str(),
		 soid.snap.val, oi.size, oi.truncate_seq, op.extent.offset,
		 op.extent.length, op.extent.truncate_size,
		 op.extent.truncate_seq);
      if (op_finisher == nullptr) {
	result = do_sparse_read(ctx, osd_op);
      } else {
	result = op_finisher->execute();
      }
      break;

    case CEPH_OSD_OP_CALL:
      {
	string cname, mname;
	bufferlist indata;
	try {
	  bp.copy(op.cls.class_len, cname);
	  bp.copy(op.cls.method_len, mname);
	  bp.copy(op.cls.indata_len, indata);
	} catch (ceph::buffer::error& e) {
	  dout(10) << "call unable to decode class + method + indata" << dendl;
	  dout(30) << "in dump: ";
	  osd_op.indata.hexdump(*_dout);
	  *_dout << dendl;
	  result = -EINVAL;
	  tracepoint(osd, do_osd_op_pre_call, soid.oid.name.c_str(), soid.snap.val, "???", "???");
	  break;
	}
	tracepoint(osd, do_osd_op_pre_call, soid.oid.name.c_str(), soid.snap.val, cname.c_str(), mname.c_str());

	ClassHandler::ClassData *cls;
	result = ClassHandler::get_instance().open_class(cname, &cls);
	ceph_assert(result == 0);   // init_op_flags() already verified this works.

	ClassHandler::ClassMethod *method = cls->get_method(mname);
	if (!method) {
	  dout(10) << "call method " << cname << "." << mname << " does not exist" << dendl;
	  result = -EOPNOTSUPP;
	  break;
	}

	int flags = method->get_flags();
	if (flags & CLS_METHOD_WR)
	  ctx->user_modify = true;

	bufferlist outdata;
	dout(10) << "call method " << cname << "." << mname << dendl;
	int prev_rd = ctx->num_read;
	int prev_wr = ctx->num_write;
	result = method->exec((cls_method_context_t)&ctx, indata, outdata);

	if (ctx->num_read > prev_rd && !(flags & CLS_METHOD_RD)) {
	  derr << "method " << cname << "." << mname << " tried to read object but is not marked RD" << dendl;
	  result = -EIO;
	  break;
	}
	if (ctx->num_write > prev_wr && !(flags & CLS_METHOD_WR)) {
	  derr << "method " << cname << "." << mname << " tried to update object but is not marked WR" << dendl;
	  result = -EIO;
	  break;
	}

	dout(10) << "method called response length=" << outdata.length() << dendl;
	op.extent.length = outdata.length();
	osd_op.outdata.claim_append(outdata);
	dout(30) << "out dump: ";
	osd_op.outdata.hexdump(*_dout);
	*_dout << dendl;
      }
      break;

    case CEPH_OSD_OP_STAT:
      // note: stat does not require RD
      {
	tracepoint(osd, do_osd_op_pre_stat, soid.oid.name.c_str(), soid.snap.val);

	if (obs.exists && !oi.is_whiteout()) {
	  encode(oi.size, osd_op.outdata);
	  encode(oi.mtime, osd_op.outdata);
	  dout(10) << "stat oi has " << oi.size << " " << oi.mtime << dendl;
	} else {
	  result = -ENOENT;
	  dout(10) << "stat oi object does not exist" << dendl;
	}

	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_ISDIRTY:
      ++ctx->num_read;
      {
	tracepoint(osd, do_osd_op_pre_isdirty, soid.oid.name.c_str(), soid.snap.val);
	bool is_dirty = obs.oi.is_dirty();
	encode(is_dirty, osd_op.outdata);
	ctx->delta_stats.num_rd++;
	result = 0;
      }
      break;

    case CEPH_OSD_OP_UNDIRTY:
      ++ctx->num_write;
      result = 0;
      {
	tracepoint(osd, do_osd_op_pre_undirty, soid.oid.name.c_str(), soid.snap.val);
	if (oi.is_dirty()) {
	  ctx->undirty = true;  // see make_writeable()
	  ctx->modify = true;
	  ctx->delta_stats.num_wr++;
	}
      }
      break;

    case CEPH_OSD_OP_CACHE_TRY_FLUSH:
      ++ctx->num_write;
      result = 0;
      {
	tracepoint(osd, do_osd_op_pre_try_flush, soid.oid.name.c_str(), soid.snap.val);
	if (ctx->lock_type != RWState::RWNONE) {
	  dout(10) << "cache-try-flush without SKIPRWLOCKS flag set" << dendl;
	  result = -EINVAL;
	  break;
	}
	if (pool.info.cache_mode == pg_pool_t::CACHEMODE_NONE) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = 0;
	  break;
	}
	if (oi.is_cache_pinned()) {
	  dout(10) << "cache-try-flush on a pinned object, consider unpin this object first" << dendl;
	  result = -EPERM;
	  break;
	}
	if (oi.is_dirty()) {
	  result = start_flush(ctx->op, ctx->obc, false, NULL, std::nullopt);
	  if (result == -EINPROGRESS)
	    result = -EAGAIN;
	} else {
	  result = 0;
	}
      }
      break;

    case CEPH_OSD_OP_CACHE_FLUSH:
      ++ctx->num_write;
      result = 0;
      {
	tracepoint(osd, do_osd_op_pre_cache_flush, soid.oid.name.c_str(), soid.snap.val);
	if (ctx->lock_type == RWState::RWNONE) {
	  dout(10) << "cache-flush with SKIPRWLOCKS flag set" << dendl;
	  result = -EINVAL;
	  break;
	}
	if (pool.info.cache_mode == pg_pool_t::CACHEMODE_NONE) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = 0;
	  break;
	}
	if (oi.is_cache_pinned()) {
	  dout(10) << "cache-flush on a pinned object, consider unpin this object first" << dendl;
	  result = -EPERM;
	  break;
	}
	hobject_t missing;
	if (oi.is_dirty()) {
	  result = start_flush(ctx->op, ctx->obc, true, &missing, std::nullopt);
	  if (result == -EINPROGRESS)
	    result = -EAGAIN;
	} else {
	  result = 0;
	}
	// Check special return value which has set missing_return
        if (result == -ENOENT) {
          dout(10) << __func__ << " CEPH_OSD_OP_CACHE_FLUSH got ENOENT" << dendl;
	  ceph_assert(!missing.is_min());
	  wait_for_unreadable_object(missing, ctx->op);
	  // Error code which is used elsewhere when wait_for_unreadable_object() is used
	  result = -EAGAIN;
	}
      }
      break;

    case CEPH_OSD_OP_CACHE_EVICT:
      ++ctx->num_write;
      result = 0;
      {
	tracepoint(osd, do_osd_op_pre_cache_evict, soid.oid.name.c_str(), soid.snap.val);
	if (pool.info.cache_mode == pg_pool_t::CACHEMODE_NONE) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = 0;
	  break;
	}
	if (oi.is_cache_pinned()) {
	  dout(10) << "cache-evict on a pinned object, consider unpin this object first" << dendl;
	  result = -EPERM;
	  break;
	}
	if (oi.is_dirty()) {
	  result = -EBUSY;
	  break;
	}
	if (!oi.watchers.empty()) {
	  result = -EBUSY;
	  break;
	}
	if (soid.snap == CEPH_NOSNAP) {
	  result = _verify_no_head_clones(soid, ssc->snapset);
	  if (result < 0)
	    break;
	}
	result = _delete_oid(ctx, true, false);
	if (result >= 0) {
	  // mark that this is a cache eviction to avoid triggering normal
	  // make_writeable() clone creation in finish_ctx()
	  ctx->cache_evict = true;
	}
	osd->logger->inc(l_osd_tier_evict);
      }
      break;

    case CEPH_OSD_OP_GETXATTR:
      ++ctx->num_read;
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	tracepoint(osd, do_osd_op_pre_getxattr, soid.oid.name.c_str(), soid.snap.val, aname.c_str());
	string name = "_" + aname;
	int r = getattr_maybe_cache(
	  ctx->obc,
	  name,
	  &(osd_op.outdata));
	if (r >= 0) {
	  op.xattr.value_len = osd_op.outdata.length();
	  result = 0;
	  ctx->delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
	} else
	  result = r;

	ctx->delta_stats.num_rd++;
      }
      break;

   case CEPH_OSD_OP_GETXATTRS:
      ++ctx->num_read;
      {
	tracepoint(osd, do_osd_op_pre_getxattrs, soid.oid.name.c_str(), soid.snap.val);
	map<string, bufferlist> out;
	result = getattrs_maybe_cache(
	  ctx->obc,
	  &out);
        
        bufferlist bl;
        encode(out, bl);
	ctx->delta_stats.num_rd_kb += shift_round_up(bl.length(), 10);
        ctx->delta_stats.num_rd++;
        osd_op.outdata.claim_append(bl);
      }
      break;
      
    case CEPH_OSD_OP_CMPXATTR:
      ++ctx->num_read;
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	tracepoint(osd, do_osd_op_pre_cmpxattr, soid.oid.name.c_str(), soid.snap.val, aname.c_str());
	string name = "_" + aname;
	name[op.xattr.name_len + 1] = 0;
	
	bufferlist xattr;
	result = getattr_maybe_cache(
	  ctx->obc,
	  name,
	  &xattr);
	if (result < 0 && result != -EEXIST && result != -ENODATA)
	  break;
	
	ctx->delta_stats.num_rd++;
	ctx->delta_stats.num_rd_kb += shift_round_up(xattr.length(), 10);

	switch (op.xattr.cmp_mode) {
	case CEPH_OSD_CMPXATTR_MODE_STRING:
	  {
	    string val;
	    bp.copy(op.xattr.value_len, val);
	    val[op.xattr.value_len] = 0;
	    dout(10) << "CEPH_OSD_OP_CMPXATTR name=" << name << " val=" << val
		     << " op=" << (int)op.xattr.cmp_op << " mode=" << (int)op.xattr.cmp_mode << dendl;
	    result = do_xattr_cmp_str(op.xattr.cmp_op, val, xattr);
	  }
	  break;

        case CEPH_OSD_CMPXATTR_MODE_U64:
	  {
	    uint64_t u64val;
	    try {
	      decode(u64val, bp);
	    }
	    catch (ceph::buffer::error& e) {
	      result = -EINVAL;
	      goto fail;
	    }
	    dout(10) << "CEPH_OSD_OP_CMPXATTR name=" << name << " val=" << u64val
		     << " op=" << (int)op.xattr.cmp_op << " mode=" << (int)op.xattr.cmp_mode << dendl;
	    result = do_xattr_cmp_u64(op.xattr.cmp_op, u64val, xattr);
	  }
	  break;

	default:
	  dout(10) << "bad cmp mode " << (int)op.xattr.cmp_mode << dendl;
	  result = -EINVAL;
	}

	if (!result) {
	  dout(10) << "comparison returned false" << dendl;
	  result = -ECANCELED;
	  break;
	}
	if (result < 0) {
	  dout(10) << "comparison returned " << result << " " << cpp_strerror(-result) << dendl;
	  break;
	}

	dout(10) << "comparison returned true" << dendl;
      }
      break;

    case CEPH_OSD_OP_ASSERT_VER:
      ++ctx->num_read;
      {
	uint64_t ver = op.assert_ver.ver;
	tracepoint(osd, do_osd_op_pre_assert_ver, soid.oid.name.c_str(), soid.snap.val, ver);
	if (!ver)
	  result = -EINVAL;
        else if (ver < oi.user_version)
	  result = -ERANGE;
	else if (ver > oi.user_version)
	  result = -EOVERFLOW;
      }
      break;

    case CEPH_OSD_OP_LIST_WATCHERS:
      ++ctx->num_read;
      {
	tracepoint(osd, do_osd_op_pre_list_watchers, soid.oid.name.c_str(), soid.snap.val);
        obj_list_watch_response_t resp;

        map<pair<uint64_t, entity_name_t>, watch_info_t>::const_iterator oi_iter;
        for (oi_iter = oi.watchers.begin(); oi_iter != oi.watchers.end();
                                       ++oi_iter) {
          dout(20) << "key cookie=" << oi_iter->first.first
               << " entity=" << oi_iter->first.second << " "
               << oi_iter->second << dendl;
          ceph_assert(oi_iter->first.first == oi_iter->second.cookie);
          ceph_assert(oi_iter->first.second.is_client());

          watch_item_t wi(oi_iter->first.second, oi_iter->second.cookie,
		 oi_iter->second.timeout_seconds, oi_iter->second.addr);
          resp.entries.push_back(wi);
        }

        resp.encode(osd_op.outdata, ctx->get_features());
        result = 0;

        ctx->delta_stats.num_rd++;
        break;
      }

    case CEPH_OSD_OP_LIST_SNAPS:
      ++ctx->num_read;
      {
	tracepoint(osd, do_osd_op_pre_list_snaps, soid.oid.name.c_str(), soid.snap.val);
        obj_list_snap_response_t resp;

        if (!ssc) {
	  ssc = ctx->obc->ssc = get_snapset_context(soid, false);
        }
        ceph_assert(ssc);
	dout(20) << " snapset " << ssc->snapset << dendl;

        int clonecount = ssc->snapset.clones.size();
	clonecount++;  // for head
        resp.clones.reserve(clonecount);
        for (auto clone_iter = ssc->snapset.clones.begin();
	     clone_iter != ssc->snapset.clones.end(); ++clone_iter) {
          clone_info ci;
          ci.cloneid = *clone_iter;

	  hobject_t clone_oid = soid;
	  clone_oid.snap = *clone_iter;

	  auto p = ssc->snapset.clone_snaps.find(*clone_iter);
	  if (p == ssc->snapset.clone_snaps.end()) {
	    osd->clog->error() << "osd." << osd->whoami
			       << ": inconsistent clone_snaps found for oid "
			       << soid << " clone " << *clone_iter
			       << " snapset " << ssc->snapset;
	    result = -EINVAL;
	    break;
	  }
	  for (auto q = p->second.rbegin(); q != p->second.rend(); ++q) {
	    ci.snaps.push_back(*q);
	  }

          dout(20) << " clone " << *clone_iter << " snaps " << ci.snaps << dendl;

          map<snapid_t, interval_set<uint64_t> >::const_iterator coi;
          coi = ssc->snapset.clone_overlap.find(ci.cloneid);
          if (coi == ssc->snapset.clone_overlap.end()) {
            osd->clog->error() << "osd." << osd->whoami
			       << ": inconsistent clone_overlap found for oid "
			      << soid << " clone " << *clone_iter;
            result = -EINVAL;
            break;
          }
          const interval_set<uint64_t> &o = coi->second;
          ci.overlap.reserve(o.num_intervals());
          for (interval_set<uint64_t>::const_iterator r = o.begin();
               r != o.end(); ++r) {
            ci.overlap.push_back(pair<uint64_t,uint64_t>(r.get_start(),
							 r.get_len()));
          }

          map<snapid_t, uint64_t>::const_iterator si;
          si = ssc->snapset.clone_size.find(ci.cloneid);
          if (si == ssc->snapset.clone_size.end()) {
            osd->clog->error() << "osd." << osd->whoami
			       << ": inconsistent clone_size found for oid "
			       << soid << " clone " << *clone_iter;
            result = -EINVAL;
            break;
          }
          ci.size = si->second;

          resp.clones.push_back(ci);
        }
	if (result < 0) {
	  break;
	}	  
        if (!ctx->obc->obs.oi.is_whiteout()) {
          ceph_assert(obs.exists);
          clone_info ci;
          ci.cloneid = CEPH_NOSNAP;

          //Size for HEAD is oi.size
          ci.size = oi.size;

          resp.clones.push_back(ci);
        }
	resp.seq = ssc->snapset.seq;

        resp.encode(osd_op.outdata);
        result = 0;

        ctx->delta_stats.num_rd++;
        break;
      }

   case CEPH_OSD_OP_NOTIFY:
      ++ctx->num_read;
      {
	uint32_t timeout;
        bufferlist bl;

	try {
	  uint32_t ver; // obsolete
          decode(ver, bp);
	  decode(timeout, bp);
          decode(bl, bp);
	} catch (const ceph::buffer::error &e) {
	  timeout = 0;
	}
	tracepoint(osd, do_osd_op_pre_notify, soid.oid.name.c_str(), soid.snap.val, timeout);
	if (!timeout)
	  timeout = cct->_conf->osd_default_notify_timeout;

	notify_info_t n;
	n.timeout = timeout;
	n.notify_id = osd->get_next_id(get_osdmap_epoch());
	n.cookie = op.notify.cookie;
        n.bl = bl;
	ctx->notifies.push_back(n);

	// return our unique notify id to the client
	encode(n.notify_id, osd_op.outdata);
      }
      break;

    case CEPH_OSD_OP_NOTIFY_ACK:
      ++ctx->num_read;
      {
	try {
	  uint64_t notify_id = 0;
	  uint64_t watch_cookie = 0;
	  decode(notify_id, bp);
	  decode(watch_cookie, bp);
	  bufferlist reply_bl;
	  if (!bp.end()) {
	    decode(reply_bl, bp);
	  }
	  tracepoint(osd, do_osd_op_pre_notify_ack, soid.oid.name.c_str(), soid.snap.val, notify_id, watch_cookie, "Y");
	  OpContext::NotifyAck ack(notify_id, watch_cookie, reply_bl);
	  ctx->notify_acks.push_back(ack);
	} catch (const ceph::buffer::error &e) {
	  tracepoint(osd, do_osd_op_pre_notify_ack, soid.oid.name.c_str(), soid.snap.val, op.watch.cookie, 0, "N");
	  OpContext::NotifyAck ack(
	    // op.watch.cookie is actually the notify_id for historical reasons
	    op.watch.cookie
	    );
	  ctx->notify_acks.push_back(ack);
	}
      }
      break;

    case CEPH_OSD_OP_SETALLOCHINT:
      ++ctx->num_write;
      result = 0;
      {
	tracepoint(osd, do_osd_op_pre_setallochint, soid.oid.name.c_str(), soid.snap.val, op.alloc_hint.expected_object_size, op.alloc_hint.expected_write_size);
	maybe_create_new_object(ctx);
	oi.expected_object_size = op.alloc_hint.expected_object_size;
	oi.expected_write_size = op.alloc_hint.expected_write_size;
	oi.alloc_hint_flags = op.alloc_hint.flags;
        t->set_alloc_hint(soid, op.alloc_hint.expected_object_size,
                          op.alloc_hint.expected_write_size,
			  op.alloc_hint.flags);
      }
      break;


      // --- WRITES ---

      // -- object data --

    case CEPH_OSD_OP_WRITE:
      ++ctx->num_write;
      result = 0;
      { // write
        __u32 seq = oi.truncate_seq;
	tracepoint(osd, do_osd_op_pre_write, soid.oid.name.c_str(), soid.snap.val, oi.size, seq, op.extent.offset, op.extent.length, op.extent.truncate_size, op.extent.truncate_seq);
	if (op.extent.length != osd_op.indata.length()) {
	  result = -EINVAL;
	  break;
	}

	if (pool.info.has_flag(pg_pool_t::FLAG_WRITE_FADVISE_DONTNEED))
	  op.flags = op.flags | CEPH_OSD_OP_FLAG_FADVISE_DONTNEED;

	if (pool.info.requires_aligned_append() &&
	    (op.extent.offset % pool.info.required_alignment() != 0)) {
	  result = -EOPNOTSUPP;
	  break;
	}

	if (!obs.exists) {
	  if (pool.info.requires_aligned_append() && op.extent.offset) {
	    result = -EOPNOTSUPP;
	    break;
	  }
	} else if (op.extent.offset != oi.size &&
		   pool.info.requires_aligned_append()) {
	  result = -EOPNOTSUPP;
	  break;
	}

        if (seq && (seq > op.extent.truncate_seq) &&
            (op.extent.offset + op.extent.length > oi.size)) {
	  // old write, arrived after trimtrunc
	  op.extent.length = (op.extent.offset > oi.size ? 0 : oi.size - op.extent.offset);
	  dout(10) << " old truncate_seq " << op.extent.truncate_seq << " < current " << seq
		   << ", adjusting write length to " << op.extent.length << dendl;
	  bufferlist t;
	  t.substr_of(osd_op.indata, 0, op.extent.length);
	  osd_op.indata.swap(t);
        }
	if (op.extent.truncate_seq > seq) {
	  // write arrives before trimtrunc
	  if (obs.exists && !oi.is_whiteout()) {
	    dout(10) << " truncate_seq " << op.extent.truncate_seq << " > current " << seq
		     << ", truncating to " << op.extent.truncate_size << dendl;
	    t->truncate(soid, op.extent.truncate_size);
	    oi.truncate_seq = op.extent.truncate_seq;
	    oi.truncate_size = op.extent.truncate_size;
	    if (oi.size > op.extent.truncate_size) {
	      interval_set<uint64_t> trim;
	      trim.insert(op.extent.truncate_size,
	        oi.size - op.extent.truncate_size);
	      ctx->modified_ranges.union_of(trim);
	      ctx->clean_regions.mark_data_region_dirty(op.extent.truncate_size, oi.size - op.extent.truncate_size);
	    }
	    if (op.extent.truncate_size != oi.size) {
              truncate_update_size_and_usage(ctx->delta_stats,
                                             oi,
                                             op.extent.truncate_size);
	    }
	  } else {
	    dout(10) << " truncate_seq " << op.extent.truncate_seq << " > current " << seq
		     << ", but object is new" << dendl;
	    oi.truncate_seq = op.extent.truncate_seq;
	    oi.truncate_size = op.extent.truncate_size;
	  }
	}
	result = check_offset_and_length(
	  op.extent.offset, op.extent.length,
	  static_cast<Option::size_t>(osd->osd_max_object_size), get_dpp());
	if (result < 0)
	  break;

	maybe_create_new_object(ctx);

	if (op.extent.length == 0) {
	  if (op.extent.offset > oi.size) {
	    t->truncate(
	      soid, op.extent.offset);
            truncate_update_size_and_usage(ctx->delta_stats, oi,
                                           op.extent.offset);
	  } else {
	    t->nop(soid);
	  }
	} else {
	  t->write(
	    soid, op.extent.offset, op.extent.length, osd_op.indata, op.flags);
	}

	if (op.extent.offset == 0 && op.extent.length >= oi.size
            && !skip_data_digest) {
	  obs.oi.set_data_digest(osd_op.indata.crc32c(-1));
	} else if (op.extent.offset == oi.size && obs.oi.is_data_digest()) {
          if (skip_data_digest) {
            obs.oi.clear_data_digest();
          } else {
	    obs.oi.set_data_digest(osd_op.indata.crc32c(obs.oi.data_digest));
          }
	} else {
	  obs.oi.clear_data_digest();
        }
	write_update_size_and_usage(ctx->delta_stats, oi, ctx->modified_ranges,
				    op.extent.offset, op.extent.length);
	ctx->clean_regions.mark_data_region_dirty(op.extent.offset, op.extent.length);
	dout(10) << "clean_regions modified" << ctx->clean_regions << dendl;
      }
      break;
      
    case CEPH_OSD_OP_WRITEFULL:
      ++ctx->num_write;
      result = 0;
      { // write full object
	tracepoint(osd, do_osd_op_pre_writefull, soid.oid.name.c_str(), soid.snap.val, oi.size, 0, op.extent.length);

	if (op.extent.length != osd_op.indata.length()) {
	  result = -EINVAL;
	  break;
	}
	result = check_offset_and_length(
	  0, op.extent.length,
          static_cast<Option::size_t>(osd->osd_max_object_size), get_dpp());
	if (result < 0)
	  break;

	if (pool.info.has_flag(pg_pool_t::FLAG_WRITE_FADVISE_DONTNEED))
	  op.flags = op.flags | CEPH_OSD_OP_FLAG_FADVISE_DONTNEED;

	maybe_create_new_object(ctx);
	if (pool.info.is_erasure()) {
	  t->truncate(soid, 0);
	} else if (obs.exists && op.extent.length < oi.size) {
	  t->truncate(soid, op.extent.length);
	}
	if (op.extent.length) {
	  t->write(soid, 0, op.extent.length, osd_op.indata, op.flags);
	}
        if (!skip_data_digest) {
	  obs.oi.set_data_digest(osd_op.indata.crc32c(-1));
        } else {
	  obs.oi.clear_data_digest();
	}
        ctx->clean_regions.mark_data_region_dirty(0,
          std::max((uint64_t)op.extent.length, oi.size));
	write_update_size_and_usage(ctx->delta_stats, oi, ctx->modified_ranges,
	    0, op.extent.length, true);
      }
      break;

    case CEPH_OSD_OP_WRITESAME:
      ++ctx->num_write;
      tracepoint(osd, do_osd_op_pre_writesame, soid.oid.name.c_str(), soid.snap.val, oi.size, op.writesame.offset, op.writesame.length, op.writesame.data_length);
      result = do_writesame(ctx, osd_op);
      break;

    case CEPH_OSD_OP_ROLLBACK :
      ++ctx->num_write;
      tracepoint(osd, do_osd_op_pre_rollback, soid.oid.name.c_str(), soid.snap.val);
      result = _rollback_to(ctx, op);
      break;

    case CEPH_OSD_OP_ZERO:
      tracepoint(osd, do_osd_op_pre_zero, soid.oid.name.c_str(), soid.snap.val, op.extent.offset, op.extent.length);
      if (pool.info.requires_aligned_append()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_write;
      { // zero
	result = check_offset_and_length(
	  op.extent.offset, op.extent.length,
          static_cast<Option::size_t>(osd->osd_max_object_size), get_dpp());
	if (result < 0)
	  break;
 
	ceph_assert(op.extent.length);
	if (obs.exists && !oi.is_whiteout()) {
	  t->zero(soid, op.extent.offset, op.extent.length);
	  interval_set<uint64_t> ch;
	  ch.insert(op.extent.offset, op.extent.length);
	  ctx->modified_ranges.union_of(ch);
	  ctx->clean_regions.mark_data_region_dirty(op.extent.offset, op.extent.length);
	  ctx->delta_stats.num_wr++;
	  oi.clear_data_digest();
	} else {
	  // no-op
	}
      }
      break;
    case CEPH_OSD_OP_CREATE:
      ++ctx->num_write;
      result = 0;
      {
	tracepoint(osd, do_osd_op_pre_create, soid.oid.name.c_str(), soid.snap.val);
	if (obs.exists && !oi.is_whiteout() &&
	    (op.flags & CEPH_OSD_OP_FLAG_EXCL)) {
          result = -EEXIST; /* this is an exclusive create */
	} else {
	  if (osd_op.indata.length()) {
	    auto p = osd_op.indata.cbegin();
	    string category;
	    try {
	      decode(category, p);
	    }
	    catch (ceph::buffer::error& e) {
	      result = -EINVAL;
	      goto fail;
	    }
	    // category is no longer implemented.
	  }
	  maybe_create_new_object(ctx);
	  t->nop(soid);
	}
      }
      break;

    case CEPH_OSD_OP_TRIMTRUNC:
      op.extent.offset = op.extent.truncate_size;
      // falling through

    case CEPH_OSD_OP_TRUNCATE:
      tracepoint(osd, do_osd_op_pre_truncate, soid.oid.name.c_str(), soid.snap.val, oi.size, oi.truncate_seq, op.extent.offset, op.extent.length, op.extent.truncate_size, op.extent.truncate_seq);
      if (pool.info.requires_aligned_append()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_write;
      result = 0;
      {
	// truncate
	if (!obs.exists || oi.is_whiteout()) {
	  dout(10) << " object dne, truncate is a no-op" << dendl;
	  break;
	}

        result = check_offset_and_length(
	  op.extent.offset, op.extent.length,
          static_cast<Option::size_t>(osd->osd_max_object_size), get_dpp());
        if (result < 0)
	  break;

	if (op.extent.truncate_seq) {
	  ceph_assert(op.extent.offset == op.extent.truncate_size);
	  if (op.extent.truncate_seq <= oi.truncate_seq) {
	    dout(10) << " truncate seq " << op.extent.truncate_seq << " <= current " << oi.truncate_seq
		     << ", no-op" << dendl;
	    break; // old
	  }
	  dout(10) << " truncate seq " << op.extent.truncate_seq << " > current " << oi.truncate_seq
		   << ", truncating" << dendl;
	  oi.truncate_seq = op.extent.truncate_seq;
	  oi.truncate_size = op.extent.truncate_size;
	}

	maybe_create_new_object(ctx);
	t->truncate(soid, op.extent.offset);
	if (oi.size > op.extent.offset) {
	  interval_set<uint64_t> trim;
	  trim.insert(op.extent.offset, oi.size-op.extent.offset);
	  ctx->modified_ranges.union_of(trim);
	  ctx->clean_regions.mark_data_region_dirty(op.extent.offset, oi.size - op.extent.offset);
	} else if (oi.size < op.extent.offset) {
          ctx->clean_regions.mark_data_region_dirty(oi.size, op.extent.offset - oi.size);
        }
	if (op.extent.offset != oi.size) {
          truncate_update_size_and_usage(ctx->delta_stats,
                                         oi,
                                         op.extent.offset);
	}
	ctx->delta_stats.num_wr++;
	// do no set exists, or we will break above DELETE -> TRUNCATE munging.

	oi.clear_data_digest();
      }
      break;
    
    case CEPH_OSD_OP_DELETE:
      ++ctx->num_write;
      result = 0;
      tracepoint(osd, do_osd_op_pre_delete, soid.oid.name.c_str(), soid.snap.val);
      {
	result = _delete_oid(ctx, false, ctx->ignore_cache);
      }
      break;

    case CEPH_OSD_OP_WATCH:
      ++ctx->num_write;
      result = 0;
      {
	tracepoint(osd, do_osd_op_pre_watch, soid.oid.name.c_str(), soid.snap.val,
		   op.watch.cookie, op.watch.op);
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	result = 0;
        uint64_t cookie = op.watch.cookie;
        entity_name_t entity = ctx->reqid.name;
	ObjectContextRef obc = ctx->obc;

	dout(10) << "watch " << ceph_osd_watch_op_name(op.watch.op)
		 << ": ctx->obc=" << (void *)obc.get() << " cookie=" << cookie
		 << " oi.version=" << oi.version.version << " ctx->at_version=" << ctx->at_version << dendl;
	dout(10) << "watch: oi.user_version=" << oi.user_version<< dendl;
	dout(10) << "watch: peer_addr="
	  << ctx->op->get_req()->get_connection()->get_peer_addr() << dendl;

	uint32_t timeout = cct->_conf->osd_client_watch_timeout;
	if (op.watch.timeout != 0) {
	  timeout = op.watch.timeout;
	}

	watch_info_t w(cookie, timeout,
	  ctx->op->get_req()->get_connection()->get_peer_addr());
	if (op.watch.op == CEPH_OSD_WATCH_OP_WATCH ||
	    op.watch.op == CEPH_OSD_WATCH_OP_LEGACY_WATCH) {
	  if (oi.watchers.count(make_pair(cookie, entity))) {
	    dout(10) << " found existing watch " << w << " by " << entity << dendl;
	  } else {
	    dout(10) << " registered new watch " << w << " by " << entity << dendl;
	    oi.watchers[make_pair(cookie, entity)] = w;
	    t->nop(soid);  // make sure update the object_info on disk!
	  }
	  bool will_ping = (op.watch.op == CEPH_OSD_WATCH_OP_WATCH);
	  ctx->watch_connects.push_back(make_pair(w, will_ping));
        } else if (op.watch.op == CEPH_OSD_WATCH_OP_RECONNECT) {
	  if (!oi.watchers.count(make_pair(cookie, entity))) {
	    result = -ENOTCONN;
	    break;
	  }
	  dout(10) << " found existing watch " << w << " by " << entity << dendl;
	  ctx->watch_connects.push_back(make_pair(w, true));
        } else if (op.watch.op == CEPH_OSD_WATCH_OP_PING) {
	  /* Note: WATCH with PING doesn't cause may_write() to return true,
	   * so if there is nothing else in the transaction, this is going
	   * to run do_osd_op_effects, but not write out a log entry */
	  if (!oi.watchers.count(make_pair(cookie, entity))) {
	    result = -ENOTCONN;
	    break;
	  }
	  map<pair<uint64_t,entity_name_t>,WatchRef>::iterator p =
	    obc->watchers.find(make_pair(cookie, entity));
	  if (p == obc->watchers.end() ||
	      !p->second->is_connected()) {
	    // client needs to reconnect
	    result = -ETIMEDOUT;
	    break;
	  }
	  dout(10) << " found existing watch " << w << " by " << entity << dendl;
	  p->second->got_ping(ceph_clock_now());
	  result = 0;
        } else if (op.watch.op == CEPH_OSD_WATCH_OP_UNWATCH) {
	  map<pair<uint64_t, entity_name_t>, watch_info_t>::iterator oi_iter =
	    oi.watchers.find(make_pair(cookie, entity));
	  if (oi_iter != oi.watchers.end()) {
	    dout(10) << " removed watch " << oi_iter->second << " by "
		     << entity << dendl;
            oi.watchers.erase(oi_iter);
	    t->nop(soid);  // update oi on disk
	    ctx->watch_disconnects.push_back(
	      watch_disconnect_t(cookie, entity, false));
	  } else {
	    dout(10) << " can't remove: no watch by " << entity << dendl;
	  }
        }
      }
      break;

    case CEPH_OSD_OP_CACHE_PIN:
      tracepoint(osd, do_osd_op_pre_cache_pin, soid.oid.name.c_str(), soid.snap.val);
      if ((!pool.info.is_tier() ||
	  pool.info.cache_mode == pg_pool_t::CACHEMODE_NONE)) {
        result = -EINVAL;
        dout(10) << " pin object is only allowed on the cache tier " << dendl;
        break;
      }
      ++ctx->num_write;
      result = 0;
      {
	if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  break;
	}

	if (!oi.is_cache_pinned()) {
	  oi.set_flag(object_info_t::FLAG_CACHE_PIN);
	  ctx->modify = true;
	  ctx->delta_stats.num_objects_pinned++;
	  ctx->delta_stats.num_wr++;
	}
      }
      break;

    case CEPH_OSD_OP_CACHE_UNPIN:
      tracepoint(osd, do_osd_op_pre_cache_unpin, soid.oid.name.c_str(), soid.snap.val);
      if ((!pool.info.is_tier() ||
	  pool.info.cache_mode == pg_pool_t::CACHEMODE_NONE)) {
        result = -EINVAL;
        dout(10) << " pin object is only allowed on the cache tier " << dendl;
        break;
      }
      ++ctx->num_write;
      result = 0;
      {
	if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  break;
	}

	if (oi.is_cache_pinned()) {
	  oi.clear_flag(object_info_t::FLAG_CACHE_PIN);
	  ctx->modify = true;
	  ctx->delta_stats.num_objects_pinned--;
	  ctx->delta_stats.num_wr++;
	}
      }
      break;

    case CEPH_OSD_OP_SET_REDIRECT:
      ++ctx->num_write;
      result = 0;
      {
	if (pool.info.is_tier()) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	if (get_osdmap()->require_osd_release < ceph_release_t::luminous) {
	  result = -EOPNOTSUPP;
	  break;
	}

	object_t target_name;
	object_locator_t target_oloc;
	snapid_t target_snapid = (uint64_t)op.copy_from.snapid;
	version_t target_version = op.copy_from.src_version;
	try {
	  decode(target_name, bp);
	  decode(target_oloc, bp);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	pg_t raw_pg;
	get_osdmap()->object_locator_to_pg(target_name, target_oloc, raw_pg);
	hobject_t target(target_name, target_oloc.key, target_snapid,
		raw_pg.ps(), raw_pg.pool(),
		target_oloc.nspace);
	if (target == soid) {
	  dout(20) << " set-redirect self is invalid" << dendl;
	  result = -EINVAL;
	  break;
	}

	bool need_reference = (osd_op.op.flags & CEPH_OSD_OP_FLAG_WITH_REFERENCE);
	bool has_reference = (oi.flags & object_info_t::FLAG_REDIRECT_HAS_REFERENCE);
	if (has_reference) {
	  result = -EINVAL;
	  dout(5) << " the object is already a manifest " << dendl;
	  break;
	}
	if (op_finisher == nullptr && need_reference) {
	  // start
	  ctx->op_finishers[ctx->current_osd_subop_num].reset(
	    new SetManifestFinisher(osd_op));
	  RefCountCallback *fin = new RefCountCallback(ctx, osd_op);
	  refcount_manifest(ctx->obc->obs.oi.soid, target, 
			    refcount_t::INCREMENT_REF, fin);
	  result = -EINPROGRESS;
	} else {
	  // finish
	  if (op_finisher) {
	    result = op_finisher->execute();
	    ceph_assert(result == 0);
	  }

	  if (!oi.has_manifest() && !oi.manifest.is_redirect())
	    ctx->delta_stats.num_objects_manifest++;

	  oi.set_flag(object_info_t::FLAG_MANIFEST);
	  oi.manifest.redirect_target = target;
	  oi.manifest.type = object_manifest_t::TYPE_REDIRECT;
	  t->truncate(soid, 0);
          ctx->clean_regions.mark_data_region_dirty(0, oi.size);
	  if (oi.is_omap() && pool.info.supports_omap()) {
	    t->omap_clear(soid);
	    obs.oi.clear_omap_digest();
	    obs.oi.clear_flag(object_info_t::FLAG_OMAP);
            ctx->clean_regions.mark_omap_dirty();
	  }
          write_update_size_and_usage(ctx->delta_stats, oi, ctx->modified_ranges,
	    0, oi.size, false);
	  ctx->delta_stats.num_bytes -= oi.size;
	  oi.size = 0;
	  oi.new_object();
	  oi.user_version = target_version;
	  ctx->user_at_version = target_version;
	  /* rm_attrs */
	  map<string,bufferlist> rmattrs;
	  result = getattrs_maybe_cache(ctx->obc, &rmattrs);
	  if (result < 0) {
	    dout(10) << __func__ << " error: " << cpp_strerror(result) << dendl;
	    return result;
	  }
	  map<string, bufferlist>::iterator iter;
	  for (iter = rmattrs.begin(); iter != rmattrs.end(); ++iter) {
	    const string& name = iter->first;
	    t->rmattr(soid, name);
	  }
	  if (!has_reference && need_reference) {
	    oi.set_flag(object_info_t::FLAG_REDIRECT_HAS_REFERENCE);
	  }
	  dout(10) << "set-redirect oid:" << oi.soid << " user_version: " << oi.user_version << dendl;
	  if (op_finisher) {
	    ctx->op_finishers.erase(ctx->current_osd_subop_num);
	  }
	}
      }

      break;

    case CEPH_OSD_OP_SET_CHUNK:
      ++ctx->num_write;
      result = 0;
      {
	if (pool.info.is_tier()) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	if (get_osdmap()->require_osd_release < ceph_release_t::luminous) {
	  result = -EOPNOTSUPP;
	  break;
	}
	if (ctx->snapc.snaps.size() || 
	    (ctx->obc->ssc && ctx->obc->ssc->snapset.clones.size()) ) {
	  result = -EOPNOTSUPP;
	  break;
	}

	object_locator_t tgt_oloc;
	uint64_t src_offset, src_length, tgt_offset;
	object_t tgt_name;
	try {
	  decode(src_offset, bp);
	  decode(src_length, bp);
	  decode(tgt_oloc, bp);
	  decode(tgt_name, bp);
	  decode(tgt_offset, bp);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	
	if (!src_length) {
	  result = -EINVAL;
	  goto fail;
	}

	for (auto &p : oi.manifest.chunk_map) {
	  if ((p.first <= src_offset && p.first + p.second.length > src_offset) ||
	      (p.first > src_offset && p.first <= src_offset + src_length)) {
	    dout(20) << __func__ << " overlapped !! offset: " << src_offset << " length: " << src_length
		    << " chunk_info: " << p << dendl;
	    result = -EOPNOTSUPP;
	    goto fail;
	  }
	}

        if (!oi.manifest.is_chunked()) {
          oi.manifest.clear();
        }

	pg_t raw_pg;
	chunk_info_t chunk_info;
	get_osdmap()->object_locator_to_pg(tgt_name, tgt_oloc, raw_pg);
	hobject_t target(tgt_name, tgt_oloc.key, snapid_t(),
			 raw_pg.ps(), raw_pg.pool(),
			 tgt_oloc.nspace);
	bool need_reference = (osd_op.op.flags & CEPH_OSD_OP_FLAG_WITH_REFERENCE);
	bool has_reference = (oi.manifest.chunk_map.find(src_offset) != oi.manifest.chunk_map.end()) &&
			     (oi.manifest.chunk_map[src_offset].flags & chunk_info_t::FLAG_HAS_REFERENCE);
	if (has_reference) {
	  result = -EINVAL;
	  dout(5) << " the object is already a manifest " << dendl;
	  break;
	}
	if (op_finisher == nullptr && need_reference) {
	  // start
	  ctx->op_finishers[ctx->current_osd_subop_num].reset(
	    new SetManifestFinisher(osd_op));
	  RefCountCallback *fin = new RefCountCallback(ctx, osd_op);
	  refcount_manifest(ctx->obc->obs.oi.soid, target, 
			    refcount_t::INCREMENT_REF, fin);
	  result = -EINPROGRESS;
	} else {
	  if (op_finisher) {
	    result = op_finisher->execute();
	    ceph_assert(result == 0);
	  }

	  chunk_info_t chunk_info;
	  chunk_info.set_flag(chunk_info_t::FLAG_MISSING);
	  chunk_info.oid = target;
	  chunk_info.offset = tgt_offset;
	  chunk_info.length = src_length;
	  oi.manifest.chunk_map[src_offset] = chunk_info;
	  if (!oi.has_manifest() && !oi.manifest.is_chunked()) 
	    ctx->delta_stats.num_objects_manifest++;
	  oi.set_flag(object_info_t::FLAG_MANIFEST);
	  oi.manifest.type = object_manifest_t::TYPE_CHUNKED;
	  if (!has_reference && need_reference) {
	    oi.manifest.chunk_map[src_offset].set_flag(chunk_info_t::FLAG_HAS_REFERENCE);
	  }
	  if (need_reference && pool.info.get_fingerprint_type() != pg_pool_t::TYPE_FINGERPRINT_NONE) {
	    oi.manifest.chunk_map[src_offset].set_flag(chunk_info_t::FLAG_HAS_FINGERPRINT);
	  }
	  ctx->modify = true;

	  dout(10) << "set-chunked oid:" << oi.soid << " user_version: " << oi.user_version 
		   << " chunk_info: " << chunk_info << dendl;
	  if (op_finisher) {
	    ctx->op_finishers.erase(ctx->current_osd_subop_num);
	  }
	}
      }

      break;

    case CEPH_OSD_OP_TIER_PROMOTE:
      ++ctx->num_write;
      result = 0;
      {
	if (pool.info.is_tier()) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	if (get_osdmap()->require_osd_release < ceph_release_t::luminous) {
	  result = -EOPNOTSUPP;
	  break;
	}
	if (!obs.oi.has_manifest()) {
	  result = 0;
	  break;
	}

	if (op_finisher == nullptr) {
	  PromoteManifestCallback *cb;
	  object_locator_t my_oloc;
	  hobject_t src_hoid;

	  if (obs.oi.manifest.is_chunked()) {
	    src_hoid = obs.oi.soid;
	  } else if (obs.oi.manifest.is_redirect()) {
	    object_locator_t src_oloc(obs.oi.manifest.redirect_target);
	    my_oloc = src_oloc;
	    src_hoid = obs.oi.manifest.redirect_target;
	  } else {
	    ceph_abort_msg("unrecognized manifest type");
	  }
	  cb = new PromoteManifestCallback(ctx->obc, this, ctx);
          ctx->op_finishers[ctx->current_osd_subop_num].reset(
            new PromoteFinisher(cb));
	  unsigned flags = CEPH_OSD_COPY_FROM_FLAG_IGNORE_OVERLAY |
			   CEPH_OSD_COPY_FROM_FLAG_IGNORE_CACHE |
			   CEPH_OSD_COPY_FROM_FLAG_MAP_SNAP_CLONE |
			   CEPH_OSD_COPY_FROM_FLAG_RWORDERED;
	  unsigned src_fadvise_flags = LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL;
	  start_copy(cb, ctx->obc, src_hoid, my_oloc, 0, flags,
		     obs.oi.soid.snap == CEPH_NOSNAP,
		     src_fadvise_flags, 0);

	  dout(10) << "tier-promote oid:" << oi.soid << " manifest: " << obs.oi.manifest << dendl;
	  result = -EINPROGRESS;
	} else {
	  result = op_finisher->execute();
	  ceph_assert(result == 0);
	  ctx->op_finishers.erase(ctx->current_osd_subop_num);
	}
      }

      break;

    case CEPH_OSD_OP_TIER_FLUSH:
      ++ctx->num_write;
      result = 0;
      {
	if (pool.info.is_tier()) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	if (get_osdmap()->require_osd_release < ceph_release_t::octopus) {
	  result = -EOPNOTSUPP;
	  break;
	}
	if (!obs.oi.has_manifest()) {
	  result = 0;
	  break;
	}

	hobject_t missing;
	bool is_dirty = false;
	for (auto& p : ctx->obc->obs.oi.manifest.chunk_map) {
	  if (p.second.is_dirty()) {
	    is_dirty = true;
	    break;
	  }
	}

	if (is_dirty) {
	  result = start_flush(ctx->op, ctx->obc, true, NULL, std::nullopt);
	  if (result == -EINPROGRESS)
	    result = -EAGAIN;
	} else {
	  result = 0;
	}
      }

      break;

    case CEPH_OSD_OP_UNSET_MANIFEST:
      ++ctx->num_write;
      result = 0;
      {
	if (pool.info.is_tier()) {
	  result = -EINVAL;
	  break;
	}
	if (!obs.exists) {
	  result = -ENOENT;
	  break;
	}
	if (!oi.has_manifest()) {
	  result = -EOPNOTSUPP;
	  break;
	}
	if (get_osdmap()->require_osd_release < ceph_release_t::luminous) {
	  result = -EOPNOTSUPP;
	  break;
	}

	dec_all_refcount_manifest(oi, ctx);

	oi.clear_flag(object_info_t::FLAG_MANIFEST);
	oi.manifest = object_manifest_t();
	ctx->delta_stats.num_objects_manifest--;
	ctx->delta_stats.num_wr++;
	ctx->modify = true;
      }

      break;

      // -- object attrs --
      
    case CEPH_OSD_OP_SETXATTR:
      ++ctx->num_write;
      result = 0;
      {
	if (cct->_conf->osd_max_attr_size > 0 &&
	    op.xattr.value_len > cct->_conf->osd_max_attr_size) {
	  tracepoint(osd, do_osd_op_pre_setxattr, soid.oid.name.c_str(), soid.snap.val, "???");
	  result = -EFBIG;
	  break;
	}
	unsigned max_name_len =
	  std::min<uint64_t>(osd->store->get_max_attr_name_length(),
			     cct->_conf->osd_max_attr_name_len);
	if (op.xattr.name_len > max_name_len) {
	  result = -ENAMETOOLONG;
	  break;
	}
	maybe_create_new_object(ctx);
	string aname;
	bp.copy(op.xattr.name_len, aname);
	tracepoint(osd, do_osd_op_pre_setxattr, soid.oid.name.c_str(), soid.snap.val, aname.c_str());
	string name = "_" + aname;
	bufferlist bl;
	bp.copy(op.xattr.value_len, bl);
	t->setattr(soid, name, bl);
 	ctx->delta_stats.num_wr++;
      }
      break;

    case CEPH_OSD_OP_RMXATTR:
      ++ctx->num_write;
      result = 0;
      {
	string aname;
	bp.copy(op.xattr.name_len, aname);
	tracepoint(osd, do_osd_op_pre_rmxattr, soid.oid.name.c_str(), soid.snap.val, aname.c_str());
	if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  break;
	}
	string name = "_" + aname;
	t->rmattr(soid, name);
 	ctx->delta_stats.num_wr++;
      }
      break;
    

      // -- fancy writers --
    case CEPH_OSD_OP_APPEND:
      {
	tracepoint(osd, do_osd_op_pre_append, soid.oid.name.c_str(), soid.snap.val, oi.size, oi.truncate_seq, op.extent.offset, op.extent.length, op.extent.truncate_size, op.extent.truncate_seq);
	// just do it inline; this works because we are happy to execute
	// fancy op on replicas as well.
	vector<OSDOp> nops(1);
	OSDOp& newop = nops[0];
	newop.op.op = CEPH_OSD_OP_WRITE;
	newop.op.extent.offset = oi.size;
	newop.op.extent.length = op.extent.length;
	newop.op.extent.truncate_seq = oi.truncate_seq;
        newop.indata = osd_op.indata;
	result = do_osd_ops(ctx, nops);
	osd_op.outdata.claim(newop.outdata);
      }
      break;

    case CEPH_OSD_OP_STARTSYNC:
      result = 0;
      t->nop(soid);
      break;

      // -- trivial map --
    case CEPH_OSD_OP_TMAPGET:
      tracepoint(osd, do_osd_op_pre_tmapget, soid.oid.name.c_str(), soid.snap.val);
      if (pool.info.is_erasure()) {
	result = -EOPNOTSUPP;
	break;
      }
      {
	vector<OSDOp> nops(1);
	OSDOp& newop = nops[0];
	newop.op.op = CEPH_OSD_OP_SYNC_READ;
	newop.op.extent.offset = 0;
	newop.op.extent.length = 0;
	result = do_osd_ops(ctx, nops);
	osd_op.outdata.claim(newop.outdata);
      }
      break;

    case CEPH_OSD_OP_TMAPPUT:
      tracepoint(osd, do_osd_op_pre_tmapput, soid.oid.name.c_str(), soid.snap.val);
      if (pool.info.is_erasure()) {
	result = -EOPNOTSUPP;
	break;
      }
      {
	//_dout_lock.Lock();
	//osd_op.data.hexdump(*_dout);
	//_dout_lock.Unlock();

	// verify sort order
	bool unsorted = false;
	if (true) {
	  bufferlist header;
	  decode(header, bp);
	  uint32_t n;
	  decode(n, bp);
	  string last_key;
	  while (n--) {
	    string key;
	    decode(key, bp);
	    dout(10) << "tmapput key " << key << dendl;
	    bufferlist val;
	    decode(val, bp);
	    if (key < last_key) {
	      dout(10) << "TMAPPUT is unordered; resorting" << dendl;
	      unsorted = true;
	      break;
	    }
	    last_key = key;
	  }
	}

	// write it
	vector<OSDOp> nops(1);
	OSDOp& newop = nops[0];
	newop.op.op = CEPH_OSD_OP_WRITEFULL;
	newop.op.extent.offset = 0;
	newop.op.extent.length = osd_op.indata.length();
	newop.indata = osd_op.indata;

	if (unsorted) {
	  bp = osd_op.indata.begin();
	  bufferlist header;
	  map<string, bufferlist> m;
	  decode(header, bp);
	  decode(m, bp);
	  ceph_assert(bp.end());
	  bufferlist newbl;
	  encode(header, newbl);
	  encode(m, newbl);
	  newop.indata = newbl;
	}
	result = do_osd_ops(ctx, nops);
	ceph_assert(result == 0);
      }
      break;

    case CEPH_OSD_OP_TMAPUP:
      tracepoint(osd, do_osd_op_pre_tmapup, soid.oid.name.c_str(), soid.snap.val);
      if (pool.info.is_erasure()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_write;
      result = do_tmapup(ctx, bp, osd_op);
      break;

    case CEPH_OSD_OP_TMAP2OMAP:
      ++ctx->num_write;
      tracepoint(osd, do_osd_op_pre_tmap2omap, soid.oid.name.c_str(), soid.snap.val);
      result = do_tmap2omap(ctx, op.tmap2omap.flags);
      break;

      // OMAP Read ops
    case CEPH_OSD_OP_OMAPGETKEYS:
      ++ctx->num_read;
      {
	string start_after;
	uint64_t max_return;
	try {
	  decode(start_after, bp);
	  decode(max_return, bp);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  tracepoint(osd, do_osd_op_pre_omapgetkeys, soid.oid.name.c_str(), soid.snap.val, "???", 0);
	  goto fail;
	}
	if (max_return > cct->_conf->osd_max_omap_entries_per_request) {
	  max_return = cct->_conf->osd_max_omap_entries_per_request;
	}
	tracepoint(osd, do_osd_op_pre_omapgetkeys, soid.oid.name.c_str(), soid.snap.val, start_after.c_str(), max_return);

	bufferlist bl;
	uint32_t num = 0;
	bool truncated = false;
	if (oi.is_omap()) {
	  ObjectMap::ObjectMapIterator iter = osd->store->get_omap_iterator(
	    ch, ghobject_t(soid)
	    );
	  ceph_assert(iter);
	  iter->upper_bound(start_after);
	  for (num = 0; iter->valid(); ++num, iter->next()) {
	    if (num >= max_return ||
		bl.length() >= cct->_conf->osd_max_omap_bytes_per_request) {
	      truncated = true;
	      break;
	    }
	    encode(iter->key(), bl);
	  }
	} // else return empty out_set
	encode(num, osd_op.outdata);
	osd_op.outdata.claim_append(bl);
	encode(truncated, osd_op.outdata);
	ctx->delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_OMAPGETVALS:
      ++ctx->num_read;
      {
	string start_after;
	uint64_t max_return;
	string filter_prefix;
	try {
	  decode(start_after, bp);
	  decode(max_return, bp);
	  decode(filter_prefix, bp);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  tracepoint(osd, do_osd_op_pre_omapgetvals, soid.oid.name.c_str(), soid.snap.val, "???", 0, "???");
	  goto fail;
	}
	if (max_return > cct->_conf->osd_max_omap_entries_per_request) {
	  max_return = cct->_conf->osd_max_omap_entries_per_request;
	}
	tracepoint(osd, do_osd_op_pre_omapgetvals, soid.oid.name.c_str(), soid.snap.val, start_after.c_str(), max_return, filter_prefix.c_str());

	uint32_t num = 0;
	bool truncated = false;
	bufferlist bl;
	if (oi.is_omap()) {
	  ObjectMap::ObjectMapIterator iter = osd->store->get_omap_iterator(
	    ch, ghobject_t(soid)
	    );
          if (!iter) {
            result = -ENOENT;
            goto fail;
          }
	  iter->upper_bound(start_after);
	  if (filter_prefix > start_after) iter->lower_bound(filter_prefix);
	  for (num = 0;
	       iter->valid() &&
		 iter->key().substr(0, filter_prefix.size()) == filter_prefix;
	       ++num, iter->next()) {
	    dout(20) << "Found key " << iter->key() << dendl;
	    if (num >= max_return ||
		bl.length() >= cct->_conf->osd_max_omap_bytes_per_request) {
	      truncated = true;
	      break;
	    }
	    encode(iter->key(), bl);
	    encode(iter->value(), bl);
	  }
	} // else return empty out_set
	encode(num, osd_op.outdata);
	osd_op.outdata.claim_append(bl);
	encode(truncated, osd_op.outdata);
	ctx->delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_OMAPGETHEADER:
      tracepoint(osd, do_osd_op_pre_omapgetheader, soid.oid.name.c_str(), soid.snap.val);
      if (!oi.is_omap()) {
	// return empty header
	break;
      }
      ++ctx->num_read;
      {
	osd->store->omap_get_header(ch, ghobject_t(soid), &osd_op.outdata);
	ctx->delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_OMAPGETVALSBYKEYS:
      ++ctx->num_read;
      {
	set<string> keys_to_get;
	try {
	  decode(keys_to_get, bp);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  tracepoint(osd, do_osd_op_pre_omapgetvalsbykeys, soid.oid.name.c_str(), soid.snap.val, "???");
	  goto fail;
	}
	tracepoint(osd, do_osd_op_pre_omapgetvalsbykeys, soid.oid.name.c_str(), soid.snap.val, list_entries(keys_to_get).c_str());
	map<string, bufferlist> out;
	if (oi.is_omap()) {
	  osd->store->omap_get_values(ch, ghobject_t(soid), keys_to_get, &out);
	} // else return empty omap entries
	encode(out, osd_op.outdata);
	ctx->delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
	ctx->delta_stats.num_rd++;
      }
      break;

    case CEPH_OSD_OP_OMAP_CMP:
      ++ctx->num_read;
      {
	if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  tracepoint(osd, do_osd_op_pre_omap_cmp, soid.oid.name.c_str(), soid.snap.val, "???");
	  break;
	}
	map<string, pair<bufferlist, int> > assertions;
	try {
	  decode(assertions, bp);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  tracepoint(osd, do_osd_op_pre_omap_cmp, soid.oid.name.c_str(), soid.snap.val, "???");
	  goto fail;
	}
	tracepoint(osd, do_osd_op_pre_omap_cmp, soid.oid.name.c_str(), soid.snap.val, list_keys(assertions).c_str());
	
	map<string, bufferlist> out;

	if (oi.is_omap()) {
	  set<string> to_get;
	  for (map<string, pair<bufferlist, int> >::iterator i = assertions.begin();
	       i != assertions.end();
	       ++i)
	    to_get.insert(i->first);
	  int r = osd->store->omap_get_values(ch, ghobject_t(soid),
					      to_get, &out);
	  if (r < 0) {
	    result = r;
	    break;
	  }
	} // else leave out empty

	//Should set num_rd_kb based on encode length of map
	ctx->delta_stats.num_rd++;

	int r = 0;
	bufferlist empty;
	for (map<string, pair<bufferlist, int> >::iterator i = assertions.begin();
	     i != assertions.end();
	     ++i) {
	  auto out_entry = out.find(i->first);
	  bufferlist &bl = (out_entry != out.end()) ?
	    out_entry->second : empty;
	  switch (i->second.second) {
	  case CEPH_OSD_CMPXATTR_OP_EQ:
	    if (!(bl == i->second.first)) {
	      r = -ECANCELED;
	    }
	    break;
	  case CEPH_OSD_CMPXATTR_OP_LT:
	    if (!(bl < i->second.first)) {
	      r = -ECANCELED;
	    }
	    break;
	  case CEPH_OSD_CMPXATTR_OP_GT:
	    if (!(bl > i->second.first)) {
	      r = -ECANCELED;
	    }
	    break;
	  default:
	    r = -EINVAL;
	    break;
	  }
	  if (r < 0)
	    break;
	}
	if (r < 0) {
	  result = r;
	}
      }
      break;

      // OMAP Write ops
    case CEPH_OSD_OP_OMAPSETVALS:
      if (!pool.info.supports_omap()) {
	result = -EOPNOTSUPP;
	tracepoint(osd, do_osd_op_pre_omapsetvals, soid.oid.name.c_str(), soid.snap.val);
	break;
      }
      ++ctx->num_write;
      result = 0;
      {
	maybe_create_new_object(ctx);
	bufferlist to_set_bl;
	try {
	  decode_str_str_map_to_bl(bp, &to_set_bl);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  tracepoint(osd, do_osd_op_pre_omapsetvals, soid.oid.name.c_str(), soid.snap.val);
	  goto fail;
	}
	tracepoint(osd, do_osd_op_pre_omapsetvals, soid.oid.name.c_str(), soid.snap.val);
	if (cct->_conf->subsys.should_gather<dout_subsys, 20>()) {
	  dout(20) << "setting vals: " << dendl;
	  map<string,bufferlist> to_set;
	  bufferlist::const_iterator pt = to_set_bl.begin();
	  decode(to_set, pt);
	  for (map<string, bufferlist>::iterator i = to_set.begin();
	       i != to_set.end();
	       ++i) {
	    dout(20) << "\t" << i->first << dendl;
	  }
	}
	t->omap_setkeys(soid, to_set_bl);
	ctx->clean_regions.mark_omap_dirty();
	ctx->delta_stats.num_wr++;
        ctx->delta_stats.num_wr_kb += shift_round_up(to_set_bl.length(), 10);
      }
      obs.oi.set_flag(object_info_t::FLAG_OMAP);
      obs.oi.clear_omap_digest();
      break;

    case CEPH_OSD_OP_OMAPSETHEADER:
      tracepoint(osd, do_osd_op_pre_omapsetheader, soid.oid.name.c_str(), soid.snap.val);
      if (!pool.info.supports_omap()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_write;
      result = 0;
      {
	maybe_create_new_object(ctx);
	t->omap_setheader(soid, osd_op.indata);
	ctx->clean_regions.mark_omap_dirty();
	ctx->delta_stats.num_wr++;
      }
      obs.oi.set_flag(object_info_t::FLAG_OMAP);
      obs.oi.clear_omap_digest();
      break;

    case CEPH_OSD_OP_OMAPCLEAR:
      tracepoint(osd, do_osd_op_pre_omapclear, soid.oid.name.c_str(), soid.snap.val);
      if (!pool.info.supports_omap()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_write;
      result = 0;
      {
	if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  break;
	}
	if (oi.is_omap()) {
	  t->omap_clear(soid);
	  ctx->clean_regions.mark_omap_dirty();
	  ctx->delta_stats.num_wr++;
	  obs.oi.clear_omap_digest();
	  obs.oi.clear_flag(object_info_t::FLAG_OMAP);
	}
      }
      break;

    case CEPH_OSD_OP_OMAPRMKEYS:
      if (!pool.info.supports_omap()) {
	result = -EOPNOTSUPP;
	tracepoint(osd, do_osd_op_pre_omaprmkeys, soid.oid.name.c_str(), soid.snap.val);
	break;
      }
      ++ctx->num_write;
      result = 0;
      {
	if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  tracepoint(osd, do_osd_op_pre_omaprmkeys, soid.oid.name.c_str(), soid.snap.val);
	  break;
	}
	bufferlist to_rm_bl;
	try {
	  decode_str_set_to_bl(bp, &to_rm_bl);
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  tracepoint(osd, do_osd_op_pre_omaprmkeys, soid.oid.name.c_str(), soid.snap.val);
	  goto fail;
	}
	tracepoint(osd, do_osd_op_pre_omaprmkeys, soid.oid.name.c_str(), soid.snap.val);
	t->omap_rmkeys(soid, to_rm_bl);
	ctx->clean_regions.mark_omap_dirty();
	ctx->delta_stats.num_wr++;
      }
      obs.oi.clear_omap_digest();
      break;

    case CEPH_OSD_OP_OMAPRMKEYRANGE:
      tracepoint(osd, do_osd_op_pre_omaprmkeyrange, soid.oid.name.c_str(), soid.snap.val);
      if (!pool.info.supports_omap()) {
	result = -EOPNOTSUPP;
	break;
      }
      ++ctx->num_write;
      result = 0;
      {
	if (!obs.exists || oi.is_whiteout()) {
	  result = -ENOENT;
	  break;
	}
	std::string key_begin, key_end;
	try {
	  decode(key_begin, bp);
	  decode(key_end, bp);
	} catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  goto fail;
	}
	t->omap_rmkeyrange(soid, key_begin, key_end);
	ctx->delta_stats.num_wr++;
      }
      obs.oi.clear_omap_digest();
      break;

    case CEPH_OSD_OP_COPY_GET:
      ++ctx->num_read;
      tracepoint(osd, do_osd_op_pre_copy_get, soid.oid.name.c_str(),
		 soid.snap.val);
      if (op_finisher == nullptr) {
	result = do_copy_get(ctx, bp, osd_op, ctx->obc);
      } else {
	result = op_finisher->execute();
      }
      break;

    case CEPH_OSD_OP_COPY_FROM:
    case CEPH_OSD_OP_COPY_FROM2:
      ++ctx->num_write;
      result = 0;
      {
	object_t src_name;
	object_locator_t src_oloc;
	uint32_t truncate_seq = 0;
	uint64_t truncate_size = 0;
	bool have_truncate = false;
	snapid_t src_snapid = (uint64_t)op.copy_from.snapid;
	version_t src_version = op.copy_from.src_version;

	if ((op.op == CEPH_OSD_OP_COPY_FROM2) &&
	    (op.copy_from.flags & ~CEPH_OSD_COPY_FROM_FLAGS)) {
	  dout(20) << "invalid copy-from2 flags 0x"
		  << std::hex << (int)op.copy_from.flags << std::dec << dendl;
	  result = -EINVAL;
	  break;
	}
	try {
	  decode(src_name, bp);
	  decode(src_oloc, bp);
	  // check if client sent us truncate_seq and truncate_size
	  if ((op.op == CEPH_OSD_OP_COPY_FROM2) &&
	      (op.copy_from.flags & CEPH_OSD_COPY_FROM_FLAG_TRUNCATE_SEQ)) {
	    decode(truncate_seq, bp);
	    decode(truncate_size, bp);
	    have_truncate = true;
	  }
	}
	catch (ceph::buffer::error& e) {
	  result = -EINVAL;
	  tracepoint(osd,
		     do_osd_op_pre_copy_from,
		     soid.oid.name.c_str(),
		     soid.snap.val,
		     "???",
		     0,
		     "???",
		     "???",
		     0,
		     src_snapid,
		     src_version);
	  goto fail;
	}
	tracepoint(osd,
		   do_osd_op_pre_copy_from,
		   soid.oid.name.c_str(),
		   soid.snap.val,
		   src_name.name.c_str(),
		   src_oloc.pool,
		   src_oloc.key.c_str(),
		   src_oloc.nspace.c_str(),
		   src_oloc.hash,
		   src_snapid,
		   src_version);
	if (op_finisher == nullptr) {
	  // start
	  pg_t raw_pg;
	  get_osdmap()->object_locator_to_pg(src_name, src_oloc, raw_pg);
	  hobject_t src(src_name, src_oloc.key, src_snapid,
			raw_pg.ps(), raw_pg.pool(),
			src_oloc.nspace);
	  if (src == soid) {
	    dout(20) << " copy from self is invalid" << dendl;
	    result = -EINVAL;
	    break;
	  }
	  CopyFromCallback *cb = new CopyFromCallback(ctx, osd_op);
	  if (have_truncate)
	    cb->set_truncate(truncate_seq, truncate_size);
          ctx->op_finishers[ctx->current_osd_subop_num].reset(
            new CopyFromFinisher(cb));
	  start_copy(cb, ctx->obc, src, src_oloc, src_version,
		     op.copy_from.flags,
		     false,
		     op.copy_from.src_fadvise_flags,
		     op.flags);
	  result = -EINPROGRESS;
	} else {
	  // finish
	  result = op_finisher->execute();
	  ceph_assert(result == 0);

          // COPY_FROM cannot be executed multiple times -- it must restart
          ctx->op_finishers.erase(ctx->current_osd_subop_num);
	}
      }
      break;

    default:
      tracepoint(osd, do_osd_op_pre_unknown, soid.oid.name.c_str(), soid.snap.val, op.op, ceph_osd_op_name(op.op));
      dout(1) << "unrecognized osd op " << op.op
	      << " " << ceph_osd_op_name(op.op)
	      << dendl;
      result = -EOPNOTSUPP;
    }

  fail:
    osd_op.rval = result;
    tracepoint(osd, do_osd_op_post, soid.oid.name.c_str(), soid.snap.val, op.op, ceph_osd_op_name(op.op), op.flags, result);
    if (result < 0 && (op.flags & CEPH_OSD_OP_FLAG_FAILOK) &&
        result != -EAGAIN && result != -EINPROGRESS)
      result = 0;

    if (result < 0)
      break;
  }
  if (result < 0) {
    dout(10) << __func__ << " error: " << cpp_strerror(result) << dendl;
  }
  return result;
}

int PrimaryLogPG::_get_tmap(OpContext *ctx, bufferlist *header, bufferlist *vals)
{
  if (ctx->new_obs.oi.size == 0) {
    dout(20) << "unable to get tmap for zero sized " << ctx->new_obs.oi.soid << dendl;
    return -ENODATA;
  }
  vector<OSDOp> nops(1);
  OSDOp &newop = nops[0];
  newop.op.op = CEPH_OSD_OP_TMAPGET;
  do_osd_ops(ctx, nops);
  try {
    bufferlist::const_iterator i = newop.outdata.begin();
    decode(*header, i);
    (*vals).substr_of(newop.outdata, i.get_off(), i.get_remaining());
  } catch (...) {
    dout(20) << "unsuccessful at decoding tmap for " << ctx->new_obs.oi.soid
	     << dendl;
    return -EINVAL;
  }
  dout(20) << "successful at decoding tmap for " << ctx->new_obs.oi.soid
	   << dendl;
  return 0;
}

int PrimaryLogPG::_verify_no_head_clones(const hobject_t& soid,
					const SnapSet& ss)
{
  // verify that all clones have been evicted
  dout(20) << __func__ << " verifying clones are absent "
	   << ss << dendl;
  for (vector<snapid_t>::const_iterator p = ss.clones.begin();
       p != ss.clones.end();
       ++p) {
    hobject_t clone_oid = soid;
    clone_oid.snap = *p;
    if (is_missing_object(clone_oid))
      return -EBUSY;
    ObjectContextRef clone_obc = get_object_context(clone_oid, false);
    if (clone_obc && clone_obc->obs.exists) {
      dout(10) << __func__ << " cannot evict head before clone "
	       << clone_oid << dendl;
      return -EBUSY;
    }
    if (copy_ops.count(clone_oid)) {
      dout(10) << __func__ << " cannot evict head, pending promote on clone "
	       << clone_oid << dendl;
      return -EBUSY;
    }
  }
  return 0;
}

inline int PrimaryLogPG::_delete_oid(
  OpContext *ctx,
  bool no_whiteout,     // no whiteouts, no matter what.
  bool try_no_whiteout) // try not to whiteout
{
  SnapSet& snapset = ctx->new_snapset;
  ObjectState& obs = ctx->new_obs;
  object_info_t& oi = obs.oi;
  const hobject_t& soid = oi.soid;
  PGTransaction* t = ctx->op_t.get();

  // cache: cache: set whiteout on delete?
  bool whiteout = false;
  if (pool.info.cache_mode != pg_pool_t::CACHEMODE_NONE
      && !no_whiteout
      && !try_no_whiteout) {
    whiteout = true;
  }

  // in luminous or later, we can't delete the head if there are
  // clones. we trust the caller passing no_whiteout has already
  // verified they don't exist.
  if (!snapset.clones.empty() ||
      (!ctx->snapc.snaps.empty() && ctx->snapc.snaps[0] > snapset.seq)) {
    if (no_whiteout) {
      dout(20) << __func__ << " has or will have clones but no_whiteout=1"
	       << dendl;
    } else {
      dout(20) << __func__ << " has or will have clones; will whiteout"
	       << dendl;
      whiteout = true;
    }
  }
  dout(20) << __func__ << " " << soid << " whiteout=" << (int)whiteout
	   << " no_whiteout=" << (int)no_whiteout
	   << " try_no_whiteout=" << (int)try_no_whiteout
	   << dendl;
  if (!obs.exists || (obs.oi.is_whiteout() && whiteout))
    return -ENOENT;

  t->remove(soid);

  if (oi.size > 0) {
    interval_set<uint64_t> ch;
    ch.insert(0, oi.size);
    ctx->modified_ranges.union_of(ch);
    ctx->clean_regions.mark_data_region_dirty(0, oi.size);
  }

  ctx->clean_regions.mark_omap_dirty();
  ctx->delta_stats.num_wr++;
  if (soid.is_snap()) {
    ceph_assert(ctx->obc->ssc->snapset.clone_overlap.count(soid.snap));
    ctx->delta_stats.num_bytes -= ctx->obc->ssc->snapset.get_clone_bytes(soid.snap);
  } else {
    ctx->delta_stats.num_bytes -= oi.size;
  }
  oi.size = 0;
  oi.new_object();

  // disconnect all watchers
  for (map<pair<uint64_t, entity_name_t>, watch_info_t>::iterator p =
	 oi.watchers.begin();
       p != oi.watchers.end();
       ++p) {
    dout(20) << __func__ << " will disconnect watcher " << p->first << dendl;
    ctx->watch_disconnects.push_back(
      watch_disconnect_t(p->first.first, p->first.second, true));
  }
  oi.watchers.clear();

  if (oi.has_manifest()) {
    ctx->delta_stats.num_objects_manifest--;
    dec_all_refcount_manifest(oi, ctx);
  }

  if (whiteout) {
    dout(20) << __func__ << " setting whiteout on " << soid << dendl;
    oi.set_flag(object_info_t::FLAG_WHITEOUT);
    ctx->delta_stats.num_whiteouts++;
    t->create(soid);
    osd->logger->inc(l_osd_tier_whiteout);
    return 0;
  }

  // delete the head
  ctx->delta_stats.num_objects--;
  if (soid.is_snap())
    ctx->delta_stats.num_object_clones--;
  if (oi.is_whiteout()) {
    dout(20) << __func__ << " deleting whiteout on " << soid << dendl;
    ctx->delta_stats.num_whiteouts--;
    oi.clear_flag(object_info_t::FLAG_WHITEOUT);
  }
  if (oi.is_cache_pinned()) {
    ctx->delta_stats.num_objects_pinned--;
  }
  obs.exists = false;
  return 0;
}

int PrimaryLogPG::_rollback_to(OpContext *ctx, ceph_osd_op& op)
{
  SnapSet& snapset = ctx->new_snapset;
  ObjectState& obs = ctx->new_obs;
  object_info_t& oi = obs.oi;
  const hobject_t& soid = oi.soid;
  PGTransaction* t = ctx->op_t.get();
  snapid_t snapid = (uint64_t)op.snap.snapid;
  hobject_t missing_oid;

  dout(10) << "_rollback_to " << soid << " snapid " << snapid << dendl;

  ObjectContextRef rollback_to;

  int ret = find_object_context(
    hobject_t(soid.oid, soid.get_key(), snapid, soid.get_hash(), info.pgid.pool(),
	      soid.get_namespace()),
    &rollback_to, false, false, &missing_oid);
  if (ret == -EAGAIN) {
    /* clone must be missing */
    ceph_assert(is_degraded_or_backfilling_object(missing_oid) || is_degraded_on_async_recovery_target(missing_oid));
    dout(20) << "_rollback_to attempted to roll back to a missing or backfilling clone "
	     << missing_oid << " (requested snapid: ) " << snapid << dendl;
    block_write_on_degraded_snap(missing_oid, ctx->op);
    return ret;
  }
  {
    ObjectContextRef promote_obc;
    cache_result_t tier_mode_result;
    if (obs.exists && obs.oi.has_manifest()) { 
      tier_mode_result = 
	maybe_handle_manifest_detail(
	  ctx->op,
	  true,
	  rollback_to);
    } else {
      tier_mode_result = 
	maybe_handle_cache_detail(
	  ctx->op,
	  true,
	  rollback_to,
	  ret,
	  missing_oid,
	  true,
	  false,
	  &promote_obc);
    }
    switch (tier_mode_result) {
    case cache_result_t::NOOP:
      break;
    case cache_result_t::BLOCKED_PROMOTE:
      ceph_assert(promote_obc);
      block_write_on_snap_rollback(soid, promote_obc, ctx->op);
      return -EAGAIN;
    case cache_result_t::BLOCKED_FULL:
      block_write_on_full_cache(soid, ctx->op);
      return -EAGAIN;
    case cache_result_t::REPLIED_WITH_EAGAIN:
      ceph_abort_msg("this can't happen, no rollback on replica");
    default:
      ceph_abort_msg("must promote was set, other values are not valid");
      return -EAGAIN;
    }
  }

  if (ret == -ENOENT || (rollback_to && rollback_to->obs.oi.is_whiteout())) {
    // there's no snapshot here, or there's no object.
    // if there's no snapshot, we delete the object; otherwise, do nothing.
    dout(20) << "_rollback_to deleting head on " << soid.oid
	     << " because got ENOENT|whiteout on find_object_context" << dendl;
    if (ctx->obc->obs.oi.watchers.size()) {
      // Cannot delete an object with watchers
      ret = -EBUSY;
    } else {
      _delete_oid(ctx, false, false);
      ret = 0;
    }
  } else if (ret) {
    // ummm....huh? It *can't* return anything else at time of writing.
    ceph_abort_msg("unexpected error code in _rollback_to");
  } else { //we got our context, let's use it to do the rollback!
    hobject_t& rollback_to_sobject = rollback_to->obs.oi.soid;
    if (is_degraded_or_backfilling_object(rollback_to_sobject) ||
	is_degraded_on_async_recovery_target(rollback_to_sobject)) {
      dout(20) << "_rollback_to attempted to roll back to a degraded object "
	       << rollback_to_sobject << " (requested snapid: ) " << snapid << dendl;
      block_write_on_degraded_snap(rollback_to_sobject, ctx->op);
      ret = -EAGAIN;
    } else if (rollback_to->obs.oi.soid.snap == CEPH_NOSNAP) {
      // rolling back to the head; we just need to clone it.
      ctx->modify = true;
    } else {
      /* 1) Delete current head
       * 2) Clone correct snapshot into head
       * 3) Calculate clone_overlaps by following overlaps
       *    forward from rollback snapshot */
      dout(10) << "_rollback_to deleting " << soid.oid
	       << " and rolling back to old snap" << dendl;

      if (obs.exists) {
	t->remove(soid);
      }
      t->clone(soid, rollback_to_sobject);
      t->add_obc(rollback_to);

      map<snapid_t, interval_set<uint64_t> >::iterator iter =
	snapset.clone_overlap.lower_bound(snapid);
      ceph_assert(iter != snapset.clone_overlap.end());
      interval_set<uint64_t> overlaps = iter->second;
      for ( ;
	    iter != snapset.clone_overlap.end();
	    ++iter)
	overlaps.intersection_of(iter->second);

      if (obs.oi.size > 0) {
	interval_set<uint64_t> modified;
	modified.insert(0, obs.oi.size);
	overlaps.intersection_of(modified);
	modified.subtract(overlaps);
	ctx->modified_ranges.union_of(modified);
      }

      // Adjust the cached objectcontext
      maybe_create_new_object(ctx, true);
      ctx->delta_stats.num_bytes -= obs.oi.size;
      ctx->delta_stats.num_bytes += rollback_to->obs.oi.size;
      ctx->clean_regions.mark_data_region_dirty(0, std::max(obs.oi.size, rollback_to->obs.oi.size));
      ctx->clean_regions.mark_omap_dirty();
      obs.oi.size = rollback_to->obs.oi.size;
      if (rollback_to->obs.oi.is_data_digest())
	obs.oi.set_data_digest(rollback_to->obs.oi.data_digest);
      else
	obs.oi.clear_data_digest();
      if (rollback_to->obs.oi.is_omap_digest())
	obs.oi.set_omap_digest(rollback_to->obs.oi.omap_digest);
      else
	obs.oi.clear_omap_digest();

      if (rollback_to->obs.oi.is_omap()) {
	dout(10) << __func__ << " setting omap flag on " << obs.oi.soid << dendl;
	obs.oi.set_flag(object_info_t::FLAG_OMAP);
      } else {
	dout(10) << __func__ << " clearing omap flag on " << obs.oi.soid << dendl;
	obs.oi.clear_flag(object_info_t::FLAG_OMAP);
      }
    }
  }
  return ret;
}

void PrimaryLogPG::_make_clone(
  OpContext *ctx,
  PGTransaction* t,
  ObjectContextRef obc,
  const hobject_t& head, const hobject_t& coid,
  object_info_t *poi)
{
  bufferlist bv;
  encode(*poi, bv, get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));

  t->clone(coid, head);
  setattr_maybe_cache(obc, t, OI_ATTR, bv);
  rmattr_maybe_cache(obc, t, SS_ATTR);
}

void PrimaryLogPG::make_writeable(OpContext *ctx)
{
  const hobject_t& soid = ctx->obs->oi.soid;
  SnapContext& snapc = ctx->snapc;

  // clone?
  ceph_assert(soid.snap == CEPH_NOSNAP);
  dout(20) << "make_writeable " << soid << " snapset=" << ctx->new_snapset
	   << "  snapc=" << snapc << dendl;
  
  bool was_dirty = ctx->obc->obs.oi.is_dirty();
  if (ctx->new_obs.exists) {
    // we will mark the object dirty
    if (ctx->undirty && was_dirty) {
      dout(20) << " clearing DIRTY flag" << dendl;
      ceph_assert(ctx->new_obs.oi.is_dirty());
      ctx->new_obs.oi.clear_flag(object_info_t::FLAG_DIRTY);
      --ctx->delta_stats.num_objects_dirty;
      osd->logger->inc(l_osd_tier_clean);
    } else if (!was_dirty && !ctx->undirty) {
      dout(20) << " setting DIRTY flag" << dendl;
      ctx->new_obs.oi.set_flag(object_info_t::FLAG_DIRTY);
      ++ctx->delta_stats.num_objects_dirty;
      osd->logger->inc(l_osd_tier_dirty);
    }
  } else {
    if (was_dirty) {
      dout(20) << " deletion, decrementing num_dirty and clearing flag" << dendl;
      ctx->new_obs.oi.clear_flag(object_info_t::FLAG_DIRTY);
      --ctx->delta_stats.num_objects_dirty;
    }
  }

  if ((ctx->new_obs.exists &&
       ctx->new_obs.oi.is_omap()) &&
      (!ctx->obc->obs.exists ||
       !ctx->obc->obs.oi.is_omap())) {
    ++ctx->delta_stats.num_objects_omap;
  }
  if ((!ctx->new_obs.exists ||
       !ctx->new_obs.oi.is_omap()) &&
      (ctx->obc->obs.exists &&
       ctx->obc->obs.oi.is_omap())) {
    --ctx->delta_stats.num_objects_omap;
  }

  if (ctx->new_snapset.seq > snapc.seq) {
    dout(10) << " op snapset is old" << dendl;
  }

  if ((ctx->obs->exists && !ctx->obs->oi.is_whiteout()) && // head exist(ed)
      snapc.snaps.size() &&                 // there are snaps
      !ctx->cache_evict &&
      snapc.snaps[0] > ctx->new_snapset.seq) {  // existing object is old
    // clone
    hobject_t coid = soid;
    coid.snap = snapc.seq;
    
    unsigned l;
    for (l = 1;
	 l < snapc.snaps.size() && snapc.snaps[l] > ctx->new_snapset.seq;
	 l++) ;

    vector<snapid_t> snaps(l);
    for (unsigned i=0; i<l; i++)
      snaps[i] = snapc.snaps[i];
    
    // prepare clone
    object_info_t static_snap_oi(coid);
    object_info_t *snap_oi;
    if (is_primary()) {
      ctx->clone_obc = object_contexts.lookup_or_create(static_snap_oi.soid);
      ctx->clone_obc->destructor_callback =
	new C_PG_ObjectContext(this, ctx->clone_obc.get());
      ctx->clone_obc->obs.oi = static_snap_oi;
      ctx->clone_obc->obs.exists = true;
      ctx->clone_obc->ssc = ctx->obc->ssc;
      ctx->clone_obc->ssc->ref++;
      if (pool.info.is_erasure())
	ctx->clone_obc->attr_cache = ctx->obc->attr_cache;
      snap_oi = &ctx->clone_obc->obs.oi;
      if (ctx->obc->obs.oi.has_manifest()) {
	if ((ctx->obc->obs.oi.flags & object_info_t::FLAG_REDIRECT_HAS_REFERENCE) && 
	    ctx->obc->obs.oi.manifest.is_redirect()) {
	  snap_oi->set_flag(object_info_t::FLAG_MANIFEST);
	  snap_oi->manifest.type = object_manifest_t::TYPE_REDIRECT;
	  snap_oi->manifest.redirect_target = ctx->obc->obs.oi.manifest.redirect_target;
	} else if (ctx->obc->obs.oi.manifest.is_chunked()) {
	  snap_oi->set_flag(object_info_t::FLAG_MANIFEST);
	  snap_oi->manifest.type = object_manifest_t::TYPE_CHUNKED;
	  snap_oi->manifest.chunk_map = ctx->obc->obs.oi.manifest.chunk_map;
	} else {
	  ceph_abort_msg("unrecognized manifest type");
	}
      }
      bool got = ctx->lock_manager.get_write_greedy(
	coid,
	ctx->clone_obc,
	ctx->op);
      ceph_assert(got);
      dout(20) << " got greedy write on clone_obc " << *ctx->clone_obc << dendl;
    } else {
      snap_oi = &static_snap_oi;
    }
    snap_oi->version = ctx->at_version;
    snap_oi->prior_version = ctx->obs->oi.version;
    snap_oi->copy_user_bits(ctx->obs->oi);

    _make_clone(ctx, ctx->op_t.get(), ctx->clone_obc, soid, coid, snap_oi);
    
    ctx->delta_stats.num_objects++;
    if (snap_oi->is_dirty()) {
      ctx->delta_stats.num_objects_dirty++;
      osd->logger->inc(l_osd_tier_dirty);
    }
    if (snap_oi->is_omap())
      ctx->delta_stats.num_objects_omap++;
    if (snap_oi->is_cache_pinned())
      ctx->delta_stats.num_objects_pinned++;
    if (snap_oi->has_manifest())
      ctx->delta_stats.num_objects_manifest++;
    ctx->delta_stats.num_object_clones++;
    ctx->new_snapset.clones.push_back(coid.snap);
    ctx->new_snapset.clone_size[coid.snap] = ctx->obs->oi.size;
    ctx->new_snapset.clone_snaps[coid.snap] = snaps;

    // clone_overlap should contain an entry for each clone 
    // (an empty interval_set if there is no overlap)
    ctx->new_snapset.clone_overlap[coid.snap];
    if (ctx->obs->oi.size)
      ctx->new_snapset.clone_overlap[coid.snap].insert(0, ctx->obs->oi.size);
    
    // log clone
    dout(10) << " cloning v " << ctx->obs->oi.version
	     << " to " << coid << " v " << ctx->at_version
	     << " snaps=" << snaps
	     << " snapset=" << ctx->new_snapset << dendl;
    ctx->log.push_back(pg_log_entry_t(
			 pg_log_entry_t::CLONE, coid, ctx->at_version,
			 ctx->obs->oi.version,
			 ctx->obs->oi.user_version,
			 osd_reqid_t(), ctx->new_obs.oi.mtime, 0));
    encode(snaps, ctx->log.back().snaps);

    ctx->at_version.version++;
  }

  // update most recent clone_overlap and usage stats
  if (ctx->new_snapset.clones.size() > 0) {
    // the clone_overlap is difference of range between head and clones.
    // we need to check whether the most recent clone exists, if it's
    // been evicted, it's not included in the stats, but the clone_overlap
    // is still exist in the snapset, so we should update the
    // clone_overlap to make it sense.
    hobject_t last_clone_oid = soid;
    last_clone_oid.snap = ctx->new_snapset.clone_overlap.rbegin()->first;
    interval_set<uint64_t> &newest_overlap =
      ctx->new_snapset.clone_overlap.rbegin()->second;
    ctx->modified_ranges.intersection_of(newest_overlap);
    if (is_present_clone(last_clone_oid)) {
      // modified_ranges is still in use by the clone
      ctx->delta_stats.num_bytes += ctx->modified_ranges.size();
    }
    newest_overlap.subtract(ctx->modified_ranges);
  }
  
  if (snapc.seq > ctx->new_snapset.seq) {
    // update snapset with latest snap context
    ctx->new_snapset.seq = snapc.seq;
    if (get_osdmap()->require_osd_release < ceph_release_t::octopus) {
      ctx->new_snapset.snaps = snapc.snaps;
    } else {
      ctx->new_snapset.snaps.clear();
    }
  }
  dout(20) << "make_writeable " << soid
	   << " done, snapset=" << ctx->new_snapset << dendl;
}


void PrimaryLogPG::write_update_size_and_usage(object_stat_sum_t& delta_stats, object_info_t& oi,
					       interval_set<uint64_t>& modified, uint64_t offset,
					       uint64_t length, bool write_full)
{
  interval_set<uint64_t> ch;
  if (write_full) {
    if (oi.size)
      ch.insert(0, oi.size);
  } else if (length)
    ch.insert(offset, length);
  modified.union_of(ch);
  if (write_full ||
      (offset + length > oi.size && length)) {
    uint64_t new_size = offset + length;
    delta_stats.num_bytes -= oi.size;
    delta_stats.num_bytes += new_size;
    oi.size = new_size;
  }
 
  if (oi.has_manifest() && oi.manifest.is_chunked()) {
    for (auto &p : oi.manifest.chunk_map) {
      if ((p.first <= offset && p.first + p.second.length > offset) ||
	  (p.first > offset && p.first < offset + length)) {
	p.second.clear_flag(chunk_info_t::FLAG_MISSING);
	p.second.set_flag(chunk_info_t::FLAG_DIRTY);
      }
    }
  }
  delta_stats.num_wr++;
  delta_stats.num_wr_kb += shift_round_up(length, 10);
}

void PrimaryLogPG::truncate_update_size_and_usage(
  object_stat_sum_t& delta_stats,
  object_info_t& oi,
  uint64_t truncate_size)
{
  if (oi.size != truncate_size) {
    delta_stats.num_bytes -= oi.size;
    delta_stats.num_bytes += truncate_size;
    oi.size = truncate_size;
  }
}

void PrimaryLogPG::complete_disconnect_watches(
  ObjectContextRef obc,
  const list<watch_disconnect_t> &to_disconnect)
{
  for (list<watch_disconnect_t>::const_iterator i =
	 to_disconnect.begin();
       i != to_disconnect.end();
       ++i) {
    pair<uint64_t, entity_name_t> watcher(i->cookie, i->name);
    auto watchers_entry = obc->watchers.find(watcher);
    if (watchers_entry != obc->watchers.end()) {
      WatchRef watch = watchers_entry->second;
      dout(10) << "do_osd_op_effects disconnect watcher " << watcher << dendl;
      obc->watchers.erase(watcher);
      watch->remove(i->send_disconnect);
    } else {
      dout(10) << "do_osd_op_effects disconnect failed to find watcher "
	       << watcher << dendl;
    }
  }
}

void PrimaryLogPG::do_osd_op_effects(OpContext *ctx, const ConnectionRef& conn)
{
  entity_name_t entity = ctx->reqid.name;
  dout(15) << "do_osd_op_effects " << entity << " con " << conn.get() << dendl;

  // disconnects first
  complete_disconnect_watches(ctx->obc, ctx->watch_disconnects);

  ceph_assert(conn);

  auto session = conn->get_priv();
  if (!session)
    return;

  for (list<pair<watch_info_t,bool> >::iterator i = ctx->watch_connects.begin();
       i != ctx->watch_connects.end();
       ++i) {
    pair<uint64_t, entity_name_t> watcher(i->first.cookie, entity);
    dout(15) << "do_osd_op_effects applying watch connect on session "
	     << session.get() << " watcher " << watcher << dendl;
    WatchRef watch;
    if (ctx->obc->watchers.count(watcher)) {
      dout(15) << "do_osd_op_effects found existing watch watcher " << watcher
	       << dendl;
      watch = ctx->obc->watchers[watcher];
    } else {
      dout(15) << "do_osd_op_effects new watcher " << watcher
	       << dendl;
      watch = Watch::makeWatchRef(
	this, osd, ctx->obc, i->first.timeout_seconds,
	i->first.cookie, entity, conn->get_peer_addr());
      ctx->obc->watchers.insert(
	make_pair(
	  watcher,
	  watch));
    }
    watch->connect(conn, i->second);
  }

  for (list<notify_info_t>::iterator p = ctx->notifies.begin();
       p != ctx->notifies.end();
       ++p) {
    dout(10) << "do_osd_op_effects, notify " << *p << dendl;
    ConnectionRef conn(ctx->op->get_req()->get_connection());
    NotifyRef notif(
      Notify::makeNotifyRef(
	conn,
	ctx->reqid.name.num(),
	p->bl,
	p->timeout,
	p->cookie,
	p->notify_id,
	ctx->obc->obs.oi.user_version,
	osd));
    for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator i =
	   ctx->obc->watchers.begin();
	 i != ctx->obc->watchers.end();
	 ++i) {
      dout(10) << "starting notify on watch " << i->first << dendl;
      i->second->start_notify(notif);
    }
    notif->init();
  }

  for (list<OpContext::NotifyAck>::iterator p = ctx->notify_acks.begin();
       p != ctx->notify_acks.end();
       ++p) {
    if (p->watch_cookie)
      dout(10) << "notify_ack " << make_pair(*(p->watch_cookie), p->notify_id) << dendl;
    else
      dout(10) << "notify_ack " << make_pair("NULL", p->notify_id) << dendl;
    for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator i =
	   ctx->obc->watchers.begin();
	 i != ctx->obc->watchers.end();
	 ++i) {
      if (i->first.second != entity) continue;
      if (p->watch_cookie &&
	  *(p->watch_cookie) != i->first.first) continue;
      dout(10) << "acking notify on watch " << i->first << dendl;
      i->second->notify_ack(p->notify_id, p->reply_bl);
    }
  }
}

hobject_t PrimaryLogPG::generate_temp_object(const hobject_t& target)
{
  ostringstream ss;
  ss << "temp_" << info.pgid << "_" << get_role()
     << "_" << osd->monc->get_global_id() << "_" << (++temp_seq);
  hobject_t hoid = target.make_temp_hobject(ss.str());
  dout(20) << __func__ << " " << hoid << dendl;
  return hoid;
}

hobject_t PrimaryLogPG::get_temp_recovery_object(
  const hobject_t& target,
  eversion_t version)
{
  ostringstream ss;
  ss << "temp_recovering_" << info.pgid  // (note this includes the shardid)
     << "_" << version
     << "_" << info.history.same_interval_since
     << "_" << target.snap;
  // pgid + version + interval + snapid is unique, and short
  hobject_t hoid = target.make_temp_hobject(ss.str());
  dout(20) << __func__ << " " << hoid << dendl;
  return hoid;
}

int PrimaryLogPG::prepare_transaction(OpContext *ctx)
{
  ceph_assert(!ctx->ops->empty());

  // valid snap context?
  if (!ctx->snapc.is_valid()) {
    dout(10) << " invalid snapc " << ctx->snapc << dendl;
    return -EINVAL;
  }

  // prepare the actual mutation
  int result = do_osd_ops(ctx, *ctx->ops);
  if (result < 0) {
    if (ctx->op->may_write() &&
	get_osdmap()->require_osd_release >= ceph_release_t::kraken) {
      // need to save the error code in the pg log, to detect dup ops,
      // but do nothing else
      ctx->update_log_only = true;
    }
    return result;
  }

  // read-op?  write-op noop? done?
  if (ctx->op_t->empty() && !ctx->modify) {
    if (ctx->pending_async_reads.empty())
      unstable_stats.add(ctx->delta_stats);
    if (ctx->op->may_write() &&
	get_osdmap()->require_osd_release >= ceph_release_t::kraken) {
      ctx->update_log_only = true;
    }
    return result;
  }

  // check for full
  if ((ctx->delta_stats.num_bytes > 0 ||
       ctx->delta_stats.num_objects > 0) &&  // FIXME: keys?
      pool.info.has_flag(pg_pool_t::FLAG_FULL)) {
    auto m = ctx->op->get_req<MOSDOp>();
    if (ctx->reqid.name.is_mds() ||   // FIXME: ignore MDS for now
	m->has_flag(CEPH_OSD_FLAG_FULL_FORCE)) {
      dout(20) << __func__ << " full, but proceeding due to FULL_FORCE or MDS"
	       << dendl;
    } else if (m->has_flag(CEPH_OSD_FLAG_FULL_TRY)) {
      // they tried, they failed.
      dout(20) << __func__ << " full, replying to FULL_TRY op" << dendl;
      return pool.info.has_flag(pg_pool_t::FLAG_FULL_QUOTA) ? -EDQUOT : -ENOSPC;
    } else {
      // drop request
      dout(20) << __func__ << " full, dropping request (bad client)" << dendl;
      return -EAGAIN;
    }
  }

  const hobject_t& soid = ctx->obs->oi.soid;
  // clone, if necessary
  if (soid.snap == CEPH_NOSNAP)
    make_writeable(ctx);

  finish_ctx(ctx,
	     ctx->new_obs.exists ? pg_log_entry_t::MODIFY :
	     pg_log_entry_t::DELETE,
	     result);

  return result;
}

void PrimaryLogPG::finish_ctx(OpContext *ctx, int log_op_type, int result)
{
  const hobject_t& soid = ctx->obs->oi.soid;
  dout(20) << __func__ << " " << soid << " " << ctx
	   << " op " << pg_log_entry_t::get_op_name(log_op_type)
	   << dendl;
  utime_t now = ceph_clock_now();

  // finish and log the op.
  if (ctx->user_modify) {
    // update the user_version for any modify ops, except for the watch op
    ctx->user_at_version = std::max(info.last_user_version, ctx->new_obs.oi.user_version) + 1;
    /* In order for new clients and old clients to interoperate properly
     * when exchanging versions, we need to lower bound the user_version
     * (which our new clients pay proper attention to)
     * by the at_version (which is all the old clients can ever see). */
    if (ctx->at_version.version > ctx->user_at_version)
      ctx->user_at_version = ctx->at_version.version;
    ctx->new_obs.oi.user_version = ctx->user_at_version;
  }
  ctx->bytes_written = ctx->op_t->get_bytes_written();
 
  if (ctx->new_obs.exists) {
    ctx->new_obs.oi.version = ctx->at_version;
    ctx->new_obs.oi.prior_version = ctx->obs->oi.version;
    ctx->new_obs.oi.last_reqid = ctx->reqid;
    if (ctx->mtime != utime_t()) {
      ctx->new_obs.oi.mtime = ctx->mtime;
      dout(10) << " set mtime to " << ctx->new_obs.oi.mtime << dendl;
      ctx->new_obs.oi.local_mtime = now;
    } else {
      dout(10) << " mtime unchanged at " << ctx->new_obs.oi.mtime << dendl;
    }

    // object_info_t
    map <string, bufferlist> attrs;
    bufferlist bv(sizeof(ctx->new_obs.oi));
    encode(ctx->new_obs.oi, bv,
	     get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
    attrs[OI_ATTR].claim(bv);

    // snapset
    if (soid.snap == CEPH_NOSNAP) {
      dout(10) << " final snapset " << ctx->new_snapset
	       << " in " << soid << dendl;
      bufferlist bss;
      encode(ctx->new_snapset, bss);
      attrs[SS_ATTR].claim(bss);
    } else {
      dout(10) << " no snapset (this is a clone)" << dendl;
    }
    ctx->op_t->setattrs(soid, attrs);
  } else {
    // reset cached oi
    ctx->new_obs.oi = object_info_t(ctx->obc->obs.oi.soid);
  }

  // append to log
  ctx->log.push_back(
    pg_log_entry_t(log_op_type, soid, ctx->at_version,
		   ctx->obs->oi.version,
		   ctx->user_at_version, ctx->reqid,
		   ctx->mtime,
		   (ctx->op && ctx->op->allows_returnvec()) ? result : 0));
  if (ctx->op && ctx->op->allows_returnvec()) {
    // also the per-op values
    ctx->log.back().set_op_returns(*ctx->ops);
    dout(20) << __func__ << " op_returns " << ctx->log.back().op_returns
	     << dendl;
  }

  ctx->log.back().clean_regions = ctx->clean_regions;
  dout(20) << __func__ << " object " << soid <<  " marks clean_regions " << ctx->log.back().clean_regions << dendl;

  if (soid.snap < CEPH_NOSNAP) {
    switch (log_op_type) {
    case pg_log_entry_t::MODIFY:
    case pg_log_entry_t::PROMOTE:
    case pg_log_entry_t::CLEAN:
      dout(20) << __func__ << " encoding snaps from " << ctx->new_snapset
	       << dendl;
      encode(ctx->new_snapset.clone_snaps[soid.snap], ctx->log.back().snaps);
      break;
    default:
      break;
    }
  }

  if (!ctx->extra_reqids.empty()) {
    dout(20) << __func__ << "  extra_reqids " << ctx->extra_reqids << " "
             << ctx->extra_reqid_return_codes << dendl;
    ctx->log.back().extra_reqids.swap(ctx->extra_reqids);
    ctx->log.back().extra_reqid_return_codes.swap(ctx->extra_reqid_return_codes);
  }

  // apply new object state.
  ctx->obc->obs = ctx->new_obs;

  if (soid.is_head() && !ctx->obc->obs.exists) {
    ctx->obc->ssc->exists = false;
    ctx->obc->ssc->snapset = SnapSet();
  } else {
    ctx->obc->ssc->exists = true;
    ctx->obc->ssc->snapset = ctx->new_snapset;
  }
}

void PrimaryLogPG::apply_stats(
  const hobject_t &soid,
  const object_stat_sum_t &delta_stats) {

  recovery_state.apply_op_stats(soid, delta_stats);
  for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
       i != get_backfill_targets().end();
       ++i) {
    pg_shard_t bt = *i;
    const pg_info_t& pinfo = recovery_state.get_peer_info(bt);
    if (soid > pinfo.last_backfill && soid <= last_backfill_started) {
      pending_backfill_updates[soid].stats.add(delta_stats);
    }
  }

  if (is_primary() && scrubber.active) {
    if (soid < scrubber.start) {
      dout(20) << __func__ << " " << soid << " < [" << scrubber.start
	       << "," << scrubber.end << ")" << dendl;
      scrub_cstat.add(delta_stats);
    } else {
      dout(20) << __func__ << " " << soid << " >= [" << scrubber.start
	       << "," << scrubber.end << ")" << dendl;
    }
  }
}

void PrimaryLogPG::complete_read_ctx(int result, OpContext *ctx)
{
  auto m = ctx->op->get_req<MOSDOp>();
  ceph_assert(ctx->async_reads_complete());

  for (vector<OSDOp>::iterator p = ctx->ops->begin();
    p != ctx->ops->end() && result >= 0; ++p) {
    if (p->rval < 0 && !(p->op.flags & CEPH_OSD_OP_FLAG_FAILOK)) {
      result = p->rval;
      break;
    }
    ctx->bytes_read += p->outdata.length();
  }
  ctx->reply->get_header().data_off = (ctx->data_off ? *ctx->data_off : 0);

  MOSDOpReply *reply = ctx->reply;
  ctx->reply = nullptr;

  if (result >= 0) {
    if (!ctx->ignore_log_op_stats) {
      log_op_stats(*ctx->op, ctx->bytes_written, ctx->bytes_read);

      publish_stats_to_osd();
    }

    // on read, return the current object version
    if (ctx->obs) {
      reply->set_reply_versions(eversion_t(), ctx->obs->oi.user_version);
    } else {
      reply->set_reply_versions(eversion_t(), ctx->user_at_version);
    }
  } else if (result == -ENOENT) {
    // on ENOENT, set a floor for what the next user version will be.
    reply->set_enoent_reply_versions(info.last_update, info.last_user_version);
  }

  reply->set_result(result);
  reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
  osd->send_message_osd_client(reply, m->get_connection());
  close_op_ctx(ctx);
}

// ========================================================================
// copyfrom

struct C_Copyfrom : public Context {
  PrimaryLogPGRef pg;
  hobject_t oid;
  epoch_t last_peering_reset;
  ceph_tid_t tid;
  PrimaryLogPG::CopyOpRef cop;	// used for keeping the cop alive
  C_Copyfrom(PrimaryLogPG *p, hobject_t o, epoch_t lpr,
	     const PrimaryLogPG::CopyOpRef& c)
    : pg(p), oid(o), last_peering_reset(lpr),
      tid(0), cop(c)
  {}
  void finish(int r) override {
    if (r == -ECANCELED)
      return;
    std::scoped_lock l{*pg};
    if (last_peering_reset == pg->get_last_peering_reset()) {
      pg->process_copy_chunk(oid, tid, r);
      cop.reset();
    }
  }
};

struct C_CopyFrom_AsyncReadCb : public Context {
  OSDOp *osd_op;
  object_copy_data_t reply_obj;
  uint64_t features;
  size_t len;
  C_CopyFrom_AsyncReadCb(OSDOp *osd_op, uint64_t features) :
    osd_op(osd_op), features(features), len(0) {}
  void finish(int r) override {
    osd_op->rval = r;
    if (r < 0) {
      return;
    }

    ceph_assert(len > 0);
    ceph_assert(len <= reply_obj.data.length());
    bufferlist bl;
    bl.substr_of(reply_obj.data, 0, len);
    reply_obj.data.swap(bl);
    encode(reply_obj, osd_op->outdata, features);
  }
};

struct C_CopyChunk : public Context {
  PrimaryLogPGRef pg;
  hobject_t oid;
  epoch_t last_peering_reset;
  ceph_tid_t tid;
  PrimaryLogPG::CopyOpRef cop;	// used for keeping the cop alive
  uint64_t offset = 0;
  C_CopyChunk(PrimaryLogPG *p, hobject_t o, epoch_t lpr,
	     const PrimaryLogPG::CopyOpRef& c)
    : pg(p), oid(o), last_peering_reset(lpr),
      tid(0), cop(c) 
  {}
  void finish(int r) override {
    if (r == -ECANCELED)
      return;
    std::scoped_lock l{*pg};
    if (last_peering_reset == pg->get_last_peering_reset()) {
      pg->process_copy_chunk_manifest(oid, tid, r, offset);
      cop.reset();
    }
  }
};

int PrimaryLogPG::do_copy_get(OpContext *ctx, bufferlist::const_iterator& bp,
			      OSDOp& osd_op, ObjectContextRef &obc)
{
  object_info_t& oi = obc->obs.oi;
  hobject_t& soid = oi.soid;
  int result = 0;
  object_copy_cursor_t cursor;
  uint64_t out_max;
  try {
    decode(cursor, bp);
    decode(out_max, bp);
  }
  catch (ceph::buffer::error& e) {
    result = -EINVAL;
    return result;
  }

  const MOSDOp *op = reinterpret_cast<const MOSDOp*>(ctx->op->get_req());
  uint64_t features = op->get_features();

  bool async_read_started = false;
  object_copy_data_t _reply_obj;
  C_CopyFrom_AsyncReadCb *cb = nullptr;
  if (pool.info.is_erasure()) {
    cb = new C_CopyFrom_AsyncReadCb(&osd_op, features);
  }
  object_copy_data_t &reply_obj = cb ? cb->reply_obj : _reply_obj;
  // size, mtime
  reply_obj.size = oi.size;
  reply_obj.mtime = oi.mtime;
  ceph_assert(obc->ssc);
  if (soid.snap < CEPH_NOSNAP) {
    auto p = obc->ssc->snapset.clone_snaps.find(soid.snap);
    ceph_assert(p != obc->ssc->snapset.clone_snaps.end()); // warn?
    reply_obj.snaps = p->second;
  } else {
    reply_obj.snap_seq = obc->ssc->snapset.seq;
  }
  if (oi.is_data_digest()) {
    reply_obj.flags |= object_copy_data_t::FLAG_DATA_DIGEST;
    reply_obj.data_digest = oi.data_digest;
  }
  if (oi.is_omap_digest()) {
    reply_obj.flags |= object_copy_data_t::FLAG_OMAP_DIGEST;
    reply_obj.omap_digest = oi.omap_digest;
  }
  reply_obj.truncate_seq = oi.truncate_seq;
  reply_obj.truncate_size = oi.truncate_size;

  // attrs
  map<string,bufferlist>& out_attrs = reply_obj.attrs;
  if (!cursor.attr_complete) {
    result = getattrs_maybe_cache(
      ctx->obc,
      &out_attrs);
    if (result < 0) {
      if (cb) {
        delete cb;
      }
      return result;
    }
    cursor.attr_complete = true;
    dout(20) << " got attrs" << dendl;
  }

  int64_t left = out_max - osd_op.outdata.length();

  // data
  bufferlist& bl = reply_obj.data;
  if (left > 0 && !cursor.data_complete) {
    if (cursor.data_offset < oi.size) {
      uint64_t max_read = std::min(oi.size - cursor.data_offset, (uint64_t)left);
      if (cb) {
	async_read_started = true;
	ctx->pending_async_reads.push_back(
	  make_pair(
	    boost::make_tuple(cursor.data_offset, max_read, osd_op.op.flags),
	    make_pair(&bl, cb)));
	cb->len = max_read;

        ctx->op_finishers[ctx->current_osd_subop_num].reset(
          new ReadFinisher(osd_op));
	result = -EINPROGRESS;

	dout(10) << __func__ << ": async_read noted for " << soid << dendl;
      } else {
	result = pgbackend->objects_read_sync(
	  oi.soid, cursor.data_offset, max_read, osd_op.op.flags, &bl);
	if (result < 0)
	  return result;
      }
      left -= max_read;
      cursor.data_offset += max_read;
    }
    if (cursor.data_offset == oi.size) {
      cursor.data_complete = true;
      dout(20) << " got data" << dendl;
    }
    ceph_assert(cursor.data_offset <= oi.size);
  }

  // omap
  uint32_t omap_keys = 0;
  if (!pool.info.supports_omap() || !oi.is_omap()) {
    cursor.omap_complete = true;
  } else {
    if (left > 0 && !cursor.omap_complete) {
      ceph_assert(cursor.data_complete);
      if (cursor.omap_offset.empty()) {
	osd->store->omap_get_header(ch, ghobject_t(oi.soid),
				    &reply_obj.omap_header);
      }
      bufferlist omap_data;
      ObjectMap::ObjectMapIterator iter =
	osd->store->get_omap_iterator(ch, ghobject_t(oi.soid));
      ceph_assert(iter);
      iter->upper_bound(cursor.omap_offset);
      for (; iter->valid(); iter->next()) {
	++omap_keys;
	encode(iter->key(), omap_data);
	encode(iter->value(), omap_data);
	left -= iter->key().length() + 4 + iter->value().length() + 4;
	if (left <= 0)
	  break;
      }
      if (omap_keys) {
	encode(omap_keys, reply_obj.omap_data);
	reply_obj.omap_data.claim_append(omap_data);
      }
      if (iter->valid()) {
	cursor.omap_offset = iter->key();
      } else {
	cursor.omap_complete = true;
	dout(20) << " got omap" << dendl;
      }
    }
  }

  if (cursor.is_complete()) {
    // include reqids only in the final step.  this is a bit fragile
    // but it works...
    recovery_state.get_pg_log().get_log().get_object_reqids(ctx->obc->obs.oi.soid, 10,
                                       &reply_obj.reqids,
                                       &reply_obj.reqid_return_codes);
    dout(20) << " got reqids" << dendl;
  }

  dout(20) << " cursor.is_complete=" << cursor.is_complete()
	   << " " << out_attrs.size() << " attrs"
	   << " " << bl.length() << " bytes"
	   << " " << reply_obj.omap_header.length() << " omap header bytes"
	   << " " << reply_obj.omap_data.length() << " omap data bytes in "
	   << omap_keys << " keys"
	   << " " << reply_obj.reqids.size() << " reqids"
	   << dendl;
  reply_obj.cursor = cursor;
  if (!async_read_started) {
    encode(reply_obj, osd_op.outdata, features);
  }
  if (cb && !async_read_started) {
    delete cb;
  }

  if (result > 0) {
    result = 0;
  }
  return result;
}

void PrimaryLogPG::fill_in_copy_get_noent(OpRequestRef& op, hobject_t oid,
                                          OSDOp& osd_op)
{
  const MOSDOp *m = static_cast<const MOSDOp*>(op->get_req());
  uint64_t features = m->get_features();
  object_copy_data_t reply_obj;

  recovery_state.get_pg_log().get_log().get_object_reqids(oid, 10, &reply_obj.reqids,
                                     &reply_obj.reqid_return_codes);
  dout(20) << __func__ << " got reqids " << reply_obj.reqids << dendl;
  encode(reply_obj, osd_op.outdata, features);
  osd_op.rval = -ENOENT;
  MOSDOpReply *reply = new MOSDOpReply(m, 0, get_osdmap_epoch(), 0, false);
  reply->set_result(-ENOENT);
  reply->add_flags(CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK);
  osd->send_message_osd_client(reply, m->get_connection());
}

void PrimaryLogPG::start_copy(CopyCallback *cb, ObjectContextRef obc,
			      hobject_t src, object_locator_t oloc,
			      version_t version, unsigned flags,
			      bool mirror_snapset,
			      unsigned src_obj_fadvise_flags,
			      unsigned dest_obj_fadvise_flags)
{
  const hobject_t& dest = obc->obs.oi.soid;
  dout(10) << __func__ << " " << dest
	   << " from " << src << " " << oloc << " v" << version
	   << " flags " << flags
	   << (mirror_snapset ? " mirror_snapset" : "")
	   << dendl;

  ceph_assert(!mirror_snapset || src.snap == CEPH_NOSNAP);

  // cancel a previous in-progress copy?
  if (copy_ops.count(dest)) {
    // FIXME: if the src etc match, we could avoid restarting from the
    // beginning.
    CopyOpRef cop = copy_ops[dest];
    vector<ceph_tid_t> tids;
    cancel_copy(cop, false, &tids);
    osd->objecter->op_cancel(tids, -ECANCELED);
  }

  CopyOpRef cop(std::make_shared<CopyOp>(cb, obc, src, oloc, version, flags,
			   mirror_snapset, src_obj_fadvise_flags,
			   dest_obj_fadvise_flags));
  copy_ops[dest] = cop;
  obc->start_block();

  if (!obc->obs.oi.has_manifest()) {
    _copy_some(obc, cop);
  } else {
    if (obc->obs.oi.manifest.is_redirect()) {
      _copy_some(obc, cop);
    } else if (obc->obs.oi.manifest.is_chunked()) {
      auto p = obc->obs.oi.manifest.chunk_map.begin();
      _copy_some_manifest(obc, cop, p->first);
    } else {
      ceph_abort_msg("unrecognized manifest type");
    }
  }
}

void PrimaryLogPG::_copy_some(ObjectContextRef obc, CopyOpRef cop)
{
  dout(10) << __func__ << " " << *obc << " " << cop << dendl;

  unsigned flags = 0;
  if (cop->flags & CEPH_OSD_COPY_FROM_FLAG_FLUSH)
    flags |= CEPH_OSD_FLAG_FLUSH;
  if (cop->flags & CEPH_OSD_COPY_FROM_FLAG_IGNORE_CACHE)
    flags |= CEPH_OSD_FLAG_IGNORE_CACHE;
  if (cop->flags & CEPH_OSD_COPY_FROM_FLAG_IGNORE_OVERLAY)
    flags |= CEPH_OSD_FLAG_IGNORE_OVERLAY;
  if (cop->flags & CEPH_OSD_COPY_FROM_FLAG_MAP_SNAP_CLONE)
    flags |= CEPH_OSD_FLAG_MAP_SNAP_CLONE;
  if (cop->flags & CEPH_OSD_COPY_FROM_FLAG_RWORDERED)
    flags |= CEPH_OSD_FLAG_RWORDERED;

  C_GatherBuilder gather(cct);

  if (cop->cursor.is_initial() && cop->mirror_snapset) {
    // list snaps too.
    ceph_assert(cop->src.snap == CEPH_NOSNAP);
    ObjectOperation op;
    op.list_snaps(&cop->results.snapset, NULL);
    ceph_tid_t tid = osd->objecter->read(cop->src.oid, cop->oloc, op,
				    CEPH_SNAPDIR, NULL,
				    flags, gather.new_sub(), NULL);
    cop->objecter_tid2 = tid;
  }

  ObjectOperation op;
  if (cop->results.user_version) {
    op.assert_version(cop->results.user_version);
  } else {
    // we should learn the version after the first chunk, if we didn't know
    // it already!
    ceph_assert(cop->cursor.is_initial());
  }
  op.copy_get(&cop->cursor, get_copy_chunk_size(),
	      &cop->results.object_size, &cop->results.mtime,
	      &cop->attrs, &cop->data, &cop->omap_header, &cop->omap_data,
	      &cop->results.snaps, &cop->results.snap_seq,
	      &cop->results.flags,
	      &cop->results.source_data_digest,
	      &cop->results.source_omap_digest,
	      &cop->results.reqids,
	      &cop->results.reqid_return_codes,
	      &cop->results.truncate_seq,
	      &cop->results.truncate_size,
	      &cop->rval);
  op.set_last_op_flags(cop->src_obj_fadvise_flags);

  C_Copyfrom *fin = new C_Copyfrom(this, obc->obs.oi.soid,
				   get_last_peering_reset(), cop);
  gather.set_finisher(new C_OnFinisher(fin,
				       osd->get_objecter_finisher(get_pg_shard())));

  ceph_tid_t tid = osd->objecter->read(cop->src.oid, cop->oloc, op,
				  cop->src.snap, NULL,
				  flags,
				  gather.new_sub(),
				  // discover the object version if we don't know it yet
				  cop->results.user_version ? NULL : &cop->results.user_version);
  fin->tid = tid;
  cop->objecter_tid = tid;
  gather.activate();
}

void PrimaryLogPG::_copy_some_manifest(ObjectContextRef obc, CopyOpRef cop, uint64_t start_offset)
{
  dout(10) << __func__ << " " << *obc << " " << cop << dendl;

  unsigned flags = 0;
  if (cop->flags & CEPH_OSD_COPY_FROM_FLAG_FLUSH)
    flags |= CEPH_OSD_FLAG_FLUSH;
  if (cop->flags & CEPH_OSD_COPY_FROM_FLAG_IGNORE_CACHE)
    flags |= CEPH_OSD_FLAG_IGNORE_CACHE;
  if (cop->flags & CEPH_OSD_COPY_FROM_FLAG_IGNORE_OVERLAY)
    flags |= CEPH_OSD_FLAG_IGNORE_OVERLAY;
  if (cop->flags & CEPH_OSD_COPY_FROM_FLAG_MAP_SNAP_CLONE)
    flags |= CEPH_OSD_FLAG_MAP_SNAP_CLONE;
  if (cop->flags & CEPH_OSD_COPY_FROM_FLAG_RWORDERED)
    flags |= CEPH_OSD_FLAG_RWORDERED;

  int num_chunks = 0;
  uint64_t last_offset = 0, chunks_size = 0;
  object_manifest_t *manifest = &obc->obs.oi.manifest;
  map<uint64_t, chunk_info_t>::iterator iter = manifest->chunk_map.find(start_offset); 
  for (;iter != manifest->chunk_map.end(); ++iter) {
    num_chunks++;
    chunks_size += iter->second.length;
    last_offset = iter->first;
    if (get_copy_chunk_size() < chunks_size) {
      break;
    }
  }

  cop->num_chunk = num_chunks;
  cop->start_offset = start_offset;
  cop->last_offset = last_offset;
  dout(20) << __func__ << " oid " << obc->obs.oi.soid << " num_chunks: " << num_chunks
	  << " start_offset: " << start_offset << " chunks_size: " << chunks_size 
	  << " last_offset: " << last_offset << dendl;

  iter = manifest->chunk_map.find(start_offset);
  for (;iter != manifest->chunk_map.end(); ++iter) {
    uint64_t obj_offset = iter->first;
    uint64_t length = manifest->chunk_map[iter->first].length;
    hobject_t soid = manifest->chunk_map[iter->first].oid;
    object_locator_t oloc(soid);
    CopyCallback * cb = NULL;
    CopyOpRef sub_cop(std::make_shared<CopyOp>(cb, ObjectContextRef(), cop->src, oloc,
		       cop->results.user_version, cop->flags, cop->mirror_snapset,
		       cop->src_obj_fadvise_flags, cop->dest_obj_fadvise_flags));
    sub_cop->cursor.data_offset = obj_offset;
    cop->chunk_cops[obj_offset] = sub_cop;

    int s = sub_cop->chunk_ops.size();
    sub_cop->chunk_ops.resize(s+1);
    sub_cop->chunk_ops[s].op.op =  CEPH_OSD_OP_READ;
    sub_cop->chunk_ops[s].op.extent.offset = manifest->chunk_map[iter->first].offset;
    sub_cop->chunk_ops[s].op.extent.length = length;

    ObjectOperation op;
    op.dup(sub_cop->chunk_ops);

    if (cop->results.user_version) {
      op.assert_version(cop->results.user_version);
    } else {
      // we should learn the version after the first chunk, if we didn't know
      // it already!
      ceph_assert(cop->cursor.is_initial());
    }
    op.set_last_op_flags(cop->src_obj_fadvise_flags);

    C_CopyChunk *fin = new C_CopyChunk(this, obc->obs.oi.soid,
				     get_last_peering_reset(), cop);
    fin->offset = obj_offset;

    ceph_tid_t tid = osd->objecter->read(
      soid.oid, oloc, op,
      sub_cop->src.snap, NULL,
      flags,
      new C_OnFinisher(fin, osd->get_objecter_finisher(get_pg_shard())),
      // discover the object version if we don't know it yet
      sub_cop->results.user_version ? NULL : &sub_cop->results.user_version);
    fin->tid = tid;
    sub_cop->objecter_tid = tid;

    dout(20) << __func__ << " tgt_oid: " << soid.oid << " tgt_offset: " 
	    << manifest->chunk_map[iter->first].offset
	    << " length: " << length << " pool id: " << oloc.pool 
	    << " tid: " << tid << dendl;

    if (last_offset < iter->first) {
      break;
    }
  }
}

void PrimaryLogPG::process_copy_chunk(hobject_t oid, ceph_tid_t tid, int r)
{
  dout(10) << __func__ << " " << oid << " tid " << tid
	   << " " << cpp_strerror(r) << dendl;
  map<hobject_t,CopyOpRef>::iterator p = copy_ops.find(oid);
  if (p == copy_ops.end()) {
    dout(10) << __func__ << " no copy_op found" << dendl;
    return;
  }
  CopyOpRef cop = p->second;
  if (tid != cop->objecter_tid) {
    dout(10) << __func__ << " tid " << tid << " != cop " << cop
	     << " tid " << cop->objecter_tid << dendl;
    return;
  }

  if (cop->omap_data.length() || cop->omap_header.length())
    cop->results.has_omap = true;

  if (r >= 0 && !pool.info.supports_omap() &&
      (cop->omap_data.length() || cop->omap_header.length())) {
    r = -EOPNOTSUPP;
  }
  cop->objecter_tid = 0;
  cop->objecter_tid2 = 0;  // assume this ordered before us (if it happened)
  ObjectContextRef& cobc = cop->obc;

  if (r < 0)
    goto out;

  ceph_assert(cop->rval >= 0);

  if (oid.snap < CEPH_NOSNAP && !cop->results.snaps.empty()) {
    // verify snap hasn't been deleted
    vector<snapid_t>::iterator p = cop->results.snaps.begin();
    while (p != cop->results.snaps.end()) {
      // make best effort to sanitize snaps/clones.
      if (get_osdmap()->in_removed_snaps_queue(info.pgid.pgid.pool(), *p)) {
	dout(10) << __func__ << " clone snap " << *p << " has been deleted"
		 << dendl;
	for (vector<snapid_t>::iterator q = p + 1;
	     q != cop->results.snaps.end();
	     ++q)
	  *(q - 1) = *q;
	cop->results.snaps.resize(cop->results.snaps.size() - 1);
      } else {
	++p;
      }
    }
    if (cop->results.snaps.empty()) {
      dout(10) << __func__ << " no more snaps for " << oid << dendl;
      r = -ENOENT;
      goto out;
    }
  }

  ceph_assert(cop->rval >= 0);

  if (!cop->temp_cursor.data_complete) {
    cop->results.data_digest = cop->data.crc32c(cop->results.data_digest);
  }
  if (pool.info.supports_omap() && !cop->temp_cursor.omap_complete) {
    if (cop->omap_header.length()) {
      cop->results.omap_digest =
	cop->omap_header.crc32c(cop->results.omap_digest);
    }
    if (cop->omap_data.length()) {
      bufferlist keys;
      keys.substr_of(cop->omap_data, 4, cop->omap_data.length() - 4);
      cop->results.omap_digest = keys.crc32c(cop->results.omap_digest);
    }
  }

  if (!cop->temp_cursor.attr_complete) {
    for (map<string,bufferlist>::iterator p = cop->attrs.begin();
	 p != cop->attrs.end();
	 ++p) {
      cop->results.attrs[string("_") + p->first] = p->second;
    }
    cop->attrs.clear();
  }

  if (!cop->cursor.is_complete()) {
    // write out what we have so far
    if (cop->temp_cursor.is_initial()) {
      ceph_assert(!cop->results.started_temp_obj);
      cop->results.started_temp_obj = true;
      cop->results.temp_oid = generate_temp_object(oid);
      dout(20) << __func__ << " using temp " << cop->results.temp_oid << dendl;
    }
    ObjectContextRef tempobc = get_object_context(cop->results.temp_oid, true);
    OpContextUPtr ctx = simple_opc_create(tempobc);
    if (cop->temp_cursor.is_initial()) {
      ctx->new_temp_oid = cop->results.temp_oid;
    }
    _write_copy_chunk(cop, ctx->op_t.get());
    simple_opc_submit(std::move(ctx));
    dout(10) << __func__ << " fetching more" << dendl;
    _copy_some(cobc, cop);
    return;
  }

  // verify digests?
  if (cop->results.is_data_digest() || cop->results.is_omap_digest()) {
    dout(20) << __func__ << std::hex
      << " got digest: rx data 0x" << cop->results.data_digest
      << " omap 0x" << cop->results.omap_digest
      << ", source: data 0x" << cop->results.source_data_digest
      << " omap 0x" <<  cop->results.source_omap_digest
      << std::dec
      << " flags " << cop->results.flags
      << dendl;
  }
  if (cop->results.is_data_digest() &&
      cop->results.data_digest != cop->results.source_data_digest) {
    derr << __func__ << std::hex << " data digest 0x" << cop->results.data_digest
	 << " != source 0x" << cop->results.source_data_digest << std::dec
	 << dendl;
    osd->clog->error() << info.pgid << " copy from " << cop->src
		       << " to " << cop->obc->obs.oi.soid << std::hex
		       << " data digest 0x" << cop->results.data_digest
		       << " != source 0x" << cop->results.source_data_digest
		       << std::dec;
    r = -EIO;
    goto out;
  }
  if (cop->results.is_omap_digest() &&
      cop->results.omap_digest != cop->results.source_omap_digest) {
    derr << __func__ << std::hex
	 << " omap digest 0x" << cop->results.omap_digest
	 << " != source 0x" << cop->results.source_omap_digest
	 << std::dec << dendl;
    osd->clog->error() << info.pgid << " copy from " << cop->src
		       << " to " << cop->obc->obs.oi.soid << std::hex
		       << " omap digest 0x" << cop->results.omap_digest
		       << " != source 0x" << cop->results.source_omap_digest
		       << std::dec;
    r = -EIO;
    goto out;
  }
  if (cct->_conf->osd_debug_inject_copyfrom_error) {
    derr << __func__ << " injecting copyfrom failure" << dendl;
    r = -EIO;
    goto out;
  }

  cop->results.fill_in_final_tx = std::function<void(PGTransaction*)>(
    [this, &cop /* avoid ref cycle */](PGTransaction *t) {
      ObjectState& obs = cop->obc->obs;
      if (cop->temp_cursor.is_initial()) {
	dout(20) << "fill_in_final_tx: writing "
		 << "directly to final object" << dendl;
	// write directly to final object
	cop->results.temp_oid = obs.oi.soid;
	_write_copy_chunk(cop, t);
      } else {
	// finish writing to temp object, then move into place
	dout(20) << "fill_in_final_tx: writing to temp object" << dendl;
	_write_copy_chunk(cop, t);
	t->rename(obs.oi.soid, cop->results.temp_oid);
      }
      t->setattrs(obs.oi.soid, cop->results.attrs);
    });

  dout(20) << __func__ << " success; committing" << dendl;

 out:
  dout(20) << __func__ << " complete r = " << cpp_strerror(r) << dendl;
  CopyCallbackResults results(r, &cop->results);
  cop->cb->complete(results);

  copy_ops.erase(cobc->obs.oi.soid);
  cobc->stop_block();

  if (r < 0 && cop->results.started_temp_obj) {
    dout(10) << __func__ << " deleting partial temp object "
	     << cop->results.temp_oid << dendl;
    ObjectContextRef tempobc = get_object_context(cop->results.temp_oid, true);
    OpContextUPtr ctx = simple_opc_create(tempobc);
    ctx->op_t->remove(cop->results.temp_oid);
    ctx->discard_temp_oid = cop->results.temp_oid;
    simple_opc_submit(std::move(ctx));
  }

  // cancel and requeue proxy ops on this object
  if (!r) {
    cancel_and_requeue_proxy_ops(cobc->obs.oi.soid);
  }

  kick_object_context_blocked(cobc);
}

void PrimaryLogPG::process_copy_chunk_manifest(hobject_t oid, ceph_tid_t tid, int r, uint64_t offset)
{
  dout(10) << __func__ << " " << oid << " tid " << tid
	   << " " << cpp_strerror(r) << dendl;
  map<hobject_t,CopyOpRef>::iterator p = copy_ops.find(oid);
  if (p == copy_ops.end()) {
    dout(10) << __func__ << " no copy_op found" << dendl;
    return;
  }
  CopyOpRef obj_cop = p->second;
  CopyOpRef chunk_cop = obj_cop->chunk_cops[offset];

  if (tid != chunk_cop->objecter_tid) {
    dout(10) << __func__ << " tid " << tid << " != cop " << chunk_cop
	     << " tid " << chunk_cop->objecter_tid << dendl;
    return;
  }

  if (chunk_cop->omap_data.length() || chunk_cop->omap_header.length()) {
    r = -EOPNOTSUPP;
  }

  chunk_cop->objecter_tid = 0;
  chunk_cop->objecter_tid2 = 0;  // assume this ordered before us (if it happened)
  ObjectContextRef& cobc = obj_cop->obc;
  OSDOp &chunk_data = chunk_cop->chunk_ops[0];

  if (r < 0) {
    obj_cop->failed = true;
    goto out;
  }   

  if (obj_cop->failed) {
    return;
  }                                                                                                                  
  if (!chunk_data.outdata.length()) {
    r = -EIO;
    obj_cop->failed = true;
    goto out;
  }

  obj_cop->num_chunk--;

  /* check all of the copyop are completed */
  if (obj_cop->num_chunk) {
    dout(20) << __func__ << " num_chunk: " << obj_cop->num_chunk << dendl;
    return;
  }

  {
    OpContextUPtr ctx = simple_opc_create(obj_cop->obc);
    if (!ctx->lock_manager.take_write_lock(
	  obj_cop->obc->obs.oi.soid,
	  obj_cop->obc)) {
      // recovery op can take read lock. 
      // so need to wait for recovery completion 
      r = -EAGAIN;
      obj_cop->failed = true;
      close_op_ctx(ctx.release());
      goto out;
    }
    dout(20) << __func__ << " took lock on obc, " << obj_cop->obc->rwstate << dendl;

    PGTransaction *t = ctx->op_t.get();
    ObjectState& obs = ctx->new_obs;
    for (auto p : obj_cop->chunk_cops) {
      OSDOp &sub_chunk = p.second->chunk_ops[0];
      t->write(cobc->obs.oi.soid,
	      p.second->cursor.data_offset,
	      sub_chunk.outdata.length(),
	      sub_chunk.outdata,
	      p.second->dest_obj_fadvise_flags);
      dout(20) << __func__ << " offset: " << p.second->cursor.data_offset 
	      << " length: " << sub_chunk.outdata.length() << dendl;
      write_update_size_and_usage(ctx->delta_stats, obs.oi, ctx->modified_ranges,
				  p.second->cursor.data_offset, sub_chunk.outdata.length());
      obs.oi.manifest.chunk_map[p.second->cursor.data_offset].clear_flag(chunk_info_t::FLAG_DIRTY); 
      obs.oi.manifest.chunk_map[p.second->cursor.data_offset].clear_flag(chunk_info_t::FLAG_MISSING); 
      ctx->clean_regions.mark_data_region_dirty(p.second->cursor.data_offset, sub_chunk.outdata.length());
      sub_chunk.outdata.clear();
    }
    obs.oi.clear_data_digest();
    ctx->at_version = get_next_version(); 
    finish_ctx(ctx.get(), pg_log_entry_t::PROMOTE);
    simple_opc_submit(std::move(ctx));

    auto p = cobc->obs.oi.manifest.chunk_map.rbegin();
    /* check remaining work */
    if (p != cobc->obs.oi.manifest.chunk_map.rend()) {
      if (obj_cop->last_offset >= p->first + p->second.length) {
	for (auto &en : cobc->obs.oi.manifest.chunk_map) {
	  if (obj_cop->last_offset < en.first) {
	    _copy_some_manifest(cobc, obj_cop, en.first);
	    return;
	  }
	}
      }
    }
  }

 out:
  dout(20) << __func__ << " complete r = " << cpp_strerror(r) << dendl;
  CopyCallbackResults results(r, &obj_cop->results);
  obj_cop->cb->complete(results);

  copy_ops.erase(cobc->obs.oi.soid);
  cobc->stop_block();

  // cancel and requeue proxy ops on this object
  if (!r) {
    cancel_and_requeue_proxy_ops(cobc->obs.oi.soid);
  }

  kick_object_context_blocked(cobc);
}

void PrimaryLogPG::cancel_and_requeue_proxy_ops(hobject_t oid) {
  vector<ceph_tid_t> tids;
  for (map<ceph_tid_t, ProxyReadOpRef>::iterator it = proxyread_ops.begin();
      it != proxyread_ops.end();) {
    if (it->second->soid == oid) {
      cancel_proxy_read((it++)->second, &tids);
    } else {
      ++it;
    }
  }
  for (map<ceph_tid_t, ProxyWriteOpRef>::iterator it = proxywrite_ops.begin();
       it != proxywrite_ops.end();) {
    if (it->second->soid == oid) {
      cancel_proxy_write((it++)->second, &tids);
    } else {
      ++it;
    }
  }
  osd->objecter->op_cancel(tids, -ECANCELED);
  kick_proxy_ops_blocked(oid);
}

void PrimaryLogPG::_write_copy_chunk(CopyOpRef cop, PGTransaction *t)
{
  dout(20) << __func__ << " " << cop
	   << " " << cop->attrs.size() << " attrs"
	   << " " << cop->data.length() << " bytes"
	   << " " << cop->omap_header.length() << " omap header bytes"
	   << " " << cop->omap_data.length() << " omap data bytes"
	   << dendl;
  if (!cop->temp_cursor.attr_complete) {
    t->create(cop->results.temp_oid);
  }
  if (!cop->temp_cursor.data_complete) {
    ceph_assert(cop->data.length() + cop->temp_cursor.data_offset ==
	   cop->cursor.data_offset);
    if (pool.info.required_alignment() &&
	!cop->cursor.data_complete) {
      /**
       * Trim off the unaligned bit at the end, we'll adjust cursor.data_offset
       * to pick it up on the next pass.
       */
      ceph_assert(cop->temp_cursor.data_offset %
	     pool.info.required_alignment() == 0);
      if (cop->data.length() % pool.info.required_alignment() != 0) {
	uint64_t to_trim =
	  cop->data.length() % pool.info.required_alignment();
	bufferlist bl;
	bl.substr_of(cop->data, 0, cop->data.length() - to_trim);
	cop->data.swap(bl);
	cop->cursor.data_offset -= to_trim;
	ceph_assert(cop->data.length() + cop->temp_cursor.data_offset ==
	       cop->cursor.data_offset);
      }
    }
    if (cop->data.length()) {
      t->write(
	cop->results.temp_oid,
	cop->temp_cursor.data_offset,
	cop->data.length(),
	cop->data,
	cop->dest_obj_fadvise_flags);
    }
    cop->data.clear();
  }
  if (pool.info.supports_omap()) {
    if (!cop->temp_cursor.omap_complete) {
      if (cop->omap_header.length()) {
	t->omap_setheader(
	  cop->results.temp_oid,
	  cop->omap_header);
	cop->omap_header.clear();
      }
      if (cop->omap_data.length()) {
	map<string,bufferlist> omap;
	bufferlist::const_iterator p = cop->omap_data.begin();
	decode(omap, p);
	t->omap_setkeys(cop->results.temp_oid, omap);
	cop->omap_data.clear();
      }
    }
  } else {
    ceph_assert(cop->omap_header.length() == 0);
    ceph_assert(cop->omap_data.length() == 0);
  }
  cop->temp_cursor = cop->cursor;
}

void PrimaryLogPG::finish_copyfrom(CopyFromCallback *cb)
{
  OpContext *ctx = cb->ctx;
  dout(20) << "finish_copyfrom on " << ctx->obs->oi.soid << dendl;

  ObjectState& obs = ctx->new_obs;
  if (obs.exists) {
    dout(20) << __func__ << ": exists, removing" << dendl;
    ctx->op_t->remove(obs.oi.soid);
  } else {
    ctx->delta_stats.num_objects++;
    obs.exists = true;
  }
  if (cb->is_temp_obj_used()) {
    ctx->discard_temp_oid = cb->results->temp_oid;
  }
  cb->results->fill_in_final_tx(ctx->op_t.get());

  // CopyFromCallback fills this in for us
  obs.oi.user_version = ctx->user_at_version;

  if (cb->results->is_data_digest()) {
    obs.oi.set_data_digest(cb->results->data_digest);
  } else {
    obs.oi.clear_data_digest();
  }
  if (cb->results->is_omap_digest()) {
    obs.oi.set_omap_digest(cb->results->omap_digest);
  } else {
    obs.oi.clear_omap_digest();
  }

  obs.oi.truncate_seq = cb->truncate_seq;
  obs.oi.truncate_size = cb->truncate_size;

  obs.oi.mtime = ceph::real_clock::to_timespec(cb->results->mtime);
  ctx->mtime = utime_t();

  ctx->extra_reqids = cb->results->reqids;
  ctx->extra_reqid_return_codes = cb->results->reqid_return_codes;

  // cache: clear whiteout?
  if (obs.oi.is_whiteout()) {
    dout(10) << __func__ << " clearing whiteout on " << obs.oi.soid << dendl;
    obs.oi.clear_flag(object_info_t::FLAG_WHITEOUT);
    --ctx->delta_stats.num_whiteouts;
  }

  if (cb->results->has_omap) {
    dout(10) << __func__ << " setting omap flag on " << obs.oi.soid << dendl;
    obs.oi.set_flag(object_info_t::FLAG_OMAP);
    ctx->clean_regions.mark_omap_dirty();
  } else {
    dout(10) << __func__ << " clearing omap flag on " << obs.oi.soid << dendl;
    obs.oi.clear_flag(object_info_t::FLAG_OMAP);
  }

  interval_set<uint64_t> ch;
  if (obs.oi.size > 0)
    ch.insert(0, obs.oi.size);
  ctx->modified_ranges.union_of(ch);
  ctx->clean_regions.mark_data_region_dirty(0, std::max(obs.oi.size, cb->get_data_size()));

  if (cb->get_data_size() != obs.oi.size) {
    ctx->delta_stats.num_bytes -= obs.oi.size;
    obs.oi.size = cb->get_data_size();
    ctx->delta_stats.num_bytes += obs.oi.size;
  }
  ctx->delta_stats.num_wr++;
  ctx->delta_stats.num_wr_kb += shift_round_up(obs.oi.size, 10);

  osd->logger->inc(l_osd_copyfrom);
}

void PrimaryLogPG::finish_promote(int r, CopyResults *results,
				  ObjectContextRef obc)
{
  const hobject_t& soid = obc->obs.oi.soid;
  dout(10) << __func__ << " " << soid << " r=" << r
	   << " uv" << results->user_version << dendl;

  if (r == -ECANCELED) {
    return;
  }

  if (r != -ENOENT && soid.is_snap()) {
    if (results->snaps.empty()) {
      // we must have read "snap" content from the head object in the
      // base pool.  use snap_seq to construct what snaps should be
      // for this clone (what is was before we evicted the clean clone
      // from this pool, and what it will be when we flush and the
      // clone eventually happens in the base pool).  we want to use
      // snaps in (results->snap_seq,soid.snap]
      SnapSet& snapset = obc->ssc->snapset;
      for (auto p = snapset.clone_snaps.rbegin();
	   p != snapset.clone_snaps.rend();
	   ++p) {
	for (auto snap : p->second) {
	  if (snap > soid.snap) {
	    continue;
	  }
	  if (snap <= results->snap_seq) {
	    break;
	  }
	  results->snaps.push_back(snap);
	}
      }
    }

    dout(20) << __func__ << " snaps " << results->snaps << dendl;
    filter_snapc(results->snaps);

    dout(20) << __func__ << " filtered snaps " << results->snaps << dendl;
    if (results->snaps.empty()) {
      dout(20) << __func__
	       << " snaps are empty, clone is invalid,"
	       << " setting r to ENOENT" << dendl;
      r = -ENOENT;
    }
  }

  if (r < 0 && results->started_temp_obj) {
    dout(10) << __func__ << " abort; will clean up partial work" << dendl;
    ObjectContextRef tempobc = get_object_context(results->temp_oid, false);
    ceph_assert(tempobc);
    OpContextUPtr ctx = simple_opc_create(tempobc);
    ctx->op_t->remove(results->temp_oid);
    simple_opc_submit(std::move(ctx));
    results->started_temp_obj = false;
  }

  if (r == -ENOENT && soid.is_snap()) {
    dout(10) << __func__
	     << ": enoent while trying to promote clone, " << soid
	     << " must have been trimmed, removing from snapset"
	     << dendl;
    hobject_t head(soid.get_head());
    ObjectContextRef obc = get_object_context(head, false);
    ceph_assert(obc);

    OpContextUPtr tctx = simple_opc_create(obc);
    tctx->at_version = get_next_version();
    if (get_osdmap()->require_osd_release < ceph_release_t::octopus) {
      filter_snapc(tctx->new_snapset.snaps);
    } else {
      tctx->new_snapset.snaps.clear();
    }
    vector<snapid_t> new_clones;
    map<snapid_t, vector<snapid_t>> new_clone_snaps;
    for (vector<snapid_t>::iterator i = tctx->new_snapset.clones.begin();
	 i != tctx->new_snapset.clones.end();
	 ++i) {
      if (*i != soid.snap) {
	new_clones.push_back(*i);
	auto p = tctx->new_snapset.clone_snaps.find(*i);
	if (p != tctx->new_snapset.clone_snaps.end()) {
	  new_clone_snaps[*i] = p->second;
	}
      }
    }
    tctx->new_snapset.clones.swap(new_clones);
    tctx->new_snapset.clone_overlap.erase(soid.snap);
    tctx->new_snapset.clone_size.erase(soid.snap);
    tctx->new_snapset.clone_snaps.swap(new_clone_snaps);

    // take RWWRITE lock for duration of our local write.  ignore starvation.
    if (!tctx->lock_manager.take_write_lock(
	  head,
	  obc)) {
      ceph_abort_msg("problem!");
    }
    dout(20) << __func__ << " took lock on obc, " << obc->rwstate << dendl;

    finish_ctx(tctx.get(), pg_log_entry_t::PROMOTE);

    simple_opc_submit(std::move(tctx));
    return;
  }

  bool whiteout = false;
  if (r == -ENOENT) {
    ceph_assert(soid.snap == CEPH_NOSNAP); // snap case is above
    dout(10) << __func__ << " whiteout " << soid << dendl;
    whiteout = true;
  }

  if (r < 0 && !whiteout) {
    derr << __func__ << " unexpected promote error " << cpp_strerror(r) << dendl;
    // pass error to everyone blocked on this object
    // FIXME: this is pretty sloppy, but at this point we got
    // something unexpected and don't have many other options.
    map<hobject_t,list<OpRequestRef>>::iterator blocked_iter =
      waiting_for_blocked_object.find(soid);
    if (blocked_iter != waiting_for_blocked_object.end()) {
      while (!blocked_iter->second.empty()) {
	osd->reply_op_error(blocked_iter->second.front(), r);
	blocked_iter->second.pop_front();
      }
      waiting_for_blocked_object.erase(blocked_iter);
    }
    return;
  }

  osd->promote_finish(results->object_size);

  OpContextUPtr tctx =  simple_opc_create(obc);
  tctx->at_version = get_next_version();

  if (!obc->obs.oi.has_manifest()) {
    ++tctx->delta_stats.num_objects;
  }
  if (soid.snap < CEPH_NOSNAP)
    ++tctx->delta_stats.num_object_clones;
  tctx->new_obs.exists = true;

  tctx->extra_reqids = results->reqids;
  tctx->extra_reqid_return_codes = results->reqid_return_codes;

  if (whiteout) {
    // create a whiteout
    tctx->op_t->create(soid);
    tctx->new_obs.oi.set_flag(object_info_t::FLAG_WHITEOUT);
    ++tctx->delta_stats.num_whiteouts;
    dout(20) << __func__ << " creating whiteout on " << soid << dendl;
    osd->logger->inc(l_osd_tier_whiteout);
  } else {
    if (results->has_omap) {
      dout(10) << __func__ << " setting omap flag on " << soid << dendl;
      tctx->new_obs.oi.set_flag(object_info_t::FLAG_OMAP);
      ++tctx->delta_stats.num_objects_omap;
    }

    results->fill_in_final_tx(tctx->op_t.get());
    if (results->started_temp_obj) {
      tctx->discard_temp_oid = results->temp_oid;
    }
    tctx->new_obs.oi.size = results->object_size;
    tctx->new_obs.oi.user_version = results->user_version;
    tctx->new_obs.oi.mtime = ceph::real_clock::to_timespec(results->mtime);
    tctx->mtime = utime_t();
    if (results->is_data_digest()) {
      tctx->new_obs.oi.set_data_digest(results->data_digest);
    } else {
      tctx->new_obs.oi.clear_data_digest();
    }
    if (results->object_size)
      tctx->clean_regions.mark_data_region_dirty(0, results->object_size);
    if (results->is_omap_digest()) {
      tctx->new_obs.oi.set_omap_digest(results->omap_digest);
    } else {
      tctx->new_obs.oi.clear_omap_digest();
    }
    if (results->has_omap)
        tctx->clean_regions.mark_omap_dirty();
    tctx->new_obs.oi.truncate_seq = results->truncate_seq;
    tctx->new_obs.oi.truncate_size = results->truncate_size;

    if (soid.snap != CEPH_NOSNAP) {
      ceph_assert(obc->ssc->snapset.clone_snaps.count(soid.snap));
      ceph_assert(obc->ssc->snapset.clone_size.count(soid.snap));
      ceph_assert(obc->ssc->snapset.clone_size[soid.snap] ==
	     results->object_size);
      ceph_assert(obc->ssc->snapset.clone_overlap.count(soid.snap));

      tctx->delta_stats.num_bytes += obc->ssc->snapset.get_clone_bytes(soid.snap);
    } else {
      tctx->delta_stats.num_bytes += results->object_size;
    }
  }

  if (results->mirror_snapset) {
    ceph_assert(tctx->new_obs.oi.soid.snap == CEPH_NOSNAP);
    tctx->new_snapset.from_snap_set(
      results->snapset,
      get_osdmap()->require_osd_release < ceph_release_t::luminous);
  }
  dout(20) << __func__ << " new_snapset " << tctx->new_snapset << dendl;

  // take RWWRITE lock for duration of our local write.  ignore starvation.
  if (!tctx->lock_manager.take_write_lock(
	obc->obs.oi.soid,
	obc)) {
    ceph_abort_msg("problem!");
  }
  dout(20) << __func__ << " took lock on obc, " << obc->rwstate << dendl;

  finish_ctx(tctx.get(), pg_log_entry_t::PROMOTE);

  simple_opc_submit(std::move(tctx));

  osd->logger->inc(l_osd_tier_promote);

  if (agent_state &&
      agent_state->is_idle())
    agent_choose_mode();
}

void PrimaryLogPG::finish_promote_manifest(int r, CopyResults *results,
					    ObjectContextRef obc)
{
  const hobject_t& soid = obc->obs.oi.soid;
  dout(10) << __func__ << " " << soid << " r=" << r
	   << " uv" << results->user_version << dendl;

  if (r == -ECANCELED || r == -EAGAIN) {
    return;
  }

  if (r < 0) {
    derr << __func__ << " unexpected promote error " << cpp_strerror(r) << dendl;
    // pass error to everyone blocked on this object
    // FIXME: this is pretty sloppy, but at this point we got
    // something unexpected and don't have many other options.
    map<hobject_t,list<OpRequestRef>>::iterator blocked_iter =
      waiting_for_blocked_object.find(soid);
    if (blocked_iter != waiting_for_blocked_object.end()) {
      while (!blocked_iter->second.empty()) {
	osd->reply_op_error(blocked_iter->second.front(), r);
	blocked_iter->second.pop_front();
      }
      waiting_for_blocked_object.erase(blocked_iter);
    }
    return;
  }
  
  osd->promote_finish(results->object_size);
  osd->logger->inc(l_osd_tier_promote);

  if (agent_state &&
      agent_state->is_idle())
    agent_choose_mode();
}

void PrimaryLogPG::cancel_copy(CopyOpRef cop, bool requeue,
			       vector<ceph_tid_t> *tids)
{
  dout(10) << __func__ << " " << cop->obc->obs.oi.soid
	   << " from " << cop->src << " " << cop->oloc
	   << " v" << cop->results.user_version << dendl;

  // cancel objecter op, if we can
  if (cop->objecter_tid) {
    tids->push_back(cop->objecter_tid);
    cop->objecter_tid = 0;
    if (cop->objecter_tid2) {
      tids->push_back(cop->objecter_tid2);
      cop->objecter_tid2 = 0;
    }
  }

  copy_ops.erase(cop->obc->obs.oi.soid);
  cop->obc->stop_block();

  kick_object_context_blocked(cop->obc);
  cop->results.should_requeue = requeue;
  CopyCallbackResults result(-ECANCELED, &cop->results);
  cop->cb->complete(result);

  // There may still be an objecter callback referencing this copy op.
  // That callback will not need the obc since it's been canceled, and
  // we need the obc reference to go away prior to flush.
  cop->obc = ObjectContextRef();
}

void PrimaryLogPG::cancel_copy_ops(bool requeue, vector<ceph_tid_t> *tids)
{
  dout(10) << __func__ << dendl;
  map<hobject_t,CopyOpRef>::iterator p = copy_ops.begin();
  while (p != copy_ops.end()) {
    // requeue this op? can I queue up all of them?
    cancel_copy((p++)->second, requeue, tids);
  }
}


// ========================================================================
// flush
//
// Flush a dirty object in the cache tier by writing it back to the
// base tier.  The sequence looks like:
//
//  * send a copy-from operation to the base tier to copy the current
//    version of the object
//  * base tier will pull the object via (perhaps multiple) copy-get(s)
//  * on completion, we check if the object has been modified.  if so,
//    just reply with -EAGAIN.
//  * try to take a write lock so we can clear the dirty flag.  if this
//    fails, wait and retry
//  * start a repop that clears the bit.
//
// If we have to wait, we will retry by coming back through the
// start_flush method.  We check if a flush is already in progress
// and, if so, try to finish it by rechecking the version and trying
// to clear the dirty bit.
//
// In order for the cache-flush (a write op) to not block the copy-get
// from reading the object, the client *must* set the SKIPRWLOCKS
// flag.
//
// NOTE: normally writes are strictly ordered for the client, but
// flushes are special in that they can be reordered with respect to
// other writes.  In particular, we can't have a flush request block
// an update to the cache pool object!

struct C_Flush : public Context {
  PrimaryLogPGRef pg;
  hobject_t oid;
  epoch_t last_peering_reset;
  ceph_tid_t tid;
  utime_t start;
  C_Flush(PrimaryLogPG *p, hobject_t o, epoch_t lpr)
    : pg(p), oid(o), last_peering_reset(lpr),
      tid(0), start(ceph_clock_now())
  {}
  void finish(int r) override {
    if (r == -ECANCELED)
      return;
    std::scoped_lock locker{*pg};
    if (last_peering_reset == pg->get_last_peering_reset()) {
      pg->finish_flush(oid, tid, r);
      pg->osd->logger->tinc(l_osd_tier_flush_lat, ceph_clock_now() - start);
    }
  }
};

int PrimaryLogPG::start_flush(
  OpRequestRef op, ObjectContextRef obc,
  bool blocking, hobject_t *pmissing,
  std::optional<std::function<void()>> &&on_flush)
{
  const object_info_t& oi = obc->obs.oi;
  const hobject_t& soid = oi.soid;
  dout(10) << __func__ << " " << soid
	   << " v" << oi.version
	   << " uv" << oi.user_version
	   << " " << (blocking ? "blocking" : "non-blocking/best-effort")
	   << dendl;

  bool preoctopus_compat =
    get_osdmap()->require_osd_release < ceph_release_t::octopus;
  SnapSet snapset;
  if (preoctopus_compat) {
    // for pre-octopus compatibility, filter SnapSet::snaps.  not
    // certain we need this, but let's be conservative.
    snapset = obc->ssc->snapset.get_filtered(pool.info);
  } else {
    // NOTE: change this to a const ref when we remove this compat code
    snapset = obc->ssc->snapset;
  }

  // verify there are no (older) check for dirty clones
  {
    dout(20) << " snapset " << snapset << dendl;
    vector<snapid_t>::reverse_iterator p = snapset.clones.rbegin();
    while (p != snapset.clones.rend() && *p >= soid.snap)
      ++p;
    if (p != snapset.clones.rend()) {
      hobject_t next = soid;
      next.snap = *p;
      ceph_assert(next.snap < soid.snap);
      if (recovery_state.get_pg_log().get_missing().is_missing(next)) {
	dout(10) << __func__ << " missing clone is " << next << dendl;
	if (pmissing)
	  *pmissing = next;
	return -ENOENT;
      }
      ObjectContextRef older_obc = get_object_context(next, false);
      if (older_obc) {
	dout(20) << __func__ << " next oldest clone is " << older_obc->obs.oi
		 << dendl;
	if (older_obc->obs.oi.is_dirty()) {
	  dout(10) << __func__ << " next oldest clone is dirty: "
		   << older_obc->obs.oi << dendl;
	  return -EBUSY;
	}
      } else {
	dout(20) << __func__ << " next oldest clone " << next
		 << " is not present; implicitly clean" << dendl;
      }
    } else {
      dout(20) << __func__ << " no older clones" << dendl;
    }
  }

  if (blocking)
    obc->start_block();

  map<hobject_t,FlushOpRef>::iterator p = flush_ops.find(soid);
  if (p != flush_ops.end()) {
    FlushOpRef fop = p->second;
    if (fop->op == op) {
      // we couldn't take the write lock on a cache-try-flush before;
      // now we are trying again for the lock.
      return try_flush_mark_clean(fop);
    }
    if (fop->flushed_version == obc->obs.oi.user_version &&
	(fop->blocking || !blocking)) {
      // nonblocking can join anything
      // blocking can only join a blocking flush
      dout(20) << __func__ << " piggybacking on existing flush " << dendl;
      if (op)
	fop->dup_ops.push_back(op);
      return -EAGAIN;   // clean up this ctx; op will retry later
    }

    // cancel current flush since it will fail anyway, or because we
    // are blocking and the existing flush is nonblocking.
    dout(20) << __func__ << " canceling previous flush; it will fail" << dendl;
    if (fop->op)
      osd->reply_op_error(fop->op, -EBUSY);
    while (!fop->dup_ops.empty()) {
      osd->reply_op_error(fop->dup_ops.front(), -EBUSY);
      fop->dup_ops.pop_front();
    }
    vector<ceph_tid_t> tids;
    cancel_flush(fop, false, &tids);
    osd->objecter->op_cancel(tids, -ECANCELED);
  }

  if (obc->obs.oi.has_manifest() && obc->obs.oi.manifest.is_chunked()) {
    int r = start_manifest_flush(op, obc, blocking, std::move(on_flush));
    if (r != -EINPROGRESS) {
      if (blocking)
	obc->stop_block();
    }
    return r;
  }

  /**
   * In general, we need to send a delete and a copyfrom.
   * Consider snapc 10:[10, 9, 8, 4, 3, 2]:[10(10, 9), 4(4,3,2)]
   * where 4 is marked as clean.  To flush 10, we have to:
   * 1) delete 4:[4,3,2] -- Logically, the object does not exist after 4
   * 2) copyfrom 8:[8,4,3,2] -- flush object after snap 8
   *
   * There is a complicating case.  Supposed there had been a clone 7
   * for snaps [7, 6] which has been trimmed since they no longer exist.
   * In the base pool, we'd have 5:[4,3,2]:[4(4,3,2)]+head.  When we submit
   * the delete, the snap will be promoted to 5, and the head will become
   * a whiteout.  When the copy-from goes through, we'll end up with
   * 8:[8,4,3,2]:[4(4,3,2)]+head.
   *
   * Another complication is the case where there is an interval change
   * after doing the delete and the flush but before marking the object
   * clean.  We'll happily delete head and then recreate it at the same
   * sequence number, which works out ok.
   */

  SnapContext snapc, dsnapc;
  if (snapset.seq != 0) {
    if (soid.snap == CEPH_NOSNAP) {
      snapc = snapset.get_ssc_as_of(snapset.seq);
    } else {
      snapid_t min_included_snap;
      auto p = snapset.clone_snaps.find(soid.snap);
      ceph_assert(p != snapset.clone_snaps.end());
      min_included_snap = p->second.back();
      snapc = snapset.get_ssc_as_of(min_included_snap - 1);
    }

    snapid_t prev_snapc = 0;
    for (vector<snapid_t>::reverse_iterator citer = snapset.clones.rbegin();
	 citer != snapset.clones.rend();
	 ++citer) {
      if (*citer < soid.snap) {
	prev_snapc = *citer;
	break;
      }
    }

    dsnapc = snapset.get_ssc_as_of(prev_snapc);
  }

  object_locator_t base_oloc(soid);
  base_oloc.pool = pool.info.tier_of;

  if (dsnapc.seq < snapc.seq) {
    ObjectOperation o;
    o.remove();
    osd->objecter->mutate(
      soid.oid,
      base_oloc,
      o,
      dsnapc,
      ceph::real_clock::from_ceph_timespec(oi.mtime),
      (CEPH_OSD_FLAG_IGNORE_OVERLAY |
       CEPH_OSD_FLAG_ENFORCE_SNAPC),
      NULL /* no callback, we'll rely on the ordering w.r.t the next op */);
  }

  FlushOpRef fop(std::make_shared<FlushOp>());
  fop->obc = obc;
  fop->flushed_version = oi.user_version;
  fop->blocking = blocking;
  fop->on_flush = std::move(on_flush);
  fop->op = op;

  ObjectOperation o;
  if (oi.is_whiteout()) {
    fop->removal = true;
    o.remove();
  } else {
    object_locator_t oloc(soid);
    o.copy_from(soid.oid.name, soid.snap, oloc, oi.user_version,
		CEPH_OSD_COPY_FROM_FLAG_FLUSH |
		CEPH_OSD_COPY_FROM_FLAG_IGNORE_OVERLAY |
		CEPH_OSD_COPY_FROM_FLAG_IGNORE_CACHE |
		CEPH_OSD_COPY_FROM_FLAG_MAP_SNAP_CLONE,
		LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL|LIBRADOS_OP_FLAG_FADVISE_NOCACHE);

    //mean the base tier don't cache data after this
    if (agent_state && agent_state->evict_mode != TierAgentState::EVICT_MODE_FULL)
      o.set_last_op_flags(LIBRADOS_OP_FLAG_FADVISE_DONTNEED);
  }
  C_Flush *fin = new C_Flush(this, soid, get_last_peering_reset());

  ceph_tid_t tid = osd->objecter->mutate(
    soid.oid, base_oloc, o, snapc,
    ceph::real_clock::from_ceph_timespec(oi.mtime),
    CEPH_OSD_FLAG_IGNORE_OVERLAY | CEPH_OSD_FLAG_ENFORCE_SNAPC,
    new C_OnFinisher(fin,
		     osd->get_objecter_finisher(get_pg_shard())));
  /* we're under the pg lock and fin->finish() is grabbing that */
  fin->tid = tid;
  fop->objecter_tid = tid;

  flush_ops[soid] = fop;

  recovery_state.update_stats(
    [&oi](auto &history, auto &stats) {
      stats.stats.sum.num_flush++;
      stats.stats.sum.num_flush_kb += shift_round_up(oi.size, 10);
      return false;
    });
  return -EINPROGRESS;
}

void PrimaryLogPG::finish_flush(hobject_t oid, ceph_tid_t tid, int r)
{
  dout(10) << __func__ << " " << oid << " tid " << tid
	   << " " << cpp_strerror(r) << dendl;
  map<hobject_t,FlushOpRef>::iterator p = flush_ops.find(oid);
  if (p == flush_ops.end()) {
    dout(10) << __func__ << " no flush_op found" << dendl;
    return;
  }
  FlushOpRef fop = p->second;
  if (tid != fop->objecter_tid && !fop->obc->obs.oi.has_manifest()) {
    dout(10) << __func__ << " tid " << tid << " != fop " << fop
	     << " tid " << fop->objecter_tid << dendl;
    return;
  }
  ObjectContextRef obc = fop->obc;
  fop->objecter_tid = 0;

  if (r < 0 && !(r == -ENOENT && fop->removal)) {
    if (fop->op)
      osd->reply_op_error(fop->op, -EBUSY);
    if (fop->blocking) {
      obc->stop_block();
      kick_object_context_blocked(obc);
    }

    if (!fop->dup_ops.empty()) {
      dout(20) << __func__ << " requeueing dups" << dendl;
      requeue_ops(fop->dup_ops);
    }
    if (fop->on_flush) {
      (*(fop->on_flush))();
      fop->on_flush = std::nullopt;
    }
    flush_ops.erase(oid);
    return;
  }

  r = try_flush_mark_clean(fop);
  if (r == -EBUSY && fop->op) {
    osd->reply_op_error(fop->op, r);
  }
}

int PrimaryLogPG::try_flush_mark_clean(FlushOpRef fop)
{
  ObjectContextRef obc = fop->obc;
  const hobject_t& oid = obc->obs.oi.soid;

  if (fop->blocking) {
    obc->stop_block();
    kick_object_context_blocked(obc);
  }

  if (fop->flushed_version != obc->obs.oi.user_version ||
      !obc->obs.exists) {
    if (obc->obs.exists)
      dout(10) << __func__ << " flushed_version " << fop->flushed_version
	       << " != current " << obc->obs.oi.user_version
	       << dendl;
    else
      dout(10) << __func__ << " object no longer exists" << dendl;

    if (!fop->dup_ops.empty()) {
      dout(20) << __func__ << " requeueing dups" << dendl;
      requeue_ops(fop->dup_ops);
    }
    if (fop->on_flush) {
      (*(fop->on_flush))();
      fop->on_flush = std::nullopt;
    }
    flush_ops.erase(oid);
    if (fop->blocking)
      osd->logger->inc(l_osd_tier_flush_fail);
    else
      osd->logger->inc(l_osd_tier_try_flush_fail);
    return -EBUSY;
  }

  if (!fop->blocking &&
      write_blocked_by_scrub(oid)) {
    if (fop->op) {
      dout(10) << __func__ << " blocked by scrub" << dendl;
      requeue_op(fop->op);
      requeue_ops(fop->dup_ops);
      return -EAGAIN;    // will retry
    } else {
      osd->logger->inc(l_osd_tier_try_flush_fail);
      vector<ceph_tid_t> tids;
      cancel_flush(fop, false, &tids);
      osd->objecter->op_cancel(tids, -ECANCELED);
      return -ECANCELED;
    }
  }

  // successfully flushed, can we evict this object?
  if (!obc->obs.oi.has_manifest() && !fop->op &&
      agent_state && agent_state->evict_mode != TierAgentState::EVICT_MODE_IDLE &&
      agent_maybe_evict(obc, true)) {
    osd->logger->inc(l_osd_tier_clean);
    if (fop->on_flush) {
      (*(fop->on_flush))();
      fop->on_flush = std::nullopt;
    }
    flush_ops.erase(oid);
    return 0;
  }

  dout(10) << __func__ << " clearing DIRTY flag for " << oid << dendl;
  OpContextUPtr ctx = simple_opc_create(fop->obc);

  // successfully flushed; can we clear the dirty bit?
  // try to take the lock manually, since we don't
  // have a ctx yet.
  if (ctx->lock_manager.get_lock_type(
	RWState::RWWRITE,
	oid,
	obc,
	fop->op)) {
    dout(20) << __func__ << " took write lock" << dendl;
  } else if (fop->op) {
    dout(10) << __func__ << " waiting on write lock " << fop->op << " "
	     << fop->dup_ops << dendl;
    // fop->op is now waiting on the lock; get fop->dup_ops to wait too.
    for (auto op : fop->dup_ops) {
      bool locked = ctx->lock_manager.get_lock_type(
	RWState::RWWRITE,
	oid,
	obc,
	op);
      ceph_assert(!locked);
    }
    close_op_ctx(ctx.release());
    return -EAGAIN;    // will retry
  } else {
    dout(10) << __func__ << " failed write lock, no op; failing" << dendl;
    close_op_ctx(ctx.release());
    osd->logger->inc(l_osd_tier_try_flush_fail);
    vector<ceph_tid_t> tids;
    cancel_flush(fop, false, &tids);
    osd->objecter->op_cancel(tids, -ECANCELED);
    return -ECANCELED;
  }

  if (fop->on_flush) {
    ctx->register_on_finish(*(fop->on_flush));
    fop->on_flush = std::nullopt;
  }

  ctx->at_version = get_next_version();

  ctx->new_obs = obc->obs;
  ctx->new_obs.oi.clear_flag(object_info_t::FLAG_DIRTY);
  --ctx->delta_stats.num_objects_dirty;
  if (fop->obc->obs.oi.has_manifest()) {
    ceph_assert(obc->obs.oi.manifest.is_chunked());
    PGTransaction* t = ctx->op_t.get();
    uint64_t chunks_size = 0;
    for (auto &p : ctx->new_obs.oi.manifest.chunk_map) {
      chunks_size += p.second.length;
    }
    if (ctx->new_obs.oi.is_omap() && pool.info.supports_omap()) {
      t->omap_clear(oid);
      ctx->new_obs.oi.clear_omap_digest();
      ctx->new_obs.oi.clear_flag(object_info_t::FLAG_OMAP);
      ctx->clean_regions.mark_omap_dirty();
    }
    if (obc->obs.oi.size == chunks_size) { 
      t->truncate(oid, 0);
      interval_set<uint64_t> trim;
      trim.insert(0, ctx->new_obs.oi.size);
      ctx->modified_ranges.union_of(trim);
      truncate_update_size_and_usage(ctx->delta_stats,
				     ctx->new_obs.oi,
				     0);
      ctx->clean_regions.mark_data_region_dirty(0, ctx->new_obs.oi.size);
      ctx->new_obs.oi.new_object();
      for (auto &p : ctx->new_obs.oi.manifest.chunk_map) {
	p.second.clear_flag(chunk_info_t::FLAG_DIRTY);
	p.second.set_flag(chunk_info_t::FLAG_MISSING);
      }
    } else {
      for (auto &p : ctx->new_obs.oi.manifest.chunk_map) {
	if (p.second.is_dirty()) {
	  dout(20) << __func__ << " offset: " << p.second.offset 
		  << " length: " << p.second.length << dendl;
	  p.second.clear_flag(chunk_info_t::FLAG_DIRTY);
	  p.second.clear_flag(chunk_info_t::FLAG_MISSING); // CLEAN
	}
      }
    }
  }

  finish_ctx(ctx.get(), pg_log_entry_t::CLEAN);

  osd->logger->inc(l_osd_tier_clean);

  if (!fop->dup_ops.empty() || fop->op) {
    dout(20) << __func__ << " requeueing for " << ctx->at_version << dendl;
    list<OpRequestRef> ls;
    if (fop->op)
      ls.push_back(fop->op);
    ls.splice(ls.end(), fop->dup_ops);
    requeue_ops(ls);
  }

  simple_opc_submit(std::move(ctx));

  flush_ops.erase(oid);

  if (fop->blocking)
    osd->logger->inc(l_osd_tier_flush);
  else
    osd->logger->inc(l_osd_tier_try_flush);

  return -EINPROGRESS;
}

void PrimaryLogPG::cancel_flush(FlushOpRef fop, bool requeue,
				vector<ceph_tid_t> *tids)
{
  dout(10) << __func__ << " " << fop->obc->obs.oi.soid << " tid "
	   << fop->objecter_tid << dendl;
  if (fop->objecter_tid) {
    tids->push_back(fop->objecter_tid);
    fop->objecter_tid = 0;
  }
  if (fop->io_tids.size()) {
    for (auto &p : fop->io_tids) {
      tids->push_back(p.second);
      p.second = 0;
    } 
  }
  if (fop->blocking && fop->obc->is_blocked()) {
    fop->obc->stop_block();
    kick_object_context_blocked(fop->obc);
  }
  if (requeue) {
    if (fop->op)
      requeue_op(fop->op);
    requeue_ops(fop->dup_ops);
  }
  if (fop->on_flush) {
    (*(fop->on_flush))();
    fop->on_flush = std::nullopt;
  }
  flush_ops.erase(fop->obc->obs.oi.soid);
}

void PrimaryLogPG::cancel_flush_ops(bool requeue, vector<ceph_tid_t> *tids)
{
  dout(10) << __func__ << dendl;
  map<hobject_t,FlushOpRef>::iterator p = flush_ops.begin();
  while (p != flush_ops.end()) {
    cancel_flush((p++)->second, requeue, tids);
  }
}

bool PrimaryLogPG::is_present_clone(hobject_t coid)
{
  if (!pool.info.allow_incomplete_clones())
    return true;
  if (is_missing_object(coid))
    return true;
  ObjectContextRef obc = get_object_context(coid, false);
  return obc && obc->obs.exists;
}

// ========================================================================
// rep op gather

class C_OSD_RepopCommit : public Context {
  PrimaryLogPGRef pg;
  boost::intrusive_ptr<PrimaryLogPG::RepGather> repop;
public:
  C_OSD_RepopCommit(PrimaryLogPG *pg, PrimaryLogPG::RepGather *repop)
    : pg(pg), repop(repop) {}
  void finish(int) override {
    pg->repop_all_committed(repop.get());
  }
};

void PrimaryLogPG::repop_all_committed(RepGather *repop)
{
  dout(10) << __func__ << ": repop tid " << repop->rep_tid << " all committed "
	   << dendl;
  repop->all_committed = true;
  if (!repop->rep_aborted) {
    if (repop->v != eversion_t()) {
      recovery_state.complete_write(repop->v, repop->pg_local_last_complete);
    }
    eval_repop(repop);
  }
}

void PrimaryLogPG::op_applied(const eversion_t &applied_version)
{
  dout(10) << "op_applied version " << applied_version << dendl;
  ceph_assert(applied_version != eversion_t());
  ceph_assert(applied_version <= info.last_update);
  recovery_state.local_write_applied(applied_version);
  if (is_primary()) {
    if (scrubber.active) {
      if (recovery_state.get_last_update_applied() >=
	scrubber.subset_last_update) {
	requeue_scrub(ops_blocked_by_scrub());
      }
    } else {
      ceph_assert(scrubber.start == scrubber.end);
    }
  }
}

void PrimaryLogPG::eval_repop(RepGather *repop)
{
  dout(10) << "eval_repop " << *repop
    << (repop->op && repop->op->get_req<MOSDOp>() ? "" : " (no op)") << dendl;

  // ondisk?
  if (repop->all_committed) {
    dout(10) << " commit: " << *repop << dendl;
    for (auto p = repop->on_committed.begin();
	 p != repop->on_committed.end();
	 repop->on_committed.erase(p++)) {
      (*p)();
    }
    // send dup commits, in order
    auto it = waiting_for_ondisk.find(repop->v);
    if (it != waiting_for_ondisk.end()) {
      ceph_assert(waiting_for_ondisk.begin()->first == repop->v);
      for (auto& i : it->second) {
        int return_code = repop->r;
        if (return_code >= 0) {
          return_code = std::get<2>(i);
        }
        osd->reply_op_error(std::get<0>(i), return_code, repop->v,
                            std::get<1>(i), std::get<3>(i));
      }
      waiting_for_ondisk.erase(it);
    }

    publish_stats_to_osd();

    dout(10) << " removing " << *repop << dendl;
    ceph_assert(!repop_queue.empty());
    dout(20) << "   q front is " << *repop_queue.front() << dendl; 
    if (repop_queue.front() == repop) {
      RepGather *to_remove = nullptr;
      while (!repop_queue.empty() &&
	     (to_remove = repop_queue.front())->all_committed) {
	repop_queue.pop_front();
	for (auto p = to_remove->on_success.begin();
	     p != to_remove->on_success.end();
	     to_remove->on_success.erase(p++)) {
	  (*p)();
	}
	remove_repop(to_remove);
      }
    }
  }
}

void PrimaryLogPG::issue_repop(RepGather *repop, OpContext *ctx)
{
  FUNCTRACE(cct);
  const hobject_t& soid = ctx->obs->oi.soid;
  dout(7) << "issue_repop rep_tid " << repop->rep_tid
          << " o " << soid
          << dendl;

  repop->v = ctx->at_version;

  ctx->op_t->add_obc(ctx->obc);
  if (ctx->clone_obc) {
    ctx->op_t->add_obc(ctx->clone_obc);
  }
  if (ctx->head_obc) {
    ctx->op_t->add_obc(ctx->head_obc);
  }

  Context *on_all_commit = new C_OSD_RepopCommit(this, repop);
  if (!(ctx->log.empty())) {
    ceph_assert(ctx->at_version >= projected_last_update);
    projected_last_update = ctx->at_version;
  }
  for (auto &&entry: ctx->log) {
    projected_log.add(entry);
  }

  recovery_state.pre_submit_op(
    soid,
    ctx->log,
    ctx->at_version);
  pgbackend->submit_transaction(
    soid,
    ctx->delta_stats,
    ctx->at_version,
    std::move(ctx->op_t),
    recovery_state.get_pg_trim_to(),
    recovery_state.get_min_last_complete_ondisk(),
    std::move(ctx->log),
    ctx->updated_hset_history,
    on_all_commit,
    repop->rep_tid,
    ctx->reqid,
    ctx->op);
}

PrimaryLogPG::RepGather *PrimaryLogPG::new_repop(
  OpContext *ctx, ObjectContextRef obc,
  ceph_tid_t rep_tid)
{
  if (ctx->op)
    dout(10) << "new_repop rep_tid " << rep_tid << " on " << *ctx->op->get_req() << dendl;
  else
    dout(10) << "new_repop rep_tid " << rep_tid << " (no op)" << dendl;

  RepGather *repop = new RepGather(
    ctx, rep_tid, info.last_complete);

  repop->start = ceph_clock_now();

  repop_queue.push_back(&repop->queue_item);
  repop->get();

  osd->logger->inc(l_osd_op_wip);

  dout(10) << __func__ << ": " << *repop << dendl;
  return repop;
}

boost::intrusive_ptr<PrimaryLogPG::RepGather> PrimaryLogPG::new_repop(
  eversion_t version,
  int r,
  ObcLockManager &&manager,
  OpRequestRef &&op,
  std::optional<std::function<void(void)> > &&on_complete)
{
  RepGather *repop = new RepGather(
    std::move(manager),
    std::move(op),
    std::move(on_complete),
    osd->get_tid(),
    info.last_complete,
    r);
  repop->v = version;

  repop->start = ceph_clock_now();

  repop_queue.push_back(&repop->queue_item);

  osd->logger->inc(l_osd_op_wip);

  dout(10) << __func__ << ": " << *repop << dendl;
  return boost::intrusive_ptr<RepGather>(repop);
}
 
void PrimaryLogPG::remove_repop(RepGather *repop)
{
  dout(20) << __func__ << " " << *repop << dendl;

  for (auto p = repop->on_finish.begin();
       p != repop->on_finish.end();
       repop->on_finish.erase(p++)) {
    (*p)();
  }

  release_object_locks(
    repop->lock_manager);
  repop->put();

  osd->logger->dec(l_osd_op_wip);
}

PrimaryLogPG::OpContextUPtr PrimaryLogPG::simple_opc_create(ObjectContextRef obc)
{
  dout(20) << __func__ << " " << obc->obs.oi.soid << dendl;
  ceph_tid_t rep_tid = osd->get_tid();
  osd_reqid_t reqid(osd->get_cluster_msgr_name(), 0, rep_tid);
  OpContextUPtr ctx(new OpContext(OpRequestRef(), reqid, nullptr, obc, this));
  ctx->op_t.reset(new PGTransaction());
  ctx->mtime = ceph_clock_now();
  return ctx;
}

void PrimaryLogPG::simple_opc_submit(OpContextUPtr ctx)
{
  RepGather *repop = new_repop(ctx.get(), ctx->obc, ctx->reqid.tid);
  dout(20) << __func__ << " " << repop << dendl;
  issue_repop(repop, ctx.get());
  eval_repop(repop);
  recovery_state.update_trim_to();
  repop->put();
}


void PrimaryLogPG::submit_log_entries(
  const mempool::osd_pglog::list<pg_log_entry_t> &entries,
  ObcLockManager &&manager,
  std::optional<std::function<void(void)> > &&_on_complete,
  OpRequestRef op,
  int r)
{
  dout(10) << __func__ << " " << entries << dendl;
  ceph_assert(is_primary());

  eversion_t version;
  if (!entries.empty()) {
    ceph_assert(entries.rbegin()->version >= projected_last_update);
    version = projected_last_update = entries.rbegin()->version;
  }

  boost::intrusive_ptr<RepGather> repop;
  std::optional<std::function<void(void)> > on_complete;
  if (get_osdmap()->require_osd_release >= ceph_release_t::jewel) {
    repop = new_repop(
      version,
      r,
      std::move(manager),
      std::move(op),
      std::move(_on_complete));
  } else {
    on_complete = std::move(_on_complete);
  }

  pgbackend->call_write_ordered(
    [this, entries, repop, on_complete]() {
      ObjectStore::Transaction t;
      eversion_t old_last_update = info.last_update;
      recovery_state.merge_new_log_entries(
	entries, t, recovery_state.get_pg_trim_to(),
	recovery_state.get_min_last_complete_ondisk());

      set<pg_shard_t> waiting_on;
      for (set<pg_shard_t>::const_iterator i = get_acting_recovery_backfill().begin();
	   i != get_acting_recovery_backfill().end();
	   ++i) {
	pg_shard_t peer(*i);
	if (peer == pg_whoami) continue;
	ceph_assert(recovery_state.get_peer_missing().count(peer));
	ceph_assert(recovery_state.has_peer_info(peer));
	if (get_osdmap()->require_osd_release >= ceph_release_t::jewel) {
	  ceph_assert(repop);
	  MOSDPGUpdateLogMissing *m = new MOSDPGUpdateLogMissing(
	    entries,
	    spg_t(info.pgid.pgid, i->shard),
	    pg_whoami.shard,
	    get_osdmap_epoch(),
	    get_last_peering_reset(),
	    repop->rep_tid,
	    recovery_state.get_pg_trim_to(),
	    recovery_state.get_min_last_complete_ondisk());
	  osd->send_message_osd_cluster(
	    peer.osd, m, get_osdmap_epoch());
	  waiting_on.insert(peer);
	} else {
	  MOSDPGLog *m = new MOSDPGLog(
	    peer.shard, pg_whoami.shard,
	    info.last_update.epoch,
	    info, get_last_peering_reset());
	  m->log.log = entries;
	  m->log.tail = old_last_update;
	  m->log.head = info.last_update;
	  osd->send_message_osd_cluster(
	    peer.osd, m, get_osdmap_epoch());
	}
      }
      ceph_tid_t rep_tid = repop->rep_tid;
      waiting_on.insert(pg_whoami);
      log_entry_update_waiting_on.insert(
	make_pair(
	  rep_tid,
	  LogUpdateCtx{std::move(repop), std::move(waiting_on)}
	  ));
      struct OnComplete : public Context {
	PrimaryLogPGRef pg;
	ceph_tid_t rep_tid;
	epoch_t epoch;
	OnComplete(
	  PrimaryLogPGRef pg,
	  ceph_tid_t rep_tid,
	  epoch_t epoch)
	  : pg(pg), rep_tid(rep_tid), epoch(epoch) {}
	void finish(int) override {
	  std::scoped_lock l{*pg};
	  if (!pg->pg_has_reset_since(epoch)) {
	    auto it = pg->log_entry_update_waiting_on.find(rep_tid);
	    ceph_assert(it != pg->log_entry_update_waiting_on.end());
	    auto it2 = it->second.waiting_on.find(pg->pg_whoami);
	    ceph_assert(it2 != it->second.waiting_on.end());
	    it->second.waiting_on.erase(it2);
	    if (it->second.waiting_on.empty()) {
	      pg->repop_all_committed(it->second.repop.get());
	      pg->log_entry_update_waiting_on.erase(it);
	    }
	  }
	}
      };
      t.register_on_commit(
	new OnComplete{this, rep_tid, get_osdmap_epoch()});
      int r = osd->store->queue_transaction(ch, std::move(t), NULL);
      ceph_assert(r == 0);
      op_applied(info.last_update);
    });

  recovery_state.update_trim_to();
}

void PrimaryLogPG::cancel_log_updates()
{
  // get rid of all the LogUpdateCtx so their references to repops are
  // dropped
  log_entry_update_waiting_on.clear();
}

// -------------------------------------------------------

void PrimaryLogPG::get_watchers(list<obj_watch_item_t> *ls)
{
  std::scoped_lock l{*this};
  pair<hobject_t, ObjectContextRef> i;
  while (object_contexts.get_next(i.first, &i)) {
    ObjectContextRef obc(i.second);
    get_obc_watchers(obc, *ls);
  }
}

void PrimaryLogPG::get_obc_watchers(ObjectContextRef obc, list<obj_watch_item_t> &pg_watchers)
{
  for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator j =
	 obc->watchers.begin();
	j != obc->watchers.end();
	++j) {
    obj_watch_item_t owi;

    owi.obj = obc->obs.oi.soid;
    owi.wi.addr = j->second->get_peer_addr();
    owi.wi.name = j->second->get_entity();
    owi.wi.cookie = j->second->get_cookie();
    owi.wi.timeout_seconds = j->second->get_timeout();

    dout(30) << "watch: Found oid=" << owi.obj << " addr=" << owi.wi.addr
      << " name=" << owi.wi.name << " cookie=" << owi.wi.cookie << dendl;

    pg_watchers.push_back(owi);
  }
}

void PrimaryLogPG::check_blacklisted_watchers()
{
  dout(20) << "PrimaryLogPG::check_blacklisted_watchers for pg " << get_pgid() << dendl;
  pair<hobject_t, ObjectContextRef> i;
  while (object_contexts.get_next(i.first, &i))
    check_blacklisted_obc_watchers(i.second);
}

void PrimaryLogPG::check_blacklisted_obc_watchers(ObjectContextRef obc)
{
  dout(20) << "PrimaryLogPG::check_blacklisted_obc_watchers for obc " << obc->obs.oi.soid << dendl;
  for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator k =
	 obc->watchers.begin();
	k != obc->watchers.end();
	) {
    //Advance iterator now so handle_watch_timeout() can erase element
    map<pair<uint64_t, entity_name_t>, WatchRef>::iterator j = k++;
    dout(30) << "watch: Found " << j->second->get_entity() << " cookie " << j->second->get_cookie() << dendl;
    entity_addr_t ea = j->second->get_peer_addr();
    dout(30) << "watch: Check entity_addr_t " << ea << dendl;
    if (get_osdmap()->is_blacklisted(ea)) {
      dout(10) << "watch: Found blacklisted watcher for " << ea << dendl;
      ceph_assert(j->second->get_pg() == this);
      j->second->unregister_cb();
      handle_watch_timeout(j->second);
    }
  }
}

void PrimaryLogPG::populate_obc_watchers(ObjectContextRef obc)
{
  ceph_assert(is_primary() && is_active());
  auto it_objects = recovery_state.get_pg_log().get_log().objects.find(obc->obs.oi.soid);
  ceph_assert((recovering.count(obc->obs.oi.soid) ||
	  !is_missing_object(obc->obs.oi.soid)) ||
	 (it_objects != recovery_state.get_pg_log().get_log().objects.end() && // or this is a revert... see recover_primary()
	  it_objects->second->op ==
	    pg_log_entry_t::LOST_REVERT &&
	  it_objects->second->reverting_to ==
	    obc->obs.oi.version));

  dout(10) << "populate_obc_watchers " << obc->obs.oi.soid << dendl;
  ceph_assert(obc->watchers.empty());
  // populate unconnected_watchers
  for (map<pair<uint64_t, entity_name_t>, watch_info_t>::iterator p =
	obc->obs.oi.watchers.begin();
       p != obc->obs.oi.watchers.end();
       ++p) {
    utime_t expire = info.stats.last_became_active;
    expire += p->second.timeout_seconds;
    dout(10) << "  unconnected watcher " << p->first << " will expire " << expire << dendl;
    WatchRef watch(
      Watch::makeWatchRef(
	this, osd, obc, p->second.timeout_seconds, p->first.first,
	p->first.second, p->second.addr));
    watch->disconnect();
    obc->watchers.insert(
      make_pair(
	make_pair(p->first.first, p->first.second),
	watch));
  }
  // Look for watchers from blacklisted clients and drop
  check_blacklisted_obc_watchers(obc);
}

void PrimaryLogPG::handle_watch_timeout(WatchRef watch)
{
  ObjectContextRef obc = watch->get_obc(); // handle_watch_timeout owns this ref
  dout(10) << "handle_watch_timeout obc " << obc << dendl;

  if (!is_active()) {
    dout(10) << "handle_watch_timeout not active, no-op" << dendl;
    return;
  }
  if (!obc->obs.exists) {
    dout(10) << __func__ << " object " << obc->obs.oi.soid << " dne" << dendl;
    return;
  }
  if (is_degraded_or_backfilling_object(obc->obs.oi.soid)) {
    callbacks_for_degraded_object[obc->obs.oi.soid].push_back(
      watch->get_delayed_cb()
      );
    dout(10) << "handle_watch_timeout waiting for degraded on obj "
	     << obc->obs.oi.soid
	     << dendl;
    return;
  }

  if (write_blocked_by_scrub(obc->obs.oi.soid)) {
    dout(10) << "handle_watch_timeout waiting for scrub on obj "
	     << obc->obs.oi.soid
	     << dendl;
    scrubber.add_callback(
      watch->get_delayed_cb() // This callback!
      );
    return;
  }

  OpContextUPtr ctx = simple_opc_create(obc);
  ctx->at_version = get_next_version();

  object_info_t& oi = ctx->new_obs.oi;
  oi.watchers.erase(make_pair(watch->get_cookie(),
			      watch->get_entity()));

  list<watch_disconnect_t> watch_disconnects = {
    watch_disconnect_t(watch->get_cookie(), watch->get_entity(), true)
  };
  ctx->register_on_success(
    [this, obc, watch_disconnects]() {
      complete_disconnect_watches(obc, watch_disconnects);
    });


  PGTransaction *t = ctx->op_t.get();
  ctx->log.push_back(pg_log_entry_t(pg_log_entry_t::MODIFY, obc->obs.oi.soid,
				    ctx->at_version,
				    oi.version,
				    0,
				    osd_reqid_t(), ctx->mtime, 0));

  oi.prior_version = obc->obs.oi.version;
  oi.version = ctx->at_version;
  bufferlist bl;
  encode(oi, bl, get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
  t->setattr(obc->obs.oi.soid, OI_ATTR, bl);

  // apply new object state.
  ctx->obc->obs = ctx->new_obs;

  // no ctx->delta_stats
  simple_opc_submit(std::move(ctx));
}

ObjectContextRef PrimaryLogPG::create_object_context(const object_info_t& oi,
						     SnapSetContext *ssc)
{
  ObjectContextRef obc(object_contexts.lookup_or_create(oi.soid));
  ceph_assert(obc->destructor_callback == NULL);
  obc->destructor_callback = new C_PG_ObjectContext(this, obc.get());  
  obc->obs.oi = oi;
  obc->obs.exists = false;
  obc->ssc = ssc;
  if (ssc)
    register_snapset_context(ssc);
  dout(10) << "create_object_context " << (void*)obc.get() << " " << oi.soid << " " << dendl;
  if (is_active())
    populate_obc_watchers(obc);
  return obc;
}

ObjectContextRef PrimaryLogPG::get_object_context(
  const hobject_t& soid,
  bool can_create,
  const map<string, bufferlist> *attrs)
{
  auto it_objects = recovery_state.get_pg_log().get_log().objects.find(soid);
  ceph_assert(
    attrs || !recovery_state.get_pg_log().get_missing().is_missing(soid) ||
    // or this is a revert... see recover_primary()
    (it_objects != recovery_state.get_pg_log().get_log().objects.end() &&
      it_objects->second->op ==
      pg_log_entry_t::LOST_REVERT));
  ObjectContextRef obc = object_contexts.lookup(soid);
  osd->logger->inc(l_osd_object_ctx_cache_total);
  if (obc) {
    osd->logger->inc(l_osd_object_ctx_cache_hit);
    dout(10) << __func__ << ": found obc in cache: " << obc
	     << dendl;
  } else {
    dout(10) << __func__ << ": obc NOT found in cache: " << soid << dendl;
    // check disk
    bufferlist bv;
    if (attrs) {
      auto it_oi = attrs->find(OI_ATTR);
      ceph_assert(it_oi != attrs->end());
      bv = it_oi->second;
    } else {
      int r = pgbackend->objects_get_attr(soid, OI_ATTR, &bv);
      if (r < 0) {
	if (!can_create) {
	  dout(10) << __func__ << ": no obc for soid "
		   << soid << " and !can_create"
		   << dendl;
	  return ObjectContextRef();   // -ENOENT!
	}

	dout(10) << __func__ << ": no obc for soid "
		 << soid << " but can_create"
		 << dendl;
	// new object.
	object_info_t oi(soid);
	SnapSetContext *ssc = get_snapset_context(
	  soid, true, 0, false);
        ceph_assert(ssc);
	obc = create_object_context(oi, ssc);
	dout(10) << __func__ << ": " << obc << " " << soid
		 << " " << obc->rwstate
		 << " oi: " << obc->obs.oi
		 << " ssc: " << obc->ssc
		 << " snapset: " << obc->ssc->snapset << dendl;
	return obc;
      }
    }

    object_info_t oi;
    try {
      bufferlist::const_iterator bliter = bv.begin();
      decode(oi, bliter);
    } catch (...) {
      dout(0) << __func__ << ": obc corrupt: " << soid << dendl;
      return ObjectContextRef();   // -ENOENT!
    }

    ceph_assert(oi.soid.pool == (int64_t)info.pgid.pool());

    obc = object_contexts.lookup_or_create(oi.soid);
    obc->destructor_callback = new C_PG_ObjectContext(this, obc.get());
    obc->obs.oi = oi;
    obc->obs.exists = true;

    obc->ssc = get_snapset_context(
      soid, true,
      soid.has_snapset() ? attrs : 0);

    if (is_primary() && is_active())
      populate_obc_watchers(obc);

    if (pool.info.is_erasure()) {
      if (attrs) {
	obc->attr_cache = *attrs;
      } else {
	int r = pgbackend->objects_get_attrs(
	  soid,
	  &obc->attr_cache);
	ceph_assert(r == 0);
      }
    }

    dout(10) << __func__ << ": creating obc from disk: " << obc
	     << dendl;
  }

  // XXX: Caller doesn't expect this
  if (obc->ssc == NULL) {
    derr << __func__ << ": obc->ssc not available, not returning context" << dendl;
    return ObjectContextRef();   // -ENOENT!
  }

  dout(10) << __func__ << ": " << obc << " " << soid
	   << " " << obc->rwstate
	   << " oi: " << obc->obs.oi
	   << " exists: " << (int)obc->obs.exists
	   << " ssc: " << obc->ssc
	   << " snapset: " << obc->ssc->snapset << dendl;
  return obc;
}

void PrimaryLogPG::context_registry_on_change()
{
  pair<hobject_t, ObjectContextRef> i;
  while (object_contexts.get_next(i.first, &i)) {
    ObjectContextRef obc(i.second);
    if (obc) {
      for (map<pair<uint64_t, entity_name_t>, WatchRef>::iterator j =
	     obc->watchers.begin();
	   j != obc->watchers.end();
	   obc->watchers.erase(j++)) {
	j->second->discard();
      }
    }
  }
}


/*
 * If we return an error, and set *pmissing, then promoting that
 * object may help.
 *
 * If we return -EAGAIN, we will always set *pmissing to the missing
 * object to wait for.
 *
 * If we return an error but do not set *pmissing, then we know the
 * object does not exist.
 */
int PrimaryLogPG::find_object_context(const hobject_t& oid,
				      ObjectContextRef *pobc,
				      bool can_create,
				      bool map_snapid_to_clone,
				      hobject_t *pmissing)
{
  FUNCTRACE(cct);
  ceph_assert(oid.pool == static_cast<int64_t>(info.pgid.pool()));
  // want the head?
  if (oid.snap == CEPH_NOSNAP) {
    ObjectContextRef obc = get_object_context(oid, can_create);
    if (!obc) {
      if (pmissing)
        *pmissing = oid;
      return -ENOENT;
    }
    dout(10) << __func__ << " " << oid
       << " @" << oid.snap
       << " oi=" << obc->obs.oi
       << dendl;
    *pobc = obc;

    return 0;
  }

  // we want a snap

  hobject_t head = oid.get_head();
  SnapSetContext *ssc = get_snapset_context(oid, can_create);
  if (!ssc || !(ssc->exists || can_create)) {
    dout(20) << __func__ << " " << oid << " no snapset" << dendl;
    if (pmissing)
      *pmissing = head;  // start by getting the head
    if (ssc)
      put_snapset_context(ssc);
    return -ENOENT;
  }

  if (map_snapid_to_clone) {
    dout(10) << __func__ << " " << oid << " @" << oid.snap
	     << " snapset " << ssc->snapset
	     << " map_snapid_to_clone=true" << dendl;
    if (oid.snap > ssc->snapset.seq) {
      // already must be readable
      ObjectContextRef obc = get_object_context(head, false);
      dout(10) << __func__ << " " << oid << " @" << oid.snap
	       << " snapset " << ssc->snapset
	       << " maps to head" << dendl;
      *pobc = obc;
      put_snapset_context(ssc);
      return (obc && obc->obs.exists) ? 0 : -ENOENT;
    } else {
      vector<snapid_t>::const_iterator citer = std::find(
	ssc->snapset.clones.begin(),
	ssc->snapset.clones.end(),
	oid.snap);
      if (citer == ssc->snapset.clones.end()) {
	dout(10) << __func__ << " " << oid << " @" << oid.snap
		 << " snapset " << ssc->snapset
		 << " maps to nothing" << dendl;
	put_snapset_context(ssc);
	return -ENOENT;
      }

      dout(10) << __func__ << " " << oid << " @" << oid.snap
	       << " snapset " << ssc->snapset
	       << " maps to " << oid << dendl;

      if (recovery_state.get_pg_log().get_missing().is_missing(oid)) {
	dout(10) << __func__ << " " << oid << " @" << oid.snap
		 << " snapset " << ssc->snapset
		 << " " << oid << " is missing" << dendl;
	if (pmissing)
	  *pmissing = oid;
	put_snapset_context(ssc);
	return -EAGAIN;
      }

      ObjectContextRef obc = get_object_context(oid, false);
      if (!obc || !obc->obs.exists) {
	dout(10) << __func__ << " " << oid << " @" << oid.snap
		 << " snapset " << ssc->snapset
		 << " " << oid << " is not present" << dendl;
	if (pmissing)
	  *pmissing = oid;
	put_snapset_context(ssc);
	return -ENOENT;
      }
      dout(10) << __func__ << " " << oid << " @" << oid.snap
	       << " snapset " << ssc->snapset
	       << " " << oid << " HIT" << dendl;
      *pobc = obc;
      put_snapset_context(ssc);
      return 0;
    }
    ceph_abort(); //unreachable
  }

  dout(10) << __func__ << " " << oid << " @" << oid.snap
	   << " snapset " << ssc->snapset << dendl;
 
  // head?
  if (oid.snap > ssc->snapset.seq) {
    ObjectContextRef obc = get_object_context(head, false);
    dout(10) << __func__ << " " << head
	     << " want " << oid.snap << " > snapset seq " << ssc->snapset.seq
	     << " -- HIT " << obc->obs
	     << dendl;
    if (!obc->ssc)
      obc->ssc = ssc;
    else {
      ceph_assert(ssc == obc->ssc);
      put_snapset_context(ssc);
    }
    *pobc = obc;
    return 0;
  }

  // which clone would it be?
  unsigned k = 0;
  while (k < ssc->snapset.clones.size() &&
	 ssc->snapset.clones[k] < oid.snap)
    k++;
  if (k == ssc->snapset.clones.size()) {
    dout(10) << __func__ << " no clones with last >= oid.snap "
	     << oid.snap << " -- DNE" << dendl;
    put_snapset_context(ssc);
    return -ENOENT;
  }
  hobject_t soid(oid.oid, oid.get_key(), ssc->snapset.clones[k], oid.get_hash(),
		 info.pgid.pool(), oid.get_namespace());

  if (recovery_state.get_pg_log().get_missing().is_missing(soid)) {
    dout(20) << __func__ << " " << soid << " missing, try again later"
	     << dendl;
    if (pmissing)
      *pmissing = soid;
    put_snapset_context(ssc);
    return -EAGAIN;
  }

  ObjectContextRef obc = get_object_context(soid, false);
  if (!obc || !obc->obs.exists) {
    if (pmissing)
      *pmissing = soid;
    put_snapset_context(ssc);
    if (is_primary()) {
      if (is_degraded_or_backfilling_object(soid)) {
	dout(20) << __func__ << " clone is degraded or backfilling " << soid << dendl;
	return -EAGAIN;
      } else if (is_degraded_on_async_recovery_target(soid)) {
	dout(20) << __func__ << " clone is recovering " << soid << dendl;
	return -EAGAIN;
      } else {
	dout(20) << __func__ << " missing clone " << soid << dendl;
	return -ENOENT;
      }
    } else {
      dout(20) << __func__ << " replica missing clone" << soid << dendl;
      return -ENOENT;
    }
  }

  if (!obc->ssc) {
    obc->ssc = ssc;
  } else {
    ceph_assert(obc->ssc == ssc);
    put_snapset_context(ssc);
  }
  ssc = 0;

  // clone
  dout(20) << __func__ << " " << soid
	   << " snapset " << obc->ssc->snapset
	   << dendl;
  snapid_t first, last;
  auto p = obc->ssc->snapset.clone_snaps.find(soid.snap);
  ceph_assert(p != obc->ssc->snapset.clone_snaps.end());
  if (p->second.empty()) {
    dout(1) << __func__ << " " << soid << " empty snapset -- DNE" << dendl;
    ceph_assert(!cct->_conf->osd_debug_verify_snaps);
    return -ENOENT;
  }
  if (std::find(p->second.begin(), p->second.end(), oid.snap) ==
      p->second.end()) {
    dout(20) << __func__ << " " << soid << " clone_snaps " << p->second
	     << " does not contain " << oid.snap << " -- DNE" << dendl;
    return -ENOENT;
  }
  if (get_osdmap()->in_removed_snaps_queue(info.pgid.pgid.pool(), oid.snap)) {
    dout(20) << __func__ << " " << soid << " snap " << oid.snap
	     << " in removed_snaps_queue" << " -- DNE" << dendl;
    return -ENOENT;
  }
  dout(20) << __func__ << " " << soid << " clone_snaps " << p->second
	   << " contains " << oid.snap << " -- HIT " << obc->obs << dendl;
  *pobc = obc;
  return 0;
}

void PrimaryLogPG::object_context_destructor_callback(ObjectContext *obc)
{
  if (obc->ssc)
    put_snapset_context(obc->ssc);
}

void PrimaryLogPG::add_object_context_to_pg_stat(ObjectContextRef obc, pg_stat_t *pgstat)
{
  object_info_t& oi = obc->obs.oi;

  dout(10) << __func__ << " " << oi.soid << dendl;
  ceph_assert(!oi.soid.is_snapdir());

  object_stat_sum_t stat;
  stat.num_objects++;
  if (oi.is_dirty())
    stat.num_objects_dirty++;
  if (oi.is_whiteout())
    stat.num_whiteouts++;
  if (oi.is_omap())
    stat.num_objects_omap++;
  if (oi.is_cache_pinned())
    stat.num_objects_pinned++;
  if (oi.has_manifest())
    stat.num_objects_manifest++;

  if (oi.soid.is_snap()) {
    stat.num_object_clones++;

    if (!obc->ssc)
      obc->ssc = get_snapset_context(oi.soid, false);
    ceph_assert(obc->ssc);
    stat.num_bytes += obc->ssc->snapset.get_clone_bytes(oi.soid.snap);
  } else {
    stat.num_bytes += oi.size;
  }

  // add it in
  pgstat->stats.sum.add(stat);
}

void PrimaryLogPG::kick_object_context_blocked(ObjectContextRef obc)
{
  const hobject_t& soid = obc->obs.oi.soid;
  if (obc->is_blocked()) {
    dout(10) << __func__ << " " << soid << " still blocked" << dendl;
    return;
  }

  map<hobject_t, list<OpRequestRef>>::iterator p = waiting_for_blocked_object.find(soid);
  if (p != waiting_for_blocked_object.end()) {
    list<OpRequestRef>& ls = p->second;
    dout(10) << __func__ << " " << soid << " requeuing " << ls.size() << " requests" << dendl;
    requeue_ops(ls);
    waiting_for_blocked_object.erase(p);
  }

  map<hobject_t, ObjectContextRef>::iterator i =
    objects_blocked_on_snap_promotion.find(obc->obs.oi.soid.get_head());
  if (i != objects_blocked_on_snap_promotion.end()) {
    ceph_assert(i->second == obc);
    objects_blocked_on_snap_promotion.erase(i);
  }

  if (obc->requeue_scrub_on_unblock) {
    obc->requeue_scrub_on_unblock = false;
    // only requeue if we are still active: we may be unblocking
    // because we are resetting for a new peering interval
    if (is_active()) {
      requeue_scrub();
    }
  }
}

SnapSetContext *PrimaryLogPG::get_snapset_context(
  const hobject_t& oid,
  bool can_create,
  const map<string, bufferlist> *attrs,
  bool oid_existed)
{
  std::lock_guard l(snapset_contexts_lock);
  SnapSetContext *ssc;
  map<hobject_t, SnapSetContext*>::iterator p = snapset_contexts.find(
    oid.get_snapdir());
  if (p != snapset_contexts.end()) {
    if (can_create || p->second->exists) {
      ssc = p->second;
    } else {
      return NULL;
    }
  } else {
    bufferlist bv;
    if (!attrs) {
      int r = -ENOENT;
      if (!(oid.is_head() && !oid_existed)) {
	r = pgbackend->objects_get_attr(oid.get_head(), SS_ATTR, &bv);
      }
      if (r < 0 && !can_create)
	return NULL;
    } else {
      auto it_ss = attrs->find(SS_ATTR);
      ceph_assert(it_ss != attrs->end());
      bv = it_ss->second;
    }
    ssc = new SnapSetContext(oid.get_snapdir());
    _register_snapset_context(ssc);
    if (bv.length()) {
      bufferlist::const_iterator bvp = bv.begin();
      try {
	ssc->snapset.decode(bvp);
      } catch (const ceph::buffer::error& e) {
        dout(0) << __func__ << " Can't decode snapset: " << e.what() << dendl;
	return NULL;
      }
      ssc->exists = true;
    } else {
      ssc->exists = false;
    }
  }
  ceph_assert(ssc);
  ssc->ref++;
  return ssc;
}

void PrimaryLogPG::put_snapset_context(SnapSetContext *ssc)
{
  std::lock_guard l(snapset_contexts_lock);
  --ssc->ref;
  if (ssc->ref == 0) {
    if (ssc->registered)
      snapset_contexts.erase(ssc->oid);
    delete ssc;
  }
}

/*
 * Return values:
 *  NONE  - didn't pull anything
 *  YES   - pulled what the caller wanted
 *  HEAD  - needed to pull head first
 */
enum { PULL_NONE, PULL_HEAD, PULL_YES };

int PrimaryLogPG::recover_missing(
  const hobject_t &soid, eversion_t v,
  int priority,
  PGBackend::RecoveryHandle *h)
{
  if (recovery_state.get_missing_loc().is_unfound(soid)) {
    dout(7) << __func__ << " " << soid
	    << " v " << v 
	    << " but it is unfound" << dendl;
    return PULL_NONE;
  }

  if (recovery_state.get_missing_loc().is_deleted(soid)) {
    start_recovery_op(soid);
    ceph_assert(!recovering.count(soid));
    recovering.insert(make_pair(soid, ObjectContextRef()));
    epoch_t cur_epoch = get_osdmap_epoch();
    remove_missing_object(soid, v, new LambdaContext(
     [=](int) {
       std::scoped_lock locker{*this};
       if (!pg_has_reset_since(cur_epoch)) {
	 bool object_missing = false;
	 for (const auto& shard : get_acting_recovery_backfill()) {
	   if (shard == pg_whoami)
	     continue;
	   if (recovery_state.get_peer_missing(shard).is_missing(soid)) {
	     dout(20) << __func__ << ": soid " << soid << " needs to be deleted from replica " << shard << dendl;
	     object_missing = true;
	     break;
	   }
	 }
	 if (!object_missing) {
	   object_stat_sum_t stat_diff;
	   stat_diff.num_objects_recovered = 1;
	   if (scrub_after_recovery)
	     stat_diff.num_objects_repaired = 1;
	   on_global_recover(soid, stat_diff, true);
	 } else {
	   auto recovery_handle = pgbackend->open_recovery_op();
	   pgbackend->recover_delete_object(soid, v, recovery_handle);
	   pgbackend->run_recovery_op(recovery_handle, priority);
	 }
       }
     }));
    return PULL_YES;
  }

  // is this a snapped object?  if so, consult the snapset.. we may not need the entire object!
  ObjectContextRef obc;
  ObjectContextRef head_obc;
  if (soid.snap && soid.snap < CEPH_NOSNAP) {
    // do we have the head?
    hobject_t head = soid.get_head();
    if (recovery_state.get_pg_log().get_missing().is_missing(head)) {
      if (recovering.count(head)) {
	dout(10) << " missing but already recovering head " << head << dendl;
	return PULL_NONE;
      } else {
	int r = recover_missing(
	  head, recovery_state.get_pg_log().get_missing().get_items().find(head)->second.need, priority,
	  h);
	if (r != PULL_NONE)
	  return PULL_HEAD;
	return PULL_NONE;
      }
    }
    head_obc = get_object_context(
      head,
      false,
      0);
    ceph_assert(head_obc);
  }
  start_recovery_op(soid);
  ceph_assert(!recovering.count(soid));
  recovering.insert(make_pair(soid, obc));
  int r = pgbackend->recover_object(
    soid,
    v,
    head_obc,
    obc,
    h);
  // This is only a pull which shouldn't return an error
  ceph_assert(r >= 0);
  return PULL_YES;
}

void PrimaryLogPG::remove_missing_object(const hobject_t &soid,
					 eversion_t v, Context *on_complete)
{
  dout(20) << __func__ << " " << soid << " " << v << dendl;
  ceph_assert(on_complete != nullptr);
  // delete locally
  ObjectStore::Transaction t;
  remove_snap_mapped_object(t, soid);

  ObjectRecoveryInfo recovery_info;
  recovery_info.soid = soid;
  recovery_info.version = v;

  epoch_t cur_epoch = get_osdmap_epoch();
  t.register_on_complete(new LambdaContext(
     [=](int) {
       std::unique_lock locker{*this};
       if (!pg_has_reset_since(cur_epoch)) {
	 ObjectStore::Transaction t2;
	 on_local_recover(soid, recovery_info, ObjectContextRef(), true, &t2);
	 t2.register_on_complete(on_complete);
	 int r = osd->store->queue_transaction(ch, std::move(t2), nullptr);
	 ceph_assert(r == 0);
	 locker.unlock();
       } else {
	 locker.unlock();
	 on_complete->complete(-EAGAIN);
       }
     }));
  int r = osd->store->queue_transaction(ch, std::move(t), nullptr);
  ceph_assert(r == 0);
}

void PrimaryLogPG::finish_degraded_object(const hobject_t oid)
{
  dout(10) << __func__ << " " << oid << dendl;
  if (callbacks_for_degraded_object.count(oid)) {
    list<Context*> contexts;
    contexts.swap(callbacks_for_degraded_object[oid]);
    callbacks_for_degraded_object.erase(oid);
    for (list<Context*>::iterator i = contexts.begin();
	 i != contexts.end();
	 ++i) {
      (*i)->complete(0);
    }
  }
  map<hobject_t, snapid_t>::iterator i = objects_blocked_on_degraded_snap.find(
    oid.get_head());
  if (i != objects_blocked_on_degraded_snap.end() &&
      i->second == oid.snap)
    objects_blocked_on_degraded_snap.erase(i);
}

void PrimaryLogPG::_committed_pushed_object(
  epoch_t epoch, eversion_t last_complete)
{
  std::scoped_lock locker{*this};
  if (!pg_has_reset_since(epoch)) {
    recovery_state.recovery_committed_to(last_complete);
  } else {
    dout(10) << __func__
	     << " pg has changed, not touching last_complete_ondisk" << dendl;
  }
}

void PrimaryLogPG::_applied_recovered_object(ObjectContextRef obc)
{
  dout(20) << __func__ << dendl;
  if (obc) {
    dout(20) << "obc = " << *obc << dendl;
  }
  ceph_assert(active_pushes >= 1);
  --active_pushes;

  // requeue an active chunky scrub waiting on recovery ops
  if (!recovery_state.is_deleting() && active_pushes == 0
      && scrubber.is_chunky_scrub_active()) {
    requeue_scrub(ops_blocked_by_scrub());
  }
}

void PrimaryLogPG::_applied_recovered_object_replica()
{
  dout(20) << __func__ << dendl;
  ceph_assert(active_pushes >= 1);
  --active_pushes;

  // requeue an active chunky scrub waiting on recovery ops
  if (!recovery_state.is_deleting() && active_pushes == 0 &&
      scrubber.active_rep_scrub && static_cast<const MOSDRepScrub*>(
	scrubber.active_rep_scrub->get_req())->chunky) {
    auto& op = scrubber.active_rep_scrub;
    osd->enqueue_back(
      OpSchedulerItem(
        unique_ptr<OpSchedulerItem::OpQueueable>(new PGOpItem(info.pgid, op)),
	op->get_req()->get_cost(),
	op->get_req()->get_priority(),
	op->get_req()->get_recv_stamp(),
	op->get_req()->get_source().num(),
	get_osdmap_epoch()));
    scrubber.active_rep_scrub.reset();
  }
}

void PrimaryLogPG::on_failed_pull(
  const set<pg_shard_t> &from,
  const hobject_t &soid,
  const eversion_t &v)
{
  dout(20) << __func__ << ": " << soid << dendl;
  ceph_assert(recovering.count(soid));
  auto obc = recovering[soid];
  if (obc) {
    list<OpRequestRef> blocked_ops;
    obc->drop_recovery_read(&blocked_ops);
    requeue_ops(blocked_ops);
  }
  recovering.erase(soid);
  for (auto&& i : from) {
    if (i != pg_whoami) { // we'll get it below in primary_error
      recovery_state.force_object_missing(i, soid, v);
    }
  }

  dout(0) << __func__ << " " << soid << " from shard " << from
	  << ", reps on " << recovery_state.get_missing_loc().get_locations(soid)
	  << " unfound? " << recovery_state.get_missing_loc().is_unfound(soid)
	  << dendl;
  finish_recovery_op(soid);  // close out this attempt,
  finish_degraded_object(soid);

  if (from.count(pg_whoami)) {
    dout(0) << " primary missing oid " << soid << " version " << v << dendl;
    primary_error(soid, v);
    backfills_in_flight.erase(soid);
  }
}

eversion_t PrimaryLogPG::pick_newest_available(const hobject_t& oid)
{
  eversion_t v;
  pg_missing_item pmi;
  bool is_missing = recovery_state.get_pg_log().get_missing().is_missing(oid, &pmi);
  ceph_assert(is_missing);
  v = pmi.have;
  dout(10) << "pick_newest_available " << oid << " " << v << " on osd." << osd->whoami << " (local)" << dendl;

  ceph_assert(!get_acting_recovery_backfill().empty());
  for (set<pg_shard_t>::iterator i = get_acting_recovery_backfill().begin();
       i != get_acting_recovery_backfill().end();
       ++i) {
    if (*i == get_primary()) continue;
    pg_shard_t peer = *i;
    if (!recovery_state.get_peer_missing(peer).is_missing(oid)) {
      continue;
    }
    eversion_t h = recovery_state.get_peer_missing(peer).get_items().at(oid).have;
    dout(10) << "pick_newest_available " << oid << " " << h << " on osd." << peer << dendl;
    if (h > v)
      v = h;
  }

  dout(10) << "pick_newest_available " << oid << " " << v << " (newest)" << dendl;
  return v;
}

void PrimaryLogPG::do_update_log_missing(OpRequestRef &op)
{
  const MOSDPGUpdateLogMissing *m = static_cast<const MOSDPGUpdateLogMissing*>(
    op->get_req());
  ceph_assert(m->get_type() == MSG_OSD_PG_UPDATE_LOG_MISSING);
  ObjectStore::Transaction t;
  std::optional<eversion_t> op_trim_to, op_roll_forward_to;
  if (m->pg_trim_to != eversion_t())
    op_trim_to = m->pg_trim_to;
  if (m->pg_roll_forward_to != eversion_t())
    op_roll_forward_to = m->pg_roll_forward_to;

  dout(20) << __func__
	   << " op_trim_to = " << op_trim_to << " op_roll_forward_to = " << op_roll_forward_to << dendl;

  recovery_state.append_log_entries_update_missing(
    m->entries, t, op_trim_to, op_roll_forward_to);
  eversion_t new_lcod = info.last_complete;

  Context *complete = new LambdaContext(
    [=](int) {
      const MOSDPGUpdateLogMissing *msg = static_cast<const MOSDPGUpdateLogMissing*>(
	op->get_req());
      std::scoped_lock locker{*this};
      if (!pg_has_reset_since(msg->get_epoch())) {
	update_last_complete_ondisk(new_lcod);
	MOSDPGUpdateLogMissingReply *reply =
	  new MOSDPGUpdateLogMissingReply(
	    spg_t(info.pgid.pgid, primary_shard().shard),
	    pg_whoami.shard,
	    msg->get_epoch(),
	    msg->min_epoch,
	    msg->get_tid(),
	    new_lcod);
	reply->set_priority(CEPH_MSG_PRIO_HIGH);
	msg->get_connection()->send_message(reply);
      }
    });

  if (get_osdmap()->require_osd_release >= ceph_release_t::kraken) {
    t.register_on_commit(complete);
  } else {
    /* Hack to work around the fact that ReplicatedBackend sends
     * ack+commit if commit happens first
     *
     * This behavior is no longer necessary, but we preserve it so old
     * primaries can keep their repops in order */
    if (pool.info.is_erasure()) {
      t.register_on_complete(complete);
    } else {
      t.register_on_commit(complete);
    }
  }
  int tr = osd->store->queue_transaction(
    ch,
    std::move(t),
    nullptr);
  ceph_assert(tr == 0);
  op_applied(info.last_update);
}

void PrimaryLogPG::do_update_log_missing_reply(OpRequestRef &op)
{
  const MOSDPGUpdateLogMissingReply *m =
    static_cast<const MOSDPGUpdateLogMissingReply*>(
    op->get_req());
  dout(20) << __func__ << " got reply from "
	   << m->get_from() << dendl;

  auto it = log_entry_update_waiting_on.find(m->get_tid());
  if (it != log_entry_update_waiting_on.end()) {
    if (it->second.waiting_on.count(m->get_from())) {
      it->second.waiting_on.erase(m->get_from());
      if (m->last_complete_ondisk != eversion_t()) {
	update_peer_last_complete_ondisk(m->get_from(), m->last_complete_ondisk);
      }
    } else {
      osd->clog->error()
	<< info.pgid << " got reply "
	<< *m << " from shard we are not waiting for "
	<< m->get_from();
    }

    if (it->second.waiting_on.empty()) {
      repop_all_committed(it->second.repop.get());
      log_entry_update_waiting_on.erase(it);
    }
  } else {
    osd->clog->error()
      << info.pgid << " got reply "
      << *m << " on unknown tid " << m->get_tid();
  }
}

/* Mark all unfound objects as lost.
 */
void PrimaryLogPG::mark_all_unfound_lost(
  int what,
  std::function<void(int,const std::string&,bufferlist&)> on_finish)
{
  dout(3) << __func__ << " " << pg_log_entry_t::get_op_name(what) << dendl;
  list<hobject_t> oids;

  dout(30) << __func__ << ": log before:\n";
  recovery_state.get_pg_log().get_log().print(*_dout);
  *_dout << dendl;

  mempool::osd_pglog::list<pg_log_entry_t> log_entries;

  utime_t mtime = ceph_clock_now();
  map<hobject_t, pg_missing_item>::const_iterator m =
    recovery_state.get_missing_loc().get_needs_recovery().begin();
  map<hobject_t, pg_missing_item>::const_iterator mend =
    recovery_state.get_missing_loc().get_needs_recovery().end();

  ObcLockManager manager;
  eversion_t v = get_next_version();
  v.epoch = get_osdmap_epoch();
  uint64_t num_unfound = recovery_state.get_missing_loc().num_unfound();
  while (m != mend) {
    const hobject_t &oid(m->first);
    if (!recovery_state.get_missing_loc().is_unfound(oid)) {
      // We only care about unfound objects
      ++m;
      continue;
    }

    ObjectContextRef obc;
    eversion_t prev;

    switch (what) {
    case pg_log_entry_t::LOST_MARK:
      ceph_abort_msg("actually, not implemented yet!");
      break;

    case pg_log_entry_t::LOST_REVERT:
      prev = pick_newest_available(oid);
      if (prev > eversion_t()) {
	// log it
	pg_log_entry_t e(
	  pg_log_entry_t::LOST_REVERT, oid, v,
	  m->second.need, 0, osd_reqid_t(), mtime, 0);
	e.reverting_to = prev;
	e.mark_unrollbackable();
	log_entries.push_back(e);
	dout(10) << e << dendl;

	// we are now missing the new version; recovery code will sort it out.
	++v.version;
	++m;
	break;
      }

    case pg_log_entry_t::LOST_DELETE:
      {
	pg_log_entry_t e(pg_log_entry_t::LOST_DELETE, oid, v, m->second.need,
			 0, osd_reqid_t(), mtime, 0);
	if (get_osdmap()->require_osd_release >= ceph_release_t::jewel) {
	  if (pool.info.require_rollback()) {
	    e.mod_desc.try_rmobject(v.version);
	  } else {
	    e.mark_unrollbackable();
	  }
	} // otherwise, just do what we used to do
	dout(10) << e << dendl;
	log_entries.push_back(e);
        oids.push_back(oid);

	// If context found mark object as deleted in case
	// of racing with new creation.  This can happen if
	// object lost and EIO at primary.
	obc = object_contexts.lookup(oid);
	if (obc)
	  obc->obs.exists = false;

	++v.version;
	++m;
      }
      break;

    default:
      ceph_abort();
    }
  }

  recovery_state.update_stats(
    [](auto &history, auto &stats) {
      stats.stats_invalid = true;
      return false;
    });

  submit_log_entries(
    log_entries,
    std::move(manager),
    std::optional<std::function<void(void)> >(
      [this, oids, num_unfound, on_finish]() {
	if (recovery_state.perform_deletes_during_peering()) {
	  for (auto oid : oids) {
	    // clear old locations - merge_new_log_entries will have
	    // handled rebuilding missing_loc for each of these
	    // objects if we have the RECOVERY_DELETES flag
	    recovery_state.object_recovered(oid, object_stat_sum_t());
	  }
	}

	if (is_recovery_unfound()) {
	  queue_peering_event(
	    PGPeeringEventRef(
	      std::make_shared<PGPeeringEvent>(
	      get_osdmap_epoch(),
	      get_osdmap_epoch(),
	      PeeringState::DoRecovery())));
	} else if (is_backfill_unfound()) {
	  queue_peering_event(
	    PGPeeringEventRef(
	      std::make_shared<PGPeeringEvent>(
	      get_osdmap_epoch(),
	      get_osdmap_epoch(),
	      PeeringState::RequestBackfill())));
	} else {
	  queue_recovery();
	}

	stringstream ss;
	ss << "pg has " << num_unfound
	   << " objects unfound and apparently lost marking";
	string rs = ss.str();
	dout(0) << "do_command r=" << 0 << " " << rs << dendl;
	osd->clog->info() << rs;
	bufferlist empty;
	on_finish(0, rs, empty);
      }),
    OpRequestRef());
}

void PrimaryLogPG::_split_into(pg_t child_pgid, PG *child, unsigned split_bits)
{
  ceph_assert(repop_queue.empty());
}

/*
 * pg status change notification
 */

void PrimaryLogPG::apply_and_flush_repops(bool requeue)
{
  list<OpRequestRef> rq;

  // apply all repops
  while (!repop_queue.empty()) {
    RepGather *repop = repop_queue.front();
    repop_queue.pop_front();
    dout(10) << " canceling repop tid " << repop->rep_tid << dendl;
    repop->rep_aborted = true;
    repop->on_committed.clear();
    repop->on_success.clear();

    if (requeue) {
      if (repop->op) {
	dout(10) << " requeuing " << *repop->op->get_req() << dendl;
	rq.push_back(repop->op);
	repop->op = OpRequestRef();
      }

      // also requeue any dups, interleaved into position
      auto p = waiting_for_ondisk.find(repop->v);
      if (p != waiting_for_ondisk.end()) {
	dout(10) << " also requeuing ondisk waiters " << p->second << dendl;
	for (auto& i : p->second) {
	  rq.push_back(std::get<0>(i));
	}
	waiting_for_ondisk.erase(p);
      }
    }

    remove_repop(repop);
  }

  ceph_assert(repop_queue.empty());

  if (requeue) {
    requeue_ops(rq);
    if (!waiting_for_ondisk.empty()) {
      for (auto& i : waiting_for_ondisk) {
        for (auto& j : i.second) {
          derr << __func__ << ": op " << *(std::get<0>(j)->get_req())
               << " waiting on " << i.first << dendl;
        }
      }
      ceph_assert(waiting_for_ondisk.empty());
    }
  }

  waiting_for_ondisk.clear();
}

void PrimaryLogPG::on_flushed()
{
  requeue_ops(waiting_for_flush);
  if (!is_peered() || !is_primary()) {
    pair<hobject_t, ObjectContextRef> i;
    while (object_contexts.get_next(i.first, &i)) {
      derr << __func__ << ": object " << i.first << " obc still alive" << dendl;
    }
    ceph_assert(object_contexts.empty());
  }
}

void PrimaryLogPG::on_removal(ObjectStore::Transaction &t)
{
  dout(10) << __func__ << dendl;

  on_shutdown();

  t.register_on_commit(new C_DeleteMore(this, get_osdmap_epoch()));
}

void PrimaryLogPG::clear_async_reads()
{
  dout(10) << __func__ << dendl;
  for(auto& i : in_progress_async_reads) {
    dout(10) << "clear ctx: "
             << "OpRequestRef " << i.first
             << " OpContext " << i.second
             << dendl;
    close_op_ctx(i.second);
  }
}

void PrimaryLogPG::clear_cache()
{
  object_contexts.clear();
}

void PrimaryLogPG::on_shutdown()
{
  dout(10) << __func__ << dendl;

  if (recovery_queued) {
    recovery_queued = false;
    osd->clear_queued_recovery(this);
  }

  clear_scrub_reserved();
  scrub_clear_state();

  unreg_next_scrub();

  vector<ceph_tid_t> tids;
  cancel_copy_ops(false, &tids);
  cancel_flush_ops(false, &tids);
  cancel_proxy_ops(false, &tids);
  cancel_manifest_ops(false, &tids);
  osd->objecter->op_cancel(tids, -ECANCELED);

  apply_and_flush_repops(false);
  cancel_log_updates();
  // we must remove PGRefs, so do this this prior to release_backoffs() callers
  clear_backoffs(); 
  // clean up snap trim references
  snap_trimmer_machine.process_event(Reset());

  pgbackend->on_change();

  context_registry_on_change();
  object_contexts.clear();

  clear_async_reads();

  osd->remote_reserver.cancel_reservation(info.pgid);
  osd->local_reserver.cancel_reservation(info.pgid);

  clear_primary_state();
  cancel_recovery();

  if (is_primary()) {
    osd->clear_ready_to_merge(this);
  }
}

void PrimaryLogPG::on_activate_complete()
{
  check_local();
  // waiters
  if (!recovery_state.needs_flush()) {
    requeue_ops(waiting_for_peered);
  } else if (!waiting_for_peered.empty()) {
    dout(10) << __func__ << " flushes in progress, moving "
	     << waiting_for_peered.size()
	     << " items to waiting_for_flush"
	     << dendl;
    ceph_assert(waiting_for_flush.empty());
    waiting_for_flush.swap(waiting_for_peered);
  }


  // all clean?
  if (needs_recovery()) {
    dout(10) << "activate not all replicas are up-to-date, queueing recovery" << dendl;
    queue_peering_event(
      PGPeeringEventRef(
	std::make_shared<PGPeeringEvent>(
	  get_osdmap_epoch(),
	  get_osdmap_epoch(),
	  PeeringState::DoRecovery())));
  } else if (needs_backfill()) {
    dout(10) << "activate queueing backfill" << dendl;
    queue_peering_event(
      PGPeeringEventRef(
	std::make_shared<PGPeeringEvent>(
	  get_osdmap_epoch(),
	  get_osdmap_epoch(),
	  PeeringState::RequestBackfill())));
  } else {
    dout(10) << "activate all replicas clean, no recovery" << dendl;
    eio_errors_to_process = false;
    queue_peering_event(
      PGPeeringEventRef(
	std::make_shared<PGPeeringEvent>(
	  get_osdmap_epoch(),
	  get_osdmap_epoch(),
	  PeeringState::AllReplicasRecovered())));
  }

  publish_stats_to_osd();

  if (get_backfill_targets().size()) {
    last_backfill_started = recovery_state.earliest_backfill();
    new_backfill = true;
    ceph_assert(!last_backfill_started.is_max());
    dout(5) << __func__ << ": bft=" << get_backfill_targets()
	   << " from " << last_backfill_started << dendl;
    for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
	 i != get_backfill_targets().end();
	 ++i) {
      dout(5) << "target shard " << *i
	     << " from " << recovery_state.get_peer_info(*i).last_backfill
	     << dendl;
    }
  }

  hit_set_setup();
  agent_setup();
}

void PrimaryLogPG::on_change(ObjectStore::Transaction &t)
{
  dout(10) << __func__ << dendl;

  if (hit_set && hit_set->insert_count() == 0) {
    dout(20) << " discarding empty hit_set" << dendl;
    hit_set_clear();
  }

  if (recovery_queued) {
    recovery_queued = false;
    osd->clear_queued_recovery(this);
  }

  // requeue everything in the reverse order they should be
  // reexamined.
  requeue_ops(waiting_for_peered);
  requeue_ops(waiting_for_flush);
  requeue_ops(waiting_for_active);
  requeue_ops(waiting_for_readable);

  clear_scrub_reserved();

  vector<ceph_tid_t> tids;
  cancel_copy_ops(is_primary(), &tids);
  cancel_flush_ops(is_primary(), &tids);
  cancel_proxy_ops(is_primary(), &tids);
  cancel_manifest_ops(is_primary(), &tids);
  osd->objecter->op_cancel(tids, -ECANCELED);

  // requeue object waiters
  for (auto& p : waiting_for_unreadable_object) {
    release_backoffs(p.first);
  }
  if (is_primary()) {
    requeue_object_waiters(waiting_for_unreadable_object);
  } else {
    waiting_for_unreadable_object.clear();
  }
  for (map<hobject_t,list<OpRequestRef>>::iterator p = waiting_for_degraded_object.begin();
       p != waiting_for_degraded_object.end();
       waiting_for_degraded_object.erase(p++)) {
    release_backoffs(p->first);
    if (is_primary())
      requeue_ops(p->second);
    else
      p->second.clear();
    finish_degraded_object(p->first);
  }

  // requeues waiting_for_scrub
  scrub_clear_state();

  for (auto p = waiting_for_blocked_object.begin();
       p != waiting_for_blocked_object.end();
       waiting_for_blocked_object.erase(p++)) {
    if (is_primary())
      requeue_ops(p->second);
    else
      p->second.clear();
  }
  for (auto i = callbacks_for_degraded_object.begin();
       i != callbacks_for_degraded_object.end();
    ) {
    finish_degraded_object((i++)->first);
  }
  ceph_assert(callbacks_for_degraded_object.empty());

  if (is_primary()) {
    requeue_ops(waiting_for_cache_not_full);
  } else {
    waiting_for_cache_not_full.clear();
  }
  objects_blocked_on_cache_full.clear();

  for (list<pair<OpRequestRef, OpContext*> >::iterator i =
         in_progress_async_reads.begin();
       i != in_progress_async_reads.end();
       in_progress_async_reads.erase(i++)) {
    close_op_ctx(i->second);
    if (is_primary())
      requeue_op(i->first);
  }

  // this will requeue ops we were working on but didn't finish, and
  // any dups
  apply_and_flush_repops(is_primary());
  cancel_log_updates();

  // do this *after* apply_and_flush_repops so that we catch any newly
  // registered watches.
  context_registry_on_change();

  pgbackend->on_change_cleanup(&t);
  scrubber.cleanup_store(&t);
  pgbackend->on_change();

  // clear snap_trimmer state
  snap_trimmer_machine.process_event(Reset());

  debug_op_order.clear();
  unstable_stats.clear();

  // we don't want to cache object_contexts through the interval change
  // NOTE: we actually assert that all currently live references are dead
  // by the time the flush for the next interval completes.
  object_contexts.clear();

  // should have been cleared above by finishing all of the degraded objects
  ceph_assert(objects_blocked_on_degraded_snap.empty());
}

void PrimaryLogPG::plpg_on_role_change()
{
  dout(10) << __func__ << dendl;
  if (get_role() != 0 && hit_set) {
    dout(10) << " clearing hit set" << dendl;
    hit_set_clear();
  }
}

void PrimaryLogPG::plpg_on_pool_change()
{
  dout(10) << __func__ << dendl;
  // requeue cache full waiters just in case the cache_mode is
  // changing away from writeback mode.  note that if we are not
  // active the normal requeuing machinery is sufficient (and properly
  // ordered).
  if (is_active() &&
      pool.info.cache_mode != pg_pool_t::CACHEMODE_WRITEBACK &&
      !waiting_for_cache_not_full.empty()) {
    dout(10) << __func__ << " requeuing full waiters (not in writeback) "
	     << dendl;
    requeue_ops(waiting_for_cache_not_full);
    objects_blocked_on_cache_full.clear();
  }
  hit_set_setup();
  agent_setup();
}

// clear state.  called on recovery completion AND cancellation.
void PrimaryLogPG::_clear_recovery_state()
{
#ifdef DEBUG_RECOVERY_OIDS
  recovering_oids.clear();
#endif
  last_backfill_started = hobject_t();
  set<hobject_t>::iterator i = backfills_in_flight.begin();
  while (i != backfills_in_flight.end()) {
    ceph_assert(recovering.count(*i));
    backfills_in_flight.erase(i++);
  }

  list<OpRequestRef> blocked_ops;
  for (map<hobject_t, ObjectContextRef>::iterator i = recovering.begin();
       i != recovering.end();
       recovering.erase(i++)) {
    if (i->second) {
      i->second->drop_recovery_read(&blocked_ops);
      requeue_ops(blocked_ops);
    }
  }
  ceph_assert(backfills_in_flight.empty());
  pending_backfill_updates.clear();
  ceph_assert(recovering.empty());
  pgbackend->clear_recovery_state();
}

void PrimaryLogPG::cancel_pull(const hobject_t &soid)
{
  dout(20) << __func__ << ": " << soid << dendl;
  ceph_assert(recovering.count(soid));
  ObjectContextRef obc = recovering[soid];
  if (obc) {
    list<OpRequestRef> blocked_ops;
    obc->drop_recovery_read(&blocked_ops);
    requeue_ops(blocked_ops);
  }
  recovering.erase(soid);
  finish_recovery_op(soid);
  release_backoffs(soid);
  if (waiting_for_degraded_object.count(soid)) {
    dout(20) << " kicking degraded waiters on " << soid << dendl;
    requeue_ops(waiting_for_degraded_object[soid]);
    waiting_for_degraded_object.erase(soid);
  }
  if (waiting_for_unreadable_object.count(soid)) {
    dout(20) << " kicking unreadable waiters on " << soid << dendl;
    requeue_ops(waiting_for_unreadable_object[soid]);
    waiting_for_unreadable_object.erase(soid);
  }
  if (is_missing_object(soid))
    recovery_state.set_last_requested(0);
  finish_degraded_object(soid);
}

void PrimaryLogPG::check_recovery_sources(const OSDMapRef& osdmap)
{
  pgbackend->check_recovery_sources(osdmap);
}

bool PrimaryLogPG::start_recovery_ops(
  uint64_t max,
  ThreadPool::TPHandle &handle,
  uint64_t *ops_started)
{
  uint64_t& started = *ops_started;
  started = 0;
  bool work_in_progress = false;
  bool recovery_started = false;
  ceph_assert(is_primary());
  ceph_assert(is_peered());
  ceph_assert(!recovery_state.is_deleting());

  ceph_assert(recovery_queued);
  recovery_queued = false;

  if (!state_test(PG_STATE_RECOVERING) &&
      !state_test(PG_STATE_BACKFILLING)) {
    /* TODO: I think this case is broken and will make do_recovery()
     * unhappy since we're returning false */
    dout(10) << "recovery raced and were queued twice, ignoring!" << dendl;
    return have_unfound();
  }

  const auto &missing = recovery_state.get_pg_log().get_missing();

  uint64_t num_unfound = get_num_unfound();

  if (!recovery_state.have_missing()) {
    recovery_state.local_recovery_complete();
  }

  if (!missing.have_missing() || // Primary does not have missing
      // or all of the missing objects are unfound.
      recovery_state.all_missing_unfound()) {
    // Recover the replicas.
    started = recover_replicas(max, handle, &recovery_started);
  }
  if (!started) {
    // We still have missing objects that we should grab from replicas.
    started += recover_primary(max, handle);
  }
  if (!started && num_unfound != get_num_unfound()) {
    // second chance to recovery replicas
    started = recover_replicas(max, handle, &recovery_started);
  }

  if (started || recovery_started)
    work_in_progress = true;

  bool deferred_backfill = false;
  if (recovering.empty() &&
      state_test(PG_STATE_BACKFILLING) &&
      !get_backfill_targets().empty() && started < max &&
      missing.num_missing() == 0 &&
      waiting_on_backfill.empty()) {
    if (get_osdmap()->test_flag(CEPH_OSDMAP_NOBACKFILL)) {
      dout(10) << "deferring backfill due to NOBACKFILL" << dendl;
      deferred_backfill = true;
    } else if (get_osdmap()->test_flag(CEPH_OSDMAP_NOREBALANCE) &&
	       !is_degraded())  {
      dout(10) << "deferring backfill due to NOREBALANCE" << dendl;
      deferred_backfill = true;
    } else if (!recovery_state.is_backfill_reserved()) {
      /* DNMNOTE I think this branch is dead */
      dout(10) << "deferring backfill due to !backfill_reserved" << dendl;
      if (!backfill_reserving) {
	dout(10) << "queueing RequestBackfill" << dendl;
	backfill_reserving = true;
	queue_peering_event(
	  PGPeeringEventRef(
	    std::make_shared<PGPeeringEvent>(
	      get_osdmap_epoch(),
	      get_osdmap_epoch(),
	      PeeringState::RequestBackfill())));
      }
      deferred_backfill = true;
    } else {
      started += recover_backfill(max - started, handle, &work_in_progress);
    }
  }

  dout(10) << " started " << started << dendl;
  osd->logger->inc(l_osd_rop, started);

  if (!recovering.empty() ||
      work_in_progress || recovery_ops_active > 0 || deferred_backfill)
    return !work_in_progress && have_unfound();

  ceph_assert(recovering.empty());
  ceph_assert(recovery_ops_active == 0);

  dout(10) << __func__ << " needs_recovery: "
	   << recovery_state.get_missing_loc().get_needs_recovery()
	   << dendl;
  dout(10) << __func__ << " missing_loc: "
	   << recovery_state.get_missing_loc().get_missing_locs()
	   << dendl;
  int unfound = get_num_unfound();
  if (unfound) {
    dout(10) << " still have " << unfound << " unfound" << dendl;
    return true;
  }

  if (missing.num_missing() > 0) {
    // this shouldn't happen!
    osd->clog->error() << info.pgid << " Unexpected Error: recovery ending with "
		       << missing.num_missing() << ": " << missing.get_items();
    return false;
  }

  if (needs_recovery()) {
    // this shouldn't happen!
    // We already checked num_missing() so we must have missing replicas
    osd->clog->error() << info.pgid 
                       << " Unexpected Error: recovery ending with missing replicas";
    return false;
  }

  if (state_test(PG_STATE_RECOVERING)) {
    state_clear(PG_STATE_RECOVERING);
    state_clear(PG_STATE_FORCED_RECOVERY);
    if (needs_backfill()) {
      dout(10) << "recovery done, queuing backfill" << dendl;
      queue_peering_event(
        PGPeeringEventRef(
          std::make_shared<PGPeeringEvent>(
            get_osdmap_epoch(),
            get_osdmap_epoch(),
            PeeringState::RequestBackfill())));
    } else {
      dout(10) << "recovery done, no backfill" << dendl;
      eio_errors_to_process = false;
      state_clear(PG_STATE_FORCED_BACKFILL);
      queue_peering_event(
        PGPeeringEventRef(
          std::make_shared<PGPeeringEvent>(
            get_osdmap_epoch(),
            get_osdmap_epoch(),
            PeeringState::AllReplicasRecovered())));
    }
  } else { // backfilling
    state_clear(PG_STATE_BACKFILLING);
    state_clear(PG_STATE_FORCED_BACKFILL);
    state_clear(PG_STATE_FORCED_RECOVERY);
    dout(10) << "recovery done, backfill done" << dendl;
    eio_errors_to_process = false;
    queue_peering_event(
      PGPeeringEventRef(
        std::make_shared<PGPeeringEvent>(
          get_osdmap_epoch(),
          get_osdmap_epoch(),
          PeeringState::Backfilled())));
  }

  return false;
}

/**
 * do one recovery op.
 * return true if done, false if nothing left to do.
 */
uint64_t PrimaryLogPG::recover_primary(uint64_t max, ThreadPool::TPHandle &handle)
{
  ceph_assert(is_primary());

  const auto &missing = recovery_state.get_pg_log().get_missing();

  dout(10) << __func__ << " recovering " << recovering.size()
           << " in pg,"
           << " missing " << missing << dendl;

  dout(25) << __func__ << " " << missing.get_items() << dendl;

  // look at log!
  pg_log_entry_t *latest = 0;
  unsigned started = 0;
  int skipped = 0;

  PGBackend::RecoveryHandle *h = pgbackend->open_recovery_op();
  map<version_t, hobject_t>::const_iterator p =
    missing.get_rmissing().lower_bound(recovery_state.get_pg_log().get_log().last_requested);
  while (p != missing.get_rmissing().end()) {
    handle.reset_tp_timeout();
    hobject_t soid;
    version_t v = p->first;

    auto it_objects = recovery_state.get_pg_log().get_log().objects.find(p->second);
    if (it_objects != recovery_state.get_pg_log().get_log().objects.end()) {
      latest = it_objects->second;
      ceph_assert(latest->is_update() || latest->is_delete());
      soid = latest->soid;
    } else {
      latest = 0;
      soid = p->second;
    }
    const pg_missing_item& item = missing.get_items().find(p->second)->second;
    ++p;

    hobject_t head = soid.get_head();

    eversion_t need = item.need;

    dout(10) << __func__ << " "
             << soid << " " << item.need
	     << (missing.is_missing(soid) ? " (missing)":"")
	     << (missing.is_missing(head) ? " (missing head)":"")
             << (recovering.count(soid) ? " (recovering)":"")
	     << (recovering.count(head) ? " (recovering head)":"")
             << dendl;

    if (latest) {
      switch (latest->op) {
      case pg_log_entry_t::CLONE:
	/*
	 * Handling for this special case removed for now, until we
	 * can correctly construct an accurate SnapSet from the old
	 * one.
	 */
	break;

      case pg_log_entry_t::LOST_REVERT:
	{
	  if (item.have == latest->reverting_to) {
	    ObjectContextRef obc = get_object_context(soid, true);
	    
	    if (obc->obs.oi.version == latest->version) {
	      // I'm already reverting
	      dout(10) << " already reverting " << soid << dendl;
	    } else {
	      dout(10) << " reverting " << soid << " to " << latest->prior_version << dendl;
	      obc->obs.oi.version = latest->version;

	      ObjectStore::Transaction t;
	      bufferlist b2;
	      obc->obs.oi.encode(
		b2,
		get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
	      ceph_assert(!pool.info.require_rollback());
	      t.setattr(coll, ghobject_t(soid), OI_ATTR, b2);

	      recovery_state.recover_got(
		soid,
		latest->version,
		false,
		t);

	      ++active_pushes;

	      t.register_on_applied(new C_OSD_AppliedRecoveredObject(this, obc));
	      t.register_on_commit(new C_OSD_CommittedPushedObject(
				     this,
				     get_osdmap_epoch(),
				     info.last_complete));
	      osd->store->queue_transaction(ch, std::move(t));
	      continue;
	    }
	  } else {
	    /*
	     * Pull the old version of the object.  Update missing_loc here to have the location
	     * of the version we want.
	     *
	     * This doesn't use the usual missing_loc paths, but that's okay:
	     *  - if we have it locally, we hit the case above, and go from there.
	     *  - if we don't, we always pass through this case during recovery and set up the location
	     *    properly.
	     *  - this way we don't need to mangle the missing code to be general about needing an old
	     *    version...
	     */
	    eversion_t alternate_need = latest->reverting_to;
	    dout(10) << " need to pull prior_version " << alternate_need << " for revert " << item << dendl;

	    set<pg_shard_t> good_peers;
	    for (auto p = recovery_state.get_peer_missing().begin();
		 p != recovery_state.get_peer_missing().end();
		 ++p) {
	      if (p->second.is_missing(soid, need) &&
		  p->second.get_items().at(soid).have == alternate_need) {
		good_peers.insert(p->first);
	      }
	    }
	    recovery_state.set_revert_with_targets(
	      soid,
	      good_peers);
	    dout(10) << " will pull " << alternate_need << " or " << need
		     << " from one of "
		     << recovery_state.get_missing_loc().get_locations(soid)
		     << dendl;
	  }
	}
	break;
      }
    }
   
    if (!recovering.count(soid)) {
      if (recovering.count(head)) {
	++skipped;
      } else {
	int r = recover_missing(
	  soid, need, get_recovery_op_priority(), h);
	switch (r) {
	case PULL_YES:
	  ++started;
	  break;
	case PULL_HEAD:
	  ++started;
	case PULL_NONE:
	  ++skipped;
	  break;
	default:
	  ceph_abort();
	}
	if (started >= max)
	  break;
      }
    }
    
    // only advance last_requested if we haven't skipped anything
    if (!skipped)
      recovery_state.set_last_requested(v);
  }
 
  pgbackend->run_recovery_op(h, get_recovery_op_priority());
  return started;
}

bool PrimaryLogPG::primary_error(
  const hobject_t& soid, eversion_t v)
{
  recovery_state.force_object_missing(pg_whoami, soid, v);
  bool uhoh = recovery_state.get_missing_loc().is_unfound(soid);
  if (uhoh)
    osd->clog->error() << info.pgid << " missing primary copy of "
		       << soid << ", unfound";
  else
    osd->clog->error() << info.pgid << " missing primary copy of "
		       << soid
		       << ", will try copies on "
		       << recovery_state.get_missing_loc().get_locations(soid);
  return uhoh;
}

int PrimaryLogPG::prep_object_replica_deletes(
  const hobject_t& soid, eversion_t v,
  PGBackend::RecoveryHandle *h,
  bool *work_started)
{
  ceph_assert(is_primary());
  dout(10) << __func__ << ": on " << soid << dendl;

  ObjectContextRef obc = get_object_context(soid, false);
  if (obc) {
    if (!obc->get_recovery_read()) {
      dout(20) << "replica delete delayed on " << soid
	       << "; could not get rw_manager lock" << dendl;
      *work_started = true;
      return 0;
    } else {
      dout(20) << "replica delete got recovery read lock on " << soid
	       << dendl;
    }
  }

  start_recovery_op(soid);
  ceph_assert(!recovering.count(soid));
  if (!obc)
    recovering.insert(make_pair(soid, ObjectContextRef()));
  else
    recovering.insert(make_pair(soid, obc));

  pgbackend->recover_delete_object(soid, v, h);
  return 1;
}

int PrimaryLogPG::prep_object_replica_pushes(
  const hobject_t& soid, eversion_t v,
  PGBackend::RecoveryHandle *h,
  bool *work_started)
{
  ceph_assert(is_primary());
  dout(10) << __func__ << ": on " << soid << dendl;

  if (soid.snap && soid.snap < CEPH_NOSNAP) {
    // do we have the head and/or snapdir?
    hobject_t head = soid.get_head();
    if (recovery_state.get_pg_log().get_missing().is_missing(head)) {
      if (recovering.count(head)) {
	dout(10) << " missing but already recovering head " << head << dendl;
	return 0;
      } else {
	int r = recover_missing(
	    head, recovery_state.get_pg_log().get_missing().get_items().find(head)->second.need,
	    get_recovery_op_priority(), h);
	if (r != PULL_NONE)
	  return 1;
	return 0;
      }
    }
  }

  // NOTE: we know we will get a valid oloc off of disk here.
  ObjectContextRef obc = get_object_context(soid, false);
  if (!obc) {
    primary_error(soid, v);
    return 0;
  }

  if (!obc->get_recovery_read()) {
    dout(20) << "recovery delayed on " << soid
	     << "; could not get rw_manager lock" << dendl;
    *work_started = true;
    return 0;
  } else {
    dout(20) << "recovery got recovery read lock on " << soid
	     << dendl;
  }

  start_recovery_op(soid);
  ceph_assert(!recovering.count(soid));
  recovering.insert(make_pair(soid, obc));

  int r = pgbackend->recover_object(
    soid,
    v,
    ObjectContextRef(),
    obc, // has snapset context
    h);
  if (r < 0) {
    dout(0) << __func__ << " Error " << r << " on oid " << soid << dendl;
    on_failed_pull({ pg_whoami }, soid, v);
    return 0;
  }
  return 1;
}

uint64_t PrimaryLogPG::recover_replicas(uint64_t max, ThreadPool::TPHandle &handle,
  bool *work_started)
{
  dout(10) << __func__ << "(" << max << ")" << dendl;
  uint64_t started = 0;

  PGBackend::RecoveryHandle *h = pgbackend->open_recovery_op();

  // this is FAR from an optimal recovery order.  pretty lame, really.
  ceph_assert(!get_acting_recovery_backfill().empty());
  // choose replicas to recover, replica has the shortest missing list first
  // so we can bring it back to normal ASAP
  std::vector<std::pair<unsigned int, pg_shard_t>> replicas_by_num_missing,
    async_by_num_missing;
  replicas_by_num_missing.reserve(get_acting_recovery_backfill().size() - 1);
  for (auto &p: get_acting_recovery_backfill()) {
    if (p == get_primary()) {
      continue;
    }
    auto pm = recovery_state.get_peer_missing().find(p);
    ceph_assert(pm != recovery_state.get_peer_missing().end());
    auto nm = pm->second.num_missing();
    if (nm != 0) {
      if (is_async_recovery_target(p)) {
        async_by_num_missing.push_back(make_pair(nm, p));
      } else {
        replicas_by_num_missing.push_back(make_pair(nm, p));
      }
    }
  }
  // sort by number of missing objects, in ascending order.
  auto func = [](const std::pair<unsigned int, pg_shard_t> &lhs,
                 const std::pair<unsigned int, pg_shard_t> &rhs) {
    return lhs.first < rhs.first;
  };
  // acting goes first
  std::sort(replicas_by_num_missing.begin(), replicas_by_num_missing.end(), func);
  // then async_recovery_targets
  std::sort(async_by_num_missing.begin(), async_by_num_missing.end(), func);
  replicas_by_num_missing.insert(replicas_by_num_missing.end(),
    async_by_num_missing.begin(), async_by_num_missing.end());
  for (auto &replica: replicas_by_num_missing) {
    pg_shard_t &peer = replica.second;
    ceph_assert(peer != get_primary());
    auto pm = recovery_state.get_peer_missing().find(peer);
    ceph_assert(pm != recovery_state.get_peer_missing().end());
    size_t m_sz = pm->second.num_missing();

    dout(10) << " peer osd." << peer << " missing " << m_sz << " objects." << dendl;
    dout(20) << " peer osd." << peer << " missing " << pm->second.get_items() << dendl;

    // oldest first!
    const pg_missing_t &m(pm->second);
    for (map<version_t, hobject_t>::const_iterator p = m.get_rmissing().begin();
	 p != m.get_rmissing().end() && started < max;
	   ++p) {
      handle.reset_tp_timeout();
      const hobject_t soid(p->second);

      if (recovery_state.get_missing_loc().is_unfound(soid)) {
	dout(10) << __func__ << ": " << soid << " still unfound" << dendl;
	continue;
      }

      const pg_info_t &pi = recovery_state.get_peer_info(peer);
      if (soid > pi.last_backfill) {
	if (!recovering.count(soid)) {
          derr << __func__ << ": object " << soid << " last_backfill "
	       << pi.last_backfill << dendl;
	  derr << __func__ << ": object added to missing set for backfill, but "
	       << "is not in recovering, error!" << dendl;
	  ceph_abort();
	}
	continue;
      }

      if (recovering.count(soid)) {
	dout(10) << __func__ << ": already recovering " << soid << dendl;
	continue;
      }

      if (recovery_state.get_missing_loc().is_deleted(soid)) {
	dout(10) << __func__ << ": " << soid << " is a delete, removing" << dendl;
	map<hobject_t,pg_missing_item>::const_iterator r = m.get_items().find(soid);
	started += prep_object_replica_deletes(soid, r->second.need, h, work_started);
	continue;
      }

      if (soid.is_snap() &&
	  recovery_state.get_pg_log().get_missing().is_missing(
	    soid.get_head())) {
	dout(10) << __func__ << ": " << soid.get_head()
		 << " still missing on primary" << dendl;
	continue;
      }

      if (recovery_state.get_pg_log().get_missing().is_missing(soid)) {
	dout(10) << __func__ << ": " << soid << " still missing on primary" << dendl;
	continue;
      }

      dout(10) << __func__ << ": recover_object_replicas(" << soid << ")" << dendl;
      map<hobject_t,pg_missing_item>::const_iterator r = m.get_items().find(soid);
      started += prep_object_replica_pushes(soid, r->second.need, h, work_started);
    }
  }

  pgbackend->run_recovery_op(h, get_recovery_op_priority());
  return started;
}

hobject_t PrimaryLogPG::earliest_peer_backfill() const
{
  hobject_t e = hobject_t::get_max();
  for (const pg_shard_t& peer : get_backfill_targets()) {
    const auto iter = peer_backfill_info.find(peer);
    ceph_assert(iter != peer_backfill_info.end());
    e = std::min(e, iter->second.begin);
  }
  return e;
}

bool PrimaryLogPG::all_peer_done() const
{
  // Primary hasn't got any more objects
  ceph_assert(backfill_info.empty());

  for (const pg_shard_t& bt : get_backfill_targets()) {
    const auto piter = peer_backfill_info.find(bt);
    ceph_assert(piter != peer_backfill_info.end());
    const BackfillInterval& pbi = piter->second;
    // See if peer has more to process
    if (!pbi.extends_to_end() || !pbi.empty())
	return false;
  }
  return true;
}

/**
 * recover_backfill
 *
 * Invariants:
 *
 * backfilled: fully pushed to replica or present in replica's missing set (both
 * our copy and theirs).
 *
 * All objects on a backfill_target in
 * [MIN,peer_backfill_info[backfill_target].begin) are valid; logically-removed
 * objects have been actually deleted and all logically-valid objects are replicated.
 * There may be PG objects in this interval yet to be backfilled.
 *
 * All objects in PG in [MIN,backfill_info.begin) have been backfilled to all
 * backfill_targets.  There may be objects on backfill_target(s) yet to be deleted.
 *
 * For a backfill target, all objects < std::min(peer_backfill_info[target].begin,
 *     backfill_info.begin) in PG are backfilled.  No deleted objects in this
 * interval remain on the backfill target.
 *
 * For a backfill target, all objects <= peer_info[target].last_backfill
 * have been backfilled to target
 *
 * There *MAY* be missing/outdated objects between last_backfill_started and
 * std::min(peer_backfill_info[*].begin, backfill_info.begin) in the event that client
 * io created objects since the last scan.  For this reason, we call
 * update_range() again before continuing backfill.
 */
uint64_t PrimaryLogPG::recover_backfill(
  uint64_t max,
  ThreadPool::TPHandle &handle, bool *work_started)
{
  dout(10) << __func__ << " (" << max << ")"
           << " bft=" << get_backfill_targets()
	   << " last_backfill_started " << last_backfill_started
	   << (new_backfill ? " new_backfill":"")
	   << dendl;
  ceph_assert(!get_backfill_targets().empty());

  // Initialize from prior backfill state
  if (new_backfill) {
    // on_activate() was called prior to getting here
    ceph_assert(last_backfill_started == recovery_state.earliest_backfill());
    new_backfill = false;

    // initialize BackfillIntervals
    for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
	 i != get_backfill_targets().end();
	 ++i) {
      peer_backfill_info[*i].reset(
	recovery_state.get_peer_info(*i).last_backfill);
    }
    backfill_info.reset(last_backfill_started);

    backfills_in_flight.clear();
    pending_backfill_updates.clear();
  }

  for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
       i != get_backfill_targets().end();
       ++i) {
    dout(10) << "peer osd." << *i
	   << " info " << recovery_state.get_peer_info(*i)
	   << " interval " << peer_backfill_info[*i].begin
	   << "-" << peer_backfill_info[*i].end
	   << " " << peer_backfill_info[*i].objects.size() << " objects"
	   << dendl;
  }

  // update our local interval to cope with recent changes
  backfill_info.begin = last_backfill_started;
  update_range(&backfill_info, handle);

  unsigned ops = 0;
  vector<boost::tuple<hobject_t, eversion_t, pg_shard_t> > to_remove;
  set<hobject_t> add_to_stat;

  for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
       i != get_backfill_targets().end();
       ++i) {
    peer_backfill_info[*i].trim_to(
      std::max(
	recovery_state.get_peer_info(*i).last_backfill,
	last_backfill_started));
  }
  backfill_info.trim_to(last_backfill_started);

  PGBackend::RecoveryHandle *h = pgbackend->open_recovery_op();
  while (ops < max) {
    if (backfill_info.begin <= earliest_peer_backfill() &&
	!backfill_info.extends_to_end() && backfill_info.empty()) {
      hobject_t next = backfill_info.end;
      backfill_info.reset(next);
      backfill_info.end = hobject_t::get_max();
      update_range(&backfill_info, handle);
      backfill_info.trim();
    }

    dout(20) << "   my backfill interval " << backfill_info << dendl;

    bool sent_scan = false;
    for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
	 i != get_backfill_targets().end();
	 ++i) {
      pg_shard_t bt = *i;
      BackfillInterval& pbi = peer_backfill_info[bt];

      dout(20) << " peer shard " << bt << " backfill " << pbi << dendl;
      if (pbi.begin <= backfill_info.begin &&
	  !pbi.extends_to_end() && pbi.empty()) {
	dout(10) << " scanning peer osd." << bt << " from " << pbi.end << dendl;
	epoch_t e = get_osdmap_epoch();
	MOSDPGScan *m = new MOSDPGScan(
	  MOSDPGScan::OP_SCAN_GET_DIGEST, pg_whoami, e, get_last_peering_reset(),
	  spg_t(info.pgid.pgid, bt.shard),
	  pbi.end, hobject_t());
	osd->send_message_osd_cluster(bt.osd, m, get_osdmap_epoch());
	ceph_assert(waiting_on_backfill.find(bt) == waiting_on_backfill.end());
	waiting_on_backfill.insert(bt);
        sent_scan = true;
      }
    }

    // Count simultaneous scans as a single op and let those complete
    if (sent_scan) {
      ops++;
      start_recovery_op(hobject_t::get_max()); // XXX: was pbi.end
      break;
    }

    if (backfill_info.empty() && all_peer_done()) {
      dout(10) << " reached end for both local and all peers" << dendl;
      break;
    }

    // Get object within set of peers to operate on and
    // the set of targets for which that object applies.
    hobject_t check = earliest_peer_backfill();

    if (check < backfill_info.begin) {

      set<pg_shard_t> check_targets;
      for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
	   i != get_backfill_targets().end();
	   ++i) {
        pg_shard_t bt = *i;
        BackfillInterval& pbi = peer_backfill_info[bt];
        if (pbi.begin == check)
          check_targets.insert(bt);
      }
      ceph_assert(!check_targets.empty());

      dout(20) << " BACKFILL removing " << check
	       << " from peers " << check_targets << dendl;
      for (set<pg_shard_t>::iterator i = check_targets.begin();
	   i != check_targets.end();
	   ++i) {
        pg_shard_t bt = *i;
        BackfillInterval& pbi = peer_backfill_info[bt];
        ceph_assert(pbi.begin == check);

        to_remove.push_back(boost::make_tuple(check, pbi.objects.begin()->second, bt));
        pbi.pop_front();
      }

      last_backfill_started = check;

      // Don't increment ops here because deletions
      // are cheap and not replied to unlike real recovery_ops,
      // and we can't increment ops without requeueing ourself
      // for recovery.
    } else {
      eversion_t& obj_v = backfill_info.objects.begin()->second;

      vector<pg_shard_t> need_ver_targs, missing_targs, keep_ver_targs, skip_targs;
      for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
	   i != get_backfill_targets().end();
	   ++i) {
	pg_shard_t bt = *i;
	BackfillInterval& pbi = peer_backfill_info[bt];
        // Find all check peers that have the wrong version
	if (check == backfill_info.begin && check == pbi.begin) {
	  if (pbi.objects.begin()->second != obj_v) {
	    need_ver_targs.push_back(bt);
	  } else {
	    keep_ver_targs.push_back(bt);
	  }
        } else {
	  const pg_info_t& pinfo = recovery_state.get_peer_info(bt);

          // Only include peers that we've caught up to their backfill line
	  // otherwise, they only appear to be missing this object
	  // because their pbi.begin > backfill_info.begin.
          if (backfill_info.begin > pinfo.last_backfill)
	    missing_targs.push_back(bt);
	  else
	    skip_targs.push_back(bt);
	}
      }

      if (!keep_ver_targs.empty()) {
        // These peers have version obj_v
	dout(20) << " BACKFILL keeping " << check
		 << " with ver " << obj_v
		 << " on peers " << keep_ver_targs << dendl;
	//assert(!waiting_for_degraded_object.count(check));
      }
      if (!need_ver_targs.empty() || !missing_targs.empty()) {
	ObjectContextRef obc = get_object_context(backfill_info.begin, false);
	ceph_assert(obc);
	if (obc->get_recovery_read()) {
	  if (!need_ver_targs.empty()) {
	    dout(20) << " BACKFILL replacing " << check
		   << " with ver " << obj_v
		   << " to peers " << need_ver_targs << dendl;
	  }
	  if (!missing_targs.empty()) {
	    dout(20) << " BACKFILL pushing " << backfill_info.begin
	         << " with ver " << obj_v
	         << " to peers " << missing_targs << dendl;
	  }
	  vector<pg_shard_t> all_push = need_ver_targs;
	  all_push.insert(all_push.end(), missing_targs.begin(), missing_targs.end());

	  handle.reset_tp_timeout();
	  int r = prep_backfill_object_push(backfill_info.begin, obj_v, obc, all_push, h);
	  if (r < 0) {
	    *work_started = true;
	    dout(0) << __func__ << " Error " << r << " trying to backfill " << backfill_info.begin << dendl;
	    break;
	  }
	  ops++;
	} else {
	  *work_started = true;
	  dout(20) << "backfill blocking on " << backfill_info.begin
		   << "; could not get rw_manager lock" << dendl;
	  break;
	}
      }
      dout(20) << "need_ver_targs=" << need_ver_targs
	       << " keep_ver_targs=" << keep_ver_targs << dendl;
      dout(20) << "backfill_targets=" << get_backfill_targets()
	       << " missing_targs=" << missing_targs
	       << " skip_targs=" << skip_targs << dendl;

      last_backfill_started = backfill_info.begin;
      add_to_stat.insert(backfill_info.begin); // XXX: Only one for all pushes?
      backfill_info.pop_front();
      vector<pg_shard_t> check_targets = need_ver_targs;
      check_targets.insert(check_targets.end(), keep_ver_targs.begin(), keep_ver_targs.end());
      for (vector<pg_shard_t>::iterator i = check_targets.begin();
	   i != check_targets.end();
	   ++i) {
        pg_shard_t bt = *i;
        BackfillInterval& pbi = peer_backfill_info[bt];
        pbi.pop_front();
      }
    }
  }

  hobject_t backfill_pos =
    std::min(backfill_info.begin, earliest_peer_backfill());

  for (set<hobject_t>::iterator i = add_to_stat.begin();
       i != add_to_stat.end();
       ++i) {
    ObjectContextRef obc = get_object_context(*i, false);
    ceph_assert(obc);
    pg_stat_t stat;
    add_object_context_to_pg_stat(obc, &stat);
    pending_backfill_updates[*i] = stat;
  }
  map<pg_shard_t,MOSDPGBackfillRemove*> reqs;
  for (unsigned i = 0; i < to_remove.size(); ++i) {
    handle.reset_tp_timeout();
    const hobject_t& oid = to_remove[i].get<0>();
    eversion_t v = to_remove[i].get<1>();
    pg_shard_t peer = to_remove[i].get<2>();
    MOSDPGBackfillRemove *m;
    auto it = reqs.find(peer);
    if (it != reqs.end()) {
      m = it->second;
    } else {
      m = reqs[peer] = new MOSDPGBackfillRemove(
	spg_t(info.pgid.pgid, peer.shard),
	get_osdmap_epoch());
    }
    m->ls.push_back(make_pair(oid, v));

    if (oid <= last_backfill_started)
      pending_backfill_updates[oid]; // add empty stat!
  }
  for (auto p : reqs) {
    osd->send_message_osd_cluster(p.first.osd, p.second,
				  get_osdmap_epoch());
  }

  pgbackend->run_recovery_op(h, get_recovery_op_priority());

  dout(5) << "backfill_pos is " << backfill_pos << dendl;
  for (set<hobject_t>::iterator i = backfills_in_flight.begin();
       i != backfills_in_flight.end();
       ++i) {
    dout(20) << *i << " is still in flight" << dendl;
  }

  hobject_t next_backfill_to_complete = backfills_in_flight.empty() ?
    backfill_pos : *(backfills_in_flight.begin());
  hobject_t new_last_backfill = recovery_state.earliest_backfill();
  dout(10) << "starting new_last_backfill at " << new_last_backfill << dendl;
  for (map<hobject_t, pg_stat_t>::iterator i =
	 pending_backfill_updates.begin();
       i != pending_backfill_updates.end() &&
	 i->first < next_backfill_to_complete;
       pending_backfill_updates.erase(i++)) {
    dout(20) << " pending_backfill_update " << i->first << dendl;
    ceph_assert(i->first > new_last_backfill);
    recovery_state.update_complete_backfill_object_stats(
      i->first,
      i->second);
    new_last_backfill = i->first;
  }
  dout(10) << "possible new_last_backfill at " << new_last_backfill << dendl;

  ceph_assert(!pending_backfill_updates.empty() ||
	 new_last_backfill == last_backfill_started);
  if (pending_backfill_updates.empty() &&
      backfill_pos.is_max()) {
    ceph_assert(backfills_in_flight.empty());
    new_last_backfill = backfill_pos;
    last_backfill_started = backfill_pos;
  }
  dout(10) << "final new_last_backfill at " << new_last_backfill << dendl;

  // If new_last_backfill == MAX, then we will send OP_BACKFILL_FINISH to
  // all the backfill targets.  Otherwise, we will move last_backfill up on
  // those targets need it and send OP_BACKFILL_PROGRESS to them.
  for (set<pg_shard_t>::const_iterator i = get_backfill_targets().begin();
       i != get_backfill_targets().end();
       ++i) {
    pg_shard_t bt = *i;
    const pg_info_t& pinfo = recovery_state.get_peer_info(bt);

    if (new_last_backfill > pinfo.last_backfill) {
      recovery_state.update_peer_last_backfill(bt, new_last_backfill);
      epoch_t e = get_osdmap_epoch();
      MOSDPGBackfill *m = NULL;
      if (pinfo.last_backfill.is_max()) {
        m = new MOSDPGBackfill(
	  MOSDPGBackfill::OP_BACKFILL_FINISH,
	  e,
	  get_last_peering_reset(),
	  spg_t(info.pgid.pgid, bt.shard));
        // Use default priority here, must match sub_op priority
        start_recovery_op(hobject_t::get_max());
      } else {
        m = new MOSDPGBackfill(
	  MOSDPGBackfill::OP_BACKFILL_PROGRESS,
	  e,
	  get_last_peering_reset(),
	  spg_t(info.pgid.pgid, bt.shard));
        // Use default priority here, must match sub_op priority
      }
      m->last_backfill = pinfo.last_backfill;
      m->stats = pinfo.stats;
      osd->send_message_osd_cluster(bt.osd, m, get_osdmap_epoch());
      dout(10) << " peer " << bt
	       << " num_objects now " << pinfo.stats.stats.sum.num_objects
	       << " / " << info.stats.stats.sum.num_objects << dendl;
    }
  }

  if (ops)
    *work_started = true;
  return ops;
}

int PrimaryLogPG::prep_backfill_object_push(
  hobject_t oid, eversion_t v,
  ObjectContextRef obc,
  vector<pg_shard_t> peers,
  PGBackend::RecoveryHandle *h)
{
  dout(10) << __func__ << " " << oid << " v " << v << " to peers " << peers << dendl;
  ceph_assert(!peers.empty());

  backfills_in_flight.insert(oid);
  recovery_state.prepare_backfill_for_missing(oid, v, peers);

  ceph_assert(!recovering.count(oid));

  start_recovery_op(oid);
  recovering.insert(make_pair(oid, obc));

  int r = pgbackend->recover_object(
    oid,
    v,
    ObjectContextRef(),
    obc,
    h);
  if (r < 0) {
    dout(0) << __func__ << " Error " << r << " on oid " << oid << dendl;
    on_failed_pull({ pg_whoami }, oid, v);
  }
  return r;
}

void PrimaryLogPG::update_range(
  BackfillInterval *bi,
  ThreadPool::TPHandle &handle)
{
  int local_min = cct->_conf->osd_backfill_scan_min;
  int local_max = cct->_conf->osd_backfill_scan_max;

  if (bi->version < info.log_tail) {
    dout(10) << __func__<< ": bi is old, rescanning local backfill_info"
	     << dendl;
    bi->version = info.last_update;
    scan_range(local_min, local_max, bi, handle);
  }

  if (bi->version >= projected_last_update) {
    dout(10) << __func__<< ": bi is current " << dendl;
    ceph_assert(bi->version == projected_last_update);
  } else if (bi->version >= info.log_tail) {
    if (recovery_state.get_pg_log().get_log().empty() && projected_log.empty()) {
      /* Because we don't move log_tail on split, the log might be
       * empty even if log_tail != last_update.  However, the only
       * way to get here with an empty log is if log_tail is actually
       * eversion_t(), because otherwise the entry which changed
       * last_update since the last scan would have to be present.
       */
      ceph_assert(bi->version == eversion_t());
      return;
    }

    dout(10) << __func__<< ": bi is old, (" << bi->version
	     << ") can be updated with log to projected_last_update "
	     << projected_last_update << dendl;

    auto func = [&](const pg_log_entry_t &e) {
      dout(10) << __func__ << ": updating from version " << e.version
               << dendl;
      const hobject_t &soid = e.soid;
      if (soid >= bi->begin &&
	  soid < bi->end) {
	if (e.is_update()) {
	  dout(10) << __func__ << ": " << e.soid << " updated to version "
		   << e.version << dendl;
	  bi->objects.erase(e.soid);
	  bi->objects.insert(
	    make_pair(
	      e.soid,
	      e.version));
	} else if (e.is_delete()) {
	  dout(10) << __func__ << ": " << e.soid << " removed" << dendl;
	  bi->objects.erase(e.soid);
	}
      }
    };
    dout(10) << "scanning pg log first" << dendl;
    recovery_state.get_pg_log().get_log().scan_log_after(bi->version, func);
    dout(10) << "scanning projected log" << dendl;
    projected_log.scan_log_after(bi->version, func);
    bi->version = projected_last_update;
  } else {
    ceph_abort_msg("scan_range should have raised bi->version past log_tail");
  }
}

void PrimaryLogPG::scan_range(
  int min, int max, BackfillInterval *bi,
  ThreadPool::TPHandle &handle)
{
  ceph_assert(is_locked());
  dout(10) << "scan_range from " << bi->begin << dendl;
  bi->clear_objects();

  vector<hobject_t> ls;
  ls.reserve(max);
  int r = pgbackend->objects_list_partial(bi->begin, min, max, &ls, &bi->end);
  ceph_assert(r >= 0);
  dout(10) << " got " << ls.size() << " items, next " << bi->end << dendl;
  dout(20) << ls << dendl;

  for (vector<hobject_t>::iterator p = ls.begin(); p != ls.end(); ++p) {
    handle.reset_tp_timeout();
    ObjectContextRef obc;
    if (is_primary())
      obc = object_contexts.lookup(*p);
    if (obc) {
      if (!obc->obs.exists) {
	/* If the object does not exist here, it must have been removed
	 * between the collection_list_partial and here.  This can happen
	 * for the first item in the range, which is usually last_backfill.
	 */
	continue;
      }
      bi->objects[*p] = obc->obs.oi.version;
      dout(20) << "  " << *p << " " << obc->obs.oi.version << dendl;
    } else {
      bufferlist bl;
      int r = pgbackend->objects_get_attr(*p, OI_ATTR, &bl);
      /* If the object does not exist here, it must have been removed
       * between the collection_list_partial and here.  This can happen
       * for the first item in the range, which is usually last_backfill.
       */
      if (r == -ENOENT)
	continue;

      ceph_assert(r >= 0);
      object_info_t oi(bl);
      bi->objects[*p] = oi.version;
      dout(20) << "  " << *p << " " << oi.version << dendl;
    }
  }
}


/** check_local
 * 
 * verifies that stray objects have been deleted
 */
void PrimaryLogPG::check_local()
{
  dout(10) << __func__ << dendl;

  ceph_assert(
    info.last_update >=
    recovery_state.get_pg_log().get_tail());  // otherwise we need some help!

  if (!cct->_conf->osd_debug_verify_stray_on_activate)
    return;

  // just scan the log.
  set<hobject_t> did;
  for (list<pg_log_entry_t>::const_reverse_iterator p = recovery_state.get_pg_log().get_log().log.rbegin();
       p != recovery_state.get_pg_log().get_log().log.rend();
       ++p) {
    if (did.count(p->soid))
      continue;
    did.insert(p->soid);

    if (p->is_delete() && !is_missing_object(p->soid)) {
      dout(10) << " checking " << p->soid
	       << " at " << p->version << dendl;
      struct stat st;
      int r = osd->store->stat(
	ch,
	ghobject_t(p->soid, ghobject_t::NO_GEN, pg_whoami.shard),
	&st);
      if (r != -ENOENT) {
	derr << __func__ << " " << p->soid << " exists, but should have been "
	     << "deleted" << dendl;
	ceph_abort_msg("erroneously present object");
      }
    } else {
      // ignore old(+missing) objects
    }
  }
}



// ===========================
// hit sets

hobject_t PrimaryLogPG::get_hit_set_current_object(utime_t stamp)
{
  ostringstream ss;
  ss << "hit_set_" << info.pgid.pgid << "_current_" << stamp;
  hobject_t hoid(sobject_t(ss.str(), CEPH_NOSNAP), "",
		 info.pgid.ps(), info.pgid.pool(),
		 cct->_conf->osd_hit_set_namespace);
  dout(20) << __func__ << " " << hoid << dendl;
  return hoid;
}

hobject_t PrimaryLogPG::get_hit_set_archive_object(utime_t start,
						   utime_t end,
						   bool using_gmt)
{
  ostringstream ss;
  ss << "hit_set_" << info.pgid.pgid << "_archive_";
  if (using_gmt) {
    start.gmtime(ss, true /* legacy pre-octopus form */) << "_";
    end.gmtime(ss, true /* legacy pre-octopus form */);
  } else {
    start.localtime(ss, true /* legacy pre-octopus form */) << "_";
    end.localtime(ss, true /* legacy pre-octopus form */);
  }
  hobject_t hoid(sobject_t(ss.str(), CEPH_NOSNAP), "",
		 info.pgid.ps(), info.pgid.pool(),
		 cct->_conf->osd_hit_set_namespace);
  dout(20) << __func__ << " " << hoid << dendl;
  return hoid;
}

void PrimaryLogPG::hit_set_clear()
{
  dout(20) << __func__ << dendl;
  hit_set.reset();
  hit_set_start_stamp = utime_t();
}

void PrimaryLogPG::hit_set_setup()
{
  if (!is_active() ||
      !is_primary()) {
    hit_set_clear();
    return;
  }

  if (is_active() && is_primary() &&
      (!pool.info.hit_set_count ||
       !pool.info.hit_set_period ||
       pool.info.hit_set_params.get_type() == HitSet::TYPE_NONE)) {
    hit_set_clear();

    // only primary is allowed to remove all the hit set objects
    hit_set_remove_all();
    return;
  }

  // FIXME: discard any previous data for now
  hit_set_create();

  // include any writes we know about from the pg log.  this doesn't
  // capture reads, but it is better than nothing!
  hit_set_apply_log();
}

void PrimaryLogPG::hit_set_remove_all()
{
  // If any archives are degraded we skip this
  for (auto p = info.hit_set.history.begin();
       p != info.hit_set.history.end();
       ++p) {
    hobject_t aoid = get_hit_set_archive_object(p->begin, p->end, p->using_gmt);

    // Once we hit a degraded object just skip
    if (is_degraded_or_backfilling_object(aoid))
      return;
    if (write_blocked_by_scrub(aoid))
      return;
  }

  if (!info.hit_set.history.empty()) {
    auto p = info.hit_set.history.rbegin();
    ceph_assert(p != info.hit_set.history.rend());
    hobject_t oid = get_hit_set_archive_object(p->begin, p->end, p->using_gmt);
    ceph_assert(!is_degraded_or_backfilling_object(oid));
    ObjectContextRef obc = get_object_context(oid, false);
    ceph_assert(obc);

    OpContextUPtr ctx = simple_opc_create(obc);
    ctx->at_version = get_next_version();
    ctx->updated_hset_history = info.hit_set;
    utime_t now = ceph_clock_now();
    ctx->mtime = now;
    hit_set_trim(ctx, 0);
    simple_opc_submit(std::move(ctx));
  }

  recovery_state.update_hset(pg_hit_set_history_t());
  if (agent_state) {
    agent_state->discard_hit_sets();
  }
}

void PrimaryLogPG::hit_set_create()
{
  utime_t now = ceph_clock_now();
  // make a copy of the params to modify
  HitSet::Params params(pool.info.hit_set_params);

  dout(20) << __func__ << " " << params << dendl;
  if (pool.info.hit_set_params.get_type() == HitSet::TYPE_BLOOM) {
    BloomHitSet::Params *p =
      static_cast<BloomHitSet::Params*>(params.impl.get());

    // convert false positive rate so it holds up across the full period
    p->set_fpp(p->get_fpp() / pool.info.hit_set_count);
    if (p->get_fpp() <= 0.0)
      p->set_fpp(.01);  // fpp cannot be zero!

    // if we don't have specified size, estimate target size based on the
    // previous bin!
    if (p->target_size == 0 && hit_set) {
      utime_t dur = now - hit_set_start_stamp;
      unsigned unique = hit_set->approx_unique_insert_count();
      dout(20) << __func__ << " previous set had approx " << unique
	       << " unique items over " << dur << " seconds" << dendl;
      p->target_size = (double)unique * (double)pool.info.hit_set_period
		     / (double)dur;
    }
    if (p->target_size <
	static_cast<uint64_t>(cct->_conf->osd_hit_set_min_size))
      p->target_size = cct->_conf->osd_hit_set_min_size;

    if (p->target_size
	> static_cast<uint64_t>(cct->_conf->osd_hit_set_max_size))
      p->target_size = cct->_conf->osd_hit_set_max_size;

    p->seed = now.sec();

    dout(10) << __func__ << " target_size " << p->target_size
	     << " fpp " << p->get_fpp() << dendl;
  }
  hit_set.reset(new HitSet(params));
  hit_set_start_stamp = now;
}

/**
 * apply log entries to set
 *
 * this would only happen after peering, to at least capture writes
 * during an interval that was potentially lost.
 */
bool PrimaryLogPG::hit_set_apply_log()
{
  if (!hit_set)
    return false;

  eversion_t to = info.last_update;
  eversion_t from = info.hit_set.current_last_update;
  if (to <= from) {
    dout(20) << __func__ << " no update" << dendl;
    return false;
  }

  dout(20) << __func__ << " " << to << " .. " << info.last_update << dendl;
  list<pg_log_entry_t>::const_reverse_iterator p =
    recovery_state.get_pg_log().get_log().log.rbegin();
  while (p != recovery_state.get_pg_log().get_log().log.rend() && p->version > to)
    ++p;
  while (p != recovery_state.get_pg_log().get_log().log.rend() && p->version > from) {
    hit_set->insert(p->soid);
    ++p;
  }

  return true;
}

void PrimaryLogPG::hit_set_persist()
{
  dout(10) << __func__  << dendl;
  bufferlist bl;
  unsigned max = pool.info.hit_set_count;

  utime_t now = ceph_clock_now();
  hobject_t oid;

  // If any archives are degraded we skip this persist request
  // account for the additional entry being added below
  for (auto p = info.hit_set.history.begin();
       p != info.hit_set.history.end();
       ++p) {
    hobject_t aoid = get_hit_set_archive_object(p->begin, p->end, p->using_gmt);

    // Once we hit a degraded object just skip further trim
    if (is_degraded_or_backfilling_object(aoid))
      return;
    if (write_blocked_by_scrub(aoid))
      return;
  }

  // If backfill is in progress and we could possibly overlap with the
  // hit_set_* objects, back off.  Since these all have
  // hobject_t::hash set to pgid.ps(), and those sort first, we can
  // look just at that.  This is necessary because our transactions
  // may include a modify of the new hit_set *and* a delete of the
  // old one, and this may span the backfill boundary.
  for (set<pg_shard_t>::const_iterator p = get_backfill_targets().begin();
       p != get_backfill_targets().end();
       ++p) {
    const pg_info_t& pi = recovery_state.get_peer_info(*p);
    if (pi.last_backfill == hobject_t() ||
	pi.last_backfill.get_hash() == info.pgid.ps()) {
      dout(10) << __func__ << " backfill target osd." << *p
	       << " last_backfill has not progressed past pgid ps"
	       << dendl;
      return;
    }
  }


  pg_hit_set_info_t new_hset = pg_hit_set_info_t(pool.info.use_gmt_hitset);
  new_hset.begin = hit_set_start_stamp;
  new_hset.end = now;
  oid = get_hit_set_archive_object(
    new_hset.begin,
    new_hset.end,
    new_hset.using_gmt);

  // If the current object is degraded we skip this persist request
  if (write_blocked_by_scrub(oid))
    return;

  hit_set->seal();
  encode(*hit_set, bl);
  dout(20) << __func__ << " archive " << oid << dendl;

  if (agent_state) {
    agent_state->add_hit_set(new_hset.begin, hit_set);
    uint32_t size = agent_state->hit_set_map.size();
    if (size >= pool.info.hit_set_count) {
      size = pool.info.hit_set_count > 0 ? pool.info.hit_set_count - 1: 0;
    }
    hit_set_in_memory_trim(size);
  }

  ObjectContextRef obc = get_object_context(oid, true);
  OpContextUPtr ctx = simple_opc_create(obc);

  ctx->at_version = get_next_version();
  ctx->updated_hset_history = info.hit_set;
  pg_hit_set_history_t &updated_hit_set_hist = *(ctx->updated_hset_history);

  updated_hit_set_hist.current_last_update = info.last_update;
  new_hset.version = ctx->at_version;

  updated_hit_set_hist.history.push_back(new_hset);
  hit_set_create();

  // fabricate an object_info_t and SnapSet
  obc->obs.oi.version = ctx->at_version;
  obc->obs.oi.mtime = now;
  obc->obs.oi.size = bl.length();
  obc->obs.exists = true;
  obc->obs.oi.set_data_digest(bl.crc32c(-1));

  ctx->new_obs = obc->obs;

  ctx->new_snapset = obc->ssc->snapset;

  ctx->delta_stats.num_objects++;
  ctx->delta_stats.num_objects_hit_set_archive++;

  ctx->delta_stats.num_bytes += bl.length();
  ctx->delta_stats.num_bytes_hit_set_archive += bl.length();

  bufferlist bss;
  encode(ctx->new_snapset, bss);
  bufferlist boi(sizeof(ctx->new_obs.oi));
  encode(ctx->new_obs.oi, boi,
	   get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));

  ctx->op_t->create(oid);
  if (bl.length()) {
    ctx->op_t->write(oid, 0, bl.length(), bl, 0);
    write_update_size_and_usage(ctx->delta_stats, obc->obs.oi, ctx->modified_ranges,
        0, bl.length());
    ctx->clean_regions.mark_data_region_dirty(0, bl.length());
  }
  map <string, bufferlist> attrs;
  attrs[OI_ATTR].claim(boi);
  attrs[SS_ATTR].claim(bss);
  setattrs_maybe_cache(ctx->obc, ctx->op_t.get(), attrs);
  ctx->log.push_back(
    pg_log_entry_t(
      pg_log_entry_t::MODIFY,
      oid,
      ctx->at_version,
      eversion_t(),
      0,
      osd_reqid_t(),
      ctx->mtime,
      0)
    );
  ctx->log.back().clean_regions = ctx->clean_regions;

  hit_set_trim(ctx, max);

  simple_opc_submit(std::move(ctx));
}

void PrimaryLogPG::hit_set_trim(OpContextUPtr &ctx, unsigned max)
{
  ceph_assert(ctx->updated_hset_history);
  pg_hit_set_history_t &updated_hit_set_hist =
    *(ctx->updated_hset_history);
  for (unsigned num = updated_hit_set_hist.history.size(); num > max; --num) {
    list<pg_hit_set_info_t>::iterator p = updated_hit_set_hist.history.begin();
    ceph_assert(p != updated_hit_set_hist.history.end());
    hobject_t oid = get_hit_set_archive_object(p->begin, p->end, p->using_gmt);

    ceph_assert(!is_degraded_or_backfilling_object(oid));

    dout(20) << __func__ << " removing " << oid << dendl;
    ++ctx->at_version.version;
    ctx->log.push_back(
        pg_log_entry_t(pg_log_entry_t::DELETE,
		       oid,
		       ctx->at_version,
		       p->version,
		       0,
		       osd_reqid_t(),
		       ctx->mtime,
		       0));

    ctx->op_t->remove(oid);
    updated_hit_set_hist.history.pop_front();

    ObjectContextRef obc = get_object_context(oid, false);
    ceph_assert(obc);
    --ctx->delta_stats.num_objects;
    --ctx->delta_stats.num_objects_hit_set_archive;
    ctx->delta_stats.num_bytes -= obc->obs.oi.size;
    ctx->delta_stats.num_bytes_hit_set_archive -= obc->obs.oi.size;
  }
}

void PrimaryLogPG::hit_set_in_memory_trim(uint32_t max_in_memory)
{
  while (agent_state->hit_set_map.size() > max_in_memory) {
    agent_state->remove_oldest_hit_set();
  }
}


// =======================================
// cache agent

void PrimaryLogPG::agent_setup()
{
  ceph_assert(is_locked());
  if (!is_active() ||
      !is_primary() ||
      state_test(PG_STATE_PREMERGE) ||
      pool.info.cache_mode == pg_pool_t::CACHEMODE_NONE ||
      pool.info.tier_of < 0 ||
      !get_osdmap()->have_pg_pool(pool.info.tier_of)) {
    agent_clear();
    return;
  }
  if (!agent_state) {
    agent_state.reset(new TierAgentState);

    // choose random starting position
    agent_state->position = hobject_t();
    agent_state->position.pool = info.pgid.pool();
    agent_state->position.set_hash(pool.info.get_random_pg_position(
      info.pgid.pgid,
      rand()));
    agent_state->start = agent_state->position;

    dout(10) << __func__ << " allocated new state, position "
	     << agent_state->position << dendl;
  } else {
    dout(10) << __func__ << " keeping existing state" << dendl;
  }

  if (info.stats.stats_invalid) {
    osd->clog->warn() << "pg " << info.pgid << " has invalid (post-split) stats; must scrub before tier agent can activate";
  }

  agent_choose_mode();
}

void PrimaryLogPG::agent_clear()
{
  agent_stop();
  agent_state.reset(NULL);
}

// Return false if no objects operated on since start of object hash space
bool PrimaryLogPG::agent_work(int start_max, int agent_flush_quota)
{
  std::scoped_lock locker{*this};
  if (!agent_state) {
    dout(10) << __func__ << " no agent state, stopping" << dendl;
    return true;
  }

  ceph_assert(!recovery_state.is_deleting());

  if (agent_state->is_idle()) {
    dout(10) << __func__ << " idle, stopping" << dendl;
    return true;
  }

  osd->logger->inc(l_osd_agent_wake);

  dout(10) << __func__
	   << " max " << start_max
	   << ", flush " << agent_state->get_flush_mode_name()
	   << ", evict " << agent_state->get_evict_mode_name()
	   << ", pos " << agent_state->position
	   << dendl;
  ceph_assert(is_primary());
  ceph_assert(is_active());

  agent_load_hit_sets();

  const pg_pool_t *base_pool = get_osdmap()->get_pg_pool(pool.info.tier_of);
  ceph_assert(base_pool);

  int ls_min = 1;
  int ls_max = cct->_conf->osd_pool_default_cache_max_evict_check_size;

  // list some objects.  this conveniently lists clones (oldest to
  // newest) before heads... the same order we want to flush in.
  //
  // NOTE: do not flush the Sequencer.  we will assume that the
  // listing we get back is imprecise.
  vector<hobject_t> ls;
  hobject_t next;
  int r = pgbackend->objects_list_partial(agent_state->position, ls_min, ls_max,
					  &ls, &next);
  ceph_assert(r >= 0);
  dout(20) << __func__ << " got " << ls.size() << " objects" << dendl;
  int started = 0;
  for (vector<hobject_t>::iterator p = ls.begin();
       p != ls.end();
       ++p) {
    if (p->nspace == cct->_conf->osd_hit_set_namespace) {
      dout(20) << __func__ << " skip (hit set) " << *p << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }
    if (is_degraded_or_backfilling_object(*p)) {
      dout(20) << __func__ << " skip (degraded) " << *p << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }
    if (is_missing_object(p->get_head())) {
      dout(20) << __func__ << " skip (missing head) " << *p << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }
    ObjectContextRef obc = get_object_context(*p, false, NULL);
    if (!obc) {
      // we didn't flush; we may miss something here.
      dout(20) << __func__ << " skip (no obc) " << *p << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }
    if (!obc->obs.exists) {
      dout(20) << __func__ << " skip (dne) " << obc->obs.oi.soid << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }
    if (range_intersects_scrub(obc->obs.oi.soid,
			       obc->obs.oi.soid.get_head())) {
      dout(20) << __func__ << " skip (scrubbing) " << obc->obs.oi << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }
    if (obc->is_blocked()) {
      dout(20) << __func__ << " skip (blocked) " << obc->obs.oi << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }
    if (obc->is_request_pending()) {
      dout(20) << __func__ << " skip (request pending) " << obc->obs.oi << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }

    // be careful flushing omap to an EC pool.
    if (!base_pool->supports_omap() &&
	obc->obs.oi.is_omap()) {
      dout(20) << __func__ << " skip (omap to EC) " << obc->obs.oi << dendl;
      osd->logger->inc(l_osd_agent_skip);
      continue;
    }

    if (agent_state->evict_mode != TierAgentState::EVICT_MODE_IDLE &&
	agent_maybe_evict(obc, false))
      ++started;
    else if (agent_state->flush_mode != TierAgentState::FLUSH_MODE_IDLE &&
             agent_flush_quota > 0 && agent_maybe_flush(obc)) {
      ++started;
      --agent_flush_quota;
    }
    if (started >= start_max) {
      // If finishing early, set "next" to the next object
      if (++p != ls.end())
	next = *p;
      break;
    }
  }

  if (++agent_state->hist_age > cct->_conf->osd_agent_hist_halflife) {
    dout(20) << __func__ << " resetting atime and temp histograms" << dendl;
    agent_state->hist_age = 0;
    agent_state->temp_hist.decay();
  }

  // Total objects operated on so far
  int total_started = agent_state->started + started;
  bool need_delay = false;

  dout(20) << __func__ << " start pos " << agent_state->position
    << " next start pos " << next
    << " started " << total_started << dendl;

  // See if we've made a full pass over the object hash space
  // This might check at most ls_max objects a second time to notice that
  // we've checked every objects at least once.
  if (agent_state->position < agent_state->start &&
      next >= agent_state->start) {
    dout(20) << __func__ << " wrap around " << agent_state->start << dendl;
    if (total_started == 0)
      need_delay = true;
    else
      total_started = 0;
    agent_state->start = next;
  }
  agent_state->started = total_started;

  // See if we are starting from beginning
  if (next.is_max())
    agent_state->position = hobject_t();
  else
    agent_state->position = next;

  // Discard old in memory HitSets
  hit_set_in_memory_trim(pool.info.hit_set_count);

  if (need_delay) {
    ceph_assert(agent_state->delaying == false);
    agent_delay();
    return false;
  }
  agent_choose_mode();
  return true;
}

void PrimaryLogPG::agent_load_hit_sets()
{
  if (agent_state->evict_mode == TierAgentState::EVICT_MODE_IDLE) {
    return;
  }

  if (agent_state->hit_set_map.size() < info.hit_set.history.size()) {
    dout(10) << __func__ << dendl;
    for (auto p = info.hit_set.history.begin();
	 p != info.hit_set.history.end(); ++p) {
      if (agent_state->hit_set_map.count(p->begin.sec()) == 0) {
	dout(10) << __func__ << " loading " << p->begin << "-"
		 << p->end << dendl;
	if (!pool.info.is_replicated()) {
	  // FIXME: EC not supported here yet
	  derr << __func__ << " on non-replicated pool" << dendl;
	  break;
	}

	hobject_t oid = get_hit_set_archive_object(p->begin, p->end, p->using_gmt);
	if (is_unreadable_object(oid)) {
	  dout(10) << __func__ << " unreadable " << oid << ", waiting" << dendl;
	  break;
	}

	ObjectContextRef obc = get_object_context(oid, false);
	if (!obc) {
	  derr << __func__ << ": could not load hitset " << oid << dendl;
	  break;
	}

	bufferlist bl;
	{
	  int r = osd->store->read(ch, ghobject_t(oid), 0, 0, bl);
	  ceph_assert(r >= 0);
	}
	HitSetRef hs(new HitSet);
	bufferlist::const_iterator pbl = bl.begin();
	decode(*hs, pbl);
	agent_state->add_hit_set(p->begin.sec(), hs);
      }
    }
  }
}

bool PrimaryLogPG::agent_maybe_flush(ObjectContextRef& obc)
{
  if (!obc->obs.oi.is_dirty()) {
    dout(20) << __func__ << " skip (clean) " << obc->obs.oi << dendl;
    osd->logger->inc(l_osd_agent_skip);
    return false;
  }
  if (obc->obs.oi.is_cache_pinned()) {
    dout(20) << __func__ << " skip (cache_pinned) " << obc->obs.oi << dendl;
    osd->logger->inc(l_osd_agent_skip);
    return false;
  }

  utime_t now = ceph_clock_now();
  utime_t ob_local_mtime;
  if (obc->obs.oi.local_mtime != utime_t()) {
    ob_local_mtime = obc->obs.oi.local_mtime;
  } else {
    ob_local_mtime = obc->obs.oi.mtime;
  }
  bool evict_mode_full =
    (agent_state->evict_mode == TierAgentState::EVICT_MODE_FULL);
  if (!evict_mode_full &&
      obc->obs.oi.soid.snap == CEPH_NOSNAP &&  // snaps immutable; don't delay
      (ob_local_mtime + utime_t(pool.info.cache_min_flush_age, 0) > now)) {
    dout(20) << __func__ << " skip (too young) " << obc->obs.oi << dendl;
    osd->logger->inc(l_osd_agent_skip);
    return false;
  }

  if (osd->agent_is_active_oid(obc->obs.oi.soid)) {
    dout(20) << __func__ << " skip (flushing) " << obc->obs.oi << dendl;
    osd->logger->inc(l_osd_agent_skip);
    return false;
  }

  dout(10) << __func__ << " flushing " << obc->obs.oi << dendl;

  // FIXME: flush anything dirty, regardless of what distribution of
  // ages we expect.

  hobject_t oid = obc->obs.oi.soid;
  osd->agent_start_op(oid);
  // no need to capture a pg ref, can't outlive fop or ctx
  std::function<void()> on_flush = [this, oid]() {
    osd->agent_finish_op(oid);
  };

  int result = start_flush(
    OpRequestRef(), obc, false, NULL,
    on_flush);
  if (result != -EINPROGRESS) {
    on_flush();
    dout(10) << __func__ << " start_flush() failed " << obc->obs.oi
      << " with " << result << dendl;
    osd->logger->inc(l_osd_agent_skip);
    return false;
  }

  osd->logger->inc(l_osd_agent_flush);
  return true;
}

bool PrimaryLogPG::agent_maybe_evict(ObjectContextRef& obc, bool after_flush)
{
  const hobject_t& soid = obc->obs.oi.soid;
  if (!after_flush && obc->obs.oi.is_dirty()) {
    dout(20) << __func__ << " skip (dirty) " << obc->obs.oi << dendl;
    return false;
  }
  // This is already checked by agent_work() which passes after_flush = false
  if (after_flush && range_intersects_scrub(soid, soid.get_head())) {
      dout(20) << __func__ << " skip (scrubbing) " << obc->obs.oi << dendl;
      return false;
  }
  if (!obc->obs.oi.watchers.empty()) {
    dout(20) << __func__ << " skip (watchers) " << obc->obs.oi << dendl;
    return false;
  }
  if (obc->is_blocked()) {
    dout(20) << __func__ << " skip (blocked) " << obc->obs.oi << dendl;
    return false;
  }
  if (obc->obs.oi.is_cache_pinned()) {
    dout(20) << __func__ << " skip (cache_pinned) " << obc->obs.oi << dendl;
    return false;
  }

  if (soid.snap == CEPH_NOSNAP) {
    int result = _verify_no_head_clones(soid, obc->ssc->snapset);
    if (result < 0) {
      dout(20) << __func__ << " skip (clones) " << obc->obs.oi << dendl;
      return false;
    }
  }

  if (agent_state->evict_mode != TierAgentState::EVICT_MODE_FULL) {
    // is this object old than cache_min_evict_age?
    utime_t now = ceph_clock_now();
    utime_t ob_local_mtime;
    if (obc->obs.oi.local_mtime != utime_t()) {
      ob_local_mtime = obc->obs.oi.local_mtime;
    } else {
      ob_local_mtime = obc->obs.oi.mtime;
    }
    if (ob_local_mtime + utime_t(pool.info.cache_min_evict_age, 0) > now) {
      dout(20) << __func__ << " skip (too young) " << obc->obs.oi << dendl;
      osd->logger->inc(l_osd_agent_skip);
      return false;
    }
    // is this object old and/or cold enough?
    int temp = 0;
    uint64_t temp_upper = 0, temp_lower = 0;
    if (hit_set)
      agent_estimate_temp(soid, &temp);
    agent_state->temp_hist.add(temp);
    agent_state->temp_hist.get_position_micro(temp, &temp_lower, &temp_upper);

    dout(20) << __func__
	     << " temp " << temp
	     << " pos " << temp_lower << "-" << temp_upper
	     << ", evict_effort " << agent_state->evict_effort
	     << dendl;
    dout(30) << "agent_state:\n";
    Formatter *f = Formatter::create("");
    f->open_object_section("agent_state");
    agent_state->dump(f);
    f->close_section();
    f->flush(*_dout);
    delete f;
    *_dout << dendl;

    if (1000000 - temp_upper >= agent_state->evict_effort)
      return false;
  }

  dout(10) << __func__ << " evicting " << obc->obs.oi << dendl;
  OpContextUPtr ctx = simple_opc_create(obc);

  auto null_op_req = OpRequestRef();
  if (!ctx->lock_manager.get_lock_type(
	RWState::RWWRITE,
	obc->obs.oi.soid,
	obc,
	null_op_req)) {
    close_op_ctx(ctx.release());
    dout(20) << __func__ << " skip (cannot get lock) " << obc->obs.oi << dendl;
    return false;
  }

  osd->agent_start_evict_op();
  ctx->register_on_finish(
    [this]() {
      osd->agent_finish_evict_op();
    });

  ctx->at_version = get_next_version();
  ceph_assert(ctx->new_obs.exists);
  int r = _delete_oid(ctx.get(), true, false);
  if (obc->obs.oi.is_omap())
    ctx->delta_stats.num_objects_omap--;
  ctx->delta_stats.num_evict++;
  ctx->delta_stats.num_evict_kb += shift_round_up(obc->obs.oi.size, 10);
  if (obc->obs.oi.is_dirty())
    --ctx->delta_stats.num_objects_dirty;
  ceph_assert(r == 0);
  finish_ctx(ctx.get(), pg_log_entry_t::DELETE);
  simple_opc_submit(std::move(ctx));
  osd->logger->inc(l_osd_tier_evict);
  osd->logger->inc(l_osd_agent_evict);
  return true;
}

void PrimaryLogPG::agent_stop()
{
  dout(20) << __func__ << dendl;
  if (agent_state && !agent_state->is_idle()) {
    agent_state->evict_mode = TierAgentState::EVICT_MODE_IDLE;
    agent_state->flush_mode = TierAgentState::FLUSH_MODE_IDLE;
    osd->agent_disable_pg(this, agent_state->evict_effort);
  }
}

void PrimaryLogPG::agent_delay()
{
  dout(20) << __func__ << dendl;
  if (agent_state && !agent_state->is_idle()) {
    ceph_assert(agent_state->delaying == false);
    agent_state->delaying = true;
    osd->agent_disable_pg(this, agent_state->evict_effort);
  }
}

void PrimaryLogPG::agent_choose_mode_restart()
{
  dout(20) << __func__ << dendl;
  std::scoped_lock locker{*this};
  if (agent_state && agent_state->delaying) {
    agent_state->delaying = false;
    agent_choose_mode(true);
  }
}

bool PrimaryLogPG::agent_choose_mode(bool restart, OpRequestRef op)
{
  bool requeued = false;
  // Let delay play out
  if (agent_state->delaying) {
    dout(20) << __func__ << " " << this << " delaying, ignored" << dendl;
    return requeued;
  }

  TierAgentState::flush_mode_t flush_mode = TierAgentState::FLUSH_MODE_IDLE;
  TierAgentState::evict_mode_t evict_mode = TierAgentState::EVICT_MODE_IDLE;
  unsigned evict_effort = 0;

  if (info.stats.stats_invalid) {
    // idle; stats can't be trusted until we scrub.
    dout(20) << __func__ << " stats invalid (post-split), idle" << dendl;
    goto skip_calc;
  }

  {
  uint64_t divisor = pool.info.get_pg_num_divisor(info.pgid.pgid);
  ceph_assert(divisor > 0);

  // adjust (effective) user objects down based on the number
  // of HitSet objects, which should not count toward our total since
  // they cannot be flushed.
  uint64_t unflushable = info.stats.stats.sum.num_objects_hit_set_archive;

  // also exclude omap objects if ec backing pool
  const pg_pool_t *base_pool = get_osdmap()->get_pg_pool(pool.info.tier_of);
  ceph_assert(base_pool);
  if (!base_pool->supports_omap())
    unflushable += info.stats.stats.sum.num_objects_omap;

  uint64_t num_user_objects = info.stats.stats.sum.num_objects;
  if (num_user_objects > unflushable)
    num_user_objects -= unflushable;
  else
    num_user_objects = 0;

  uint64_t num_user_bytes = info.stats.stats.sum.num_bytes;
  uint64_t unflushable_bytes = info.stats.stats.sum.num_bytes_hit_set_archive;
  num_user_bytes -= unflushable_bytes;
  uint64_t num_overhead_bytes = osd->store->estimate_objects_overhead(num_user_objects);
  num_user_bytes += num_overhead_bytes;

  // also reduce the num_dirty by num_objects_omap
  int64_t num_dirty = info.stats.stats.sum.num_objects_dirty;
  if (!base_pool->supports_omap()) {
    if (num_dirty > info.stats.stats.sum.num_objects_omap)
      num_dirty -= info.stats.stats.sum.num_objects_omap;
    else
      num_dirty = 0;
  }

  dout(10) << __func__
	   << " flush_mode: "
	   << TierAgentState::get_flush_mode_name(agent_state->flush_mode)
	   << " evict_mode: "
	   << TierAgentState::get_evict_mode_name(agent_state->evict_mode)
	   << " num_objects: " << info.stats.stats.sum.num_objects
	   << " num_bytes: " << info.stats.stats.sum.num_bytes
	   << " num_objects_dirty: " << info.stats.stats.sum.num_objects_dirty
	   << " num_objects_omap: " << info.stats.stats.sum.num_objects_omap
	   << " num_dirty: " << num_dirty
	   << " num_user_objects: " << num_user_objects
	   << " num_user_bytes: " << num_user_bytes
	   << " num_overhead_bytes: " << num_overhead_bytes
	   << " pool.info.target_max_bytes: " << pool.info.target_max_bytes
	   << " pool.info.target_max_objects: " << pool.info.target_max_objects
	   << dendl;

  // get dirty, full ratios
  uint64_t dirty_micro = 0;
  uint64_t full_micro = 0;
  if (pool.info.target_max_bytes && num_user_objects > 0) {
    uint64_t avg_size = num_user_bytes / num_user_objects;
    dirty_micro =
      num_dirty * avg_size * 1000000 /
      std::max<uint64_t>(pool.info.target_max_bytes / divisor, 1);
    full_micro =
      num_user_objects * avg_size * 1000000 /
      std::max<uint64_t>(pool.info.target_max_bytes / divisor, 1);
  }
  if (pool.info.target_max_objects > 0) {
    uint64_t dirty_objects_micro =
      num_dirty * 1000000 /
      std::max<uint64_t>(pool.info.target_max_objects / divisor, 1);
    if (dirty_objects_micro > dirty_micro)
      dirty_micro = dirty_objects_micro;
    uint64_t full_objects_micro =
      num_user_objects * 1000000 /
      std::max<uint64_t>(pool.info.target_max_objects / divisor, 1);
    if (full_objects_micro > full_micro)
      full_micro = full_objects_micro;
  }
  dout(20) << __func__ << " dirty " << ((float)dirty_micro / 1000000.0)
	   << " full " << ((float)full_micro / 1000000.0)
	   << dendl;

  // flush mode
  uint64_t flush_target = pool.info.cache_target_dirty_ratio_micro;
  uint64_t flush_high_target = pool.info.cache_target_dirty_high_ratio_micro;
  uint64_t flush_slop = (float)flush_target * cct->_conf->osd_agent_slop;
  if (restart || agent_state->flush_mode == TierAgentState::FLUSH_MODE_IDLE) {
    flush_target += flush_slop;
    flush_high_target += flush_slop;
  } else {
    flush_target -= std::min(flush_target, flush_slop);
    flush_high_target -= std::min(flush_high_target, flush_slop);
  }

  if (dirty_micro > flush_high_target) {
    flush_mode = TierAgentState::FLUSH_MODE_HIGH;
  } else if (dirty_micro > flush_target || (!flush_target && num_dirty > 0)) {
    flush_mode = TierAgentState::FLUSH_MODE_LOW;
  }

  // evict mode
  uint64_t evict_target = pool.info.cache_target_full_ratio_micro;
  uint64_t evict_slop = (float)evict_target * cct->_conf->osd_agent_slop;
  if (restart || agent_state->evict_mode == TierAgentState::EVICT_MODE_IDLE)
    evict_target += evict_slop;
  else
    evict_target -= std::min(evict_target, evict_slop);

  if (full_micro > 1000000) {
    // evict anything clean
    evict_mode = TierAgentState::EVICT_MODE_FULL;
    evict_effort = 1000000;
  } else if (full_micro > evict_target) {
    // set effort in [0..1] range based on where we are between
    evict_mode = TierAgentState::EVICT_MODE_SOME;
    uint64_t over = full_micro - evict_target;
    uint64_t span  = 1000000 - evict_target;
    evict_effort = std::max(over * 1000000 / span,
			    uint64_t(1000000.0 *
				     cct->_conf->osd_agent_min_evict_effort));

    // quantize effort to avoid too much reordering in the agent_queue.
    uint64_t inc = cct->_conf->osd_agent_quantize_effort * 1000000;
    ceph_assert(inc > 0);
    uint64_t was = evict_effort;
    evict_effort -= evict_effort % inc;
    if (evict_effort < inc)
      evict_effort = inc;
    ceph_assert(evict_effort >= inc && evict_effort <= 1000000);
    dout(30) << __func__ << " evict_effort " << was << " quantized by " << inc << " to " << evict_effort << dendl;
  }
  }

  skip_calc:
  bool old_idle = agent_state->is_idle();
  if (flush_mode != agent_state->flush_mode) {
    dout(5) << __func__ << " flush_mode "
	    << TierAgentState::get_flush_mode_name(agent_state->flush_mode)
	    << " -> "
	    << TierAgentState::get_flush_mode_name(flush_mode)
	    << dendl;
    recovery_state.update_stats(
      [=](auto &history, auto &stats) {
	if (flush_mode == TierAgentState::FLUSH_MODE_HIGH) {
	  osd->agent_inc_high_count();
	  stats.stats.sum.num_flush_mode_high = 1;
	} else if (flush_mode == TierAgentState::FLUSH_MODE_LOW) {
	  stats.stats.sum.num_flush_mode_low = 1;
	}
	if (agent_state->flush_mode == TierAgentState::FLUSH_MODE_HIGH) {
	  osd->agent_dec_high_count();
	  stats.stats.sum.num_flush_mode_high = 0;
	} else if (agent_state->flush_mode == TierAgentState::FLUSH_MODE_LOW) {
	  stats.stats.sum.num_flush_mode_low = 0;
	}
	return false;
      });
    agent_state->flush_mode = flush_mode;
  }
  if (evict_mode != agent_state->evict_mode) {
    dout(5) << __func__ << " evict_mode "
	    << TierAgentState::get_evict_mode_name(agent_state->evict_mode)
	    << " -> "
	    << TierAgentState::get_evict_mode_name(evict_mode)
	    << dendl;
    if (agent_state->evict_mode == TierAgentState::EVICT_MODE_FULL &&
	is_active()) {
      if (op)
	requeue_op(op);
      requeue_ops(waiting_for_flush);
      requeue_ops(waiting_for_active);
      requeue_ops(waiting_for_readable);
      requeue_ops(waiting_for_scrub);
      requeue_ops(waiting_for_cache_not_full);
      objects_blocked_on_cache_full.clear();
      requeued = true;
    }
    recovery_state.update_stats(
      [=](auto &history, auto &stats) {
	if (evict_mode == TierAgentState::EVICT_MODE_SOME) {
	  stats.stats.sum.num_evict_mode_some = 1;
	} else if (evict_mode == TierAgentState::EVICT_MODE_FULL) {
	  stats.stats.sum.num_evict_mode_full = 1;
	}
	if (agent_state->evict_mode == TierAgentState::EVICT_MODE_SOME) {
	  stats.stats.sum.num_evict_mode_some = 0;
	} else if (agent_state->evict_mode == TierAgentState::EVICT_MODE_FULL) {
	  stats.stats.sum.num_evict_mode_full = 0;
	}
	return false;
      });
    agent_state->evict_mode = evict_mode;
  }
  uint64_t old_effort = agent_state->evict_effort;
  if (evict_effort != agent_state->evict_effort) {
    dout(5) << __func__ << " evict_effort "
	    << ((float)agent_state->evict_effort / 1000000.0)
	    << " -> "
	    << ((float)evict_effort / 1000000.0)
	    << dendl;
    agent_state->evict_effort = evict_effort;
  }

  // NOTE: we are using evict_effort as a proxy for *all* agent effort
  // (including flush).  This is probably fine (they should be
  // correlated) but it is not precisely correct.
  if (agent_state->is_idle()) {
    if (!restart && !old_idle) {
      osd->agent_disable_pg(this, old_effort);
    }
  } else {
    if (restart || old_idle) {
      osd->agent_enable_pg(this, agent_state->evict_effort);
    } else if (old_effort != agent_state->evict_effort) {
      osd->agent_adjust_pg(this, old_effort, agent_state->evict_effort);
    }
  }
  return requeued;
}

void PrimaryLogPG::agent_estimate_temp(const hobject_t& oid, int *temp)
{
  ceph_assert(hit_set);
  ceph_assert(temp);
  *temp = 0;
  if (hit_set->contains(oid))
    *temp = 1000000;
  unsigned i = 0;
  int last_n = pool.info.hit_set_search_last_n;
  for (map<time_t,HitSetRef>::reverse_iterator p =
       agent_state->hit_set_map.rbegin(); last_n > 0 &&
       p != agent_state->hit_set_map.rend(); ++p, ++i) {
    if (p->second->contains(oid)) {
      *temp += pool.info.get_grade(i);
      --last_n;
    }
  }
}

// Dup op detection

bool PrimaryLogPG::already_complete(eversion_t v)
{
  dout(20) << __func__ << ": " << v << dendl;
  for (xlist<RepGather*>::iterator i = repop_queue.begin();
       !i.end();
       ++i) {
    dout(20) << __func__ << ": " << **i << dendl;
    // skip copy from temp object ops
    if ((*i)->v == eversion_t()) {
      dout(20) << __func__ << ": " << **i
	       << " version is empty" << dendl;
      continue;
    }
    if ((*i)->v > v) {
      dout(20) << __func__ << ": " << **i
	       << " (*i)->v past v" << dendl;
      break;
    }
    if (!(*i)->all_committed) {
      dout(20) << __func__ << ": " << **i
	       << " not committed, returning false"
	       << dendl;
      return false;
    }
  }
  dout(20) << __func__ << ": returning true" << dendl;
  return true;
}


// ==========================================================================================
// SCRUB


bool PrimaryLogPG::_range_available_for_scrub(
  const hobject_t &begin, const hobject_t &end)
{
  pair<hobject_t, ObjectContextRef> next;
  next.second = object_contexts.lookup(begin);
  next.first = begin;
  bool more = true;
  while (more && next.first < end) {
    if (next.second && next.second->is_blocked()) {
      next.second->requeue_scrub_on_unblock = true;
      dout(10) << __func__ << ": scrub delayed, "
	       << next.first << " is blocked"
	       << dendl;
      return false;
    }
    more = object_contexts.get_next(next.first, &next);
  }
  return true;
}

static bool doing_clones(const std::optional<SnapSet> &snapset,
			 const vector<snapid_t>::reverse_iterator &curclone) {
    return snapset && curclone != snapset->clones.rend();
}

void PrimaryLogPG::log_missing(unsigned missing,
			const std::optional<hobject_t> &head,
			LogChannelRef clog,
			const spg_t &pgid,
			const char *func,
			const char *mode,
			bool allow_incomplete_clones)
{
  ceph_assert(head);
  if (allow_incomplete_clones) {
    dout(20) << func << " " << mode << " " << pgid << " " << *head
	     << " skipped " << missing << " clone(s) in cache tier" << dendl;
  } else {
    clog->info() << mode << " " << pgid << " " << *head
		 << " : " << missing << " missing clone(s)";
  }
}

unsigned PrimaryLogPG::process_clones_to(const std::optional<hobject_t> &head,
  const std::optional<SnapSet> &snapset,
  LogChannelRef clog,
  const spg_t &pgid,
  const char *mode,
  bool allow_incomplete_clones,
  std::optional<snapid_t> target,
  vector<snapid_t>::reverse_iterator *curclone,
  inconsistent_snapset_wrapper &e)
{
  ceph_assert(head);
  ceph_assert(snapset);
  unsigned missing = 0;

  // NOTE: clones are in descending order, thus **curclone > target test here
  hobject_t next_clone(*head);
  while(doing_clones(snapset, *curclone) && (!target || **curclone > *target)) {
    ++missing;
    // it is okay to be missing one or more clones in a cache tier.
    // skip higher-numbered clones in the list.
    if (!allow_incomplete_clones) {
      next_clone.snap = **curclone;
      clog->error() << mode << " " << pgid << " " << *head
			 << " : expected clone " << next_clone << " " << missing
                         << " missing";
      ++scrubber.shallow_errors;
      e.set_clone_missing(next_clone.snap);
    }
    // Clones are descending
    ++(*curclone);
  }
  return missing;
}

/*
 * Validate consistency of the object info and snap sets.
 *
 * We are sort of comparing 2 lists. The main loop is on objmap.objects. But
 * the comparison of the objects is against multiple snapset.clones. There are
 * multiple clone lists and in between lists we expect head.
 *
 * Example
 *
 * objects              expected
 * =======              =======
 * obj1 snap 1          head, unexpected obj1 snap 1
 * obj2 head            head, match
 *              [SnapSet clones 6 4 2 1]
 * obj2 snap 7          obj2 snap 6, unexpected obj2 snap 7
 * obj2 snap 6          obj2 snap 6, match
 * obj2 snap 4          obj2 snap 4, match
 * obj3 head            obj2 snap 2 (expected), obj2 snap 1 (expected), match
 *              [Snapset clones 3 1]
 * obj3 snap 3          obj3 snap 3 match
 * obj3 snap 1          obj3 snap 1 match
 * obj4 head            head, match
 *              [Snapset clones 4]
 * EOL                  obj4 snap 4, (expected)
 */
void PrimaryLogPG::scrub_snapshot_metadata(
  ScrubMap &scrubmap,
  const map<hobject_t,
            pair<std::optional<uint32_t>,
                 std::optional<uint32_t>>> &missing_digest)
{
  dout(10) << __func__ << dendl;

  bool repair = state_test(PG_STATE_REPAIR);
  bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
  const char *mode = (repair ? "repair": (deep_scrub ? "deep-scrub" : "scrub"));
  std::optional<snapid_t> all_clones;   // Unspecified snapid_t or std::nullopt

  // traverse in reverse order.
  std::optional<hobject_t> head;
  std::optional<SnapSet> snapset; // If initialized so will head (above)
  vector<snapid_t>::reverse_iterator curclone; // Defined only if snapset initialized
  unsigned missing = 0;
  inconsistent_snapset_wrapper soid_error, head_error;
  unsigned soid_error_count = 0;

  for (map<hobject_t,ScrubMap::object>::reverse_iterator
       p = scrubmap.objects.rbegin(); p != scrubmap.objects.rend(); ++p) {
    const hobject_t& soid = p->first;
    ceph_assert(!soid.is_snapdir());
    soid_error = inconsistent_snapset_wrapper{soid};
    object_stat_sum_t stat;
    std::optional<object_info_t> oi;

    stat.num_objects++;

    if (soid.nspace == cct->_conf->osd_hit_set_namespace)
      stat.num_objects_hit_set_archive++;

    if (soid.is_snap()) {
      // it's a clone
      stat.num_object_clones++;
    }

    // basic checks.
    if (p->second.attrs.count(OI_ATTR) == 0) {
      oi = std::nullopt;
      osd->clog->error() << mode << " " << info.pgid << " " << soid
			<< " : no '" << OI_ATTR << "' attr";
      ++scrubber.shallow_errors;
      soid_error.set_info_missing();
    } else {
      bufferlist bv;
      bv.push_back(p->second.attrs[OI_ATTR]);
      try {
	oi = object_info_t(); // Initialize optional<> before decode into it
	oi->decode(bv);
      } catch (ceph::buffer::error& e) {
	oi = std::nullopt;
	osd->clog->error() << mode << " " << info.pgid << " " << soid
		<< " : can't decode '" << OI_ATTR << "' attr " << e.what();
	++scrubber.shallow_errors;
	soid_error.set_info_corrupted();
        soid_error.set_info_missing(); // Not available too
      }
    }

    if (oi) {
      if (pgbackend->be_get_ondisk_size(oi->size) != p->second.size) {
	osd->clog->error() << mode << " " << info.pgid << " " << soid
			   << " : on disk size (" << p->second.size
			   << ") does not match object info size ("
			   << oi->size << ") adjusted for ondisk to ("
			   << pgbackend->be_get_ondisk_size(oi->size)
			   << ")";
	soid_error.set_size_mismatch();
	++scrubber.shallow_errors;
      }

      dout(20) << mode << "  " << soid << " " << *oi << dendl;

      // A clone num_bytes will be added later when we have snapset
      if (!soid.is_snap()) {
        stat.num_bytes += oi->size;
      }
      if (soid.nspace == cct->_conf->osd_hit_set_namespace)
	stat.num_bytes_hit_set_archive += oi->size;

      if (oi->is_dirty())
	++stat.num_objects_dirty;
      if (oi->is_whiteout())
	++stat.num_whiteouts;
      if (oi->is_omap())
	++stat.num_objects_omap;
      if (oi->is_cache_pinned())
	++stat.num_objects_pinned;
      if (oi->has_manifest())
	++stat.num_objects_manifest;
    }

    // Check for any problems while processing clones
    if (doing_clones(snapset, curclone)) {
      std::optional<snapid_t> target;
      // Expecting an object with snap for current head
      if (soid.has_snapset() || soid.get_head() != head->get_head()) {

	dout(10) << __func__ << " " << mode << " " << info.pgid << " new object "
		 << soid << " while processing " << *head << dendl;

        target = all_clones;
      } else {
        ceph_assert(soid.is_snap());
        target = soid.snap;
      }

      // Log any clones we were expecting to be there up to target
      // This will set missing, but will be a no-op if snap.soid == *curclone.
      missing += process_clones_to(head, snapset, osd->clog, info.pgid, mode,
		        pool.info.allow_incomplete_clones(), target, &curclone,
			head_error);
    }
    bool expected;
    // Check doing_clones() again in case we ran process_clones_to()
    if (doing_clones(snapset, curclone)) {
      // A head would have processed all clones above
      // or all greater than *curclone.
      ceph_assert(soid.is_snap() && *curclone <= soid.snap);

      // After processing above clone snap should match the expected curclone
      expected = (*curclone == soid.snap);
    } else {
      // If we aren't doing clones any longer, then expecting head
      expected = soid.has_snapset();
    }
    if (!expected) {
      // If we couldn't read the head's snapset, just ignore clones
      if (head && !snapset) {
	osd->clog->error() << mode << " " << info.pgid << " " << soid
			  << " : clone ignored due to missing snapset";
      } else {
	osd->clog->error() << mode << " " << info.pgid << " " << soid
			   << " : is an unexpected clone";
      }
      ++scrubber.shallow_errors;
      soid_error.set_headless();
      scrubber.store->add_snap_error(pool.id, soid_error);
      ++soid_error_count;
      if (head && soid.get_head() == head->get_head())
	head_error.set_clone(soid.snap);
      continue;
    }

    // new snapset?
    if (soid.has_snapset()) {

      if (missing) {
	log_missing(missing, head, osd->clog, info.pgid, __func__, mode,
		    pool.info.allow_incomplete_clones());
      }

      // Save previous head error information
      if (head && (head_error.errors || soid_error_count))
	scrubber.store->add_snap_error(pool.id, head_error);
      // Set this as a new head object
      head = soid;
      missing = 0;
      head_error = soid_error;
      soid_error_count = 0;

      dout(20) << __func__ << " " << mode << " new head " << head << dendl;

      if (p->second.attrs.count(SS_ATTR) == 0) {
	osd->clog->error() << mode << " " << info.pgid << " " << soid
			  << " : no '" << SS_ATTR << "' attr";
        ++scrubber.shallow_errors;
	snapset = std::nullopt;
	head_error.set_snapset_missing();
      } else {
	bufferlist bl;
	bl.push_back(p->second.attrs[SS_ATTR]);
	auto blp = bl.cbegin();
        try {
	  snapset = SnapSet(); // Initialize optional<> before decoding into it
	  decode(*snapset, blp);
          head_error.ss_bl.push_back(p->second.attrs[SS_ATTR]);
        } catch (ceph::buffer::error& e) {
	  snapset = std::nullopt;
          osd->clog->error() << mode << " " << info.pgid << " " << soid
		<< " : can't decode '" << SS_ATTR << "' attr " << e.what();
	  ++scrubber.shallow_errors;
	  head_error.set_snapset_corrupted();
        }
      }

      if (snapset) {
	// what will be next?
	curclone = snapset->clones.rbegin();

	if (!snapset->clones.empty()) {
	  dout(20) << "  snapset " << *snapset << dendl;
	  if (snapset->seq == 0) {
	    osd->clog->error() << mode << " " << info.pgid << " " << soid
			       << " : snaps.seq not set";
	    ++scrubber.shallow_errors;
	    head_error.set_snapset_error();
          }
	}
      }
    } else {
      ceph_assert(soid.is_snap());
      ceph_assert(head);
      ceph_assert(snapset);
      ceph_assert(soid.snap == *curclone);

      dout(20) << __func__ << " " << mode << " matched clone " << soid << dendl;

      if (snapset->clone_size.count(soid.snap) == 0) {
	osd->clog->error() << mode << " " << info.pgid << " " << soid
			   << " : is missing in clone_size";
	++scrubber.shallow_errors;
	soid_error.set_size_mismatch();
      } else {
        if (oi && oi->size != snapset->clone_size[soid.snap]) {
	  osd->clog->error() << mode << " " << info.pgid << " " << soid
			     << " : size " << oi->size << " != clone_size "
			     << snapset->clone_size[*curclone];
	  ++scrubber.shallow_errors;
	  soid_error.set_size_mismatch();
        }

        if (snapset->clone_overlap.count(soid.snap) == 0) {
	  osd->clog->error() << mode << " " << info.pgid << " " << soid
			     << " : is missing in clone_overlap";
	  ++scrubber.shallow_errors;
	  soid_error.set_size_mismatch();
	} else {
	  // This checking is based on get_clone_bytes().  The first 2 asserts
	  // can't happen because we know we have a clone_size and
	  // a clone_overlap.  Now we check that the interval_set won't
	  // cause the last assert.
	  uint64_t size = snapset->clone_size.find(soid.snap)->second;
	  const interval_set<uint64_t> &overlap =
	        snapset->clone_overlap.find(soid.snap)->second;
	  bool bad_interval_set = false;
	  for (interval_set<uint64_t>::const_iterator i = overlap.begin();
	       i != overlap.end(); ++i) {
	    if (size < i.get_len()) {
	      bad_interval_set = true;
	      break;
	    }
	    size -= i.get_len();
	  }

	  if (bad_interval_set) {
	    osd->clog->error() << mode << " " << info.pgid << " " << soid
			       << " : bad interval_set in clone_overlap";
	    ++scrubber.shallow_errors;
	    soid_error.set_size_mismatch();
	  } else {
            stat.num_bytes += snapset->get_clone_bytes(soid.snap);
	  }
        }
      }

      // what's next?
      ++curclone;
      if (soid_error.errors) {
        scrubber.store->add_snap_error(pool.id, soid_error);
	++soid_error_count;
      }
    }

    scrub_cstat.add(stat);
  }

  if (doing_clones(snapset, curclone)) {
    dout(10) << __func__ << " " << mode << " " << info.pgid
	     << " No more objects while processing " << *head << dendl;

    missing += process_clones_to(head, snapset, osd->clog, info.pgid, mode,
		      pool.info.allow_incomplete_clones(), all_clones, &curclone,
		      head_error);
  }
  // There could be missing found by the test above or even
  // before dropping out of the loop for the last head.
  if (missing) {
    log_missing(missing, head, osd->clog, info.pgid, __func__,
		mode, pool.info.allow_incomplete_clones());
  }
  if (head && (head_error.errors || soid_error_count))
    scrubber.store->add_snap_error(pool.id, head_error);

  for (auto p = missing_digest.begin(); p != missing_digest.end(); ++p) {
    ceph_assert(!p->first.is_snapdir());
    dout(10) << __func__ << " recording digests for " << p->first << dendl;
    ObjectContextRef obc = get_object_context(p->first, false);
    if (!obc) {
      osd->clog->error() << info.pgid << " " << mode
			 << " cannot get object context for object "
			 << p->first;
      continue;
    } else if (obc->obs.oi.soid != p->first) {
      osd->clog->error() << info.pgid << " " << mode
			 << " " << p->first
			 << " : object has a valid oi attr with a mismatched name, "
			 << " obc->obs.oi.soid: " << obc->obs.oi.soid;
      continue;
    }
    OpContextUPtr ctx = simple_opc_create(obc);
    ctx->at_version = get_next_version();
    ctx->mtime = utime_t();      // do not update mtime
    if (p->second.first) {
      ctx->new_obs.oi.set_data_digest(*p->second.first);
    } else {
      ctx->new_obs.oi.clear_data_digest();
    }
    if (p->second.second) {
      ctx->new_obs.oi.set_omap_digest(*p->second.second);
    } else {
      ctx->new_obs.oi.clear_omap_digest();
    }
    finish_ctx(ctx.get(), pg_log_entry_t::MODIFY);

    ctx->register_on_success(
      [this]() {
	dout(20) << "updating scrub digest" << dendl;
	if (--scrubber.num_digest_updates_pending == 0) {
	  requeue_scrub();
	}
      });

    simple_opc_submit(std::move(ctx));
    ++scrubber.num_digest_updates_pending;
  }

  dout(10) << __func__ << " (" << mode << ") finish" << dendl;
}

void PrimaryLogPG::_scrub_clear_state()
{
  scrub_cstat = object_stat_collection_t();
}

void PrimaryLogPG::_scrub_finish()
{
  bool repair = state_test(PG_STATE_REPAIR);
  bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
  const char *mode = (repair ? "repair": (deep_scrub ? "deep-scrub" : "scrub"));

  if (info.stats.stats_invalid) {
    recovery_state.update_stats(
      [=](auto &history, auto &stats) {
	stats.stats = scrub_cstat;
	stats.stats_invalid = false;
	return false;
      });

    if (agent_state)
      agent_choose_mode();
  }

  dout(10) << mode << " got "
	   << scrub_cstat.sum.num_objects << "/" << info.stats.stats.sum.num_objects << " objects, "
	   << scrub_cstat.sum.num_object_clones << "/" << info.stats.stats.sum.num_object_clones << " clones, "
	   << scrub_cstat.sum.num_objects_dirty << "/" << info.stats.stats.sum.num_objects_dirty << " dirty, "
	   << scrub_cstat.sum.num_objects_omap << "/" << info.stats.stats.sum.num_objects_omap << " omap, "
	   << scrub_cstat.sum.num_objects_pinned << "/" << info.stats.stats.sum.num_objects_pinned << " pinned, "
	   << scrub_cstat.sum.num_objects_hit_set_archive << "/" << info.stats.stats.sum.num_objects_hit_set_archive << " hit_set_archive, "
	   << scrub_cstat.sum.num_bytes << "/" << info.stats.stats.sum.num_bytes << " bytes, "
	   << scrub_cstat.sum.num_objects_manifest << "/" << info.stats.stats.sum.num_objects_manifest << " manifest objects, "
	   << scrub_cstat.sum.num_bytes_hit_set_archive << "/" << info.stats.stats.sum.num_bytes_hit_set_archive << " hit_set_archive bytes."
	   << dendl;

  if (scrub_cstat.sum.num_objects != info.stats.stats.sum.num_objects ||
      scrub_cstat.sum.num_object_clones != info.stats.stats.sum.num_object_clones ||
      (scrub_cstat.sum.num_objects_dirty != info.stats.stats.sum.num_objects_dirty &&
       !info.stats.dirty_stats_invalid) ||
      (scrub_cstat.sum.num_objects_omap != info.stats.stats.sum.num_objects_omap &&
       !info.stats.omap_stats_invalid) ||
      (scrub_cstat.sum.num_objects_pinned != info.stats.stats.sum.num_objects_pinned &&
       !info.stats.pin_stats_invalid) ||
      (scrub_cstat.sum.num_objects_hit_set_archive != info.stats.stats.sum.num_objects_hit_set_archive &&
       !info.stats.hitset_stats_invalid) ||
      (scrub_cstat.sum.num_bytes_hit_set_archive != info.stats.stats.sum.num_bytes_hit_set_archive &&
       !info.stats.hitset_bytes_stats_invalid) ||
      (scrub_cstat.sum.num_objects_manifest != info.stats.stats.sum.num_objects_manifest &&
       !info.stats.manifest_stats_invalid) ||
      scrub_cstat.sum.num_whiteouts != info.stats.stats.sum.num_whiteouts ||
      scrub_cstat.sum.num_bytes != info.stats.stats.sum.num_bytes) {
    osd->clog->error() << info.pgid << " " << mode
		      << " : stat mismatch, got "
		      << scrub_cstat.sum.num_objects << "/" << info.stats.stats.sum.num_objects << " objects, "
		      << scrub_cstat.sum.num_object_clones << "/" << info.stats.stats.sum.num_object_clones << " clones, "
		      << scrub_cstat.sum.num_objects_dirty << "/" << info.stats.stats.sum.num_objects_dirty << " dirty, "
		      << scrub_cstat.sum.num_objects_omap << "/" << info.stats.stats.sum.num_objects_omap << " omap, "
		      << scrub_cstat.sum.num_objects_pinned << "/" << info.stats.stats.sum.num_objects_pinned << " pinned, "
		      << scrub_cstat.sum.num_objects_hit_set_archive << "/" << info.stats.stats.sum.num_objects_hit_set_archive << " hit_set_archive, "
		      << scrub_cstat.sum.num_whiteouts << "/" << info.stats.stats.sum.num_whiteouts << " whiteouts, "
		      << scrub_cstat.sum.num_bytes << "/" << info.stats.stats.sum.num_bytes << " bytes, "
		      << scrub_cstat.sum.num_objects_manifest << "/" << info.stats.stats.sum.num_objects_manifest << " manifest objects, "
		      << scrub_cstat.sum.num_bytes_hit_set_archive << "/" << info.stats.stats.sum.num_bytes_hit_set_archive << " hit_set_archive bytes.";
    ++scrubber.shallow_errors;

    if (repair) {
      ++scrubber.fixed;
      recovery_state.update_stats(
	[this](auto &history, auto &stats) {
	  stats.stats = scrub_cstat;
	  stats.dirty_stats_invalid = false;
	  stats.omap_stats_invalid = false;
	  stats.hitset_stats_invalid = false;
	  stats.hitset_bytes_stats_invalid = false;
	  stats.pin_stats_invalid = false;
	  stats.manifest_stats_invalid = false;
	  return false;
	});
      publish_stats_to_osd();
      recovery_state.share_pg_info();
    }
  }
  // Clear object context cache to get repair information
  if (repair)
    object_contexts.clear();
}

int PrimaryLogPG::rep_repair_primary_object(const hobject_t& soid, OpContext *ctx)
{
  OpRequestRef op = ctx->op;
  // Only supports replicated pools
  ceph_assert(!pool.info.is_erasure());
  ceph_assert(is_primary());

  dout(10) << __func__ << " " << soid
	   << " peers osd.{" << get_acting_recovery_backfill() << "}" << dendl;

  if (!is_clean()) {
    block_for_clean(soid, op);
    return -EAGAIN;
  }

  ceph_assert(!recovery_state.get_pg_log().get_missing().is_missing(soid));
  auto& oi = ctx->new_obs.oi;
  eversion_t v = oi.version;

  if (primary_error(soid, v)) {
    dout(0) << __func__ << " No other replicas available for " << soid << dendl;
    // XXX: If we knew that there is no down osd which could include this
    // object, it would be nice if we could return EIO here.
    // If a "never fail" flag was available, that could be used
    // for rbd to NOT return EIO until object marked lost.

    // Drop through to save this op in case an osd comes up with the object.
  }

  // Restart the op after object becomes readable again
  waiting_for_unreadable_object[soid].push_back(op);
  op->mark_delayed("waiting for missing object");

  if (!eio_errors_to_process) {
    eio_errors_to_process = true;
    ceph_assert(is_clean());
    state_set(PG_STATE_REPAIR);
    state_clear(PG_STATE_CLEAN);
    queue_peering_event(
        PGPeeringEventRef(
	  std::make_shared<PGPeeringEvent>(
	  get_osdmap_epoch(),
	  get_osdmap_epoch(),
	  PeeringState::DoRecovery())));
  } else {
    // A prior error must have already cleared clean state and queued recovery
    // or a map change has triggered re-peering.
    // Not inlining the recovery by calling maybe_kick_recovery(soid);
    dout(5) << __func__<< ": Read error on " << soid << ", but already seen errors" << dendl;
  }

  return -EAGAIN;
}

/*---SnapTrimmer Logging---*/
#undef dout_prefix
#define dout_prefix pg->gen_prefix(*_dout)

void PrimaryLogPG::SnapTrimmer::log_enter(const char *state_name)
{
  ldout(pg->cct, 20) << "enter " << state_name << dendl;
}

void PrimaryLogPG::SnapTrimmer::log_exit(const char *state_name, utime_t enter_time)
{
  ldout(pg->cct, 20) << "exit " << state_name << dendl;
}

/*---SnapTrimmer states---*/
#undef dout_prefix
#define dout_prefix (context< SnapTrimmer >().pg->gen_prefix(*_dout) \
		     << "SnapTrimmer state<" << get_state_name() << ">: ")

/* NotTrimming */
PrimaryLogPG::NotTrimming::NotTrimming(my_context ctx)
  : my_base(ctx), 
    NamedState(nullptr, "NotTrimming")
{
  context< SnapTrimmer >().log_enter(state_name);
}

void PrimaryLogPG::NotTrimming::exit()
{
  context< SnapTrimmer >().log_exit(state_name, enter_time);
}

boost::statechart::result PrimaryLogPG::NotTrimming::react(const KickTrim&)
{
  PrimaryLogPG *pg = context< SnapTrimmer >().pg;
  ldout(pg->cct, 10) << "NotTrimming react KickTrim" << dendl;

  if (!(pg->is_primary() && pg->is_active())) {
    ldout(pg->cct, 10) << "NotTrimming not primary or active" << dendl;
    return discard_event();
  }
  if (!pg->is_clean() ||
      pg->snap_trimq.empty()) {
    ldout(pg->cct, 10) << "NotTrimming not clean or nothing to trim" << dendl;
    return discard_event();
  }
  if (pg->scrubber.active) {
    ldout(pg->cct, 10) << " scrubbing, will requeue snap_trimmer after" << dendl;
    return transit< WaitScrub >();
  } else {
    return transit< Trimming >();
  }
}

boost::statechart::result PrimaryLogPG::WaitReservation::react(const SnapTrimReserved&)
{
  PrimaryLogPG *pg = context< SnapTrimmer >().pg;
  ldout(pg->cct, 10) << "WaitReservation react SnapTrimReserved" << dendl;

  pending = nullptr;
  if (!context< SnapTrimmer >().can_trim()) {
    post_event(KickTrim());
    return transit< NotTrimming >();
  }

  context<Trimming>().snap_to_trim = pg->snap_trimq.range_start();
  ldout(pg->cct, 10) << "NotTrimming: trimming "
		     << pg->snap_trimq.range_start()
		     << dendl;
  return transit< AwaitAsyncWork >();
}

/* AwaitAsyncWork */
PrimaryLogPG::AwaitAsyncWork::AwaitAsyncWork(my_context ctx)
  : my_base(ctx),
    NamedState(nullptr, "Trimming/AwaitAsyncWork")
{
  auto *pg = context< SnapTrimmer >().pg;
  context< SnapTrimmer >().log_enter(state_name);
  context< SnapTrimmer >().pg->osd->queue_for_snap_trim(pg);
  pg->state_set(PG_STATE_SNAPTRIM);
  pg->state_clear(PG_STATE_SNAPTRIM_ERROR);
  pg->publish_stats_to_osd();
}

boost::statechart::result PrimaryLogPG::AwaitAsyncWork::react(const DoSnapWork&)
{
  PrimaryLogPGRef pg = context< SnapTrimmer >().pg;
  snapid_t snap_to_trim = context<Trimming>().snap_to_trim;
  auto &in_flight = context<Trimming>().in_flight;
  ceph_assert(in_flight.empty());

  ceph_assert(pg->is_primary() && pg->is_active());
  if (!context< SnapTrimmer >().can_trim()) {
    ldout(pg->cct, 10) << "something changed, reverting to NotTrimming" << dendl;
    post_event(KickTrim());
    return transit< NotTrimming >();
  }

  ldout(pg->cct, 10) << "AwaitAsyncWork: trimming snap " << snap_to_trim << dendl;

  vector<hobject_t> to_trim;
  unsigned max = pg->cct->_conf->osd_pg_max_concurrent_snap_trims;
  to_trim.reserve(max);
  int r = pg->snap_mapper.get_next_objects_to_trim(
    snap_to_trim,
    max,
    &to_trim);
  if (r != 0 && r != -ENOENT) {
    lderr(pg->cct) << "get_next_objects_to_trim returned "
		   << cpp_strerror(r) << dendl;
    ceph_abort_msg("get_next_objects_to_trim returned an invalid code");
  } else if (r == -ENOENT) {
    // Done!
    ldout(pg->cct, 10) << "got ENOENT" << dendl;

    pg->snap_trimq.erase(snap_to_trim);

    if (pg->snap_trimq_repeat.count(snap_to_trim)) {
      ldout(pg->cct, 10) << " removing from snap_trimq_repeat" << dendl;
      pg->snap_trimq_repeat.erase(snap_to_trim);
    } else {
      ldout(pg->cct, 10) << "adding snap " << snap_to_trim
			 << " to purged_snaps"
			 << dendl;
      ObjectStore::Transaction t;
      pg->recovery_state.adjust_purged_snaps(
	[snap_to_trim](auto &purged_snaps) {
	  purged_snaps.insert(snap_to_trim);
	});
      pg->write_if_dirty(t);

      ldout(pg->cct, 10) << "purged_snaps now "
			 << pg->info.purged_snaps << ", snap_trimq now "
			 << pg->snap_trimq << dendl;

      int tr = pg->osd->store->queue_transaction(pg->ch, std::move(t), NULL);
      ceph_assert(tr == 0);

      pg->recovery_state.share_pg_info();
    }
    post_event(KickTrim());
    return transit< NotTrimming >();
  }
  ceph_assert(!to_trim.empty());

  for (auto &&object: to_trim) {
    // Get next
    ldout(pg->cct, 10) << "AwaitAsyncWork react trimming " << object << dendl;
    OpContextUPtr ctx;
    int error = pg->trim_object(in_flight.empty(), object, snap_to_trim, &ctx);
    if (error) {
      if (error == -ENOLCK) {
	ldout(pg->cct, 10) << "could not get write lock on obj "
			   << object << dendl;
      } else {
	pg->state_set(PG_STATE_SNAPTRIM_ERROR);
	ldout(pg->cct, 10) << "Snaptrim error=" << error << dendl;
      }
      if (!in_flight.empty()) {
	ldout(pg->cct, 10) << "letting the ones we already started finish" << dendl;
	return transit< WaitRepops >();
      }
      if (error == -ENOLCK) {
	ldout(pg->cct, 10) << "waiting for it to clear"
			   << dendl;
	return transit< WaitRWLock >();
      } else {
        return transit< NotTrimming >();
      }
    }

    in_flight.insert(object);
    ctx->register_on_success(
      [pg, object, &in_flight]() {
	ceph_assert(in_flight.find(object) != in_flight.end());
	in_flight.erase(object);
	if (in_flight.empty()) {
	  if (pg->state_test(PG_STATE_SNAPTRIM_ERROR)) {
	    pg->snap_trimmer_machine.process_event(Reset());
	  } else {
	    pg->snap_trimmer_machine.process_event(RepopsComplete());
	  }
	}
      });

    pg->simple_opc_submit(std::move(ctx));
  }

  return transit< WaitRepops >();
}

void PrimaryLogPG::setattr_maybe_cache(
  ObjectContextRef obc,
  PGTransaction *t,
  const string &key,
  bufferlist &val)
{
  t->setattr(obc->obs.oi.soid, key, val);
}

void PrimaryLogPG::setattrs_maybe_cache(
  ObjectContextRef obc,
  PGTransaction *t,
  map<string, bufferlist> &attrs)
{
  t->setattrs(obc->obs.oi.soid, attrs);
}

void PrimaryLogPG::rmattr_maybe_cache(
  ObjectContextRef obc,
  PGTransaction *t,
  const string &key)
{
  t->rmattr(obc->obs.oi.soid, key);
}

int PrimaryLogPG::getattr_maybe_cache(
  ObjectContextRef obc,
  const string &key,
  bufferlist *val)
{
  if (pool.info.is_erasure()) {
    map<string, bufferlist>::iterator i = obc->attr_cache.find(key);
    if (i != obc->attr_cache.end()) {
      if (val)
	*val = i->second;
      return 0;
    } else {
      return -ENODATA;
    }
  }
  return pgbackend->objects_get_attr(obc->obs.oi.soid, key, val);
}

int PrimaryLogPG::getattrs_maybe_cache(
  ObjectContextRef obc,
  map<string, bufferlist> *out)
{
  int r = 0;
  ceph_assert(out);
  if (pool.info.is_erasure()) {
    *out = obc->attr_cache;
  } else {
    r = pgbackend->objects_get_attrs(obc->obs.oi.soid, out);
  }
  map<string, bufferlist> tmp;
  for (map<string, bufferlist>::iterator i = out->begin();
       i != out->end();
       ++i) {
    if (i->first.size() > 1 && i->first[0] == '_')
      tmp[i->first.substr(1, i->first.size())].claim(i->second);
  }
  tmp.swap(*out);
  return r;
}

bool PrimaryLogPG::check_failsafe_full() {
    return osd->check_failsafe_full(get_dpp());
}

void intrusive_ptr_add_ref(PrimaryLogPG *pg) { pg->get("intptr"); }
void intrusive_ptr_release(PrimaryLogPG *pg) { pg->put("intptr"); }

#ifdef PG_DEBUG_REFS
uint64_t get_with_id(PrimaryLogPG *pg) { return pg->get_with_id(); }
void put_with_id(PrimaryLogPG *pg, uint64_t id) { return pg->put_with_id(id); }
#endif

void intrusive_ptr_add_ref(PrimaryLogPG::RepGather *repop) { repop->get(); }
void intrusive_ptr_release(PrimaryLogPG::RepGather *repop) { repop->put(); }
