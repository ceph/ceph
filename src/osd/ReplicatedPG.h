// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_REPLICATEDPG_H
#define CEPH_REPLICATEDPG_H

#include <boost/optional/optional_io.hpp>
#include <boost/tuple/tuple.hpp>

#include "include/assert.h" 
#include "include/unordered_map.h"
#include "common/cmdparse.h"

#include "HitSet.h"
#include "OSD.h"
#include "PG.h"
#include "Watch.h"
#include "OpRequest.h"
#include "TierAgentState.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDSubOp.h"

#include "common/sharedptr_registry.hpp"

#include "PGBackend.h"
#include "ReplicatedBackend.h"
#include "ECBackend.h"

class MOSDSubOpReply;

class CopyFromCallback;
class PromoteCallback;

class ReplicatedPG;
class PGLSFilter;
void intrusive_ptr_add_ref(ReplicatedPG *pg);
void intrusive_ptr_release(ReplicatedPG *pg);
uint64_t get_with_id(ReplicatedPG *pg);
void put_with_id(ReplicatedPG *pg, uint64_t id);

#ifdef PG_DEBUG_REFS
  typedef TrackedIntPtr<ReplicatedPG> ReplicatedPGRef;
#else
  typedef boost::intrusive_ptr<ReplicatedPG> ReplicatedPGRef;
#endif

class ReplicatedPG : public PG, public PGBackend::Listener {
  friend class OSD;
  friend class Watch;

public:

  /*
   * state associated with a copy operation
   */
  struct OpContext;
  class CopyCallback;

  /**
   * CopyResults stores the object metadata of interest to a copy initiator.
   */
  struct CopyResults {
    ceph::real_time mtime; ///< the copy source's mtime
    uint64_t object_size; ///< the copied object's size
    bool started_temp_obj; ///< true if the callback needs to delete temp object
    hobject_t temp_oid;    ///< temp object (if any)
    /**
     * Final transaction; if non-empty the callback must execute it before any
     * other accesses to the object (in order to complete the copy).
     */
    PGBackend::PGTransaction *final_tx;
    version_t user_version; ///< The copy source's user version
    bool should_requeue;  ///< op should be requeued on cancel
    vector<snapid_t> snaps;  ///< src's snaps (if clone)
    snapid_t snap_seq;       ///< src's snap_seq (if head)
    librados::snap_set_t snapset; ///< src snapset (if head)
    bool mirror_snapset;
    map<string, bufferlist> attrs; ///< src user attrs
    bool has_omap;
    uint32_t flags;    // object_copy_data_t::FLAG_*
    uint32_t source_data_digest, source_omap_digest;
    uint32_t data_digest, omap_digest;
    vector<pair<osd_reqid_t, version_t> > reqids; // [(reqid, user_version)]
    uint64_t truncate_seq;
    uint64_t truncate_size;
    bool is_data_digest() {
      return flags & object_copy_data_t::FLAG_DATA_DIGEST;
    }
    bool is_omap_digest() {
      return flags & object_copy_data_t::FLAG_OMAP_DIGEST;
    }
    CopyResults()
      : object_size(0), started_temp_obj(false),
	final_tx(NULL), user_version(0),
	should_requeue(false), mirror_snapset(false),
	has_omap(false),
	flags(0),
	source_data_digest(-1), source_omap_digest(-1),
	data_digest(-1), omap_digest(-1),
	truncate_seq(0), truncate_size(0)
    {}
  };

  struct CopyOp {
    CopyCallback *cb;
    ObjectContextRef obc;
    hobject_t src;
    object_locator_t oloc;
    unsigned flags;
    bool mirror_snapset;

    CopyResults results;

    ceph_tid_t objecter_tid;
    ceph_tid_t objecter_tid2;

    object_copy_cursor_t cursor;
    map<string,bufferlist> attrs;
    bufferlist data;
    bufferlist omap_header;
    bufferlist omap_data;
    int rval;

    object_copy_cursor_t temp_cursor;

    /*
     * For CopyOp the process is:
     * step1: read the data(attr/omap/data) from the source object
     * step2: handle those data(w/ those data create a new object)
     * src_obj_fadvise_flags used in step1;
     * dest_obj_fadvise_flags used in step2
     */
    unsigned src_obj_fadvise_flags;
    unsigned dest_obj_fadvise_flags;

    CopyOp(CopyCallback *cb_, ObjectContextRef _obc, hobject_t s,
	   object_locator_t l,
           version_t v,
	   unsigned f,
	   bool ms,
	   unsigned src_obj_fadvise_flags,
	   unsigned dest_obj_fadvise_flags)
      : cb(cb_), obc(_obc), src(s), oloc(l), flags(f),
	mirror_snapset(ms),
	objecter_tid(0),
	objecter_tid2(0),
	rval(-1),
	src_obj_fadvise_flags(src_obj_fadvise_flags),
	dest_obj_fadvise_flags(dest_obj_fadvise_flags)
    {
      results.user_version = v;
      results.mirror_snapset = mirror_snapset;
    }
  };
  typedef boost::shared_ptr<CopyOp> CopyOpRef;

  /**
   * The CopyCallback class defines an interface for completions to the
   * copy_start code. Users of the copy infrastructure must implement
   * one and give an instance of the class to start_copy.
   *
   * The implementer is responsible for making sure that the CopyCallback
   * can associate itself with the correct copy operation.
   */
  typedef boost::tuple<int, CopyResults*> CopyCallbackResults;
  class CopyCallback : public GenContext<CopyCallbackResults> {
  protected:
    CopyCallback() {}
    /**
     * results.get<0>() is the return code: 0 for success; -ECANCELLED if
     * the operation was cancelled by the local OSD; -errno for other issues.
     * results.get<1>() is a pointer to a CopyResults object, which you are
     * responsible for deleting.
     */
    virtual void finish(CopyCallbackResults results_) = 0;

  public:
    /// Provide the final size of the copied object to the CopyCallback
    virtual ~CopyCallback() {}
  };

  friend class CopyFromCallback;
  friend class PromoteCallback;

  struct ProxyReadOp {
    OpRequestRef op;
    hobject_t soid;
    ceph_tid_t objecter_tid;
    vector<OSDOp> &ops;
    version_t user_version;
    int data_offset;
    bool canceled;              ///< true if canceled

    ProxyReadOp(OpRequestRef _op, hobject_t oid, vector<OSDOp>& _ops)
      : op(_op), soid(oid),
        objecter_tid(0), ops(_ops),
	user_version(0), data_offset(0),
	canceled(false) { }
  };
  typedef boost::shared_ptr<ProxyReadOp> ProxyReadOpRef;

  struct ProxyWriteOp {
    OpContext *ctx;
    OpRequestRef op;
    hobject_t soid;
    ceph_tid_t objecter_tid;
    vector<OSDOp> &ops;
    version_t user_version;
    bool sent_disk;
    bool sent_ack;
    utime_t mtime;
    bool canceled;
    osd_reqid_t reqid;

    ProxyWriteOp(OpRequestRef _op, hobject_t oid, vector<OSDOp>& _ops, osd_reqid_t _reqid)
      : ctx(NULL), op(_op), soid(oid),
        objecter_tid(0), ops(_ops),
	user_version(0), sent_disk(false),
	sent_ack(false), canceled(false),
        reqid(_reqid) { }
  };
  typedef boost::shared_ptr<ProxyWriteOp> ProxyWriteOpRef;

  struct FlushOp {
    ObjectContextRef obc;       ///< obc we are flushing
    OpRequestRef op;            ///< initiating op
    list<OpRequestRef> dup_ops; ///< bandwagon jumpers
    version_t flushed_version;  ///< user version we are flushing
    ceph_tid_t objecter_tid;    ///< copy-from request tid
    int rval;                   ///< copy-from result
    bool blocking;              ///< whether we are blocking updates
    bool removal;               ///< we are removing the backend object
    Context *on_flush;          ///< callback, may be null

    FlushOp()
      : flushed_version(0), objecter_tid(0), rval(0),
	blocking(false), removal(false),
	on_flush(NULL) {}
    ~FlushOp() { assert(!on_flush); }
  };
  typedef boost::shared_ptr<FlushOp> FlushOpRef;

  boost::scoped_ptr<PGBackend> pgbackend;
  PGBackend *get_pgbackend() {
    return pgbackend.get();
  }

  /// Listener methods
  void on_local_recover(
    const hobject_t &oid,
    const object_stat_sum_t &stat_diff,
    const ObjectRecoveryInfo &recovery_info,
    ObjectContextRef obc,
    ObjectStore::Transaction *t
    );
  void on_peer_recover(
    pg_shard_t peer,
    const hobject_t &oid,
    const ObjectRecoveryInfo &recovery_info,
    const object_stat_sum_t &stat
    );
  void begin_peer_recover(
    pg_shard_t peer,
    const hobject_t oid);
  void on_global_recover(
    const hobject_t &oid);
  void failed_push(pg_shard_t from, const hobject_t &soid);
  void cancel_pull(const hobject_t &soid);

  template <typename T>
  class BlessedGenContext : public GenContext<T> {
    ReplicatedPGRef pg;
    GenContext<T> *c;
    epoch_t e;
  public:
    BlessedGenContext(ReplicatedPG *pg, GenContext<T> *c, epoch_t e)
      : pg(pg), c(c), e(e) {}
    void finish(T t) {
      pg->lock();
      if (pg->pg_has_reset_since(e))
	delete c;
      else
	c->complete(t);
      pg->unlock();
    }
  };
  class BlessedContext : public Context {
    ReplicatedPGRef pg;
    Context *c;
    epoch_t e;
  public:
    BlessedContext(ReplicatedPG *pg, Context *c, epoch_t e)
      : pg(pg), c(c), e(e) {}
    void finish(int r) {
      pg->lock();
      if (pg->pg_has_reset_since(e))
	delete c;
      else
	c->complete(r);
      pg->unlock();
    }
  };
  Context *bless_context(Context *c) {
    return new BlessedContext(this, c, get_osdmap()->get_epoch());
  }
  GenContext<ThreadPool::TPHandle&> *bless_gencontext(
    GenContext<ThreadPool::TPHandle&> *c) {
    return new BlessedGenContext<ThreadPool::TPHandle&>(
      this, c, get_osdmap()->get_epoch());
  }
    
  void send_message(int to_osd, Message *m) {
    osd->send_message_osd_cluster(to_osd, m, get_osdmap()->get_epoch());
  }
  void queue_transaction(ObjectStore::Transaction *t, OpRequestRef op) {
    osd->store->queue_transaction(osr.get(), t, 0, 0, 0, op);
  }
  void queue_transactions(list<ObjectStore::Transaction*>& tls, OpRequestRef op) {
    osd->store->queue_transactions(osr.get(), tls, 0, 0, 0, op);
  }
  epoch_t get_epoch() const {
    return get_osdmap()->get_epoch();
  }
  const set<pg_shard_t> &get_actingbackfill_shards() const {
    return actingbackfill;
  }
  const set<pg_shard_t> &get_acting_shards() const {
    return actingset;
  }
  const set<pg_shard_t> &get_backfill_shards() const {
    return backfill_targets;
  }

  std::string gen_dbg_prefix() const { return gen_prefix(); }
  
  const map<hobject_t, set<pg_shard_t>, hobject_t::BitwiseComparator> &get_missing_loc_shards() const {
    return missing_loc.get_missing_locs();
  }
  const map<pg_shard_t, pg_missing_t> &get_shard_missing() const {
    return peer_missing;
  }
  using PGBackend::Listener::get_shard_missing;
  const map<pg_shard_t, pg_info_t> &get_shard_info() const {
    return peer_info;
  }
  using PGBackend::Listener::get_shard_info;  
  const pg_missing_t &get_local_missing() const {
    return pg_log.get_missing();
  }
  const PGLog &get_log() const {
    return pg_log;
  }
  bool pgb_is_primary() const {
    return is_primary();
  }
  OSDMapRef pgb_get_osdmap() const {
    return get_osdmap();
  }
  const pg_info_t &get_info() const {
    return info;
  }
  const pg_pool_t &get_pool() const {
    return pool.info;
  }
  ObjectContextRef get_obc(
    const hobject_t &hoid,
    map<string, bufferlist> &attrs) {
    return get_object_context(hoid, true, &attrs);
  }
  void log_operation(
    const vector<pg_log_entry_t> &logv,
    boost::optional<pg_hit_set_history_t> &hset_history,
    const eversion_t &trim_to,
    const eversion_t &trim_rollback_to,
    bool transaction_applied,
    ObjectStore::Transaction *t) {
    if (hset_history) {
      info.hit_set = *hset_history;
      dirty_info = true;
    }
    append_log(logv, trim_to, trim_rollback_to, *t, transaction_applied);
  }

  void op_applied(
    const eversion_t &applied_version);

  bool should_send_op(
    pg_shard_t peer,
    const hobject_t &hoid) {
    if (peer == get_primary())
      return true;
    assert(peer_info.count(peer));
    bool should_send =
      hoid.pool != (int64_t)info.pgid.pool() ||
      cmp(hoid, last_backfill_started, get_sort_bitwise()) <= 0 ||
      cmp(hoid, peer_info[peer].last_backfill, get_sort_bitwise()) <= 0;
    if (!should_send)
      assert(is_backfill_targets(peer));
    return should_send;
  }
  
  void update_peer_last_complete_ondisk(
    pg_shard_t fromosd,
    eversion_t lcod) {
    peer_last_complete_ondisk[fromosd] = lcod;
  }

  void update_last_complete_ondisk(
    eversion_t lcod) {
    last_complete_ondisk = lcod;
  }

  void update_stats(
    const pg_stat_t &stat) {
    info.stats = stat;
  }

  void schedule_recovery_work(
    GenContext<ThreadPool::TPHandle&> *c);

  pg_shard_t whoami_shard() const {
    return pg_whoami;
  }
  spg_t primary_spg_t() const {
    return spg_t(info.pgid.pgid, primary.shard);
  }
  pg_shard_t primary_shard() const {
    return primary;
  }
  uint64_t min_peer_features() const {
    return get_min_peer_features();
  }
  bool sort_bitwise() const {
    return get_sort_bitwise();
  }

  bool transaction_use_tbl() {
    uint64_t min_features = get_min_upacting_features();
    return !(min_features & CEPH_FEATURE_OSD_TRANSACTION_MAY_LAYOUT);
  }

  void send_message_osd_cluster(
    int peer, Message *m, epoch_t from_epoch);
  void send_message_osd_cluster(
    Message *m, Connection *con);
  void send_message_osd_cluster(
    Message *m, const ConnectionRef& con);
  ConnectionRef get_con_osd_cluster(int peer, epoch_t from_epoch);
  entity_name_t get_cluster_msgr_name() {
    return osd->get_cluster_msgr_name();
  }

  PerfCounters *get_logger();

  ceph_tid_t get_tid() { return osd->get_tid(); }

  LogClientTemp clog_error() { return osd->clog->error(); }

  /*
   * Capture all object state associated with an in-progress read or write.
   */
  struct OpContext {
    OpRequestRef op;
    osd_reqid_t reqid;
    vector<OSDOp> &ops;

    const ObjectState *obs; // Old objectstate
    const SnapSet *snapset; // Old snapset

    ObjectState new_obs;  // resulting ObjectState
    SnapSet new_snapset;  // resulting SnapSet (in case of a write)
    //pg_stat_t new_stats;  // resulting Stats
    object_stat_sum_t delta_stats;

    bool modify;          // (force) modification (even if op_t is empty)
    bool user_modify;     // user-visible modification
    bool undirty;         // user explicitly un-dirtying this object
    bool cache_evict;     ///< true if this is a cache eviction
    bool ignore_cache;    ///< true if IGNORE_CACHE flag is set
    bool ignore_log_op_stats;  // don't log op stats

    // side effects
    list<pair<watch_info_t,bool> > watch_connects; ///< new watch + will_ping flag
    struct watch_disconnect_t {
      uint64_t cookie;
      entity_name_t name;
      bool send_disconnect;
      watch_disconnect_t(uint64_t c, entity_name_t n, bool sd)
	: cookie(c), name(n), send_disconnect(sd) {}
    };
    list<watch_disconnect_t> watch_disconnects; ///< old watch + send_discon
    list<notify_info_t> notifies;
    struct NotifyAck {
      boost::optional<uint64_t> watch_cookie;
      uint64_t notify_id;
      bufferlist reply_bl;
      NotifyAck(uint64_t notify_id) : notify_id(notify_id) {}
      NotifyAck(uint64_t notify_id, uint64_t cookie, bufferlist& rbl)
	: watch_cookie(cookie), notify_id(notify_id) {
	reply_bl.claim(rbl);
      }
    };
    list<NotifyAck> notify_acks;

    uint64_t bytes_written, bytes_read;

    utime_t mtime;
    SnapContext snapc;           // writer snap context
    eversion_t at_version;       // pg's current version pointer
    version_t user_at_version;   // pg's current user version pointer

    int current_osd_subop_num;

    PGBackend::PGTransaction *op_t;
    vector<pg_log_entry_t> log;
    boost::optional<pg_hit_set_history_t> updated_hset_history;

    interval_set<uint64_t> modified_ranges;
    ObjectContextRef obc;
    map<hobject_t,ObjectContextRef, hobject_t::BitwiseComparator> src_obc;
    ObjectContextRef clone_obc;    // if we created a clone
    ObjectContextRef snapset_obc;  // if we created/deleted a snapdir

    int data_off;        // FIXME: we may want to kill this msgr hint off at some point!

    MOSDOpReply *reply;

    utime_t readable_stamp;  // when applied on all replicas
    ReplicatedPG *pg;

    int num_read;    ///< count read ops
    int num_write;   ///< count update ops

    vector<pair<osd_reqid_t, version_t> > extra_reqids;

    CopyFromCallback *copy_cb;

    hobject_t new_temp_oid, discard_temp_oid;  ///< temp objects we should start/stop tracking

    // pending xattr updates
    map<ObjectContextRef,
	map<string, boost::optional<bufferlist> > > pending_attrs;
    void apply_pending_attrs() {
      for (map<ObjectContextRef,
	     map<string, boost::optional<bufferlist> > >::iterator i =
	     pending_attrs.begin();
	   i != pending_attrs.end();
	   ++i) {
	if (i->first->obs.exists) {
	  for (map<string, boost::optional<bufferlist> >::iterator j =
		 i->second.begin();
	       j != i->second.end();
	       ++j) {
	    if (j->second)
	      i->first->attr_cache[j->first] = j->second.get();
	    else
	      i->first->attr_cache.erase(j->first);
	  }
	} else {
	  i->first->attr_cache.clear();
	}
      }
      pending_attrs.clear();
    }

    // pending async reads <off, len, op_flags> -> <outbl, outr>
    list<pair<boost::tuple<uint64_t, uint64_t, unsigned>,
	      pair<bufferlist*, Context*> > > pending_async_reads;
    int async_read_result;
    unsigned inflightreads;
    friend struct OnReadComplete;
    void start_async_reads(ReplicatedPG *pg);
    void finish_read(ReplicatedPG *pg);
    bool async_reads_complete() {
      return inflightreads == 0;
    }

    ObjectModDesc mod_desc;

    enum { W_LOCK, R_LOCK, E_LOCK, NONE } lock_to_release;

    Context *on_finish;

    OpContext(const OpContext& other);
    const OpContext& operator=(const OpContext& other);

    bool release_snapset_obc;
    ThreadPool::TPHandle *handle;
    OpContext(OpRequestRef _op, osd_reqid_t _reqid, vector<OSDOp>& _ops,
	      ObjectContextRef& obc,
	      ReplicatedPG *_pg) :
      op(_op), reqid(_reqid), ops(_ops),
      obs(&obc->obs),
      snapset(0),
      new_obs(obs->oi, obs->exists),
      modify(false), user_modify(false), undirty(false), cache_evict(false),
      ignore_cache(false), ignore_log_op_stats(false),
      bytes_written(0), bytes_read(0), user_at_version(0),
      current_osd_subop_num(0),
      op_t(NULL),
      obc(obc),
      data_off(0), reply(NULL), pg(_pg),
      num_read(0),
      num_write(0),
      copy_cb(NULL),
      async_read_result(0),
      inflightreads(0),
      lock_to_release(NONE),
      on_finish(NULL),
      release_snapset_obc(false),
      handle(NULL) {
      if (obc->ssc) {
	new_snapset = obc->ssc->snapset;
	snapset = &obc->ssc->snapset;
      }
    }
    OpContext(OpRequestRef _op, osd_reqid_t _reqid,
              vector<OSDOp>& _ops, ReplicatedPG *_pg) :
      op(_op), reqid(_reqid), ops(_ops), obs(NULL), snapset(0),
      modify(false), user_modify(false), undirty(false), cache_evict(false),
      ignore_cache(false), ignore_log_op_stats(false),
      bytes_written(0), bytes_read(0), user_at_version(0),
      current_osd_subop_num(0),
      op_t(NULL),
      data_off(0), reply(NULL), pg(_pg),
      num_read(0),
      num_write(0),
      copy_cb(NULL),
      async_read_result(0),
      inflightreads(0),
      lock_to_release(NONE),
      on_finish(NULL),
      release_snapset_obc(false),
      handle(NULL) { }
    void reset_obs(ObjectContextRef obc) {
      new_obs = ObjectState(obc->obs.oi, obc->obs.exists);
      if (obc->ssc) {
	new_snapset = obc->ssc->snapset;
	snapset = &obc->ssc->snapset;
      }
    }
    ~OpContext() {
      assert(!op_t);
      assert(lock_to_release == NONE);
      if (reply)
	reply->put();
      for (list<pair<boost::tuple<uint64_t, uint64_t, unsigned>,
		     pair<bufferlist*, Context*> > >::iterator i =
	     pending_async_reads.begin();
	   i != pending_async_reads.end();
	   pending_async_reads.erase(i++)) {
	delete i->second.second;
      }
      assert(on_finish == NULL);
    }
    void finish(int r) {
      if (on_finish) {
	on_finish->complete(r);
	on_finish = NULL;
      }
    }
  };
  friend struct OpContext;

  /*
   * State on the PG primary associated with the replicated mutation
   */
  class RepGather {
  public:
    xlist<RepGather*>::item queue_item;
    int nref;

    eversion_t v;

    OpContext *ctx;
    ObjectContextRef obc;
    map<hobject_t,ObjectContextRef, hobject_t::BitwiseComparator> src_obc;

    ceph_tid_t rep_tid;

    bool rep_aborted, rep_done;

    bool all_applied;
    bool all_committed;
    bool sent_ack;
    //bool sent_nvram;
    bool sent_disk;
    
    utime_t   start;
    
    eversion_t          pg_local_last_complete;

    bool queue_snap_trimmer;

    Context *on_applied;
    bool log_op_stat;
    
    RepGather(OpContext *c, ObjectContextRef pi, ceph_tid_t rt,
	      eversion_t lc) :
      queue_item(this),
      nref(1),
      ctx(c), obc(pi),
      rep_tid(rt), 
      rep_aborted(false), rep_done(false),
      all_applied(false), all_committed(false), sent_ack(false),
      //sent_nvram(false),
      sent_disk(false),
      pg_local_last_complete(lc),
      queue_snap_trimmer(false),
      on_applied(NULL),
      log_op_stat(false) { }

    RepGather *get() {
      nref++;
      return this;
    }
    void put() {
      assert(nref > 0);
      if (--nref == 0) {
	delete ctx; // must already be unlocked
	assert(on_applied == NULL);
	delete this;
	//generic_dout(0) << "deleting " << this << dendl;
      }
    }
  };


protected:

  /**
   * Grabs locks for OpContext, should be cleaned up in close_op_ctx
   *
   * @param ctx [in,out] ctx to get locks for
   * @return true on success, false if we are queued
   */
  bool get_rw_locks(bool write_ordered, OpContext *ctx) {
    /* If snapset_obc, !obc->obs->exists and we will always take the
     * snapdir lock *before* the head lock.  Since all callers will do
     * this (read or write) if we get the first we will be guaranteed
     * to get the second.
     */
    ObjectContext::RWState::State type = ObjectContext::RWState::RWNONE;
    if (write_ordered && ctx->op->may_read()) {
      type = ObjectContext::RWState::RWEXCL;
      ctx->lock_to_release = OpContext::E_LOCK;
    } else if (write_ordered) {
      type = ObjectContext::RWState::RWWRITE;
      ctx->lock_to_release = OpContext::W_LOCK;
    } else {
      assert(ctx->op->may_read());
      type = ObjectContext::RWState::RWREAD;
      ctx->lock_to_release = OpContext::R_LOCK;
    }

    if (ctx->snapset_obc) {
      assert(!ctx->obc->obs.exists);
      if (ctx->snapset_obc->get_lock_type(ctx->op, type)) {
	ctx->release_snapset_obc = true;
      } else {
	ctx->lock_to_release = OpContext::NONE;
	return false;
      }
    }
    if (ctx->obc->get_lock_type(ctx->op, type)) {
      return true;
    } else {
      assert(!ctx->snapset_obc);
      ctx->lock_to_release = OpContext::NONE;
      return false;
    }
  }

  /**
   * Cleans up OpContext
   *
   * @param ctx [in] ctx to clean up
   */
  void close_op_ctx(OpContext *ctx, int r) {
    release_op_ctx_locks(ctx);
    delete ctx->op_t;
    ctx->op_t = NULL;
    ctx->finish(r);
    delete ctx;
  }

  /**
   * Releases ctx locks
   *
   * @param ctx [in] ctx to clean up
   */
  void release_op_ctx_locks(OpContext *ctx) {
    list<OpRequestRef> to_req;
    bool requeue_recovery = false;
    bool requeue_recovery_clone = false;
    bool requeue_recovery_snapset = false;
    bool requeue_snaptrimmer = false;
    bool requeue_snaptrimmer_clone = false;
    bool requeue_snaptrimmer_snapset = false;
    switch (ctx->lock_to_release) {
    case OpContext::W_LOCK:
      if (ctx->snapset_obc && ctx->release_snapset_obc) {
	ctx->snapset_obc->put_write(
	  &to_req,
	  &requeue_recovery_snapset,
	  &requeue_snaptrimmer_snapset);
	ctx->release_snapset_obc = false;
      }
      ctx->obc->put_write(
	&to_req,
	&requeue_recovery,
	&requeue_snaptrimmer);
      if (ctx->clone_obc)
	ctx->clone_obc->put_write(
	  &to_req,
	  &requeue_recovery_clone,
	  &requeue_snaptrimmer_clone);
      break;
    case OpContext::E_LOCK:
      if (ctx->snapset_obc && ctx->release_snapset_obc) {
	ctx->snapset_obc->put_excl(
	  &to_req,
	  &requeue_recovery_snapset,
	  &requeue_snaptrimmer_snapset);
	ctx->release_snapset_obc = false;
      }
      ctx->obc->put_excl(
	&to_req,
	&requeue_recovery,
	&requeue_snaptrimmer);
      if (ctx->clone_obc)
	ctx->clone_obc->put_write(
	  &to_req,
	  &requeue_recovery_clone,
	  &requeue_snaptrimmer_clone);
      break;
    case OpContext::R_LOCK:
      if (ctx->snapset_obc && ctx->release_snapset_obc) {
	ctx->snapset_obc->put_read(&to_req);
	ctx->release_snapset_obc = false;
      }
      ctx->obc->put_read(&to_req);
      break;
    case OpContext::NONE:
      break;
    default:
      assert(0);
    };
    assert(ctx->release_snapset_obc == false);
    ctx->lock_to_release = OpContext::NONE;
    if (requeue_recovery || requeue_recovery_clone || requeue_recovery_snapset)
      osd->recovery_wq.queue(this);
    if (requeue_snaptrimmer ||
	requeue_snaptrimmer_clone ||
	requeue_snaptrimmer_snapset)
      queue_snap_trim();

    if (!to_req.empty()) {
      assert(ctx->obc);
      // requeue at front of scrub blocking queue if we are blocked by scrub
      if (scrubber.write_blocked_by_scrub(ctx->obc->obs.oi.soid.get_head(),
					  get_sort_bitwise())) {
	waiting_for_active.splice(
	  waiting_for_active.begin(),
	  to_req,
	  to_req.begin(),
	  to_req.end());
      } else {
	requeue_ops(to_req);
      }
    }
  }

  // replica ops
  // [primary|tail]
  xlist<RepGather*> repop_queue;
  ceph::unordered_map<ceph_tid_t, RepGather*> repop_map;

  friend class C_OSD_RepopApplied;
  friend class C_OSD_RepopCommit;
  void repop_all_applied(RepGather *repop);
  void repop_all_committed(RepGather *repop);
  void eval_repop(RepGather*);
  void issue_repop(RepGather *repop);
  RepGather *new_repop(OpContext *ctx, ObjectContextRef obc, ceph_tid_t rep_tid);
  void remove_repop(RepGather *repop);

  RepGather *simple_repop_create(ObjectContextRef obc);
  void simple_repop_submit(RepGather *repop);

  // hot/cold tracking
  HitSetRef hit_set;        ///< currently accumulating HitSet
  utime_t hit_set_start_stamp;    ///< time the current HitSet started recording

  map<time_t,HitSetRef> hit_set_flushing; ///< currently being written, not yet readable

  void hit_set_clear();     ///< discard any HitSet state
  void hit_set_setup();     ///< initialize HitSet state
  void hit_set_create();    ///< create a new HitSet
  void hit_set_persist();   ///< persist hit info
  bool hit_set_apply_log(); ///< apply log entries to update in-memory HitSet
  void hit_set_trim(RepGather *repop, unsigned max); ///< discard old HitSets
  void hit_set_in_memory_trim(uint32_t max_in_memory); ///< discard old in memory HitSets
  void hit_set_remove_all();

  hobject_t get_hit_set_current_object(utime_t stamp);
  hobject_t get_hit_set_archive_object(utime_t start,
				       utime_t end,
				       bool using_gmt);

  // agent
  boost::scoped_ptr<TierAgentState> agent_state;

  friend struct C_AgentFlushStartStop;
  friend struct C_AgentEvictStartStop;
  friend struct C_HitSetFlushing;

  void agent_setup();       ///< initialize agent state
  bool agent_work(int max) ///< entry point to do some agent work
  {
    return agent_work(max, max);
  }
  bool agent_work(int max, int agent_flush_quota);
  bool agent_maybe_flush(ObjectContextRef& obc);  ///< maybe flush
  bool agent_maybe_evict(ObjectContextRef& obc);  ///< maybe evict

  void agent_load_hit_sets();  ///< load HitSets, if needed

  /// estimate object atime and temperature
  ///
  /// @param oid [in] object name
  /// @param temperature [out] relative temperature (# consider both access time and frequency)
  void agent_estimate_temp(const hobject_t& oid, int *temperature);

  /// stop the agent
  void agent_stop();
  void agent_delay();

  /// clear agent state
  void agent_clear();

  /// choose (new) agent mode(s), returns true if op is requeued
  bool agent_choose_mode(bool restart = false, OpRequestRef op = OpRequestRef());
  void agent_choose_mode_restart();

  /// true if we can send an ondisk/commit for v
  bool already_complete(eversion_t v) {
    for (xlist<RepGather*>::iterator i = repop_queue.begin();
	 !i.end();
	 ++i) {
      // skip copy from temp object ops
      if ((*i)->v == eversion_t())
	continue;
      if ((*i)->v > v)
        break;
      if (!(*i)->all_committed)
	return false;
    }
    return true;
  }
  /// true if we can send an ack for v
  bool already_ack(eversion_t v) {
    for (xlist<RepGather*>::iterator i = repop_queue.begin();
	 !i.end();
	 ++i) {
      // skip copy from temp object ops
      if ((*i)->v == eversion_t())
	continue;
      if ((*i)->v > v)
        break;
      if (!(*i)->all_applied)
	return false;
    }
    return true;
  }

  // projected object info
  SharedLRU<hobject_t, ObjectContext, hobject_t::ComparatorWithDefault> object_contexts;
  // map from oid.snapdir() to SnapSetContext *
  map<hobject_t, SnapSetContext*, hobject_t::BitwiseComparator> snapset_contexts;
  Mutex snapset_contexts_lock;

  // debug order that client ops are applied
  map<hobject_t, map<client_t, ceph_tid_t>, hobject_t::BitwiseComparator> debug_op_order;

  void populate_obc_watchers(ObjectContextRef obc);
  void check_blacklisted_obc_watchers(ObjectContextRef obc);
  void check_blacklisted_watchers();
  void get_watchers(list<obj_watch_item_t> &pg_watchers);
  void get_obc_watchers(ObjectContextRef obc, list<obj_watch_item_t> &pg_watchers);
public:
  void handle_watch_timeout(WatchRef watch);
protected:

  ObjectContextRef create_object_context(const object_info_t& oi, SnapSetContext *ssc);
  ObjectContextRef get_object_context(
    const hobject_t& soid,
    bool can_create,
    map<string, bufferlist> *attrs = 0
    );

  void context_registry_on_change();
  void object_context_destructor_callback(ObjectContext *obc);
  struct C_PG_ObjectContext : public Context {
    ReplicatedPGRef pg;
    ObjectContext *obc;
    C_PG_ObjectContext(ReplicatedPG *p, ObjectContext *o) :
      pg(p), obc(o) {}
    void finish(int r) {
      pg->object_context_destructor_callback(obc);
    }
  };

  int find_object_context(const hobject_t& oid,
			  ObjectContextRef *pobc,
			  bool can_create,
			  bool map_snapid_to_clone=false,
			  hobject_t *missing_oid=NULL);

  void add_object_context_to_pg_stat(ObjectContextRef obc, pg_stat_t *stat);

  void get_src_oloc(const object_t& oid, const object_locator_t& oloc, object_locator_t& src_oloc);

  SnapSetContext *create_snapset_context(const hobject_t& oid);
  SnapSetContext *get_snapset_context(
    const hobject_t& oid,
    bool can_create,
    map<string, bufferlist> *attrs = 0,
    bool oid_existed = true //indicate this oid whether exsited in backend
    );
  void register_snapset_context(SnapSetContext *ssc) {
    Mutex::Locker l(snapset_contexts_lock);
    _register_snapset_context(ssc);
  }
  void _register_snapset_context(SnapSetContext *ssc) {
    assert(snapset_contexts_lock.is_locked());
    if (!ssc->registered) {
      assert(snapset_contexts.count(ssc->oid) == 0);
      ssc->registered = true;
      snapset_contexts[ssc->oid] = ssc;
    }
  }
  void put_snapset_context(SnapSetContext *ssc);

  map<hobject_t, ObjectContextRef, hobject_t::BitwiseComparator> recovering;

  /*
   * Backfill
   *
   * peer_info[backfill_target].last_backfill == info.last_backfill on the peer.
   *
   * objects prior to peer_info[backfill_target].last_backfill
   *   - are on the peer
   *   - are included in the peer stats
   *
   * objects \in (last_backfill, last_backfill_started]
   *   - are on the peer or are in backfills_in_flight
   *   - are not included in pg stats (yet)
   *   - have their stats in pending_backfill_updates on the primary
   */
  set<hobject_t, hobject_t::Comparator> backfills_in_flight;
  map<hobject_t, pg_stat_t, hobject_t::Comparator> pending_backfill_updates;

  void dump_recovery_info(Formatter *f) const {
    f->open_array_section("backfill_targets");
    for (set<pg_shard_t>::const_iterator p = backfill_targets.begin();
        p != backfill_targets.end(); ++p)
      f->dump_stream("replica") << *p;
    f->close_section();
    f->open_array_section("waiting_on_backfill");
    for (set<pg_shard_t>::const_iterator p = waiting_on_backfill.begin();
        p != waiting_on_backfill.end(); ++p)
      f->dump_stream("osd") << *p;
    f->close_section();
    f->dump_stream("last_backfill_started") << last_backfill_started;
    {
      f->open_object_section("backfill_info");
      backfill_info.dump(f);
      f->close_section();
    }
    {
      f->open_array_section("peer_backfill_info");
      for (map<pg_shard_t, BackfillInterval>::const_iterator pbi =
	     peer_backfill_info.begin();
          pbi != peer_backfill_info.end(); ++pbi) {
        f->dump_stream("osd") << pbi->first;
        f->open_object_section("BackfillInterval");
          pbi->second.dump(f);
        f->close_section();
      }
      f->close_section();
    }
    {
      f->open_array_section("backfills_in_flight");
      for (set<hobject_t, hobject_t::BitwiseComparator>::const_iterator i = backfills_in_flight.begin();
	   i != backfills_in_flight.end();
	   ++i) {
	f->dump_stream("object") << *i;
      }
      f->close_section();
    }
    {
      f->open_array_section("recovering");
      for (map<hobject_t, ObjectContextRef, hobject_t::BitwiseComparator>::const_iterator i = recovering.begin();
	   i != recovering.end();
	   ++i) {
	f->dump_stream("object") << i->first;
      }
      f->close_section();
    }
    {
      f->open_object_section("pg_backend");
      pgbackend->dump_recovery_info(f);
      f->close_section();
    }
  }

  /// last backfill operation started
  hobject_t last_backfill_started;
  bool new_backfill;

  int prep_object_replica_pushes(const hobject_t& soid, eversion_t v,
				 PGBackend::RecoveryHandle *h);

  void finish_degraded_object(const hobject_t& oid);

  // Cancels/resets pulls from peer
  void check_recovery_sources(const OSDMapRef map);

  int recover_missing(
    const hobject_t& oid,
    eversion_t v,
    int priority,
    PGBackend::RecoveryHandle *h);

  // low level ops

  void _make_clone(
    OpContext *ctx,
    PGBackend::PGTransaction* t,
    ObjectContextRef obc,
    const hobject_t& head, const hobject_t& coid,
    object_info_t *poi);
  void execute_ctx(OpContext *ctx);
  void finish_ctx(OpContext *ctx, int log_op_type, bool maintain_ssc=true,
		  bool scrub_ok=false);
  void reply_ctx(OpContext *ctx, int err);
  void reply_ctx(OpContext *ctx, int err, eversion_t v, version_t uv);
  void make_writeable(OpContext *ctx);
  void log_op_stats(OpContext *ctx);
  void apply_ctx_stats(OpContext *ctx,
		       bool scrub_ok=false); ///< true if we should skip scrub stat update

  void write_update_size_and_usage(object_stat_sum_t& stats, object_info_t& oi,
				   interval_set<uint64_t>& modified, uint64_t offset,
				   uint64_t length, bool count_bytes,
				   bool force_changesize=false);
  void add_interval_usage(interval_set<uint64_t>& s, object_stat_sum_t& st);


  enum class cache_result_t {
    NOOP,
    BLOCKED_FULL,
    BLOCKED_PROMOTE,
    HANDLED_PROXY,
    HANDLED_REDIRECT,
  };
  cache_result_t maybe_handle_cache_detail(OpRequestRef op,
					   bool write_ordered,
					   ObjectContextRef obc, int r,
					   hobject_t missing_oid,
					   bool must_promote,
					   bool in_hit_set,
					   ObjectContextRef *promote_obc);
  /**
   * This helper function is called from do_op if the ObjectContext lookup fails.
   * @returns true if the caching code is handling the Op, false otherwise.
   */
  bool maybe_handle_cache(OpRequestRef op,
			  bool write_ordered,
			  ObjectContextRef obc, int r,
			  const hobject_t& missing_oid,
			  bool must_promote,
			  bool in_hit_set = false) {
    return cache_result_t::NOOP != maybe_handle_cache_detail(
      op,
      write_ordered,
      obc,
      r,
      missing_oid,
      must_promote,
      in_hit_set,
      nullptr);
  }

  /**
   * This helper function checks if a promotion is needed.
   */
  bool maybe_promote(ObjectContextRef obc,
		     const hobject_t& missing_oid,
		     const object_locator_t& oloc,
		     bool in_hit_set,
		     uint32_t recency,
		     OpRequestRef promote_op,
		     ObjectContextRef *promote_obc = nullptr);
  /**
   * This helper function tells the client to redirect their request elsewhere.
   */
  void do_cache_redirect(OpRequestRef op);
  /**
   * This function attempts to start a promote.  Either it succeeds,
   * or places op on a wait list.  If op is null, failure means that
   * this is a noop.  If a future user wants to be able to distinguish
   * these cases, a return value should be added.
   */
  void promote_object(
    ObjectContextRef obc,            ///< [optional] obc
    const hobject_t& missing_object, ///< oid (if !obc)
    const object_locator_t& oloc,    ///< locator for obc|oid
    OpRequestRef op,                 ///< [optional] client op
    ObjectContextRef *promote_obc = nullptr ///< [optional] new obc for object
    );

  int prepare_transaction(OpContext *ctx);
  list<pair<OpRequestRef, OpContext*> > in_progress_async_reads;
  void complete_read_ctx(int result, OpContext *ctx);
  
  // pg on-disk content
  void check_local();

  void _clear_recovery_state();

  void queue_for_recovery();
  bool start_recovery_ops(
    int max, ThreadPool::TPHandle &handle, int *started);

  int recover_primary(int max, ThreadPool::TPHandle &handle);
  int recover_replicas(int max, ThreadPool::TPHandle &handle);
  hobject_t earliest_peer_backfill() const;
  bool all_peer_done() const;
  /**
   * @param work_started will be set to true if recover_backfill got anywhere
   * @returns the number of operations started
   */
  int recover_backfill(int max, ThreadPool::TPHandle &handle,
                       bool *work_started);

  /**
   * scan a (hash) range of objects in the current pg
   *
   * @begin first item should be >= this value
   * @min return at least this many items, unless we are done
   * @max return no more than this many items
   * @bi [out] resulting map of objects to eversion_t's
   */
  void scan_range(
    int min, int max, BackfillInterval *bi,
    ThreadPool::TPHandle &handle
    );

  /// Update a hash range to reflect changes since the last scan
  void update_range(
    BackfillInterval *bi,        ///< [in,out] interval to update
    ThreadPool::TPHandle &handle ///< [in] tp handle
    );

  void prep_backfill_object_push(
    hobject_t oid, eversion_t v, ObjectContextRef obc,
    vector<pg_shard_t> peers,
    PGBackend::RecoveryHandle *h);
  void send_remove_op(const hobject_t& oid, eversion_t v, pg_shard_t peer);


  struct C_OSD_OndiskWriteUnlock : public Context {
    ObjectContextRef obc, obc2, obc3;
    C_OSD_OndiskWriteUnlock(
      ObjectContextRef o,
      ObjectContextRef o2 = ObjectContextRef(),
      ObjectContextRef o3 = ObjectContextRef()) : obc(o), obc2(o2), obc3(o3) {}
    void finish(int r) {
      obc->ondisk_write_unlock();
      if (obc2)
	obc2->ondisk_write_unlock();
      if (obc3)
	obc3->ondisk_write_unlock();
    }
  };
  struct C_OSD_OndiskWriteUnlockList : public Context {
    list<ObjectContextRef> *pls;
    C_OSD_OndiskWriteUnlockList(list<ObjectContextRef> *l) : pls(l) {}
    void finish(int r) {
      for (list<ObjectContextRef>::iterator p = pls->begin(); p != pls->end(); ++p)
	(*p)->ondisk_write_unlock();
    }
  };
  struct C_OSD_AppliedRecoveredObject : public Context {
    ReplicatedPGRef pg;
    ObjectContextRef obc;
    C_OSD_AppliedRecoveredObject(ReplicatedPG *p, ObjectContextRef o) :
      pg(p), obc(o) {}
    void finish(int r) {
      pg->_applied_recovered_object(obc);
    }
  };
  struct C_OSD_CommittedPushedObject : public Context {
    ReplicatedPGRef pg;
    epoch_t epoch;
    eversion_t last_complete;
    C_OSD_CommittedPushedObject(
      ReplicatedPG *p, epoch_t epoch, eversion_t lc) :
      pg(p), epoch(epoch), last_complete(lc) {
    }
    void finish(int r) {
      pg->_committed_pushed_object(epoch, last_complete);
    }
  };
  struct C_OSD_AppliedRecoveredObjectReplica : public Context {
    ReplicatedPGRef pg;
    C_OSD_AppliedRecoveredObjectReplica(ReplicatedPG *p) :
      pg(p) {}
    void finish(int r) {
      pg->_applied_recovered_object_replica();
    }
  };

  void sub_op_remove(OpRequestRef op);

  void _applied_recovered_object(ObjectContextRef obc);
  void _applied_recovered_object_replica();
  void _committed_pushed_object(epoch_t epoch, eversion_t lc);
  void recover_got(hobject_t oid, eversion_t v);

  // -- copyfrom --
  map<hobject_t, CopyOpRef, hobject_t::BitwiseComparator> copy_ops;

  int fill_in_copy_get(
    OpContext *ctx,
    bufferlist::iterator& bp,
    OSDOp& op,
    ObjectContextRef& obc,
    bool classic);
  void fill_in_copy_get_noent(OpRequestRef& op, hobject_t oid,
                              OSDOp& osd_op, bool classic);

  /**
   * To copy an object, call start_copy.
   *
   * @param cb: The CopyCallback to be activated when the copy is complete
   * @param obc: The ObjectContext we are copying into
   * @param src: The source object
   * @param oloc: the source object locator
   * @param version: the version of the source object to copy (0 for any)
   */
  void start_copy(CopyCallback *cb, ObjectContextRef obc, hobject_t src,
		  object_locator_t oloc, version_t version, unsigned flags,
		  bool mirror_snapset, unsigned src_obj_fadvise_flags,
		  unsigned dest_obj_fadvise_flags);
  void process_copy_chunk(hobject_t oid, ceph_tid_t tid, int r);
  void _write_copy_chunk(CopyOpRef cop, PGBackend::PGTransaction *t);
  uint64_t get_copy_chunk_size() const {
    uint64_t size = cct->_conf->osd_copyfrom_max_chunk;
    if (pool.info.requires_aligned_append()) {
      uint64_t alignment = pool.info.required_alignment();
      if (size % alignment) {
	size += alignment - (size % alignment);
      }
    }
    return size;
  }
  void _copy_some(ObjectContextRef obc, CopyOpRef cop);
  void _build_finish_copy_transaction(CopyOpRef cop,
                                      PGBackend::PGTransaction *t);
  void finish_copyfrom(OpContext *ctx);
  void finish_promote(int r, CopyResults *results, ObjectContextRef obc);
  void cancel_copy(CopyOpRef cop, bool requeue);
  void cancel_copy_ops(bool requeue);

  friend struct C_Copyfrom;

  // -- flush --
  map<hobject_t, FlushOpRef, hobject_t::BitwiseComparator> flush_ops;

  /// start_flush takes ownership of on_flush iff ret == -EINPROGRESS
  int start_flush(
    OpRequestRef op, ObjectContextRef obc,
    bool blocking, hobject_t *pmissing,
    Context *on_flush);
  void finish_flush(hobject_t oid, ceph_tid_t tid, int r);
  int try_flush_mark_clean(FlushOpRef fop);
  void cancel_flush(FlushOpRef fop, bool requeue);
  void cancel_flush_ops(bool requeue);

  /// @return false if clone is has been evicted
  bool is_present_clone(hobject_t coid);

  friend struct C_Flush;

  // -- scrub --
  virtual bool _range_available_for_scrub(
    const hobject_t &begin, const hobject_t &end);
  virtual void _scrub(
    ScrubMap &map,
    const std::map<hobject_t, pair<uint32_t, uint32_t>, hobject_t::BitwiseComparator> &missing_digest);
  void _scrub_digest_updated();
  virtual void _scrub_clear_state();
  virtual void _scrub_finish();
  object_stat_collection_t scrub_cstat;
  friend class C_ScrubDigestUpdated;

  virtual void _split_into(pg_t child_pgid, PG *child, unsigned split_bits);
  void apply_and_flush_repops(bool requeue);

  void calc_trim_to();
  int do_xattr_cmp_u64(int op, __u64 v1, bufferlist& xattr);
  int do_xattr_cmp_str(int op, string& v1s, bufferlist& xattr);

  bool pgls_filter(PGLSFilter *filter, hobject_t& sobj, bufferlist& outdata);
  int get_pgls_filter(bufferlist::iterator& iter, PGLSFilter **pfilter);

  map<hobject_t, list<OpRequestRef>, hobject_t::BitwiseComparator> in_progress_proxy_ops;
  void kick_proxy_ops_blocked(hobject_t& soid);
  void cancel_proxy_ops(bool requeue);

  // -- proxyread --
  map<ceph_tid_t, ProxyReadOpRef> proxyread_ops;

  void do_proxy_read(OpRequestRef op);
  void finish_proxy_read(hobject_t oid, ceph_tid_t tid, int r);
  void cancel_proxy_read(ProxyReadOpRef prdop);

  friend struct C_ProxyRead;

  // -- proxywrite --
  map<ceph_tid_t, ProxyWriteOpRef> proxywrite_ops;

  void do_proxy_write(OpRequestRef op, const hobject_t& missing_oid);
  void finish_proxy_write(hobject_t oid, ceph_tid_t tid, int r);
  void cancel_proxy_write(ProxyWriteOpRef pwop);

  friend struct C_ProxyWrite_Apply;
  friend struct C_ProxyWrite_Commit;

public:
  ReplicatedPG(OSDService *o, OSDMapRef curmap,
	       const PGPool &_pool, spg_t p);
  ~ReplicatedPG() {}

  int do_command(cmdmap_t cmdmap, ostream& ss, bufferlist& idata,
		 bufferlist& odata);

  void do_request(
    OpRequestRef& op,
    ThreadPool::TPHandle &handle);
  void do_op(OpRequestRef& op,
	     ThreadPool::TPHandle &handle);
  bool pg_op_must_wait(MOSDOp *op);
  void do_pg_op(OpRequestRef op);
  void do_sub_op(OpRequestRef op);
  void do_sub_op_reply(OpRequestRef op);
  void do_scan(
    OpRequestRef op,
    ThreadPool::TPHandle &handle);
  void do_backfill(OpRequestRef op);

  RepGather *trim_object(const hobject_t &coid);
  void snap_trimmer(epoch_t e);
  int do_osd_ops(OpContext *ctx, vector<OSDOp>& ops);

  int _get_tmap(OpContext *ctx, bufferlist *header, bufferlist *vals);
  int do_tmap2omap(OpContext *ctx, unsigned flags);
  int do_tmapup(OpContext *ctx, bufferlist::iterator& bp, OSDOp& osd_op);
  int do_tmapup_slow(OpContext *ctx, bufferlist::iterator& bp, OSDOp& osd_op, bufferlist& bl);

  void do_osd_op_effects(OpContext *ctx, const ConnectionRef& conn);
private:
  hobject_t earliest_backfill() const;
  bool check_src_targ(const hobject_t& soid, const hobject_t& toid) const;

  uint64_t temp_seq; ///< last id for naming temp objects
  hobject_t generate_temp_object();  ///< generate a new temp object name
  /// generate a new temp object name (for recovery)
  hobject_t get_temp_recovery_object(eversion_t version, snapid_t snap);
  int get_recovery_op_priority() const {
      int pri = 0;
      pool.info.opts.get(pool_opts_t::RECOVERY_OP_PRIORITY, &pri);
      return  pri > 0 ? pri : cct->_conf->osd_recovery_op_priority;
  }
  void log_missing(unsigned missing,
			const boost::optional<hobject_t> &head,
			LogChannelRef clog,
			const spg_t &pgid,
			const char *func,
			const char *mode,
			bool allow_incomplete_clones);
  unsigned process_clones_to(const boost::optional<hobject_t> &head,
    const boost::optional<SnapSet> &snapset,
    LogChannelRef clog,
    const spg_t &pgid,
    const char *mode,
    bool allow_incomplete_clones,
    boost::optional<snapid_t> target,
    vector<snapid_t>::reverse_iterator *curclone);

public:
  coll_t get_coll() {
    return coll;
  }
  void split_colls(
    spg_t child,
    int split_bits,
    int seed,
    const pg_pool_t *pool,
    ObjectStore::Transaction *t) {
    coll_t target = coll_t(child);
    PG::_create(*t, child, split_bits);
    t->split_collection(
      coll,
      split_bits,
      seed,
      target);
    PG::_init(*t, child, pool);
  }
private:
  struct NotTrimming;
  struct SnapTrim : boost::statechart::event< SnapTrim > {
    SnapTrim() : boost::statechart::event < SnapTrim >() {}
  };
  struct Reset : boost::statechart::event< Reset > {
    Reset() : boost::statechart::event< Reset >() {}
  };
  struct SnapTrimmer : public boost::statechart::state_machine< SnapTrimmer, NotTrimming > {
    ReplicatedPG *pg;
    set<RepGather *> repops;
    snapid_t snap_to_trim;
    bool need_share_pg_info;
    SnapTrimmer(ReplicatedPG *pg) : pg(pg), need_share_pg_info(false) {}
    ~SnapTrimmer();
    void log_enter(const char *state_name);
    void log_exit(const char *state_name, utime_t duration);
  } snap_trimmer_machine;

  /* SnapTrimmerStates */
  struct TrimmingObjects : boost::statechart::state< TrimmingObjects, SnapTrimmer >, NamedState {
    typedef boost::mpl::list <
      boost::statechart::custom_reaction< SnapTrim >,
      boost::statechart::transition< Reset, NotTrimming >
      > reactions;
    hobject_t pos;
    TrimmingObjects(my_context ctx);
    void exit();
    boost::statechart::result react(const SnapTrim&);
  };

  struct WaitingOnReplicas : boost::statechart::state< WaitingOnReplicas, SnapTrimmer >, NamedState {
    typedef boost::mpl::list <
      boost::statechart::custom_reaction< SnapTrim >,
      boost::statechart::transition< Reset, NotTrimming >
      > reactions;
    WaitingOnReplicas(my_context ctx);
    void exit();
    boost::statechart::result react(const SnapTrim&);
  };
  
  struct NotTrimming : boost::statechart::state< NotTrimming, SnapTrimmer >, NamedState {
    typedef boost::mpl::list <
      boost::statechart::custom_reaction< SnapTrim >,
      boost::statechart::transition< Reset, NotTrimming >
      > reactions;
    NotTrimming(my_context ctx);
    void exit();
    boost::statechart::result react(const SnapTrim&);
  };

  int _verify_no_head_clones(const hobject_t& soid,
			     const SnapSet& ss);
  // return true if we're creating a local object, false for a
  // whiteout or no change.
  bool maybe_create_new_object(OpContext *ctx);
  int _delete_oid(OpContext *ctx, bool no_whiteout);
  int _rollback_to(OpContext *ctx, ceph_osd_op& op);
public:
  bool is_missing_object(const hobject_t& oid) const;
  bool is_unreadable_object(const hobject_t &oid) const {
    return is_missing_object(oid) ||
      !missing_loc.readable_with_acting(oid, actingset);
  }
  void maybe_kick_recovery(const hobject_t &soid);
  void wait_for_unreadable_object(const hobject_t& oid, OpRequestRef op);
  void wait_for_all_missing(OpRequestRef op);

  bool is_degraded_or_backfilling_object(const hobject_t& oid);
  void wait_for_degraded_object(const hobject_t& oid, OpRequestRef op);

  void block_write_on_full_cache(
    const hobject_t& oid, OpRequestRef op);
  void block_write_on_snap_rollback(
    const hobject_t& oid, ObjectContextRef obc, OpRequestRef op);
  void block_write_on_degraded_snap(const hobject_t& oid, OpRequestRef op);

  bool maybe_await_blocked_snapset(const hobject_t &soid, OpRequestRef op);
  void wait_for_blocked_object(const hobject_t& soid, OpRequestRef op);
  void kick_object_context_blocked(ObjectContextRef obc);

  void mark_all_unfound_lost(int what);
  eversion_t pick_newest_available(const hobject_t& oid);
  ObjectContextRef mark_object_lost(ObjectStore::Transaction *t,
				  const hobject_t& oid, eversion_t version,
				  utime_t mtime, int what);
  void _finish_mark_all_unfound_lost(list<ObjectContextRef>& obcs);

  void on_role_change();
  void on_pool_change();
  void _on_new_interval();
  void on_change(ObjectStore::Transaction *t);
  void on_activate();
  void on_flushed();
  void on_removal(ObjectStore::Transaction *t);
  void on_shutdown();

  // attr cache handling
  void replace_cached_attrs(
    OpContext *ctx,
    ObjectContextRef obc,
    const map<string, bufferlist> &new_attrs);
  void setattr_maybe_cache(
    ObjectContextRef obc,
    OpContext *op,
    PGBackend::PGTransaction *t,
    const string &key,
    bufferlist &val);
  void setattrs_maybe_cache(
    ObjectContextRef obc,
    OpContext *op,
    PGBackend::PGTransaction *t,
    map<string, bufferlist> &attrs);
  void rmattr_maybe_cache(
    ObjectContextRef obc,
    OpContext *op,
    PGBackend::PGTransaction *t,
    const string &key);
  int getattr_maybe_cache(
    ObjectContextRef obc,
    const string &key,
    bufferlist *val);
  int getattrs_maybe_cache(
    ObjectContextRef obc,
    map<string, bufferlist> *out,
    bool user_only = false);
};

inline ostream& operator<<(ostream& out, ReplicatedPG::RepGather& repop)
{
  out << "repgather(" << &repop
      << " " << repop.v
      << " rep_tid=" << repop.rep_tid 
      << " committed?=" << repop.all_committed
      << " applied?=" << repop.all_applied;
  if (repop.ctx->lock_to_release != ReplicatedPG::OpContext::NONE)
    out << " lock=" << (int)repop.ctx->lock_to_release;
  if (repop.ctx->op)
    out << " op=" << *(repop.ctx->op->get_req());
  out << ")";
  return out;
}

inline ostream& operator<<(ostream& out, ReplicatedPG::ProxyWriteOpRef pwop)
{
  out << "proxywrite(" << &pwop
      << " " << pwop->user_version
      << " pwop_tid=" << pwop->objecter_tid;
  if (pwop->ctx->op)
    out << " op=" << *(pwop->ctx->op->get_req());
  out << ")";
  return out;
}

void intrusive_ptr_add_ref(ReplicatedPG::RepGather *repop);
void intrusive_ptr_release(ReplicatedPG::RepGather *repop);


#endif
