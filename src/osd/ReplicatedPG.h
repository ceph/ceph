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

#include <boost/optional.hpp>
#include <boost/tuple/tuple.hpp>

#include "include/assert.h" 
#include "common/cmdparse.h"

#include "PG.h"
#include "OSD.h"
#include "Watch.h"
#include "OpRequest.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDSubOp.h"

#include "common/sharedptr_registry.hpp"

#include "PGBackend.h"
#include "ReplicatedBackend.h"

class MOSDSubOpReply;

class ReplicatedPG;
void intrusive_ptr_add_ref(ReplicatedPG *pg);
void intrusive_ptr_release(ReplicatedPG *pg);
uint64_t get_with_id(ReplicatedPG *pg);
void put_with_id(ReplicatedPG *pg, uint64_t id);

#ifdef PG_DEBUG_REFS
  typedef TrackedIntPtr<ReplicatedPG> ReplicatedPGRef;
#else
  typedef boost::intrusive_ptr<ReplicatedPG> ReplicatedPGRef;
#endif

class PGLSFilter {
protected:
  string xattr;
public:
  PGLSFilter();
  virtual ~PGLSFilter();
  virtual bool filter(bufferlist& xattr_data, bufferlist& outdata) = 0;
  virtual string& get_xattr() { return xattr; }
};

class PGLSPlainFilter : public PGLSFilter {
  string val;
public:
  PGLSPlainFilter(bufferlist::iterator& params) {
    ::decode(xattr, params);
    ::decode(val, params);
  }
  virtual ~PGLSPlainFilter() {}
  virtual bool filter(bufferlist& xattr_data, bufferlist& outdata);
};

class PGLSParentFilter : public PGLSFilter {
  inodeno_t parent_ino;
public:
  PGLSParentFilter(bufferlist::iterator& params) {
    xattr = "_parent";
    ::decode(parent_ino, params);
    generic_dout(0) << "parent_ino=" << parent_ino << dendl;
  }
  virtual ~PGLSParentFilter() {}
  virtual bool filter(bufferlist& xattr_data, bufferlist& outdata);
};

class ReplicatedPG : public PG, public PGBackend::Listener {
  friend class OSD;
  friend class Watch;

public:

  /*
   * state associated with a copy operation
   */
  struct OpContext;
  class CopyCallback;

  struct CopyOp {
    CopyCallback *cb;
    ObjectContextRef obc;
    hobject_t src;
    object_locator_t oloc;
    version_t version;

    tid_t objecter_tid;

    object_copy_cursor_t cursor;
    uint64_t size;
    utime_t mtime;
    map<string,bufferlist> attrs;
    bufferlist data;
    map<string,bufferlist> omap;
    int rval;

    coll_t temp_coll;
    hobject_t temp_oid;
    object_copy_cursor_t temp_cursor;

    CopyOp(CopyCallback *cb_, ObjectContextRef _obc, hobject_t s, object_locator_t l,
           version_t v, const hobject_t& dest)
      : cb(cb_), obc(_obc), src(s), oloc(l), version(v),
	objecter_tid(0),
	size(0),
	rval(-1),
	temp_oid(dest)
    {}
  };
  typedef boost::shared_ptr<CopyOp> CopyOpRef;

  /**
   * The CopyCallback class defines an interface for completions to the
   * copy_start code. Users of the copy infrastructure must implement
   * one and give an instance of the class to start_copy.
   *
   * The implementer is responsible for making sure that the CopyCallback
   * can associate itself with the correct copy operation. The presence
   * of the closing Transaction ensures that write operations can be performed
   * atomically with the copy being completed (which doing them in separate
   * transactions would not allow); if you are doing the copy for a read
   * op you will have to generate a separate op to finish the copy with.
   */
  /// return code, total object size, data in temp object?, final Transaction
  typedef boost::tuple<int, size_t, bool, ObjectStore::Transaction> CopyResults;
  class CopyCallback : public GenContext<CopyResults&> {
  protected:
    CopyCallback() {}
    /**
     * results.get<0>() is the return code: 0 for success; -ECANCELLED if
     * the operation was cancelled by the local OSD; -errno for other issues.
     * results.get<1>() is the total size of the object (for updating pg stats)
     * results.get<2>() indicates whether we have already written data to
     * the temp object (so it needs to get cleaned up, if the return code
     * indicates a failure)
     * results.get<3>() is a Transaction; if non-empty you need to perform
     * its results before any other accesses to the object in order to
     * complete the copy.
     */
    virtual void finish(CopyResults& results_) = 0;

  public:
    /// Provide the final size of the copied object to the CopyCallback
    virtual ~CopyCallback() {};
  };

  class CopyFromCallback: public CopyCallback {
  public:
    CopyResults results;
    OpContext *ctx;
    hobject_t temp_obj;
    CopyFromCallback(OpContext *ctx_, const hobject_t& temp_obj_) :
      ctx(ctx_), temp_obj(temp_obj_) {}
    ~CopyFromCallback() {}

    virtual void finish(CopyResults& results_) {
      results = results_;
      int r = results.get<0>();
      if (r >= 0) {
	ctx->pg->execute_ctx(ctx);
      }
      ctx->copy_cb = NULL;
      if (r < 0) {
	if (r != -ECANCELED) { // on cancel just toss it out; client resends
	  ctx->pg->osd->reply_op_error(ctx->op, r);
	}
	ctx->pg->close_op_ctx(ctx);
      }
    }

    bool is_temp_obj_used() { return results.get<2>(); }
    uint64_t get_data_size() { return results.get<1>(); }
    int get_result() { return results.get<0>(); }
  };
  friend class CopyFromCallback;

  boost::scoped_ptr<PGBackend> pgbackend;
  PGBackend *get_pgbackend() {
    return pgbackend.get();
  }

  /// Listener methods
  void on_local_recover_start(
    const hobject_t &oid,
    ObjectStore::Transaction *t);
  void on_local_recover(
    const hobject_t &oid,
    const object_stat_sum_t &stat_diff,
    const ObjectRecoveryInfo &recovery_info,
    ObjectContextRef obc,
    ObjectStore::Transaction *t
    );
  void on_peer_recover(
    int peer,
    const hobject_t &oid,
    const ObjectRecoveryInfo &recovery_info,
    const object_stat_sum_t &stat
    );
  void begin_peer_recover(
    int peer,
    const hobject_t oid);
  void on_global_recover(
    const hobject_t &oid);
  void failed_push(int from, const hobject_t &soid);
  void cancel_pull(const hobject_t &soid);

  template <typename T>
  class BlessedGenContext : public GenContext<T> {
    ReplicatedPG *pg;
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
    ReplicatedPG *pg;
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
  void queue_transaction(ObjectStore::Transaction *t) {
    osd->store->queue_transaction(osr.get(), t);
  }
  epoch_t get_epoch() {
    return get_osdmap()->get_epoch();
  }
  const vector<int> &get_acting() {
    return acting;
  }
  std::string gen_dbg_prefix() const { return gen_prefix(); }
  
  const map<hobject_t, set<int> > &get_missing_loc() {
    return missing_loc;
  }
  const map<int, pg_missing_t> &get_peer_missing() {
    return peer_missing;
  }
  const map<int, pg_info_t> &get_peer_info() {
    return peer_info;
  }
  const pg_missing_t &get_local_missing() {
    return pg_log.get_missing();
  }
  const PGLog &get_log() {
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
  ObjectContextRef get_obc(
    const hobject_t &hoid,
    map<string, bufferptr> &attrs) {
    return get_object_context(hoid, true, &attrs);
  }

  /*
   * Capture all object state associated with an in-progress read or write.
   */
  struct OpContext {
    OpRequestRef op;
    osd_reqid_t reqid;
    vector<OSDOp> ops;

    const ObjectState *obs; // Old objectstate
    const SnapSet *snapset; // Old snapset

    ObjectState new_obs;  // resulting ObjectState
    SnapSet new_snapset;  // resulting SnapSet (in case of a write)
    //pg_stat_t new_stats;  // resulting Stats
    object_stat_sum_t delta_stats;

    bool modify;          // (force) modification (even if op_t is empty)
    bool user_modify;     // user-visible modification
    bool undirty;         // user explicitly un-dirtying this object

    // side effects
    list<watch_info_t> watch_connects;
    list<watch_info_t> watch_disconnects;
    list<notify_info_t> notifies;
    struct NotifyAck {
      boost::optional<uint64_t> watch_cookie;
      uint64_t notify_id;
      NotifyAck(uint64_t notify_id) : notify_id(notify_id) {}
      NotifyAck(uint64_t notify_id, uint64_t cookie)
	: watch_cookie(cookie), notify_id(notify_id) {}
    };
    list<NotifyAck> notify_acks;
    
    uint64_t bytes_written, bytes_read;

    utime_t mtime;
    SnapContext snapc;           // writer snap context
    eversion_t at_version;       // pg's current version pointer
    version_t user_at_version;   // pg's current user version pointer

    int current_osd_subop_num;

    ObjectStore::Transaction op_t, local_t;
    vector<pg_log_entry_t> log;

    interval_set<uint64_t> modified_ranges;
    ObjectContextRef obc;
    map<hobject_t,ObjectContextRef> src_obc;
    ObjectContextRef clone_obc;    // if we created a clone
    ObjectContextRef snapset_obc;  // if we created/deleted a snapdir

    int data_off;        // FIXME: we may want to kill this msgr hint off at some point!

    MOSDOpReply *reply;

    utime_t readable_stamp;  // when applied on all replicas
    ReplicatedPG *pg;

    int num_read;    ///< count read ops
    int num_write;   ///< count update ops

    CopyFromCallback *copy_cb;

    hobject_t new_temp_oid, discard_temp_oid;  ///< temp objects we should start/stop tracking

    enum { W_LOCK, R_LOCK, NONE } lock_to_release;

    OpContext(const OpContext& other);
    const OpContext& operator=(const OpContext& other);

    OpContext(OpRequestRef _op, osd_reqid_t _reqid, vector<OSDOp>& _ops,
	      ObjectState *_obs, SnapSetContext *_ssc,
	      ReplicatedPG *_pg) :
      op(_op), reqid(_reqid), ops(_ops), obs(_obs), snapset(0),
      new_obs(_obs->oi, _obs->exists),
      modify(false), user_modify(false), undirty(false),
      bytes_written(0), bytes_read(0), user_at_version(0),
      current_osd_subop_num(0),
      data_off(0), reply(NULL), pg(_pg),
      num_read(0),
      num_write(0),
      copy_cb(NULL),
      lock_to_release(NONE) {
      if (_ssc) {
	new_snapset = _ssc->snapset;
	snapset = &_ssc->snapset;
      }
    }
    ~OpContext() {
      assert(!clone_obc);
      assert(lock_to_release == NONE);
      if (reply)
	reply->put();
    }
  };

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
    map<hobject_t,ObjectContextRef> src_obc;

    tid_t rep_tid;

    bool applying, applied, aborted, done;

    set<int>  waitfor_ack;
    //set<int>  waitfor_nvram;
    set<int>  waitfor_disk;
    bool sent_ack;
    //bool sent_nvram;
    bool sent_disk;
    
    utime_t   start;
    
    eversion_t          pg_local_last_complete;

    list<ObjectStore::Transaction*> tls;
    bool queue_snap_trimmer;
    
    RepGather(OpContext *c, ObjectContextRef pi, tid_t rt, 
	      eversion_t lc) :
      queue_item(this),
      nref(1),
      ctx(c), obc(pi),
      rep_tid(rt), 
      applying(false), applied(false), aborted(false), done(false),
      sent_ack(false),
      //sent_nvram(false),
      sent_disk(false),
      pg_local_last_complete(lc),
      queue_snap_trimmer(false) { }

    void get() {
      nref++;
    }
    void put() {
      assert(nref > 0);
      if (--nref == 0) {
	assert(!obc);
	assert(src_obc.empty());
	delete ctx; // must already be unlocked
	delete this;
	//generic_dout(0) << "deleting " << this << dendl;
      }
    }
  };



protected:

  /// Tracks pending readers or writers on an object
  class RWTracker {
    struct ObjState {
      enum State {
	NONE,
	READ,
	WRITE
      };
      State state;                 /// rw state
      uint64_t count;              /// number of readers or writers
      list<OpRequestRef> waiters;  /// ops waiting on state change

      ObjState() : state(NONE), count(0) {}
      bool get_read(OpRequestRef op) {
	// don't starve!
	if (!waiters.empty()) {
	  waiters.push_back(op);
	  return false;
	}
	switch (state) {
	case NONE:
	  assert(count == 0);
	  state = READ;
	  // fall through
	case READ:
	  count++;
	  return true;
	case WRITE:
	  waiters.push_back(op);
	  return false;
	default:
	  assert(0 == "unhandled case");
	  return false;
	}
      }
      bool get_write(OpRequestRef op) {
	if (!waiters.empty()) {
	  // don't starve!
	  waiters.push_back(op);
	  return false;
	}
	switch (state) {
	case NONE:
	  assert(count == 0);
	  state = WRITE;
	  // fall through
	case WRITE:
	  count++;
	  return true;
	case READ:
	  waiters.push_back(op);
	  return false;
	default:
	  assert(0 == "unhandled case");
	  return false;
	}
      }
      void dec(list<OpRequestRef> *requeue) {
	assert(count > 0);
	assert(requeue);
	assert(requeue->empty());
	count--;
	if (count == 0) {
	  state = NONE;
	  requeue->swap(waiters);
	}
      }
      void put_read(list<OpRequestRef> *requeue) {
	assert(state == READ);
	dec(requeue);
      }
      void put_write(list<OpRequestRef> *requeue) {
	assert(state == WRITE);
	dec(requeue);
      }
      bool empty() const { return state == NONE; }
    };
    map<hobject_t, ObjState > obj_state;
  public:
    bool get_read(const hobject_t &hoid, OpRequestRef op) {
      return obj_state[hoid].get_read(op);
    }
    bool get_write(const hobject_t &hoid, OpRequestRef op) {
      return obj_state[hoid].get_write(op);
    }
    void put_read(const hobject_t &hoid, list<OpRequestRef> *to_wake) {
      obj_state[hoid].put_read(to_wake);
      if (obj_state[hoid].empty()) {
	obj_state.erase(hoid);
      }
    }
    void put_write(const hobject_t &hoid, list<OpRequestRef> *to_wake) {
      obj_state[hoid].put_write(to_wake);
      if (obj_state[hoid].empty()) {
	obj_state.erase(hoid);
      }
    }
  } rw_manager;

  /**
   * Grabs locks for OpContext, should be cleaned up in close_op_ctx
   *
   * @param ctx [in,out] ctx to get locks for
   * @return true on success, false if we are queued
   */
  bool get_rw_locks(OpContext *ctx) {
    if (ctx->op->may_write()) {
      if (rw_manager.get_write(ctx->obs->oi.soid, ctx->op)) {
	ctx->lock_to_release = OpContext::W_LOCK;
	return true;
      } else {
	return false;
      }
    } else {
      assert(ctx->op->may_read());
      if (rw_manager.get_read(ctx->obs->oi.soid, ctx->op)) {
	ctx->lock_to_release = OpContext::R_LOCK;
	return true;
      } else {
	return false;
      }
    }
  }

  /**
   * Cleans up OpContext
   *
   * @param ctx [in] ctx to clean up
   */
  void close_op_ctx(OpContext *ctx) {
    release_op_ctx_locks(ctx);
    delete ctx;
  }

  /**
   * Releases ctx locks
   *
   * @param ctx [in] ctx to clean up
   */
  void release_op_ctx_locks(OpContext *ctx) {
    list<OpRequestRef> to_req;
    switch (ctx->lock_to_release) {
    case OpContext::W_LOCK:
      rw_manager.put_write(ctx->obs->oi.soid, &to_req);
      break;
    case OpContext::R_LOCK:
      rw_manager.put_read(ctx->obs->oi.soid, &to_req);
      break;
    case OpContext::NONE:
      break;
    default:
      assert(0);
    };
    ctx->lock_to_release = OpContext::NONE;
    requeue_ops(to_req);
  }

  // replica ops
  // [primary|tail]
  xlist<RepGather*> repop_queue;
  map<tid_t, RepGather*> repop_map;

  void apply_repop(RepGather *repop);
  void op_applied(RepGather *repop);
  void op_commit(RepGather *repop);
  void eval_repop(RepGather*);
  void issue_repop(RepGather *repop, utime_t now);
  RepGather *new_repop(OpContext *ctx, ObjectContextRef obc, tid_t rep_tid);
  void remove_repop(RepGather *repop);
  void repop_ack(RepGather *repop,
                 int result, int ack_type,
                 int fromosd, eversion_t pg_complete_thru=eversion_t(0,0));

  /// true if we can send an ondisk/commit for v
  bool already_complete(eversion_t v) {
    for (xlist<RepGather*>::iterator i = repop_queue.begin();
	 !i.end();
	 ++i) {
      if ((*i)->v > v)
        break;
      if (!(*i)->waitfor_disk.empty())
	return false;
    }
    return true;
  }
  /// true if we can send an ack for v
  bool already_ack(eversion_t v) {
    for (xlist<RepGather*>::iterator i = repop_queue.begin();
	 !i.end();
	 ++i) {
      if ((*i)->v > v)
        break;
      if (!(*i)->waitfor_ack.empty())
	return false;
    }
    return true;
  }

  friend class C_OSD_OpCommit;
  friend class C_OSD_OpApplied;
  friend struct C_OnPushCommit;

  // projected object info
  SharedPtrRegistry<hobject_t, ObjectContext> object_contexts;
  map<object_t, SnapSetContext*> snapset_contexts;
  Mutex snapset_contexts_lock;

  // debug order that client ops are applied
  map<hobject_t, map<client_t, tid_t> > debug_op_order;

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
    map<string, bufferptr> *attrs = 0
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
			  bool can_create, snapid_t *psnapid=NULL);

  void add_object_context_to_pg_stat(ObjectContextRef obc, pg_stat_t *stat);

  void get_src_oloc(const object_t& oid, const object_locator_t& oloc, object_locator_t& src_oloc);

  SnapSetContext *create_snapset_context(const object_t& oid);
  SnapSetContext *get_snapset_context(
    const object_t& oid, const string &key,
    ps_t seed, bool can_create, const string &nspace,
    map<string, bufferptr> *attrs = 0
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

  set<hobject_t> recovering;

  /*
   * Backfill
   *
   * peer_info[backfill_target].last_backfill == info.last_backfill on the peer.
   *
   * objects prior to peer_info[backfill_target].last_backfill
   *   - are on the peer
   *   - are included in the peer stats
   *
   * objects between last_backfill and backfill_pos
   *   - are on the peer or are in backfills_in_flight
   *   - are not included in pg stats (yet)
   *   - have their stats in pending_backfill_updates on the primary
   */
  set<hobject_t> backfills_in_flight;
  map<hobject_t, pg_stat_t> pending_backfill_updates;

  void dump_recovery_info(Formatter *f) const {
    f->dump_int("backfill_target", get_backfill_target());
    f->dump_int("waiting_on_backfill", waiting_on_backfill);
    f->dump_stream("backfill_pos") << backfill_pos;
    {
      f->open_object_section("backfill_info");
      backfill_info.dump(f);
      f->close_section();
    }
    {
      f->open_object_section("peer_backfill_info");
      peer_backfill_info.dump(f);
      f->close_section();
    }
    {
      f->open_array_section("backfills_in_flight");
      for (set<hobject_t>::const_iterator i = backfills_in_flight.begin();
	   i != backfills_in_flight.end();
	   ++i) {
	f->dump_stream("object") << *i;
      }
      f->close_section();
    }
    {
      f->open_array_section("recovering");
      for (set<hobject_t>::const_iterator i = recovering.begin();
	   i != recovering.end();
	   ++i) {
	f->dump_stream("object") << *i;
      }
      f->close_section();
    }
    {
      f->open_object_section("pg_backend");
      pgbackend->dump_recovery_info(f);
      f->close_section();
    }
  }

  /// leading edge of backfill
  hobject_t backfill_pos;

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

  void _make_clone(ObjectStore::Transaction& t,
		   const hobject_t& head, const hobject_t& coid,
		   object_info_t *poi);
  void execute_ctx(OpContext *ctx);
  void reply_ctx(OpContext *ctx, int err);
  void reply_ctx(OpContext *ctx, int err, eversion_t v, version_t uv);
  void make_writeable(OpContext *ctx);
  void log_op_stats(OpContext *ctx);

  void write_update_size_and_usage(object_stat_sum_t& stats, object_info_t& oi,
				   SnapSet& ss, interval_set<uint64_t>& modified,
				   uint64_t offset, uint64_t length, bool count_bytes);
  void add_interval_usage(interval_set<uint64_t>& s, object_stat_sum_t& st);

  inline bool maybe_handle_cache(OpRequestRef op, ObjectContextRef obc, int r);
  void do_cache_redirect(OpRequestRef op, ObjectContextRef obc);

  int prepare_transaction(OpContext *ctx);
  
  // pg on-disk content
  void check_local();

  void _clear_recovery_state();

  void queue_for_recovery();
  int start_recovery_ops(
    int max, RecoveryCtx *prctx,
    ThreadPool::TPHandle &handle);

  int recover_primary(int max, ThreadPool::TPHandle &handle);
  int recover_replicas(int max, ThreadPool::TPHandle &handle);
  int recover_backfill(int max, ThreadPool::TPHandle &handle);

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
    hobject_t oid, eversion_t v, eversion_t have, int peer,
    PGBackend::RecoveryHandle *h);
  void send_remove_op(const hobject_t& oid, eversion_t v, int peer);


  struct RepModify {
    ReplicatedPG *pg;
    OpRequestRef op;
    OpContext *ctx;
    bool applied, committed;
    int ackerosd;
    eversion_t last_complete;
    epoch_t epoch_started;

    uint64_t bytes_written;

    ObjectStore::Transaction opt, localt;
    list<ObjectStore::Transaction*> tls;
    
    RepModify() : pg(NULL), ctx(NULL), applied(false), committed(false), ackerosd(-1),
		  epoch_started(0), bytes_written(0) {}
  };

  struct C_OSD_RepModifyApply : public Context {
    RepModify *rm;
    C_OSD_RepModifyApply(RepModify *r) : rm(r) { }
    void finish(int r) {
      rm->pg->sub_op_modify_applied(rm);
    }
  };
  struct C_OSD_RepModifyCommit : public Context {
    RepModify *rm;
    C_OSD_RepModifyCommit(RepModify *r) : rm(r) { }
    void finish(int r) {
      rm->pg->sub_op_modify_commit(rm);
    }
  };
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

  void sub_op_modify(OpRequestRef op);
  void sub_op_modify_applied(RepModify *rm);
  void sub_op_modify_commit(RepModify *rm);

  void sub_op_modify_reply(OpRequestRef op);
  void _applied_recovered_object(ObjectContextRef obc);
  void _applied_recovered_object_replica();
  void _committed_pushed_object(epoch_t epoch, eversion_t lc);
  void recover_got(hobject_t oid, eversion_t v);

  // -- copyfrom --
  map<hobject_t, CopyOpRef> copy_ops;

  int start_copy(CopyCallback *cb, ObjectContextRef obc, hobject_t src,
                 object_locator_t oloc, version_t version,
                 const hobject_t& temp_dest_oid);
  void process_copy_chunk(hobject_t oid, tid_t tid, int r);
  void _write_copy_chunk(CopyOpRef cop, ObjectStore::Transaction *t);
  void _copy_some(ObjectContextRef obc, CopyOpRef cop);
  void _build_finish_copy_transaction(CopyOpRef cop,
                                      ObjectStore::Transaction& t);
  int finish_copyfrom(OpContext *ctx);
  void cancel_copy(CopyOpRef cop);
  void cancel_copy_ops();

  friend class C_Copyfrom;

  // -- scrub --
  virtual void _scrub(ScrubMap& map);
  virtual void _scrub_clear_state();
  virtual void _scrub_finish();
  object_stat_collection_t scrub_cstat;

  virtual void _split_into(pg_t child_pgid, PG *child, unsigned split_bits);
  void apply_and_flush_repops(bool requeue);

  void calc_trim_to();
  int do_xattr_cmp_u64(int op, __u64 v1, bufferlist& xattr);
  int do_xattr_cmp_str(int op, string& v1s, bufferlist& xattr);

  bool pgls_filter(PGLSFilter *filter, hobject_t& sobj, bufferlist& outdata);
  int get_pgls_filter(bufferlist::iterator& iter, PGLSFilter **pfilter);

public:
  ReplicatedPG(OSDService *o, OSDMapRef curmap,
	       const PGPool &_pool, pg_t p, const hobject_t& oid,
	       const hobject_t& ioid);
  ~ReplicatedPG() {}

  int do_command(cmdmap_t cmdmap, ostream& ss, bufferlist& idata,
		 bufferlist& odata);

  void do_request(
    OpRequestRef op,
    ThreadPool::TPHandle &handle);
  void do_op(OpRequestRef op);
  bool pg_op_must_wait(MOSDOp *op);
  void do_pg_op(OpRequestRef op);
  void do_sub_op(OpRequestRef op);
  void do_sub_op_reply(OpRequestRef op);
  void do_scan(
    OpRequestRef op,
    ThreadPool::TPHandle &handle);
  void do_backfill(OpRequestRef op);

  RepGather *trim_object(const hobject_t &coid);
  void snap_trimmer();
  int do_osd_ops(OpContext *ctx, vector<OSDOp>& ops);

  int do_tmapup(OpContext *ctx, bufferlist::iterator& bp, OSDOp& osd_op);
  int do_tmapup_slow(OpContext *ctx, bufferlist::iterator& bp, OSDOp& osd_op, bufferlist& bl);

  void do_osd_op_effects(OpContext *ctx);
private:
  uint64_t temp_seq; ///< last id for naming temp objects
  coll_t get_temp_coll(ObjectStore::Transaction *t);
  hobject_t generate_temp_object();  ///< generate a new temp object name
public:
  void get_colls(list<coll_t> *out) {
    out->push_back(coll);
    return pgbackend->temp_colls(out);
  }
  void split_colls(
    pg_t child,
    int split_bits,
    int seed,
    ObjectStore::Transaction *t) {
    coll_t target = coll_t(child);
    t->create_collection(target);
    t->split_collection(
      coll,
      split_bits,
      seed,
      target);
    pgbackend->split_colls(child, split_bits, seed, t);
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
    bool requeue;
    SnapTrimmer(ReplicatedPG *pg) : pg(pg), need_share_pg_info(false), requeue(false) {}
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

  int _get_tmap(OpContext *ctx, map<string, bufferlist> *out,
		bufferlist *header);
  int _delete_head(OpContext *ctx);
  int _rollback_to(OpContext *ctx, ceph_osd_op& op);
public:
  bool same_for_read_since(epoch_t e);
  bool same_for_modify_since(epoch_t e);
  bool same_for_rep_modify_since(epoch_t e);

  bool is_missing_object(const hobject_t& oid);
  void wait_for_missing_object(const hobject_t& oid, OpRequestRef op);
  void wait_for_all_missing(OpRequestRef op);
  void wait_for_backfill_pos(OpRequestRef op);
  void release_waiting_for_backfill_pos();

  bool is_degraded_object(const hobject_t& oid);
  void wait_for_degraded_object(const hobject_t& oid, OpRequestRef op);

  void wait_for_blocked_object(const hobject_t& soid, OpRequestRef op);
  void kick_object_context_blocked(ObjectContextRef obc);

  void mark_all_unfound_lost(int what);
  eversion_t pick_newest_available(const hobject_t& oid);
  ObjectContextRef mark_object_lost(ObjectStore::Transaction *t,
				  const hobject_t& oid, eversion_t version,
				  utime_t mtime, int what);
  void _finish_mark_all_unfound_lost(list<ObjectContextRef>& obcs);

  void on_role_change();
  void on_change(ObjectStore::Transaction *t);
  void on_activate();
  void on_flushed() {
    assert(object_contexts.empty());
    pgbackend->on_flushed();
  }
  void on_removal(ObjectStore::Transaction *t);
  void on_shutdown();
};

inline ostream& operator<<(ostream& out, ReplicatedPG::RepGather& repop)
{
  out << "repgather(" << &repop
      << (repop.applying ? " applying" : "")
      << (repop.applied ? " applied" : "")
      << " " << repop.v
      << " rep_tid=" << repop.rep_tid 
      << " wfack=" << repop.waitfor_ack
    //<< " wfnvram=" << repop.waitfor_nvram
      << " wfdisk=" << repop.waitfor_disk;
  if (repop.ctx->op)
    out << " op=" << *(repop.ctx->op->get_req());
  out << ")";
  return out;
}

#endif
