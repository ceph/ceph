// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#ifndef CEPH_REPLICATEDPG_H
#define CEPH_REPLICATEDPG_H

#include <boost/optional.hpp>

#include "include/assert.h" 

#include "PG.h"
#include "OSD.h"
#include "Watch.h"
#include "OpRequest.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
#include "messages/MOSDSubOp.h"
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

class ReplicatedPG : public PG {
  friend class OSD;
  friend class Watch;
public:  

  /*
    object access states:

    - idle
      - no in-progress or waiting writes.
      - read: ok
      - write: ok.  move to 'delayed' or 'rmw'
      - rmw: ok.  move to 'rmw'
	  
    - delayed
      - delayed write in progress.  delay write application on primary.
      - when done, move to 'idle'
      - read: ok
      - write: ok
      - rmw: no.  move to 'delayed-flushing'

    - rmw
      - rmw cycles in flight.  applied immediately at primary.
      - when done, move to 'idle'
      - read: same client ok.  otherwise, move to 'rmw-flushing'
      - write: same client ok.  otherwise, start write, but also move to 'rmw-flushing'
      - rmw: same client ok.  otherwise, move to 'rmw-flushing'
      
    - delayed-flushing
      - waiting for delayed writes to flush, then move to 'rmw'
      - read, write, rmw: wait

    - rmw-flushing
      - waiting for rmw to flush, then move to 'idle'
      - read, write, rmw: wait
    
   */

  struct AccessMode {
    typedef enum {
      IDLE,
      DELAYED,
      RMW,
      DELAYED_FLUSHING,
      RMW_FLUSHING
    } state_t;
    static const char *get_state_name(int s) {
      switch (s) {
      case IDLE: return "idle";
      case DELAYED: return "delayed";
      case RMW: return "rmw";
      case DELAYED_FLUSHING: return "delayed-flushing";
      case RMW_FLUSHING: return "rmw-flushing";
      default: return "???";
      }
    }
    state_t state;
    int num_wr;
    list<OpRequestRef> waiting;
    list<Cond*> waiting_cond;
    bool wake;

    AccessMode() : state(IDLE),
		   num_wr(0), wake(false) {}

    void check_mode() {
      assert(state != DELAYED_FLUSHING && state != RMW_FLUSHING);
      if (num_wr == 0)
	state = IDLE;
    }

    bool want_delayed() {
      check_mode();
      switch (state) {
      case IDLE:
	state = DELAYED;
      case DELAYED:
	return true;
      case RMW:
	state = RMW_FLUSHING;
	return true;
      case DELAYED_FLUSHING:
      case RMW_FLUSHING:
	return false;
      default:
	assert(0);
      }
    }
    bool want_rmw() {
      check_mode();
      switch (state) {
      case IDLE:
	state = RMW;
	return true;
      case DELAYED:
	state = DELAYED_FLUSHING;
	return false;
      case RMW:
	state = RMW_FLUSHING;
	return false;
      case DELAYED_FLUSHING:
      case RMW_FLUSHING:
	return false;
      default:
	assert(0);
      }
    }

    bool try_read(entity_inst_t& c) {
      check_mode();
      switch (state) {
      case IDLE:
      case DELAYED:
      case RMW:
	return true;
      case DELAYED_FLUSHING:
      case RMW_FLUSHING:
	return false;
      default:
	assert(0);
      }
    }
    bool try_write(entity_inst_t& c) {
      check_mode();
      switch (state) {
      case IDLE:
	state = RMW;  /* default to RMW; it's a better all around policy */
      case DELAYED:
      case RMW:
	return true;
      case DELAYED_FLUSHING:
      case RMW_FLUSHING:
	return false;
      default:
	assert(0);
      }
    }
    bool try_rmw(entity_inst_t& c) {
      check_mode();
      switch (state) {
      case IDLE:
	state = RMW;
	return true;
      case DELAYED:
	state = DELAYED_FLUSHING;
	return false;
      case RMW:
	return true;
      case DELAYED_FLUSHING:
      case RMW_FLUSHING:
	return false;
      default:
	assert(0);
      }
    }

    bool is_delayed_mode() {
      return state == DELAYED || state == DELAYED_FLUSHING;
    }
    bool is_rmw_mode() {
      return state == RMW || state == RMW_FLUSHING;
    }

    void write_start() {
      num_wr++;
      assert(state == DELAYED || state == RMW);
    }
    void write_applied() {
      assert(num_wr > 0);
      --num_wr;
      if (num_wr == 0) {
	state = IDLE;
	wake = true;
      }
    }
    void write_commit() {
    }
  };

  /*
   * Capture all object state associated with an in-progress read or write.
   */
  struct OpContext {
    OpRequestRef op;
    osd_reqid_t reqid;
    vector<OSDOp>& ops;

    const ObjectState *obs; // Old objectstate
    const SnapSet *snapset; // Old snapset

    ObjectState new_obs;  // resulting ObjectState
    SnapSet new_snapset;  // resulting SnapSet (in case of a write)
    //pg_stat_t new_stats;  // resulting Stats
    object_stat_sum_t delta_stats;

    bool modify;          // (force) modification (even if op_t is empty)
    bool user_modify;     // user-visible modification

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
    eversion_t reply_version;    // the version that we report the client (depends on the op)

    ObjectStore::Transaction op_t, local_t;
    vector<pg_log_entry_t> log;

    interval_set<uint64_t> modified_ranges;
    ObjectContext *obc;          // For ref counting purposes
    map<hobject_t,ObjectContext*> src_obc;
    ObjectContext *clone_obc;    // if we created a clone
    ObjectContext *snapset_obc;  // if we created/deleted a snapdir

    int data_off;        // FIXME: we may want to kill this msgr hint off at some point!

    MOSDOpReply *reply;

    utime_t readable_stamp;  // when applied on all replicas
    ReplicatedPG *pg;

    OpContext(const OpContext& other);
    const OpContext& operator=(const OpContext& other);

    OpContext(OpRequestRef _op, osd_reqid_t _reqid, vector<OSDOp>& _ops,
	      ObjectState *_obs, SnapSetContext *_ssc,
	      ReplicatedPG *_pg) :
      op(_op), reqid(_reqid), ops(_ops), obs(_obs), snapset(0),
      new_obs(_obs->oi, _obs->exists),
      modify(false), user_modify(false),
      bytes_written(0), bytes_read(0),
      obc(0), clone_obc(0), snapset_obc(0), data_off(0), reply(NULL), pg(_pg) { 
      if (_ssc) {
	new_snapset = _ssc->snapset;
	snapset = &_ssc->snapset;
      }
    }
    ~OpContext() {
      assert(!clone_obc);
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
    ObjectContext *obc;
    map<hobject_t,ObjectContext*> src_obc;

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
    
    RepGather(OpContext *c, ObjectContext *pi, tid_t rt, 
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
	delete ctx;
	delete this;
	//generic_dout(0) << "deleting " << this << dendl;
      }
    }
  };



protected:

  AccessMode mode;

  // replica ops
  // [primary|tail]
  xlist<RepGather*> repop_queue;
  map<tid_t, RepGather*> repop_map;

  void apply_repop(RepGather *repop);
  void op_applied(RepGather *repop);
  void op_commit(RepGather *repop);
  void eval_repop(RepGather*);
  void issue_repop(RepGather *repop, utime_t now,
		   eversion_t old_last_update, bool old_exists, uint64_t old_size, eversion_t old_version);
  RepGather *new_repop(OpContext *ctx, ObjectContext *obc, tid_t rep_tid);
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

  // projected object info
  map<hobject_t, ObjectContext*> object_contexts;
  map<object_t, SnapSetContext*> snapset_contexts;

  // debug order that client ops are applied
  map<hobject_t, map<client_t, tid_t> > debug_op_order;

  void populate_obc_watchers(ObjectContext *obc);
public:
  void handle_watch_timeout(WatchRef watch);
protected:

  ObjectContext *lookup_object_context(const hobject_t& soid) {
    if (object_contexts.count(soid)) {
      ObjectContext *obc = object_contexts[soid];
      obc->ref++;
      return obc;
    }
    return NULL;
  }
  ObjectContext *_lookup_object_context(const hobject_t& oid);
  ObjectContext *create_object_context(const object_info_t& oi, SnapSetContext *ssc);
  ObjectContext *get_object_context(const hobject_t& soid, const object_locator_t& oloc,
				    bool can_create);
  void register_object_context(ObjectContext *obc) {
    if (!obc->registered) {
      assert(object_contexts.count(obc->obs.oi.soid) == 0);
      obc->registered = true;
      object_contexts[obc->obs.oi.soid] = obc;
    }
    if (obc->ssc)
      register_snapset_context(obc->ssc);
  }

  void context_registry_on_change();
  void put_object_context(ObjectContext *obc);
  void put_object_contexts(map<hobject_t,ObjectContext*>& obcv);
  int find_object_context(const hobject_t& oid,
			  const object_locator_t& oloc,
			  ObjectContext **pobc,
			  bool can_create, snapid_t *psnapid=NULL);

  void add_object_context_to_pg_stat(ObjectContext *obc, pg_stat_t *stat);

  void get_src_oloc(const object_t& oid, const object_locator_t& oloc, object_locator_t& src_oloc);

  SnapSetContext *create_snapset_context(const object_t& oid);
  SnapSetContext *get_snapset_context(const object_t& oid, const string &key,
				      ps_t seed, bool can_create);
  void register_snapset_context(SnapSetContext *ssc) {
    if (!ssc->registered) {
      assert(snapset_contexts.count(ssc->oid) == 0);
      ssc->registered = true;
      snapset_contexts[ssc->oid] = ssc;
    }
  }
  void put_snapset_context(SnapSetContext *ssc);

  // push
  struct PushInfo {
    ObjectRecoveryProgress recovery_progress;
    ObjectRecoveryInfo recovery_info;
    int priority;

    void dump(Formatter *f) const {
      {
	f->open_object_section("recovery_progress");
	recovery_progress.dump(f);
	f->close_section();
      }
      {
	f->open_object_section("recovery_info");
	recovery_info.dump(f);
	f->close_section();
      }
    }
  };
  map<hobject_t, map<int, PushInfo> > pushing;

  // pull
  struct PullInfo {
    ObjectRecoveryProgress recovery_progress;
    ObjectRecoveryInfo recovery_info;
    int priority;

    void dump(Formatter *f) const {
      {
	f->open_object_section("recovery_progress");
	recovery_progress.dump(f);
	f->close_section();
      }
      {
	f->open_object_section("recovery_info");
	recovery_info.dump(f);
	f->close_section();
      }
    }

    bool is_complete() const {
      return recovery_progress.is_complete(recovery_info);
    }
  };
  map<hobject_t, PullInfo> pulling;

  ObjectRecoveryInfo recalc_subsets(const ObjectRecoveryInfo& recovery_info);
  static void trim_pushed_data(const interval_set<uint64_t> &copy_subset,
			       const interval_set<uint64_t> &intervals_received,
			       bufferlist data_received,
			       interval_set<uint64_t> *intervals_usable,
			       bufferlist *data_usable);
  void handle_pull_response(OpRequestRef op);
  void handle_push(OpRequestRef op);
  int send_push(int priority, int peer,
		const ObjectRecoveryInfo& recovery_info,
		ObjectRecoveryProgress progress,
		ObjectRecoveryProgress *out_progress = 0);
  int send_pull(int priority, int peer,
		const ObjectRecoveryInfo& recovery_info,
		ObjectRecoveryProgress progress);
  void submit_push_data(const ObjectRecoveryInfo &recovery_info,
			bool first,
			const interval_set<uint64_t> &intervals_included,
			bufferlist data_included,
			bufferlist omap_header,
			map<string, bufferptr> &attrs,
			map<string, bufferlist> &omap_entries,
			ObjectStore::Transaction *t);
  void submit_push_complete(ObjectRecoveryInfo &recovery_info,
			    ObjectStore::Transaction *t);

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
      f->open_array_section("pull_from_peer");
      for (map<int, set<hobject_t> >::const_iterator i = pull_from_peer.begin();
	   i != pull_from_peer.end();
	   ++i) {
	f->open_object_section("pulling_from");
	f->dump_int("pull_from", i->first);
	{
	  f->open_array_section("pulls");
	  for (set<hobject_t>::const_iterator j = i->second.begin();
	       j != i->second.end();
	       ++j) {
	    f->open_object_section("pull_info");
	    assert(pulling.count(*j));
	    pulling.find(*j)->second.dump(f);
	    f->close_section();
	  }
	  f->close_section();
	}
	f->close_section();
      }
      f->close_section();
    }
    {
      f->open_array_section("pushing");
      for (map<hobject_t, map<int, PushInfo> >::const_iterator i =
	     pushing.begin();
	   i != pushing.end();
	   ++i) {
	f->open_object_section("object");
	f->dump_stream("pushing") << i->first;
	{
	  f->open_array_section("pushing_to");
	  for (map<int, PushInfo>::const_iterator j = i->second.begin();
	       j != i->second.end();
	       ++j) {
	    f->open_object_section("push_progress");
	    f->dump_stream("object_pushing") << j->first;
	    {
	      f->open_object_section("push_info");
	      j->second.dump(f);
	      f->close_section();
	    }
	    f->close_section();
	  }
	  f->close_section();
	}
	f->close_section();
      }
      f->close_section();
    }
  }

  /// leading edge of backfill
  hobject_t backfill_pos;

  // Reverse mapping from osd peer to objects beging pulled from that peer
  map<int, set<hobject_t> > pull_from_peer;

  int recover_object_replicas(const hobject_t& soid, eversion_t v,
			      int priority);
  void calc_head_subsets(ObjectContext *obc, SnapSet& snapset, const hobject_t& head,
			 pg_missing_t& missing,
			 const hobject_t &last_backfill,
			 interval_set<uint64_t>& data_subset,
			 map<hobject_t, interval_set<uint64_t> >& clone_subsets);
  void calc_clone_subsets(SnapSet& snapset, const hobject_t& poid, const pg_missing_t& missing,
			  const hobject_t &last_backfill,
			  interval_set<uint64_t>& data_subset,
			  map<hobject_t, interval_set<uint64_t> >& clone_subsets);
  void push_to_replica(
    ObjectContext *obc,
    const hobject_t& oid,
    int dest,
    int priority);
  void push_start(int priority,
		  ObjectContext *obc,
		  const hobject_t& oid, int dest);
  void push_start(int priority,
		  ObjectContext *obc,
		  const hobject_t& soid, int peer,
		  eversion_t version,
		  interval_set<uint64_t> &data_subset,
		  map<hobject_t, interval_set<uint64_t> >& clone_subsets);
  void send_push_op_blank(const hobject_t& soid, int peer);

  void finish_degraded_object(const hobject_t& oid);

  // Cancels/resets pulls from peer
  void check_recovery_sources(const OSDMapRef map);
  int pull(
    const hobject_t& oid, eversion_t v,
    int priority);

  // low level ops

  void _make_clone(ObjectStore::Transaction& t,
		   const hobject_t& head, const hobject_t& coid,
		   object_info_t *poi);
  void make_writeable(OpContext *ctx);
  void log_op_stats(OpContext *ctx);

  void write_update_size_and_usage(object_stat_sum_t& stats, object_info_t& oi,
				   SnapSet& ss, interval_set<uint64_t>& modified,
				   uint64_t offset, uint64_t length, bool count_bytes);
  void add_interval_usage(interval_set<uint64_t>& s, object_stat_sum_t& st);  

  int prepare_transaction(OpContext *ctx);
  
  // pg on-disk content
  void clean_up_local(ObjectStore::Transaction& t);

  void _clear_recovery_state();

  void queue_for_recovery();
  int start_recovery_ops(int max, RecoveryCtx *prctx);
  int recover_primary(int max);
  int recover_replicas(int max);
  int recover_backfill(int max);

  /**
   * scan a (hash) range of objects in the current pg
   *
   * @begin first item should be >= this value
   * @min return at least this many items, unless we are done
   * @max return no more than this many items
   * @bi [out] resulting map of objects to eversion_t's
   */
  void scan_range(hobject_t begin, int min, int max, BackfillInterval *bi);

  void push_backfill_object(hobject_t oid, eversion_t v, eversion_t have, int peer);
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
    ObjectContext *obc, *obc2;
    C_OSD_OndiskWriteUnlock(ObjectContext *o, ObjectContext *o2=0) : obc(o), obc2(o2) {}
    void finish(int r) {
      obc->ondisk_write_unlock();
      if (obc2)
	obc2->ondisk_write_unlock();
    }
  };
  struct C_OSD_OndiskWriteUnlockList : public Context {
    list<ObjectContext*> *pls;
    C_OSD_OndiskWriteUnlockList(list<ObjectContext*> *l) : pls(l) {}
    void finish(int r) {
      for (list<ObjectContext*>::iterator p = pls->begin(); p != pls->end(); ++p)
	(*p)->ondisk_write_unlock();
    }
  };
  struct C_OSD_AppliedRecoveredObject : public Context {
    ReplicatedPGRef pg;
    ObjectStore::Transaction *t;
    ObjectContext *obc;
    C_OSD_AppliedRecoveredObject(ReplicatedPG *p, ObjectStore::Transaction *tt, ObjectContext *o) :
      pg(p), t(tt), obc(o) {}
    void finish(int r) {
      pg->_applied_recovered_object(t, obc);
    }
  };
  struct C_OSD_CommittedPushedObject : public Context {
    ReplicatedPGRef pg;
    OpRequestRef op;
    epoch_t epoch;
    eversion_t last_complete;
    C_OSD_CommittedPushedObject(
      ReplicatedPG *p, OpRequestRef o, epoch_t epoch, eversion_t lc) :
      pg(p), op(o), epoch(epoch), last_complete(lc) {
    }
    void finish(int r) {
      pg->_committed_pushed_object(op, epoch, last_complete);
    }
  };
  struct C_OSD_CompletedPushedObjectReplica : public Context {
    OSDService *osd;
    Message *reply;
    ConnectionRef conn;
    C_OSD_CompletedPushedObjectReplica (
      OSDService *osd,
      Message *reply,
      ConnectionRef conn) : osd(osd), reply(reply), conn(conn) {}
    void finish(int) {
      osd->send_message_osd_cluster(reply, conn.get());
    }
  };
  struct C_OSD_CompletedPull : public Context {
    ReplicatedPGRef pg;
    hobject_t hoid;
    epoch_t epoch;
    C_OSD_CompletedPull(
      ReplicatedPG *pg,
      const hobject_t &hoid,
      epoch_t epoch) : pg(pg), hoid(hoid), epoch(epoch) {}
    void finish(int) {
      pg->lock();
      if (!pg->pg_has_reset_since(epoch)) {
	pg->finish_recovery_op(hoid);
      }
      pg->unlock();
    }
  };
  friend class C_OSD_CompletedPull;
  struct C_OSD_AppliedRecoveredObjectReplica : public Context {
    ReplicatedPGRef pg;
    ObjectStore::Transaction *t;
    C_OSD_AppliedRecoveredObjectReplica(ReplicatedPG *p, ObjectStore::Transaction *tt) :
      pg(p), t(tt) {}
    void finish(int r) {
      pg->_applied_recovered_object_replica(t);
    }
  };

  void sub_op_remove(OpRequestRef op);

  void sub_op_modify(OpRequestRef op);
  void sub_op_modify_applied(RepModify *rm);
  void sub_op_modify_commit(RepModify *rm);

  void sub_op_modify_reply(OpRequestRef op);
  void _applied_recovered_object(ObjectStore::Transaction *t, ObjectContext *obc);
  void _applied_recovered_object_replica(ObjectStore::Transaction *t);
  void _committed_pushed_object(OpRequestRef op, epoch_t epoch, eversion_t lc);
  void recover_got(hobject_t oid, eversion_t v);
  void sub_op_push(OpRequestRef op);
  void _failed_push(OpRequestRef op);
  void sub_op_push_reply(OpRequestRef op);
  void sub_op_pull(OpRequestRef op);

  void log_subop_stats(OpRequestRef op, int tag_inb, int tag_lat);


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

  int do_command(vector<string>& cmd, ostream& ss, bufferlist& idata, bufferlist& odata);

  void do_op(OpRequestRef op);
  bool pg_op_must_wait(MOSDOp *op);
  void do_pg_op(OpRequestRef op);
  void do_sub_op(OpRequestRef op);
  void do_sub_op_reply(OpRequestRef op);
  void do_scan(OpRequestRef op);
  void do_backfill(OpRequestRef op);
  RepGather *trim_object(const hobject_t &coid);
  void snap_trimmer();
  int do_osd_ops(OpContext *ctx, vector<OSDOp>& ops);

  int do_tmapup(OpContext *ctx, bufferlist::iterator& bp, OSDOp& osd_op);
  int do_tmapup_slow(OpContext *ctx, bufferlist::iterator& bp, OSDOp& osd_op, bufferlist& bl);

  void do_osd_op_effects(OpContext *ctx);
private:
  bool temp_created;
  coll_t temp_coll;
  coll_t get_temp_coll(ObjectStore::Transaction *t);
public:
  bool have_temp_coll();
  coll_t get_temp_coll() {
    return temp_coll;
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
  int _copy_up_tmap(OpContext *ctx);
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

  void mark_all_unfound_lost(int what);
  eversion_t pick_newest_available(const hobject_t& oid);
  ObjectContext *mark_object_lost(ObjectStore::Transaction *t,
				  const hobject_t& oid, eversion_t version,
				  utime_t mtime, int what);
  void _finish_mark_all_unfound_lost(list<ObjectContext*>& obcs);

  void on_role_change();
  void on_change();
  void on_activate();
  void on_flushed();
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
    out << " op=" << *(repop.ctx->op->request);
  out << ")";
  return out;
}

inline ostream& operator<<(ostream& out, ReplicatedPG::AccessMode& mode)
{
  out << mode.get_state_name(mode.state) << "(wr=" << mode.num_wr;
  if (mode.wake)
    out << " WAKE";
  out << ")";
  return out;
}

#endif
