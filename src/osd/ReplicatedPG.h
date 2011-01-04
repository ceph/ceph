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


#include "PG.h"
#include "OSD.h"
#include "Watch.h"

#include "messages/MOSDOp.h"
#include "messages/MOSDOpReply.h"
class MOSDSubOp;
class MOSDSubOpReply;

class ReplicatedPG : public PG {
  friend class OSD;
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

  struct SnapSetContext {
    object_t oid;
    int ref;
    bool registered; 
    SnapSet snapset;

    SnapSetContext(const object_t& o) : oid(o), ref(0), registered(false) { }
  };

  struct ObjectState {
    object_info_t oi;
    bool exists;
    SnapSetContext *ssc;  // may be null

    ObjectState(const object_info_t &oi_, bool exists_, SnapSetContext *ssc_)
      : oi(oi_), exists(exists_), ssc(ssc_) {}
  };


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
    list<Message*> waiting;
    list<Cond*> waiting_cond;
    bool wake;

    AccessMode() : state(IDLE),
		   num_wr(0), wake(false) {}

    void check_mode() {
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
   * keep tabs on object modifications that are in flight.
   * we need to know the projected existence, size, snapset,
   * etc., because we don't send writes down to disk until after
   * replicas ack.
   */
  struct ObjectContext {
    int ref;
    bool registered; 
    ObjectState obs;

    Mutex lock;
    Cond cond;
    int unstable_writes, readers, writers_waiting, readers_waiting;

    // any entity in obs.oi.watchers MUST be in either watchers or unconnected_watchers.
    map<entity_name_t, OSD::Session *> watchers;
    map<entity_name_t, utime_t> unconnected_watchers;
    map<Watch::Notification *, bool> notifs;

    /*    ObjectContext(const sobject_t& s, const object_locator_t& ol) :
      ref(0), registered(false), obs(s, ol),
      lock("ReplicatedPG::ObjectContext::lock"),
      unstable_writes(0), readers(0), writers_waiting(0), readers_waiting(0) {}*/
    ObjectContext(const object_info_t &oi_, bool exists_, SnapSetContext *ssc_)
      : ref(0), registered(false), obs(oi_, exists_, ssc_),
      lock("ReplicatedPG::ObjectContext::lock"),
      unstable_writes(0), readers(0), writers_waiting(0), readers_waiting(0) {}

    void get() { ++ref; }

    // do simple synchronous mutual exclusion, for now.  now waitqueues or anything fancy.
    void ondisk_write_lock() {
      lock.Lock();
      writers_waiting++;
      while (readers_waiting || readers)
	cond.Wait(lock);
      writers_waiting--;
      unstable_writes++;
      lock.Unlock();
    }
    void ondisk_write_unlock() {
      lock.Lock();
      assert(unstable_writes > 0);
      unstable_writes--;
      if (!unstable_writes && readers_waiting)
	cond.Signal();
      lock.Unlock();
    }
    void ondisk_read_lock() {
      lock.Lock();
      readers_waiting++;
      while (unstable_writes)
	cond.Wait(lock);
      readers_waiting--;
      readers++;
      lock.Unlock();
    }
    void ondisk_read_unlock() {
      lock.Lock();
      assert(readers > 0);
      readers--;
      if (!readers && writers_waiting)
	cond.Signal();
      lock.Unlock();
    }
  };


  /*
   * Capture all object state associated with an in-progress read or write.
   */
  struct OpContext {
    Message *op;
    osd_reqid_t reqid;
    vector<OSDOp>& ops;
    bufferlist outdata;

    ObjectState *obs;

    uint64_t bytes_written;

    utime_t mtime;
    SnapContext snapc;           // writer snap context
    eversion_t at_version;       // pg's current version pointer
    eversion_t reply_version;    // the version that we report the client (depends on the op)

    ObjectStore::Transaction op_t, local_t;
    vector<PG::Log::Entry> log;

    ObjectContext *obc;
    ObjectContext *clone_obc;    // if we created a clone
    ObjectContext *snapset_obc;  // if we created/deleted a snapdir

    int data_off;        // FIXME: we may want to kill this msgr hint off at some point!

    MOSDOpReply *reply;

    ReplicatedPG *pg;

    OpContext(Message *_op, osd_reqid_t _reqid, vector<OSDOp>& _ops,
	      ObjectState *_obs, ReplicatedPG *_pg) :
      op(_op), reqid(_reqid), ops(_ops), obs(_obs),
      bytes_written(0),
      obc(0), clone_obc(0), snapset_obc(0), data_off(0), reply(NULL), pg(_pg) {}
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

    tid_t rep_tid;
    bool noop;

    bool applying, applied, aborted;

    set<int>  waitfor_ack;
    //set<int>  waitfor_nvram;
    set<int>  waitfor_disk;
    bool sent_ack;
    //bool sent_nvram;
    bool sent_disk;
    
    utime_t   start;
    
    eversion_t          pg_local_last_complete;

    list<ObjectStore::Transaction*> tls;
    
    RepGather(OpContext *c, ObjectContext *pi, bool noop_, tid_t rt, 
	      eversion_t lc) :
      queue_item(this),
      nref(1),
      ctx(c), obc(pi),
      rep_tid(rt), 
      noop(noop_),
      applying(false), applied(false), aborted(false),
      sent_ack(false),
      //sent_nvram(false),
      sent_disk(false),
      pg_local_last_complete(lc) { }

    void get() {
      nref++;
    }
    void put() {
      assert(nref > 0);
      if (--nref == 0) {
	assert(!obc);
	if (ctx->op)
	  ctx->op->put();
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
  RepGather *new_repop(OpContext *ctx, ObjectContext *obc, bool noop, tid_t rep_tid);
  void remove_repop(RepGather *repop);
  void repop_ack(RepGather *repop,
                 int result, int ack_type,
                 int fromosd, eversion_t pg_complete_thru=eversion_t(0,0));

  friend class C_OSD_OpCommit;
  friend class C_OSD_OpApplied;

  // projected object info
  map<sobject_t, ObjectContext*> object_contexts;
  map<object_t, SnapSetContext*> snapset_contexts;

  ObjectContext *lookup_object_context(const sobject_t& soid) {
    if (object_contexts.count(soid)) {
      ObjectContext *obc = object_contexts[soid];
      obc->ref++;
      return obc;
    }
    return NULL;
  }
  ObjectContext *get_object_context(const sobject_t& soid, const object_locator_t& oloc,
				    bool can_create);
  void register_object_context(ObjectContext *obc) {
    if (!obc->registered) {
      obc->registered = true;
      object_contexts[obc->obs.oi.soid] = obc;
    }
    if (obc->obs.ssc)
      register_snapset_context(obc->obs.ssc);
  }
  void put_object_context(ObjectContext *obc);
  int find_object_context(const object_t& oid, const object_locator_t& oloc,
			  snapid_t snapid, ObjectContext **pobc,
			  bool can_create, snapid_t *psnapid=NULL);

  SnapSetContext *get_snapset_context(const object_t& oid, bool can_create);
  void register_snapset_context(SnapSetContext *ssc) {
    if (!ssc->registered) {
      ssc->registered = true;
      snapset_contexts[ssc->oid] = ssc;
    }
  }
  void put_snapset_context(SnapSetContext *ssc);

  bool is_write_in_progress() {
    return !object_contexts.empty();
  }

  // load balancing
  set<sobject_t> balancing_reads;
  set<sobject_t> unbalancing_reads;
  hash_map<sobject_t, list<Message*> > waiting_for_unbalanced_reads;  // i.e. primary-lock

  
  // pull
  struct pull_info_t {
    eversion_t version;
    int from;
    bool need_size;
    interval_set<uint64_t> data_subset, data_subset_pulling;
  };
  map<sobject_t, pull_info_t> pulling;

  // push
  struct push_info_t {
    uint64_t size;
    eversion_t version;
    interval_set<uint64_t> data_subset, data_subset_pushing;
    map<sobject_t, interval_set<uint64_t> > clone_subsets;
  };
  map<sobject_t, map<int, push_info_t> > pushing;

  int recover_object_replicas(const sobject_t& soid);
  void calc_head_subsets(SnapSet& snapset, const sobject_t& head,
			 Missing& missing,
			 interval_set<uint64_t>& data_subset,
			 map<sobject_t, interval_set<uint64_t> >& clone_subsets);
  void calc_clone_subsets(SnapSet& snapset, const sobject_t& poid, Missing& missing,
			  interval_set<uint64_t>& data_subset,
			  map<sobject_t, interval_set<uint64_t> >& clone_subsets);
  void push_to_replica(ObjectContext *obc, const sobject_t& oid, int dest);
  void push_start(const sobject_t& oid, int dest);
  void push_start(const sobject_t& soid, int peer,
		  uint64_t size, eversion_t version,
		  interval_set<uint64_t> &data_subset,
		  map<sobject_t, interval_set<uint64_t> >& clone_subsets);
  void send_push_op(const sobject_t& oid, int dest,
		    uint64_t size, bool first, bool complete,
		    interval_set<uint64_t>& data_subset, 
		    map<sobject_t, interval_set<uint64_t> >& clone_subsets);

  bool pull(const sobject_t& oid);
  void send_pull_op(const sobject_t& soid, eversion_t v, bool first, const interval_set<uint64_t>& data_subset, int fromosd);


  // low level ops

  void _make_clone(ObjectStore::Transaction& t,
		   const sobject_t& head, const sobject_t& coid,
		   object_info_t *poi);
  void make_writeable(OpContext *ctx);
  void log_op_stats(OpContext *ctx);
  void add_interval_usage(interval_set<uint64_t>& s, pg_stat_t& st);  

  int prepare_transaction(OpContext *ctx);
  void log_op(vector<Log::Entry>& log, eversion_t trim_to, ObjectStore::Transaction& t);
  
  // pg on-disk content
  void remove_object_with_snap_hardlinks(ObjectStore::Transaction& t, const sobject_t& soid);
  void clean_up_local(ObjectStore::Transaction& t);

  void _clear_recovery_state();

  void queue_for_recovery();
  int start_recovery_ops(int max);
  int recover_primary(int max);
  int recover_replicas(int max);

  void dump_watchers(ObjectContext *obc);
  void do_complete_notify(Watch::Notification *notif, ObjectContext *obc);

  struct RepModify {
    ReplicatedPG *pg;
    MOSDSubOp *op;
    OpContext *ctx;
    bool applied, committed;
    int ackerosd;
    eversion_t last_complete;

    uint64_t bytes_written;

    ObjectStore::Transaction opt, localt;
    list<ObjectStore::Transaction*> tls;
    
    RepModify() : pg(NULL), op(NULL), ctx(NULL), applied(false), committed(false), ackerosd(-1),
		  bytes_written(0) {}
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
  struct C_OSD_WrotePushedObject : public Context {
    ReplicatedPG *pg;
    ObjectStore::Transaction *t;
    ObjectContext *obc;
    C_OSD_WrotePushedObject(ReplicatedPG *p, ObjectStore::Transaction *tt, ObjectContext *o) :
      pg(p), t(tt), obc(o) {}
    void finish(int r) {
      pg->_wrote_pushed_object(t, obc);
    }
  };

  void sub_op_modify(MOSDSubOp *op);
  void sub_op_modify_applied(RepModify *rm);
  void sub_op_modify_commit(RepModify *rm);

  void sub_op_modify_reply(MOSDSubOpReply *reply);
  void _wrote_pushed_object(ObjectStore::Transaction *t, ObjectContext *obc);
  void sub_op_push(MOSDSubOp *op);
  void sub_op_push_reply(MOSDSubOpReply *reply);
  void sub_op_pull(MOSDSubOp *op);

  void _committed(epoch_t same_since, eversion_t lc);
  friend class C_OSD_Commit;
  


  // -- scrub --
  int _scrub(ScrubMap& map, int& errors, int& fixed);

  void apply_and_flush_repops(bool requeue);

  void calc_trim_to();
  int do_xattr_cmp_u64(int op, __u64 v1, bufferlist& xattr);
  int do_xattr_cmp_str(int op, string& v1s, bufferlist& xattr);

  int prepare_call(MOSDOp *osd_op, ceph_osd_op& op,
		   string& cname, string& mname,
		   bufferlist::iterator& bp,
		   ClassHandler::ClassMethod **pmethod);

  bool pgls_filter(sobject_t& sobj, bufferlist::iterator& bp);

public:
  ReplicatedPG(OSD *o, PGPool *_pool, pg_t p, const sobject_t& oid, const sobject_t& ioid) : 
    PG(o, _pool, p, oid, ioid)
  { }
  ~ReplicatedPG() {}

  void do_op(MOSDOp *op);
  void do_pg_op(MOSDOp *op);
  void do_sub_op(MOSDSubOp *op);
  void do_sub_op_reply(MOSDSubOpReply *op);
  bool snap_trimmer();
  int do_osd_ops(OpContext *ctx, vector<OSDOp>& ops,
		 bufferlist& odata);
private:
  void _delete_head(OpContext *ctx);
  void _rollback_to(OpContext *ctx, ceph_osd_op& op);
public:
  bool same_for_read_since(epoch_t e);
  bool same_for_modify_since(epoch_t e);
  bool same_for_rep_modify_since(epoch_t e);

  bool is_missing_object(const sobject_t& oid);
  void wait_for_missing_object(const sobject_t& oid, Message *op);

  bool is_degraded_object(const sobject_t& oid);
  void wait_for_degraded_object(const sobject_t& oid, Message *op);

  void on_osd_failure(int o);
  void on_acker_change();
  void on_role_change();
  void on_change();
  void on_shutdown();
};


inline ostream& operator<<(ostream& out, ReplicatedPG::ObjectState& obs)
{
  out << obs.oi.soid;
  if (!obs.exists)
    out << "(dne)";
  return out;
}

inline ostream& operator<<(ostream& out, ReplicatedPG::ObjectContext& obc)
{
  return out << "obc(" << obc.obs << ")";
}

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
    out << " op=" << *(repop.ctx->op);
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
