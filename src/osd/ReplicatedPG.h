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

#ifndef __REPLICATEDPG_H
#define __REPLICATEDPG_H


#include "PG.h"

#include "messages/MOSDOp.h"
class MOSDSubOp;
class MOSDSubOpReply;

class ReplicatedPG : public PG {
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

    ObjectState(const sobject_t& s) : oi(s), exists(false), ssc(NULL) {}
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
    entity_inst_t client;
    list<Message*> waiting;
    bool wake;

    AccessMode() : state(IDLE),
		   num_wr(0), wake(false) {}

    void check_mode() {
      if (num_wr == 0)
	state = IDLE;
    }

    bool try_read(entity_inst_t& c) {
      check_mode();
      switch (state) {
      case IDLE:
      case DELAYED:
	return true;
      case RMW:
	if (c == client)
	  return true;
	state = RMW_FLUSHING;
	return false;
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
	if (g_conf.filestore_journal_writeahead ||
	    g_conf.filestore_journal_parallel) {
	  state = RMW;
	  client = c;
	} else
	  state = DELAYED;
      case DELAYED:
	return true;
      case RMW:
	if (c == client)
	  return true;
	state = RMW_FLUSHING;
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
	client = c;
	return true;
      case DELAYED:
	state = DELAYED_FLUSHING;
	return false;
      case RMW:
	if (c == client)
	  return true;
	state = RMW_FLUSHING;
	return false;
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

    ObjectContext(const sobject_t& s) :
      ref(0), registered(false), obs(s) {}

    void get() { ++ref; }
  };


  /*
   * Capture all object state associated with an in-progress read or write.
   */
  struct OpContext {
    Message *op;
    osd_reqid_t reqid;
    vector<OSDOp>& ops;
    bufferlist& indata;
    bufferlist outdata;

    ObjectState *obs;

    utime_t mtime;
    SnapContext snapc;           // writer snap context
    eversion_t at_version;       // pg's current version pointer

    ObjectStore::Transaction op_t, local_t;
    vector<PG::Log::Entry> log;

    ObjectContext *clone_obc;    // if we created a clone

    int data_off;        // FIXME: we may want to kill this msgr hint off at some point!

    ReplicatedPG *pg;

    OpContext(Message *_op, osd_reqid_t _reqid, vector<OSDOp>& _ops, bufferlist& _data,
	      ObjectState *_obs, ReplicatedPG *_pg) :
      op(_op), reqid(_reqid), ops(_ops), indata(_data), obs(_obs),
      clone_obc(0), data_off(0), pg(_pg) {}
    ~OpContext() {
      assert(!clone_obc);
    }
  };

  /*
   * State on the PG primary associated with the replicated mutation
   */
  class RepGather {
  public:
    xlist<RepGather*>::item queue_item;
    int nref;

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

    bool can_send_ack() { 
      return
	!sent_ack && /*!sent_nvram && */!sent_disk &&
	waitfor_ack.empty(); 
    }
    /*
    bool can_send_nvram() { 
      return
	!sent_nvram && !sent_disk &&
	waitfor_ack.empty() && waitfor_disk.empty(); 
    }
    */
    bool can_send_disk() { 
      return
	!sent_disk &&
	waitfor_disk.empty(); 
    }
    bool can_delete() { 
      return waitfor_ack.empty() && /*waitfor_nvram.empty() &&*/ waitfor_disk.empty(); 
    }

    void get() {
      nref++;
    }
    void put() {
      assert(nref > 0);
      if (--nref == 0) {
	assert(!obc);
	delete ctx->op;
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
  void issue_repop(RepGather *repop, int dest, utime_t now,
		   bool old_exists, __u64 old_size, eversion_t old_version);
  RepGather *new_repop(OpContext *ctx, ObjectContext *obc, bool noop, tid_t rep_tid);
  void repop_ack(RepGather *repop,
                 int result, int ack_type,
                 int fromosd, eversion_t pg_complete_thru=eversion_t(0,0));

  friend class C_OSD_OpCommit;
  friend class C_OSD_OpApplied;

  // projected object info
  map<sobject_t, ObjectContext*> object_contexts;
  map<object_t, SnapSetContext*> snapset_contexts;

  ObjectContext *get_object_context(const sobject_t& soid, bool can_create=true);
  void register_object_context(ObjectContext *obc) {
    if (!obc->registered) {
      obc->registered = true;
      object_contexts[obc->obs.oi.soid] = obc;
    }
    if (obc->obs.ssc)
      register_snapset_context(obc->obs.ssc);
  }
  void put_object_context(ObjectContext *obc);
  int find_object_context(const object_t& oid, snapid_t snapid, ObjectContext **pobc, bool can_create);

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

  
  // push/pull
  map<sobject_t, pair<eversion_t, int> > pulling;  // which objects are currently being pulled, and from where
  map<sobject_t, set<int> > pushing;

  void calc_head_subsets(SnapSet& snapset, const sobject_t& head,
			 Missing& missing,
			 interval_set<__u64>& data_subset,
			 map<sobject_t, interval_set<__u64> >& clone_subsets);
  void calc_clone_subsets(SnapSet& snapset, const sobject_t& poid, Missing& missing,
			  interval_set<__u64>& data_subset,
			  map<sobject_t, interval_set<__u64> >& clone_subsets);
  void push_to_replica(const sobject_t& oid, int dest);
  void push(const sobject_t& oid, int dest);
  void push(const sobject_t& oid, int dest, interval_set<__u64>& data_subset, 
	    map<sobject_t, interval_set<__u64> >& clone_subsets);
  bool pull(const sobject_t& oid);


  // low level ops

  void _make_clone(ObjectStore::Transaction& t,
		   const sobject_t& head, const sobject_t& coid,
		   object_info_t *poi);
  void make_writeable(OpContext *ctx);
  void log_op_stats(const sobject_t &soid, OpContext *ctx);
  void add_interval_usage(interval_set<__u64>& s, pg_stat_t& st);  

  int prepare_transaction(OpContext *ctx);
  void log_op(vector<Log::Entry>& log, eversion_t trim_to, ObjectStore::Transaction& t);
  
  // pg on-disk content
  void clean_up_local(ObjectStore::Transaction& t);

  void _clear_recovery_state();

  void queue_for_recovery();
  int start_recovery_ops(int max);
  int recover_primary(int max);
  int recover_replicas(int max);

  struct RepModify {
    ReplicatedPG *pg;
    MOSDSubOp *op;
    OpContext *ctx;
    bool applied, committed;
    int ackerosd;
    eversion_t last_complete;

    ObjectStore::Transaction opt, localt;
    list<ObjectStore::Transaction*> tls;
    
    RepModify() : pg(NULL), op(NULL), ctx(NULL), applied(false), committed(false), ackerosd(-1) {}
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

  void sub_op_modify(MOSDSubOp *op);
  void sub_op_modify_applied(RepModify *rm);
  void sub_op_modify_commit(RepModify *rm);

  void sub_op_modify_reply(MOSDSubOpReply *reply);
  void sub_op_push(MOSDSubOp *op);
  void sub_op_push_reply(MOSDSubOpReply *reply);
  void sub_op_pull(MOSDSubOp *op);

  void _committed(epoch_t same_since, eversion_t lc);
  friend class C_OSD_Commit;
  


  // -- scrub --
  int _scrub(ScrubMap& map, int& errors, int& fixed);

  void apply_and_flush_repops(bool requeue);

  void calc_trim_to();

public:
  ReplicatedPG(OSD *o, PGPool *_pool, pg_t p, const sobject_t& oid) : 
    PG(o, _pool, p, oid)
  { }
  ~ReplicatedPG() {}

  bool preprocess_op(MOSDOp *op, utime_t now);
  void do_op(MOSDOp *op);
  void do_pg_op(MOSDOp *op);
  void do_sub_op(MOSDSubOp *op);
  void do_sub_op_reply(MOSDSubOpReply *op);
  bool snap_trimmer();
  int do_osd_ops(OpContext *ctx, vector<OSDOp>& ops,
		 bufferlist& odata);

  bool same_for_read_since(epoch_t e);
  bool same_for_modify_since(epoch_t e);
  bool same_for_rep_modify_since(epoch_t e);

  bool is_missing_object(const sobject_t& oid);
  void wait_for_missing_object(const sobject_t& oid, Message *op);

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
