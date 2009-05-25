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


  /*
   * keep tabs on object modifications that are in flight.
   * we need to know the projected existence, size, snapset,
   * etc., because we don't send writes down to disk until after
   * replicas ack.
   */
  struct ObjectContext {
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

    sobject_t soid;
    int ref;
    bool registered; 

    state_t state;

    int num_wr, num_rmw;
    entity_inst_t client;
    list<Message*> waiting;
    bool wake;

    bool exists;
    __u64 size;

    object_info_t oi;


    void get() { ++ref; }
    
    bool is_delayed_mode() {
      return state == DELAYED || state == DELAYED_FLUSHING;
    }
    bool is_rmw_mode() {
      return state == RMW || state == RMW_FLUSHING;
    }

    bool try_read(entity_inst_t& c) {
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
      switch (state) {
      case IDLE:
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

    void start_write() {
      num_wr++;
      assert(state == DELAYED || state == RMW);
    }
    void force_start_write() {
      num_wr++;
    }
    void finish_write() {
      assert(num_wr > 0);
      --num_wr;
      if (num_wr == 0)
	switch (state) {
	case DELAYED:
	  assert(!num_rmw);
	  state = IDLE;
	  wake = true;
	  break;
	case RMW:
	case DELAYED_FLUSHING:
	case RMW_FLUSHING:
	  if (!num_rmw && !num_wr) {
	    state = IDLE;
	    wake = true;
	  }
	  break;
	default:
	  assert(0);
	}
    }

    void start_rmw() {
      ++num_rmw;
      assert(state == RMW);
    }
    void finish_rmw() {
      assert(num_rmw > 0);
      --num_rmw;
      if (num_rmw == 0) {
	switch (state) {
	case RMW:
	case RMW_FLUSHING:
	  if (!num_rmw && !num_wr) {
	    state = IDLE;
	    wake = true;
	  }
	  break;
	default:
	  assert(0);
	}
      }
    }

    ObjectContext() : ref(0), registered(true), state(IDLE), num_wr(0), num_rmw(0), wake(false),
		      exists(false), size(0), oi(soid) {}
  };


  /*
   * Capture all object state associated with an in-progress read or write.
   */
  struct OpContext {
    Message *op;
    osd_reqid_t reqid;
    vector<ceph_osd_op>& ops;
    bufferlist& indata;
    bufferlist outdata;

    ObjectContext::state_t mode;  // DELAYED or RMW (or _FLUSHING variant?)
    object_info_t *poi;

    utime_t mtime;
    SnapContext snapc;           // writer snap context
    eversion_t at_version;       // pg's current version pointer

    ObjectStore::Transaction op_t, local_t;
    vector<PG::Log::Entry> log;

    ObjectContext *clone_obc;    // if we created a clone

    int data_off;        // FIXME: we may want to kill this msgr hint off at some point!

    OpContext(Message *_op, osd_reqid_t _reqid, vector<ceph_osd_op>& _ops, bufferlist& _data,
	      ObjectContext::state_t _mode, object_info_t *_poi) :
      op(_op), reqid(_reqid), ops(_ops), indata(_data), mode(_mode), poi(_poi),
      clone_obc(0), data_off(0) {}
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

    bool applied, aborted;

    set<int>  waitfor_ack;
    set<int>  waitfor_nvram;
    set<int>  waitfor_disk;
    bool sent_ack, sent_nvram, sent_disk;
    
    utime_t   start;
    
    eversion_t          pg_local_last_complete;
    map<int,eversion_t> pg_complete_thru;
    
    RepGather(OpContext *c, ObjectContext *pi, bool noop_, tid_t rt, 
	      eversion_t lc) :
      queue_item(this),
      nref(1),
      ctx(c), obc(pi),
      rep_tid(rt), 
      noop(noop_),
      applied(false), aborted(false),
      sent_ack(false), sent_nvram(false), sent_disk(false),
      pg_local_last_complete(lc) { }

    bool can_send_ack() { 
      return
	!sent_ack && !sent_nvram && !sent_disk &&
	waitfor_ack.empty(); 
    }
    bool can_send_nvram() { 
      return
	!sent_nvram && !sent_disk &&
	waitfor_ack.empty() && waitfor_disk.empty(); 
    }
    bool can_send_disk() { 
      return
	!sent_disk &&
	waitfor_ack.empty() && waitfor_nvram.empty() && waitfor_disk.empty(); 
    }
    bool can_delete() { 
      return waitfor_ack.empty() && waitfor_nvram.empty() && waitfor_disk.empty(); 
    }

    void get() {
      nref++;
    }
    void put() {
      assert(nref > 0);
      if (--nref == 0) {
	assert(!obc);
	delete ctx;
	delete this;
	//generic_dout(0) << "deleting " << this << dendl;
      }
    }
  };



protected:
  // replica ops
  // [primary|tail]
  xlist<RepGather*> repop_queue;
  map<tid_t, RepGather*> repop_map;

  void apply_repop(RepGather *repop);
  void eval_repop(RepGather*);
  void issue_repop(RepGather *repop, int dest, utime_t now,
		   bool old_exists, __u64 old_size, eversion_t old_version);
  RepGather *new_repop(OpContext *ctx, ObjectContext *obc, bool noop, tid_t rep_tid);
  void repop_ack(RepGather *repop,
                 int result, int ack_type,
                 int fromosd, eversion_t pg_complete_thru=eversion_t(0,0));


  // projected object info
  map<sobject_t, ObjectContext*> object_contexts;

  ObjectContext *get_object_context(const sobject_t& soid, bool can_create=true);
  void register_object_context(ObjectContext *obc) {
    if (!obc->registered) {
      obc->registered = true;
      object_contexts[obc->soid] = obc;
    }
  }
  void put_object_context(ObjectContext *obc);
  int find_object_context(object_t oid, snapid_t snapid, ObjectContext **pobc, bool can_create);

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

  void calc_head_subsets(SnapSet& snapset, sobject_t head,
			 Missing& missing,
			 interval_set<__u64>& data_subset,
			 map<sobject_t, interval_set<__u64> >& clone_subsets);
  void calc_clone_subsets(SnapSet& snapset, sobject_t poid, Missing& missing,
			  interval_set<__u64>& data_subset,
			  map<sobject_t, interval_set<__u64> >& clone_subsets);
  void push_to_replica(sobject_t oid, int dest);
  void push(sobject_t oid, int dest);
  void push(sobject_t oid, int dest, interval_set<__u64>& data_subset, 
	    map<sobject_t, interval_set<__u64> >& clone_subsets);
  bool pull(sobject_t oid);


  // modify
  void op_modify_ondisk(RepGather *repop);
  void sub_op_modify_ondisk(MOSDSubOp *op, int ackerosd, eversion_t last_complete);

  void _make_clone(ObjectStore::Transaction& t,
		   sobject_t head, sobject_t coid,
		   object_info_t *poi);
  void make_writeable(OpContext *ctx, __u64 size);
  int do_osd_ops(OpContext *ctx, vector<ceph_osd_op>& ops,
		 bufferlist::iterator& bp, bufferlist& odata,
		 bool& exists, __u64& size);

  void log_op_stats(const sobject_t &soid, OpContext *ctx);
  void add_interval_usage(interval_set<__u64>& s, pg_stat_t& st);  

  int prepare_transaction(OpContext *ctx, bool& exists, __u64& size);
  void log_op(OpContext *ctx);
  
  friend class C_OSD_ModifyCommit;
  friend class C_OSD_RepModifyCommit;

  // pg on-disk content
  void clean_up_local(ObjectStore::Transaction& t);

  void _clear_recovery_state();

  void queue_for_recovery();
  int start_recovery_ops(int max);
  void finish_recovery_op();
  int recover_primary(int max);
  int recover_replicas(int max);

  void sub_op_modify(MOSDSubOp *op);
  void sub_op_modify_reply(MOSDSubOpReply *reply);
  void sub_op_push(MOSDSubOp *op);
  void sub_op_push_reply(MOSDSubOpReply *reply);
  void sub_op_pull(MOSDSubOp *op);


  // -- scrub --
  int _scrub(ScrubMap& map);

  void apply_and_flush_repops(bool requeue);


public:
  ReplicatedPG(OSD *o, pg_t p) : 
    PG(o,p)
  { }
  ~ReplicatedPG() {}

  bool preprocess_op(MOSDOp *op, utime_t now);
  void do_op(MOSDOp *op);
  void do_sub_op(MOSDSubOp *op);
  void do_sub_op_reply(MOSDSubOpReply *op);
  bool snap_trimmer();

  bool same_for_read_since(epoch_t e);
  bool same_for_modify_since(epoch_t e);
  bool same_for_rep_modify_since(epoch_t e);

  bool is_missing_object(sobject_t oid);
  void wait_for_missing_object(sobject_t oid, Message *op);

  void on_osd_failure(int o);
  void on_acker_change();
  void on_role_change();
  void on_change();
  void on_shutdown();
};


inline ostream& operator<<(ostream& out, ReplicatedPG::ObjectContext& obc)
{
  out << "obc(" << obc.soid << " " << obc.get_state_name(obc.state);
  if (!obc.waiting.empty())
    out << " WAITING";
  out << ")";
  return out;
}

inline ostream& operator<<(ostream& out, ReplicatedPG::RepGather& repop)
{
  out << "repgather(" << &repop << " rep_tid=" << repop.rep_tid 
      << " wfack=" << repop.waitfor_ack
    //<< " wfnvram=" << repop.waitfor_nvram
      << " wfdisk=" << repop.waitfor_disk;
  out << " pct=" << repop.pg_complete_thru;
  if (repop.ctx->op)
    out << " op=" << *(repop.ctx->op);
  out << ")";
  return out;
}


#endif
