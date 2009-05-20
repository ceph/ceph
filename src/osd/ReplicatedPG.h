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
   * Capture all object state associated with an in-progress read.
   */
  struct ReadOpContext {
    MOSDOp *op;
    vector<ceph_osd_op>& ops;

    object_info_t oi;
    int data_off;        // FIXME: we may want to kill this msgr hint off at some point!

    ReadOpContext(MOSDOp *_op, vector<ceph_osd_op>& _ops, sobject_t _soid) :
      op(_op), ops(_ops), oi(_soid), data_off(0) {}
  };


  /*
   * keep tabs on object modifications that are in flight.
   * we need to know the projected existence, size, snapset,
   * etc., because we don't send writes down to disk until after
   * replicas ack.
   */
  struct ProjectedObjectInfo {
    int ref;
    sobject_t poid;

    bool exists;
    __u64 size;

    object_info_t oi;
    
    ProjectedObjectInfo() : ref(0), exists(false), size(0), oi(poid) {}
  };

  /*
   * gather state on the primary/head while replicating an osd op.
   */
  class RepGather {
  public:
    xlist<RepGather*>::item queue_item;
    int nref;

    class MOSDOp *op;
    tid_t rep_tid;
    utime_t mtime;
    bool noop;

    ObjectStore::Transaction t;
    bool applied, aborted;

    set<int>  waitfor_ack;
    set<int>  waitfor_nvram;
    set<int>  waitfor_disk;
    bool sent_ack, sent_nvram, sent_disk;
    
    utime_t   start;
    
    ProjectedObjectInfo *pinfo;

    eversion_t at_version;
    SnapContext snapc;

    eversion_t          pg_local_last_complete;
    map<int,eversion_t> pg_complete_thru;
    
    RepGather(MOSDOp *o, bool noop_, tid_t rt, 
	      ProjectedObjectInfo *i,
	      eversion_t av, eversion_t lc,
	      SnapContext& sc) :
      queue_item(this),
      nref(1), op(o), rep_tid(rt), 
      mtime(op->get_mtime()), noop(noop_),
      applied(false), aborted(false),
      sent_ack(false), sent_nvram(false), sent_disk(false),
      pinfo(i),
      at_version(av), 
      snapc(sc),
      pg_local_last_complete(lc) {
      mtime = op->get_mtime();
    }

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
	delete op;
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
  void issue_repop(RepGather *repop, int dest, utime_t now);
  RepGather *new_repop(MOSDOp *op, bool noop, tid_t rep_tid,
		       ProjectedObjectInfo *pinfo,
		       eversion_t nv,
		       SnapContext& snapc);
  void repop_ack(RepGather *repop,
                 int result, int ack_type,
                 int fromosd, eversion_t pg_complete_thru=eversion_t(0,0));


  // projected object info
  map<sobject_t, ProjectedObjectInfo> projected_objects;

  ProjectedObjectInfo *get_projected_object(sobject_t poid);
  void put_projected_object(ProjectedObjectInfo *pinfo);

  bool is_write_in_progress() {
    return !projected_objects.empty();
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
		   eversion_t ov, eversion_t v, osd_reqid_t& reqid, utime_t mtime, vector<snapid_t>& snaps);
  void prepare_clone(ObjectStore::Transaction& t, bufferlist& logbl, osd_reqid_t reqid, pg_stat_t& st,
		     sobject_t poid, loff_t old_size, object_info_t& oi,
		     eversion_t& at_version, SnapContext& snapc);
  void add_interval_usage(interval_set<__u64>& s, pg_stat_t& st);  
  int prepare_simple_op(ObjectStore::Transaction& t, osd_reqid_t reqid, pg_stat_t& st,
			sobject_t poid, __u64& old_size, bool& exists, object_info_t& oi,
			vector<ceph_osd_op>& ops, int opn, bufferlist::iterator& bp, SnapContext& snapc); 
  void prepare_transaction(ObjectStore::Transaction& t, osd_reqid_t reqid,
			   sobject_t poid, 
			   vector<ceph_osd_op>& ops, bufferlist& bl, utime_t mtime,
			   bool& exists, __u64& size, object_info_t& oi,
			   eversion_t at_version, SnapContext& snapc,
			   eversion_t trim_to);
  
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

  int pick_read_snap(sobject_t& poid, object_info_t& coi);
  void op_read(MOSDOp *op);
  void op_modify(MOSDOp *op);

  int do_read_ops(ReadOpContext *ctx, bufferlist::iterator& bp, bufferlist& data);

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


inline ostream& operator<<(ostream& out, ReplicatedPG::RepGather& repop)
{
  out << "repgather(" << &repop << " rep_tid=" << repop.rep_tid 
      << " wfack=" << repop.waitfor_ack
    //<< " wfnvram=" << repop.waitfor_nvram
      << " wfdisk=" << repop.waitfor_disk;
  out << " pct=" << repop.pg_complete_thru;
  if (repop.op)
    out << " op=" << *(repop.op);
  out << ")";
  return out;
}


#endif
