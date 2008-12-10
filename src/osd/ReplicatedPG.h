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
   * keep tabs on object modifications that are in flight.
   * we need to know the projected existence, size, snapset,
   * etc., because we don't send writes down to disk until after
   * replicas ack.
   */
  struct ProjectedObjectInfo {
    int ref;
    pobject_t poid;

    SnapSet snapset;
    bool exists;
    __u64 size;
    eversion_t version;
    
    ProjectedObjectInfo() : ref(0), exists(false), size(0) {}
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
      noop(noop_),
      applied(false), aborted(false),
      sent_ack(false), sent_nvram(false), sent_disk(false),
      pinfo(i),
      at_version(av), 
      snapc(sc),
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
      if (--nref == 0) {
	delete op;
	delete this;
	generic_dout(0) << "deleting " << this << dendl;
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
  map<pobject_t, ProjectedObjectInfo> projected_objects;

  ProjectedObjectInfo *get_projected_object(pobject_t poid);
  void put_projected_object(ProjectedObjectInfo *pinfo);

  bool is_write_in_progress() {
    return !projected_objects.empty();
  }

  // load balancing
  set<object_t> balancing_reads;
  set<object_t> unbalancing_reads;
  hash_map<object_t, list<Message*> > waiting_for_unbalanced_reads;  // i.e. primary-lock

  
  // push/pull
  map<object_t, pair<eversion_t, int> > pulling;  // which objects are currently being pulled, and from where
  map<object_t, set<int> > pushing;

  void calc_head_subsets(SnapSet& snapset, pobject_t head,
			 Missing& missing,
			 interval_set<__u64>& data_subset,
			 map<pobject_t, interval_set<__u64> >& clone_subsets);
  void calc_clone_subsets(SnapSet& snapset, pobject_t poid, Missing& missing,
			  interval_set<__u64>& data_subset,
			  map<pobject_t, interval_set<__u64> >& clone_subsets);
  void push_to_replica(pobject_t oid, int dest);
  void push(pobject_t oid, int dest);
  void push(pobject_t oid, int dest, interval_set<__u64>& data_subset, 
	    map<pobject_t, interval_set<__u64> >& clone_subsets);
  bool pull(pobject_t oid);


  // modify
  void op_modify_ondisk(RepGather *repop);
  void sub_op_modify_ondisk(MOSDSubOp *op, int ackerosd, eversion_t last_complete);

  void _make_clone(ObjectStore::Transaction& t,
		   pobject_t head, pobject_t coid,
		   eversion_t ov, eversion_t v, bufferlist& snaps);
  void prepare_clone(ObjectStore::Transaction& t, bufferlist& logbl, osd_reqid_t reqid, pg_stat_t& st,
		     pobject_t poid, loff_t old_size,
		     eversion_t old_version, eversion_t& at_version,
		     SnapSet& snapset, SnapContext& snapc);
  void add_interval_usage(interval_set<__u64>& s, pg_stat_t& st);  
  int prepare_simple_op(ObjectStore::Transaction& t, osd_reqid_t reqid, pg_stat_t& st,
			pobject_t poid, __u64& old_size, bool& exists,
			ceph_osd_op& op, bufferlist::iterator& bp,
			SnapSet& snapset, SnapContext& snapc); 
  void prepare_transaction(ObjectStore::Transaction& t, osd_reqid_t reqid,
			   pobject_t poid, 
			   vector<ceph_osd_op>& ops, bufferlist& bl,
			   bool& exists, __u64& size, eversion_t& version,
			   eversion_t at_version,
			   SnapSet& snapset, SnapContext& snapc,
			   __u32 inc_lock, eversion_t trim_to);
  
  friend class C_OSD_ModifyCommit;
  friend class C_OSD_RepModifyCommit;

  // pg on-disk content
  void clean_up_local(ObjectStore::Transaction& t);

  void cancel_recovery();

  void queue_for_recovery();
  int start_recovery_ops(int max);
  void finish_recovery_op();
  int recover_primary(int max);
  int recover_replicas(int max);

  int pick_read_snap(pobject_t& poid);
  void op_read(MOSDOp *op);
  void op_modify(MOSDOp *op);

  void sub_op_modify(MOSDSubOp *op);
  void sub_op_modify_reply(MOSDSubOpReply *reply);
  void sub_op_push(MOSDSubOp *op);
  void sub_op_push_reply(MOSDSubOpReply *reply);
  void sub_op_pull(MOSDSubOp *op);


  // -- scrub --
  int _scrub(ScrubMap& map);



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

  bool is_missing_object(object_t oid);
  void wait_for_missing_object(object_t oid, Message *op);

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
  out << " op=" << *(repop.op);
  out << ")";
  return out;
}


#endif
