// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef REPBACKEND_H
#define REPBACKEND_H

#include "OSD.h"
#include "PGBackend.h"
#include "osd_types.h"

struct C_ReplicatedBackend_OnPullComplete;
class ReplicatedBackend : public PGBackend {
  struct RPGHandle : public PGBackend::RecoveryHandle {
    map<int, vector<PushOp> > pushes;
    map<int, vector<PullOp> > pulls;
  };
  friend struct C_ReplicatedBackend_OnPullComplete;
private:
  bool temp_created;
  const coll_t temp_coll;
  coll_t get_temp_coll() const {
    return temp_coll;
  }
  bool have_temp_coll() const { return temp_created; }

  // Track contents of temp collection, clear on reset
  set<hobject_t> temp_contents;
public:
  coll_t coll;
  OSDService *osd;
  CephContext *cct;

  ReplicatedBackend(PGBackend::Listener *pg, coll_t coll, OSDService *osd);

  /// @see PGBackend::open_recovery_op
  RPGHandle *_open_recovery_op() {
    return new RPGHandle();
  }
  PGBackend::RecoveryHandle *open_recovery_op() {
    return _open_recovery_op();
  }

  /// @see PGBackend::run_recovery_op
  void run_recovery_op(
    PGBackend::RecoveryHandle *h,
    int priority);

  /// @see PGBackend::recover_object
  void recover_object(
    const hobject_t &hoid,
    ObjectContextRef head,
    ObjectContextRef obc,
    RecoveryHandle *h
    );

  void check_recovery_sources(const OSDMapRef osdmap);

  /// @see PGBackend::delay_message_until_active
  bool can_handle_while_inactive(OpRequestRef op);

  /// @see PGBackend::handle_message
  bool handle_message(
    OpRequestRef op
    );

  void on_change(ObjectStore::Transaction *t);
  void clear_state();
  void on_flushed();

  void temp_colls(list<coll_t> *out) {
    if (temp_created)
      out->push_back(temp_coll);
  }
  void split_colls(
    pg_t child,
    int split_bits,
    int seed,
    ObjectStore::Transaction *t) {
    coll_t target = coll_t::make_temp_coll(child);
    if (!temp_created)
      return;
    t->create_collection(target);
    t->split_collection(
      temp_coll,
      split_bits,
      seed,
      target);
  }

  virtual void dump_recovery_info(Formatter *f) const {
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

  /// List objects in collection
  int objects_list_partial(
    const hobject_t &begin,
    int min,
    int max,
    snapid_t seq,
    vector<hobject_t> *ls,
    hobject_t *next);

  int objects_list_range(
    const hobject_t &start,
    const hobject_t &end,
    snapid_t seq,
    vector<hobject_t> *ls);

  int objects_get_attr(
    const hobject_t &hoid,
    const string &attr,
    bufferlist *out);
private:
  // push
  struct PushInfo {
    ObjectRecoveryProgress recovery_progress;
    ObjectRecoveryInfo recovery_info;
    ObjectContextRef obc;
    object_stat_sum_t stat;

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
    ObjectContextRef head_ctx;
    ObjectContextRef obc;
    object_stat_sum_t stat;

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

  coll_t get_temp_coll(ObjectStore::Transaction *t);
  void add_temp_obj(const hobject_t &oid) {
    temp_contents.insert(oid);
  }
  void clear_temp_obj(const hobject_t &oid) {
    temp_contents.erase(oid);
  }

  map<hobject_t, PullInfo> pulling;

  // Reverse mapping from osd peer to objects beging pulled from that peer
  map<int, set<hobject_t> > pull_from_peer;

  void sub_op_push(OpRequestRef op);
  void sub_op_push_reply(OpRequestRef op);
  void sub_op_pull(OpRequestRef op);

  void _do_push(OpRequestRef op);
  void _do_pull_response(OpRequestRef op);
  void do_push(OpRequestRef op) {
    if (is_primary()) {
      _do_pull_response(op);
    } else {
      _do_push(op);
    }
  }
  void do_pull(OpRequestRef op);
  void do_push_reply(OpRequestRef op);

  bool handle_push_reply(int peer, PushReplyOp &op, PushOp *reply);
  void handle_pull(int peer, PullOp &op, PushOp *reply);
  bool handle_pull_response(
    int from, PushOp &op, PullOp *response,
    list<hobject_t> *to_continue,
    ObjectStore::Transaction *t);
  void handle_push(int from, PushOp &op, PushReplyOp *response,
		   ObjectStore::Transaction *t);

  static void trim_pushed_data(const interval_set<uint64_t> &copy_subset,
			       const interval_set<uint64_t> &intervals_received,
			       bufferlist data_received,
			       interval_set<uint64_t> *intervals_usable,
			       bufferlist *data_usable);
  void _failed_push(int from, const hobject_t &soid);

  void send_pushes(int prio, map<int, vector<PushOp> > &pushes);
  void prep_push_op_blank(const hobject_t& soid, PushOp *op);
  int send_push_op_legacy(int priority, int peer,
			  PushOp &pop);
  int send_pull_legacy(int priority, int peer,
		       const ObjectRecoveryInfo& recovery_info,
		       ObjectRecoveryProgress progress);
  void send_pulls(
    int priority,
    map<int, vector<PullOp> > &pulls);

  int build_push_op(const ObjectRecoveryInfo &recovery_info,
		    const ObjectRecoveryProgress &progress,
		    ObjectRecoveryProgress *out_progress,
		    PushOp *out_op,
		    object_stat_sum_t *stat = 0);
  void submit_push_data(ObjectRecoveryInfo &recovery_info,
			bool first,
			bool complete,
			const interval_set<uint64_t> &intervals_included,
			bufferlist data_included,
			bufferlist omap_header,
			map<string, bufferptr> &attrs,
			map<string, bufferlist> &omap_entries,
			ObjectStore::Transaction *t);
  void submit_push_complete(ObjectRecoveryInfo &recovery_info,
			    ObjectStore::Transaction *t);

  void calc_clone_subsets(
    SnapSet& snapset, const hobject_t& poid, const pg_missing_t& missing,
    const hobject_t &last_backfill,
    interval_set<uint64_t>& data_subset,
    map<hobject_t, interval_set<uint64_t> >& clone_subsets);
  void prepare_pull(
    const hobject_t& soid,
    ObjectContextRef headctx,
    RPGHandle *h);
  int start_pushes(
    const hobject_t &soid,
    ObjectContextRef obj,
    RPGHandle *h);
  void prep_push_to_replica(
    ObjectContextRef obc, const hobject_t& soid, int peer,
    PushOp *pop);
  void prep_push(ObjectContextRef obc,
		 const hobject_t& oid, int dest,
		 PushOp *op);
  void prep_push(ObjectContextRef obc,
		 const hobject_t& soid, int peer,
		 eversion_t version,
		 interval_set<uint64_t> &data_subset,
		 map<hobject_t, interval_set<uint64_t> >& clone_subsets,
		 PushOp *op);
  void calc_head_subsets(ObjectContextRef obc, SnapSet& snapset, const hobject_t& head,
			 const pg_missing_t& missing,
			 const hobject_t &last_backfill,
			 interval_set<uint64_t>& data_subset,
			 map<hobject_t, interval_set<uint64_t> >& clone_subsets);
  ObjectRecoveryInfo recalc_subsets(
    const ObjectRecoveryInfo& recovery_info,
    SnapSetContext *ssc
    );
};

#endif
