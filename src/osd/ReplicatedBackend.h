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

#include "PGBackend.h"

struct C_ReplicatedBackend_OnPullComplete;
class ReplicatedBackend : public PGBackend {
  struct RPGHandle : public PGBackend::RecoveryHandle {
    std::map<pg_shard_t, std::vector<PushOp> > pushes;
    std::map<pg_shard_t, std::vector<PullOp> > pulls;
  };
  friend struct C_ReplicatedBackend_OnPullComplete;
public:
  ReplicatedBackend(
    PGBackend::Listener *pg,
    const coll_t &coll,
    ObjectStore::CollectionHandle &ch,
    ObjectStore *store,
    CephContext *cct);

  /// @see PGBackend::open_recovery_op
  RPGHandle *_open_recovery_op() {
    return new RPGHandle();
  }
  PGBackend::RecoveryHandle *open_recovery_op() override {
    return _open_recovery_op();
  }

  /// @see PGBackend::run_recovery_op
  void run_recovery_op(
    PGBackend::RecoveryHandle *h,
    int priority) override;

  /// @see PGBackend::recover_object
  int recover_object(
    const hobject_t &hoid,
    eversion_t v,
    ObjectContextRef head,
    ObjectContextRef obc,
    RecoveryHandle *h
    ) override;

  void check_recovery_sources(const OSDMapRef& osdmap) override;

  bool can_handle_while_inactive(OpRequestRef op) override;

  /// @see PGBackend::handle_message
  bool _handle_message(
    OpRequestRef op
    ) override;

  void on_change() override;
  void clear_recovery_state() override;

  class RPCRecPred : public IsPGRecoverablePredicate {
  public:
    bool operator()(const std::set<pg_shard_t> &have) const override {
      return !have.empty();
    }
  };
  IsPGRecoverablePredicate *get_is_recoverable_predicate() const override {
    return new RPCRecPred;
  }

  class RPCReadPred : public IsPGReadablePredicate {
    pg_shard_t whoami;
  public:
    explicit RPCReadPred(pg_shard_t whoami) : whoami(whoami) {}
    bool operator()(const std::set<pg_shard_t> &have) const override {
      return have.count(whoami);
    }
  };
  IsPGReadablePredicate *get_is_readable_predicate() const override {
    return new RPCReadPred(get_parent()->whoami_shard());
  }

  void dump_recovery_info(ceph::Formatter *f) const override {
    {
      f->open_array_section("pull_from_peer");
      for (const auto& i : pull_from_peer) {
	f->open_object_section("pulling_from");
	f->dump_stream("pull_from") << i.first;
	{
	  f->open_array_section("pulls");
	  for (const auto& j : i.second) {
	    f->open_object_section("pull_info");
	    ceph_assert(pulling.count(j));
	    pulling.find(j)->second.dump(f);
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
      for(const auto& i : pushing) {
	f->open_object_section("object");
	f->dump_stream("pushing") << i.first;
	{
	  f->open_array_section("pushing_to");
	  for (const auto& j : i.second) {
	    f->open_object_section("push_progress");
	    f->dump_stream("pushing_to") << j.first;
	    {
	      f->open_object_section("push_info");
	      j.second.dump(f);
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

  int objects_read_sync(
    const hobject_t &hoid,
    uint64_t off,
    uint64_t len,
    uint32_t op_flags,
    ceph::buffer::list *bl) override;

  int objects_readv_sync(
    const hobject_t &hoid,
    std::map<uint64_t, uint64_t>& m,
    uint32_t op_flags,
    ceph::buffer::list *bl) override;

  void objects_read_async(
    const hobject_t &hoid,
    const std::list<std::pair<boost::tuple<uint64_t, uint64_t, uint32_t>,
	       std::pair<ceph::buffer::list*, Context*> > > &to_read,
               Context *on_complete,
               bool fast_read = false) override;

private:
  // push
  struct push_info_t {
    ObjectRecoveryProgress recovery_progress;
    ObjectRecoveryInfo recovery_info;
    ObjectContextRef obc;
    object_stat_sum_t stat;
    ObcLockManager lock_manager;

    void dump(ceph::Formatter *f) const {
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
  std::map<hobject_t, std::map<pg_shard_t, push_info_t>> pushing;

  // pull
  struct pull_info_t {
    pg_shard_t from;
    hobject_t soid;
    ObjectRecoveryProgress recovery_progress;
    ObjectRecoveryInfo recovery_info;
    ObjectContextRef head_ctx;
    ObjectContextRef obc;
    object_stat_sum_t stat;
    bool cache_dont_need;
    ObcLockManager lock_manager;

    void dump(ceph::Formatter *f) const {
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

  std::map<hobject_t, pull_info_t> pulling;

  // Reverse mapping from osd peer to objects being pulled from that peer
  std::map<pg_shard_t, std::set<hobject_t> > pull_from_peer;
  void clear_pull(
    std::map<hobject_t, pull_info_t>::iterator piter,
    bool clear_pull_from_peer = true);
  void clear_pull_from(
    std::map<hobject_t, pull_info_t>::iterator piter);

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

  bool handle_push_reply(pg_shard_t peer, const PushReplyOp &op, PushOp *reply);
  void handle_pull(pg_shard_t peer, PullOp &op, PushOp *reply);

  struct pull_complete_info {
    hobject_t hoid;
    object_stat_sum_t stat;
  };
  bool handle_pull_response(
    pg_shard_t from, const PushOp &op, PullOp *response,
    std::list<pull_complete_info> *to_continue,
    ObjectStore::Transaction *t);
  void handle_push(pg_shard_t from, const PushOp &op, PushReplyOp *response,
		   ObjectStore::Transaction *t, bool is_repair);

  static void trim_pushed_data(const interval_set<uint64_t> &copy_subset,
			       const interval_set<uint64_t> &intervals_received,
			       ceph::buffer::list data_received,
			       interval_set<uint64_t> *intervals_usable,
			       ceph::buffer::list *data_usable);
  void _failed_pull(pg_shard_t from, const hobject_t &soid);

  void send_pushes(int prio, std::map<pg_shard_t, std::vector<PushOp> > &pushes);
  void prep_push_op_blank(const hobject_t& soid, PushOp *op);
  void send_pulls(
    int priority,
    std::map<pg_shard_t, std::vector<PullOp> > &pulls);

  int build_push_op(const ObjectRecoveryInfo &recovery_info,
		    const ObjectRecoveryProgress &progress,
		    ObjectRecoveryProgress *out_progress,
		    PushOp *out_op,
		    object_stat_sum_t *stat = 0,
                    bool cache_dont_need = true);
  void submit_push_data(const ObjectRecoveryInfo &recovery_info,
			bool first,
			bool complete,
			bool clear_omap,
			bool cache_dont_need,
			interval_set<uint64_t> &data_zeros,
			const interval_set<uint64_t> &intervals_included,
			ceph::buffer::list data_included,
			ceph::buffer::list omap_header,
			const std::map<std::string, ceph::buffer::list, std::less<>> &attrs,
			const std::map<std::string, ceph::buffer::list> &omap_entries,
			ObjectStore::Transaction *t);
  void submit_push_complete(const ObjectRecoveryInfo &recovery_info,
			    ObjectStore::Transaction *t);

  void calc_clone_subsets(
    SnapSet& snapset, const hobject_t& poid, const pg_missing_t& missing,
    const hobject_t &last_backfill,
    interval_set<uint64_t>& data_subset,
    std::map<hobject_t, interval_set<uint64_t>>& clone_subsets,
    ObcLockManager &lock_manager);
  void prepare_pull(
    eversion_t v,
    const hobject_t& soid,
    ObjectContextRef headctx,
    RPGHandle *h);
  int start_pushes(
    const hobject_t &soid,
    ObjectContextRef obj,
    RPGHandle *h);
  int prep_push_to_replica(
    ObjectContextRef obc, const hobject_t& soid, pg_shard_t peer,
    PushOp *pop, bool cache_dont_need = true);
  int prep_push(
    ObjectContextRef obc,
    const hobject_t& oid, pg_shard_t dest,
    PushOp *op,
    bool cache_dont_need);
  int prep_push(
    ObjectContextRef obc,
    const hobject_t& soid, pg_shard_t peer,
    eversion_t version,
    interval_set<uint64_t> &data_subset,
    std::map<hobject_t, interval_set<uint64_t>>& clone_subsets,
    PushOp *op,
    bool cache,
    ObcLockManager &&lock_manager);
  void calc_head_subsets(
    ObjectContextRef obc, SnapSet& snapset, const hobject_t& head,
    const pg_missing_t& missing,
    const hobject_t &last_backfill,
    interval_set<uint64_t>& data_subset,
    std::map<hobject_t, interval_set<uint64_t>>& clone_subsets,
    ObcLockManager &lock_manager);
  ObjectRecoveryInfo recalc_subsets(
    const ObjectRecoveryInfo& recovery_info,
    SnapSetContext *ssc,
    ObcLockManager &lock_manager);

  /**
   * Client IO
   */
  struct InProgressOp : public RefCountedObject {
    ceph_tid_t tid;
    std::set<pg_shard_t> waiting_for_commit;
    Context *on_commit;
    OpRequestRef op;
    eversion_t v;
    bool done() const {
      return waiting_for_commit.empty();
    }
  private:
    FRIEND_MAKE_REF(InProgressOp);
    InProgressOp(ceph_tid_t tid, Context *on_commit, OpRequestRef op, eversion_t v)
      :
	tid(tid), on_commit(on_commit),
	op(op), v(v) {}
  };
  std::map<ceph_tid_t, ceph::ref_t<InProgressOp>> in_progress_ops;
public:
  friend class C_OSD_OnOpCommit;

  void call_write_ordered(std::function<void(void)> &&cb) override {
    // ReplicatedBackend submits writes inline in submit_transaction, so
    // we can just call the callback.
    cb();
  }

  void submit_transaction(
    const hobject_t &hoid,
    const object_stat_sum_t &delta_stats,
    const eversion_t &at_version,
    PGTransactionUPtr &&t,
    const eversion_t &trim_to,
    const eversion_t &min_last_complete_ondisk,
    std::vector<pg_log_entry_t>&& log_entries,
    std::optional<pg_hit_set_history_t> &hset_history,
    Context *on_all_commit,
    ceph_tid_t tid,
    osd_reqid_t reqid,
    OpRequestRef op
    ) override;

private:
  Message * generate_subop(
    const hobject_t &soid,
    const eversion_t &at_version,
    ceph_tid_t tid,
    osd_reqid_t reqid,
    eversion_t pg_trim_to,
    eversion_t min_last_complete_ondisk,
    hobject_t new_temp_oid,
    hobject_t discard_temp_oid,
    const ceph::buffer::list &log_entries,
    std::optional<pg_hit_set_history_t> &hset_history,
    ObjectStore::Transaction &op_t,
    pg_shard_t peer,
    const pg_info_t &pinfo);
  void issue_op(
    const hobject_t &soid,
    const eversion_t &at_version,
    ceph_tid_t tid,
    osd_reqid_t reqid,
    eversion_t pg_trim_to,
    eversion_t min_last_complete_ondisk,
    hobject_t new_temp_oid,
    hobject_t discard_temp_oid,
    const std::vector<pg_log_entry_t> &log_entries,
    std::optional<pg_hit_set_history_t> &hset_history,
    InProgressOp *op,
    ObjectStore::Transaction &op_t);
  void op_commit(const ceph::ref_t<InProgressOp>& op);
  void do_repop_reply(OpRequestRef op);
  void do_repop(OpRequestRef op);

  struct RepModify {
    OpRequestRef op;
    bool committed;
    int ackerosd;
    eversion_t last_complete;
    epoch_t epoch_started;

    ObjectStore::Transaction opt, localt;
    
    RepModify() : committed(false), ackerosd(-1),
		  epoch_started(0) {}
  };
  typedef std::shared_ptr<RepModify> RepModifyRef;

  struct C_OSD_RepModifyCommit;

  void repop_commit(RepModifyRef rm);
  bool auto_repair_supported() const override { return store->has_builtin_csum(); }


  int be_deep_scrub(
    const hobject_t &poid,
    ScrubMap &map,
    ScrubMapBuilder &pos,
    ScrubMap::object &o) override;

  uint64_t be_get_ondisk_size(uint64_t logical_size) const final {
    return logical_size;
  }
};

#endif
