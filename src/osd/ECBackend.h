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

#ifndef ECBACKEND_H
#define ECBACKEND_H

#include <boost/intrusive/set.hpp>
#include <boost/intrusive/list.hpp>

#include "ECCommon.h"
#include "OSD.h"
#include "PGBackend.h"
#include "erasure-code/ErasureCodeInterface.h"
#include "ECUtil.h"
#include "ECTransaction.h"
#include "ExtentCache.h"

//forward declaration
struct ECSubWrite;
struct ECSubWriteReply;
struct ECSubRead;
struct ECSubReadReply;

struct RecoveryMessages;

class ECBackend : public PGBackend, public ECCommon {
public:
  RecoveryHandle *open_recovery_op() override;

  void run_recovery_op(
    RecoveryHandle *h,
    int priority
    ) override;

  int recover_object(
    const hobject_t &hoid,
    eversion_t v,
    ObjectContextRef head,
    ObjectContextRef obc,
    RecoveryHandle *h
    ) override;

  bool _handle_message(
    OpRequestRef op
    ) override;
  bool can_handle_while_inactive(
    OpRequestRef op
    ) override;
  friend struct SubWriteApplied;
  friend struct SubWriteCommitted;
  void sub_write_committed(
    ceph_tid_t tid,
    eversion_t version,
    eversion_t last_complete,
    const ZTracer::Trace &trace);
  void handle_sub_write(
    pg_shard_t from,
    OpRequestRef msg,
    ECSubWrite &op,
    const ZTracer::Trace &trace,
    ECListener& eclistener
    ) override;
  void handle_sub_read(
    pg_shard_t from,
    const ECSubRead &op,
    ECSubReadReply *reply,
    const ZTracer::Trace &trace
    );
  void handle_sub_write_reply(
    pg_shard_t from,
    const ECSubWriteReply &op,
    const ZTracer::Trace &trace
    );
  void handle_sub_read_reply(
    pg_shard_t from,
    ECSubReadReply &op,
    const ZTracer::Trace &trace
    );

  /// @see ReadOp below
  void check_recovery_sources(const OSDMapRef& osdmap) override;

  void on_change() override;
  void clear_recovery_state() override;

  void dump_recovery_info(ceph::Formatter *f) const override;

  void call_write_ordered(std::function<void(void)> &&cb) override {
    rmw_pipeline.call_write_ordered(std::move(cb));
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

  int objects_read_sync(
    const hobject_t &hoid,
    uint64_t off,
    uint64_t len,
    uint32_t op_flags,
    ceph::buffer::list *bl) override;

  /**
   * Async read mechanism
   *
   * Async reads use the same async read mechanism as does recovery.
   * CallClientContexts is responsible for reconstructing the response
   * buffer as well as for calling the callbacks.
   *
   * One tricky bit is that two reads may possibly not read from the same
   * std::set of replicas.  This could result in two reads completing in the
   * wrong (from the interface user's point of view) order.  Thus, we
   * maintain a queue of in progress reads (@see in_progress_client_reads)
   * to ensure that we always call the completion callback in order.
   *
   * Another subtly is that while we may read a degraded object, we will
   * still only perform a client read from shards in the acting std::set.  This
   * ensures that we won't ever have to restart a client initiated read in
   * check_recovery_sources.
   */
  void objects_read_and_reconstruct(
    const std::map<hobject_t, std::list<boost::tuple<uint64_t, uint64_t, uint32_t> >
    > &reads,
    bool fast_read,
    GenContextURef<std::map<hobject_t,std::pair<int, extent_map> > &&> &&func) override;

  void objects_read_async(
    const hobject_t &hoid,
    const std::list<std::pair<boost::tuple<uint64_t, uint64_t, uint32_t>,
		    std::pair<ceph::buffer::list*, Context*> > > &to_read,
    Context *on_complete,
    bool fast_read = false) override;

private:
  friend struct ECRecoveryHandle;

  void kick_reads();

  /**
   * Recovery
   *
   * Recovery uses the same underlying read mechanism as client reads
   * with the slight difference that recovery reads may come from non
   * acting shards.  Thus, check_recovery_sources may wind up calling
   * cancel_pull for a read originating with RecoveryOp.
   *
   * The recovery process is expressed as a state machine:
   * - IDLE: Nothing is currently in progress, reads will be started and
   *         we will transition to READING
   * - READING: We are awaiting a pending read op.  Once complete, we will
   *            decode the buffers and proceed to WRITING
   * - WRITING: We are awaiting a completed push.  Once complete, we will
   *            either transition to COMPLETE or to IDLE to continue.
   * - COMPLETE: complete
   *
   * We use the existing Push and PushReply messages and structures to
   * handle actually shuffling the data over to the replicas.  recovery_info
   * and recovery_progress are expressed in terms of the logical offset
   * space except for data_included which is in terms of the chunked object
   * space (to match the passed buffer).
   *
   * xattrs are requested on the first read and used to initialize the
   * object_context if missing on completion of the first read.
   *
   * In order to batch up reads and writes, we batch Push, PushReply,
   * Transaction, and reads in a RecoveryMessages object which is passed
   * among the recovery methods.
   */
public:
  struct RecoveryBackend {
    CephContext* cct;
    const coll_t &coll;
    ceph::ErasureCodeInterfaceRef ec_impl;
    const ECUtil::stripe_info_t& sinfo;
    ReadPipeline& read_pipeline;
    UnstableHashInfoRegistry& unstable_hashinfo_registry;
    // TODO: lay an interface down here
    ECListener* parent;
    ECBackend* ecbackend;

    ECListener *get_parent() const { return parent; }
    const OSDMapRef& get_osdmap() const { return get_parent()->pgb_get_osdmap(); }
    epoch_t get_osdmap_epoch() const { return get_parent()->pgb_get_osdmap_epoch(); }
    const pg_info_t &get_info() { return get_parent()->get_info(); }
    void add_temp_obj(const hobject_t &oid) { get_parent()->add_temp_obj(oid); }
    void clear_temp_obj(const hobject_t &oid) { get_parent()->clear_temp_obj(oid); }

    RecoveryBackend(CephContext* cct,
		    const coll_t &coll,
		    ceph::ErasureCodeInterfaceRef ec_impl,
		    const ECUtil::stripe_info_t& sinfo,
		    ReadPipeline& read_pipeline,
		    UnstableHashInfoRegistry& unstable_hashinfo_registry,
		    ECListener* parent,
		    ECBackend* ecbackend);
  struct RecoveryOp {
    hobject_t hoid;
    eversion_t v;
    std::set<pg_shard_t> missing_on;
    std::set<shard_id_t> missing_on_shards;

    ObjectRecoveryInfo recovery_info;
    ObjectRecoveryProgress recovery_progress;

    enum state_t { IDLE, READING, WRITING, COMPLETE } state;

    static const char* tostr(state_t state) {
      switch (state) {
      case RecoveryOp::IDLE:
	return "IDLE";
      case RecoveryOp::READING:
	return "READING";
      case RecoveryOp::WRITING:
	return "WRITING";
      case RecoveryOp::COMPLETE:
	return "COMPLETE";
      default:
	ceph_abort();
	return "";
      }
    }

    // must be filled if state == WRITING
    std::map<int, ceph::buffer::list> returned_data;
    std::map<std::string, ceph::buffer::list, std::less<>> xattrs;
    ECUtil::HashInfoRef hinfo;
    ObjectContextRef obc;
    std::set<pg_shard_t> waiting_on_pushes;

    // valid in state READING
    std::pair<uint64_t, uint64_t> extent_requested;

    void dump(ceph::Formatter *f) const;

    RecoveryOp() : state(IDLE) {}
  };
  friend ostream &operator<<(ostream &lhs, const RecoveryOp &rhs);
  std::map<hobject_t, RecoveryOp> recovery_ops;

  uint64_t get_recovery_chunk_size() const {
    return round_up_to(cct->_conf->osd_recovery_max_chunk,
			sinfo.get_stripe_width());
  }

  virtual ~RecoveryBackend() = default;
  virtual void commit_txn_send_replies(
    ceph::os::Transaction&& txn,
    std::map<int, MOSDPGPushReply*> replies) = 0;
  void dispatch_recovery_messages(RecoveryMessages &m, int priority);

  RecoveryHandle *open_recovery_op();
  void run_recovery_op(
    struct ECRecoveryHandle &h,
    int priority);
  int recover_object(
    const hobject_t &hoid,
    eversion_t v,
    ObjectContextRef head,
    ObjectContextRef obc,
    RecoveryHandle *h);
  void continue_recovery_op(
    RecoveryBackend::RecoveryOp &op,
    RecoveryMessages *m);
  void handle_recovery_read_complete(
    const hobject_t &hoid,
    boost::tuple<uint64_t, uint64_t, std::map<pg_shard_t, ceph::buffer::list> > &to_read,
    std::optional<std::map<std::string, ceph::buffer::list, std::less<>> > attrs,
    RecoveryMessages *m);
  void handle_recovery_push(
    const PushOp &op,
    RecoveryMessages *m,
    bool is_repair);
  void handle_recovery_push_reply(
    const PushReplyOp &op,
    pg_shard_t from,
    RecoveryMessages *m);
  friend struct RecoveryMessages;
  int get_ec_data_chunk_count() const {
    return ec_impl->get_data_chunk_count();
  }
  void _failed_push(const hobject_t &hoid, ECCommon::read_result_t &res);
  };
  struct ECRecoveryBackend : RecoveryBackend {
    ECRecoveryBackend(CephContext* cct,
		      const coll_t &coll,
		      ceph::ErasureCodeInterfaceRef ec_impl,
		      const ECUtil::stripe_info_t& sinfo,
		      ReadPipeline& read_pipeline,
		      UnstableHashInfoRegistry& unstable_hashinfo_registry,
		      Listener* parent,
		      ECBackend* ecbackend)
      : RecoveryBackend(cct, coll, std::move(ec_impl), sinfo, read_pipeline, unstable_hashinfo_registry, parent->get_eclistener(), ecbackend),
	parent(parent) {
    }

    void commit_txn_send_replies(
      ceph::os::Transaction&& txn,
      std::map<int, MOSDPGPushReply*> replies) override;

    Listener *get_parent() const { return parent; }

  private:
    Listener *parent;
  };
  friend ostream &operator<<(ostream &lhs, const RecoveryBackend::RecoveryOp &rhs);
  friend struct RecoveryMessages;
  friend struct OnRecoveryReadComplete;
  friend struct RecoveryReadCompleter;

  void handle_recovery_push(
    const PushOp &op,
    RecoveryMessages *m,
    bool is_repair);

public:
  struct ReadPipeline read_pipeline;
  struct RMWPipeline rmw_pipeline;
  struct ECRecoveryBackend recovery_backend;

  ceph::ErasureCodeInterfaceRef ec_impl;


  /**
   * ECRecPred
   *
   * Determines the whether _have is sufficient to recover an object
   */
  class ECRecPred : public IsPGRecoverablePredicate {
    std::set<int> want;
    ceph::ErasureCodeInterfaceRef ec_impl;
  public:
    explicit ECRecPred(ceph::ErasureCodeInterfaceRef ec_impl) : ec_impl(ec_impl) {
      for (unsigned i = 0; i < ec_impl->get_chunk_count(); ++i) {
	want.insert(i);
      }
    }
    bool operator()(const std::set<pg_shard_t> &_have) const override {
      std::set<int> have;
      for (std::set<pg_shard_t>::const_iterator i = _have.begin();
	   i != _have.end();
	   ++i) {
	have.insert(i->shard);
      }
      std::map<int, std::vector<std::pair<int, int>>> min;
      return ec_impl->minimum_to_decode(want, have, &min) == 0;
    }
  };
  IsPGRecoverablePredicate *get_is_recoverable_predicate() const override {
    return new ECRecPred(ec_impl);
  }

  int get_ec_data_chunk_count() const override {
    return ec_impl->get_data_chunk_count();
  }
  int get_ec_stripe_chunk_size() const override {
    return sinfo.get_chunk_size();
  }

  /**
   * ECReadPred
   *
   * Determines the whether _have is sufficient to read an object
   */
  class ECReadPred : public IsPGReadablePredicate {
    pg_shard_t whoami;
    ECRecPred rec_pred;
  public:
    ECReadPred(
      pg_shard_t whoami,
      ceph::ErasureCodeInterfaceRef ec_impl) : whoami(whoami), rec_pred(ec_impl) {}
    bool operator()(const std::set<pg_shard_t> &_have) const override {
      return _have.count(whoami) && rec_pred(_have);
    }
  };
  IsPGReadablePredicate *get_is_readable_predicate() const override {
    return new ECReadPred(get_parent()->whoami_shard(), ec_impl);
  }


  const ECUtil::stripe_info_t sinfo;

  ECCommon::UnstableHashInfoRegistry unstable_hashinfo_registry;


  std::tuple<
    int,
    std::map<std::string, ceph::bufferlist, std::less<>>,
    size_t
  > get_attrs_n_size_from_disk(const hobject_t& hoid);

public:
  int object_stat(const hobject_t &hoid, struct stat* st);
  ECBackend(
    PGBackend::Listener *pg,
    const coll_t &coll,
    ObjectStore::CollectionHandle &ch,
    ObjectStore *store,
    CephContext *cct,
    ceph::ErasureCodeInterfaceRef ec_impl,
    uint64_t stripe_width);

  int objects_get_attrs(
    const hobject_t &hoid,
    std::map<std::string, ceph::buffer::list, std::less<>> *out) override;

  void rollback_append(
    const hobject_t &hoid,
    uint64_t old_size,
    ObjectStore::Transaction *t) override;

  bool auto_repair_supported() const override { return true; }

  int be_deep_scrub(
    const hobject_t &poid,
    ScrubMap &map,
    ScrubMapBuilder &pos,
    ScrubMap::object &o) override;

  uint64_t be_get_ondisk_size(uint64_t logical_size) const final {
    return sinfo.logical_to_next_chunk_offset(logical_size);
  }
};
ostream &operator<<(ostream &lhs, const ECBackend::RMWPipeline::pipeline_state_t &rhs);

#endif
