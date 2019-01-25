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
class ECBackend : public PGBackend {
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
    const ZTracer::Trace &trace
    );
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
    RecoveryMessages *m,
    const ZTracer::Trace &trace
    );

  /// @see ReadOp below
  void check_recovery_sources(const OSDMapRef& osdmap) override;

  void on_change() override;
  void clear_recovery_state() override;

  void dump_recovery_info(Formatter *f) const override;

  void call_write_ordered(std::function<void(void)> &&cb) override;

  void submit_transaction(
    const hobject_t &hoid,
    const object_stat_sum_t &delta_stats,
    const eversion_t &at_version,
    PGTransactionUPtr &&t,
    const eversion_t &trim_to,
    const eversion_t &roll_forward_to,
    const vector<pg_log_entry_t> &log_entries,
    boost::optional<pg_hit_set_history_t> &hset_history,
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
    bufferlist *bl) override;

  /**
   * Async read mechanism
   *
   * Async reads use the same async read mechanism as does recovery.
   * CallClientContexts is responsible for reconstructing the response
   * buffer as well as for calling the callbacks.
   *
   * One tricky bit is that two reads may possibly not read from the same
   * set of replicas.  This could result in two reads completing in the
   * wrong (from the interface user's point of view) order.  Thus, we
   * maintain a queue of in progress reads (@see in_progress_client_reads)
   * to ensure that we always call the completion callback in order.
   *
   * Another subtly is that while we may read a degraded object, we will
   * still only perform a client read from shards in the acting set.  This
   * ensures that we won't ever have to restart a client initiated read in
   * check_recovery_sources.
   */
  void objects_read_and_reconstruct(
    const map<hobject_t, std::list<boost::tuple<uint64_t, uint64_t, uint32_t> >
    > &reads,
    bool fast_read,
    GenContextURef<map<hobject_t,pair<int, extent_map> > &&> &&func);

  friend struct CallClientContexts;
  struct ClientAsyncReadStatus {
    unsigned objects_to_read;
    GenContextURef<map<hobject_t,pair<int, extent_map> > &&> func;
    map<hobject_t,pair<int, extent_map> > results;
    explicit ClientAsyncReadStatus(
      unsigned objects_to_read,
      GenContextURef<map<hobject_t,pair<int, extent_map> > &&> &&func)
      : objects_to_read(objects_to_read), func(std::move(func)) {}
    void complete_object(
      const hobject_t &hoid,
      int err,
      extent_map &&buffers) {
      ceph_assert(objects_to_read);
      --objects_to_read;
      ceph_assert(!results.count(hoid));
      results.emplace(hoid, make_pair(err, std::move(buffers)));
    }
    bool is_complete() const {
      return objects_to_read == 0;
    }
    void run() {
      func.release()->complete(std::move(results));
    }
  };
  list<ClientAsyncReadStatus> in_progress_client_reads;
  void objects_read_async(
    const hobject_t &hoid,
    const list<pair<boost::tuple<uint64_t, uint64_t, uint32_t>,
		    pair<bufferlist*, Context*> > > &to_read,
    Context *on_complete,
    bool fast_read = false) override;

  template <typename Func>
  void objects_read_async_no_cache(
    const map<hobject_t,extent_set> &to_read,
    Func &&on_complete) {
    map<hobject_t,std::list<boost::tuple<uint64_t, uint64_t, uint32_t> > > _to_read;
    for (auto &&hpair: to_read) {
      auto &l = _to_read[hpair.first];
      for (auto extent: hpair.second) {
	l.emplace_back(extent.first, extent.second, 0);
      }
    }
    objects_read_and_reconstruct(
      _to_read,
      false,
      make_gen_lambda_context<
      map<hobject_t,pair<int, extent_map> > &&, Func>(
	  std::forward<Func>(on_complete)));
  }
  void kick_reads() {
    while (in_progress_client_reads.size() &&
	   in_progress_client_reads.front().is_complete()) {
      in_progress_client_reads.front().run();
      in_progress_client_reads.pop_front();
    }
  }

private:
  friend struct ECRecoveryHandle;
  uint64_t get_recovery_chunk_size() const {
    return round_up_to(cct->_conf->osd_recovery_max_chunk,
			sinfo.get_stripe_width());
  }

  void get_want_to_read_shards(set<int> *want_to_read) const {
    const vector<int> &chunk_mapping = ec_impl->get_chunk_mapping();
    for (int i = 0; i < (int)ec_impl->get_data_chunk_count(); ++i) {
      int chunk = (int)chunk_mapping.size() > i ? chunk_mapping[i] : i;
      want_to_read->insert(chunk);
    }
  }

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
  struct RecoveryOp {
    hobject_t hoid;
    eversion_t v;
    set<pg_shard_t> missing_on;
    set<shard_id_t> missing_on_shards;

    ObjectRecoveryInfo recovery_info;
    ObjectRecoveryProgress recovery_progress;

    enum state_t { IDLE, READING, WRITING, COMPLETE } state;

    static const char* tostr(state_t state) {
      switch (state) {
      case ECBackend::RecoveryOp::IDLE:
	return "IDLE";
	break;
      case ECBackend::RecoveryOp::READING:
	return "READING";
	break;
      case ECBackend::RecoveryOp::WRITING:
	return "WRITING";
	break;
      case ECBackend::RecoveryOp::COMPLETE:
	return "COMPLETE";
	break;
      default:
	ceph_abort();
	return "";
      }
    }

    // must be filled if state == WRITING
    map<int, bufferlist> returned_data;
    map<string, bufferlist> xattrs;
    ECUtil::HashInfoRef hinfo;
    ObjectContextRef obc;
    set<pg_shard_t> waiting_on_pushes;

    // valid in state READING
    pair<uint64_t, uint64_t> extent_requested;

    void dump(Formatter *f) const;

    RecoveryOp() : state(IDLE) {}
  };
  friend ostream &operator<<(ostream &lhs, const RecoveryOp &rhs);
  map<hobject_t, RecoveryOp> recovery_ops;

  void continue_recovery_op(
    RecoveryOp &op,
    RecoveryMessages *m);
  void dispatch_recovery_messages(RecoveryMessages &m, int priority);
  friend struct OnRecoveryReadComplete;
  void handle_recovery_read_complete(
    const hobject_t &hoid,
    boost::tuple<uint64_t, uint64_t, map<pg_shard_t, bufferlist> > &to_read,
    boost::optional<map<string, bufferlist> > attrs,
    RecoveryMessages *m);
  void handle_recovery_push(
    const PushOp &op,
    RecoveryMessages *m);
  void handle_recovery_push_reply(
    const PushReplyOp &op,
    pg_shard_t from,
    RecoveryMessages *m);
  void get_all_avail_shards(
    const hobject_t &hoid,
    const set<pg_shard_t> &error_shards,
    set<int> &have,
    map<shard_id_t, pg_shard_t> &shards,
    bool for_recovery);

public:
  /**
   * Low level async read mechanism
   *
   * To avoid duplicating the logic for requesting and waiting for
   * multiple object shards, there is a common async read mechanism
   * taking a map of hobject_t->read_request_t which defines callbacks
   * taking read_result_ts as arguments.
   *
   * tid_to_read_map gives open read ops.  check_recovery_sources uses
   * shard_to_read_map and ReadOp::source_to_obj to restart reads
   * involving down osds.
   *
   * The user is responsible for specifying replicas on which to read
   * and for reassembling the buffer on the other side since client
   * reads require the original object buffer while recovery only needs
   * the missing pieces.
   *
   * Rather than handling reads on the primary directly, we simply send
   * ourselves a message.  This avoids a dedicated primary path for that
   * part.
   */
  struct read_result_t {
    int r;
    map<pg_shard_t, int> errors;
    boost::optional<map<string, bufferlist> > attrs;
    list<
      boost::tuple<
	uint64_t, uint64_t, map<pg_shard_t, bufferlist> > > returned;
    read_result_t() : r(0) {}
  };
  struct read_request_t {
    const list<boost::tuple<uint64_t, uint64_t, uint32_t> > to_read;
    const map<pg_shard_t, vector<pair<int, int>>> need;
    const bool want_attrs;
    GenContext<pair<RecoveryMessages *, read_result_t& > &> *cb;
    read_request_t(
      const list<boost::tuple<uint64_t, uint64_t, uint32_t> > &to_read,
      const map<pg_shard_t, vector<pair<int, int>>> &need,
      bool want_attrs,
      GenContext<pair<RecoveryMessages *, read_result_t& > &> *cb)
      : to_read(to_read), need(need), want_attrs(want_attrs),
	cb(cb) {}
  };
  friend ostream &operator<<(ostream &lhs, const read_request_t &rhs);

  struct ReadOp {
    int priority;
    ceph_tid_t tid;
    OpRequestRef op; // may be null if not on behalf of a client
    // True if redundant reads are issued, false otherwise,
    // this is useful to tradeoff some resources (redundant ops) for
    // low latency read, especially on relatively idle cluster
    bool do_redundant_reads;
    // True if reading for recovery which could possibly reading only a subset
    // of the available shards.
    bool for_recovery;

    ZTracer::Trace trace;

    map<hobject_t, set<int>> want_to_read;
    map<hobject_t, read_request_t> to_read;
    map<hobject_t, read_result_t> complete;

    map<hobject_t, set<pg_shard_t>> obj_to_source;
    map<pg_shard_t, set<hobject_t> > source_to_obj;

    void dump(Formatter *f) const;

    set<pg_shard_t> in_progress;

    ReadOp(
      int priority,
      ceph_tid_t tid,
      bool do_redundant_reads,
      bool for_recovery,
      OpRequestRef op,
      map<hobject_t, set<int>> &&_want_to_read,
      map<hobject_t, read_request_t> &&_to_read)
      : priority(priority), tid(tid), op(op), do_redundant_reads(do_redundant_reads),
	for_recovery(for_recovery), want_to_read(std::move(_want_to_read)),
	to_read(std::move(_to_read)) {
      for (auto &&hpair: to_read) {
	auto &returned = complete[hpair.first].returned;
	for (auto &&extent: hpair.second.to_read) {
	  returned.push_back(
	    boost::make_tuple(
	      extent.get<0>(),
	      extent.get<1>(),
	      map<pg_shard_t, bufferlist>()));
	}
      }
    }
    ReadOp() = delete;
    ReadOp(const ReadOp &) = default;
    ReadOp(ReadOp &&) = default;
  };
  friend struct FinishReadOp;
  void filter_read_op(
    const OSDMapRef& osdmap,
    ReadOp &op);
  void complete_read_op(ReadOp &rop, RecoveryMessages *m);
  friend ostream &operator<<(ostream &lhs, const ReadOp &rhs);
  map<ceph_tid_t, ReadOp> tid_to_read_map;
  map<pg_shard_t, set<ceph_tid_t> > shard_to_read_map;
  void start_read_op(
    int priority,
    map<hobject_t, set<int>> &want_to_read,
    map<hobject_t, read_request_t> &to_read,
    OpRequestRef op,
    bool do_redundant_reads, bool for_recovery);

  void do_read_op(ReadOp &rop);
  int send_all_remaining_reads(
    const hobject_t &hoid,
    ReadOp &rop);


  /**
   * Client writes
   *
   * ECTransaction is responsible for generating a transaction for
   * each shard to which we need to send the write.  As required
   * by the PGBackend interface, the ECBackend write mechanism
   * passes trim information with the write and last_complete back
   * with the reply.
   *
   * As with client reads, there is a possibility of out-of-order
   * completions. Thus, callbacks and completion are called in order
   * on the writing list.
   */
  struct Op : boost::intrusive::list_base_hook<> {
    /// From submit_transaction caller, describes operation
    hobject_t hoid;
    object_stat_sum_t delta_stats;
    eversion_t version;
    eversion_t trim_to;
    boost::optional<pg_hit_set_history_t> updated_hit_set_history;
    vector<pg_log_entry_t> log_entries;
    ceph_tid_t tid;
    osd_reqid_t reqid;
    ZTracer::Trace trace;

    eversion_t roll_forward_to; /// Soon to be generated internally

    /// Ancillary also provided from submit_transaction caller
    map<hobject_t, ObjectContextRef> obc_map;

    /// see call_write_ordered
    std::list<std::function<void(void)> > on_write;

    /// Generated internally
    set<hobject_t> temp_added;
    set<hobject_t> temp_cleared;

    ECTransaction::WritePlan plan;
    bool requires_rmw() const { return !plan.to_read.empty(); }
    bool invalidates_cache() const { return plan.invalidates_cache; }

    // must be true if requires_rmw(), must be false if invalidates_cache()
    bool using_cache = true;

    /// In progress read state;
    map<hobject_t,extent_set> pending_read; // subset already being read
    map<hobject_t,extent_set> remote_read;  // subset we must read
    map<hobject_t,extent_map> remote_read_result;
    bool read_in_progress() const {
      return !remote_read.empty() && remote_read_result.empty();
    }

    /// In progress write state.
    set<pg_shard_t> pending_commit;
    // we need pending_apply for pre-mimic peers so that we don't issue a
    // read on a remote shard before it has applied a previous write.  We can
    // remove this after nautilus.
    set<pg_shard_t> pending_apply;
    bool write_in_progress() const {
      return !pending_commit.empty() || !pending_apply.empty();
    }

    /// optional, may be null, for tracking purposes
    OpRequestRef client_op;

    /// pin for cache
    ExtentCache::write_pin pin;

    /// Callbacks
    Context *on_all_commit = nullptr;
    ~Op() {
      delete on_all_commit;
    }
  };
  using op_list = boost::intrusive::list<Op>;
  friend ostream &operator<<(ostream &lhs, const Op &rhs);

  ExtentCache cache;
  map<ceph_tid_t, Op> tid_to_op_map; /// Owns Op structure

  /**
   * We model the possible rmw states as a set of waitlists.
   * All writes at this time complete in order, so a write blocked
   * at waiting_state blocks all writes behind it as well (same for
   * other states).
   *
   * Future work: We can break this up into a per-object pipeline
   * (almost).  First, provide an ordering token to submit_transaction
   * and require that all operations within a single transaction take
   * place on a subset of hobject_t space partitioned by that token
   * (the hashid seem about right to me -- even works for temp objects
   * if you recall that a temp object created for object head foo will
   * only ever be referenced by other transactions on foo and aren't
   * reused).  Next, factor this part into a class and maintain one per
   * ordering token.  Next, fixup PrimaryLogPG's repop queue to be
   * partitioned by ordering token.  Finally, refactor the op pipeline
   * so that the log entries passed into submit_transaction aren't
   * versioned.  We can't assign versions to them until we actually
   * submit the operation.  That's probably going to be the hard part.
   */
  class pipeline_state_t {
    enum {
      CACHE_VALID = 0,
      CACHE_INVALID = 1
    } pipeline_state = CACHE_VALID;
  public:
    bool caching_enabled() const {
      return pipeline_state == CACHE_VALID;
    }
    bool cache_invalid() const {
      return !caching_enabled();
    }
    void invalidate() {
      pipeline_state = CACHE_INVALID;
    }
    void clear() {
      pipeline_state = CACHE_VALID;
    }
    friend ostream &operator<<(ostream &lhs, const pipeline_state_t &rhs);
  } pipeline_state;


  op_list waiting_state;        /// writes waiting on pipe_state
  op_list waiting_reads;        /// writes waiting on partial stripe reads
  op_list waiting_commit;       /// writes waiting on initial commit
  eversion_t completed_to;
  eversion_t committed_to;
  void start_rmw(Op *op, PGTransactionUPtr &&t);
  bool try_state_to_reads();
  bool try_reads_to_commit();
  bool try_finish_rmw();
  void check_ops();

  ErasureCodeInterfaceRef ec_impl;


  /**
   * ECRecPred
   *
   * Determines the whether _have is sufficient to recover an object
   */
  class ECRecPred : public IsPGRecoverablePredicate {
    set<int> want;
    ErasureCodeInterfaceRef ec_impl;
  public:
    explicit ECRecPred(ErasureCodeInterfaceRef ec_impl) : ec_impl(ec_impl) {
      for (unsigned i = 0; i < ec_impl->get_chunk_count(); ++i) {
	want.insert(i);
      }
    }
    bool operator()(const set<pg_shard_t> &_have) const override {
      set<int> have;
      for (set<pg_shard_t>::const_iterator i = _have.begin();
	   i != _have.end();
	   ++i) {
	have.insert(i->shard);
      }
      map<int, vector<pair<int, int>>> min;
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
      ErasureCodeInterfaceRef ec_impl) : whoami(whoami), rec_pred(ec_impl) {}
    bool operator()(const set<pg_shard_t> &_have) const override {
      return _have.count(whoami) && rec_pred(_have);
    }
  };
  IsPGReadablePredicate *get_is_readable_predicate() const override {
    return new ECReadPred(get_parent()->whoami_shard(), ec_impl);
  }


  const ECUtil::stripe_info_t sinfo;
  /// If modified, ensure that the ref is held until the update is applied
  SharedPtrRegistry<hobject_t, ECUtil::HashInfo> unstable_hashinfo_registry;
  ECUtil::HashInfoRef get_hash_info(const hobject_t &hoid, bool checks = true,
				    const map<string,bufferptr> *attr = NULL);

public:
  ECBackend(
    PGBackend::Listener *pg,
    const coll_t &coll,
    ObjectStore::CollectionHandle &ch,
    ObjectStore *store,
    CephContext *cct,
    ErasureCodeInterfaceRef ec_impl,
    uint64_t stripe_width);

  /// Returns to_read replicas sufficient to reconstruct want
  int get_min_avail_to_read_shards(
    const hobject_t &hoid,     ///< [in] object
    const set<int> &want,      ///< [in] desired shards
    bool for_recovery,         ///< [in] true if we may use non-acting replicas
    bool do_redundant_reads,   ///< [in] true if we want to issue redundant reads to reduce latency
    map<pg_shard_t, vector<pair<int, int>>> *to_read   ///< [out] shards, corresponding subchunks to read
    ); ///< @return error code, 0 on success

  int get_remaining_shards(
    const hobject_t &hoid,
    const set<int> &avail,
    const set<int> &want,
    const read_result_t &result,
    map<pg_shard_t, vector<pair<int, int>>> *to_read,
    bool for_recovery);

  int objects_get_attrs(
    const hobject_t &hoid,
    map<string, bufferlist> *out) override;

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
  uint64_t be_get_ondisk_size(uint64_t logical_size) override {
    return sinfo.logical_to_next_chunk_offset(logical_size);
  }
  void _failed_push(const hobject_t &hoid,
    pair<RecoveryMessages *, ECBackend::read_result_t &> &in);
};
ostream &operator<<(ostream &lhs, const ECBackend::pipeline_state_t &rhs);

#endif
