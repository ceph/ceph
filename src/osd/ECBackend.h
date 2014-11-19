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

#include "OSD.h"
#include "PGBackend.h"
#include "osd_types.h"
#include <boost/optional.hpp>
#include "erasure-code/ErasureCodeInterface.h"
#include "ECTransaction.h"
#include "ECMsgTypes.h"
#include "ECUtil.h"
#include "messages/MOSDECSubOpWrite.h"
#include "messages/MOSDECSubOpWriteReply.h"
#include "messages/MOSDECSubOpRead.h"
#include "messages/MOSDECSubOpReadReply.h"

struct RecoveryMessages;
class ECBackend : public PGBackend {
public:
  RecoveryHandle *open_recovery_op();

  void run_recovery_op(
    RecoveryHandle *h,
    int priority
    );

  void recover_object(
    const hobject_t &hoid,
    eversion_t v,
    ObjectContextRef head,
    ObjectContextRef obc,
    RecoveryHandle *h
    );

  bool handle_message(
    OpRequestRef op
    );
  bool can_handle_while_inactive(
    OpRequestRef op
    );
  friend struct SubWriteApplied;
  friend struct SubWriteCommitted;
  void sub_write_applied(
    ceph_tid_t tid, eversion_t version);
  void sub_write_committed(
    ceph_tid_t tid, eversion_t version, eversion_t last_complete);
  void handle_sub_write(
    pg_shard_t from,
    OpRequestRef msg,
    ECSubWrite &op,
    Context *on_local_applied_sync = 0
    );
  void handle_sub_read(
    pg_shard_t from,
    ECSubRead &op,
    ECSubReadReply *reply
    );
  void handle_sub_write_reply(
    pg_shard_t from,
    ECSubWriteReply &op
    );
  void handle_sub_read_reply(
    pg_shard_t from,
    ECSubReadReply &op,
    RecoveryMessages *m
    );

  /// @see ReadOp below
  void check_recovery_sources(const OSDMapRef osdmap);

  void on_change();
  void clear_recovery_state();

  void on_flushed();

  void dump_recovery_info(Formatter *f) const;

  /// @see osd/ECTransaction.cc/h
  PGTransaction *get_transaction();

  void submit_transaction(
    const hobject_t &hoid,
    const eversion_t &at_version,
    PGTransaction *t,
    const eversion_t &trim_to,
    const eversion_t &trim_rollback_to,
    const vector<pg_log_entry_t> &log_entries,
    boost::optional<pg_hit_set_history_t> &hset_history,
    Context *on_local_applied_sync,
    Context *on_all_applied,
    Context *on_all_commit,
    ceph_tid_t tid,
    osd_reqid_t reqid,
    OpRequestRef op
    );

  int objects_read_sync(
    const hobject_t &hoid,
    uint64_t off,
    uint64_t len,
    uint32_t op_flags,
    bufferlist *bl);

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
   * Another subtely is that while we may read a degraded object, we will
   * still only perform a client read from shards in the acting set.  This
   * ensures that we won't ever have to restart a client initiated read in
   * check_recovery_sources.
   */
  friend struct CallClientContexts;
  struct ClientAsyncReadStatus {
    bool complete;
    Context *on_complete;
    ClientAsyncReadStatus(Context *on_complete)
    : complete(false), on_complete(on_complete) {}
  };
  list<ClientAsyncReadStatus> in_progress_client_reads;
  void objects_read_async(
    const hobject_t &hoid,
    const list<pair<boost::tuple<uint64_t, uint64_t, uint32_t>,
		    pair<bufferlist*, Context*> > > &to_read,
    Context *on_complete);

private:
  friend struct ECRecoveryHandle;
  uint64_t get_recovery_chunk_size() const {
    return ROUND_UP_TO(cct->_conf->osd_recovery_max_chunk,
			sinfo.get_stripe_width());
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

    bool pending_read;
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
	assert(0);
	return "";
      }
    }

    // must be filled if state == WRITING
    map<shard_id_t, bufferlist> returned_data;
    map<string, bufferlist> xattrs;
    ECUtil::HashInfoRef hinfo;
    ObjectContextRef obc;
    set<pg_shard_t> waiting_on_pushes;

    // valid in state READING
    pair<uint64_t, uint64_t> extent_requested;

    void dump(Formatter *f) const;

    RecoveryOp() : pending_read(false), state(IDLE) {}
  };
  friend ostream &operator<<(ostream &lhs, const RecoveryOp &rhs);
  map<hobject_t, RecoveryOp> recovery_ops;

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
    const set<pg_shard_t> need;
    const bool want_attrs;
    GenContext<pair<RecoveryMessages *, read_result_t& > &> *cb;
    read_request_t(
      const hobject_t &hoid,
      const list<boost::tuple<uint64_t, uint64_t, uint32_t> > &to_read,
      const set<pg_shard_t> &need,
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

    map<hobject_t, read_request_t> to_read;
    map<hobject_t, read_result_t> complete;

    map<hobject_t, set<pg_shard_t> > obj_to_source;
    map<pg_shard_t, set<hobject_t> > source_to_obj;

    void dump(Formatter *f) const;

    set<pg_shard_t> in_progress;
  };
  friend struct FinishReadOp;
  void filter_read_op(
    const OSDMapRef osdmap,
    ReadOp &op);
  void complete_read_op(ReadOp &rop, RecoveryMessages *m);
  friend ostream &operator<<(ostream &lhs, const ReadOp &rhs);
  map<ceph_tid_t, ReadOp> tid_to_read_map;
  map<pg_shard_t, set<ceph_tid_t> > shard_to_read_map;
  void start_read_op(
    int priority,
    map<hobject_t, read_request_t> &to_read,
    OpRequestRef op);


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
  struct Op {
    hobject_t hoid;
    eversion_t version;
    eversion_t trim_to;
    eversion_t trim_rollback_to;
    vector<pg_log_entry_t> log_entries;
    boost::optional<pg_hit_set_history_t> updated_hit_set_history;
    Context *on_local_applied_sync;
    Context *on_all_applied;
    Context *on_all_commit;
    ceph_tid_t tid;
    osd_reqid_t reqid;
    OpRequestRef client_op;

    ECTransaction *t;

    set<hobject_t> temp_added;
    set<hobject_t> temp_cleared;

    set<pg_shard_t> pending_commit;
    set<pg_shard_t> pending_apply;

    map<hobject_t, ECUtil::HashInfoRef> unstable_hash_infos;
    ~Op() {
      delete t;
      delete on_local_applied_sync;
      delete on_all_applied;
      delete on_all_commit;
    }
  };
  friend ostream &operator<<(ostream &lhs, const Op &rhs);

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
    PushOp &op,
    RecoveryMessages *m);
  void handle_recovery_push_reply(
    PushReplyOp &op,
    pg_shard_t from,
    RecoveryMessages *m);

  map<ceph_tid_t, Op> tid_to_op_map; /// lists below point into here
  list<Op*> writing;

  CephContext *cct;
  ErasureCodeInterfaceRef ec_impl;


  /**
   * ECRecPred
   *
   * Determines the whether _have is suffient to recover an object
   */
  class ECRecPred : public IsRecoverablePredicate {
    set<int> want;
    ErasureCodeInterfaceRef ec_impl;
  public:
    ECRecPred(ErasureCodeInterfaceRef ec_impl) : ec_impl(ec_impl) {
      for (unsigned i = 0; i < ec_impl->get_chunk_count(); ++i) {
	want.insert(i);
      }
    }
    bool operator()(const set<pg_shard_t> &_have) const {
      set<int> have;
      for (set<pg_shard_t>::const_iterator i = _have.begin();
	   i != _have.end();
	   ++i) {
	have.insert(i->shard);
      }
      set<int> min;
      return ec_impl->minimum_to_decode(want, have, &min) == 0;
    }
  };
  IsRecoverablePredicate *get_is_recoverable_predicate() {
    return new ECRecPred(ec_impl);
  }

  /**
   * ECReadPred
   *
   * Determines the whether _have is suffient to read an object
   */
  class ECReadPred : public IsReadablePredicate {
    pg_shard_t whoami;
    ECRecPred rec_pred;
  public:
    ECReadPred(
      pg_shard_t whoami,
      ErasureCodeInterfaceRef ec_impl) : whoami(whoami), rec_pred(ec_impl) {}
    bool operator()(const set<pg_shard_t> &_have) const {
      return _have.count(whoami) && rec_pred(_have);
    }
  };
  IsReadablePredicate *get_is_readable_predicate() {
    return new ECReadPred(get_parent()->whoami_shard(), ec_impl);
  }


  const ECUtil::stripe_info_t sinfo;
  /// If modified, ensure that the ref is held until the update is applied
  SharedPtrRegistry<hobject_t, ECUtil::HashInfo> unstable_hashinfo_registry;
  ECUtil::HashInfoRef get_hash_info(const hobject_t &hoid);

  friend struct ReadCB;
  void check_op(Op *op);
  void start_write(Op *op);
public:
  ECBackend(
    PGBackend::Listener *pg,
    coll_t coll,
    coll_t temp_coll,
    ObjectStore *store,
    CephContext *cct,
    ErasureCodeInterfaceRef ec_impl,
    uint64_t stripe_width);

  /// Returns to_read replicas sufficient to reconstruct want
  int get_min_avail_to_read_shards(
    const hobject_t &hoid,     ///< [in] object
    const set<int> &want,      ///< [in] desired shards
    bool for_recovery,         ///< [in] true if we may use non-acting replicas
    set<pg_shard_t> *to_read   ///< [out] shards to read
    ); ///< @return error code, 0 on success

  int objects_get_attrs(
    const hobject_t &hoid,
    map<string, bufferlist> *out);

  void rollback_append(
    const hobject_t &hoid,
    uint64_t old_size,
    ObjectStore::Transaction *t);

  bool scrub_supported() { return true; }

  void be_deep_scrub(
    const hobject_t &obj,
    uint32_t seed,
    ScrubMap::object &o,
    ThreadPool::TPHandle &handle);
  uint64_t be_get_ondisk_size(uint64_t logical_size) {
    return sinfo.logical_to_next_chunk_offset(logical_size);
  }
};

#endif
