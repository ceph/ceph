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

#pragma once

#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>

#include "ECCommon.h"
#include "ECExtentCache.h"
#include "ECListener.h"
#include "ECTypes.h"
#include "ECUtil.h"
#include "OSD.h"
#include "PGBackend.h"
#include "erasure-code/ErasureCodeInterface.h"
#include "include/buffer.h"
#include "osd/scrubber/scrub_backend.h"

/* This file is soon going to be replaced (before next release), so we are going
 * to simply ignore all deprecated warnings.
 * */

//forward declaration
struct ECSubWrite;
struct ECSubWriteReply;
struct ECSubRead;
struct ECSubReadReply;
class ECSwitch;

struct RecoveryMessages;
class ECSwitch;

class ECBackend : public ECCommon {
 public:
  PGBackend::RecoveryHandle *open_recovery_op();

  void run_recovery_op(
      PGBackend::RecoveryHandle *h,
      int priority
    );

  int recover_object(
      const hobject_t &hoid,
      eversion_t v,
      ObjectContextRef head,
      ObjectContextRef obc,
      PGBackend::RecoveryHandle *h
    );

  bool _handle_message(OpRequestRef op);
  bool can_handle_while_inactive(OpRequestRef op);
  friend struct SubWriteApplied;
  friend struct SubWriteCommitted;
  void sub_write_committed(
      ceph_tid_t tid,
      eversion_t version,
      eversion_t last_complete,
      const ZTracer::Trace &trace
    );
  void handle_sub_write(
      pg_shard_t from,
      OpRequestRef msg,
      ECSubWrite &op,
      const ZTracer::Trace &trace,
      ECListener &eclistener
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
  void check_recovery_sources(const OSDMapRef &osdmap);

  void on_change();
  void clear_recovery_state();

  void dump_recovery_info(ceph::Formatter *f) const;

  void call_write_ordered(std::function<void(void)> &&cb) {
    rmw_pipeline.call_write_ordered(std::move(cb));
  }

  void submit_transaction(
      const hobject_t &hoid,
      const object_stat_sum_t &delta_stats,
      const eversion_t &at_version,
      PGTransactionUPtr &&t,
      const eversion_t &trim_to,
      const eversion_t &pg_committed_to,
      std::vector<pg_log_entry_t> &&log_entries,
      std::optional<pg_hit_set_history_t> &hset_history,
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
      ceph::buffer::list *bl
    );

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
      const std::map<hobject_t, std::list<ec_align_t>> &reads,
      bool fast_read,
      uint64_t object_size,
      GenContextURef<ECCommon::ec_extents_t&&> &&func
    ) override;

  /**
   * Async read mechanism for read-modify-write (RMW) code paths. Here wthe
   * client already knows the set of shard reads that are required, so these
   * can be passed in directly.  The "fast_read" mechanism is not needed.
   *
   * Otherwise this is the same as objects_read_and_reconstruct.
   */
  void objects_read_and_reconstruct_for_rmw(
      std::map<hobject_t, read_request_t> &&reads,
      GenContextURef<ECCommon::ec_extents_t&&> &&func
    ) override;

  void objects_read_async(
      const hobject_t &hoid,
      uint64_t object_size,
      const std::list<std::pair<ec_align_t,
                                std::pair<ceph::buffer::list*, Context*>>> &
      to_read,
      Context *on_complete,
      bool fast_read = false
    );

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
    CephContext *cct;
    const coll_t &coll;
    ceph::ErasureCodeInterfaceRef ec_impl;
    const ECUtil::stripe_info_t &sinfo;
    ReadPipeline &read_pipeline;
    UnstableHashInfoRegistry &unstable_hashinfo_registry;
    // TODO: lay an interface down here
    ECListener *parent;
    ECBackend *ecbackend;

    ECListener *get_parent() const { return parent; }

    const OSDMapRef &get_osdmap() const {
      return get_parent()->pgb_get_osdmap();
    }

    epoch_t get_osdmap_epoch() const {
      return get_parent()->pgb_get_osdmap_epoch();
    }

    const pg_info_t &get_info() { return get_parent()->get_info(); }
    void add_temp_obj(const hobject_t &oid) { get_parent()->add_temp_obj(oid); }

    void clear_temp_obj(const hobject_t &oid) {
      get_parent()->clear_temp_obj(oid);
    }

    RecoveryBackend(CephContext *cct,
                    const coll_t &coll,
                    ceph::ErasureCodeInterfaceRef ec_impl,
                    const ECUtil::stripe_info_t &sinfo,
                    ReadPipeline &read_pipeline,
                    UnstableHashInfoRegistry &unstable_hashinfo_registry,
                    ECListener *parent,
                    ECBackend *ecbackend);

    struct RecoveryOp {
      hobject_t hoid;
      eversion_t v;
      std::set<pg_shard_t> missing_on;
      shard_id_set missing_on_shards;

      ObjectRecoveryInfo recovery_info;
      ObjectRecoveryProgress recovery_progress;

      enum state_t { IDLE, READING, WRITING, COMPLETE } state;

      static const char *tostr(state_t state) {
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
      std::optional<ECUtil::shard_extent_map_t> returned_data;
      std::map<std::string, ceph::buffer::list, std::less<>> xattrs;
      ECUtil::HashInfoRef hinfo;
      ObjectContextRef obc;
      std::set<pg_shard_t> waiting_on_pushes;

      void dump(ceph::Formatter *f) const;

      RecoveryOp() : state(IDLE) {}

      void print(std::ostream &os) const {
        os << "RecoveryOp("
            << "hoid=" << hoid
            << " v=" << v
            << " missing_on=" << missing_on
            << " missing_on_shards=" << missing_on_shards
            << " recovery_info=" << recovery_info
            << " recovery_progress=" << recovery_progress
            << " obc refcount=" << obc.use_count()
            << " state=" << ECBackend::RecoveryBackend::RecoveryOp::tostr(state)
            << " waiting_on_pushes=" << waiting_on_pushes
            << ")";
      }
    };

    std::map<hobject_t, RecoveryOp> recovery_ops;

    uint64_t get_recovery_chunk_size() const {
      return round_up_to(cct->_conf->osd_recovery_max_chunk,
                         sinfo.get_stripe_width());
    }

    virtual ~RecoveryBackend() = default;
    virtual void commit_txn_send_replies(
        ceph::os::Transaction &&txn,
        std::map<int, MOSDPGPushReply*> replies) = 0;
    void dispatch_recovery_messages(RecoveryMessages &m, int priority);

    PGBackend::RecoveryHandle *open_recovery_op();
    void run_recovery_op(
        struct ECRecoveryHandle &h,
        int priority);
    int recover_object(
        const hobject_t &hoid,
        eversion_t v,
        ObjectContextRef head,
        ObjectContextRef obc,
        PGBackend::RecoveryHandle *h);
    void continue_recovery_op(
        RecoveryBackend::RecoveryOp &op,
        RecoveryMessages *m);
    void handle_recovery_read_complete(
        const hobject_t &hoid,
        ECUtil::shard_extent_map_t &&buffers_read,
        std::optional<std::map<std::string, ceph::buffer::list, std::less<>>>
          attrs,
        const ECUtil::shard_extent_set_t &want_to_read,
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
    void _failed_push(const hobject_t &hoid, ECCommon::read_result_t &res);
  };

  struct ECRecoveryBackend : RecoveryBackend {
    ECRecoveryBackend(CephContext *cct,
                      const coll_t &coll,
                      ceph::ErasureCodeInterfaceRef ec_impl,
                      const ECUtil::stripe_info_t &sinfo,
                      ReadPipeline &read_pipeline,
                      UnstableHashInfoRegistry &unstable_hashinfo_registry,
                      PGBackend::Listener *parent,
                      ECBackend *ecbackend)
      : RecoveryBackend(cct, coll, std::move(ec_impl), sinfo, read_pipeline,
                        unstable_hashinfo_registry, parent->get_eclistener(),
                        ecbackend),
        parent(parent) {}

    void commit_txn_send_replies(
        ceph::os::Transaction &&txn,
        std::map<int, MOSDPGPushReply*> replies) override;

    PGBackend::Listener *get_parent() const { return parent; }

   private:
    PGBackend::Listener *parent;
  };

  friend ostream &operator<<(ostream &lhs,
                             const RecoveryBackend::RecoveryOp &rhs
    );
  friend struct RecoveryMessages;
  friend struct OnRecoveryReadComplete;
  friend struct RecoveryReadCompleter;

  void handle_recovery_push(
      const PushOp &op,
      RecoveryMessages *m,
      bool is_repair
    );

  PGBackend::Listener *parent;
  CephContext *cct;
  ECSwitch *switcher;
  ReadPipeline read_pipeline;
  RMWPipeline rmw_pipeline;
  ECRecoveryBackend recovery_backend;

  ceph::ErasureCodeInterfaceRef ec_impl;

  PGBackend::Listener *get_parent() { return parent; }

  /**
   * ECRecPred
   *
   * Determines whether _have is sufficient to recover an object
   */
  class ECRecPred : public IsPGRecoverablePredicate {
    shard_id_set want;
    const ECUtil::stripe_info_t *sinfo;
    ceph::ErasureCodeInterfaceRef ec_impl;

   public:
    explicit ECRecPred(const ECUtil::stripe_info_t *sinfo,
                       ceph::ErasureCodeInterfaceRef ec_impl) :
      sinfo(sinfo), ec_impl(ec_impl) {
      want.insert_range(shard_id_t(0), sinfo->get_k_plus_m());
    }

    bool operator()(const std::set<pg_shard_t> &_have) const override {
      shard_id_set have;
      for (pg_shard_t p: _have) {
        have.insert(p.shard);
      }
      std::unique_ptr<shard_id_map<std::vector<std::pair<int, int>>>>
          min_sub_chunks = nullptr;
      if (sinfo->supports_sub_chunks()) {
        min_sub_chunks = std::make_unique<shard_id_map<std::vector<std::pair<
          int, int>>>>(sinfo->get_k_plus_m());
      }
      shard_id_set min;

      return ec_impl->minimum_to_decode(want, have, min, min_sub_chunks.get())
          == 0;
    }
  };

  std::unique_ptr<ECRecPred> get_is_recoverable_predicate() const {
    return std::make_unique<ECRecPred>(&sinfo, ec_impl);
  }

  unsigned get_ec_data_chunk_count() const {
    return sinfo.get_k();
  }

  int get_ec_stripe_chunk_size() const {
    return sinfo.get_chunk_size();
  }

  uint64_t object_size_to_shard_size(const uint64_t size, shard_id_t shard
    ) const {
    return sinfo.object_size_to_shard_size(size, shard);
  }

  uint64_t get_is_nonprimary_shard(shard_id_t shard) const {
    return sinfo.is_nonprimary_shard(shard);
  }

  bool get_is_hinfo_required() const {
    return sinfo.get_is_hinfo_required();
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
        const ECUtil::stripe_info_t *sinfo,
        ceph::ErasureCodeInterfaceRef ec_impl) : whoami(whoami), rec_pred(sinfo, ec_impl) {}

    bool operator()(const std::set<pg_shard_t> &_have) const override {
      return _have.count(whoami) && rec_pred(_have);
    }
  };

  std::unique_ptr<ECReadPred>
  get_is_readable_predicate(pg_shard_t whoami) const {
    return std::make_unique<ECReadPred>(whoami, &sinfo, ec_impl);
  }

  const ECUtil::stripe_info_t sinfo;

  ECCommon::UnstableHashInfoRegistry unstable_hashinfo_registry;


  std::tuple<
    int,
    std::map<std::string, ceph::bufferlist, std::less<>>,
    size_t
  > get_attrs_n_size_from_disk(const hobject_t &hoid);

  ECUtil::HashInfoRef get_hinfo_from_disk(hobject_t oid);

  std::optional<object_info_t> get_object_info_from_obc(
      ObjectContextRef &obc_map
    );

 public:
  int object_stat(const hobject_t &hoid, struct stat *st);
  ECBackend(
      PGBackend::Listener *pg,
      CephContext *cct,
      ceph::ErasureCodeInterfaceRef ec_impl,
      uint64_t stripe_width,
      ECSwitch *s,
      ECExtentCache::LRU &ec_extent_cache_lru
    );

  int objects_get_attrs(
      const hobject_t &hoid,
      std::map<std::string, ceph::buffer::list, std::less<>> *out
    );

  bool auto_repair_supported() const { return true; }

  int be_deep_scrub(
      const Scrub::ScrubCounterSet& io_counters,
      const hobject_t &poid,
      ScrubMap &map,
      ScrubMapBuilder &pos,
      ScrubMap::object &o
    );

  uint64_t be_get_ondisk_size(uint64_t logical_size, shard_id_t shard_id
    ) const {
    return object_size_to_shard_size(logical_size, shard_id);
  }
};
