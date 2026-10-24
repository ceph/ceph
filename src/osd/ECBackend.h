// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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

#include <chrono>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/set.hpp>

#include "ECCommon.h"
#include "ECExtentCache.h"
#include "ECListener.h"
#include "ECTypes.h"
#include "ECUtil.h"
#include "ECWriteJournal.h"
#include "OSD.h"
#include "PGBackend.h"
#include "erasure-code/ErasureCodeInterface.h"
#include "include/buffer.h"
#include "osd/scrubber/scrub_backend.h"
#include "Coroutines.h"

/* This file is soon going to be replaced (before next release), so we are going
 * to simply ignore all deprecated warnings.
 * */

//forward declaration
struct ECSubWrite;
struct ECSubWriteReply;
struct ECSubRead;
struct ECSubReadReply;
class ECSwitch;

class ECSwitch;
struct ECClassicalOp;

class ECBackend : public ECCommon {
 public:
  ~ECBackend();
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
  void handle_sub_read_n_reply(
    pg_shard_t from,
    ECSubRead &op,
    const ZTracer::Trace &trace
#ifdef WITH_CRIMSON
    ) override;
#else
    );
#endif
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

  void call_write_ordered(std::function<void(void)> &&cb);

  void drain_journal_for_shutdown(std::function<void(int)> on_finish);
  PGBackend::ec_journal_admission_t journal_admit(
    const hobject_t& oid, const ObjectState& obs, const SnapSet& snapset,
    snapid_t snap_seq, const std::vector<OSDOp>& ops, bool may_write);
  bool journal_enrolled() const { return journal_state != nullptr; }
  // Journal a write do_op admitted (PGBackend::ec_journal_write).
  void journal_write(OpRequestRef op, ObjectContextRef obc);
  eversion_t journal_version_now() const;
  bool journal_object_dirty(const hobject_t& oid) const;
  std::optional<eversion_t> journal_oldest_live(const hobject_t& oid,
    std::optional<uint64_t> skip_stripe) const;
  bool journal_ready(const hobject_t& oid) const;
  // A journaled write of this request not yet materialized (PGBackend).
  std::optional<PGBackend::ec_journal_request_t> journal_find_request(
    const osd_reqid_t& reqid) const;
  // Hold a resend of a journaled write whose record is in flight; it is
  // answered when the record is durable (journal_record_committed).
  void journal_hold_resend(const osd_reqid_t& reqid, OpRequestRef op);
  // The metadata overlay of an object with records, and the newest user
  // version a journaled write took (PGBackend).
  std::optional<PGBackend::ec_journal_object_meta_t> journal_object_meta(
    const hobject_t& oid) const;
  version_t journal_newest_user_version() const;
  // A direct write of these bytes of oid is being built: take the journaled
  // stripes it overlaps (PGBackend::ec_journal_take_for_write).
  PGBackend::ec_journal_take_t journal_take_for_write(
    const hobject_t& oid, const interval_set<uint64_t>& written);
  // A client read of this range of oid may be issued now
  // (PGBackend::ec_journal_read_ready); if not, it is counted as waiting.
  bool journal_read_ready(const hobject_t& oid, uint64_t offset,
                          uint64_t length);
  bool journal_flush_in_flight(const hobject_t& oid) const;
  void journal_on_activate();
  void journal_split(spg_t child, ObjectStore::Transaction& t);
  void journal_object_recovered(const hobject_t& oid);
  // Replay: records the primary's slots lack, read from another holder.
  void journal_fetch_result(const hobject_t& slot,
                            const ECUtil::shard_extent_map_t& data,
                            shard_id_t shard);
  void journal_fetch_finished();
  // Replay: another holder's slot listing (its OMAP headers, read as the
  // slot's attributes), and the end of one holder's listings.
  void journal_headers_result(
    shard_id_t shard, const hobject_t& slot,
    const std::map<std::string, ceph::bufferlist, std::less<>>& headers);
  void journal_headers_finished();

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
    uint64_t object_size,
    const std::list<std::pair<ec_align_t,
    std::pair<ceph::buffer::list*, Context*>>> &to_read,
    CoroHandles coro
  );

  int objects_read_local(
    const hobject_t &hoid,
    uint64_t off,
    uint64_t len,
    uint32_t op_flags,
    ceph::buffer::list *bl
  );

  std::pair<uint64_t, uint64_t> extent_to_shard_extent(uint64_t off, uint64_t len);

  int objects_readv_sync(const hobject_t &hoid,
     std::map<uint64_t, uint64_t>& m,
     uint32_t op_flags,
     ceph::buffer::list *bl);

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

  bool ec_can_decode(const shard_id_set &available_shards) const;
  shard_id_map<bufferlist> ec_encode_acting_set(const bufferlist &in_bl) const;
  shard_id_map<bufferlist> ec_decode_acting_set(
      const shard_id_map<bufferlist> &shard_map, int chunk_size) const;
  ECUtil::stripe_info_t ec_get_sinfo() const;

 private:
  friend struct ECRecoveryHandle;

  void kick_reads();

  int _objects_read_sync(
    const hobject_t &hoid,
    uint64_t off,
    uint64_t len,
    uint32_t op_flags,
    ceph::buffer::list *bl
  );
  // objects_read_async with the journal's blocks over the ranges read
  // (none without a journal); journal_only: they are the whole answer;
  // counted: the journal counts the read on its stripes (journal_read_done).
  void issue_read_async(
      const hobject_t &hoid,
      uint64_t object_size,
      const std::list<std::pair<ec_align_t,
                                std::pair<ceph::buffer::list*, Context*>>> &
      to_read,
      Context *on_complete,
      bool fast_read,
      ECWriteJournal::Overlay overlay,
      bool journal_only,
      bool counted);
  // Requeue the reads and direct writes parked for oid if nothing holds
  // them back any more.
  void journal_kick_reads(const hobject_t& oid);
  // The shard reads of a client read of oid that the journal counted on
  // their stripes (Journal::read_begin) are done.
  void journal_read_done(
      const hobject_t& oid,
      const std::list<std::pair<ec_align_t,
                                std::pair<ceph::buffer::list*, Context*>>> &
      read);
  struct JournalState;
  std::unique_ptr<JournalState> journal_state;
  // Defers the RMW pipeline's roll-forward kick by
  // osd_ec_rollforward_delay_ms (RMWPipeline::defer_rollforward).
  struct RollforwardKick;
  std::unique_ptr<RollforwardKick> rollforward_kick;
  void init_journal();
  void pump_journal();
  void remove_journal_slots();
  void schedule_journal(std::chrono::milliseconds delay = std::chrono::milliseconds(0));
  void reset_journal();
  void dump_journal(ceph::Formatter* f) const;
  bool journal_admission_open() const;
  // A copy of a journaled write's record committed on a holder.
  void journal_record_committed(pg_shard_t from, ceph_tid_t tid);
  // Replay: every other holder's listing is in.
  void journal_replay_listed();
  void judge_replayed(ECWriteJournal::Record record);
  void journal_fetch_next();
  bool journal_blocked(const hobject_t& oid) const;

public:
  struct ECRecoveryBackend : RecoveryBackend {
    ECRecoveryBackend(CephContext *cct,
                      const coll_t &coll,
                      ceph::ErasureCodeInterfaceRef ec_impl,
                      const ECUtil::stripe_info_t &sinfo,
                      ReadPipeline &read_pipeline,
                      PGBackend::Listener *parent,
                      ECBackend *)
      : RecoveryBackend(cct, coll, std::move(ec_impl), sinfo, read_pipeline,
                        parent->get_eclistener()),
        parent(parent) {}

    struct ECRecoveryHandle;

    ECRecoveryHandle *open_recovery_op();

    void run_recovery_op(
      ECRecoveryHandle &h,
      int priority);

    void commit_txn_send_replies(
        ceph::os::Transaction &&txn,
        std::map<int, MOSDPGPushReply*> replies) override;

    void maybe_load_obc(
      const std::map<std::string, ceph::bufferlist, std::less<>>& raw_attrs,
      RecoveryOp &op) final;

    PGBackend::Listener *get_parent() const { return parent; }

   private:
    PGBackend::Listener *parent;
  };

  friend std::ostream &operator<<(std::ostream &lhs,
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
      shard_id_set min;

      return ec_impl->minimum_to_decode(want, have, min) == 0;
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

  bool get_ec_supports_crc_encode_decode() const {
    return sinfo.supports_encode_decode_crcs();
  }

  uint64_t object_size_to_shard_size(const uint64_t size, shard_id_t shard
    ) const {
    return sinfo.object_size_to_shard_size(size, shard);
  }

  uint64_t get_is_nonprimary_shard(shard_id_t shard) const {
    return sinfo.is_nonprimary_shard(shard);
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

  std::tuple<
    int,
    std::map<std::string, ceph::bufferlist, std::less<>>,
    size_t
  > get_attrs_n_size_from_disk(const hobject_t &hoid);

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

  uint64_t be_get_ondisk_size(uint64_t logical_size, shard_id_t shard_id,
      bool object_is_legacy_ec) const {
    if (object_is_legacy_ec) {
      // In legacy EC, all shards were padded to the next chunk boundry.
      return sinfo.ro_offset_to_next_chunk_offset(logical_size);
    }
    return object_size_to_shard_size(logical_size, shard_id);
  }

  bool remove_ec_omap_journal_entry(const hobject_t &hoid, const ECOmapJournalEntry &entry);
  std::pair<gen_t, bool> omap_get_generation(const hobject_t &hoid);
  void omap_trim_delete_from_journal(const hobject_t &hoid, const version_t version);

  using OmapIterFunction = std::function<ObjectStore::omap_iter_ret_t(std::string_view, std::string_view)>;
  int omap_iterate (
    ObjectStore::CollectionHandle &c_, ///< [in] collection
    const ghobject_t &oid, ///< [in] object
    const ObjectStore::omap_iter_seek_t &start_from, ///< [in] where the iterator should point to at the beginning
    const OmapIterFunction &f, ///< [in] function to call for each key/value pair
    ObjectStore *store
  );

  int omap_get_values(
    ObjectStore::CollectionHandle &c_, ///< [in] collection
    const ghobject_t &oid,              ///< [in] object
    const std::set<std::string> &keys,  ///< [in] keys to get
    std::map<std::string, ceph::buffer::list> *out, ///< [out] returned key/values
    ObjectStore *store
  );

  int omap_get_header(
    ObjectStore::CollectionHandle &c_,    ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    ceph::buffer::list *header,      ///< [out] omap header
    bool allow_eio, ///< [in] don't assert on eio
    ObjectStore *store
  );

  int omap_get(
    ObjectStore::CollectionHandle &c_,    ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    ceph::buffer::list *header,      ///< [out] omap header
    std::map<std::string, ceph::buffer::list> *out, /// < [out] Key to value map
    ObjectStore *store
  );

  int omap_check_keys(
    ObjectStore::CollectionHandle &c_,    ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const std::set<std::string> &keys, ///< [in] Keys to check
    std::set<std::string> *out,         ///< [out] Subset of keys defined on oid
    ObjectStore *store
  );

  static bool should_be_removed(
    const std::map<std::string, std::optional<std::string>>& removed_ranges,
    const std::string_view key
  );

  static void remove_keys_in_ranges(
    const std::map<std::string, std::optional<std::string>>& removed_ranges,
    std::map<std::string, ceph::buffer::list>* out
  );
};
