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

  bool ec_can_decode(const shard_id_set &available_shards) const;
  shard_id_map<bufferlist> ec_encode_acting_set(const bufferlist &in_bl) const;
  shard_id_map<bufferlist> ec_decode_acting_set(
      const shard_id_map<bufferlist> &shard_map, int chunk_size) const;
  ECUtil::stripe_info_t ec_get_sinfo() const;

 private:
  friend struct ECRecoveryHandle;

  void kick_reads();

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

  bool remove_ec_omap_journal_entry(const eversion_t version) {
    return ec_omap_journal.remove_entry_by_version(version);
  }

  using UpdateMapType = std::map<std::string, std::optional<ceph::buffer::list>>;
  using RangeListType = std::list<std::pair<std::optional<std::string>, std::optional<std::string>>>;

  std::tuple<UpdateMapType, RangeListType> get_journal_updates() {
    std::map<std::string, std::optional<ceph::buffer::list>> update_map;
    std::list<std::pair<std::optional<std::string>, std::optional<std::string>>> remove_ranges;
    for (auto &entry : ec_omap_journal) {
      if (entry.clear_omap) {
        // Clear all previous updates
        update_map.clear();
        // Mark entire range as removed
        remove_ranges.clear();
        remove_ranges.push_back({std::nullopt, std::nullopt});
      }

      for (auto &&update : entry.omap_updates) {
        OmapUpdateType type = update.first;
        auto iter = update.second.cbegin();
        switch (type) {
          case OmapUpdateType::Insert: {
            std::map<std::string, ceph::buffer::list> vals;
            decode(vals, iter);
            // Insert key value pairs into update_map
            for (auto it = vals.begin(); it != vals.end(); ++it) {
              const auto &key = it->first;
              const auto &val = it->second;
              update_map[key] = val;
            }
            break;
          }
          case OmapUpdateType::Remove: {
            std::set<std::string> keys;
            decode(keys, iter);
            // Mark keys in update_map as removed
            for (const auto &key : keys) {
              update_map[key] = std::nullopt;
            }
            break;
          }
          case OmapUpdateType::RemoveRange: {
            std::string key_begin, key_end;
            decode(key_begin, iter);
            decode(key_end, iter);
            
            // Add range to remove_ranges, merging overlapping ranges
            std::optional<std::string> start = key_begin;
            std::optional<std::string> end = key_end;
            auto it = remove_ranges.begin();
            bool inserted = false;
            while (it != remove_ranges.end()) {
              // Current range is to the left of new range
              if (it->second && *it->second < *start) {
                it++;
                continue;
              }
              // Current range is to the right of new range
              if (it->first && (!end || *end < *it->first)) {
                remove_ranges.insert(it, {start, end});
                inserted = true;
                break;
              }
              // Ranges overlap, merge them
              if (!it->first || (start && *it->first < *start)) {
                start = it->first;
              }
              if (!it->second) {
                end = std::nullopt;
              } else if (end && *it->second > *end) {
                end = it->second;
              }
              it = remove_ranges.erase(it);
            }
            if (!inserted) {
              remove_ranges.emplace_back(start, end);
            }

            // Erase keys in update_map that fall within the removed range
            auto map_it = update_map.lower_bound(key_begin);
            while (map_it != update_map.end()) {
              if (map_it->first >= key_end) {
                break;
              }
              map_it = update_map.erase(map_it);
            }
            break;
          }
          default:
            ceph_abort_msg("Unknown OmapUpdateType");
        }
      }
    }
    return {update_map, remove_ranges};
  }
  
  std::optional<ceph::buffer::list> get_header_from_journal() {
    std::optional<ceph::buffer::list> updated_header = std::nullopt;
    for (auto &entry : ec_omap_journal) {
      if (entry.omap_header) {
        updated_header = entry.omap_header;
      }
    }
    return updated_header;
  }

  using OmapIterFunction = std::function<ObjectStore::omap_iter_ret_t(std::string_view, std::string_view)>;

  int omap_iterate (
    ObjectStore::CollectionHandle &c_, ///< [in] collection
    const ghobject_t &oid, ///< [in] object
    ObjectStore::omap_iter_seek_t start_from, ///< [in] where the iterator should point to at the beginning
    OmapIterFunction f, ///< [in] function to call for each key/value pair
    ObjectStore *store
  ) {
    // Updates in update_map take priority over removed_ranges
    auto [update_map, removed_ranges] = get_journal_updates();

    auto journal_it = update_map.begin();
    if (!start_from.seek_position.empty()) {
      journal_it = update_map.lower_bound(start_from.seek_position);
    }

    auto wrapper = [&](std::string_view store_key, std::string_view store_value) {
      bool found_store_key_in_journal = false;
      
      while (journal_it != update_map.end() && journal_it->first <= store_key) {
        if (journal_it->first == store_key) {
          found_store_key_in_journal = true;
        }
        if (journal_it->second.has_value()) {
          ObjectStore::omap_iter_ret_t r = f(journal_it->first, std::string_view(journal_it->second->c_str(), journal_it->second->length()));
          if (r == ObjectStore::omap_iter_ret_t::STOP) {
            return r;
          }
        }
        ++journal_it;
      }

      if (found_store_key_in_journal) {
        return ObjectStore::omap_iter_ret_t::NEXT;
      }

      if (should_be_removed(removed_ranges, store_key)) {
        return ObjectStore::omap_iter_ret_t::NEXT;
      }

      return f(store_key, store_value);
    };

	  const auto result = store->omap_iterate(c_, oid, start_from, wrapper);
	  if (result < 0) {
	    return result;
	  }

    ObjectStore::omap_iter_ret_t ret = ObjectStore::omap_iter_ret_t::NEXT;
    while (journal_it != update_map.end()) {
      if (journal_it->second.has_value()) {
        ret = f(journal_it->first, std::string_view(journal_it->second->c_str(), journal_it->second->length()));
        if (ret == ObjectStore::omap_iter_ret_t::STOP) {
          break;
        }
      }
      ++journal_it;
    }

    return ret == ObjectStore::omap_iter_ret_t::STOP ? 0 : 1;
  }

  bool should_be_removed(
  const std::list<std::pair<std::optional<std::string>,
                            std::optional<std::string>>>& removed_ranges,
        std::string_view key) {
    for (const auto& range : removed_ranges) {
      const auto& start_opt = range.first;
      const auto& end_opt = range.second;

      bool start_ok = true;
      bool end_ok = true;

      if (start_opt.has_value()) {
        start_ok = key >= start_opt.value();
      }
      if (end_opt.has_value()) {
        end_ok = key < end_opt.value();
      }

      if (start_ok && end_ok) {
        return true;
      }
    }
    return false;
  }
};
