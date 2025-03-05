// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2025 IBM
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

/* This module is intended as a temporary switcher between the "legacy" EC
 * implementation and the "optimized" version. Once we trust the optimized
 * version sufficiently, we can remove both that version and this switcher and
 * make the optimised ec backend interact directly with the optimized backend.
 */

#pragma once

#include "PGBackend.h"
#include "ECBackendL.h"
#include "ECBackend.h"

class ECSwitch : public PGBackend
{
  friend class ECRecPred;
  friend class ECReadPred;

  ECLegacy::ECBackendL legacy;
  ECLegacy::ECBackendL optimized;
  bool is_optimized_actual;

public:
  ECSwitch(
    PGBackend::Listener *pg,
    const coll_t &coll,
    ObjectStore::CollectionHandle &ch,
    ObjectStore *store,
    CephContext *cct,
    ceph::ErasureCodeInterfaceRef ec_impl,
    uint64_t stripe_width) :
    PGBackend(cct, pg, store, coll, ch),
    legacy(pg, cct, ec_impl, stripe_width, this),
    optimized(pg, cct, ec_impl, stripe_width, this),
    is_optimized_actual(false) {}

  bool is_optimized() const
  {
    // FIXME: Interface not yet implemented.
    //ceph_assert(is_optimized_actual == get_parent()->get_pool().allows_ecoptimizations());
    return is_optimized_actual;
  }

  /* When switching, we coordinate such that all actions up to on change
   * use the old EC and after on change need the new.  There are a small
   * number of functions here where we need to tolerate the cache being
   * inaccurate. */
  bool is_optimized_unchecked() const
  {
    return is_optimized_actual;
  }

  class ECRecPred : public IsPGRecoverablePredicate
  {
  public:
    bool operator()(const std::set<pg_shard_t> &have) const override
    {
      if (switcher->is_optimized_unchecked()) {
        return (*optimized)(have);
      }
      return (*legacy)(have);
    }

    ECRecPred(const ECSwitch *s) :
      IsPGRecoverablePredicate(),
      switcher(s)
    {
      legacy = s->legacy.get_is_recoverable_predicate();
      optimized = s->optimized.get_is_recoverable_predicate();
    }

  private:
    const ECSwitch *switcher;
    std::unique_ptr<ECLegacy::ECBackendL::ECRecPred> legacy;
    std::unique_ptr<ECLegacy::ECBackendL::ECRecPred> optimized;
  };

  class ECReadPred : public IsPGReadablePredicate
  {
  public:
    bool operator()(const std::set<pg_shard_t> &have) const override
    {
      if (switcher->is_optimized()) {
        return (*optimized)(have);
      }
      return (*legacy)(have);
    }

    ECReadPred(const ECSwitch *s) :
      IsPGReadablePredicate(),
      switcher(s)
    {
      legacy = s->legacy.get_is_readable_predicate(
        s->get_parent()->whoami_shard());
      optimized = s->optimized.get_is_readable_predicate(
        s->get_parent()->whoami_shard());
    }

  private:
    const ECSwitch *switcher;
    std::unique_ptr<ECLegacy::ECBackendL::ECReadPred> legacy;
    std::unique_ptr<ECLegacy::ECBackendL::ECReadPred> optimized;
  };

  RecoveryHandle *open_recovery_op() override
  {
    if (is_optimized()) {
      return optimized.open_recovery_op();
    }
    return legacy.open_recovery_op();
  }

  void run_recovery_op(RecoveryHandle *h, int priority) override
  {
    if (is_optimized()) {
      return optimized.run_recovery_op(h, priority);
    }
    return legacy.run_recovery_op(h, priority);
  }

  int recover_object(const hobject_t &hoid, eversion_t v, ObjectContextRef head,
                     ObjectContextRef obc, RecoveryHandle *h) override
  {
    if (is_optimized()) {
      return optimized.recover_object(hoid, v, head, obc, h);
    }
    return legacy.recover_object(hoid, v, head, obc, h);
  }

  bool can_handle_while_inactive(OpRequestRef op) override
  {
    if (is_optimized()) {
      return optimized.can_handle_while_inactive(op);
    }
    return legacy.can_handle_while_inactive(op);
  }

  bool _handle_message(OpRequestRef op) override
  {
    bool r;
    if (is_optimized_unchecked()) {
      r = optimized._handle_message(op);
    }
    else {
      r = legacy._handle_message(op);
    }

    if (r) {
      // Call to use the assert in here.
      is_optimized();
    }

    return r;
  }

  void check_recovery_sources(const OSDMapRef &osdmap) override
  {
    if (is_optimized_unchecked()) {
      return optimized.check_recovery_sources(osdmap);
    }
    return legacy.check_recovery_sources(osdmap);
  }

  void on_change() override
  {
    if (is_optimized_unchecked()) {
      optimized.on_change();
    }
    else {
      legacy.on_change();
    }
    //FIXME: Switch to new EC here.
  }

  void clear_recovery_state() override
  {
    if (is_optimized_unchecked()) {
      optimized.clear_recovery_state();
    }
    else {
      legacy.clear_recovery_state();
    }
  }

  IsPGRecoverablePredicate *get_is_recoverable_predicate() const override
  {
    return new ECRecPred(this);
  }

  IsPGReadablePredicate *get_is_readable_predicate() const override
  {
    return new ECReadPred(this);
  }

  void dump_recovery_info(ceph::Formatter *f) const override
  {
    if (is_optimized()) {
      optimized.dump_recovery_info(f);
    }
    else {
      legacy.dump_recovery_info(f);
    }
  }

  void submit_transaction(const hobject_t &hoid,
                          const object_stat_sum_t &delta_stats,
                          const eversion_t &at_version,
                          PGTransactionUPtr &&t, const eversion_t &trim_to,
                          const eversion_t &pg_committed_to,
                          std::vector<pg_log_entry_t> &&log_entries,
                          std::optional<pg_hit_set_history_t> &hset_history,
                          Context *on_all_commit,
                          ceph_tid_t tid, osd_reqid_t reqid,
                          OpRequestRef op) override
  {
    if (is_optimized()) {
      optimized.submit_transaction(hoid, delta_stats,
                                   at_version, std::move(t), trim_to,
                                   pg_committed_to,
                                   std::move(log_entries), hset_history,
                                   on_all_commit, tid, reqid, op);
    }
    else {
      legacy.submit_transaction(hoid, delta_stats,
                                at_version, std::move(t), trim_to,
                                pg_committed_to,
                                std::move(log_entries), hset_history,
                                on_all_commit, tid, reqid, op);
    }
  }

  void call_write_ordered(std::function<void()> &&cb) override
  {
    if (is_optimized()) {
      optimized.call_write_ordered(std::move(cb));
    }
    else {
      legacy.call_write_ordered(std::move(cb));
    }
  }

  int objects_read_sync(const hobject_t &hoid, uint64_t off, uint64_t len,
                        uint32_t op_flags, ceph::buffer::list *bl) override
  {
    if (is_optimized()) {
      return optimized.objects_read_sync(hoid, off, len, op_flags, bl);
    }
    return legacy.objects_read_sync(hoid, off, len, op_flags, bl);
  }

  void objects_read_async(
    const hobject_t &hoid,
    uint64_t object_size,
    const std::list<std::pair<ec_align_t,
                              std::pair<ceph::buffer::list*, Context*>>> &
    to_read,
    Context *on_complete, bool fast_read = false) override
  {
    if (is_optimized()) {
      optimized.objects_read_async(hoid, object_size, to_read, on_complete,
                                   fast_read);
    }
    else {
      legacy.objects_read_async(hoid, object_size, to_read, on_complete,
                                fast_read);
    }
  }

  bool auto_repair_supported() const override
  {
    if (is_optimized()) {
      return optimized.auto_repair_supported();
    }
    return legacy.auto_repair_supported();
  }

  uint64_t be_get_ondisk_size(uint64_t logical_size) const final
  {
    if (is_optimized()) {
      return optimized.be_get_ondisk_size(logical_size);
    }
    return legacy.be_get_ondisk_size(logical_size);
  }

  int be_deep_scrub(const hobject_t &oid, ScrubMap &map, ScrubMapBuilder &pos
                    , ScrubMap::object &o)
  {
    if (is_optimized()) {
      return optimized.be_deep_scrub(oid, map, pos, o);
    }
    return legacy.be_deep_scrub(oid, map, pos, o);
  }

  unsigned get_ec_data_chunk_count() const override
  {
    if (is_optimized()) {
      return optimized.get_ec_data_chunk_count();
    }
    return legacy.get_ec_data_chunk_count();
  }

  int get_ec_stripe_chunk_size() const override
  {
    if (is_optimized()) {
      return optimized.get_ec_stripe_chunk_size();
    }
    return legacy.get_ec_stripe_chunk_size();
  }

  int objects_get_attrs(
    const hobject_t &hoid,
    std::map<std::string, ceph::buffer::list, std::less<>> *out) override
  {
    // call from parents -- get raw attrs, without any filtering for hinfo
    int r = PGBackend::objects_get_attrs(hoid, out);
    if (r < 0)
      return r;
    if (is_optimized()) {
      return optimized.objects_get_attrs(hoid, out);
    }
    return legacy.objects_get_attrs(hoid, out);
  }

  int objects_get_attrs_with_hinfo(
    const hobject_t &hoid,
    std::map<std::string, ceph::buffer::list, std::less<>> *out)
  {
    // call from parents -- get raw attrs, without any filtering for hinfo
    return PGBackend::objects_get_attrs(hoid, out);
  }

  uint64_t
  object_size_to_shard_size(const uint64_t size, int shard) const override
  {
    if (is_optimized()) {
      return optimized.object_size_to_shard_size(size);
    }
    return legacy.object_size_to_shard_size(size);
    // All shards are the same size.
  }
};
