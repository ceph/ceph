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

#ifndef WITH_CRIMSON
#include "osd_internal_types.h"
#endif
#include "OSDMap.h"
#include "common/WorkQueue.h"
#include "PGLog.h"
#include "messages/MOSDPGPush.h"
#include "osd/oob_direct_read.h"

// ECListener -- an interface decoupling the pipelines from
// particular implementation of ECBackendL (crimson vs cassical).
// https://stackoverflow.com/q/7872958
class OSDOobExecutor;
namespace ceph::osd::oob {
struct xfer_job;
}
namespace ceph::oob {
class Arena;
}

struct ECListener {
  virtual ~ECListener() = default;

  /// the out-of-band executor that serves a delivery token; a shard
  /// uses it to push its part of a gather into the primary's window
  virtual OSDOobExecutor* oob_executor_for(const std::string& token) {
    return nullptr;
  }
  /// hand j, a shard's push into a gather window, to the OSD's
  /// out-of-band transfer service (oob::Transfers::submit()): 0 when it
  /// took the job, which then runs, or expires, exactly once, before
  /// this returns when the OSD has no transfer threads; a negative
  /// errno, with j untouched and nothing started, when the service
  /// refuses it or there is none. Called with the PG lock held.
  virtual int oob_submit(std::unique_ptr<ceph::osd::oob::xfer_job>& j) {
    return -EOPNOTSUPP;
  }
  /// whether e's transfers run on the thread that submits them even
  /// when the OSD has transfer threads (OSDService::oob_runs_inline())
  virtual bool oob_runs_inline(const OSDOobExecutor* e) const {
    return true;
  }
  /// the executor that lends receive windows for gathers, when the
  /// primary gathers shard reads out of band; null otherwise
  virtual OSDOobExecutor* oob_gather_executor() {
    return nullptr;
  }
  /// the memory that the OSD's transports write from without a copy
  /// (osd_oob_direct_read); null when the OSD has none
  virtual ceph::oob::Arena* oob_arena() {
    return nullptr;
  }
  /// osd_oob_direct_read, as the OSD has it now
  virtual ceph::osd::oob::direct_read_t oob_direct_read() const {
    return ceph::osd::oob::direct_read_t::off;
  }

  virtual const OSDMapRef& pgb_get_osdmap() const = 0;
  virtual epoch_t pgb_get_osdmap_epoch() const = 0;
  virtual const pg_info_t &get_info() const = 0;
  virtual uint64_t min_peer_features() const = 0;
  /**
   * Called when a pull on soid cannot be completed due to
   * down peers
   */
  // XXX
  virtual void cancel_pull(
    const hobject_t &soid) = 0;

  // XXX
  virtual pg_shard_t primary_shard() const = 0;
  virtual bool pgb_is_primary() const = 0;

  /**
   * Called when a client read of hoid decoded its data without shards
   * whose own object store failed the read with a media error (-EIO).
   * The read completes with the decoded data whatever this does; a
   * clean PG may mark the object missing on those shards and recover it
   * (osd_ec_repair_on_read). Called with the PG lock held.
   */
  virtual void ec_repair_on_read(
    const hobject_t &hoid,
    const std::set<pg_shard_t> &shards) = 0;

  /**
   * Called when a read from a std::set of replicas/primary fails
   */
  virtual void on_failed_pull(
    const std::set<pg_shard_t> &from,
    const hobject_t &soid,
    const eversion_t &v
    ) = 0;

  /**
   * Called with the transaction recovering oid
   */
  virtual void on_local_recover(
    const hobject_t &oid,
    const ObjectRecoveryInfo &recovery_info,
    ObjectContextRef obc,
    bool is_delete,
    ceph::os::Transaction *t
    ) = 0;

  /**
   * Called when transaction recovering oid is durable and
   * applied on all replicas
   */
  virtual void on_global_recover(
    const hobject_t &oid,
    const object_stat_sum_t &stat_diff,
    bool is_delete
    ) = 0;

  /**
   * Called when peer is recovered
   */
  virtual void on_peer_recover(
    pg_shard_t peer,
    const hobject_t &oid,
    const ObjectRecoveryInfo &recovery_info
    ) = 0;

  virtual void begin_peer_recover(
    pg_shard_t peer,
    const hobject_t oid) = 0;

  virtual bool pg_is_repair() const = 0;

#ifndef WITH_CRIMSON
  virtual ObjectContextRef get_obc(
    const hobject_t &hoid,
    const std::map<std::string, ceph::buffer::list, std::less<>> &attrs) = 0;
#endif

  virtual bool check_failsafe_full() = 0;
  virtual hobject_t get_temp_recovery_object(const hobject_t& target,
						eversion_t version) = 0;
#ifndef WITH_CRIMSON
     virtual bool pg_is_remote_backfilling() = 0;
     virtual void pg_add_local_num_bytes(int64_t num_bytes) = 0;
     //virtual void pg_sub_local_num_bytes(int64_t num_bytes) = 0;
     virtual void pg_add_num_bytes(int64_t num_bytes) = 0;
     //virtual void pg_sub_num_bytes(int64_t num_bytes) = 0;
#endif
   virtual void inc_osd_stat_repaired() = 0;
   virtual void add_temp_obj(const hobject_t &oid) = 0;
   virtual void clear_temp_obj(const hobject_t &oid) = 0;
   virtual epoch_t get_last_peering_reset_epoch() const = 0;

  // XXX
#ifndef WITH_CRIMSON
  virtual GenContext<ThreadPool::TPHandle&> *bless_unlocked_gencontext(
    GenContext<ThreadPool::TPHandle&> *c) = 0;

  virtual void schedule_recovery_work(
    GenContext<ThreadPool::TPHandle&> *c,
    uint64_t cost) = 0;
#endif

  virtual epoch_t get_interval_start_epoch() const = 0;
  virtual const std::set<pg_shard_t> &get_acting_shards() const = 0;
  virtual const std::set<pg_shard_t> &get_backfill_shards() const = 0;
  virtual const std::map<hobject_t, std::set<pg_shard_t>> &get_missing_loc_shards()
    const = 0;

  virtual const std::map<pg_shard_t,
			 pg_missing_t> &get_shard_missing() const = 0;
  virtual const pg_missing_const_i &get_shard_missing(pg_shard_t peer) const = 0;
#if 1
  virtual const pg_missing_const_i * maybe_get_shard_missing(
    pg_shard_t peer) const = 0;
  virtual const pg_info_t &get_shard_info(pg_shard_t peer) const = 0;
#endif
  virtual ceph_tid_t get_tid() = 0;
  virtual pg_shard_t whoami_shard() const = 0;
#if 0
  int whoami() const {
    return whoami_shard().osd;
  }
  spg_t whoami_spg_t() const {
    return get_info().pgid;
  }
#endif
  // XXX
  virtual void send_message_osd_cluster(
    std::vector<std::pair<int, Message*>>& messages, epoch_t from_epoch) = 0;
  virtual void send_message_osd_cluster(
    int osd, MOSDPGPush* msg, epoch_t from_epoch) = 0;

  virtual std::ostream& gen_dbg_prefix(std::ostream& out) const = 0;

  // RMWPipeline
  virtual const pg_pool_t &get_pool() const = 0;
  virtual const std::set<pg_shard_t> &get_acting_recovery_backfill_shards() const = 0;
  virtual const shard_id_set &get_acting_recovery_backfill_shard_id_set() const = 0;
  // XXX
  virtual bool should_send_op(
    pg_shard_t peer,
    const hobject_t &hoid) = 0;
  virtual const std::map<pg_shard_t, pg_info_t> &get_shard_info() const = 0;
  virtual spg_t primary_spg_t() const = 0;
  virtual const PGLog &get_log() const = 0;
  virtual DoutPrefixProvider *get_dpp() = 0;
  // XXX
  virtual void apply_stats(
     const hobject_t &soid,
     const object_stat_sum_t &delta_stats) = 0;

  // new batch
  virtual bool is_missing_object(const hobject_t& oid) const = 0;
  virtual void add_local_next_event(const pg_log_entry_t& e) = 0;
  virtual void log_operation(
    std::vector<pg_log_entry_t>&& logv,
    const std::optional<pg_hit_set_history_t> &hset_history,
    const eversion_t &trim_to,
    const eversion_t &roll_forward_to,
    const eversion_t &pg_committed_to,
    bool transaction_applied,
    ObjectStore::Transaction &t,
    bool async = false) = 0;
  virtual void op_applied(
    const eversion_t &applied_version) = 0;
};
