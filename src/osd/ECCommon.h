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

#include <boost/intrusive/set.hpp>
#include <boost/intrusive/list.hpp>
#include <fmt/format.h>

#include "common/sharedptr_registry.hpp"
#include "erasure-code/ErasureCodeInterface.h"
#include "ECUtil.h"
#include "ECTypes.h"
#if WITH_SEASTAR
#include "ExtentCache.h"
#include "crimson/osd/object_context.h"
#include "os/Transaction.h"
#include "osd/OSDMap.h"
#include "osd/osd_op_util.h"

struct ECTransaction {
  struct WritePlan {
    bool invalidates_cache = false; // Yes, both are possible
    std::map<hobject_t,extent_set> to_read;
    std::map<hobject_t,extent_set> will_write; // superset of to_read

    std::map<hobject_t,ECUtil::HashInfoRef> hash_infos;
  };
};

typedef void* OpRequestRef;
typedef crimson::osd::ObjectContextRef ObjectContextRef;
#else
#include "common/WorkQueue.h"
#endif

#include "ECTransaction.h"
#include "ExtentCache.h"
#include "ECListener.h"

//forward declaration
struct ECSubWrite;
struct PGLog;

struct ECCommon {
  struct ec_extent_t {
    int err;
    extent_map emap;
  };
  friend std::ostream &operator<<(std::ostream &lhs, const ec_extent_t &rhs);
  using ec_extents_t = std::map<hobject_t, ec_extent_t>;

  virtual ~ECCommon() = default;

  virtual void handle_sub_write(
    pg_shard_t from,
    OpRequestRef msg,
    ECSubWrite &op,
    const ZTracer::Trace &trace,
    ECListener& eclistener
    ) = 0;

  virtual void objects_read_and_reconstruct(
    const std::map<hobject_t, std::list<ec_align_t>> &reads,
    bool fast_read,
    GenContextURef<ec_extents_t &&> &&func) = 0;

  struct read_request_t {
    const std::list<ec_align_t> to_read;
    std::map<pg_shard_t, std::vector<std::pair<int, int>>> need;
    bool want_attrs;
    read_request_t(
      const std::list<ec_align_t> &to_read,
      const std::map<pg_shard_t, std::vector<std::pair<int, int>>> &need,
      bool want_attrs)
      : to_read(to_read), need(need), want_attrs(want_attrs) {}
  };
  friend std::ostream &operator<<(std::ostream &lhs, const read_request_t &rhs);
  struct ReadOp;
  /**
   * Low level async read mechanism
   *
   * To avoid duplicating the logic for requesting and waiting for
   * multiple object shards, there is a common async read mechanism
   * taking a std::map of hobject_t->read_request_t which defines callbacks
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
    std::map<pg_shard_t, int> errors;
    std::optional<std::map<std::string, ceph::buffer::list, std::less<>> > attrs;
    std::list<
      boost::tuple<
	uint64_t, uint64_t, std::map<pg_shard_t, ceph::buffer::list> > > returned;
    read_result_t() : r(0) {}
  };

  struct ReadCompleter {
    virtual void finish_single_request(
      const hobject_t &hoid,
      read_result_t &res,
      std::list<ec_align_t> to_read,
      std::set<int> wanted_to_read) = 0;

    virtual void finish(int priority) && = 0;

    virtual ~ReadCompleter() = default;
  };

  friend struct CallClientContexts;
  struct ClientAsyncReadStatus {
    unsigned objects_to_read;
    GenContextURef<ec_extents_t &&> func;
    ec_extents_t results;
    explicit ClientAsyncReadStatus(
      unsigned objects_to_read,
      GenContextURef<ec_extents_t &&> &&func)
      : objects_to_read(objects_to_read), func(std::move(func)) {}
    void complete_object(
      const hobject_t &hoid,
      int err,
      extent_map &&buffers) {
      ceph_assert(objects_to_read);
      --objects_to_read;
      ceph_assert(!results.count(hoid));
      results.emplace(hoid, ec_extent_t{err, std::move(buffers)});
    }
    bool is_complete() const {
      return objects_to_read == 0;
    }
    void run() {
      func.release()->complete(std::move(results));
    }
  };

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
    std::unique_ptr<ReadCompleter> on_complete;

    ZTracer::Trace trace;

    std::map<hobject_t, std::set<int>> want_to_read;
    std::map<hobject_t, read_request_t> to_read;
    std::map<hobject_t, read_result_t> complete;

    std::map<hobject_t, std::set<pg_shard_t>> obj_to_source;
    std::map<pg_shard_t, std::set<hobject_t> > source_to_obj;

    void dump(ceph::Formatter *f) const;

    std::set<pg_shard_t> in_progress;

    ReadOp(
      int priority,
      ceph_tid_t tid,
      bool do_redundant_reads,
      bool for_recovery,
      std::unique_ptr<ReadCompleter> _on_complete,
      OpRequestRef op,
      std::map<hobject_t, std::set<int>> &&_want_to_read,
      std::map<hobject_t, read_request_t> &&_to_read)
      : priority(priority),
        tid(tid),
        op(op),
        do_redundant_reads(do_redundant_reads),
        for_recovery(for_recovery),
        on_complete(std::move(_on_complete)),
        want_to_read(std::move(_want_to_read)),
	to_read(std::move(_to_read)) {
      for (auto &&hpair: to_read) {
	auto &returned = complete[hpair.first].returned;
	for (auto &&extent: hpair.second.to_read) {
	  returned.push_back(
	    boost::make_tuple(
	      extent.offset,
	      extent.size,
	      std::map<pg_shard_t, ceph::buffer::list>()));
	}
      }
    }
    ReadOp() = delete;
    ReadOp(const ReadOp &) = delete; // due to on_complete being unique_ptr
    ReadOp(ReadOp &&) = default;
  };
  struct ReadPipeline {
    void objects_read_and_reconstruct(
      const std::map<hobject_t, std::list<ec_align_t>> &reads,
      bool fast_read,
      GenContextURef<ec_extents_t &&> &&func);

    template <class F, class G>
    void filter_read_op(
      const OSDMapRef& osdmap,
      ReadOp &op,
      F&& on_erase,
      G&& on_schedule_recovery);

    template <class F, class G>
    void check_recovery_sources(
      const OSDMapRef& osdmap,
      F&& on_erase,
      G&& on_schedule_recovery);

    void complete_read_op(ReadOp &rop);

    void start_read_op(
      int priority,
      std::map<hobject_t, std::set<int>> &want_to_read,
      std::map<hobject_t, read_request_t> &to_read,
      OpRequestRef op,
      bool do_redundant_reads,
      bool for_recovery,
      std::unique_ptr<ReadCompleter> on_complete);

    void do_read_op(ReadOp &rop);

    int send_all_remaining_reads(
      const hobject_t &hoid,
      ReadOp &rop);

    void on_change();

    void kick_reads();

    std::map<ceph_tid_t, ReadOp> tid_to_read_map;
    std::map<pg_shard_t, std::set<ceph_tid_t> > shard_to_read_map;
    std::list<ClientAsyncReadStatus> in_progress_client_reads;

    CephContext* cct;
    ceph::ErasureCodeInterfaceRef ec_impl;
    const ECUtil::stripe_info_t& sinfo;
    // TODO: lay an interface down here
    ECListener* parent;

    ECListener *get_parent() const { return parent; }
    const OSDMapRef& get_osdmap() const { return get_parent()->pgb_get_osdmap(); }
    epoch_t get_osdmap_epoch() const { return get_parent()->pgb_get_osdmap_epoch(); }
    const pg_info_t &get_info() { return get_parent()->get_info(); }

    ReadPipeline(CephContext* cct,
                ceph::ErasureCodeInterfaceRef ec_impl,
                const ECUtil::stripe_info_t& sinfo,
                ECListener* parent)
      : cct(cct),
        ec_impl(std::move(ec_impl)),
        sinfo(sinfo),
        parent(parent) {
    }

    /**
     * While get_want_to_read_shards creates a want_to_read based on the EC
     * plugin's all get_data_chunk_count() (full stripe), this method
     * inserts only the chunks actually necessary to read the length of data.
     * That is, we can do so called "partial read" -- fetch subset of stripe.
     *
     * Like in get_want_to_read_shards, we check the plugin's mapping.
     *
     */
    void get_min_want_to_read_shards(
      uint64_t offset,				///< [in]
      uint64_t length,    			///< [in]
      std::set<int> *want_to_read               ///< [out]
      );
    static void get_min_want_to_read_shards(
      const uint64_t offset,
      const uint64_t length,
      const ECUtil::stripe_info_t& sinfo,
      std::set<int> *want_to_read);

    int get_remaining_shards(
      const hobject_t &hoid,
      const std::set<int> &avail,
      const std::set<int> &want,
      const read_result_t &result,
      std::map<pg_shard_t, std::vector<std::pair<int, int>>> *to_read,
      bool for_recovery);

    void get_all_avail_shards(
      const hobject_t &hoid,
      const std::set<pg_shard_t> &error_shards,
      std::set<int> &have,
      std::map<shard_id_t, pg_shard_t> &shards,
      bool for_recovery);

    friend std::ostream &operator<<(std::ostream &lhs, const ReadOp &rhs);
    friend struct FinishReadOp;

    void get_want_to_read_shards(std::set<int> *want_to_read) const;

    /// Returns to_read replicas sufficient to reconstruct want
    int get_min_avail_to_read_shards(
      const hobject_t &hoid,     ///< [in] object
      const std::set<int> &want,      ///< [in] desired shards
      bool for_recovery,         ///< [in] true if we may use non-acting replicas
      bool do_redundant_reads,   ///< [in] true if we want to issue redundant reads to reduce latency
      std::map<pg_shard_t, std::vector<std::pair<int, int>>> *to_read   ///< [out] shards, corresponding subchunks to read
      ); ///< @return error code, 0 on success

    void schedule_recovery_work();

  };

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
   * on the writing std::list.
   */

  struct RMWPipeline {
    struct Op : boost::intrusive::list_base_hook<> {
      /// From submit_transaction caller, describes operation
      hobject_t hoid;
      object_stat_sum_t delta_stats;
      eversion_t version;
      eversion_t trim_to;
      std::optional<pg_hit_set_history_t> updated_hit_set_history;
      std::vector<pg_log_entry_t> log_entries;
      ceph_tid_t tid;
      osd_reqid_t reqid;
      ZTracer::Trace trace;

      /**
       * pg_commited_to
       *
       * Represents a version v such that all v' < v handled by RMWPipeline
       * have fully committed. This may actually lag
       * PeeringState::pg_committed_to if PrimaryLogPG::submit_log_entries
       * submits an out-of-band log update.
       *
       * Soon to be generated internally.
       */
      eversion_t pg_committed_to;

      /// Ancillary also provided from submit_transaction caller
      std::map<hobject_t, ObjectContextRef> obc_map;

      /// see call_write_ordered
      std::list<std::function<void(void)> > on_write;

      /// Generated internally
      std::set<hobject_t> temp_added;
      std::set<hobject_t> temp_cleared;

      ECTransaction::WritePlan plan;
      bool requires_rmw() const { return !plan.to_read.empty(); }
      bool invalidates_cache() const { return plan.invalidates_cache; }

      // must be true if requires_rmw(), must be false if invalidates_cache()
      bool using_cache = true;

      /// In progress read state;
      std::map<hobject_t,extent_set> pending_read; // subset already being read
      std::map<hobject_t,extent_set> remote_read;  // subset we must read
      std::map<hobject_t,extent_map> remote_read_result;
      bool read_in_progress() const {
        return !remote_read.empty() && remote_read_result.empty();
      }

      /// In progress write state.
      std::set<pg_shard_t> pending_commit;
      // we need pending_apply for pre-mimic peers so that we don't issue a
      // read on a remote shard before it has applied a previous write.  We can
      // remove this after nautilus.
      std::set<pg_shard_t> pending_apply;
      bool write_in_progress() const {
        return !pending_commit.empty() || !pending_apply.empty();
      }

      /// optional, may be null, for tracking purposes
      OpRequestRef client_op;

      /// pin for cache
      ExtentCache::write_pin pin;

      /// Callbacks
      Context *on_all_commit = nullptr;
      virtual ~Op() {
        delete on_all_commit;
      }

      virtual void generate_transactions(
        ceph::ErasureCodeInterfaceRef &ecimpl,
        pg_t pgid,
        const ECUtil::stripe_info_t &sinfo,
        std::map<hobject_t,extent_map> *written,
        std::map<shard_id_t, ceph::os::Transaction> *transactions,
        DoutPrefixProvider *dpp,
        const ceph_release_t require_osd_release = ceph_release_t::unknown) = 0;
    };
    using OpRef = std::unique_ptr<Op>;
    using op_list = boost::intrusive::list<Op>;
    friend std::ostream &operator<<(std::ostream &lhs, const Op &rhs);

    ExtentCache cache;
    std::map<ceph_tid_t, OpRef> tid_to_op_map; /// Owns Op structure
    /**
     * We model the possible rmw states as a std::set of waitlists.
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
      friend std::ostream &operator<<(std::ostream &lhs, const pipeline_state_t &rhs);
    } pipeline_state;

    op_list waiting_state;        /// writes waiting on pipe_state
    op_list waiting_reads;        /// writes waiting on partial stripe reads
    op_list waiting_commit;       /// writes waiting on initial commit
    eversion_t completed_to;
    eversion_t committed_to;
    void start_rmw(OpRef op);
    bool try_state_to_reads();
    bool try_reads_to_commit();
    bool try_finish_rmw();
    void check_ops();

    void on_change();
    void call_write_ordered(std::function<void(void)> &&cb);

    CephContext* cct;
    ECListener *get_parent() const { return parent; }
    const OSDMapRef& get_osdmap() const { return get_parent()->pgb_get_osdmap(); }
    epoch_t get_osdmap_epoch() const { return get_parent()->pgb_get_osdmap_epoch(); }
    const pg_info_t &get_info() { return get_parent()->get_info(); }

    template <typename Func>
    void objects_read_async_no_cache(
      const std::map<hobject_t,extent_set> &to_read,
      Func &&on_complete
    ) {
      std::map<hobject_t, std::list<ec_align_t>> _to_read;
      for (auto &&hpair: to_read) {
        auto &l = _to_read[hpair.first];
        for (auto extent: hpair.second) {
          l.emplace_back(ec_align_t{extent.first, extent.second, 0});
        }
      }
      ec_backend.objects_read_and_reconstruct(
        _to_read,
        false,
        make_gen_lambda_context<
        ECCommon::ec_extents_t &&, Func>(
            std::forward<Func>(on_complete)));
    }
    void handle_sub_write(
      pg_shard_t from,
      OpRequestRef msg,
      ECSubWrite &op,
      const ZTracer::Trace &trace
    ) {
      ec_backend.handle_sub_write(from, std::move(msg), op, trace, *get_parent());
    }
    // end of iface

    ceph::ErasureCodeInterfaceRef ec_impl;
    const ECUtil::stripe_info_t& sinfo;
    ECListener* parent;
    ECCommon& ec_backend;

    RMWPipeline(CephContext* cct,
                ceph::ErasureCodeInterfaceRef ec_impl,
                const ECUtil::stripe_info_t& sinfo,
                ECListener* parent,
                ECCommon& ec_backend)
      : cct(cct),
        ec_impl(std::move(ec_impl)),
        sinfo(sinfo),
        parent(parent),
        ec_backend(ec_backend) {
    }
  };

  class UnstableHashInfoRegistry {
    CephContext *cct;
    ceph::ErasureCodeInterfaceRef ec_impl;
    /// If modified, ensure that the ref is held until the update is applied
    SharedPtrRegistry<hobject_t, ECUtil::HashInfo> registry;

  public:
    UnstableHashInfoRegistry(
      CephContext *cct,
      ceph::ErasureCodeInterfaceRef ec_impl)
      : cct(cct),
	ec_impl(std::move(ec_impl)) {}

    ECUtil::HashInfoRef maybe_put_hash_info(
      const hobject_t &hoid,
      ECUtil::HashInfo &&hinfo);

    ECUtil::HashInfoRef get_hash_info(
      const hobject_t &hoid,
      bool create,
      const std::map<std::string, ceph::buffer::list, std::less<>>& attr,
      uint64_t size);
  };
};

std::ostream &operator<<(std::ostream &lhs,
			 const ECCommon::RMWPipeline::pipeline_state_t &rhs);
std::ostream &operator<<(std::ostream &lhs,
			 const ECCommon::read_request_t &rhs);
std::ostream &operator<<(std::ostream &lhs,
			 const ECCommon::read_result_t &rhs);
std::ostream &operator<<(std::ostream &lhs,
			 const ECCommon::ReadOp &rhs);
std::ostream &operator<<(std::ostream &lhs,
			 const ECCommon::RMWPipeline::Op &rhs);

template <class F, class G>
void ECCommon::ReadPipeline::check_recovery_sources(
  const OSDMapRef& osdmap,
  F&& on_erase,
  G&& on_schedule_recovery)
{
  std::set<ceph_tid_t> tids_to_filter;
  for (std::map<pg_shard_t, std::set<ceph_tid_t> >::iterator 
       i = shard_to_read_map.begin();
       i != shard_to_read_map.end();
       ) {
    if (osdmap->is_down(i->first.osd)) {
      tids_to_filter.insert(i->second.begin(), i->second.end());
      shard_to_read_map.erase(i++);
    } else {
      ++i;
    }
  }
  for (std::set<ceph_tid_t>::iterator i = tids_to_filter.begin();
       i != tids_to_filter.end();
       ++i) {
    std::map<ceph_tid_t, ReadOp>::iterator j = tid_to_read_map.find(*i);
    ceph_assert(j != tid_to_read_map.end());
    filter_read_op(osdmap, j->second, on_erase, on_schedule_recovery);
  }
}

template <class F, class G>
void ECCommon::ReadPipeline::filter_read_op(
  const OSDMapRef& osdmap,
  ReadOp &op,
  F&& on_erase,
  G&& on_schedule_recovery)
{
  std::set<hobject_t> to_cancel;
  for (std::map<pg_shard_t, std::set<hobject_t> >::iterator i = op.source_to_obj.begin();
       i != op.source_to_obj.end();
       ++i) {
    if (osdmap->is_down(i->first.osd)) {
      to_cancel.insert(i->second.begin(), i->second.end());
      op.in_progress.erase(i->first);
      continue;
    }
  }

  if (to_cancel.empty())
    return;

  for (std::map<pg_shard_t, std::set<hobject_t> >::iterator i = op.source_to_obj.begin();
       i != op.source_to_obj.end();
       ) {
    for (std::set<hobject_t>::iterator j = i->second.begin();
	 j != i->second.end();
	 ) {
      if (to_cancel.count(*j))
	i->second.erase(j++);
      else
	++j;
    }
    if (i->second.empty()) {
      op.source_to_obj.erase(i++);
    } else {
      ceph_assert(!osdmap->is_down(i->first.osd));
      ++i;
    }
  }

  for (std::set<hobject_t>::iterator i = to_cancel.begin();
       i != to_cancel.end();
       ++i) {
    get_parent()->cancel_pull(*i);

    ceph_assert(op.to_read.count(*i));
    op.to_read.erase(*i);
    op.complete.erase(*i);
    on_erase(*i);
  }

  if (op.in_progress.empty()) {
    /* This case is odd.  filter_read_op gets called while processing
     * an OSDMap.  Normal, non-recovery reads only happen from acting
     * set osds.  For this op to have had a read source go down and
     * there not be an interval change, it must be part of a pull during
     * log-based recovery.
     *
     * This callback delays calling complete_read_op until later to avoid
     * dealing with recovery while handling an OSDMap.  We assign a
     * cost here of 1 because:
     * 1) This should be very rare, and the operation itself was already
     *    throttled.
     * 2) It shouldn't result in IO, rather it should result in restarting
     *    the pull on the affected objects and pushes from in-memory buffers
     *    on any now complete unaffected objects.
     */
    on_schedule_recovery(op);
  }
}

template <> struct fmt::formatter<ECCommon::RMWPipeline::pipeline_state_t> : fmt::ostream_formatter {};
template <> struct fmt::formatter<ECCommon::read_request_t> : fmt::ostream_formatter {};
template <> struct fmt::formatter<ECCommon::read_result_t> : fmt::ostream_formatter {};
template <> struct fmt::formatter<ECCommon::ReadOp> : fmt::ostream_formatter {};
template <> struct fmt::formatter<ECCommon::RMWPipeline::Op> : fmt::ostream_formatter {};