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
#include <fmt/format.h>

#include "common/sharedptr_registry.hpp"
#include "erasure-code/ErasureCodeInterface.h"
#include "ECUtil.h"
#include "ECTypes.h"
#include "messages/MOSDPGPushReply.h"
#include "msg/MessageRef.h"
#if WITH_CRIMSON
#include "crimson/osd/object_context.h"
#include "os/Transaction.h"
#include "osd/OSDMap.h"
#include "osd/osd_op_util.h"

struct ECTransaction {
  struct WritePlan {
    bool invalidates_cache = false; // Yes, both are possible
    std::map<hobject_t,extent_set> to_read;
    std::map<hobject_t,extent_set> will_write;
  };
};

typedef void* OpRequestRef;
typedef crimson::osd::ObjectContextRef ObjectContextRef;
#else
#include "common/WorkQueue.h"
#endif

#include "ECTransaction.h"
#include "ECExtentCache.h"
#include "ECListener.h"
#include "common/dout.h"

//forward declaration
struct ECBackend;
struct ECSubWrite;
struct ECSubRead;
struct PGLog;
struct RecoveryMessages;

struct ECCommon {
  struct ec_extent_t {
    int err;
    extent_map emap;
    ECUtil::shard_extent_map_t shard_extent_map;

    void print(std::ostream &os) const {
      os << err << "," << emap;
    }
  };

  using ec_extents_t = std::map<hobject_t, ec_extent_t>;

  static inline const uint32_t scrub_fadvise_flags{
      CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL |
      CEPH_OSD_OP_FLAG_FADVISE_DONTNEED |
      CEPH_OSD_OP_FLAG_BYPASS_CLEAN_CACHE};

  virtual ~ECCommon() = default;

  virtual void handle_sub_write(
      pg_shard_t from,
      OpRequestRef msg,
      ECSubWrite &op,
      const ZTracer::Trace &trace,
      ECListener &eclistener) = 0;

#ifdef WITH_CRIMSON
  virtual void handle_sub_read_n_reply(
    pg_shard_t from,
    ECSubRead &op,
    const ZTracer::Trace &trace
    ) = 0;
#endif

  virtual void objects_read_and_reconstruct(
      const std::map<hobject_t, std::list<ec_align_t>> &reads,
      bool fast_read,
      uint64_t object_size,
      GenContextURef<ec_extents_t&&> &&func) = 0;

  struct shard_read_t {
    extent_set extents;
    std::optional<std::vector<std::pair<int, int>>> subchunk;
    pg_shard_t pg_shard;
    bool operator==(const shard_read_t &other) const;

    void print(std::ostream &os) const {
      os << "shard_read_t(extents=[" << extents << "]"
          << ", subchunk=" << subchunk
          << ", pg_shard=" << pg_shard
          << ")";
    }
  };

  struct read_request_t {
    const std::list<ec_align_t> to_read;
    const uint32_t flags = 0;
    ECUtil::shard_extent_set_t shard_want_to_read;
    ECUtil::shard_extent_set_t zeros_for_decode;
    shard_id_map<shard_read_t> shard_reads;
    bool want_attrs = false;
    uint64_t object_size;

    read_request_t(
        const std::list<ec_align_t> &to_read,
        const ECUtil::shard_extent_set_t &shard_want_to_read,
        bool want_attrs, uint64_t object_size) :
      to_read(to_read),
      flags(to_read.front().flags),
      shard_want_to_read(shard_want_to_read),
      zeros_for_decode(shard_want_to_read.get_max_shards()),
      shard_reads(shard_want_to_read.get_max_shards()),
      want_attrs(want_attrs),
      object_size(object_size) {}

    read_request_t(const ECUtil::shard_extent_set_t &shard_want_to_read,
               bool want_attrs, uint64_t object_size) :
      shard_want_to_read(shard_want_to_read),
      zeros_for_decode(shard_want_to_read.get_max_shards()),
      shard_reads(shard_want_to_read.get_max_shards()),
      want_attrs(want_attrs),
      object_size(object_size) {}

    bool operator==(const read_request_t &other) const;

    void print(std::ostream &os) const {
      os << "read_request_t(to_read=[" << to_read << "]"
          << ", flags=" << flags
          << ", shard_want_to_read=" << shard_want_to_read
          << ", zeros_for_decode=" << zeros_for_decode
          << ", shard_reads=" << shard_reads
          << ", want_attrs=" << want_attrs
          << ")";
    }
  };

  virtual void objects_read_and_reconstruct_for_rmw(
      std::map<hobject_t, read_request_t> &&to_read,
      GenContextURef<ec_extents_t&&> &&func) = 0;

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
    std::optional<std::map<std::string, ceph::buffer::list, std::less<>>> attrs;
    ECUtil::shard_extent_map_t buffers_read;
    ECUtil::shard_extent_set_t processed_read_requests;
    shard_id_set zero_length_reads;

    read_result_t(const ECUtil::stripe_info_t *sinfo) :
      r(0), buffers_read(sinfo),
      processed_read_requests(sinfo->get_k_plus_m()) {}

    void print(std::ostream &os) const {
      os << "read_result_t(r=" << r << ", errors=" << errors;
      if (attrs) {
        os << ", attrs=" << *(attrs);
      } else {
        os << ", noattrs";
      }
      os << ", buffers_read=" << buffers_read;
      os << ", processed_read_requests=" << processed_read_requests;
      os << ", zero_length_reads=" << zero_length_reads << ")";
    }
  };

  struct ReadCompleter {
    virtual void finish_single_request(
        const hobject_t &hoid,
        read_result_t &&res,
        ECCommon::read_request_t &req) = 0;

    virtual void finish(int priority) && = 0;

    virtual ~ReadCompleter() = default;
  };

  friend struct CallClientContexts;

  struct ClientAsyncReadStatus {
    unsigned objects_to_read;
    GenContextURef<ec_extents_t&&> func;
    ec_extents_t results;

    explicit ClientAsyncReadStatus(
        unsigned objects_to_read,
        GenContextURef<ec_extents_t&&> &&func)
      : objects_to_read(objects_to_read), func(std::move(func)) {}

    void complete_object(
        const hobject_t &hoid,
        int err,
        extent_map &&buffers,
        ECUtil::shard_extent_map_t &&shard_extent_map) {
      ceph_assert(objects_to_read);
      --objects_to_read;
      ceph_assert(!results.contains(hoid));
      results.emplace(hoid, ec_extent_t{
                        err, std::move(buffers),
                        std::move(shard_extent_map)
                      });
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

    std::map<hobject_t, read_request_t> to_read;
    std::map<hobject_t, read_result_t> complete;

    std::map<hobject_t, std::set<pg_shard_t>> obj_to_source;
    std::map<pg_shard_t, std::set<hobject_t>> source_to_obj;

    void dump(ceph::Formatter *f) const;

    std::set<pg_shard_t> in_progress;

    std::list<ECUtil::log_entry_t> debug_log;

    ReadOp(
        int priority,
        ceph_tid_t tid,
        bool do_redundant_reads,
        bool for_recovery,
        std::unique_ptr<ReadCompleter> _on_complete,
        std::map<hobject_t, read_request_t> &&_to_read)
      : priority(priority),
        tid(tid),
        do_redundant_reads(do_redundant_reads),
        for_recovery(for_recovery),
        on_complete(std::move(_on_complete)),
        to_read(std::move(_to_read)) {}

    ReadOp() = delete;
    ReadOp(const ReadOp &) = delete; // due to on_complete being unique_ptr
    ReadOp(ReadOp &&) = default;

    void print(std::ostream &os) const {
      os << "ReadOp(tid=" << tid;
#ifndef WITH_CRIMSON
      if (op && op->get_req()) {
        os << ", op=";
        op->get_req()->print(os);
      }
#endif
      os << ", to_read=" << to_read << ", complete=" << complete
          << ", priority=" << priority << ", obj_to_source=" << obj_to_source
          << ", source_to_obj=" << source_to_obj << ", in_progress=" <<
          in_progress
          << ", debug_log=" << debug_log << ")";
    }
  };

  struct ReadPipeline {
    void objects_read_and_reconstruct(
        const std::map<hobject_t, std::list<ec_align_t>> &reads,
        bool fast_read,
        uint64_t object_size,
        GenContextURef<ec_extents_t&&> &&func);

    void objects_read_and_reconstruct_for_rmw(
        std::map<hobject_t, read_request_t> &&to_read,
        GenContextURef<ECCommon::ec_extents_t&&> &&func);

    template <class F, class G>
    void filter_read_op(
        const OSDMapRef &osdmap,
        ReadOp &op,
        F &&on_erase,
        G &&on_schedule_recovery);

    template <class F, class G>
    void check_recovery_sources(
        const OSDMapRef &osdmap,
        F &&on_erase,
        G &&on_schedule_recovery);

    void complete_read_op(ReadOp &&rop);

    void start_read_op(
        int priority,
        std::map<hobject_t, read_request_t> &to_read,
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
    std::map<pg_shard_t, std::set<ceph_tid_t>> shard_to_read_map;
    std::list<ClientAsyncReadStatus> in_progress_client_reads;

    CephContext *cct;
    ceph::ErasureCodeInterfaceRef ec_impl;
    const ECUtil::stripe_info_t &sinfo;
    // TODO: lay an interface down here
    ECListener *parent;
#ifdef WITH_CRIMSON
    ECCommon &ec_backend;
#endif

    ECListener *get_parent() const { return parent; }

    const OSDMapRef &get_osdmap() const {
      return get_parent()->pgb_get_osdmap();
    }

    epoch_t get_osdmap_epoch() const {
      return get_parent()->pgb_get_osdmap_epoch();
    }

    const pg_info_t &get_info() const { return get_parent()->get_info(); }

    ReadPipeline(CephContext *cct,
                 ceph::ErasureCodeInterfaceRef ec_impl,
                 const ECUtil::stripe_info_t &sinfo,
#ifdef WITH_CRIMSON
                 ECListener *parent,
                 ECCommon &ec_backend)
#else
                 ECListener *parent)
#endif
      : cct(cct),
        ec_impl(std::move(ec_impl)),
        sinfo(sinfo),
#ifdef WITH_CRIMSON
        parent(parent),
        ec_backend(ec_backend) {}
#else
        parent(parent) {}
#endif

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
        const ec_align_t &to_read, ///< [in]
        ECUtil::shard_extent_set_t &want_shard_reads); ///< [out]

    int get_remaining_shards(
        const hobject_t &hoid,
        read_result_t &read_result,
        read_request_t &read_request,
        bool for_recovery,
        bool want_attrs);

    void get_all_avail_shards(
        const hobject_t &hoid,
        shard_id_set &have,
        shard_id_map<pg_shard_t> &shards,
        bool for_recovery,
        const std::optional<std::set<pg_shard_t>> &error_shards = std::nullopt);

    std::pair<const shard_id_set, const shard_id_set> get_readable_writable_shard_id_sets();

    friend struct FinishReadOp;

    void get_want_to_read_shards(
        const std::list<ec_align_t> &to_read,
        ECUtil::shard_extent_set_t &want_shard_reads);

    void get_want_to_read_all_shards(
        const std::list<ec_align_t> &to_read,
        ECUtil::shard_extent_set_t &want_shard_reads);
    void create_parity_read_buffer(
        ECUtil::shard_extent_map_t buffers_read,
        ec_align_t read,
        bufferlist *outbl);

    /// Returns to_read replicas sufficient to reconstruct want
    int get_min_avail_to_read_shards(
        const hobject_t &hoid, ///< [in] object
        bool for_recovery, ///< [in] true if we may use non-acting replicas
        bool do_redundant_reads,
        ///< [in] true if we want to issue redundant reads to reduce latency
        read_request_t &read_request,
        ///< [out] shard_reads, corresponding subchunks / other sub reads to read
        const std::optional<std::set<pg_shard_t>> &error_shards = std::nullopt
        //< [in] Shards where reads have failed (optional)
      ); ///< @return error code, 0 on success

#ifdef WITH_CRIMSON
    void handle_sub_read_n_reply(
      pg_shard_t from,
      ECSubRead &op,
      const ZTracer::Trace &trace) {
      ec_backend.handle_sub_read_n_reply(from, op, trace);
    }
#endif
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

  struct RMWPipeline : ECExtentCache::BackendReadListener {
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

      /// Generated internally
      std::set<hobject_t> temp_added;
      std::set<hobject_t> temp_cleared;

      ECTransaction::WritePlan plan;
      bool requires_rmw() const { return !plan.want_read; }

      // must be true if requires_rmw(), must be false if invalidates_cache()
      bool using_cache = true;

      /// In progress read state;
      int pending_cache_ops = 0;
      std::map<hobject_t, ECUtil::shard_extent_map_t> remote_shard_extent_map;

      /// In progress write state.
      int pending_commits = 0;

      bool write_in_progress() const {
        return pending_commits != 0;
      }

      /// optional, may be null, for tracking purposes
      OpRequestRef client_op;

      /// pin for cache
      std::list<ECExtentCache::OpRef> cache_ops;
      RMWPipeline *pipeline;

      Op() : tid(), plan(), pipeline(nullptr) {}

      /// Callbacks
      Context *on_all_commit = nullptr;

      virtual ~Op() {
        delete on_all_commit;
      }

      virtual void generate_transactions(
          ceph::ErasureCodeInterfaceRef &ec_impl,
          pg_t pgid,
          const ECUtil::stripe_info_t &sinfo,
          std::map<hobject_t, ECUtil::shard_extent_map_t> *written,
          shard_id_map<ceph::os::Transaction> *transactions,
          DoutPrefixProvider *dpp,
          const OSDMapRef &osdmap) = 0;

      virtual bool skip_transaction(
          std::set<shard_id_t> &pending_roll_forward,
          shard_id_t shard,
          ceph::os::Transaction &transaction) = 0;

      void cache_ready(const hobject_t &oid, const ECUtil::shard_extent_map_t &result) {
        if (!result.empty()) {
          remote_shard_extent_map.insert(std::pair(oid, result));
        }

        if (!--pending_cache_ops) {
          pipeline->cache_ready(*this);
        }
      }

      void print(std::ostream &os) const {
        os << "Op(" << hoid << " v=" << version << " tt=" << trim_to
            << " tid=" << tid << " reqid=" << reqid;
#ifndef WITH_CRIMSON
        if (client_op && client_op->get_req()) {
          os << " client_op=";
          client_op->get_req()->print(os);
        }
#endif
        os << " pg_committed_to=" << pg_committed_to
            << " temp_added=" << temp_added
            << " temp_cleared=" << temp_cleared
            << " remote_read_result=" << remote_shard_extent_map
            << " pending_commits=" << pending_commits
            << " plans=" << plan
            << ")";
      }
    };

    void backend_read(hobject_t oid, ECUtil::shard_extent_set_t const &request,
                      uint64_t object_size) override {
      std::map<hobject_t, read_request_t> to_read;
      to_read.emplace(oid, read_request_t(request, false, object_size));

      objects_read_async_no_cache(
        std::move(to_read),
        [this](ec_extents_t &&results) {
          for (auto &&[oid, result]: results) {
            extent_cache.read_done(oid, std::move(result.shard_extent_map));
          }
        });
    }

    using OpRef = std::shared_ptr<Op>;

    std::map<ceph_tid_t, OpRef> tid_to_op_map; /// Owns Op structure
    std::map<hobject_t, eversion_t> oid_to_version;

    std::list<OpRef> waiting_commit;
    eversion_t completed_to;
    eversion_t committed_to;
    void start_rmw(OpRef op);
    void cache_ready(Op &op);
    void try_finish_rmw();
    void finish_rmw(OpRef const &op);

    void on_change();
    void on_change2();
    void call_write_ordered(std::function<void(void)> &&cb);

    CephContext *cct;
    ECListener *get_parent() const { return parent; }

    const OSDMapRef &get_osdmap() const {
      return get_parent()->pgb_get_osdmap();
    }

    epoch_t get_osdmap_epoch() const {
      return get_parent()->pgb_get_osdmap_epoch();
    }

    const pg_info_t &get_info() const { return get_parent()->get_info(); }

    template <typename Func>
    void objects_read_async_no_cache(
        std::map<hobject_t, read_request_t> &&to_read,
        Func &&on_complete) {
      ec_backend.objects_read_and_reconstruct_for_rmw(
        std::move(to_read),
        make_gen_lambda_context<
          ECCommon::ec_extents_t&&, Func>(
          std::forward<Func>(on_complete)));
    }

    void handle_sub_write(
        pg_shard_t from,
        OpRequestRef msg,
        ECSubWrite &op,
        const ZTracer::Trace &trace) const {
      ec_backend.handle_sub_write(from, std::move(msg), op, trace,
                                  *get_parent());
    }

    // end of iface

    // Set of shards that will need a dummy transaction for the final
    // roll forward
    std::set<shard_id_t> pending_roll_forward;

    ceph::ErasureCodeInterfaceRef ec_impl;
    const ECUtil::stripe_info_t &sinfo;
    ECListener *parent;
    ECCommon &ec_backend;
    ECExtentCache extent_cache;
    uint64_t ec_pdw_write_mode;
    bool next_write_all_shards = false;

    RMWPipeline(CephContext *cct,
                ceph::ErasureCodeInterfaceRef ec_impl,
                const ECUtil::stripe_info_t &sinfo,
                ECListener *parent,
                ECCommon &ec_backend,
                ECExtentCache::LRU &ec_extent_cache_lru)
      : cct(cct),
        ec_impl(std::move(ec_impl)),
        sinfo(sinfo),
        parent(parent),
        ec_backend(ec_backend),
        extent_cache(*this, ec_extent_cache_lru, sinfo, cct),
        ec_pdw_write_mode(cct->_conf.get_val<uint64_t>("ec_pdw_write_mode")) {}
  };


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
    // TODO: lay an interface down here
    ECListener *parent;

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
                    ECListener *parent);

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
            << " state=" << ECCommon::RecoveryBackend::RecoveryOp::tostr(state)
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
    virtual void maybe_load_obc(
      const std::map<std::string, ceph::bufferlist, std::less<>> &raw_attrs,
      RecoveryOp &op) = 0;
    void dispatch_recovery_messages(RecoveryMessages &m, int priority);

    RecoveryBackend::RecoveryOp recover_object(
        const hobject_t &hoid,
        eversion_t v,
        ObjectContextRef head,
        ObjectContextRef obc);
    void continue_recovery_op(
        RecoveryBackend::RecoveryOp &op,
        RecoveryMessages *m);
    void update_object_size_after_read(
        uint64_t size,
        read_result_t &res,
        read_request_t &req);
    void handle_recovery_read_complete(
        const hobject_t &hoid,
        read_result_t &&res,
        read_request_t &req,
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

  static std::optional<object_info_t> get_object_info_from_obc(
      ObjectContextRef &obc_map
    );

  static ECTransaction::WritePlan get_write_plan(
    const ECUtil::stripe_info_t &sinfo,
    PGTransaction &t,
    ECCommon::ReadPipeline &read_pipeline,
    ECCommon::RMWPipeline &rmw_pipeline,
    DoutPrefixProvider *dpp);
};

struct RecoveryMessages {
  std::map<hobject_t, ECCommon::read_request_t> recovery_reads;

  void recovery_read(const hobject_t &hoid,
                     const ECCommon::read_request_t &read_request) {
    ceph_assert(!recovery_reads.count(hoid));
    recovery_reads.insert(std::make_pair(hoid, read_request));
  }

  std::map<pg_shard_t, std::vector<PushOp>> pushes;
  std::map<pg_shard_t, std::vector<PushReplyOp>> push_replies;
  ceph::os::Transaction t;
};

template <>
struct fmt::formatter<ECCommon::read_request_t> : fmt::ostream_formatter {};

template <>
struct fmt::formatter<ECCommon::read_result_t> : fmt::ostream_formatter {};

template <>
struct fmt::formatter<ECCommon::ReadOp> : fmt::ostream_formatter {};

template <>
struct fmt::formatter<ECCommon::RMWPipeline::Op> : fmt::ostream_formatter {};


template <class F, class G>
void ECCommon::ReadPipeline::check_recovery_sources(
    const OSDMapRef &osdmap,
    F &&on_erase,
    G &&on_schedule_recovery
  ) {
  std::set<ceph_tid_t> tids_to_filter;
  for (std::map<pg_shard_t, std::set<ceph_tid_t>>::iterator
       i = shard_to_read_map.begin();
       i != shard_to_read_map.end();) {
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
    const OSDMapRef &osdmap,
    ReadOp &op,
    F &&on_erase,
    G &&on_schedule_recovery
  ) {
  std::set<hobject_t> to_cancel;
  for (auto &&[pg_shard, hoid_set] : op.source_to_obj) {
    if (osdmap->is_down(pg_shard.osd)) {
      to_cancel.insert(hoid_set.begin(), hoid_set.end());
      op.in_progress.erase(pg_shard);
    }
  }

  if (to_cancel.empty())
    return;

  for (auto iter = op.source_to_obj.begin();
       iter != op.source_to_obj.end();) {
    auto &[pg_shard, hoid_set] = *iter;
    for (auto &hoid : hoid_set) {
      if (to_cancel.contains(hoid)) {
        hoid_set.erase(hoid);
      }
    }
    if (hoid_set.empty()) {
      op.source_to_obj.erase(iter++);
    } else {
      ceph_assert(!osdmap->is_down(pg_shard.osd));
      ++iter;
    }
  }

  for (auto hoid : to_cancel) {
    get_parent()->cancel_pull(hoid);

    ceph_assert(op.to_read.contains(hoid));
    op.to_read.erase(hoid);
    op.complete.erase(hoid);
    on_erase(hoid);
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
