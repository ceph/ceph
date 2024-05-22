// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_REPLICATEDPG_H
#define CEPH_REPLICATEDPG_H

#include <boost/tuple/tuple.hpp>
#include "include/ceph_assert.h"
#include "DynamicPerfStats.h"
#include "OSD.h"
#include "PG.h"
#include "Watch.h"
#include "TierAgentState.h"
#include "messages/MOSDOpReply.h"
#include "common/Checksummer.h"
#include "common/sharedptr_registry.hpp"
#include "common/shared_cache.hpp"
#include "ReplicatedBackend.h"
#include "ECBackend.h"
#include "PGTransaction.h"
#include "cls/cas/cls_cas_ops.h"

class CopyFromCallback;
class PromoteCallback;
struct RefCountCallback;

class PrimaryLogPG;
class PGLSFilter;
class HitSet;
struct TierAgentState;
class OSDService;

void intrusive_ptr_add_ref(PrimaryLogPG *pg);
void intrusive_ptr_release(PrimaryLogPG *pg);
uint64_t get_with_id(PrimaryLogPG *pg);
void put_with_id(PrimaryLogPG *pg, uint64_t id);

#ifdef PG_DEBUG_REFS
  typedef TrackedIntPtr<PrimaryLogPG> PrimaryLogPGRef;
#else
  typedef boost::intrusive_ptr<PrimaryLogPG> PrimaryLogPGRef;
#endif

struct inconsistent_snapset_wrapper;

class PrimaryLogPG : public PG,
		     public PGBackend::Listener,
		     public ECListener {
  friend class OSD;
  friend class Watch;
  friend class PrimaryLogScrub;

public:
  MEMPOOL_CLASS_HELPERS();

  /*
   * state associated with a copy operation
   */
  struct OpContext;
  class CopyCallback;

  /**
   * CopyResults stores the object metadata of interest to a copy initiator.
   */
  struct CopyResults {
    ceph::real_time mtime; ///< the copy source's mtime
    uint64_t object_size; ///< the copied object's size
    bool started_temp_obj; ///< true if the callback needs to delete temp object
    hobject_t temp_oid;    ///< temp object (if any)

    /**
     * Function to fill in transaction; if non-empty the callback
     * must execute it before any other accesses to the object
     * (in order to complete the copy).
     */
    std::function<void(PGTransaction *)> fill_in_final_tx;

    version_t user_version; ///< The copy source's user version
    bool should_requeue;  ///< op should be requeued on cancel
    std::vector<snapid_t> snaps;  ///< src's snaps (if clone)
    snapid_t snap_seq;       ///< src's snap_seq (if head)
    librados::snap_set_t snapset; ///< src snapset (if head)
    bool mirror_snapset;
    bool has_omap;
    uint32_t flags;    // object_copy_data_t::FLAG_*
    uint32_t source_data_digest, source_omap_digest;
    uint32_t data_digest, omap_digest;
    mempool::osd_pglog::vector<std::pair<osd_reqid_t, version_t> > reqids; // [(reqid, user_version)]
    mempool::osd_pglog::map<uint32_t, int> reqid_return_codes; // std::map reqids by index to error code
    std::map<std::string, ceph::buffer::list, std::less<>> attrs; // xattrs
    uint64_t truncate_seq;
    uint64_t truncate_size;
    bool is_data_digest() {
      return flags & object_copy_data_t::FLAG_DATA_DIGEST;
    }
    bool is_omap_digest() {
      return flags & object_copy_data_t::FLAG_OMAP_DIGEST;
    }
    CopyResults()
      : object_size(0), started_temp_obj(false),
	user_version(0),
	should_requeue(false), mirror_snapset(false),
	has_omap(false),
	flags(0),
	source_data_digest(-1), source_omap_digest(-1),
	data_digest(-1), omap_digest(-1),
	truncate_seq(0), truncate_size(0)
    {}
  };

  struct CopyOp;
  typedef std::shared_ptr<CopyOp> CopyOpRef;

  struct CopyOp {
    CopyCallback *cb;
    ObjectContextRef obc;
    hobject_t src;
    object_locator_t oloc;
    unsigned flags;
    bool mirror_snapset;

    CopyResults results;

    ceph_tid_t objecter_tid;
    ceph_tid_t objecter_tid2;

    object_copy_cursor_t cursor;
    std::map<std::string,ceph::buffer::list,std::less<>> attrs;
    ceph::buffer::list data;
    ceph::buffer::list omap_header;
    ceph::buffer::list omap_data;
    int rval;

    object_copy_cursor_t temp_cursor;

    /*
     * For CopyOp the process is:
     * step1: read the data(attr/omap/data) from the source object
     * step2: handle those data(w/ those data create a new object)
     * src_obj_fadvise_flags used in step1;
     * dest_obj_fadvise_flags used in step2
     */
    unsigned src_obj_fadvise_flags;
    unsigned dest_obj_fadvise_flags;

    std::map<uint64_t, CopyOpRef> chunk_cops;
    int num_chunk;
    bool failed;
    uint64_t start_offset = 0;
    uint64_t last_offset = 0;
    std::vector<OSDOp> chunk_ops;

    CopyOp(CopyCallback *cb_, ObjectContextRef _obc, hobject_t s,
	   object_locator_t l,
           version_t v,
	   unsigned f,
	   bool ms,
	   unsigned src_obj_fadvise_flags,
	   unsigned dest_obj_fadvise_flags)
      : cb(cb_), obc(_obc), src(s), oloc(l), flags(f),
	mirror_snapset(ms),
	objecter_tid(0),
	objecter_tid2(0),
	rval(-1),
	src_obj_fadvise_flags(src_obj_fadvise_flags),
	dest_obj_fadvise_flags(dest_obj_fadvise_flags),
	num_chunk(0),
	failed(false)
    {
      results.user_version = v;
      results.mirror_snapset = mirror_snapset;
    }
  };

  /**
   * The CopyCallback class defines an interface for completions to the
   * copy_start code. Users of the copy infrastructure must implement
   * one and give an instance of the class to start_copy.
   *
   * The implementer is responsible for making sure that the CopyCallback
   * can associate itself with the correct copy operation.
   */
  typedef boost::tuple<int, CopyResults*> CopyCallbackResults;

  friend class CopyFromCallback;
  friend struct CopyFromFinisher;
  friend class PromoteCallback;
  friend struct PromoteFinisher;
  friend struct C_gather;
  
  struct ProxyReadOp {
    OpRequestRef op;
    hobject_t soid;
    ceph_tid_t objecter_tid;
    std::vector<OSDOp> &ops;
    version_t user_version;
    int data_offset;
    bool canceled;              ///< true if canceled

    ProxyReadOp(OpRequestRef _op, hobject_t oid, std::vector<OSDOp>& _ops)
      : op(_op), soid(oid),
        objecter_tid(0), ops(_ops),
	user_version(0), data_offset(0),
	canceled(false) { }
  };
  typedef std::shared_ptr<ProxyReadOp> ProxyReadOpRef;

  struct ProxyWriteOp {
    OpContext *ctx;
    OpRequestRef op;
    hobject_t soid;
    ceph_tid_t objecter_tid;
    std::vector<OSDOp> &ops;
    version_t user_version;
    bool sent_reply;
    utime_t mtime;
    bool canceled;
    osd_reqid_t reqid;

    ProxyWriteOp(OpRequestRef _op, hobject_t oid, std::vector<OSDOp>& _ops, osd_reqid_t _reqid)
      : ctx(NULL), op(_op), soid(oid),
        objecter_tid(0), ops(_ops),
	user_version(0), sent_reply(false),
	canceled(false),
        reqid(_reqid) { }
  };
  typedef std::shared_ptr<ProxyWriteOp> ProxyWriteOpRef;

  struct FlushOp {
    ObjectContextRef obc;       ///< obc we are flushing
    OpRequestRef op;            ///< initiating op
    std::list<OpRequestRef> dup_ops; ///< bandwagon jumpers
    version_t flushed_version;  ///< user version we are flushing
    ceph_tid_t objecter_tid;    ///< copy-from request tid
    int rval;                   ///< copy-from result
    bool blocking;              ///< whether we are blocking updates
    bool removal;               ///< we are removing the backend object
    std::optional<std::function<void()>> on_flush; ///< callback, may be null
    // for chunked object
    std::map<uint64_t, int> io_results;
    std::map<uint64_t, ceph_tid_t> io_tids;
    uint64_t chunks;

    FlushOp()
      : flushed_version(0), objecter_tid(0), rval(0),
	blocking(false), removal(false), chunks(0) {}
    ~FlushOp() { ceph_assert(!on_flush); }
  };
  typedef std::shared_ptr<FlushOp> FlushOpRef;

  struct CLSGatherOp {
    OpContext *ctx = nullptr;
    ObjectContextRef obc;
    OpRequestRef op;
    std::vector<ceph_tid_t> objecter_tids;
    int rval = 0;

    CLSGatherOp(OpContext *ctx_, ObjectContextRef obc_, OpRequestRef op_)
      : ctx(ctx_), obc(obc_), op(op_)  {}
    CLSGatherOp() {}
    ~CLSGatherOp() {}
  };

  friend struct RefCountCallback;
  struct ManifestOp {
    RefCountCallback *cb = nullptr;
    ceph_tid_t objecter_tid = 0;
    OpRequestRef op;
    std::map<uint64_t, int> results;
    std::map<uint64_t, ceph_tid_t> tids; 
    std::map<hobject_t, std::pair<uint64_t, uint64_t>> chunks;
    uint64_t num_chunks = 0;
    object_manifest_t new_manifest;
    ObjectContextRef obc;
    

    ManifestOp(ObjectContextRef obc, RefCountCallback* cb)
      : cb(cb), obc(obc) {}
    ManifestOp() = delete;
  };
  typedef std::shared_ptr<ManifestOp> ManifestOpRef;
  std::map<hobject_t, ManifestOpRef> manifest_ops;

  boost::scoped_ptr<PGBackend> pgbackend;
  PGBackend *get_pgbackend() override {
    return pgbackend.get();
  }

  const PGBackend *get_pgbackend() const override {
    return pgbackend.get();
  }

  /// Listener methods
  void add_temp_obj(const hobject_t &oid) override { get_pgbackend()->add_temp_obj(oid); }
  void clear_temp_obj(const hobject_t &oid) override { get_pgbackend()->clear_temp_obj(oid); }
  DoutPrefixProvider *get_dpp() override {
    return this;
  }

  void on_local_recover(
    const hobject_t &oid,
    const ObjectRecoveryInfo &recovery_info,
    ObjectContextRef obc,
    bool is_delete,
    ObjectStore::Transaction *t
    ) override;
  void on_peer_recover(
    pg_shard_t peer,
    const hobject_t &oid,
    const ObjectRecoveryInfo &recovery_info
    ) override {
    recovery_state.on_peer_recover(peer, oid, recovery_info.version);
  }
  void begin_peer_recover(
    pg_shard_t peer,
    const hobject_t oid) override {
    recovery_state.begin_peer_recover(peer, oid);
  }
  void on_global_recover(
    const hobject_t &oid,
    const object_stat_sum_t &stat_diff,
    bool is_delete) override;
  void on_failed_pull(
    const std::set<pg_shard_t> &from,
    const hobject_t &soid,
    const eversion_t &version) override;
  void cancel_pull(const hobject_t &soid) override;
  void apply_stats(
    const hobject_t &soid,
    const object_stat_sum_t &delta_stats) override;

  bool primary_error(const hobject_t& soid, eversion_t v);

  void remove_missing_object(const hobject_t &oid,
			     eversion_t v,
			     Context *on_complete) override;

  template<class T> class BlessedGenContext;
  template<class T> class UnlockedBlessedGenContext;
  class BlessedContext;
  Context *bless_context(Context *c) override;

  GenContext<ThreadPool::TPHandle&> *bless_gencontext(
    GenContext<ThreadPool::TPHandle&> *c) override;
  GenContext<ThreadPool::TPHandle&> *bless_unlocked_gencontext(
    GenContext<ThreadPool::TPHandle&> *c) override;

  void send_message(int to_osd, Message *m) override {
    osd->send_message_osd_cluster(to_osd, m, get_osdmap_epoch());
  }
  void queue_transaction(ObjectStore::Transaction&& t,
			 OpRequestRef op) override {
    osd->store->queue_transaction(ch, std::move(t), op);
  }
  void queue_transactions(std::vector<ObjectStore::Transaction>& tls,
			  OpRequestRef op) override {
    osd->store->queue_transactions(ch, tls, op, NULL);
  }
  epoch_t get_interval_start_epoch() const override {
    return info.history.same_interval_since;
  }
  epoch_t get_last_peering_reset_epoch() const override {
    return get_last_peering_reset();
  }
  const std::set<pg_shard_t> &get_acting_recovery_backfill_shards() const override {
    return get_acting_recovery_backfill();
  }
  const std::set<pg_shard_t> &get_acting_shards() const override {
    return recovery_state.get_actingset();
  }
  const std::set<pg_shard_t> &get_backfill_shards() const override {
    return get_backfill_targets();
  }

  std::ostream& gen_dbg_prefix(std::ostream& out) const override {
    return gen_prefix(out);
  }

  const HobjToShardSetMapping& get_missing_loc_shards() const override
  {
    return recovery_state.get_missing_loc().get_missing_locs();
  }
  const std::map<pg_shard_t, pg_missing_t> &get_shard_missing() const override {
    return recovery_state.get_peer_missing();
  }
  const pg_missing_const_i &get_shard_missing(pg_shard_t peer) const override {
    auto m = maybe_get_shard_missing(peer);
    ceph_assert(m);
    return *m;
  }
  const std::map<pg_shard_t, pg_info_t> &get_shard_info() const override {
    return recovery_state.get_peer_info();
  }
  const pg_info_t &get_shard_info(pg_shard_t peer) const override {
    if (peer == primary_shard()) {
      return get_info();
    } else {
      std::map<pg_shard_t, pg_info_t>::const_iterator i =
        get_shard_info().find(peer);
      ceph_assert(i != get_shard_info().end());
      return i->second;
    }
  }
  const pg_missing_tracker_t &get_local_missing() const override {
    return recovery_state.get_pg_log().get_missing();
  }
  const PGLog &get_log() const override {
    return recovery_state.get_pg_log();
  }
  void add_local_next_event(const pg_log_entry_t& e) override {
    recovery_state.add_local_next_event(e);
  }
  bool pgb_is_primary() const override {
    return is_primary();
  }
  const OSDMapRef& pgb_get_osdmap() const override final {
    return get_osdmap();
  }
  epoch_t pgb_get_osdmap_epoch() const override final {
    return get_osdmap_epoch();
  }
  const pg_info_t &get_info() const override {
    return info;
  }
  const pg_pool_t &get_pool() const override {
    return pool.info;
  }

  ObjectContextRef get_obc(
    const hobject_t &hoid,
    const std::map<std::string, ceph::buffer::list, std::less<>> &attrs) override {
    return get_object_context(hoid, true, &attrs);
  }

  bool try_lock_for_read(
    const hobject_t &hoid,
    ObcLockManager &manager) override {
    if (is_missing_object(hoid))
      return false;
    auto obc = get_object_context(hoid, false, nullptr);
    if (!obc)
      return false;
    return manager.try_get_read_lock(hoid, obc);
  }

  void release_locks(ObcLockManager &manager) override {
    release_object_locks(manager);
  }

  void inc_osd_stat_repaired() override {
    osd->inc_osd_stat_repaired();
  }
  bool pg_is_remote_backfilling() override {
    return is_remote_backfilling();
  }
  void pg_add_local_num_bytes(int64_t num_bytes) override {
    add_local_num_bytes(num_bytes);
  }
  void pg_sub_local_num_bytes(int64_t num_bytes) override {
    sub_local_num_bytes(num_bytes);
  }
  void pg_add_num_bytes(int64_t num_bytes) override {
    add_num_bytes(num_bytes);
  }
  void pg_sub_num_bytes(int64_t num_bytes) override {
    sub_num_bytes(num_bytes);
  }

  void pgb_set_object_snap_mapping(
    const hobject_t &soid,
    const std::set<snapid_t> &snaps,
    ObjectStore::Transaction *t) override {
    return update_object_snap_mapping(t, soid, snaps);
  }
  void pgb_clear_object_snap_mapping(
    const hobject_t &soid,
    ObjectStore::Transaction *t) override {
    return clear_object_snap_mapping(t, soid);
  }

  void log_operation(
    std::vector<pg_log_entry_t>&& logv,
    const std::optional<pg_hit_set_history_t> &hset_history,
    const eversion_t &trim_to,
    const eversion_t &roll_forward_to,
    const eversion_t &min_last_complete_ondisk,
    bool transaction_applied,
    ObjectStore::Transaction &t,
    bool async = false) override {
    if (is_primary()) {
      ceph_assert(trim_to <= recovery_state.get_last_update_ondisk());
    }
    if (hset_history) {
      recovery_state.update_hset(*hset_history);
    }
    if (transaction_applied) {
      update_snap_map(logv, t);
    }
    auto last = logv.rbegin();
    if (is_primary() && last != logv.rend()) {
      projected_log.skip_can_rollback_to_to_head();
      projected_log.trim(cct, last->version, nullptr, nullptr, nullptr);
    }
    if (!is_primary() && !is_ec_pg()) {
      replica_clear_repop_obc(logv, t);
    }
    recovery_state.append_log(
      std::move(logv), trim_to, roll_forward_to, min_last_complete_ondisk,
      t, transaction_applied, async);
  }

  void replica_clear_repop_obc(
    const std::vector<pg_log_entry_t> &logv,
    ObjectStore::Transaction &t);

  void op_applied(const eversion_t &applied_version) override;

  bool should_send_op(
    pg_shard_t peer,
    const hobject_t &hoid) override;

  bool pg_is_undersized() const override {
    return is_undersized();
  }

  bool pg_is_repair() const override {
    return is_repair();
  }

  void update_peer_last_complete_ondisk(
    pg_shard_t fromosd,
    eversion_t lcod) override {
    recovery_state.update_peer_last_complete_ondisk(fromosd, lcod);
  }

  void update_last_complete_ondisk(
    eversion_t lcod) override {
    recovery_state.update_last_complete_ondisk(lcod);
  }

  void update_stats(
    const pg_stat_t &stat) override {
    recovery_state.update_stats(
      [&stat](auto &history, auto &stats) {
	stats = stat;
	return false;
      });
  }

  void schedule_recovery_work(
    GenContext<ThreadPool::TPHandle&> *c,
    uint64_t cost) override;

  pg_shard_t whoami_shard() const override {
    return pg_whoami;
  }
  spg_t primary_spg_t() const override {
    return spg_t(info.pgid.pgid, get_primary().shard);
  }
  pg_shard_t primary_shard() const override {
    return get_primary();
  }
  uint64_t min_peer_features() const override {
    return recovery_state.get_min_peer_features();
  }
  uint64_t min_upacting_features() const override {
    return recovery_state.get_min_upacting_features();
  }
  void send_message_osd_cluster(
    int peer, Message *m, epoch_t from_epoch) override {
    osd->send_message_osd_cluster(peer, m, from_epoch);
  }
  void send_message_osd_cluster(
    std::vector<std::pair<int, Message*>>& messages, epoch_t from_epoch) override {
    osd->send_message_osd_cluster(messages, from_epoch);
  }
  void send_message_osd_cluster(
    MessageRef m, Connection *con) override {
    osd->send_message_osd_cluster(std::move(m), con);
  }
  void send_message_osd_cluster(
    Message *m, const ConnectionRef& con) override {
    osd->send_message_osd_cluster(m, con);
  }
  ConnectionRef get_con_osd_cluster(int peer, epoch_t from_epoch) override;
  entity_name_t get_cluster_msgr_name() override {
    return osd->get_cluster_msgr_name();
  }

  PerfCounters *get_logger() override;

  ceph_tid_t get_tid() override { return osd->get_tid(); }

  OstreamTemp clog_error() override { return osd->clog->error(); }
  OstreamTemp clog_warn() override { return osd->clog->warn(); }

  /**
   * a scrub-map arrived from a replica
   */
  void do_replica_scrub_map(OpRequestRef op);

  struct watch_disconnect_t {
    uint64_t cookie;
    entity_name_t name;
    bool send_disconnect;
    watch_disconnect_t(uint64_t c, entity_name_t n, bool sd)
      : cookie(c), name(n), send_disconnect(sd) {}
  };
  void complete_disconnect_watches(
    ObjectContextRef obc,
    const std::list<watch_disconnect_t> &to_disconnect);

  struct OpFinisher {
    virtual ~OpFinisher() {
    }

    virtual int execute() = 0;
  };

  /*
   * Capture all object state associated with an in-progress read or write.
   */
  struct OpContext {
    OpRequestRef op;
    osd_reqid_t reqid;
    std::vector<OSDOp> *ops;

    const ObjectState *obs; // Old objectstate
    const SnapSet *snapset; // Old snapset

    ObjectState new_obs;  // resulting ObjectState
    SnapSet new_snapset;  // resulting SnapSet (in case of a write)
    //pg_stat_t new_stats;  // resulting Stats
    object_stat_sum_t delta_stats;

    bool modify;          // (force) modification (even if op_t is empty)
    bool user_modify;     // user-visible modification
    bool undirty;         // user explicitly un-dirtying this object
    bool cache_operation;     ///< true if this is a cache eviction
    bool ignore_cache;    ///< true if IGNORE_CACHE flag is std::set
    bool ignore_log_op_stats;  // don't log op stats
    bool update_log_only; ///< this is a write that returned an error - just record in pg log for dup detection
    ObjectCleanRegions clean_regions;

    // side effects
    std::list<std::pair<watch_info_t,bool> > watch_connects; ///< new watch + will_ping flag
    std::list<watch_disconnect_t> watch_disconnects; ///< old watch + send_discon
    std::list<notify_info_t> notifies;
    struct NotifyAck {
      std::optional<uint64_t> watch_cookie;
      uint64_t notify_id;
      ceph::buffer::list reply_bl;
      explicit NotifyAck(uint64_t notify_id) : notify_id(notify_id) {}
      NotifyAck(uint64_t notify_id, uint64_t cookie, ceph::buffer::list& rbl)
	: watch_cookie(cookie), notify_id(notify_id) {
	reply_bl = std::move(rbl);
      }
    };
    std::list<NotifyAck> notify_acks;

    uint64_t bytes_written, bytes_read;

    utime_t mtime;
    SnapContext snapc;           // writer snap context
    eversion_t at_version;       // pg's current version pointer
    version_t user_at_version;   // pg's current user version pointer

    /// index of the current subop - only valid inside of do_osd_ops()
    int current_osd_subop_num;
    /// total number of subops processed in this context for cls_cxx_subop_version()
    int processed_subop_count = 0;

    PGTransactionUPtr op_t;
    std::vector<pg_log_entry_t> log;
    std::optional<pg_hit_set_history_t> updated_hset_history;

    interval_set<uint64_t> modified_ranges;
    ObjectContextRef obc;
    ObjectContextRef clone_obc;    // if we created a clone
    ObjectContextRef head_obc;     // if we also update snapset (see trim_object)

    // FIXME: we may want to kill this msgr hint off at some point!
    std::optional<int> data_off = std::nullopt;

    MOSDOpReply *reply;

    PrimaryLogPG *pg;

    int num_read;    ///< count read ops
    int num_write;   ///< count update ops

    mempool::osd_pglog::vector<std::pair<osd_reqid_t, version_t> > extra_reqids;
    mempool::osd_pglog::map<uint32_t, int> extra_reqid_return_codes;

    hobject_t new_temp_oid, discard_temp_oid;  ///< temp objects we should start/stop tracking

    std::list<std::function<void()>> on_applied;
    std::list<std::function<void()>> on_committed;
    std::list<std::function<void()>> on_finish;
    std::list<std::function<void()>> on_success;
    template <typename F>
    void register_on_finish(F &&f) {
      on_finish.emplace_back(std::forward<F>(f));
    }
    template <typename F>
    void register_on_success(F &&f) {
      on_success.emplace_back(std::forward<F>(f));
    }
    template <typename F>
    void register_on_applied(F &&f) {
      on_applied.emplace_back(std::forward<F>(f));
    }
    template <typename F>
    void register_on_commit(F &&f) {
      on_committed.emplace_back(std::forward<F>(f));
    }

    bool sent_reply = false;

    // pending async reads <off, len, op_flags> -> <outbl, outr>
    std::list<std::pair<boost::tuple<uint64_t, uint64_t, unsigned>,
	      std::pair<ceph::buffer::list*, Context*> > > pending_async_reads;
    int inflightreads;
    friend struct OnReadComplete;
    void start_async_reads(PrimaryLogPG *pg);
    void finish_read(PrimaryLogPG *pg);
    bool async_reads_complete() {
      return inflightreads == 0;
    }

    RWState::State lock_type;
    ObcLockManager lock_manager;

    std::map<int, std::unique_ptr<OpFinisher>> op_finishers;

    OpContext(const OpContext& other);
    const OpContext& operator=(const OpContext& other);

    OpContext(OpRequestRef _op, osd_reqid_t _reqid, std::vector<OSDOp>* _ops,
	      ObjectContextRef& obc,
	      PrimaryLogPG *_pg) :
      op(_op), reqid(_reqid), ops(_ops),
      obs(&obc->obs),
      snapset(0),
      new_obs(obs->oi, obs->exists),
      modify(false), user_modify(false), undirty(false), cache_operation(false),
      ignore_cache(false), ignore_log_op_stats(false), update_log_only(false),
      bytes_written(0), bytes_read(0), user_at_version(0),
      current_osd_subop_num(0),
      obc(obc),
      reply(NULL), pg(_pg),
      num_read(0),
      num_write(0),
      sent_reply(false),
      inflightreads(0),
      lock_type(RWState::RWNONE) {
      if (obc->ssc) {
	new_snapset = obc->ssc->snapset;
	snapset = &obc->ssc->snapset;
      }
    }
    OpContext(OpRequestRef _op, osd_reqid_t _reqid,
              std::vector<OSDOp>* _ops, PrimaryLogPG *_pg) :
      op(_op), reqid(_reqid), ops(_ops), obs(NULL), snapset(0),
      modify(false), user_modify(false), undirty(false), cache_operation(false),
      ignore_cache(false), ignore_log_op_stats(false), update_log_only(false),
      bytes_written(0), bytes_read(0), user_at_version(0),
      current_osd_subop_num(0),
      reply(NULL), pg(_pg),
      num_read(0),
      num_write(0),
      inflightreads(0),
      lock_type(RWState::RWNONE) {}
    void reset_obs(ObjectContextRef obc) {
      new_obs = ObjectState(obc->obs.oi, obc->obs.exists);
      if (obc->ssc) {
	new_snapset = obc->ssc->snapset;
	snapset = &obc->ssc->snapset;
      }
    }
    ~OpContext() {
      ceph_assert(!op_t);
      if (reply)
	reply->put();
      for (std::list<std::pair<boost::tuple<uint64_t, uint64_t, unsigned>,
		     std::pair<ceph::buffer::list*, Context*> > >::iterator i =
	     pending_async_reads.begin();
	   i != pending_async_reads.end();
	   pending_async_reads.erase(i++)) {
	delete i->second.second;
      }
    }
    uint64_t get_features() {
      if (op && op->get_req()) {
        return op->get_req()->get_connection()->get_features();
      }
      return -1ull;
    }
  };
  using OpContextUPtr = std::unique_ptr<OpContext>;
  friend struct OpContext;

  /*
   * State on the PG primary associated with the replicated mutation
   */
  class RepGather {
  public:
    hobject_t hoid;
    OpRequestRef op;
    xlist<RepGather*>::item queue_item;
    int nref;

    eversion_t v;
    int r = 0;

    ceph_tid_t rep_tid;

    bool rep_aborted;
    bool all_committed;

    utime_t   start;

    eversion_t          pg_local_last_complete;

    ObcLockManager lock_manager;

    std::list<std::function<void()>> on_committed;
    std::list<std::function<void()>> on_success;
    std::list<std::function<void()>> on_finish;

    RepGather(
      OpContext *c, ceph_tid_t rt,
      eversion_t lc) :
      hoid(c->obc->obs.oi.soid),
      op(c->op),
      queue_item(this),
      nref(1),
      rep_tid(rt),
      rep_aborted(false),
      all_committed(false),
      pg_local_last_complete(lc),
      lock_manager(std::move(c->lock_manager)),
      on_committed(std::move(c->on_committed)),
      on_success(std::move(c->on_success)),
      on_finish(std::move(c->on_finish)) {}

    RepGather(
      ObcLockManager &&manager,
      OpRequestRef &&o,
      std::optional<std::function<void(void)> > &&on_complete,
      ceph_tid_t rt,
      eversion_t lc,
      int r) :
      op(o),
      queue_item(this),
      nref(1),
      r(r),
      rep_tid(rt),
      rep_aborted(false),
      all_committed(false),
      pg_local_last_complete(lc),
      lock_manager(std::move(manager)) {
      if (on_complete) {
	on_success.push_back(std::move(*on_complete));
      }
    }

    RepGather *get() {
      nref++;
      return this;
    }
    void put() {
      ceph_assert(nref > 0);
      if (--nref == 0) {
	delete this;
	//generic_dout(0) << "deleting " << this << dendl;
      }
    }
  };


protected:

  /**
   * Grabs locks for OpContext, should be cleaned up in close_op_ctx
   *
   * @param ctx [in,out] ctx to get locks for
   * @return true on success, false if we are queued
   */
  bool get_rw_locks(bool write_ordered, OpContext *ctx);

  /**
   * Cleans up OpContext
   *
   * @param ctx [in] ctx to clean up
   */
  void close_op_ctx(OpContext *ctx);

  /**
   * Releases locks
   *
   * @param manager [in] manager with locks to release
   *
   * (moved to .cc due to scrubber access)
   */
  void release_object_locks(ObcLockManager &lock_manager);

  // replica ops
  // [primary|tail]
  xlist<RepGather*> repop_queue;

  friend class C_OSD_RepopCommit;
  void repop_all_committed(RepGather *repop);
  void eval_repop(RepGather*);
  void issue_repop(RepGather *repop, OpContext *ctx);
  RepGather *new_repop(
    OpContext *ctx,
    ceph_tid_t rep_tid);
  boost::intrusive_ptr<RepGather> new_repop(
    eversion_t version,
    int r,
    ObcLockManager &&manager,
    OpRequestRef &&op,
    std::optional<std::function<void(void)> > &&on_complete);
  void remove_repop(RepGather *repop);

  OpContextUPtr simple_opc_create(ObjectContextRef obc);
  void simple_opc_submit(OpContextUPtr ctx);

  /**
   * Merge entries atomically into all acting_recovery_backfill osds
   * adjusting missing and recovery state as necessary.
   *
   * Also used to store error log entries for dup detection.
   */
  void submit_log_entries(
    const mempool::osd_pglog::list<pg_log_entry_t> &entries,
    ObcLockManager &&manager,
    std::optional<std::function<void(void)> > &&on_complete,
    OpRequestRef op = OpRequestRef(),
    int r = 0);
  struct LogUpdateCtx {
    boost::intrusive_ptr<RepGather> repop;
    std::set<pg_shard_t> waiting_on;
  };
  void cancel_log_updates();
  std::map<ceph_tid_t, LogUpdateCtx> log_entry_update_waiting_on;


  // hot/cold tracking
  HitSetRef hit_set;        ///< currently accumulating HitSet
  utime_t hit_set_start_stamp;    ///< time the current HitSet started recording


  void hit_set_clear();     ///< discard any HitSet state
  void hit_set_setup();     ///< initialize HitSet state
  void hit_set_create();    ///< create a new HitSet
  void hit_set_persist();   ///< persist hit info
  bool hit_set_apply_log(); ///< apply log entries to update in-memory HitSet
  void hit_set_trim(OpContextUPtr &ctx, unsigned max); ///< discard old HitSets
  void hit_set_in_memory_trim(uint32_t max_in_memory); ///< discard old in memory HitSets
  void hit_set_remove_all();

  hobject_t get_hit_set_current_object(utime_t stamp);
  hobject_t get_hit_set_archive_object(utime_t start,
				       utime_t end,
				       bool using_gmt);

  // agent
  boost::scoped_ptr<TierAgentState> agent_state;

  void agent_setup();       ///< initialize agent state
  bool agent_work(int max) override ///< entry point to do some agent work
  {
    return agent_work(max, max);
  }
  bool agent_work(int max, int agent_flush_quota) override;
  bool agent_maybe_flush(ObjectContextRef& obc);  ///< maybe flush
  bool agent_maybe_evict(ObjectContextRef& obc, bool after_flush);  ///< maybe evict

  void agent_load_hit_sets();  ///< load HitSets, if needed

  /// estimate object atime and temperature
  ///
  /// @param oid [in] object name
  /// @param temperature [out] relative temperature (# consider both access time and frequency)
  void agent_estimate_temp(const hobject_t& oid, int *temperature);

  /// stop the agent
  void agent_stop() override;
  void agent_delay() override;

  /// clear agent state
  void agent_clear() override;

  /// choose (new) agent mode(s), returns true if op is requeued
  bool agent_choose_mode(bool restart = false, OpRequestRef op = OpRequestRef());
  void agent_choose_mode_restart() override;

  /// true if we can send an ondisk/commit for v
  bool already_complete(eversion_t v);

  // projected object info
  SharedLRU<hobject_t, ObjectContext> object_contexts;
  // std::map from oid.snapdir() to SnapSetContext *
  std::map<hobject_t, SnapSetContext*> snapset_contexts;
  ceph::mutex snapset_contexts_lock =
    ceph::make_mutex("PrimaryLogPG::snapset_contexts_lock");

  // debug order that client ops are applied
  std::map<hobject_t, std::map<client_t, ceph_tid_t>> debug_op_order;

  void populate_obc_watchers(ObjectContextRef obc);
  void check_blocklisted_obc_watchers(ObjectContextRef obc);
  void check_blocklisted_watchers() override;
  void get_watchers(std::list<obj_watch_item_t> *ls) override;
  void get_obc_watchers(ObjectContextRef obc, std::list<obj_watch_item_t> &pg_watchers);
public:
  void handle_watch_timeout(WatchRef watch);
protected:

  ObjectContextRef create_object_context(const object_info_t& oi, SnapSetContext *ssc);
  ObjectContextRef get_object_context(
    const hobject_t& soid,
    bool can_create,
    const std::map<std::string, ceph::buffer::list, std::less<>> *attrs = 0
    );

  void context_registry_on_change();
  void object_context_destructor_callback(ObjectContext *obc);
  class C_PG_ObjectContext;

  int find_object_context(const hobject_t& oid,
			  ObjectContextRef *pobc,
			  bool can_create,
			  bool map_snapid_to_clone=false,
			  hobject_t *missing_oid=NULL);

  void add_object_context_to_pg_stat(ObjectContextRef obc, pg_stat_t *stat);

  void get_src_oloc(const object_t& oid, const object_locator_t& oloc, object_locator_t& src_oloc);

  SnapSetContext *get_snapset_context(
    const hobject_t& oid,
    bool can_create,
    const std::map<std::string, ceph::buffer::list, std::less<>> *attrs = 0,
    bool oid_existed = true //indicate this oid whether exsited in backend
    );
  void register_snapset_context(SnapSetContext *ssc) {
    std::lock_guard l(snapset_contexts_lock);
    _register_snapset_context(ssc);
  }
  void _register_snapset_context(SnapSetContext *ssc) {
    ceph_assert(ceph_mutex_is_locked(snapset_contexts_lock));
    if (!ssc->registered) {
      ceph_assert(snapset_contexts.count(ssc->oid) == 0);
      ssc->registered = true;
      snapset_contexts[ssc->oid] = ssc;
    }
  }
  void put_snapset_context(SnapSetContext *ssc);

  std::map<hobject_t, ObjectContextRef> recovering;

  /*
   * Backfill
   *
   * peer_info[backfill_target].last_backfill == info.last_backfill on the peer.
   *
   * objects prior to peer_info[backfill_target].last_backfill
   *   - are on the peer
   *   - are included in the peer stats
   *
   * objects \in (last_backfill, last_backfill_started]
   *   - are on the peer or are in backfills_in_flight
   *   - are not included in pg stats (yet)
   *   - have their stats in pending_backfill_updates on the primary
   */
  std::set<hobject_t> backfills_in_flight;
  std::map<hobject_t, pg_stat_t> pending_backfill_updates;

  void dump_recovery_info(ceph::Formatter *f) const override {
    f->open_array_section("waiting_on_backfill");
    for (std::set<pg_shard_t>::const_iterator p = waiting_on_backfill.begin();
        p != waiting_on_backfill.end(); ++p)
      f->dump_stream("osd") << *p;
    f->close_section();
    f->dump_stream("last_backfill_started") << last_backfill_started;
    {
      f->open_object_section("backfill_info");
      backfill_info.dump(f);
      f->close_section();
    }
    {
      f->open_array_section("peer_backfill_info");
      for (std::map<pg_shard_t, BackfillInterval>::const_iterator pbi =
	     peer_backfill_info.begin();
          pbi != peer_backfill_info.end(); ++pbi) {
        f->dump_stream("osd") << pbi->first;
        f->open_object_section("BackfillInterval");
          pbi->second.dump(f);
        f->close_section();
      }
      f->close_section();
    }
    {
      f->open_array_section("backfills_in_flight");
      for (std::set<hobject_t>::const_iterator i = backfills_in_flight.begin();
	   i != backfills_in_flight.end();
	   ++i) {
	f->dump_stream("object") << *i;
      }
      f->close_section();
    }
    {
      f->open_array_section("recovering");
      for (std::map<hobject_t, ObjectContextRef>::const_iterator i = recovering.begin();
	   i != recovering.end();
	   ++i) {
	f->dump_stream("object") << i->first;
      }
      f->close_section();
    }
    {
      f->open_object_section("pg_backend");
      pgbackend->dump_recovery_info(f);
      f->close_section();
    }
  }

  /// last backfill operation started
  hobject_t last_backfill_started;
  bool new_backfill;

  int prep_object_replica_pushes(const hobject_t& soid, eversion_t v,
				 PGBackend::RecoveryHandle *h,
				 bool *work_started);
  int prep_object_replica_deletes(const hobject_t& soid, eversion_t v,
				  PGBackend::RecoveryHandle *h,
				  bool *work_started);

  void finish_degraded_object(const hobject_t oid);

  // Cancels/resets pulls from peer
  void check_recovery_sources(const OSDMapRef& map) override ;

  int recover_missing(
    const hobject_t& oid,
    eversion_t v,
    int priority,
    PGBackend::RecoveryHandle *h);

  // low level ops

  void _make_clone(
    OpContext *ctx,
    PGTransaction* t,
    ObjectContextRef clone_obc,
    const hobject_t& head, const hobject_t& coid,
    object_info_t *poi);
  void execute_ctx(OpContext *ctx);
  void finish_ctx(OpContext *ctx, int log_op_type, int result=0);
  void reply_ctx(OpContext *ctx, int err);
  void make_writeable(OpContext *ctx);
  void log_op_stats(const OpRequest& op, uint64_t inb, uint64_t outb);

  void write_update_size_and_usage(object_stat_sum_t& stats, object_info_t& oi,
				   interval_set<uint64_t>& modified, uint64_t offset,
				   uint64_t length, bool write_full=false);
  inline void truncate_update_size_and_usage(
    object_stat_sum_t& delta_stats,
    object_info_t& oi,
    uint64_t truncate_size);

  enum class cache_result_t {
    NOOP,
    BLOCKED_FULL,
    BLOCKED_PROMOTE,
    HANDLED_PROXY,
    HANDLED_REDIRECT,
    REPLIED_WITH_EAGAIN,
    BLOCKED_RECOVERY,
  };
  cache_result_t maybe_handle_cache_detail(OpRequestRef op,
					   bool write_ordered,
					   ObjectContextRef obc, int r,
					   hobject_t missing_oid,
					   bool must_promote,
					   bool in_hit_set,
					   ObjectContextRef *promote_obc);
  cache_result_t maybe_handle_manifest_detail(OpRequestRef op,
						     bool write_ordered,
						     ObjectContextRef obc);
  bool maybe_handle_manifest(OpRequestRef op,
			      bool write_ordered,
			      ObjectContextRef obc) {
    return cache_result_t::NOOP != maybe_handle_manifest_detail(
      op,
      write_ordered,
      obc);
  }

  /**
   * This helper function is called from do_op if the ObjectContext lookup fails.
   * @returns true if the caching code is handling the Op, false otherwise.
   */
  bool maybe_handle_cache(OpRequestRef op,
			  bool write_ordered,
			  ObjectContextRef obc, int r,
			  const hobject_t& missing_oid,
			  bool must_promote,
			  bool in_hit_set = false) {
    return cache_result_t::NOOP != maybe_handle_cache_detail(
      op,
      write_ordered,
      obc,
      r,
      missing_oid,
      must_promote,
      in_hit_set,
      nullptr);
  }

  /**
   * This helper function checks if a promotion is needed.
   */
  bool maybe_promote(ObjectContextRef obc,
		     const hobject_t& missing_oid,
		     const object_locator_t& oloc,
		     bool in_hit_set,
		     uint32_t recency,
		     OpRequestRef promote_op,
		     ObjectContextRef *promote_obc = nullptr);
  /**
   * This helper function tells the client to redirect their request elsewhere.
   */
  void do_cache_redirect(OpRequestRef op);
  /**
   * This function attempts to start a promote.  Either it succeeds,
   * or places op on a wait std::list.  If op is null, failure means that
   * this is a noop.  If a future user wants to be able to distinguish
   * these cases, a return value should be added.
   */
  void promote_object(
    ObjectContextRef obc,            ///< [optional] obc
    const hobject_t& missing_object, ///< oid (if !obc)
    const object_locator_t& oloc,    ///< locator for obc|oid
    OpRequestRef op,                 ///< [optional] client op
    ObjectContextRef *promote_obc = nullptr ///< [optional] new obc for object
    );

  int prepare_transaction(OpContext *ctx);
  std::list<std::pair<OpRequestRef, OpContext*> > in_progress_async_reads;
  void complete_read_ctx(int result, OpContext *ctx);

  // pg on-disk content
  void check_local() override;

  void _clear_recovery_state() override;

  bool start_recovery_ops(
    uint64_t max,
    ThreadPool::TPHandle &handle, uint64_t *started) override;

  uint64_t recover_primary(uint64_t max, ThreadPool::TPHandle &handle);
  uint64_t recover_replicas(uint64_t max, ThreadPool::TPHandle &handle,
		            bool *recovery_started);
  hobject_t earliest_peer_backfill() const;
  bool all_peer_done() const;
  /**
   * @param work_started will be std::set to true if recover_backfill got anywhere
   * @returns the number of operations started
   */
  uint64_t recover_backfill(uint64_t max, ThreadPool::TPHandle &handle,
			    bool *work_started);

  /**
   * scan a (hash) range of objects in the current pg
   *
   * @min return at least this many items, unless we are done
   * @max return no more than this many items
   * @bi.begin first item should be >= this value
   * @bi [out] resulting std::map of objects to eversion_t's
   */
  void scan_range(
    int min, int max, BackfillInterval *bi,
    ThreadPool::TPHandle &handle
    );

  /// Update a hash range to reflect changes since the last scan
  void update_range(
    BackfillInterval *bi,        ///< [in,out] interval to update
    ThreadPool::TPHandle &handle ///< [in] tp handle
    );

  int prep_backfill_object_push(
    hobject_t oid, eversion_t v, ObjectContextRef obc,
    std::vector<pg_shard_t> peers,
    PGBackend::RecoveryHandle *h);
  void send_remove_op(const hobject_t& oid, eversion_t v, pg_shard_t peer);


  class C_OSD_AppliedRecoveredObject;
  class C_OSD_CommittedPushedObject;
  class C_OSD_AppliedRecoveredObjectReplica;

  void _applied_recovered_object(ObjectContextRef obc);
  void _applied_recovered_object_replica();
  void _committed_pushed_object(epoch_t epoch, eversion_t lc);
  void recover_got(hobject_t oid, eversion_t v);

  // -- copyfrom --
  std::map<hobject_t, CopyOpRef> copy_ops;

  int do_copy_get(OpContext *ctx, ceph::buffer::list::const_iterator& bp, OSDOp& op,
		  ObjectContextRef& obc);
  int finish_copy_get();

  void fill_in_copy_get_noent(OpRequestRef& op, hobject_t oid,
                              OSDOp& osd_op);

  /**
   * To copy an object, call start_copy.
   *
   * @param cb: The CopyCallback to be activated when the copy is complete
   * @param obc: The ObjectContext we are copying into
   * @param src: The source object
   * @param oloc: the source object locator
   * @param version: the version of the source object to copy (0 for any)
   */
  void start_copy(CopyCallback *cb, ObjectContextRef obc, hobject_t src,
		  object_locator_t oloc, version_t version, unsigned flags,
		  bool mirror_snapset, unsigned src_obj_fadvise_flags,
		  unsigned dest_obj_fadvise_flags);
  void process_copy_chunk(hobject_t oid, ceph_tid_t tid, int r);
  void _write_copy_chunk(CopyOpRef cop, PGTransaction *t);
  uint64_t get_copy_chunk_size() const {
    uint64_t size = cct->_conf->osd_copyfrom_max_chunk;
    if (pool.info.required_alignment()) {
      uint64_t alignment = pool.info.required_alignment();
      if (size % alignment) {
	size += alignment - (size % alignment);
      }
    }
    return size;
  }
  void _copy_some(ObjectContextRef obc, CopyOpRef cop);
  void finish_copyfrom(CopyFromCallback *cb);
  void finish_promote(int r, CopyResults *results, ObjectContextRef obc);
  void cancel_copy(CopyOpRef cop, bool requeue, std::vector<ceph_tid_t> *tids);
  void cancel_copy_ops(bool requeue, std::vector<ceph_tid_t> *tids);

  friend struct C_Copyfrom;

  // -- flush --
  std::map<hobject_t, FlushOpRef> flush_ops;

  /// start_flush takes ownership of on_flush iff ret == -EINPROGRESS
  int start_flush(
    OpRequestRef op, ObjectContextRef obc,
    bool blocking, hobject_t *pmissing,
    std::optional<std::function<void()>> &&on_flush,
    bool force_dedup = false);
  void finish_flush(hobject_t oid, ceph_tid_t tid, int r);
  int try_flush_mark_clean(FlushOpRef fop);
  void cancel_flush(FlushOpRef fop, bool requeue, std::vector<ceph_tid_t> *tids);
  void cancel_flush_ops(bool requeue, std::vector<ceph_tid_t> *tids);

  /// @return false if clone is has been evicted
  bool is_present_clone(hobject_t coid);

  friend struct C_Flush;

  // -- cls_gather --
  std::map<hobject_t, CLSGatherOp> cls_gather_ops;
  void cancel_cls_gather(std::map<hobject_t,CLSGatherOp>::iterator iter, bool requeue, std::vector<ceph_tid_t> *tids);
  void cancel_cls_gather_ops(bool requeue, std::vector<ceph_tid_t> *tids);

  // -- scrub --
  bool _range_available_for_scrub(
    const hobject_t &begin, const hobject_t &end) override;

  void _split_into(pg_t child_pgid, PG *child,
                   unsigned split_bits) override;
  void apply_and_flush_repops(bool requeue);

  int do_xattr_cmp_u64(int op, uint64_t v1, ceph::buffer::list& xattr);
  int do_xattr_cmp_str(int op, std::string& v1s, ceph::buffer::list& xattr);

  // -- checksum --
  int do_checksum(OpContext *ctx, OSDOp& osd_op, ceph::buffer::list::const_iterator *bl_it);
  int finish_checksum(OSDOp& osd_op, Checksummer::CSumType csum_type,
                      ceph::buffer::list::const_iterator *init_value_bl_it,
                      const ceph::buffer::list &read_bl);

  friend struct C_ChecksumRead;

  int do_extent_cmp(OpContext *ctx, OSDOp& osd_op);
  int finish_extent_cmp(OSDOp& osd_op, const ceph::buffer::list &read_bl);

  friend struct C_ExtentCmpRead;

  int do_read(OpContext *ctx, OSDOp& osd_op);
  int do_sparse_read(OpContext *ctx, OSDOp& osd_op);
  int do_writesame(OpContext *ctx, OSDOp& osd_op);

  bool pgls_filter(const PGLSFilter& filter, const hobject_t& sobj);

  std::pair<int, std::unique_ptr<const PGLSFilter>> get_pgls_filter(
    ceph::buffer::list::const_iterator& iter);

  std::map<hobject_t, std::list<OpRequestRef>> in_progress_proxy_ops;
  void kick_proxy_ops_blocked(hobject_t& soid);
  void cancel_proxy_ops(bool requeue, std::vector<ceph_tid_t> *tids);

  // -- proxyread --
  std::map<ceph_tid_t, ProxyReadOpRef> proxyread_ops;

  void do_proxy_read(OpRequestRef op, ObjectContextRef obc = NULL);
  void finish_proxy_read(hobject_t oid, ceph_tid_t tid, int r);
  void cancel_proxy_read(ProxyReadOpRef prdop, std::vector<ceph_tid_t> *tids);

  friend struct C_ProxyRead;

  // -- proxywrite --
  std::map<ceph_tid_t, ProxyWriteOpRef> proxywrite_ops;

  void do_proxy_write(OpRequestRef op, ObjectContextRef obc = NULL);
  void finish_proxy_write(hobject_t oid, ceph_tid_t tid, int r);
  void cancel_proxy_write(ProxyWriteOpRef pwop, std::vector<ceph_tid_t> *tids);

  friend struct C_ProxyWrite_Commit;

  // -- chunkop --
  enum class refcount_t {
    INCREMENT_REF,
    DECREMENT_REF,
    CREATE_OR_GET_REF,
  };
  void do_proxy_chunked_op(OpRequestRef op, const hobject_t& missing_oid,
			   ObjectContextRef obc, bool write_ordered);
  void do_proxy_chunked_read(OpRequestRef op, ObjectContextRef obc, int op_index,
			     uint64_t chunk_index, uint64_t req_offset, uint64_t req_length,
			     uint64_t req_total_len, bool write_ordered);
  bool can_proxy_chunked_read(OpRequestRef op, ObjectContextRef obc);
  void _copy_some_manifest(ObjectContextRef obc, CopyOpRef cop, uint64_t start_offset);
  void process_copy_chunk_manifest(hobject_t oid, ceph_tid_t tid, int r, uint64_t offset);
  void finish_promote_manifest(int r, CopyResults *results, ObjectContextRef obc);
  void cancel_and_requeue_proxy_ops(hobject_t oid);
  void cancel_manifest_ops(bool requeue, std::vector<ceph_tid_t> *tids);
  ceph_tid_t refcount_manifest(hobject_t src_soid, hobject_t tgt_soid, refcount_t type,
			      Context *cb, std::optional<bufferlist> chunk);
  void dec_all_refcount_manifest(const object_info_t& oi, OpContext* ctx);
  void dec_refcount(const hobject_t& soid, const object_ref_delta_t& refs);
  void update_chunk_map_by_dirty(OpContext* ctx);
  void dec_refcount_by_dirty(OpContext* ctx);
  ObjectContextRef get_prev_clone_obc(ObjectContextRef obc);
  bool recover_adjacent_clones(ObjectContextRef obc, OpRequestRef op);
  snapid_t do_recover_adjacent_clones(ObjectContextRef obc, OpRequestRef op);
  void get_adjacent_clones(ObjectContextRef src_obc, 
			   ObjectContextRef& _l, ObjectContextRef& _g);
  bool inc_refcount_by_set(OpContext* ctx, object_manifest_t& tgt,
			   OSDOp& osd_op);
  int do_cdc(const object_info_t& oi, std::map<uint64_t, chunk_info_t>& chunk_map,
	     std::map<uint64_t, bufferlist>& chunks);
  int start_dedup(OpRequestRef op, ObjectContextRef obc);
  std::pair<int, hobject_t> get_fpoid_from_chunk(const hobject_t soid, bufferlist& chunk);
  int finish_set_dedup(hobject_t oid, int r, ceph_tid_t tid, uint64_t offset);
  int finish_set_manifest_refcount(hobject_t oid, int r, ceph_tid_t tid, uint64_t offset);

  friend struct C_ProxyChunkRead;
  friend class PromoteManifestCallback;
  friend struct C_CopyChunk;
  friend struct RefCountCallback;
  friend struct C_SetDedupChunks;
  friend struct C_SetManifestRefCountDone;
  friend struct SetManifestFinisher;

public:
  PrimaryLogPG(OSDService *o, OSDMapRef curmap,
	       const PGPool &_pool,
	       const std::map<std::string,std::string>& ec_profile,
	       spg_t p);
  ~PrimaryLogPG() override;

  void do_command(
    std::string_view prefix,
    const cmdmap_t& cmdmap,
    const ceph::buffer::list& idata,
    std::function<void(int,const std::string&,ceph::buffer::list&)> on_finish) override;

  void clear_cache() override;
  int get_cache_obj_count() override {
    return object_contexts.get_count();
  }
  unsigned get_pg_shard() const {
    return info.pgid.hash_to_shard(osd->get_num_shards());
  }
  void do_request(
    OpRequestRef& op,
    ThreadPool::TPHandle &handle) override;
  void do_op(OpRequestRef& op);
  void record_write_error(OpRequestRef op, const hobject_t &soid,
			  MOSDOpReply *orig_reply, int r,
			  OpContext *ctx_for_op_returns=nullptr);
  void do_pg_op(OpRequestRef op);
  void do_scan(
    OpRequestRef op,
    ThreadPool::TPHandle &handle);
  void do_backfill(OpRequestRef op);
  void do_backfill_remove(OpRequestRef op);

  void handle_backoff(OpRequestRef& op);

  int trim_object(bool first, const hobject_t &coid, snapid_t snap_to_trim,
		  OpContextUPtr *ctxp);
  void snap_trimmer(epoch_t e) override;
  void kick_snap_trim() override;
  void snap_trimmer_scrub_complete() override;
  int do_osd_ops(OpContext *ctx, std::vector<OSDOp>& ops);

  int _get_tmap(OpContext *ctx, ceph::buffer::list *header, ceph::buffer::list *vals);
  int do_tmap2omap(OpContext *ctx, unsigned flags);
  int do_tmapup(OpContext *ctx, ceph::buffer::list::const_iterator& bp, OSDOp& osd_op);
  int do_tmapup_slow(OpContext *ctx, ceph::buffer::list::const_iterator& bp, OSDOp& osd_op, ceph::buffer::list& bl);

  void do_osd_op_effects(OpContext *ctx, const ConnectionRef& conn);
  int start_cls_gather(OpContext *ctx, std::map<std::string, bufferlist> *src_objs, const std::string& pool,
		       const char *cls, const char *method, bufferlist& inbl);

private:
  int do_scrub_ls(const MOSDOp *op, OSDOp *osd_op);
  bool check_src_targ(const hobject_t& soid, const hobject_t& toid) const;

  uint64_t temp_seq; ///< last id for naming temp objects
  /// generate a new temp object name
  hobject_t generate_temp_object(const hobject_t& target);
  /// generate a new temp object name (for recovery)
  hobject_t get_temp_recovery_object(const hobject_t& target,
				     eversion_t version) override;
public:
  coll_t get_coll() {
    return coll;
  }
  void split_colls(
    spg_t child,
    int split_bits,
    int seed,
    const pg_pool_t *pool,
    ObjectStore::Transaction &t) override {
    coll_t target = coll_t(child);
    create_pg_collection(t, child, split_bits);
    t.split_collection(
      coll,
      split_bits,
      seed,
      target);
    init_pg_ondisk(t, child, pool);
  }
private:

  struct DoSnapWork : boost::statechart::event< DoSnapWork > {
    DoSnapWork() : boost::statechart::event < DoSnapWork >() {}
  };
  struct KickTrim : boost::statechart::event< KickTrim > {
    KickTrim() : boost::statechart::event < KickTrim >() {}
  };
  struct RepopsComplete : boost::statechart::event< RepopsComplete > {
    RepopsComplete() : boost::statechart::event < RepopsComplete >() {}
  };
  struct ScrubComplete : boost::statechart::event< ScrubComplete > {
    ScrubComplete() : boost::statechart::event < ScrubComplete >() {}
  };
  struct TrimWriteUnblocked : boost::statechart::event< TrimWriteUnblocked > {
    TrimWriteUnblocked() : boost::statechart::event < TrimWriteUnblocked >() {}
  };
  struct Reset : boost::statechart::event< Reset > {
    Reset() : boost::statechart::event< Reset >() {}
  };
  struct SnapTrimReserved : boost::statechart::event< SnapTrimReserved > {
    SnapTrimReserved() : boost::statechart::event< SnapTrimReserved >() {}
  };
  struct SnapTrimTimerReady : boost::statechart::event< SnapTrimTimerReady > {
    SnapTrimTimerReady() : boost::statechart::event< SnapTrimTimerReady >() {}
  };

  struct NotTrimming;
  struct SnapTrimmer : public boost::statechart::state_machine< SnapTrimmer, NotTrimming > {
    PrimaryLogPG *pg;
    explicit SnapTrimmer(PrimaryLogPG *pg) : pg(pg) {}
    void log_enter(const char *state_name);
    void log_exit(const char *state_name, utime_t duration);
    bool permit_trim();
    bool can_trim() {
      return
	permit_trim() &&
	!pg->get_osdmap()->test_flag(CEPH_OSDMAP_NOSNAPTRIM);
    }
  } snap_trimmer_machine;

  struct WaitReservation;
  struct Trimming : boost::statechart::state< Trimming, SnapTrimmer, WaitReservation >, NamedState {
    typedef boost::mpl::list <
      boost::statechart::custom_reaction< KickTrim >,
      boost::statechart::transition< Reset, NotTrimming >
      > reactions;

    std::set<hobject_t> in_flight;
    snapid_t snap_to_trim;

    explicit Trimming(my_context ctx)
      : my_base(ctx),
	NamedState(nullptr, "Trimming") {
      context< SnapTrimmer >().log_enter(state_name);
      ceph_assert(context< SnapTrimmer >().permit_trim());
      ceph_assert(in_flight.empty());
    }
    void exit() {
      context< SnapTrimmer >().log_exit(state_name, enter_time);
      auto *pg = context< SnapTrimmer >().pg;
      pg->osd->snap_reserver.cancel_reservation(pg->get_pgid());
      pg->state_clear(PG_STATE_SNAPTRIM);
      pg->publish_stats_to_osd();
    }
    boost::statechart::result react(const KickTrim&) {
      return discard_event();
    }
  };

  /* SnapTrimmerStates */
  struct WaitTrimTimer : boost::statechart::state< WaitTrimTimer, Trimming >, NamedState {
    typedef boost::mpl::list <
      boost::statechart::custom_reaction< SnapTrimTimerReady >
      > reactions;
    Context *wakeup = nullptr;
    explicit WaitTrimTimer(my_context ctx)
      : my_base(ctx),
	NamedState(nullptr, "Trimming/WaitTrimTimer") {
      context< SnapTrimmer >().log_enter(state_name);
      ceph_assert(context<Trimming>().in_flight.empty());
      struct OnTimer : Context {
	PrimaryLogPGRef pg;
	epoch_t epoch;
	OnTimer(PrimaryLogPGRef pg, epoch_t epoch) : pg(pg), epoch(epoch) {}
	void finish(int) override {
	  pg->lock();
	  if (!pg->pg_has_reset_since(epoch))
	    pg->snap_trimmer_machine.process_event(SnapTrimTimerReady());
	  pg->unlock();
	}
      };
      auto *pg = context< SnapTrimmer >().pg;
      float osd_snap_trim_sleep = pg->osd->osd->get_osd_snap_trim_sleep();
      if (osd_snap_trim_sleep > 0) {
	std::lock_guard l(pg->osd->sleep_lock);
	wakeup = pg->osd->sleep_timer.add_event_after(
	  osd_snap_trim_sleep,
	  new OnTimer{pg, pg->get_osdmap_epoch()});
      } else {
	post_event(SnapTrimTimerReady());
      }
    }
    void exit() {
      context< SnapTrimmer >().log_exit(state_name, enter_time);
      auto *pg = context< SnapTrimmer >().pg;
      if (wakeup) {
	std::lock_guard l(pg->osd->sleep_lock);
	pg->osd->sleep_timer.cancel_event(wakeup);
	wakeup = nullptr;
      }
    }
    boost::statechart::result react(const SnapTrimTimerReady &) {
      wakeup = nullptr;
      if (!context< SnapTrimmer >().can_trim()) {
	post_event(KickTrim());
	return transit< NotTrimming >();
      } else {
	return transit< AwaitAsyncWork >();
      }
    }
  };

  struct WaitRWLock : boost::statechart::state< WaitRWLock, Trimming >, NamedState {
    typedef boost::mpl::list <
      boost::statechart::custom_reaction< TrimWriteUnblocked >
      > reactions;
    explicit WaitRWLock(my_context ctx)
      : my_base(ctx),
	NamedState(nullptr, "Trimming/WaitRWLock") {
      context< SnapTrimmer >().log_enter(state_name);
      ceph_assert(context<Trimming>().in_flight.empty());
    }
    void exit() {
      context< SnapTrimmer >().log_exit(state_name, enter_time);
    }
    boost::statechart::result react(const TrimWriteUnblocked&) {
      if (!context< SnapTrimmer >().can_trim()) {
	post_event(KickTrim());
	return transit< NotTrimming >();
      } else {
	return transit< AwaitAsyncWork >();
      }
    }
  };

  struct WaitRepops : boost::statechart::state< WaitRepops, Trimming >, NamedState {
    typedef boost::mpl::list <
      boost::statechart::custom_reaction< RepopsComplete >
      > reactions;
    explicit WaitRepops(my_context ctx)
      : my_base(ctx),
	NamedState(nullptr, "Trimming/WaitRepops") {
      context< SnapTrimmer >().log_enter(state_name);
      ceph_assert(!context<Trimming>().in_flight.empty());
    }
    void exit() {
      context< SnapTrimmer >().log_exit(state_name, enter_time);
    }
    boost::statechart::result react(const RepopsComplete&) {
      if (!context< SnapTrimmer >().can_trim()) {
	post_event(KickTrim());
	return transit< NotTrimming >();
      } else {
	return transit< WaitTrimTimer >();
      }
    }
  };

  struct AwaitAsyncWork : boost::statechart::state< AwaitAsyncWork, Trimming >, NamedState {
    typedef boost::mpl::list <
      boost::statechart::custom_reaction< DoSnapWork >
      > reactions;
    explicit AwaitAsyncWork(my_context ctx);
    void exit() {
      context< SnapTrimmer >().log_exit(state_name, enter_time);
    }
    boost::statechart::result react(const DoSnapWork&);
  };

  struct WaitReservation : boost::statechart::state< WaitReservation, Trimming >, NamedState {
    /* WaitReservation is a sub-state of trimming simply so that exiting Trimming
     * always cancels the reservation */
    typedef boost::mpl::list <
      boost::statechart::custom_reaction< SnapTrimReserved >
      > reactions;
    struct ReservationCB : public Context {
      PrimaryLogPGRef pg;
      bool canceled;
      explicit ReservationCB(PrimaryLogPG *pg) : pg(pg), canceled(false) {}
      void finish(int) override {
	pg->lock();
	if (!canceled)
	  pg->snap_trimmer_machine.process_event(SnapTrimReserved());
	pg->unlock();
      }
      void cancel() {
	ceph_assert(pg->is_locked());
	ceph_assert(!canceled);
	canceled = true;
      }
    };
    ReservationCB *pending = nullptr;

    explicit WaitReservation(my_context ctx)
      : my_base(ctx),
	NamedState(nullptr, "Trimming/WaitReservation") {
      context< SnapTrimmer >().log_enter(state_name);
      ceph_assert(context<Trimming>().in_flight.empty());
      auto *pg = context< SnapTrimmer >().pg;
      pending = new ReservationCB(pg);
      pg->osd->snap_reserver.request_reservation(
	pg->get_pgid(),
	pending,
	0);
      pg->state_set(PG_STATE_SNAPTRIM_WAIT);
      pg->publish_stats_to_osd();
    }
    boost::statechart::result react(const SnapTrimReserved&);
    void exit() {
      context< SnapTrimmer >().log_exit(state_name, enter_time);
      if (pending)
	pending->cancel();
      pending = nullptr;
      auto *pg = context< SnapTrimmer >().pg;
      pg->state_clear(PG_STATE_SNAPTRIM_WAIT);
      pg->state_clear(PG_STATE_SNAPTRIM_ERROR);
      pg->publish_stats_to_osd();
    }
  };

  struct WaitScrub : boost::statechart::state< WaitScrub, SnapTrimmer >, NamedState {
    typedef boost::mpl::list <
      boost::statechart::custom_reaction< ScrubComplete >,
      boost::statechart::custom_reaction< KickTrim >,
      boost::statechart::transition< Reset, NotTrimming >
      > reactions;
    explicit WaitScrub(my_context ctx)
      : my_base(ctx),
	NamedState(nullptr, "WaitScrub") {
      context< SnapTrimmer >().log_enter(state_name);
    }
    void exit() {
      context< SnapTrimmer >().log_exit(state_name, enter_time);
    }
    boost::statechart::result react(const ScrubComplete&) {
      post_event(KickTrim());
      return transit< NotTrimming >();
    }
    boost::statechart::result react(const KickTrim&) {
      return discard_event();
    }
  };

  struct NotTrimming : boost::statechart::state< NotTrimming, SnapTrimmer >, NamedState {
    typedef boost::mpl::list <
      boost::statechart::custom_reaction< KickTrim >,
      boost::statechart::transition< Reset, NotTrimming >
      > reactions;
    explicit NotTrimming(my_context ctx);
    void exit();
    boost::statechart::result react(const KickTrim&);
  };

  int _verify_no_head_clones(const hobject_t& soid,
			     const SnapSet& ss);
  // return true if we're creating a local object, false for a
  // whiteout or no change.
  void maybe_create_new_object(OpContext *ctx, bool ignore_transaction=false);
  int _delete_oid(OpContext *ctx, bool no_whiteout, bool try_no_whiteout);
  int _rollback_to(OpContext *ctx, OSDOp& op);
  void _do_rollback_to(OpContext *ctx, ObjectContextRef rollback_to,
				    OSDOp& op);
public:
  bool is_missing_object(const hobject_t& oid) const;
  bool is_unreadable_object(const hobject_t &oid) const {
    return is_missing_object(oid) ||
      !recovery_state.get_missing_loc().readable_with_acting(
	oid, get_actingset());
  }
  void maybe_kick_recovery(const hobject_t &soid);
  void wait_for_unreadable_object(const hobject_t& oid, OpRequestRef op);
  void finish_unreadable_object(const hobject_t oid);

  int get_manifest_ref_count(ObjectContextRef obc, std::string& fp_oid, OpRequestRef op);

  bool check_laggy(OpRequestRef& op);
  bool check_laggy_requeue(OpRequestRef& op);
  void recheck_readable() override;

  bool is_backfill_target(pg_shard_t osd) const {
    return recovery_state.is_backfill_target(osd);
  }
  const std::set<pg_shard_t> &get_backfill_targets() const {
    return recovery_state.get_backfill_targets();
  }
  bool is_async_recovery_target(pg_shard_t peer) const {
    return recovery_state.is_async_recovery_target(peer);
  }
  const std::set<pg_shard_t> &get_async_recovery_targets() const {
    return recovery_state.get_async_recovery_targets();
  }
  bool is_degraded_or_backfilling_object(const hobject_t& oid);
  bool is_degraded_on_async_recovery_target(const hobject_t& soid);
  void wait_for_degraded_object(const hobject_t& oid, OpRequestRef op);

  void block_write_on_full_cache(
    const hobject_t& oid, OpRequestRef op);
  void block_for_clean(
    const hobject_t& oid, OpRequestRef op);
  void block_write_on_snap_rollback(
    const hobject_t& oid, ObjectContextRef obc, OpRequestRef op);
  void block_write_on_degraded_snap(const hobject_t& oid, OpRequestRef op);
  void block_write_on_unreadable_snap(const hobject_t& snap, OpRequestRef op);

  bool maybe_await_blocked_head(const hobject_t &soid, OpRequestRef op);
  void wait_for_blocked_object(const hobject_t& soid, OpRequestRef op);
  void kick_object_context_blocked(ObjectContextRef obc);
  void requeue_op_blocked_by_object(const hobject_t &soid);

  void maybe_force_recovery();

  void mark_all_unfound_lost(
    int what,
    std::function<void(int,const std::string&,ceph::buffer::list&)> on_finish);
  eversion_t pick_newest_available(const hobject_t& oid);

  void do_update_log_missing(
    OpRequestRef &op);

  void do_update_log_missing_reply(
    OpRequestRef &op);

  void plpg_on_role_change() override;
  void plpg_on_pool_change() override;
  void clear_async_reads();
  void on_change(ObjectStore::Transaction &t) override;
  void on_activate_complete() override;
  void on_flushed() override;
  void on_removal(ObjectStore::Transaction &t) override;
  void on_shutdown() override;
  bool check_failsafe_full() override;
  bool maybe_preempt_replica_scrub(const hobject_t& oid) override;
  struct ECListener *get_eclistener() override;
  const pg_missing_const_i * maybe_get_shard_missing(
    pg_shard_t peer) const {
    if (peer == primary_shard()) {
      return &get_local_missing();
    } else {
      std::map<pg_shard_t, pg_missing_t>::const_iterator i =
        get_shard_missing().find(peer);
      if (i == get_shard_missing().end()) {
        return nullptr;
      } else {
        return &(i->second);
      }
    }
  }
  int rep_repair_primary_object(const hobject_t& soid, OpContext *ctx);

  // attr cache handling
  void setattr_maybe_cache(
    ObjectContextRef obc,
    PGTransaction *t,
    const std::string &key,
    ceph::buffer::list &val);
  void setattrs_maybe_cache(
    ObjectContextRef obc,
    PGTransaction *t,
    std::map<std::string, ceph::buffer::list, std::less<>> &attrs);
  void rmattr_maybe_cache(
    ObjectContextRef obc,
    PGTransaction *t,
    const std::string &key);
  /** 
   * getattr_maybe_cache 
   *
   * Populates val (if non-null) with the value of the attr with the specified key. 
   * Returns -ENOENT if object does not exist, -ENODATA if the object exists, 
   * but the specified key does not. 
   */
  int getattr_maybe_cache(
    ObjectContextRef obc,
    const std::string &key,
    ceph::buffer::list *val);
  int getattrs_maybe_cache(
    ObjectContextRef obc,
    std::map<std::string, ceph::buffer::list, std::less<>> *out);

public:
  void set_dynamic_perf_stats_queries(
      const std::list<OSDPerfMetricQuery> &queries)  override;
  void get_dynamic_perf_stats(DynamicPerfStats *stats)  override;

private:
  DynamicPerfStats m_dynamic_perf_stats;
};

inline ostream& operator<<(ostream& out, const PrimaryLogPG::RepGather& repop)
{
  out << "repgather(" << &repop
      << " " << repop.v
      << " rep_tid=" << repop.rep_tid
      << " committed?=" << repop.all_committed
      << " r=" << repop.r
      << ")";
  return out;
}

inline ostream& operator<<(ostream& out,
			   const PrimaryLogPG::ProxyWriteOpRef& pwop)
{
  out << "proxywrite(" << &pwop
      << " " << pwop->user_version
      << " pwop_tid=" << pwop->objecter_tid;
  if (pwop->ctx->op)
    out << " op=" << *(pwop->ctx->op->get_req());
  out << ")";
  return out;
}

void intrusive_ptr_add_ref(PrimaryLogPG::RepGather *repop);
void intrusive_ptr_release(PrimaryLogPG::RepGather *repop);


#endif
