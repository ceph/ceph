// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013,2014 Inktank Storage, Inc.
 * Copyright (C) 2013,2014 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef PGBACKEND_H
#define PGBACKEND_H

#include "ECCommon.h"
#include "osd_types.h"
#include "pg_features.h"
#include "common/intrusive_timer.h"
#include "common/WorkQueue.h"
#include "include/Context.h"
#include "os/ObjectStore.h"
#include "common/LogClient.h"
#include <string>
#include "PGTransaction.h"
#include "common/ostream_temp.h"

namespace Scrub {
  class Store;
}
struct shard_info_wrapper;
struct inconsistent_obj_wrapper;

//forward declaration
class OSDMap;
class PGLog;
typedef std::shared_ptr<const OSDMap> OSDMapRef;

 /**
  * PGBackend
  *
  * PGBackend defines an interface for logic handling IO and
  * replication on RADOS objects.  The PGBackend implementation
  * is responsible for:
  *
  * 1) Handling client operations
  * 2) Handling object recovery
  * 3) Handling object access
  * 4) Handling scrub, deep-scrub, repair
  */
 class PGBackend {
 public:
  virtual int object_stat(const hobject_t &hoid, struct stat* st) { return -1;};
   CephContext* cct;
 protected:
   ObjectStore *store;
   const coll_t coll;
   ObjectStore::CollectionHandle &ch;
 public:
   /**
    * Provides interfaces for PGBackend callbacks
    *
    * The intention is that the parent calls into the PGBackend
    * implementation holding a lock and that the callbacks are
    * called under the same locks.
    */
   class Listener {
   public:
     /// Debugging
     virtual DoutPrefixProvider *get_dpp() = 0;

     /// Recovery

     /**
      * Called with the transaction recovering oid
      */
     virtual void on_local_recover(
       const hobject_t &oid,
       const ObjectRecoveryInfo &recovery_info,
       ObjectContextRef obc,
       bool is_delete,
       ObjectStore::Transaction *t
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

     virtual void apply_stats(
       const hobject_t &soid,
       const object_stat_sum_t &delta_stats) = 0;

     /**
      * Called when a read from a std::set of replicas/primary fails
      */
     virtual void on_failed_pull(
       const std::set<pg_shard_t> &from,
       const hobject_t &soid,
       const eversion_t &v
       ) = 0;

     /**
      * Called when a pull on soid cannot be completed due to
      * down peers
      */
     virtual void cancel_pull(
       const hobject_t &soid) = 0;

     /**
      * Called to remove an object.
      */
     virtual void remove_missing_object(
       const hobject_t &oid,
       eversion_t v,
       Context *on_complete) = 0;

     /**
      * pg_lock, pg_unlock, pg_add_ref, pg_dec_ref
      *
      * Utilities for locking and manipulating refcounts on
      * implementation.
      */
     virtual void pg_lock() = 0;
     virtual void pg_unlock() = 0;
     virtual void pg_add_ref() = 0;
     virtual void pg_dec_ref() = 0;

     /**
      * Bless a context
      *
      * Wraps a context in whatever outer layers the parent usually
      * uses to call into the PGBackend
      */
     virtual Context *bless_context(Context *c) = 0;
     virtual GenContext<ThreadPool::TPHandle&> *bless_gencontext(
       GenContext<ThreadPool::TPHandle&> *c) = 0;
     virtual GenContext<ThreadPool::TPHandle&> *bless_unlocked_gencontext(
       GenContext<ThreadPool::TPHandle&> *c) = 0;

     virtual void send_message(int to_osd, Message *m) = 0;
     virtual void queue_transaction(
       ObjectStore::Transaction&& t,
       OpRequestRef op = OpRequestRef()
       ) = 0;
     virtual void queue_transactions(
       std::vector<ObjectStore::Transaction>& tls,
       OpRequestRef op = OpRequestRef()
       ) = 0;
     virtual epoch_t get_interval_start_epoch() const = 0;
     virtual epoch_t get_last_peering_reset_epoch() const = 0;

     virtual const std::set<pg_shard_t> &get_acting_recovery_backfill_shards() const = 0;
     virtual const std::set<pg_shard_t> &get_acting_shards() const = 0;
     virtual const std::set<pg_shard_t> &get_backfill_shards() const = 0;

     virtual std::ostream& gen_dbg_prefix(std::ostream& out) const = 0;

     virtual const std::map<hobject_t, std::set<pg_shard_t>> &get_missing_loc_shards()
       const = 0;

     virtual const pg_missing_tracker_t &get_local_missing() const = 0;
     virtual void add_local_next_event(const pg_log_entry_t& e) = 0;
     virtual const std::map<pg_shard_t, pg_missing_t> &get_shard_missing()
       const = 0;
     virtual const pg_missing_const_i &get_shard_missing(pg_shard_t peer) const = 0;

     virtual const std::map<pg_shard_t, pg_info_t> &get_shard_info() const = 0;
     virtual const pg_info_t &get_shard_info(pg_shard_t peer) const {
       if (peer == primary_shard()) {
	 return get_info();
       } else {
	 std::map<pg_shard_t, pg_info_t>::const_iterator i =
	   get_shard_info().find(peer);
	 ceph_assert(i != get_shard_info().end());
	 return i->second;
       }
     }

     virtual const PGLog &get_log() const = 0;
     virtual bool pgb_is_primary() const = 0;
     virtual const OSDMapRef& pgb_get_osdmap() const = 0;
     virtual epoch_t pgb_get_osdmap_epoch() const = 0;
     virtual const pg_info_t &get_info() const = 0;
     virtual const pg_pool_t &get_pool() const = 0;
     virtual eversion_t get_pg_committed_to() const = 0;

     virtual ObjectContextRef get_obc(
       const hobject_t &hoid,
       const std::map<std::string, ceph::buffer::list, std::less<>> &attrs) = 0;

     virtual bool try_lock_for_read(
       const hobject_t &hoid,
       ObcLockManager &manager) = 0;

     virtual void release_locks(ObcLockManager &manager) = 0;

     virtual void op_applied(
       const eversion_t &applied_version) = 0;

     virtual bool should_send_op(
       pg_shard_t peer,
       const hobject_t &hoid) = 0;

     virtual bool pg_is_undersized() const = 0;
     virtual bool pg_is_repair() const = 0;

     virtual void log_operation(
       std::vector<pg_log_entry_t>&& logv,
       const std::optional<pg_hit_set_history_t> &hset_history,
       const eversion_t &trim_to,
       const eversion_t &roll_forward_to,
       const eversion_t &pg_committed_to,
       bool transaction_applied,
       ObjectStore::Transaction &t,
       bool async = false) = 0;

     virtual void pgb_set_object_snap_mapping(
       const hobject_t &soid,
       const std::set<snapid_t> &snaps,
       ObjectStore::Transaction *t) = 0;

     virtual void pgb_clear_object_snap_mapping(
       const hobject_t &soid,
       ObjectStore::Transaction *t) = 0;

     virtual void update_peer_last_complete_ondisk(
       pg_shard_t fromosd,
       eversion_t lcod) = 0;

     virtual void update_last_complete_ondisk(
       eversion_t lcod) = 0;

     virtual void update_pct(
       eversion_t pct) = 0;

     virtual void update_stats(
       const pg_stat_t &stat) = 0;

     virtual void schedule_recovery_work(
       GenContext<ThreadPool::TPHandle&> *c,
       uint64_t cost) = 0;

     virtual common::intrusive_timer &get_pg_timer() = 0;

     virtual pg_shard_t whoami_shard() const = 0;
     int whoami() const {
       return whoami_shard().osd;
     }
     spg_t whoami_spg_t() const {
       return get_info().pgid;
     }

     virtual spg_t primary_spg_t() const = 0;
     virtual pg_shard_t primary_shard() const = 0;
     virtual uint64_t min_peer_features() const = 0;
     virtual uint64_t min_upacting_features() const = 0;
     virtual pg_feature_vec_t get_pg_acting_features() const = 0;
     virtual hobject_t get_temp_recovery_object(const hobject_t& target,
						eversion_t version) = 0;

      virtual void send_message_osd_cluster(
       int peer, Message *m, epoch_t from_epoch) = 0;
      virtual void send_message_osd_cluster(
       std::vector<std::pair<int, Message*>>& messages, epoch_t from_epoch) = 0;
     virtual void send_message_osd_cluster(
       MessageRef, Connection *con) = 0;
     virtual void send_message_osd_cluster(
       Message *m, const ConnectionRef& con) = 0;
     virtual ConnectionRef get_con_osd_cluster(int peer, epoch_t from_epoch) = 0;
     virtual entity_name_t get_cluster_msgr_name() = 0;

     virtual PerfCounters *get_logger() = 0;

     virtual ceph_tid_t get_tid() = 0;

     virtual OstreamTemp clog_error() = 0;
     virtual OstreamTemp clog_warn() = 0;

     virtual bool check_failsafe_full() = 0;

     virtual void inc_osd_stat_repaired() = 0;
     virtual bool pg_is_remote_backfilling() = 0;
     virtual void pg_add_local_num_bytes(int64_t num_bytes) = 0;
     virtual void pg_sub_local_num_bytes(int64_t num_bytes) = 0;
     virtual void pg_add_num_bytes(int64_t num_bytes) = 0;
     virtual void pg_sub_num_bytes(int64_t num_bytes) = 0;
     virtual bool maybe_preempt_replica_scrub(const hobject_t& oid) = 0;
     virtual struct ECListener *get_eclistener() = 0;
     virtual ~Listener() {}
   };
   Listener *parent;
   Listener *get_parent() const { return parent; }
   PGBackend(CephContext* cct, Listener *l, ObjectStore *store, const coll_t &coll,
	     ObjectStore::CollectionHandle &ch) :
     cct(cct),
     store(store),
     coll(coll),
     ch(ch),
     parent(l) {}
   bool is_primary() const { return get_parent()->pgb_is_primary(); }
   const OSDMapRef& get_osdmap() const { return get_parent()->pgb_get_osdmap(); }
   epoch_t get_osdmap_epoch() const { return get_parent()->pgb_get_osdmap_epoch(); }
   const pg_info_t &get_info() { return get_parent()->get_info(); }

   std::ostream& gen_prefix(std::ostream& out) const {
     return parent->gen_dbg_prefix(out);
   }

   /**
    * RecoveryHandle
    *
    * We may want to recover multiple objects in the same std::set of
    * messages.  RecoveryHandle is an interface for the opaque
    * object used by the implementation to store the details of
    * the pending recovery operations.
    */
   struct RecoveryHandle {
     bool cache_dont_need;
     std::map<pg_shard_t, std::vector<std::pair<hobject_t, eversion_t> > > deletes;

     RecoveryHandle(): cache_dont_need(false) {}
     virtual ~RecoveryHandle() {}
   };

   /// Get a fresh recovery operation
   virtual RecoveryHandle *open_recovery_op() = 0;

   /// run_recovery_op: finish the operation represented by h
   virtual void run_recovery_op(
     RecoveryHandle *h,     ///< [in] op to finish
     int priority           ///< [in] msg priority
     ) = 0;

   void recover_delete_object(const hobject_t &oid, eversion_t v,
			      RecoveryHandle *h);
   void send_recovery_deletes(int prio,
			      const std::map<pg_shard_t, std::vector<std::pair<hobject_t, eversion_t> > > &deletes);

   /**
    * recover_object
    *
    * Triggers a recovery operation on the specified hobject_t
    * onreadable must be called before onwriteable
    *
    * On each replica (primary included), get_parent()->on_not_missing()
    * must be called when the transaction finalizing the recovery
    * is queued.  Similarly, get_parent()->on_readable() must be called
    * when the transaction is applied in the backing store.
    *
    * get_parent()->on_not_degraded() should be called on the primary
    * when writes can resume on the object.
    *
    * obc may be NULL if the primary lacks the object.
    *
    * head may be NULL only if the head/snapdir is missing
    *
    * @param missing [in] std::set of info, missing pairs for queried nodes
    * @param overlaps [in] mapping of object to file offset overlaps
    */
   virtual int recover_object(
     const hobject_t &hoid, ///< [in] object to recover
     eversion_t v,          ///< [in] version to recover
     ObjectContextRef head,  ///< [in] context of the head/snapdir object
     ObjectContextRef obc,  ///< [in] context of the object
     RecoveryHandle *h      ///< [in,out] handle to attach recovery op to
     ) = 0;

   /**
    * true if PGBackend can handle this message while inactive
    *
    * If it returns true, handle_message *must* also return true
    */
   virtual bool can_handle_while_inactive(OpRequestRef op) = 0;

   /// gives PGBackend a crack at an incoming message
   bool handle_message(
     OpRequestRef op ///< [in] message received
     ); ///< @return true if the message was handled

   /// the variant of handle_message that is overridden by child classes
   virtual bool _handle_message(OpRequestRef op) = 0;

   virtual void check_recovery_sources(const OSDMapRef& osdmap) = 0;


   /**
    * clean up any temporary on-disk state due to a pg interval change
    */
   void on_change_cleanup(ObjectStore::Transaction *t);
   /**
    * implementation should clear itself, contexts blessed prior to on_change
    * won't be called after on_change()
    */
   virtual void on_change() = 0;
   virtual void clear_recovery_state() = 0;

   virtual IsPGRecoverablePredicate *get_is_recoverable_predicate() const = 0;
   virtual IsPGReadablePredicate *get_is_readable_predicate() const = 0;
   virtual int get_ec_data_chunk_count() const { return 0; };
   virtual int get_ec_stripe_chunk_size() const { return 0; };

   virtual void dump_recovery_info(ceph::Formatter *f) const = 0;

 private:
   std::set<hobject_t> temp_contents;
 public:
   // Track contents of temp collection, clear on reset
   void add_temp_obj(const hobject_t &oid) {
     temp_contents.insert(oid);
   }
   void add_temp_objs(const std::set<hobject_t> &oids) {
     temp_contents.insert(oids.begin(), oids.end());
   }
   void clear_temp_obj(const hobject_t &oid) {
     temp_contents.erase(oid);
   }
   void clear_temp_objs(const std::set<hobject_t> &oids) {
     for (std::set<hobject_t>::const_iterator i = oids.begin();
	  i != oids.end();
	  ++i) {
       temp_contents.erase(*i);
     }
   }

   virtual ~PGBackend() {}

   /// execute implementation specific transaction
   virtual void submit_transaction(
     const hobject_t &hoid,               ///< [in] object
     const object_stat_sum_t &delta_stats,///< [in] stat change
     const eversion_t &at_version,        ///< [in] version
     PGTransactionUPtr &&t,               ///< [in] trans to execute (move)
     const eversion_t &trim_to,           ///< [in] trim log to here
     const eversion_t &pg_committed_to,   ///< [in] lower bound on
                                          ///       committed version
     std::vector<pg_log_entry_t>&& log_entries, ///< [in] log entries for t
     /// [in] hitset history (if updated with this transaction)
     std::optional<pg_hit_set_history_t> &hset_history,
     Context *on_all_commit,              ///< [in] called when all commit
     ceph_tid_t tid,                      ///< [in] tid
     osd_reqid_t reqid,                   ///< [in] reqid
     OpRequestRef op                      ///< [in] op
     ) = 0;

   /// submit callback to be called in order with pending writes
   virtual void call_write_ordered(std::function<void(void)> &&cb) = 0;

   void try_stash(
     const hobject_t &hoid,
     version_t v,
     ObjectStore::Transaction *t);

   void rollback(
     const pg_log_entry_t &entry,
     ObjectStore::Transaction *t);

   friend class LRBTrimmer;
   void rollforward(
     const pg_log_entry_t &entry,
     ObjectStore::Transaction *t);

   void trim(
     const pg_log_entry_t &entry,
     ObjectStore::Transaction *t);

   void remove(
     const hobject_t &hoid,
     ObjectStore::Transaction *t);

 protected:

   void handle_recovery_delete(OpRequestRef op);
   void handle_recovery_delete_reply(OpRequestRef op);

   /// Reapply old attributes
   void rollback_setattrs(
     const hobject_t &hoid,
     std::map<std::string, std::optional<ceph::buffer::list> > &old_attrs,
     ObjectStore::Transaction *t);

   /// Truncate object to rollback append
   virtual void rollback_append(
     const hobject_t &hoid,
     uint64_t old_size,
     ObjectStore::Transaction *t);

   /// Unstash object to rollback stash
   void rollback_stash(
     const hobject_t &hoid,
     version_t old_version,
     ObjectStore::Transaction *t);

   /// Unstash object to rollback stash
   void rollback_try_stash(
     const hobject_t &hoid,
     version_t old_version,
     ObjectStore::Transaction *t);

   /// Delete object to rollback create
   void rollback_create(
     const hobject_t &hoid,
     ObjectStore::Transaction *t) {
     remove(hoid, t);
   }

   /// Clone the extents back into place
   void rollback_extents(
     version_t gen,
     const std::vector<std::pair<uint64_t, uint64_t> > &extents,
     const hobject_t &hoid,
     ObjectStore::Transaction *t);
 public:

   /// Trim object stashed at version
   void trim_rollback_object(
     const hobject_t &hoid,
     version_t gen,
     ObjectStore::Transaction *t);

   /// Std::list objects in collection
   int objects_list_partial(
     const hobject_t &begin,
     int min,
     int max,
     std::vector<hobject_t> *ls,
     hobject_t *next);

   int objects_list_range(
     const hobject_t &start,
     const hobject_t &end,
     std::vector<hobject_t> *ls,
     std::vector<ghobject_t> *gen_obs=0);

   int objects_get_attr(
     const hobject_t &hoid,
     const std::string &attr,
     ceph::buffer::list *out);

   virtual int objects_get_attrs(
     const hobject_t &hoid,
     std::map<std::string, ceph::buffer::list, std::less<>> *out);

   virtual int objects_read_sync(
     const hobject_t &hoid,
     uint64_t off,
     uint64_t len,
     uint32_t op_flags,
     ceph::buffer::list *bl) = 0;

   virtual int objects_readv_sync(
     const hobject_t &hoid,
     std::map<uint64_t, uint64_t>& m,
     uint32_t op_flags,
     ceph::buffer::list *bl) {
     return -EOPNOTSUPP;
   }

   virtual void objects_read_async(
     const hobject_t &hoid,
     const std::list<std::pair<ECCommon::ec_align_t,
		std::pair<ceph::buffer::list*, Context*> > > &to_read,
     Context *on_complete, bool fast_read = false) = 0;

   virtual bool auto_repair_supported() const = 0;
   int be_scan_list(
     ScrubMap &map,
     ScrubMapBuilder &pos);

   virtual uint64_t be_get_ondisk_size(
     uint64_t logical_size) const = 0;

   virtual int be_deep_scrub(
     const hobject_t &oid,
     ScrubMap &map,
     ScrubMapBuilder &pos,
     ScrubMap::object &o) = 0;

   static PGBackend *build_pg_backend(
     const pg_pool_t &pool,
     const std::map<std::string,std::string>& profile,
     Listener *l,
     coll_t coll,
     ObjectStore::CollectionHandle &ch,
     ObjectStore *store,
     CephContext *cct);
};

#endif
