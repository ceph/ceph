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

#include "OSDMap.h"
#include "PGLog.h"
#include "osd_types.h"
#include "common/WorkQueue.h"
#include "include/Context.h"
#include "os/ObjectStore.h"
#include "common/LogClient.h"
#include <string>

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
 protected:
   ObjectStore *store;
   const coll_t coll;
   const coll_t temp_coll;
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
     /// Recovery

     virtual void on_local_recover_start(
       const hobject_t &oid,
       ObjectStore::Transaction *t) = 0;
     /**
      * Called with the transaction recovering oid
      */
     virtual void on_local_recover(
       const hobject_t &oid,
       const object_stat_sum_t &stat_diff,
       const ObjectRecoveryInfo &recovery_info,
       ObjectContextRef obc,
       ObjectStore::Transaction *t
       ) = 0;

     /**
      * Called when transaction recovering oid is durable and
      * applied on all replicas
      */
     virtual void on_global_recover(const hobject_t &oid) = 0;

     /**
      * Called when peer is recovered
      */
     virtual void on_peer_recover(
       pg_shard_t peer,
       const hobject_t &oid,
       const ObjectRecoveryInfo &recovery_info,
       const object_stat_sum_t &stat
       ) = 0;

     virtual void begin_peer_recover(
       pg_shard_t peer,
       const hobject_t oid) = 0;

     virtual void failed_push(pg_shard_t from, const hobject_t &soid) = 0;
     
     virtual void cancel_pull(const hobject_t &soid) = 0;

     /**
      * Bless a context
      *
      * Wraps a context in whatever outer layers the parent usually
      * uses to call into the PGBackend
      */
     virtual Context *bless_context(Context *c) = 0;
     virtual GenContext<ThreadPool::TPHandle&> *bless_gencontext(
       GenContext<ThreadPool::TPHandle&> *c) = 0;

     virtual void send_message(int to_osd, Message *m) = 0;
     virtual void queue_transaction(
       ObjectStore::Transaction *t,
       OpRequestRef op = OpRequestRef()
       ) = 0;
     virtual epoch_t get_epoch() const = 0;

     virtual const set<pg_shard_t> &get_actingbackfill_shards() const = 0;
     virtual const set<pg_shard_t> &get_acting_shards() const = 0;
     virtual const set<pg_shard_t> &get_backfill_shards() const = 0;

     virtual std::string gen_dbg_prefix() const = 0;

     virtual const map<hobject_t, set<pg_shard_t> > &get_missing_loc_shards()
       const = 0;

     virtual const pg_missing_t &get_local_missing() const = 0;
     virtual const map<pg_shard_t, pg_missing_t> &get_shard_missing()
       const = 0;
     virtual boost::optional<const pg_missing_t &> maybe_get_shard_missing(
       pg_shard_t peer) const {
       if (peer == primary_shard()) {
	 return get_local_missing();
       } else {
	 map<pg_shard_t, pg_missing_t>::const_iterator i =
	   get_shard_missing().find(peer);
	 if (i == get_shard_missing().end()) {
	   return boost::optional<const pg_missing_t &>();
	 } else {
	   return i->second;
	 }
       }
     }
     virtual const pg_missing_t &get_shard_missing(pg_shard_t peer) const {
       boost::optional<const pg_missing_t &> m = maybe_get_shard_missing(peer);
       assert(m);
       return *m;
     }

     virtual const map<pg_shard_t, pg_info_t> &get_shard_info() const = 0;
     virtual const pg_info_t &get_shard_info(pg_shard_t peer) const {
       if (peer == primary_shard()) {
	 return get_info();
       } else {
	 map<pg_shard_t, pg_info_t>::const_iterator i =
	   get_shard_info().find(peer);
	 assert(i != get_shard_info().end());
	 return i->second;
       }
     }

     virtual const PGLog &get_log() const = 0;
     virtual bool pgb_is_primary() const = 0;
     virtual OSDMapRef pgb_get_osdmap() const = 0;
     virtual const pg_info_t &get_info() const = 0;
     virtual const pg_pool_t &get_pool() const = 0;

     virtual ObjectContextRef get_obc(
       const hobject_t &hoid,
       map<string, bufferlist> &attrs) = 0;

     virtual void op_applied(
       const eversion_t &applied_version) = 0;

     virtual bool should_send_op(
       pg_shard_t peer,
       const hobject_t &hoid) = 0;

     virtual void log_operation(
       const vector<pg_log_entry_t> &logv,
       boost::optional<pg_hit_set_history_t> &hset_history,
       const eversion_t &trim_to,
       const eversion_t &trim_rollback_to,
       bool transaction_applied,
       ObjectStore::Transaction *t) = 0;

     virtual void update_peer_last_complete_ondisk(
       pg_shard_t fromosd,
       eversion_t lcod) = 0;

     virtual void update_last_complete_ondisk(
       eversion_t lcod) = 0;

     virtual void update_stats(
       const pg_stat_t &stat) = 0;

     virtual void schedule_recovery_work(
       GenContext<ThreadPool::TPHandle&> *c) = 0;

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

     virtual bool transaction_use_tbl() = 0;

     virtual void send_message_osd_cluster(
       int peer, Message *m, epoch_t from_epoch) = 0;
     virtual void send_message_osd_cluster(
       Message *m, Connection *con) = 0;
     virtual void send_message_osd_cluster(
       Message *m, const ConnectionRef& con) = 0;
     virtual ConnectionRef get_con_osd_cluster(int peer, epoch_t from_epoch) = 0;
     virtual entity_name_t get_cluster_msgr_name() = 0;

     virtual PerfCounters *get_logger() = 0;

     virtual ceph_tid_t get_tid() = 0;

     virtual LogClientTemp clog_error() = 0;

     virtual ~Listener() {}
   };
   Listener *parent;
   Listener *get_parent() const { return parent; }
   PGBackend(Listener *l, ObjectStore *store, coll_t coll, coll_t temp_coll) :
     store(store),
     coll(coll),
     temp_coll(temp_coll),
     parent(l), temp_created(false) {}
   bool is_primary() const { return get_parent()->pgb_is_primary(); }
   OSDMapRef get_osdmap() const { return get_parent()->pgb_get_osdmap(); }
   const pg_info_t &get_info() { return get_parent()->get_info(); }

   std::string gen_prefix() const {
     return parent->gen_dbg_prefix();
   }

   /**
    * RecoveryHandle
    *
    * We may want to recover multiple objects in the same set of
    * messages.  RecoveryHandle is an interface for the opaque
    * object used by the implementation to store the details of
    * the pending recovery operations.
    */
   struct RecoveryHandle {
     virtual ~RecoveryHandle() {}
   };

   /// Get a fresh recovery operation
   virtual RecoveryHandle *open_recovery_op() = 0;

   /// run_recovery_op: finish the operation represented by h
   virtual void run_recovery_op(
     RecoveryHandle *h,     ///< [in] op to finish
     int priority           ///< [in] msg priority
     ) = 0;

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
    * @param missing [in] set of info, missing pairs for queried nodes
    * @param overlaps [in] mapping of object to file offset overlaps
    */
   virtual void recover_object(
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
   virtual bool handle_message(
     OpRequestRef op ///< [in] message received
     ) = 0; ///< @return true if the message was handled

   virtual void check_recovery_sources(const OSDMapRef osdmap) = 0;


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

   virtual void on_flushed() = 0;

   virtual IsPGRecoverablePredicate *get_is_recoverable_predicate() = 0;
   virtual IsPGReadablePredicate *get_is_readable_predicate() = 0;

   void temp_colls(list<coll_t> *out) {
     if (temp_created)
       out->push_back(temp_coll);
   }
   void split_colls(
     spg_t child,
     int split_bits,
     int seed,
     ObjectStore::Transaction *t) {
     coll_t target = coll_t::make_temp_coll(child);
     if (!temp_created)
       return;
     t->create_collection(target);
     t->split_collection(
       temp_coll,
       split_bits,
       seed,
       target);
   }

   virtual void dump_recovery_info(Formatter *f) const = 0;

 private:
   bool temp_created;
   set<hobject_t> temp_contents;
 public:
   coll_t get_temp_coll(ObjectStore::Transaction *t);
   coll_t get_temp_coll() const {
    return temp_coll;
   }
   bool have_temp_coll() const { return temp_created; }

   // Track contents of temp collection, clear on reset
   void add_temp_obj(const hobject_t &oid) {
     temp_contents.insert(oid);
   }
   void add_temp_objs(const set<hobject_t> &oids) {
     temp_contents.insert(oids.begin(), oids.end());
   }
   void clear_temp_obj(const hobject_t &oid) {
     temp_contents.erase(oid);
   }
   void clear_temp_objs(const set<hobject_t> &oids) {
     for (set<hobject_t>::const_iterator i = oids.begin();
	  i != oids.end();
	  ++i) {
       temp_contents.erase(*i);
     }
   }

   virtual ~PGBackend() {}

   /**
    * Client IO Interface
    */
   class PGTransaction {
   public:
     /// Write
     virtual void touch(
       const hobject_t &hoid  ///< [in] obj to touch
       ) = 0;
     virtual void stash(
       const hobject_t &hoid,   ///< [in] obj to remove
       version_t former_version ///< [in] former object version
       ) = 0;
     virtual void remove(
       const hobject_t &hoid ///< [in] obj to remove
       ) = 0;
     virtual void setattrs(
       const hobject_t &hoid,         ///< [in] object to write
       map<string, bufferlist> &attrs ///< [in] attrs, may be cleared
       ) = 0;
     virtual void setattr(
       const hobject_t &hoid,         ///< [in] object to write
       const string &attrname,        ///< [in] attr to write
       bufferlist &bl                 ///< [in] val to write, may be claimed
       ) = 0;
     virtual void rmattr(
       const hobject_t &hoid,         ///< [in] object to write
       const string &attrname         ///< [in] attr to remove
       ) = 0;
     virtual void clone(
       const hobject_t &from,
       const hobject_t &to
       ) = 0;
     virtual void rename(
       const hobject_t &from,
       const hobject_t &to
       ) = 0;
     virtual void set_alloc_hint(
       const hobject_t &hoid,
       uint64_t expected_object_size,
       uint64_t expected_write_size
       ) = 0;

     /// Optional, not supported on ec-pool
     virtual void write(
       const hobject_t &hoid, ///< [in] object to write
       uint64_t off,          ///< [in] off at which to write
       uint64_t len,          ///< [in] len to write from bl
       bufferlist &bl,        ///< [in] bl to write will be claimed to len
       uint32_t fadvise_flags = 0 ///< [in] fadvise hint
       ) { assert(0); }
     virtual void omap_setkeys(
       const hobject_t &hoid,         ///< [in] object to write
       map<string, bufferlist> &keys  ///< [in] omap keys, may be cleared
       ) { assert(0); }
     virtual void omap_rmkeys(
       const hobject_t &hoid,         ///< [in] object to write
       set<string> &keys              ///< [in] omap keys, may be cleared
       ) { assert(0); }
     virtual void omap_clear(
       const hobject_t &hoid          ///< [in] object to clear omap
       ) { assert(0); }
     virtual void omap_setheader(
       const hobject_t &hoid,         ///< [in] object to write
       bufferlist &header             ///< [in] header
       ) { assert(0); }
     virtual void clone_range(
       const hobject_t &from,         ///< [in] from
       const hobject_t &to,           ///< [in] to
       uint64_t fromoff,              ///< [in] offset
       uint64_t len,                  ///< [in] len
       uint64_t tooff                 ///< [in] offset
       ) { assert(0); }
     virtual void truncate(
       const hobject_t &hoid,
       uint64_t off
       ) { assert(0); }
     virtual void zero(
       const hobject_t &hoid,
       uint64_t off,
       uint64_t len
       ) { assert(0); }

     /// Supported on all backends

     /// off must be the current object size
     virtual void append(
       const hobject_t &hoid, ///< [in] object to write
       uint64_t off,          ///< [in] off at which to write
       uint64_t len,          ///< [in] len to write from bl
       bufferlist &bl,        ///< [in] bl to write will be claimed to len
       uint32_t fadvise_flags ///< [in] fadvise hint
       ) { write(hoid, off, len, bl, fadvise_flags); }

     /// to_append *must* have come from the same PGBackend (same concrete type)
     virtual void append(
       PGTransaction *to_append ///< [in] trans to append, to_append is cleared
       ) = 0;
     virtual void nop() = 0;
     virtual bool empty() const = 0;
     virtual uint64_t get_bytes_written() const = 0;
     virtual ~PGTransaction() {}
   };
   /// Get implementation specific empty transaction
   virtual PGTransaction *get_transaction() = 0;

   /// execute implementation specific transaction
   virtual void submit_transaction(
     const hobject_t &hoid,               ///< [in] object
     const eversion_t &at_version,        ///< [in] version
     PGTransaction *t,                    ///< [in] trans to execute
     const eversion_t &trim_to,           ///< [in] trim log to here
     const eversion_t &trim_rollback_to,  ///< [in] trim rollback info to here
     const vector<pg_log_entry_t> &log_entries, ///< [in] log entries for t
     /// [in] hitset history (if updated with this transaction)
     boost::optional<pg_hit_set_history_t> &hset_history,
     Context *on_local_applied_sync,      ///< [in] called when applied locally
     Context *on_all_applied,             ///< [in] called when all acked
     Context *on_all_commit,              ///< [in] called when all commit
     ceph_tid_t tid,                      ///< [in] tid
     osd_reqid_t reqid,                   ///< [in] reqid
     OpRequestRef op                      ///< [in] op
     ) = 0;


   void rollback(
     const hobject_t &hoid,
     const ObjectModDesc &desc,
     ObjectStore::Transaction *t);

   /// Reapply old attributes
   void rollback_setattrs(
     const hobject_t &hoid,
     map<string, boost::optional<bufferlist> > &old_attrs,
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

   /// Delete object to rollback create
   void rollback_create(
     const hobject_t &hoid,
     ObjectStore::Transaction *t);

   /// Trim object stashed at stashed_version
   void trim_stashed_object(
     const hobject_t &hoid,
     version_t stashed_version,
     ObjectStore::Transaction *t);

   /// List objects in collection
   int objects_list_partial(
     const hobject_t &begin,
     int min,
     int max,
     snapid_t seq,
     vector<hobject_t> *ls,
     hobject_t *next);

   int objects_list_range(
     const hobject_t &start,
     const hobject_t &end,
     snapid_t seq,
     vector<hobject_t> *ls,
     vector<ghobject_t> *gen_obs=0);

   int objects_get_attr(
     const hobject_t &hoid,
     const string &attr,
     bufferlist *out);

   virtual int objects_get_attrs(
     const hobject_t &hoid,
     map<string, bufferlist> *out);

   virtual int objects_read_sync(
     const hobject_t &hoid,
     uint64_t off,
     uint64_t len,
     uint32_t op_flags,
     bufferlist *bl) = 0;

   virtual void objects_read_async(
     const hobject_t &hoid,
     const list<pair<boost::tuple<uint64_t, uint64_t, uint32_t>,
		pair<bufferlist*, Context*> > > &to_read,
     Context *on_complete) = 0;

   virtual bool scrub_supported() { return false; }
   void be_scan_list(
     ScrubMap &map, const vector<hobject_t> &ls, bool deep, uint32_t seed,
     ThreadPool::TPHandle &handle);
   enum scrub_error_type be_compare_scrub_objects(
     pg_shard_t auth_shard,
     const ScrubMap::object &auth,
     const object_info_t& auth_oi,
     bool okseed,
     const ScrubMap::object &candidate,
     ostream &errorstream);
   map<pg_shard_t, ScrubMap *>::const_iterator be_select_auth_object(
     const hobject_t &obj,
     const map<pg_shard_t,ScrubMap*> &maps,
     bool okseed,
     object_info_t *auth_oi);
   void be_compare_scrubmaps(
     const map<pg_shard_t,ScrubMap*> &maps,
     bool okseed,   ///< true if scrub digests have same seed our oi digests
     bool repair,
     map<hobject_t, set<pg_shard_t> > &missing,
     map<hobject_t, set<pg_shard_t> > &inconsistent,
     map<hobject_t, list<pg_shard_t> > &authoritative,
     map<hobject_t, pair<uint32_t,uint32_t> > &missing_digest,
     int &shallow_errors, int &deep_errors,
     const spg_t& pgid,
     const vector<int> &acting,
     ostream &errorstream);
   virtual uint64_t be_get_ondisk_size(
     uint64_t logical_size) { assert(0); return 0; }
   virtual void be_deep_scrub(
     const hobject_t &poid,
     uint32_t seed,
     ScrubMap::object &o,
     ThreadPool::TPHandle &handle) { assert(0); }

   static PGBackend *build_pg_backend(
     const pg_pool_t &pool,
     const OSDMapRef curmap,
     Listener *l,
     coll_t coll,
     coll_t temp_coll,
     ObjectStore *store,
     CephContext *cct);
 };

struct PG_SendMessageOnConn: public Context {
  PGBackend::Listener *pg;
  Message *reply;
  ConnectionRef conn;
  PG_SendMessageOnConn(
    PGBackend::Listener *pg,
    Message *reply,
    ConnectionRef conn) : pg(pg), reply(reply), conn(conn) {}
  void finish(int) {
    pg->send_message_osd_cluster(reply, conn.get());
  }
};

struct PG_RecoveryQueueAsync : public Context {
  PGBackend::Listener *pg;
  GenContext<ThreadPool::TPHandle&> *c;
  PG_RecoveryQueueAsync(
    PGBackend::Listener *pg,
    GenContext<ThreadPool::TPHandle&> *c) : pg(pg), c(c) {}
  void finish(int) {
    pg->schedule_recovery_work(c);
  }
};

#endif
