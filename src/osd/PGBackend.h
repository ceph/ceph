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

#ifndef PGBACKEND_H
#define PGBACKEND_H

#include "OSDMap.h"
#include "PGLog.h"
#include "osd_types.h"
#include "common/WorkQueue.h"
#include "osd_types.h"
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
       int peer,
       const hobject_t &oid,
       const ObjectRecoveryInfo &recovery_info,
       const object_stat_sum_t &stat
       ) = 0;

     virtual void begin_peer_recover(
       int peer,
       const hobject_t oid) = 0;

     virtual void failed_push(int from, const hobject_t &soid) = 0;

     
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
     virtual epoch_t get_epoch() = 0;
     virtual const vector<int> &get_actingbackfill() = 0;
     virtual std::string gen_dbg_prefix() const = 0;

     virtual const map<hobject_t, set<int> > &get_missing_loc() = 0;
     virtual const map<int, pg_missing_t> &get_peer_missing() = 0;
     virtual const map<int, pg_info_t> &get_peer_info() = 0;
     virtual const pg_missing_t &get_local_missing() = 0;
     virtual const PGLog &get_log() = 0;
     virtual bool pgb_is_primary() const = 0;
     virtual OSDMapRef pgb_get_osdmap() const = 0;
     virtual const pg_info_t &get_info() const = 0;

     virtual ObjectContextRef get_obc(
       const hobject_t &hoid,
       map<string, bufferptr> &attrs) = 0;

     virtual void op_applied(
       const eversion_t &applied_version) = 0;

     virtual bool should_send_op(
       int peer,
       const hobject_t &hoid) = 0;

     virtual void log_operation(
       vector<pg_log_entry_t> &logv,
       const eversion_t &trim_to,
       bool transaction_applied,
       ObjectStore::Transaction *t) = 0;

     virtual void update_peer_last_complete_ondisk(
       int fromosd,
       eversion_t lcod) = 0;

     virtual void update_last_complete_ondisk(
       eversion_t lcod) = 0;

     virtual void update_stats(
       const pg_stat_t &stat) = 0;

     virtual ~Listener() {}
   };
   Listener *parent;
   Listener *get_parent() const { return parent; }
   PGBackend(Listener *l) : parent(l) {}
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
    * implementation should clear itself, contexts blessed prior to on_change
    * won't be called after on_change()
    */
   virtual void on_change(ObjectStore::Transaction *t) = 0;
   virtual void clear_state() = 0;

   virtual void on_flushed() = 0;


   virtual void split_colls(
     pg_t child,
     int split_bits,
     int seed,
     ObjectStore::Transaction *t) = 0;

   virtual void temp_colls(list<coll_t> *out) = 0;

   virtual void dump_recovery_info(Formatter *f) const = 0;

   virtual coll_t get_temp_coll(ObjectStore::Transaction *t) = 0;
   virtual void add_temp_obj(const hobject_t &oid) = 0;
   virtual void clear_temp_obj(const hobject_t &oid) = 0;

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

     /// Optional, not supported on ec-pool
     virtual void write(
       const hobject_t &hoid, ///< [in] object to write
       uint64_t off,          ///< [in] off at which to write
       uint64_t len,          ///< [in] len to write from bl
       bufferlist &bl         ///< [in] bl to write will be claimed to len
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
       bufferlist &bl         ///< [in] bl to write will be claimed to len
       ) { write(hoid, off, len, bl); }

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
     vector<pg_log_entry_t> &log_entries, ///< [in] log entries for t
     Context *on_local_applied_sync,      ///< [in] called when applied locally
     Context *on_all_applied,               ///< [in] called when all acked
     Context *on_all_commit,              ///< [in] called when all commit
     tid_t tid,                           ///< [in] tid
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
   virtual int objects_list_partial(
     const hobject_t &begin,
     int min,
     int max,
     snapid_t seq,
     vector<hobject_t> *ls,
     hobject_t *next) = 0;

   virtual int objects_list_range(
     const hobject_t &start,
     const hobject_t &end,
     snapid_t seq,
     vector<hobject_t> *ls) = 0;

   virtual int objects_get_attr(
     const hobject_t &hoid,
     const string &attr,
     bufferlist *out) = 0;

   virtual int objects_get_attrs(
     const hobject_t &hoid,
     map<string, bufferlist> *out) = 0;

   virtual int objects_read_sync(
     const hobject_t &hoid,
     uint64_t off,
     uint64_t len,
     bufferlist *bl) = 0;

   virtual void objects_read_async(
     const hobject_t &hoid,
     const list<pair<pair<uint64_t, uint64_t>,
		pair<bufferlist*, Context*> > > &to_read,
     Context *on_complete) = 0;

   virtual bool scrub_supported() { return false; }
   virtual void be_scan_list(ScrubMap &map, const vector<hobject_t> &ls, bool deep,
     ThreadPool::TPHandle &handle) { assert(0); }
   virtual enum scrub_error_type be_compare_scrub_objects(
				const ScrubMap::object &auth,
				const ScrubMap::object &candidate,
				ostream &errorstream) { assert(0); }
   virtual map<int, ScrubMap *>::const_iterator be_select_auth_object(
     const hobject_t &obj,
     const map<int,ScrubMap*> &maps) { assert(0); }
   virtual void be_compare_scrubmaps(const map<int,ScrubMap*> &maps,
			    map<hobject_t, set<int> > &missing,
			    map<hobject_t, set<int> > &inconsistent,
			    map<hobject_t, int> &authoritative,
			    map<hobject_t, set<int> > &invalid_snapcolls,
			    int &shallow_errors, int &deep_errors,
			    const pg_t pgid,
			    const vector<int> &acting,
			    ostream &errorstream) { assert(0); }
 };

#endif
