// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef STRAY_MANAGER_H
#define STRAY_MANAGER_H

#include "include/elist.h"
#include <list>
#include "osdc/Filer.h"

class MDSRank;
class PerfCounters;
class CInode;
class CDentry;

class StrayManager
{
  protected:
  class QueuedStray {
    public:
    CDentry *dn;
    bool trunc;
    uint32_t ops_required;
    QueuedStray(CDentry *dn_, bool t, uint32_t ops)
      : dn(dn_), trunc(t), ops_required(ops) {}
  };

  // Has passed through eval_stray and still has refs
  elist<CDentry*> delayed_eval_stray;

  // No more refs, can purge these
  std::list<QueuedStray> ready_for_purge;

  // Global references for doing I/O
  MDSRank *mds;
  PerfCounters *logger;

  // Throttled allowances
  uint64_t ops_in_flight;
  uint64_t files_purging;

  // Dynamic op limit per MDS based on PG count
  uint64_t max_purge_ops;

  // Statistics
  uint64_t num_strays;
  uint64_t num_strays_purging;
  uint64_t num_strays_delayed;

  Filer filer;

  void truncate(CDentry *dn, uint32_t op_allowance);

  /**
   * Purge a dentry from a stray directory.  This function
   * is called once eval_stray is satisfied and StrayManager
   * throttling is also satisfied.  There is no going back
   * at this stage!
   */
  void purge(CDentry *dn, uint32_t op_allowance);

  /**
   * Completion handler for a Filer::purge on a stray inode.
   */
  void _purge_stray_purged(CDentry *dn, uint32_t ops, bool only_head);

  void _purge_stray_logged(CDentry *dn, version_t pdv, LogSegment *ls);

  /**
   * Callback: we have logged the update to an inode's metadata
   * reflecting it's newly-zeroed length.
   */
  void _truncate_stray_logged(CDentry *dn, LogSegment *ls);

  friend class StrayManagerIOContext;
  friend class StrayManagerContext;

  friend class C_PurgeStrayLogged;
  friend class C_TruncateStrayLogged;
  friend class C_IO_PurgeStrayPurged;

  /**
   * Enqueue a purge operation on a dentry that has passed the tests
   * in eval_stray.  This may start the operation inline if the throttle
   * allowances are already available.
   *
   * @param trunc false to purge dentry (normal), true to just truncate
   *                 inode (snapshots)
   */
  void enqueue(CDentry *dn, bool trunc);

  /**
   * Iteratively call _consume on items from the ready_for_purge
   * list until it returns false (throttle limit reached)
   */
  void _advance();

  /**
   * Attempt to purge an inode, if throttling permits
   * its.
   *
   * Return true if we successfully consumed resource,
   * false if insufficient resource was available.
   */
  bool _consume(CDentry *dn, bool trunc, uint32_t ops_required);

  /**
   * Return the maximum number of concurrent RADOS ops that
   * may be executed while purging this inode.
   *
   * @param trunc true if it's a truncate, false if it's a purge
   */
  uint32_t _calculate_ops_required(CInode *in, bool trunc);

  /**
   * When hard links exist to an inode whose primary dentry
   * is unlinked, the inode gets a stray primary dentry.
   *
   * We may later "reintegrate" the inode into a remaining
   * non-stray dentry (one of what was previously a remote
   * dentry) by issuing a rename from the stray to the other
   * dentry.
   */
  void reintegrate_stray(CDentry *dn, CDentry *rlink);

  /**
   * Evaluate a stray dentry for purging or reintegration.
   *
   * purging: If the inode has no linkage, and no more references, then
   *          we may decide to purge it.
   *
   * reintegration: If the inode still has linkage, then it means someone else
   *                (a hard link) is still referring to it, and we should
   *                think about reintegrating that inode into the remote dentry.
   *
   * @returns true if the dentry will be purged (caller should never
   *          take more refs after this happens), else false.
   */
  bool __eval_stray(CDentry *dn, bool delay=false);

  // My public interface is for consumption by MDCache
  public:
  explicit StrayManager(MDSRank *mds);
  void set_logger(PerfCounters *l) {logger = l;}

  bool eval_stray(CDentry *dn, bool delay=false);

  /**
   * Where eval_stray was previously invoked with delay=true, call
   * eval_stray again for any dentries that were put on the
   * delayed_eval_stray list as a result of the original call.
   *
   * Used so that various places can call eval_stray(delay=true) during
   * an operation to identify dentries of interest, and then call
   * this function later during trim in order to do the final
   * evaluation (and resulting actions) while not in the middle of another
   * metadata operation.
   */
  void advance_delayed();

  /**
   * When a metadata op touches a remote dentry that points to
   * a stray, call in here to evaluate it for migration (move
   * a stray residing on another MDS to this MDS) or reintegration
   * (move a stray dentry's inode into a non-stray hardlink dentry and
   * clean up the stray).
   *
   * @param stray_dn a stray dentry whose inode has been referenced
   *                 by a remote dentry
   * @param remote_dn (optional) which remote dentry was touched
   *                  in an operation that led us here: this is used
   *                  as a hint for which remote to reintegrate into
   *                  if there are multiple remotes.
   */
  void eval_remote_stray(CDentry *stray_dn, CDentry *remote_dn=NULL);

  /**
   * Given a dentry within one of my stray directories,
   * send it off to a stray directory in another MDS.
   *
   * This is for use:
   *  * Case A: when shutting down a rank, we migrate strays
   *    away from ourselves rather than waiting for purge
   *  * Case B: when a client request has a trace that refers to
   *    a stray inode on another MDS, we migrate that inode from
   *    there to here, in order that we can later re-integrate it
   *    here.
   *
   * In case B, the receiver should be calling into eval_stray
   * on completion of mv (i.e. inode put), resulting in a subsequent
   * reintegration.
   */
  void migrate_stray(CDentry *dn, mds_rank_t dest);

  /**
   * Update stats to reflect a newly created stray dentry.  Needed
   * because stats on strays live here, but creation happens
   * in Server or MDCache.  For our purposes "creation" includes
   * loading a stray from a dirfrag and migrating a stray from
   * another MDS, in addition to creations per-se.
   */
  void notify_stray_created();

  /**
   * Update stats to reflect a removed stray dentry.  Needed because
   * stats on strays live here, but removal happens in Server or
   * MDCache.  Also includes migration (rename) of strays from
   * this MDS to another MDS.
   */
  void notify_stray_removed();

  /**
   * For any strays that are enqueued for purge, but
   * currently blocked on throttling, clear their
   * purging status.  Used during MDS rank shutdown
   * so that it can migrate these strays instead
   * of waiting for them to trickle through the
   * queue.
   */
  void abort_queue();

  /*
   * Calculate our local RADOS op throttle limit based on
   * (mds_max_purge_ops_per_pg / number_of_mds) * number_of_pg
   *
   * Call this whenever one of those operands changes.
   */
  void update_op_limit();
};

#endif  // STRAY_MANAGER_H
