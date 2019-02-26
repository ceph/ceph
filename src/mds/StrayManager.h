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
#include "mds/PurgeQueue.h"

class MDSRank;
class PerfCounters;
class CInode;
class CDentry;

class StrayManager
{
  protected:
  // Has passed through eval_stray and still has refs
  elist<CDentry*> delayed_eval_stray;

  // strays that have been trimmed from cache
  std::set<std::string> trimmed_strays;

  // Global references for doing I/O
  MDSRank *mds;
  PerfCounters *logger;

  bool started;

  // Stray dentries for this rank (including those not in cache)
  uint64_t num_strays;

  // Stray dentries
  uint64_t num_strays_delayed;

  // Entries that have entered enqueue() but not been persistently
  // recorded by PurgeQueue yet
  uint64_t num_strays_enqueuing;

  PurgeQueue &purge_queue;

  void truncate(CDentry *dn);

  /**
   * Purge a dentry from a stray directory.  This function
   * is called once eval_stray is satisfied and StrayManager
   * throttling is also satisfied.  There is no going back
   * at this stage!
   */
  void purge(CDentry *dn);

  /**
   * Completion handler for a Filer::purge on a stray inode.
   */
  void _purge_stray_purged(CDentry *dn, bool only_head);

  void _purge_stray_logged(CDentry *dn, version_t pdv, LogSegment *ls);

  /**
   * Callback: we have logged the update to an inode's metadata
   * reflecting it's newly-zeroed length.
   */
  void _truncate_stray_logged(CDentry *dn, LogSegment *ls);

  friend class StrayManagerIOContext;
  friend class StrayManagerLogContext;
  friend class StrayManagerContext;

  friend class C_StraysFetched;
  friend class C_OpenSnapParents;
  friend class C_PurgeStrayLogged;
  friend class C_TruncateStrayLogged;
  friend class C_IO_PurgeStrayPurged;


  // Call this on a dentry that has been identified as
  // eligible for purging.  It will be passed on to PurgeQueue.
  void enqueue(CDentry *dn, bool trunc);

  // Final part of enqueue() which we may have to retry
  // after opening snap parents.
  void _enqueue(CDentry *dn, bool trunc);


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
  bool _eval_stray(CDentry *dn);

  void _eval_stray_remote(CDentry *stray_dn, CDentry *remote_dn);

  // My public interface is for consumption by MDCache
  public:
  explicit StrayManager(MDSRank *mds, PurgeQueue &purge_queue_);
  void set_logger(PerfCounters *l) {logger = l;}
  void activate();

  bool eval_stray(CDentry *dn);

  void set_num_strays(uint64_t num);
  uint64_t get_num_strays() const { return num_strays; }

  /**
   * Queue dentry for later evaluation. (evaluate it while not in the
   * middle of another metadata operation)
   */
  void queue_delayed(CDentry *dn);

  /**
   * Eval strays in the delayed_eval_stray list
   */
  void advance_delayed();

  /**
   * Remote dentry potentially points to a stray. When it is touched,
   * call in here to evaluate it for migration (move a stray residing
   * on another MDS to this MDS) or reintegration (move a stray dentry's
   * inode into a non-stray hardlink dentry and clean up the stray).
   *
   * @param stray_dn a stray dentry whose inode has been referenced
   *                 by a remote dentry
   * @param remote_dn (optional) which remote dentry was touched
   *                  in an operation that led us here: this is used
   *                  as a hint for which remote to reintegrate into
   *                  if there are multiple remotes.
   */
  void eval_remote(CDentry *remote_dn);

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
};

#endif  // STRAY_MANAGER_H
