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

class MDSRank;
class MDCache;
class PerfCounters;
class CInode;
class CDentry;

struct MutationImpl;
typedef ceph::shared_ptr<MutationImpl> MutationRef;

class StrayManager
{
protected:
  Mutex lock;
public:
  // Global references for doing I/O
  MDSRank* const mds;
  MDCache* const &mdcache;

  explicit StrayManager(MDSRank *mds);
  ~StrayManager();
protected:
//  PerfCounters *logger;

  void truncate(CDentry *dn, CInode *in, uint32_t op_allowance);

  /**
   * Callback: we have logged the update to an inode's metadata
   * reflecting it's newly-zeroed length.
   */
  void _truncate_stray_logged(CDentry *dn, MutationRef& mut);


  /**
   * Purge a dentry from a stray directory.  This function
   * is called once eval_stray is satisfied and StrayManager
   * throttling is also satisfied.  There is no going back
   * at this stage!
   */
  void purge(CDentry *dn, CInode *in, uint32_t op_allowance);

  /**
   * Completion handler for a Filer::purge on a stray inode.
   */
  void _purge_stray_purged(CDentry *dn, uint32_t ops, bool only_head);

  void _purge_stray_logged(CDentry *dn, version_t pdv, MutationRef& mut);

  friend class StrayManagerIOContext;
  friend class StrayManagerLogContext;

  friend class C_IO_PurgeStray;
  friend class C_PurgeStrayLogged;
  friend class C_TruncateStrayLogged;
  friend class C_IO_PurgeStrayPurged;

  std::list<std::pair<CDir*, std::string> > delayed_eval_stray;

public:
  bool eval_stray(CDentry *dn);
  bool eval_stray(CDentry *dn, CInode *in);
  void advance_delayed();

};

#endif  // STRAY_MANAGER_H
