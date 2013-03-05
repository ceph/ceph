// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#ifndef CEPH_LOGSEGMENT_H
#define CEPH_LOGSEGMENT_H

#include "include/dlist.h"
#include "include/elist.h"
#include "include/interval_set.h"
#include "include/Context.h"
#include "mdstypes.h"
#include "CInode.h"
#include "CDentry.h"
#include "CDir.h"

#include <ext/hash_set>
using __gnu_cxx::hash_set;

class CDir;
class CInode;
class CDentry;
class MDS;
class MDSlaveUpdate;

// The backtrace info struct here is used to maintain the backtrace in
// a queue that we will eventually want to write out (on journal segment
// expiry).
class BacktraceInfo {
public:
  int64_t location;
  int64_t pool;
  struct inode_backtrace_t bt;
  elist<BacktraceInfo*>::item item_logseg;
  BacktraceInfo(int64_t l, CInode *i, LogSegment *ls, int64_t p = -1);
  ~BacktraceInfo();
};

class LogSegment {
 public:
  uint64_t offset, end;
  int num_events;
  uint64_t trimmable_at;

  // dirty items
  elist<CDir*>    dirty_dirfrags, new_dirfrags;
  elist<CInode*>  dirty_inodes;
  elist<CDentry*> dirty_dentries;

  elist<CInode*>  open_files;
  elist<CInode*>  dirty_dirfrag_dir;
  elist<CInode*>  dirty_dirfrag_nest;
  elist<CInode*>  dirty_dirfrag_dirfragtree;

  elist<BacktraceInfo*> update_backtraces;

  elist<MDSlaveUpdate*> slave_updates;
  
  set<CInode*> truncating_inodes;

  map<int, hash_set<version_t> > pending_commit_tids;  // mdstable
  set<metareqid_t> uncommitted_masters;

  // client request ids
  map<int, tid_t> last_client_tids;

  // table version
  version_t inotablev;
  version_t sessionmapv;
  map<int,version_t> tablev;

  // try to expire
  void try_to_expire(MDS *mds, C_GatherBuilder &gather_bld);

  // cons
  LogSegment(loff_t off) :
    offset(off), end(off), num_events(0), trimmable_at(0),
    dirty_dirfrags(member_offset(CDir, item_dirty)),
    new_dirfrags(member_offset(CDir, item_new)),
    dirty_inodes(member_offset(CInode, item_dirty)),
    dirty_dentries(member_offset(CDentry, item_dirty)),
    open_files(member_offset(CInode, item_open_file)),
    dirty_dirfrag_dir(member_offset(CInode, item_dirty_dirfrag_dir)),
    dirty_dirfrag_nest(member_offset(CInode, item_dirty_dirfrag_nest)),
    dirty_dirfrag_dirfragtree(member_offset(CInode, item_dirty_dirfrag_dirfragtree)),
    update_backtraces(member_offset(BacktraceInfo, item_logseg)),
    slave_updates(0), // passed to begin() manually
    inotablev(0), sessionmapv(0)
  { }

  // backtrace handling
  void queue_backtrace_update(CInode *in, int64_t location, int64_t pool = -1);
  void remove_pending_backtraces(inodeno_t ino, int64_t pool);
  void store_backtrace_update(MDS *mds, BacktraceInfo *info, Context *fin);
  void _stored_backtrace(BacktraceInfo *info, Context *fin);
  unsigned encode_parent_mutation(ObjectOperation& m, BacktraceInfo *info);
    };

#endif
