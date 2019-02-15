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

#include "include/elist.h"
#include "include/interval_set.h"
#include "include/Context.h"
#include "MDSContext.h"
#include "mdstypes.h"
#include "CInode.h"
#include "CDentry.h"
#include "CDir.h"

#include "include/unordered_set.h"
using ceph::unordered_set;

class CDir;
class CInode;
class CDentry;
class MDSRank;
struct MDSlaveUpdate;

typedef uint64_t log_segment_seq_t;

class LogSegment {
 public:
  const log_segment_seq_t seq;
  uint64_t offset, end;
  int num_events;

  // dirty items
  elist<CDir*>    dirty_dirfrags, new_dirfrags;
  elist<CInode*>  dirty_inodes;
  elist<CDentry*> dirty_dentries;

  elist<CInode*>  open_files;
  elist<CInode*>  dirty_parent_inodes;
  elist<CInode*>  dirty_dirfrag_dir;
  elist<CInode*>  dirty_dirfrag_nest;
  elist<CInode*>  dirty_dirfrag_dirfragtree;

  elist<MDSlaveUpdate*> slave_updates;
  
  set<CInode*> truncating_inodes;

  map<int, ceph::unordered_set<version_t> > pending_commit_tids;  // mdstable
  set<metareqid_t> uncommitted_masters;
  set<dirfrag_t> uncommitted_fragments;

  // client request ids
  map<int, ceph_tid_t> last_client_tids;

  // potentially dirty sessions
  std::set<entity_name_t> touched_sessions;

  // table version
  version_t inotablev;
  version_t sessionmapv;
  map<int,version_t> tablev;

  // try to expire
  void try_to_expire(MDSRank *mds, MDSGatherBuilder &gather_bld, int op_prio);

  MDSContext::vec expiry_waiters;

  void wait_for_expiry(MDSContext *c)
  {
    ceph_assert(c != NULL);
    expiry_waiters.push_back(c);
  }

  // cons
  LogSegment(uint64_t _seq, loff_t off=-1) :
    seq(_seq), offset(off), end(off), num_events(0),
    dirty_dirfrags(member_offset(CDir, item_dirty)),
    new_dirfrags(member_offset(CDir, item_new)),
    dirty_inodes(member_offset(CInode, item_dirty)),
    dirty_dentries(member_offset(CDentry, item_dirty)),
    open_files(member_offset(CInode, item_open_file)),
    dirty_parent_inodes(member_offset(CInode, item_dirty_parent)),
    dirty_dirfrag_dir(member_offset(CInode, item_dirty_dirfrag_dir)),
    dirty_dirfrag_nest(member_offset(CInode, item_dirty_dirfrag_nest)),
    dirty_dirfrag_dirfragtree(member_offset(CInode, item_dirty_dirfrag_dirfragtree)),
    slave_updates(0), // passed to begin() manually
    inotablev(0), sessionmapv(0)
  { }
};

#endif
