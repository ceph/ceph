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
struct MDPeerUpdate;

class LogSegment {
 public:
  using seq_t = uint64_t;

  LogSegment(uint64_t _seq, loff_t off=-1) :
    seq(_seq), offset(off), end(off),
    dirty_dirfrags(member_offset(CDir, item_dirty)),
    new_dirfrags(member_offset(CDir, item_new)),
    dirty_inodes(member_offset(CInode, item_dirty)),
    dirty_dentries(member_offset(CDentry, item_dirty)),
    open_files(member_offset(CInode, item_open_file)),
    dirty_parent_inodes(member_offset(CInode, item_dirty_parent)),
    dirty_dirfrag_dir(member_offset(CInode, item_dirty_dirfrag_dir)),
    dirty_dirfrag_nest(member_offset(CInode, item_dirty_dirfrag_nest)),
    dirty_dirfrag_dirfragtree(member_offset(CInode, item_dirty_dirfrag_dirfragtree))
  {}

  void try_to_expire(MDSRank *mds, MDSGatherBuilder &gather_bld, int op_prio);
  void purge_inodes_finish(interval_set<inodeno_t>& inos){
    purging_inodes.subtract(inos);
    if (NULL != purged_cb &&
	purging_inodes.empty())
      purged_cb->complete(0);
  }
  void set_purged_cb(MDSContext* c){
    ceph_assert(purged_cb == NULL);
    purged_cb = c;
  }
  void wait_for_expiry(MDSContext *c)
  {
    ceph_assert(c != NULL);
    expiry_waiters.push_back(c);
  }

  const seq_t seq;
  uint64_t offset, end;
  uint64_t num_events = 0;

  // dirty items
  elist<CDir*>    dirty_dirfrags, new_dirfrags;
  elist<CInode*>  dirty_inodes;
  elist<CDentry*> dirty_dentries;

  elist<CInode*>  open_files;
  elist<CInode*>  dirty_parent_inodes;
  elist<CInode*>  dirty_dirfrag_dir;
  elist<CInode*>  dirty_dirfrag_nest;
  elist<CInode*>  dirty_dirfrag_dirfragtree;

  std::set<CInode*> truncating_inodes;
  interval_set<inodeno_t> purging_inodes;
  MDSContext* purged_cb = nullptr;

  std::map<int, ceph::unordered_set<version_t> > pending_commit_tids;  // mdstable
  std::set<metareqid_t> uncommitted_leaders;
  std::set<metareqid_t> uncommitted_peers;
  std::set<dirfrag_t> uncommitted_fragments;

  // client request ids
  std::map<int, ceph_tid_t> last_client_tids;

  // potentially dirty sessions
  std::set<entity_name_t> touched_sessions;

  // table version
  version_t inotablev = 0;
  version_t sessionmapv = 0;
  std::map<int,version_t> tablev;

  MDSContext::vec expiry_waiters;
};

static inline std::ostream& operator<<(std::ostream& out, const LogSegment& ls) {
  return out << "LogSegment(" << ls.seq << "/0x" << std::hex << ls.offset
             << "~" << ls.end << std::dec << " events=" << ls.num_events << ")";
}

#endif
