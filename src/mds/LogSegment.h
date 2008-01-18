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

#ifndef __LOGSEGMENT_H
#define __LOGSEGMENT_H

#include "include/xlist.h"
#include "include/interval_set.h"
#include "include/Context.h"

#include <ext/hash_set>
using __gnu_cxx::hash_set;

class CDir;
class CInode;
class CDentry;
class MDS;
class MDSlaveUpdate;

class LogSegment {
 public:
  off_t offset, end;
  int num_events;

  // dirty items
  xlist<CDir*>    dirty_dirfrags;
  xlist<CInode*>  dirty_inodes;
  xlist<CDentry*> dirty_dentries;

  xlist<CInode*>  open_files;
  xlist<CInode*>  dirty_inode_mtimes;

  xlist<MDSlaveUpdate*> slave_updates;

  //xlist<CInode*>  purging_inodes;
  map<CInode*, map<off_t,off_t> > purging_inodes;

  // committed anchor transactions
  hash_set<version_t> pending_commit_atids;

  // client request ids
  map<int, tid_t> last_client_tids;

  // table version
  version_t allocv;
  version_t sessionmapv;
  version_t anchortablev;

  // try to expire
  C_Gather *try_to_expire(MDS *mds);

  // cons
  LogSegment(off_t off) : offset(off), end(off), num_events(0),
			  allocv(0), sessionmapv(0), anchortablev(0) 
  { }
};

#endif
