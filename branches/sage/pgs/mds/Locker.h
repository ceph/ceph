// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
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

#ifndef __MDS_LOCKER_H
#define __MDS_LOCKER_H

#include "include/types.h"

#include <map>
#include <list>
#include <set>
using std::map;
using std::list;
using std::set;

class MDS;
class CDir;
class CInode;
class CDentry;

class Message;

class MDiscover;
class MDiscoverReply;
class MCacheExpire;
class MDirUpdate;
class MDentryUnlink;
class MLock;

class MClientRequest;


class Anchor;
class Capability;

class SimpleLock;
class FileLock;
class ScatterLock;

class Locker {
private:
  MDS *mds;
  MDCache *mdcache;
 
 public:
  Locker(MDS *m, MDCache *c) : mds(m), mdcache(c) {}  

  void dispatch(Message *m);
  void handle_lock(MLock *m);

  void send_lock_message(SimpleLock *lock, int msg);
  void send_lock_message(SimpleLock *lock, int msg, bufferlist &data);

  // -- locks --
  bool acquire_locks(MDRequest *mdr,
		     set<SimpleLock*> &rdlocks,
		     set<SimpleLock*> &wrlocks,
		     set<SimpleLock*> &xlocks);

  bool rdlock_start(SimpleLock *lock, MDRequest *mdr);
  void rdlock_finish(SimpleLock *lock, MDRequest *mdr);
  bool xlock_start(SimpleLock *lock, MDRequest *mdr);
  void xlock_finish(SimpleLock *lock, MDRequest *mdr);
  bool wrlock_start(SimpleLock *lock, MDRequest *mdr);
  void wrlock_finish(SimpleLock *lock, MDRequest *mdr);

  // simple
  void handle_simple_lock(SimpleLock *lock, MLock *m);
  void simple_eval(SimpleLock *lock);
  void simple_sync(SimpleLock *lock);
  void simple_lock(SimpleLock *lock);
  bool simple_rdlock_try(SimpleLock *lock, Context *con);
  bool simple_rdlock_start(SimpleLock *lock, MDRequest *mdr);
  void simple_rdlock_finish(SimpleLock *lock, MDRequest *mdr);
  bool simple_xlock_start(SimpleLock *lock, MDRequest *mdr);
  void simple_xlock_finish(SimpleLock *lock, MDRequest *mdr);

  bool dentry_can_rdlock_trace(vector<CDentry*>& trace, MClientRequest *req);
  void dentry_anon_rdlock_trace_start(vector<CDentry*>& trace);
  void dentry_anon_rdlock_trace_finish(vector<CDentry*>& trace);

  // scatter
  void handle_scatter_lock(ScatterLock *lock, MLock *m);
  void scatter_eval(ScatterLock *lock);
  void scatter_sync(ScatterLock *lock);
  void scatter_scatter(ScatterLock *lock);
  bool scatter_rdlock_start(ScatterLock *lock, MDRequest *mdr);
  void scatter_rdlock_finish(ScatterLock *lock, MDRequest *mdr);
  bool scatter_wrlock_start(ScatterLock *lock, MDRequest *mdr);
  void scatter_wrlock_finish(ScatterLock *lock, MDRequest *mdr);

  // file
  void handle_file_lock(FileLock *lock, MLock *m);
  void file_eval(FileLock *lock);
  bool file_sync(FileLock *lock);
  void file_lock(FileLock *lock);
  void file_mixed(FileLock *lock);
  void file_loner(FileLock *lock);
  bool file_rdlock_try(FileLock *lock, Context *con);
  bool file_rdlock_start(FileLock *lock, MDRequest *mdr);
  void file_rdlock_finish(FileLock *lock, MDRequest *mdr);
  bool file_xlock_start(FileLock *lock, MDRequest *mdr);
  void file_xlock_finish(FileLock *lock, MDRequest *mdr);



  // -- file i/o --
 public:
  version_t issue_file_data_version(CInode *in);
  Capability* issue_new_caps(CInode *in, int mode, MClientRequest *req);
  bool issue_caps(CInode *in);

 protected:
  void handle_client_file_caps(class MClientFileCaps *m);

  void request_inode_file_caps(CInode *in);
  void handle_inode_file_caps(class MInodeFileCaps *m);


};


#endif
