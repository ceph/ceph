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
class Session;
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
class LogSegment;

class SimpleLock;
class FileLock;
class ScatterLock;
class LocalLock;

class Locker {
private:
  MDS *mds;
  MDCache *mdcache;
 
 public:
  Locker(MDS *m, MDCache *c) : mds(m), mdcache(c) {}  

  SimpleLock *get_lock(int lock_type, MDSCacheObjectInfo &info);
  
  void dispatch(Message *m);
  void handle_lock(MLock *m);


protected:
  void send_lock_message(SimpleLock *lock, int msg);
  void send_lock_message(SimpleLock *lock, int msg, const bufferlist &data);

  // -- locks --
public:
  bool acquire_locks(MDRequest *mdr,
		     set<SimpleLock*> &rdlocks,
		     set<SimpleLock*> &wrlocks,
		     set<SimpleLock*> &xlocks);

  void drop_locks(MDRequest *mdr);

protected:
  bool rdlock_start(SimpleLock *lock, MDRequest *mdr);
  void rdlock_finish(SimpleLock *lock, MDRequest *mdr);
  bool xlock_start(SimpleLock *lock, MDRequest *mdr);
public:
  void xlock_finish(SimpleLock *lock, MDRequest *mdr);  // public for Server's slave UNXLOCK
protected:
  bool wrlock_start(SimpleLock *lock, MDRequest *mdr);
  void wrlock_finish(SimpleLock *lock, MDRequest *mdr);

public:
  void rejoin_set_state(SimpleLock *lock, int s, list<Context*>& waiters);

  // simple
public:
  void try_simple_eval(SimpleLock *lock);
  void simple_eval_gather(SimpleLock *lock);
  bool simple_rdlock_try(SimpleLock *lock, Context *con);
protected:
  void simple_eval(SimpleLock *lock);
  void handle_simple_lock(SimpleLock *lock, MLock *m);
  void simple_sync(SimpleLock *lock);
  void simple_lock(SimpleLock *lock);
  bool simple_rdlock_start(SimpleLock *lock, MDRequest *mdr);
  void simple_rdlock_finish(SimpleLock *lock, MDRequest *mdr);
  bool simple_xlock_start(SimpleLock *lock, MDRequest *mdr);
  void simple_xlock_finish(SimpleLock *lock, MDRequest *mdr);

public:
  bool dentry_can_rdlock_trace(vector<CDentry*>& trace);
  void dentry_anon_rdlock_trace_start(vector<CDentry*>& trace);
  void dentry_anon_rdlock_trace_finish(vector<CDentry*>& trace);

  // scatter
protected:
  xlist<ScatterLock*> autoscattered;

public:
  void try_scatter_eval(ScatterLock *lock);
  void scatter_eval(ScatterLock *lock);        // public for MDCache::adjust_subtree_auth()
  void scatter_eval_gather(ScatterLock *lock);

  void scatter_unscatter_autoscattered();
  void scatter_try_unscatter(ScatterLock *lock, Context *c);
  void note_autoscattered(ScatterLock *lock);

  void scatter_lock(ScatterLock *lock);  // called by LogSegment::try_to_expire

protected:
  void handle_scatter_lock(ScatterLock *lock, MLock *m);
  void _scatter_replica_lock(ScatterLock *lock, int auth);
  void scatter_sync(ScatterLock *lock);
  void scatter_scatter(ScatterLock *lock);
  void scatter_tempsync(ScatterLock *lock);
  bool scatter_rdlock_start(ScatterLock *lock, MDRequest *mdr);
  void scatter_rdlock_finish(ScatterLock *lock, MDRequest *mdr);
  bool scatter_wrlock_start(ScatterLock *lock, MDRequest *mdr);
  void scatter_wrlock_finish(ScatterLock *lock, MDRequest *mdr);

  void scatter_writebehind(ScatterLock *lock);
  class C_Locker_ScatterWB : public Context {
    Locker *locker;
    ScatterLock *lock;
    LogSegment *ls;
  public:
    C_Locker_ScatterWB(Locker *l, ScatterLock *sl, LogSegment *s) : locker(l), lock(sl), ls(s) {}
    void finish(int r) { 
      locker->scatter_writebehind_finish(lock, ls); 
    }
  };
  void scatter_writebehind_finish(ScatterLock *lock, LogSegment *ls);

  // local
protected:
  bool local_wrlock_start(LocalLock *lock, MDRequest *mdr);
  void local_wrlock_finish(LocalLock *lock, MDRequest *mdr);
  bool local_xlock_start(LocalLock *lock, MDRequest *mdr);
  void local_xlock_finish(LocalLock *lock, MDRequest *mdr);


  // file
public:
  void file_eval_gather(FileLock *lock);
  void try_file_eval(FileLock *lock);
protected:
  void file_eval(FileLock *lock);
  void handle_file_lock(FileLock *lock, MLock *m);
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
  Capability* issue_new_caps(CInode *in, int mode, Session *session);
  bool issue_caps(CInode *in);
  void revoke_stale_caps(Session *session);
  void resume_stale_caps(Session *session);

 protected:
  void handle_client_file_caps(class MClientFileCaps *m);

  void request_inode_file_caps(CInode *in);
  void handle_inode_file_caps(class MInodeFileCaps *m);

  friend class C_MDL_RequestInodeFileCaps;

};


#endif
