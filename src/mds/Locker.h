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
class Mutation;
class MDRequest;
class EMetaBlob;
class SnapRealm;

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
class ScatterLock;
class LocalLock;
class MDCache;

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
  void include_snap_rdlocks(set<SimpleLock*>& rdlocks, CInode *in);

  bool acquire_locks(MDRequest *mdr,
		     set<SimpleLock*> &rdlocks,
		     set<SimpleLock*> &wrlocks,
		     set<SimpleLock*> &xlocks);

  void drop_locks(Mutation *mut);
  void set_xlocks_done(Mutation *mut);
  void drop_rdlocks(Mutation *mut);

  void eval_gather(SimpleLock *lock, bool first=false);
  void eval_cap_gather(CInode *in);
  void eval(SimpleLock *lock);

  bool rdlock_try(SimpleLock *lock, int client, Context *c);
  bool rdlock_start(SimpleLock *lock, MDRequest *mut);
  void rdlock_finish(SimpleLock *lock, Mutation *mut);

  void wrlock_force(SimpleLock *lock, Mutation *mut);
  bool wrlock_start(SimpleLock *lock, MDRequest *mut, bool nowait=false);
  void wrlock_finish(SimpleLock *lock, Mutation *mut);

  bool xlock_start(SimpleLock *lock, MDRequest *mut);
  void xlock_finish(SimpleLock *lock, Mutation *mut);  // public for Server's slave UNXLOCK

  // simple
public:
  void try_simple_eval(SimpleLock *lock);
  bool simple_rdlock_try(SimpleLock *lock, Context *con);
protected:
  void simple_eval(SimpleLock *lock);
  void handle_simple_lock(SimpleLock *lock, MLock *m);

public:
  bool simple_sync(SimpleLock *lock);
protected:
  void simple_lock(SimpleLock *lock);
  void simple_xlock(SimpleLock *lock);

public:
  bool dentry_can_rdlock_trace(vector<CDentry*>& trace);
  void dentry_anon_rdlock_trace_start(vector<CDentry*>& trace);
  void dentry_anon_rdlock_trace_finish(vector<CDentry*>& trace);

  // scatter
public:
  void try_scatter_eval(ScatterLock *lock);
  void scatter_eval(ScatterLock *lock);        // public for MDCache::adjust_subtree_auth()

  void scatter_tick();
  void scatter_nudge(ScatterLock *lock, Context *c);

protected:
  void handle_scatter_lock(ScatterLock *lock, MLock *m);
  void _scatter_replica_lock(ScatterLock *lock, int auth);
  bool scatter_scatter_fastpath(ScatterLock *lock);
  void scatter_scatter(ScatterLock *lock, bool nowait=false);
  void scatter_tempsync(ScatterLock *lock);

  void scatter_writebehind(ScatterLock *lock);
  class C_Locker_ScatterWB : public Context {
    Locker *locker;
    ScatterLock *lock;
    Mutation *mut;
  public:
    C_Locker_ScatterWB(Locker *l, ScatterLock *sl, Mutation *m) : locker(l), lock(sl), mut(m) {}
    void finish(int r) { 
      locker->scatter_writebehind_finish(lock, mut); 
    }
  };
  void scatter_writebehind_finish(ScatterLock *lock, Mutation *mut);

  xlist<ScatterLock*> updated_scatterlocks;
public:
  void mark_updated_scatterlock(ScatterLock *lock);

  // local
  void local_wrlock_grab(LocalLock *lock, Mutation *mut);
protected:
  bool local_wrlock_start(LocalLock *lock, MDRequest *mut);
  void local_wrlock_finish(LocalLock *lock, Mutation *mut);
  bool local_xlock_start(LocalLock *lock, MDRequest *mut);
  void local_xlock_finish(LocalLock *lock, Mutation *mut);


  // file
public:
  void try_file_eval(ScatterLock *lock);
  void file_eval(ScatterLock *lock);
protected:
  void handle_file_lock(ScatterLock *lock, MLock *m);
  void file_mixed(ScatterLock *lock);
  void file_excl(ScatterLock *lock);

  xlist<ScatterLock*> updated_filelocks;
public:
  void mark_updated_Filelock(ScatterLock *lock);

  // -- file i/o --
 public:
  version_t issue_file_data_version(CInode *in);
  Capability* issue_new_caps(CInode *in, int mode, Session *session, SnapRealm *conrealm=0);
  bool issue_caps(CInode *in);
  void issue_truncate(CInode *in);
  void revoke_stale_caps(Session *session);
  void resume_stale_caps(Session *session);
  void remove_stale_leases(Session *session);

 protected:
  void handle_client_caps(class MClientCaps *m);
  bool _do_cap_update(CInode *in, Capability *cap, int had, int wanted, snapid_t follows, MClientCaps *m,
		      MClientCaps *ack=0);
  void handle_client_cap_release(class MClientCapRelease *m);

public:
  void request_inode_file_caps(CInode *in);
protected:
  void handle_inode_file_caps(class MInodeFileCaps *m);

  void file_update_finish(CInode *in, Mutation *mut, bool share, int client, Capability *cap,
			  MClientCaps *ack);
public:
  bool check_inode_max_size(CInode *in, bool force_wrlock=false, bool update_size=false, __u64 newsize=0);
private:
  void share_inode_max_size(CInode *in);

  friend class C_MDL_CheckMaxSize;
  friend class C_MDL_RequestInodeFileCaps;
  friend class C_Locker_FileUpdate_finish;

  
  // -- client leases --
public:
  void handle_client_lease(class MClientLease *m);

  void _issue_client_lease(MDSCacheObject *p, int mask, int pool, int client, bufferlist &bl, utime_t now, Session *session);
  int issue_client_lease(CInode *in, int client, bufferlist &bl, utime_t now, Session *session);
  int issue_client_lease(CDentry *dn, int client, bufferlist &bl, utime_t now, Session *session);
  void revoke_client_leases(SimpleLock *lock);
};


#endif
