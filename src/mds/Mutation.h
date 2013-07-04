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

#ifndef CEPH_MDS_MUTATION_H
#define CEPH_MDS_MUTATION_H

#include "include/interval_set.h"
#include "include/elist.h"

#include "mdstypes.h"

#include "SimpleLock.h"
#include "Capability.h"

class LogSegment;
class Capability;
class CInode;
class CDir;
class CDentry;
class Session;
class ScatterLock;
class MClientRequest;
class MMDSSlaveRequest;

struct Mutation {
  metareqid_t reqid;
  __u32 attempt;      // which attempt for this request
  LogSegment *ls;  // the log segment i'm committing to
  utime_t now;

  // flag mutation as slave
  int slave_to_mds;                // this is a slave request if >= 0.

  // -- my pins and locks --
  // cache pins (so things don't expire)
  set< MDSCacheObject* > pins;
  set<CInode*> stickydirs;

  // auth pins
  set< MDSCacheObject* > remote_auth_pins;
  set< MDSCacheObject* > auth_pins;
  
  // held locks
  set< SimpleLock* > rdlocks;  // always local.
  set< SimpleLock* > wrlocks;  // always local.
  map< SimpleLock*, int > remote_wrlocks;
  set< SimpleLock* > xlocks;   // local or remote.
  set< SimpleLock*, SimpleLock::ptr_lt > locks;  // full ordering

  // lock we are currently trying to acquire.  if we give up for some reason,
  // be sure to eval() this.
  SimpleLock *locking;
  int locking_target_mds;

  // if this flag is set, do not attempt to acquire further locks.
  //  (useful for wrlock, which may be a moving auth target)
  bool done_locking; 
  bool committing;
  bool aborted;
  bool killed;

  // for applying projected inode changes
  list<CInode*> projected_inodes;
  list<CDir*> projected_fnodes;
  list<ScatterLock*> updated_locks;

  list<CInode*> dirty_cow_inodes;
  list<pair<CDentry*,version_t> > dirty_cow_dentries;

  Mutation()
    : attempt(0),
      ls(0),
      slave_to_mds(-1),
      locking(NULL),
      locking_target_mds(-1),
      done_locking(false), committing(false), aborted(false), killed(false) { }
  Mutation(metareqid_t ri, __u32 att=0, int slave_to=-1)
    : reqid(ri), attempt(att),
      ls(0),
      slave_to_mds(slave_to), 
      locking(NULL),
      locking_target_mds(-1),
      done_locking(false), committing(false), aborted(false), killed(false) { }
  virtual ~Mutation() {
    assert(locking == NULL);
    assert(pins.empty());
    assert(auth_pins.empty());
    assert(xlocks.empty());
    assert(rdlocks.empty());
    assert(wrlocks.empty());
    assert(remote_wrlocks.empty());
  }

  bool is_master() { return slave_to_mds < 0; }
  bool is_slave() { return slave_to_mds >= 0; }

  client_t get_client() {
    if (reqid.name.is_client())
      return client_t(reqid.name.num());
    return -1;
  }

  // pin items in cache
  void pin(MDSCacheObject *o);
  void unpin(MDSCacheObject *o);
  void set_stickydirs(CInode *in);
  void drop_pins();

  void start_locking(SimpleLock *lock, int target=-1);
  void finish_locking(SimpleLock *lock);

  // auth pins
  bool is_auth_pinned(MDSCacheObject *object);
  void auth_pin(MDSCacheObject *object);
  void auth_unpin(MDSCacheObject *object);
  void drop_local_auth_pins();
  void add_projected_inode(CInode *in);
  void pop_and_dirty_projected_inodes();
  void add_projected_fnode(CDir *dir);
  void pop_and_dirty_projected_fnodes();
  void add_updated_lock(ScatterLock *lock);
  void add_cow_inode(CInode *in);
  void add_cow_dentry(CDentry *dn);
  void apply();
  void cleanup();

  virtual void print(ostream &out) {
    out << "mutation(" << this << ")";
  }
};

inline ostream& operator<<(ostream& out, Mutation &mut)
{
  mut.print(out);
  return out;
}





/** active_request_t
 * state we track for requests we are currently processing.
 * mostly information about locks held, so that we can drop them all
 * the request is finished or forwarded.  see request_*().
 */
struct MDRequest : public Mutation {
  int ref;
  Session *session;
  elist<MDRequest*>::item item_session_request;  // if not on list, op is aborted.

  // -- i am a client (master) request
  MClientRequest *client_request; // client request (if any)

  // store up to two sets of dn vectors, inode pointers, for request path1 and path2.
  vector<CDentry*> dn[2];
  CDentry *straydn;
  CInode *in[2];
  snapid_t snapid;

  CInode *tracei;
  CDentry *tracedn;

  inodeno_t alloc_ino, used_prealloc_ino;  
  interval_set<inodeno_t> prealloc_inos;

  int snap_caps;
  bool did_early_reply;
  bool o_trunc;           ///< request is an O_TRUNC mutation

  bufferlist reply_extra_bl;

  // inos we did a embedded cap release on, and may need to eval if we haven't since reissued
  map<vinodeno_t, ceph_seq_t> cap_releases;  

  // -- i am a slave request
  MMDSSlaveRequest *slave_request; // slave request (if one is pending; implies slave == true)

  // -- i am an internal op
  int internal_op;

  // indicates how may retries of request have been made
  int retry;

  // indicator for vxattr osdmap update
  bool waited_for_osdmap;

  // break rarely-used fields into a separately allocated structure 
  // to save memory for most ops
  struct More {
    set<int> slaves;           // mds nodes that have slave requests to me (implies client_request)
    set<int> waiting_on_slave; // peers i'm waiting for slavereq replies from. 

    // for rename/link/unlink
    set<int> witnessed;       // nodes who have journaled a RenamePrepare
    map<MDSCacheObject*,version_t> pvmap;
    
    // for rename
    set<int> extra_witnesses; // replica list from srcdn auth (rename)
    int srcdn_auth_mds;
    version_t src_reanchor_atid;  // src->dst
    version_t dst_reanchor_atid;  // dst->stray
    bufferlist inode_import;
    version_t inode_import_v;
    CInode* rename_inode;
    bool is_freeze_authpin;
    bool is_ambiguous_auth;
    bool is_remote_frozen_authpin;
    bool is_inode_exporter;

    map<client_t,entity_inst_t> imported_client_map;
    map<client_t,uint64_t> sseq_map;
    map<CInode*, map<client_t,Capability::Export> > cap_imports;
    
    // for lock/flock
    bool flock_was_waiting;

    // for snaps
    version_t stid;
    bufferlist snapidbl;

    // called when slave commits or aborts
    Context *slave_commit;
    bufferlist rollback_bl;

    More() : 
      srcdn_auth_mds(-1),
      src_reanchor_atid(0), dst_reanchor_atid(0), inode_import_v(0),
      rename_inode(0), is_freeze_authpin(false), is_ambiguous_auth(false),
      is_remote_frozen_authpin(false), is_inode_exporter(false),
      flock_was_waiting(false), stid(0), slave_commit(0) { }
  } *_more;


  // ---------------------------------------------------
  MDRequest() : 
    ref(1),
    session(0), item_session_request(this),
    client_request(0), straydn(NULL), snapid(CEPH_NOSNAP), tracei(0), tracedn(0),
    alloc_ino(0), used_prealloc_ino(0), snap_caps(0), did_early_reply(false),
    o_trunc(false),
    slave_request(0),
    internal_op(-1),
    retry(0),
    waited_for_osdmap(false),
    _more(0) {
    in[0] = in[1] = 0; 
  }
  MDRequest(metareqid_t ri, __u32 attempt, MClientRequest *req) : 
    Mutation(ri, attempt),
    ref(1),
    session(0), item_session_request(this),
    client_request(req), straydn(NULL), snapid(CEPH_NOSNAP), tracei(0), tracedn(0),
    alloc_ino(0), used_prealloc_ino(0), snap_caps(0), did_early_reply(false),
    o_trunc(false),
    slave_request(0),
    internal_op(-1),
    retry(0),
    waited_for_osdmap(false),
    _more(0) {
    in[0] = in[1] = 0; 
  }
  MDRequest(metareqid_t ri, __u32 attempt, int by) : 
    Mutation(ri, attempt, by),
    ref(1),
    session(0), item_session_request(this),
    client_request(0), straydn(NULL), snapid(CEPH_NOSNAP), tracei(0), tracedn(0),
    alloc_ino(0), used_prealloc_ino(0), snap_caps(0), did_early_reply(false),
    o_trunc(false),
    slave_request(0),
    internal_op(-1),
    retry(0),
    waited_for_osdmap(false),
    _more(0) {
    in[0] = in[1] = 0; 
  }
  ~MDRequest();

  MDRequest *get() {
    ++ref;
    return this;
  }
  void put() {
    if (--ref == 0)
      delete this;
  }
  
  More* more();
  bool has_more();
  bool are_slaves();
  bool slave_did_prepare();
  bool did_ino_allocation();
  bool freeze_auth_pin(CInode *inode);
  void unfreeze_auth_pin();
  void set_remote_frozen_auth_pin(CInode *inode);
  bool can_auth_pin(MDSCacheObject *object);
  void drop_local_auth_pins();
  void set_ambiguous_auth(CInode *inode);
  void clear_ambiguous_auth();

  void print(ostream &out);
};


struct MDSlaveUpdate {
  int origop;
  bufferlist rollback;
  elist<MDSlaveUpdate*>::item item;
  Context *waiter;
  set<CDir*> olddirs;
  set<CInode*> unlinked;
  MDSlaveUpdate(int oo, bufferlist &rbl, elist<MDSlaveUpdate*> &list) :
    origop(oo),
    item(this),
    waiter(0) {
    rollback.claim(rbl);
    list.push_back(&item);
  }
  ~MDSlaveUpdate() {
    item.remove_myself();
    if (waiter)
      waiter->finish(0);
    delete waiter;
  }
};


#endif
