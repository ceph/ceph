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

#include "mdstypes.h"
#include "CObject.h"
#include "SimpleLock.h"
#include "include/elist.h"
#include "common/Mutex.h"

class LogSegment;
class Session;
class MClientRequest;
class filepath;
class ScatterLock;

struct MutationImpl {
  metareqid_t reqid;
  __u32 attempt;      // which attempt for this request
  LogSegment *ls;  // the log segment i'm committing to

  utime_t mds_stamp; ///< mds-local timestamp (real time)
  utime_t op_stamp;  ///< op timestamp (client provided)

  set<CObjectRef> pins;

  // for applying projected inode/fnode changes
  list<CObject*> projected_nodes[2];
  map<CInode*, int> updated_locks;

  // mutex locks we hold
  set<CObject*> locked_objects;

  // held locks
  set< SimpleLock* > rdlocks;  // always local.
  set< SimpleLock* > wrlocks;  // always local.
  set< SimpleLock* > xlocks;   // local or remote.
  set< SimpleLock*, SimpleLock::ptr_lt > locks;  // full ordering

  SimpleLock *locking;
  bool locking_xlock;
  bool locking_done;

  std::atomic<bool> committing;

  // keep our default values synced with MDRequestParam's
  MutationImpl(metareqid_t ri, __u32 att=0)
    : reqid(ri), attempt(att), ls(0),
      locking(NULL), locking_xlock(false), locking_done(false),
      committing(ATOMIC_VAR_INIT(false)) { }
  MutationImpl() : MutationImpl(metareqid_t()) {}
  virtual ~MutationImpl() { }

  client_t get_client() {
    if (reqid.name.is_client())
      return client_t(reqid.name.num());
    return -1;
  }
  void set_mds_stamp(utime_t t) {
    mds_stamp = t;
  }
  utime_t get_mds_stamp() const {
    return mds_stamp;
  }
  void set_op_stamp(utime_t t) {
    op_stamp = t;
  }
  utime_t get_op_stamp() const {
    if (op_stamp != utime_t())
      return op_stamp;
    return get_mds_stamp();
  }

  // pin items in cache
  void pin(CObject *o);
  void unpin(CObject *o);
  void drop_pins();

  void add_projected_inode(CInode *in, bool early);
  void add_projected_fnode(CDir *dir, bool early);
  void add_updated_lock(CInode *in, int mask);
  void pop_and_dirty_projected_nodes();
  void pop_and_dirty_early_projected_nodes();
  CObject* pop_early_projected_node();

  void lock_object(CObject *o);
  void unlock_object(CObject *o);
  void unlock_all_objects();
  void add_locked_object(CObject *o);
  void remove_locked_object(CObject *o);
  bool is_object_locked(CObject *o) {
    return locked_objects.count(o);
  }
  bool is_any_object_locked() {
    return !locked_objects.empty();
  }

  void start_locking(SimpleLock *lock, bool xlock);
  void finish_locking(SimpleLock *lock);

  void apply();
  void early_apply();
  void cleanup();

  void start_committing() {
    std::atomic_store(&committing, true);
  }
  bool is_committing() {
    return std::atomic_load(&committing);
  }
  void wait_committing() {
    while(!is_committing());
  }

  virtual void print(ostream &out) const {
    out << "mutation(" << this << ")";
  }

  virtual void dump(Formatter *f) const {}
};

inline ostream& operator<<(ostream &out, const MutationImpl &mut)
{
  mut.print(out);
  return out;
}

typedef ceph::shared_ptr<MutationImpl> MutationRef;


/** active_request_t
 * state we track for requests we are currently processing.
 * mostly information about locks held, so that we can drop them all
 * the request is finished or forwarded.  see request_*().
 */
struct MDRequestImpl : public MutationImpl {
  Mutex dispatch_mutex;

  Session *session;
  elist<MDRequestImpl*>::item item_session_request;  // if not on list, op is aborted.

  // -- i am a client (master) request
  MClientRequest *client_request; // client request (if any)

  // store up to two sets of dn vectors, inode pointers, for request path1 and path2.
  vector<CDentryRef> dn[2];
  CInodeRef in[2];
  CDentryRef straydn;

  int tracei;
  int tracedn;

  bufferlist reply_extra_bl;

  inodeno_t alloc_ino, used_prealloc_ino;
  interval_set<inodeno_t> prealloc_inos;

  bool completed;
  bool killed;
  bool hold_rename_dir_mutex;
  bool did_early_reply;
  bool replaying;

  int getattr_mask;
  int retries;

  // ---------------------------------------------------
  struct Params {
    metareqid_t reqid;
    __u32 attempt;
    MClientRequest *client_req;
    // keep these default values synced to MutationImpl's
    Params() : attempt(0), client_req(NULL) {}
  };
  MDRequestImpl(const Params& params) :
    MutationImpl(params.reqid, params.attempt),
    dispatch_mutex("MDRequestImpl::dispatch_mutex"),
    session(NULL),
    client_request(params.client_req),
    straydn(NULL), tracei(-1), tracedn(-1),
    completed(false), killed(false),
    hold_rename_dir_mutex(false),
    did_early_reply(false),
    replaying(false),
    getattr_mask(0), retries(0) { }
  ~MDRequestImpl();
  
  const filepath& get_filepath();
  const filepath& get_filepath2();
};

typedef ceph::shared_ptr<MDRequestImpl> MDRequestRef;
#endif
