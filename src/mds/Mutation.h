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
#include "include/filepath.h"

#include "MDSCacheObject.h"

#include "SimpleLock.h"
#include "Capability.h"

#include "common/TrackedOp.h"

class LogSegment;
class Capability;
class CInode;
class CDir;
class CDentry;
class Session;
class ScatterLock;
class MClientRequest;
class MMDSSlaveRequest;

struct MutationImpl {
  metareqid_t reqid;
  __u32 attempt;      // which attempt for this request
  LogSegment *ls;  // the log segment i'm committing to

private:
  utime_t mds_stamp; ///< mds-local timestamp (real time)
  utime_t op_stamp;  ///< op timestamp (client provided)

public:
  // flag mutation as slave
  mds_rank_t slave_to_mds;                // this is a slave request if >= 0.

  // -- my pins and locks --
  // cache pins (so things don't expire)
  set< MDSCacheObject* > pins;
  set<CInode*> stickydirs;

  // auth pins
  map<MDSCacheObject*, mds_rank_t> remote_auth_pins;
  set< MDSCacheObject* > auth_pins;
  
  // held locks
  set< SimpleLock* > rdlocks;  // always local.
  set< SimpleLock* > wrlocks;  // always local.
  map< SimpleLock*, mds_rank_t > remote_wrlocks;
  set< SimpleLock* > xlocks;   // local or remote.
  set< SimpleLock*, SimpleLock::ptr_lt > locks;  // full ordering

  // lock we are currently trying to acquire.  if we give up for some reason,
  // be sure to eval() this.
  SimpleLock *locking;
  mds_rank_t locking_target_mds;

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

  // keep our default values synced with MDRequestParam's
  MutationImpl()
    : attempt(0),
      ls(0),
      slave_to_mds(MDS_RANK_NONE),
      locking(NULL),
      locking_target_mds(-1),
      done_locking(false), committing(false), aborted(false), killed(false) { }
  MutationImpl(metareqid_t ri, __u32 att=0, mds_rank_t slave_to=MDS_RANK_NONE)
    : reqid(ri), attempt(att),
      ls(0),
      slave_to_mds(slave_to), 
      locking(NULL),
      locking_target_mds(-1),
      done_locking(false), committing(false), aborted(false), killed(false) { }
  virtual ~MutationImpl() {
    assert(locking == NULL);
    assert(pins.empty());
    assert(auth_pins.empty());
    assert(xlocks.empty());
    assert(rdlocks.empty());
    assert(wrlocks.empty());
    assert(remote_wrlocks.empty());
  }

  bool is_master() const { return slave_to_mds == MDS_RANK_NONE; }
  bool is_slave() const { return slave_to_mds != MDS_RANK_NONE; }

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
struct MDRequestImpl : public MutationImpl, public TrackedOp {
  Session *session;
  elist<MDRequestImpl*>::item item_session_request;  // if not on list, op is aborted.

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
  int getattr_caps;       ///< caps requested by getattr
  bool did_early_reply;
  bool o_trunc;           ///< request is an O_TRUNC mutation
  bool has_completed;     ///< request has already completed

  bufferlist reply_extra_bl;

  // inos we did a embedded cap release on, and may need to eval if we haven't since reissued
  map<vinodeno_t, ceph_seq_t> cap_releases;  

  // -- i am a slave request
  MMDSSlaveRequest *slave_request; // slave request (if one is pending; implies slave == true)

  // -- i am an internal op
  int internal_op;
  Context *internal_op_finish;
  void *internal_op_private;

  // indicates how may retries of request have been made
  int retry;

  // indicator for vxattr osdmap update
  bool waited_for_osdmap;

  // break rarely-used fields into a separately allocated structure 
  // to save memory for most ops
  struct More {
    int slave_error;
    set<mds_rank_t> slaves;           // mds nodes that have slave requests to me (implies client_request)
    set<mds_rank_t> waiting_on_slave; // peers i'm waiting for slavereq replies from. 

    // for rename/link/unlink
    set<mds_rank_t> witnessed;       // nodes who have journaled a RenamePrepare
    map<MDSCacheObject*,version_t> pvmap;

    bool has_journaled_slaves;
    bool slave_update_journaled;
    
    // for rename
    set<mds_rank_t> extra_witnesses; // replica list from srcdn auth (rename)
    mds_rank_t srcdn_auth_mds;
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

    list<MDSInternalContextBase*> waiting_for_finish;

    // export & fragment
    CDir* export_dir;
    dirfrag_t fragment_base;

    // for internal ops doing lookup
    filepath filepath1;
    filepath filepath2;

    More() : 
      slave_error(0),
      has_journaled_slaves(false), slave_update_journaled(false),
      srcdn_auth_mds(-1), inode_import_v(0), rename_inode(0),
      is_freeze_authpin(false), is_ambiguous_auth(false),
      is_remote_frozen_authpin(false), is_inode_exporter(false),
      flock_was_waiting(false), stid(0), slave_commit(0), export_dir(NULL)  { }
  } *_more;


  // ---------------------------------------------------
  struct Params {
    metareqid_t reqid;
    __u32 attempt;
    MClientRequest *client_req;
    class Message *triggering_slave_req;
    mds_rank_t slave_to;
    utime_t initiated;
    utime_t throttled, all_read, dispatched;
    int internal_op;
    // keep these default values synced to MutationImpl's
    Params() : attempt(0), client_req(NULL),
        triggering_slave_req(NULL), slave_to(MDS_RANK_NONE), internal_op(-1) {}
  };
  MDRequestImpl(const Params& params, OpTracker *tracker) :
    MutationImpl(params.reqid, params.attempt, params.slave_to),
    TrackedOp(tracker, params.initiated),
    session(NULL), item_session_request(this),
    client_request(params.client_req), straydn(NULL), snapid(CEPH_NOSNAP),
    tracei(NULL), tracedn(NULL), alloc_ino(0), used_prealloc_ino(0),
    snap_caps(0), getattr_caps(0),
    did_early_reply(false), o_trunc(false), has_completed(false),
    slave_request(NULL), internal_op(params.internal_op), internal_op_finish(NULL),
    internal_op_private(NULL),
    retry(0),
    waited_for_osdmap(false), _more(NULL) {
    in[0] = in[1] = NULL;
    if (!params.throttled.is_zero())
      tracker->mark_event(this, "throttled", params.throttled);
    if (!params.all_read.is_zero())
      tracker->mark_event(this, "all_read", params.all_read);
    if (!params.dispatched.is_zero())
      tracker->mark_event(this, "dispatched", params.dispatched);
  }
  ~MDRequestImpl();
  
  More* more();
  bool has_more();
  bool has_witnesses();
  bool slave_did_prepare();
  bool did_ino_allocation();
  bool freeze_auth_pin(CInode *inode);
  void unfreeze_auth_pin(bool clear_inode=false);
  void set_remote_frozen_auth_pin(CInode *inode);
  bool can_auth_pin(MDSCacheObject *object);
  void drop_local_auth_pins();
  void set_ambiguous_auth(CInode *inode);
  void clear_ambiguous_auth();
  const filepath& get_filepath();
  const filepath& get_filepath2();
  void set_filepath(const filepath& fp);
  void set_filepath2(const filepath& fp);

  void print(ostream &out) const override;
  void dump(Formatter *f) const override;

  // TrackedOp stuff
  typedef ceph::shared_ptr<MDRequestImpl> Ref;
protected:
  void _dump_op_descriptor_unlocked(ostream& stream) const;
};

typedef ceph::shared_ptr<MDRequestImpl> MDRequestRef;


struct MDSlaveUpdate {
  int origop;
  bufferlist rollback;
  elist<MDSlaveUpdate*>::item item;
  Context *waiter;
  set<CInode*> olddirs;
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
      waiter->complete(0);
  }
};


#endif
