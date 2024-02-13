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

#include <limits>

#include "include/interval_set.h"
#include "include/elist.h"
#include "include/filepath.h"

#include "MDSCacheObject.h"
#include "MDSContext.h"

#include "SimpleLock.h"
#include "Capability.h"
#include "BatchOp.h"

#include "common/TrackedOp.h"
#include "messages/MClientRequest.h"
#include "messages/MMDSPeerRequest.h"
#include "messages/MClientReply.h"

class LogSegment;
class CInode;
class CDir;
class CDentry;
class Session;
class ScatterLock;
struct sr_t;
struct MDLockCache;

struct MutationImpl : public TrackedOp {
public:
  // -- my pins and auth_pins --
  struct ObjectState {
    bool pinned = false;
    bool auth_pinned = false;
    mds_rank_t remote_auth_pinned = MDS_RANK_NONE;
  };

  // held locks
  struct LockOp {
    enum {
      RDLOCK		= 1,
      WRLOCK		= 2,
      XLOCK		= 4,
      REMOTE_WRLOCK	= 8,
      STATE_PIN		= 16, // no RW after locked, just pin lock state
    };

    LockOp(SimpleLock *l, unsigned f=0, mds_rank_t t=MDS_RANK_NONE) :
      lock(l), flags(f), wrlock_target(t) {}

    bool is_rdlock() const { return !!(flags & RDLOCK); }
    bool is_xlock() const { return !!(flags & XLOCK); }
    bool is_wrlock() const { return !!(flags & WRLOCK); }
    void clear_wrlock() const { flags &= ~WRLOCK; }
    bool is_remote_wrlock() const { return !!(flags & REMOTE_WRLOCK); }
    void clear_remote_wrlock() const {
      flags &= ~REMOTE_WRLOCK;
      wrlock_target = MDS_RANK_NONE;
    }
    bool is_state_pin() const { return !!(flags & STATE_PIN); }
    bool operator<(const LockOp& r) const {
      return lock < r.lock;
    }

    void print(std::ostream& out) const {
      CachedStackStringStream css;
      *css << "0x" << std::hex << flags;
      out << "LockOp(l=" << *lock << ",f=" << css->strv();
      if (wrlock_target != MDS_RANK_NONE) {
        out << ",wt=" << wrlock_target;
      }
      out << ")";
    }

    SimpleLock* lock;
    mutable unsigned flags;
    mutable mds_rank_t wrlock_target;
  };

  struct LockOpVec : public std::vector<LockOp> {
    LockOpVec() {
      reserve(32);
    }

    void add_rdlock(SimpleLock *lock, int idx=-1) {
      add_lock(LockOp(lock, LockOp::RDLOCK), idx);
    }
    void erase_rdlock(SimpleLock *lock);
    void add_xlock(SimpleLock *lock, int idx=-1) {
      add_lock(LockOp(lock, LockOp::XLOCK), idx);
    }
    void add_wrlock(SimpleLock *lock, int idx=-1) {
      add_lock(LockOp(lock, LockOp::WRLOCK), idx);
    }
    void add_remote_wrlock(SimpleLock *lock, mds_rank_t rank) {
      ceph_assert(rank != MDS_RANK_NONE);
      add_lock(LockOp(lock, LockOp::REMOTE_WRLOCK, rank), -1);
    }
    void lock_scatter_gather(SimpleLock *lock) {
      add_lock(LockOp(lock, LockOp::WRLOCK | LockOp::STATE_PIN), -1);
    }
    void sort_and_merge();

  protected:
    void add_lock(LockOp op, int idx) {
      if (idx >= 0) {
	emplace(cbegin() + idx, std::move(op));
      } else {
	emplace_back(std::move(op));
      }
    }
  };

  using lock_set = std::set<LockOp>;
  using lock_iterator = lock_set::iterator;

  // keep our default values synced with MDRequestParam's
  MutationImpl() : TrackedOp(nullptr, ceph_clock_now()) {}
  MutationImpl(OpTracker *tracker, utime_t initiated,
	       const metareqid_t &ri, __u32 att=0, mds_rank_t peer_to=MDS_RANK_NONE)
    : TrackedOp(tracker, initiated),
      reqid(ri), attempt(att),
      peer_to_mds(peer_to) {}
  ~MutationImpl() override {
    ceph_assert(!locking);
    ceph_assert(!lock_cache);
    ceph_assert(num_pins == 0);
    ceph_assert(num_auth_pins == 0);
  }

  const ObjectState* find_object_state(MDSCacheObject *obj) const {
    auto it = object_states.find(obj);
    return it != object_states.end() ? &it->second : nullptr;
  }

  bool is_any_remote_auth_pin() const { return num_remote_auth_pins > 0; }

  void disable_lock_cache() {
    lock_cache_disabled = true;
  }

  lock_iterator emplace_lock(SimpleLock *l, unsigned f=0, mds_rank_t t=MDS_RANK_NONE) {
    last_locked = l;
    return locks.emplace(l, f, t).first;
  }

  bool is_rdlocked(SimpleLock *lock) const;
  bool is_wrlocked(SimpleLock *lock) const;
  bool is_xlocked(SimpleLock *lock) const {
    auto it = locks.find(lock);
    return it != locks.end() && it->is_xlock();
  }
  bool is_remote_wrlocked(SimpleLock *lock) const {
    auto it = locks.find(lock);
    return it != locks.end() && it->is_remote_wrlock();
  }
  bool is_last_locked(SimpleLock *lock) const {
    return lock == last_locked;
  }

  bool is_leader() const { return peer_to_mds == MDS_RANK_NONE; }
  bool is_peer() const { return peer_to_mds != MDS_RANK_NONE; }

  client_t get_client() const {
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
  void pin(MDSCacheObject *object);
  void unpin(MDSCacheObject *object);
  void set_stickydirs(CInode *in);
  void put_stickydirs();
  void drop_pins();

  void start_locking(SimpleLock *lock, int target=-1);
  void finish_locking(SimpleLock *lock);

  // auth pins
  bool is_auth_pinned(MDSCacheObject *object) const;
  void auth_pin(MDSCacheObject *object);
  void auth_unpin(MDSCacheObject *object);
  void drop_local_auth_pins();
  void set_remote_auth_pinned(MDSCacheObject* object, mds_rank_t from);
  void _clear_remote_auth_pinned(ObjectState& stat);

  void add_projected_node(MDSCacheObject* obj) {
    projected_nodes.insert(obj);
  }
  void remove_projected_node(MDSCacheObject* obj) {
    projected_nodes.erase(obj);
  }
  bool is_projected(MDSCacheObject *obj) const {
    return projected_nodes.count(obj);
  }
  void add_updated_lock(ScatterLock *lock);
  void add_cow_inode(CInode *in);
  void add_cow_dentry(CDentry *dn);
  void apply();
  void cleanup();

  virtual void print(std::ostream &out) const {
    out << "mutation(" << this << ")";
  }

  virtual void dump(ceph::Formatter *f) const {
    _dump(f);
  }
  void _dump_op_descriptor(std::ostream& stream) const override;

  metareqid_t reqid;
  int result = std::numeric_limits<int>::min();
  __u32 attempt = 0;      // which attempt for this request
  LogSegment *ls = nullptr;  // the log segment i'm committing to

  // flag mutation as peer
  mds_rank_t peer_to_mds = MDS_RANK_NONE;  // this is a peer request if >= 0.

  ceph::unordered_map<MDSCacheObject*, ObjectState> object_states;
  int num_pins = 0;
  int num_auth_pins = 0;
  int num_remote_auth_pins = 0;
  // cache pins (so things don't expire)
  CInode* stickydiri = nullptr;

  lock_set locks;  // full ordering
  MDLockCache* lock_cache = nullptr;
  bool lock_cache_disabled = false;
  SimpleLock *last_locked = nullptr;
  // Lock we are currently trying to acquire. If we give up for some reason,
  // be sure to eval() this.
  SimpleLock *locking = nullptr;
  mds_rank_t locking_target_mds = -1;

  // if this flag is set, do not attempt to acquire further locks.
  //  (useful for wrlock, which may be a moving auth target)
  enum {
    SNAP_LOCKED		= 1,
    SNAP2_LOCKED	= 2,
    PATH_LOCKED		= 4,
    ALL_LOCKED		= 8,
  };
  int locking_state = 0;

  bool committing = false;
  bool aborted = false;
  bool killed = false;
  bool dead = false;

  // for applying projected inode changes
  std::set<MDSCacheObject*> projected_nodes;
  std::list<ScatterLock*> updated_locks;

  std::list<CInode*> dirty_cow_inodes;
  std::list<std::pair<CDentry*,version_t> > dirty_cow_dentries;

private:
  utime_t mds_stamp; ///< mds-local timestamp (real time)
  utime_t op_stamp;  ///< op timestamp (client provided)
};

/**
 * MDRequestImpl: state we track for requests we are currently processing.
 * mostly information about locks held, so that we can drop them all
 * the request is finished or forwarded. see request_*().
 */
struct MDRequestImpl : public MutationImpl {
  // TrackedOp stuff
  typedef boost::intrusive_ptr<MDRequestImpl> Ref;

  // break rarely-used fields into a separately allocated structure 
  // to save memory for most ops
  struct More {
    More() {}

    int peer_error = 0;
    std::set<mds_rank_t> peers;           // mds nodes that have peer requests to me (implies client_request)
    std::set<mds_rank_t> waiting_on_peer; // peers i'm waiting for peerreq replies from.

    // for rename/link/unlink
    std::set<mds_rank_t> witnessed;       // nodes who have journaled a RenamePrepare
    std::map<MDSCacheObject*,version_t> pvmap;

    bool has_journaled_peers = false;
    bool peer_update_journaled = false;
    bool peer_rolling_back = false;
    
    // for rename
    std::set<mds_rank_t> extra_witnesses; // replica list from srcdn auth (rename)
    mds_rank_t srcdn_auth_mds = MDS_RANK_NONE;
    ceph::buffer::list inode_import;
    version_t inode_import_v = 0;
    CInode* rename_inode = nullptr;
    bool is_freeze_authpin = false;
    bool is_ambiguous_auth = false;
    bool is_remote_frozen_authpin = false;
    bool is_inode_exporter = false;
    bool rdonly_checks = false;

    std::map<client_t, std::pair<Session*, uint64_t> > imported_session_map;
    std::map<CInode*, std::map<client_t,Capability::Export> > cap_imports;
    
    // for lock/flock
    bool flock_was_waiting = false;

    // for snaps
    version_t stid = 0;
    ceph::buffer::list snapidbl;

    sr_t *srci_srnode = nullptr;
    sr_t *desti_srnode = nullptr;

    // called when peer commits or aborts
    Context *peer_commit = nullptr;
    ceph::buffer::list rollback_bl;

    MDSContext::vec waiting_for_finish;

    // export & fragment
    CDir* export_dir = nullptr;
    dirfrag_t fragment_base;

    // for internal ops doing lookup
    filepath filepath1;
    filepath filepath2;
  } *_more = nullptr;

  // ---------------------------------------------------
  struct Params {
    // keep these default values synced to MutationImpl's
    Params() {}
    const utime_t& get_recv_stamp() const {
      return initiated;
    }
    const utime_t& get_throttle_stamp() const {
      return throttled;
    }
    const utime_t& get_recv_complete_stamp() const {
      return all_read;
    }
    const utime_t& get_dispatch_stamp() const {
      return dispatched;
    }
    metareqid_t reqid;
    __u32 attempt = 0;
    ceph::cref_t<MClientRequest> client_req;
    ceph::cref_t<Message> triggering_peer_req;
    mds_rank_t peer_to = MDS_RANK_NONE;
    utime_t initiated;
    utime_t throttled, all_read, dispatched;
    int internal_op = -1;
  };
  MDRequestImpl(const Params* params, OpTracker *tracker) :
    MutationImpl(tracker, params->initiated,
		 params->reqid, params->attempt, params->peer_to),
    item_session_request(this), client_request(params->client_req),
    internal_op(params->internal_op) {}
  ~MDRequestImpl() override;
  
  More* more();
  More const* more() const;
  bool has_more() const;
  bool has_witnesses();
  bool peer_did_prepare();
  bool peer_rolling_back();
  bool freeze_auth_pin(CInode *inode);
  void unfreeze_auth_pin(bool clear_inode=false);
  void set_remote_frozen_auth_pin(CInode *inode);
  bool can_auth_pin(MDSCacheObject *object);
  void drop_local_auth_pins();
  void set_ambiguous_auth(CInode *inode);
  void clear_ambiguous_auth();
  const filepath& get_filepath() const;
  const filepath& get_filepath2() const;
  void set_filepath(const filepath& fp);
  void set_filepath2(const filepath& fp);
  bool is_queued_for_replay() const;
  bool get_queued_next_replay_op() const {
    return queued_next_replay_op;
  }
  void set_queued_next_replay_op() {
    queued_next_replay_op = true;
  }
  int compare_paths();

  bool can_batch();
  bool is_batch_head() {
    return batch_op_map != nullptr;
  }
  std::unique_ptr<BatchOp> release_batch_op();

  void print(std::ostream &out) const override;
  void dump_with_mds_lock(ceph::Formatter* f) const {
    return _dump(f, true);
  }

  ceph::cref_t<MClientRequest> release_client_request();
  void reset_peer_request(const ceph::cref_t<MMDSPeerRequest>& req=nullptr);

  Session *session = nullptr;
  elist<MDRequestImpl*>::item item_session_request;  // if not on list, op is aborted.

  // -- i am a client (leader) request
  ceph::cref_t<MClientRequest> client_request; // client request (if any)

  // tree and depth info of path1 and path2
  inodeno_t dir_root[2] = {0, 0};
  int dir_depth[2] = {-1, -1};
  file_layout_t dir_layout;
  // store up to two sets of dn vectors, inode pointers, for request path1 and path2.
  std::vector<CDentry*> dn[2];
  CInode *in[2] = {};
  CDentry *straydn = nullptr;
  snapid_t snapid = CEPH_NOSNAP;
  snapid_t snapid_diff_other = CEPH_NOSNAP;

  CInode *tracei = nullptr;
  CDentry *tracedn = nullptr;

  inodeno_t alloc_ino = 0, used_prealloc_ino = 0;
  interval_set<inodeno_t> prealloc_inos;

  int snap_caps = 0;
  int getattr_caps = 0;		///< caps requested by getattr
  bool no_early_reply = false;
  bool did_early_reply = false;
  bool o_trunc = false;		///< request is an O_TRUNC mutation
  bool has_completed = false;	///< request has already completed

  ceph::buffer::list reply_extra_bl;

  // inos we did a embedded cap release on, and may need to eval if we haven't since reissued
  std::map<vinodeno_t, ceph_seq_t> cap_releases;

  // -- i am a peer request
  ceph::cref_t<MMDSPeerRequest> peer_request; // peer request (if one is pending; implies peer == true)

  // -- i am an internal op
  int internal_op;
  Context *internal_op_finish = nullptr;
  void *internal_op_private = nullptr;

  // indicates how may retries of request have been made
  int retry = 0;

  std::map<int, std::unique_ptr<BatchOp> > *batch_op_map = nullptr;

  // indicator for vxattr osdmap update
  bool waited_for_osdmap = false;

protected:
  void _dump(ceph::Formatter *f) const override {
    _dump(f, false);
  }
  void _dump(ceph::Formatter *f, bool has_mds_lock) const;
  void _dump_op_descriptor(std::ostream& stream) const override;
  bool queued_next_replay_op = false;
};

struct MDPeerUpdate {
  MDPeerUpdate(int oo, ceph::buffer::list &rbl) :
    origop(oo) {
    rollback = std::move(rbl);
  }
  ~MDPeerUpdate() {
    if (waiter)
      waiter->complete(0);
  }
  int origop;
  ceph::buffer::list rollback;
  Context *waiter = nullptr;
  std::set<CInode*> olddirs;
  std::set<CInode*> unlinked;
};

struct MDLockCacheItem {
  MDLockCache *parent = nullptr;
  elist<MDLockCacheItem*>::item item_lock;
};

struct MDLockCache : public MutationImpl {
  using LockItem = MDLockCacheItem;

  struct DirItem {
    MDLockCache *parent = nullptr;
    elist<DirItem*>::item item_dir;
  };

  MDLockCache(Capability *cap, int op) :
    MutationImpl(), diri(cap->get_inode()), client_cap(cap), opcode(op) {
    client_cap->lock_caches.push_back(&item_cap_lock_cache);
  }

  CInode *get_dir_inode() { return diri; }
  void set_dir_layout(file_layout_t& layout) {
    dir_layout = layout;
  }
  const file_layout_t& get_dir_layout() const {
    return dir_layout;
  }

  void attach_locks();
  void attach_dirfrags(std::vector<CDir*>&& dfv);
  void detach_locks();
  void detach_dirfrags();

  CInode *diri;
  Capability *client_cap;
  int opcode;
  file_layout_t dir_layout;

  elist<MDLockCache*>::item item_cap_lock_cache;

  // link myself to locked locks
  std::unique_ptr<LockItem[]> items_lock;

  // link myself to auth-pinned dirfrags
  std::unique_ptr<DirItem[]> items_dir;
  std::vector<CDir*> auth_pinned_dirfrags;

  int ref = 1;
  bool invalidating = false;
};

typedef boost::intrusive_ptr<MutationImpl> MutationRef;
typedef boost::intrusive_ptr<MDRequestImpl> MDRequestRef;

#endif
