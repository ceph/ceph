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



#ifndef CEPH_CDENTRY_H
#define CEPH_CDENTRY_H

#include <string>
#include <set>

#include "include/types.h"
#include "include/buffer_fwd.h"
#include "include/lru.h"
#include "include/elist.h"
#include "include/filepath.h"
#include "mdstypes.h"

#include "SimpleLock.h"
#include "LocalLock.h"
#include "ScrubHeader.h"

class CInode;
class CDir;
class Locker;
class Message;
class CDentry;
class LogSegment;

class Session;



// define an ordering
bool operator<(const CDentry& l, const CDentry& r);

// dentry
class CDentry : public MDSCacheObject, public LRUObject {
  /*
   * This class uses a boost::pool to handle allocation. This is *not*
   * thread-safe, so don't do allocations from multiple threads!
   *
   * Alternatively, switch the pool to use a boost::singleton_pool.
   */

private:
  static boost::pool<> pool;
public:
  static void *operator new(size_t num_bytes) { 
    void *n = pool.malloc();
    if (!n)
      throw std::bad_alloc();
    return n;
  }
  void operator delete(void *p) {
    pool.free(p);
  }

public:
  // -- state --
  static const int STATE_NEW =          (1<<0);
  static const int STATE_FRAGMENTING =  (1<<1);
  static const int STATE_PURGING =      (1<<2);
  static const int STATE_BADREMOTEINO = (1<<3);
  static const int STATE_EVALUATINGSTRAY = (1<<4);
  // stray dentry needs notification of releasing reference
  static const int STATE_STRAY =	STATE_NOTIFYREF;

  // -- pins --
  static const int PIN_INODEPIN =     1;  // linked inode is pinned
  static const int PIN_FRAGMENTING = -2;  // containing dir is refragmenting
  static const int PIN_PURGING =      3;
  static const int PIN_SCRUBPARENT =  4;

  const char *pin_name(int p) const {
    switch (p) {
    case PIN_INODEPIN: return "inodepin";
    case PIN_FRAGMENTING: return "fragmenting";
    case PIN_PURGING: return "purging";
    case PIN_SCRUBPARENT: return "scrubparent";
    default: return generic_pin_name(p);
    }
  }

  // -- wait --
  //static const int WAIT_LOCK_OFFSET = 8;

  void add_waiter(uint64_t tag, MDSInternalContextBase *c);

  static const unsigned EXPORT_NONCE = 1;

  bool is_lt(const MDSCacheObject *r) const {
    return *this < *static_cast<const CDentry*>(r);
  }

public:
  std::string name;
  __u32 hash;
  snapid_t first, last;

  dentry_key_t key() { 
    return dentry_key_t(last, name.c_str()); 
  }

public:
  struct linkage_t {
    CInode *inode;
    inodeno_t remote_ino;
    unsigned char remote_d_type;
    
    linkage_t() : inode(0), remote_ino(0), remote_d_type(0) {}

    // dentry type is primary || remote || null
    // inode ptr is required for primary, optional for remote, undefined for null
    bool is_primary() const { return remote_ino == 0 && inode != 0; }
    bool is_remote() const { return remote_ino > 0; }
    bool is_null() const { return remote_ino == 0 && inode == 0; }

    CInode *get_inode() { return inode; }
    const CInode *get_inode() const { return inode; }
    inodeno_t get_remote_ino() const { return remote_ino; }
    unsigned char get_remote_d_type() const { return remote_d_type; }
    std::string get_remote_d_type_string() const;

    void set_remote(inodeno_t ino, unsigned char d_type) { 
      remote_ino = ino;
      remote_d_type = d_type;
      inode = 0;
    }
    void link_remote(CInode *in);
  };

protected:
  CDir *dir;     // containing dirfrag
  linkage_t linkage;
  list<linkage_t> projected;
  
  version_t version;  // dir version when last touched.
  version_t projected_version;  // what it will be when i unlock/commit.

public:
  elist<CDentry*>::item item_dirty;
  elist<CDentry*>::item item_stray;

protected:
  friend class Migrator;
  friend class Locker;
  friend class MDCache;
  friend class StrayManager;
  friend class CInode;
  friend class C_MDC_XlockRequest;


public:
  // lock
  static LockType lock_type;
  static LockType versionlock_type;

  SimpleLock lock;
  LocalLock versionlock;

 public:
  // cons
  CDentry(const std::string& n, __u32 h,
	  snapid_t f, snapid_t l) :
    name(n), hash(h),
    first(f), last(l),
    dir(0),
    version(0), projected_version(0),
    item_dirty(this),
    lock(this, &lock_type),
    versionlock(this, &versionlock_type) {
    g_num_dn++;
    g_num_dna++;
  }
  CDentry(const std::string& n, __u32 h, inodeno_t ino, unsigned char dt,
	  snapid_t f, snapid_t l) :
    name(n), hash(h),
    first(f), last(l),
    dir(0),
    version(0), projected_version(0),
    item_dirty(this),
    lock(this, &lock_type),
    versionlock(this, &versionlock_type) {
    g_num_dn++;
    g_num_dna++;
    linkage.remote_ino = ino;
    linkage.remote_d_type = dt;
  }
  ~CDentry() {
    g_num_dn--;
    g_num_dns++;
  }


  const CDir *get_dir() const { return dir; }
  CDir *get_dir() { return dir; }
  const std::string& get_name() const { return name; }

  __u32 get_hash() const { return hash; }

  // linkage
  const linkage_t *get_linkage() const { return &linkage; }
  linkage_t *get_linkage() { return &linkage; }

  linkage_t *_project_linkage() {
    projected.push_back(linkage_t());
    return &projected.back();
  }
  void push_projected_linkage() {
    _project_linkage();
  }
  void push_projected_linkage(inodeno_t ino, char d_type) {
    linkage_t *p = _project_linkage();
    p->remote_ino = ino;
    p->remote_d_type = d_type;
  }
  void push_projected_linkage(CInode *inode); 
  linkage_t *pop_projected_linkage();

  bool is_projected() const { return !projected.empty(); }

  linkage_t *get_projected_linkage() {
    if (!projected.empty())
      return &projected.back();
    return &linkage;
  }
  CInode *get_projected_inode() {
    return get_projected_linkage()->inode;
  }

  bool use_projected(client_t client, const MutationRef& mut) const {
    return lock.can_read_projected(client) || 
      lock.get_xlock_by() == mut;
  }
  linkage_t *get_linkage(client_t client, const MutationRef& mut) {
    return use_projected(client, mut) ? get_projected_linkage() : get_linkage();
  }

  // ref counts: pin ourselves in the LRU when we're pinned.
  void first_get() {
    lru_pin();
  }
  void last_put() {
    lru_unpin();
  }
  void _put();

  // auth pins
  bool can_auth_pin() const;
  void auth_pin(void *by);
  void auth_unpin(void *by);
  void adjust_nested_auth_pins(int adjustment, int diradj, void *by);
  bool is_frozen() const;
  bool is_freezing() const;
  int get_num_dir_auth_pins() const;
  
  // remote links
  void link_remote(linkage_t *dnl, CInode *in);
  void unlink_remote(linkage_t *dnl);
  
  // copy cons
  CDentry(const CDentry& m);
  const CDentry& operator= (const CDentry& right);

  // misc
  void make_path_string(std::string& s) const;
  void make_path(filepath& fp) const;

  // -- version --
  version_t get_version() const { return version; }
  void set_version(version_t v) { projected_version = version = v; }
  version_t get_projected_version() const { return projected_version; }
  void set_projected_version(version_t v) { projected_version = v; }
  
  mds_authority_t authority() const;

  version_t pre_dirty(version_t min=0);
  void _mark_dirty(LogSegment *ls);
  void mark_dirty(version_t projected_dirv, LogSegment *ls);
  void mark_clean();

  void mark_new();
  bool is_new() const { return state_test(STATE_NEW); }
  void clear_new() { state_clear(STATE_NEW); }
  
  // -- replication
  void encode_replica(mds_rank_t mds, bufferlist& bl) {
    if (!is_replicated())
      lock.replicate_relax();

    __u32 nonce = add_replica(mds);
    ::encode(nonce, bl);
    ::encode(first, bl);
    ::encode(linkage.remote_ino, bl);
    ::encode(linkage.remote_d_type, bl);
    __s32 ls = lock.get_replica_state();
    ::encode(ls, bl);
  }
  void decode_replica(bufferlist::iterator& p, bool is_new);

  // -- exporting
  // note: this assumes the dentry already exists.  
  // i.e., the name is already extracted... so we just need the other state.
  void encode_export(bufferlist& bl) {
    ::encode(first, bl);
    ::encode(state, bl);
    ::encode(version, bl);
    ::encode(projected_version, bl);
    ::encode(lock, bl);
    ::encode(replica_map, bl);
    get(PIN_TEMPEXPORTING);
  }
  void finish_export() {
    // twiddle
    clear_replica_map();
    replica_nonce = EXPORT_NONCE;
    state_clear(CDentry::STATE_AUTH);
    if (is_dirty())
      mark_clean();
    put(PIN_TEMPEXPORTING);
  }
  void abort_export() {
    put(PIN_TEMPEXPORTING);
  }
  void decode_import(bufferlist::iterator& blp, LogSegment *ls) {
    ::decode(first, blp);
    __u32 nstate;
    ::decode(nstate, blp);
    ::decode(version, blp);
    ::decode(projected_version, blp);
    ::decode(lock, blp);
    ::decode(replica_map, blp);

    // twiddle
    state = 0;
    state_set(CDentry::STATE_AUTH);
    if (nstate & STATE_DIRTY)
      _mark_dirty(ls);
    if (!replica_map.empty())
      get(PIN_REPLICATED);
  }

  // -- locking --
  SimpleLock* get_lock(int type) {
    assert(type == CEPH_LOCK_DN);
    return &lock;
  }
  void set_object_info(MDSCacheObjectInfo &info);
  void encode_lock_state(int type, bufferlist& bl);
  void decode_lock_state(int type, bufferlist& bl);


  // ---------------------------------------------
  // replicas (on clients)
 public:
  map<client_t,ClientLease*> client_lease_map;

  bool is_any_leases() const {
    return !client_lease_map.empty();
  }
  const ClientLease *get_client_lease(client_t c) const {
    if (client_lease_map.count(c))
      return client_lease_map.find(c)->second;
    return 0;
  }
  ClientLease *get_client_lease(client_t c) {
    if (client_lease_map.count(c))
      return client_lease_map.find(c)->second;
    return 0;
  }
  bool have_client_lease(client_t c) const {
    const ClientLease *l = get_client_lease(c);
    if (l) 
      return true;
    else
      return false;
  }

  ClientLease *add_client_lease(client_t c, Session *session);
  void remove_client_lease(ClientLease *r, Locker *locker);  // returns remaining mask (if any), and kicks locker eval_gathers
  void remove_client_leases(Locker *locker);

  
  ostream& print_db_line_prefix(ostream& out);
  void print(ostream& out);
  void dump(Formatter *f) const;

  friend class CDir;
};

ostream& operator<<(ostream& out, const CDentry& dn);


#endif
