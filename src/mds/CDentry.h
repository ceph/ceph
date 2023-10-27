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
#include <string_view>
#include <set>

#include "include/counter.h"
#include "include/types.h"
#include "include/buffer_fwd.h"
#include "include/lru.h"
#include "include/elist.h"
#include "include/filepath.h"

#include "BatchOp.h"
#include "MDSCacheObject.h"
#include "MDSContext.h"
#include "SimpleLock.h"
#include "LocalLockC.h"
#include "ScrubHeader.h"

class CInode;
class CDir;
class Locker;
class CDentry;
class LogSegment;

class Session;

// define an ordering
bool operator<(const CDentry& l, const CDentry& r);

// dentry
class CDentry : public MDSCacheObject, public LRUObject, public Counter<CDentry> {
public:
  MEMPOOL_CLASS_HELPERS();
  friend class CDir;

  struct linkage_t {
    CInode *inode = nullptr;
    inodeno_t remote_ino = 0;
    unsigned char remote_d_type = 0;
    
    linkage_t() {}

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
  };


  // -- state --
  static const int STATE_NEW =          (1<<0);
  static const int STATE_FRAGMENTING =  (1<<1);
  static const int STATE_PURGING =      (1<<2);
  static const int STATE_BADREMOTEINO = (1<<3);
  static const int STATE_EVALUATINGSTRAY = (1<<4);
  static const int STATE_PURGINGPINNED =  (1<<5);
  static const int STATE_BOTTOMLRU =    (1<<6);
  // stray dentry needs notification of releasing reference
  static const int STATE_STRAY =	STATE_NOTIFYREF;
  static const int MASK_STATE_IMPORT_KEPT = STATE_BOTTOMLRU;

  // -- pins --
  static const int PIN_INODEPIN =     1;  // linked inode is pinned
  static const int PIN_FRAGMENTING = -2;  // containing dir is refragmenting
  static const int PIN_PURGING =      3;
  static const int PIN_SCRUBPARENT =  4;

  static const unsigned EXPORT_NONCE = 1;


  CDentry(std::string_view n, __u32 h,
          mempool::mds_co::string alternate_name,
	  snapid_t f, snapid_t l) :
    hash(h),
    first(f), last(l),
    item_dirty(this),
    lock(this, &lock_type),
    versionlock(this, &versionlock_type),
    name(n),
    alternate_name(std::move(alternate_name))
  {}
  CDentry(std::string_view n, __u32 h,
          mempool::mds_co::string alternate_name,
          inodeno_t ino, unsigned char dt,
	  snapid_t f, snapid_t l) :
    hash(h),
    first(f), last(l),
    item_dirty(this),
    lock(this, &lock_type),
    versionlock(this, &versionlock_type),
    name(n),
    alternate_name(std::move(alternate_name))
  {
    linkage.remote_ino = ino;
    linkage.remote_d_type = dt;
  }

  ~CDentry() override {
    ceph_assert(batch_ops.empty());
  }

  std::string_view pin_name(int p) const override {
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

  void add_waiter(uint64_t tag, MDSContext *c) override;

  bool is_lt(const MDSCacheObject *r) const override {
    return *this < *static_cast<const CDentry*>(r);
  }

  dentry_key_t key() {
    return dentry_key_t(last, name.c_str(), hash);
  }

  bool check_corruption(bool load);

  const CDir *get_dir() const { return dir; }
  CDir *get_dir() { return dir; }
  std::string_view get_name() const { return std::string_view(name); }
  std::string_view get_alternate_name() const {
    return std::string_view(alternate_name);
  }
  void set_alternate_name(mempool::mds_co::string altn) {
    alternate_name = std::move(altn);
  }
  void set_alternate_name(std::string_view altn) {
    alternate_name = mempool::mds_co::string(altn);
  }

  __u32 get_hash() const { return hash; }

  // linkage
  const linkage_t *get_linkage() const { return &linkage; }
  linkage_t *get_linkage() { return &linkage; }

  linkage_t *_project_linkage() {
    projected.push_back(linkage_t());
    return &projected.back();
  }
  void push_projected_linkage();
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

  const linkage_t *get_projected_linkage() const {
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
  void first_get() override {
    lru_pin();
  }
  void last_put() override {
    lru_unpin();
  }
  void _put() override;

  // auth pins
  bool can_auth_pin(int *err_ret=nullptr) const override;
  void auth_pin(void *by) override;
  void auth_unpin(void *by) override;
  void adjust_nested_auth_pins(int diradj, void *by);
  bool is_frozen() const override;
  bool is_freezing() const override;
  int get_num_dir_auth_pins() const;
  
  // remote links
  void link_remote(linkage_t *dnl, CInode *in);
  void unlink_remote(linkage_t *dnl);
  
  // copy cons
  CDentry(const CDentry& m);
  const CDentry& operator= (const CDentry& right);

  // misc
  void make_path_string(std::string& s, bool projected=false) const;
  void make_path(filepath& fp, bool projected=false) const;

  // -- version --
  version_t get_version() const { return version; }
  void set_version(version_t v) { projected_version = version = v; }
  version_t get_projected_version() const { return projected_version; }
  void set_projected_version(version_t v) { projected_version = v; }
  
  mds_authority_t authority() const override;

  version_t pre_dirty(version_t min=0);
  void _mark_dirty(LogSegment *ls);
  void mark_dirty(version_t pv, LogSegment *ls);
  void mark_clean();

  void mark_new();
  bool is_new() const { return state_test(STATE_NEW); }
  void clear_new() { state_clear(STATE_NEW); }

  void mark_auth();
  void clear_auth();
  
  bool scrub(snapid_t next_seq);

  // -- exporting
  // note: this assumes the dentry already exists.  
  // i.e., the name is already extracted... so we just need the other state.
  void encode_export(ceph::buffer::list& bl) {
    ENCODE_START(1, 1, bl);
    encode(first, bl);
    encode(state, bl);
    encode(version, bl);
    encode(projected_version, bl);
    encode(lock, bl);
    encode(get_replicas(), bl);
    get(PIN_TEMPEXPORTING);
    ENCODE_FINISH(bl);
  }
  void finish_export() {
    // twiddle
    clear_replica_map();
    replica_nonce = EXPORT_NONCE;
    clear_auth();
    if (is_dirty())
      mark_clean();
    put(PIN_TEMPEXPORTING);
  }
  void abort_export() {
    put(PIN_TEMPEXPORTING);
  }
  void decode_import(ceph::buffer::list::const_iterator& blp, LogSegment *ls) {
    DECODE_START(1, blp);
    decode(first, blp);
    __u32 nstate;
    decode(nstate, blp);
    decode(version, blp);
    decode(projected_version, blp);
    decode(lock, blp);
    decode(get_replicas(), blp);

    // twiddle
    state &= MASK_STATE_IMPORT_KEPT;
    mark_auth();
    if (nstate & STATE_DIRTY)
      _mark_dirty(ls);
    if (is_replicated())
      get(PIN_REPLICATED);
    replica_nonce = 0;
    DECODE_FINISH(blp);
  }

  // -- locking --
  SimpleLock* get_lock(int type) override {
    ceph_assert(type == CEPH_LOCK_DN);
    return &lock;
  }
  void set_object_info(MDSCacheObjectInfo &info) override;
  void encode_lock_state(int type, ceph::buffer::list& bl) override;
  void decode_lock_state(int type, const ceph::buffer::list& bl) override;

  // ---------------------------------------------
  // replicas (on clients)

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

  std::ostream& print_db_line_prefix(std::ostream& out) const override;
  void print(std::ostream& out) const override;
  void dump(ceph::Formatter *f) const;

  static void encode_remote(inodeno_t& ino, unsigned char d_type,
                            std::string_view alternate_name,
                            bufferlist &bl);
  static void decode_remote(char icode, inodeno_t& ino, unsigned char& d_type,
                            mempool::mds_co::string& alternate_name,
                            ceph::buffer::list::const_iterator& bl);

  __u32 hash;
  snapid_t first, last;
  bool corrupt_first_loaded = false; /* for Postgres corruption detection */

  elist<CDentry*>::item item_dirty, item_dir_dirty;
  elist<CDentry*>::item item_stray;

  // lock
  static LockType lock_type;
  static LockType versionlock_type;

  SimpleLock lock; // FIXME referenced containers not in mempool
  LocalLockC versionlock; // FIXME referenced containers not in mempool

  mempool::mds_co::map<client_t,ClientLease*> client_lease_map;
  std::map<int, std::unique_ptr<BatchOp>> batch_ops;

  ceph_tid_t reintegration_reqid = 0;


protected:
  friend class Migrator;
  friend class Locker;
  friend class MDCache;
  friend class StrayManager;
  friend class CInode;
  friend class C_MDC_XlockRequest;

  CDir *dir = nullptr;     // containing dirfrag
  linkage_t linkage; /* durable */
  mempool::mds_co::list<linkage_t> projected;

  version_t version = 0;  // dir version when last touched.
  version_t projected_version = 0;  // what it will be when i unlock/commit.

private:
  mempool::mds_co::string name;
  mempool::mds_co::string alternate_name;
};

std::ostream& operator<<(std::ostream& out, const CDentry& dn);


#endif
