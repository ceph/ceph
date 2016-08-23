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


#ifndef CEPH_SIMPLELOCK_H
#define CEPH_SIMPLELOCK_H

#include "mds/mdstypes.h"
#include "CObject.h"

// -- lock types --
// see CEPH_LOCK_*

inline const char *get_lock_type_name(int t) {
  switch (t) {
  case CEPH_LOCK_DN: return "dn";
  case CEPH_LOCK_DVERSION: return "dversion";
  case CEPH_LOCK_IVERSION: return "iversion";
  case CEPH_LOCK_IFILE: return "ifile";
  case CEPH_LOCK_IAUTH: return "iauth";
  case CEPH_LOCK_ILINK: return "ilink";
  case CEPH_LOCK_IDFT: return "idft";
  case CEPH_LOCK_INEST: return "inest";
  case CEPH_LOCK_IXATTR: return "ixattr";
  case CEPH_LOCK_ISNAP: return "isnap";
  case CEPH_LOCK_INO: return "ino";
  case CEPH_LOCK_IFLOCK: return "iflock";
  case CEPH_LOCK_IPOLICY: return "ipolicy";
  default: assert(0); return 0;
  }
}

#include "include/memory.h"
struct MutationImpl;
typedef ceph::shared_ptr<MutationImpl> MutationRef;

extern "C" {
#include "mds/locks.h"
}


#define CAP_ANY     0
#define CAP_LONER   1
#define CAP_XLOCKER 2

struct LockType {
  const int type;
  const sm_t *sm;

  explicit LockType(int t) : type(t) {
    switch (type) {
    case CEPH_LOCK_DN:
    case CEPH_LOCK_IAUTH:
    case CEPH_LOCK_ILINK:
    case CEPH_LOCK_IXATTR:
    case CEPH_LOCK_ISNAP:
    case CEPH_LOCK_IFLOCK:
    case CEPH_LOCK_IPOLICY:
      sm = &sm_simplelock;
      break;
    case CEPH_LOCK_IDFT:
    case CEPH_LOCK_INEST:
      sm = &sm_scatterlock;
      break;
    case CEPH_LOCK_IFILE:
      sm = &sm_filelock;
      break;
    case CEPH_LOCK_DVERSION:
    case CEPH_LOCK_IVERSION:
      sm = &sm_locallock;
      break;
    default:
      sm = 0;
    }
  }
};


class SimpleLock {
public:
  LockType *type;
  
  const char *get_state_name(int n) const {
    switch (n) {
    case LOCK_UNDEF: return "UNDEF";
    case LOCK_SYNC: return "sync";
    case LOCK_LOCK: return "lock";

    case LOCK_PREXLOCK: return "prexlock";
    case LOCK_XLOCK: return "xlock";
    case LOCK_XLOCKDONE: return "xlockdone";
    case LOCK_LOCK_XLOCK: return "lock->xlock";

    case LOCK_SYNC_LOCK: return "sync->lock";
    case LOCK_LOCK_SYNC: return "lock->sync";
    case LOCK_REMOTEXLOCK: return "remote_xlock";
    case LOCK_EXCL: return "excl";
    case LOCK_EXCL_SYNC: return "excl->sync";
    case LOCK_EXCL_LOCK: return "excl->lock";
    case LOCK_SYNC_EXCL: return "sync->excl";
    case LOCK_LOCK_EXCL: return "lock->excl";      

    case LOCK_XSYN: return "xsyn";
    case LOCK_XSYN_EXCL: return "xsyn->excl";
    case LOCK_EXCL_XSYN: return "excl->xsyn";
    case LOCK_XSYN_SYNC: return "xsyn->sync";

    case LOCK_SYNC_MIX: return "sync->mix";
    case LOCK_SYNC_MIX2: return "sync->mix(2)";
    case LOCK_LOCK_TSYN: return "lock->tsyn";
      
    case LOCK_MIX_LOCK: return "mix->lock";
    case LOCK_MIX_LOCK2: return "mix->lock(2)";
    case LOCK_MIX: return "mix";
    case LOCK_MIX_TSYN: return "mix->tsyn";
      
    case LOCK_TSYN_MIX: return "tsyn->mix";
    case LOCK_TSYN_LOCK: return "tsyn->lock";
    case LOCK_TSYN: return "tsyn";

    case LOCK_MIX_SYNC: return "mix->sync";
    case LOCK_MIX_SYNC2: return "mix->sync(2)";
    case LOCK_EXCL_MIX: return "excl->mix";
    case LOCK_MIX_EXCL: return "mix->excl";

    case LOCK_PRE_SCAN: return "*->scan";
    case LOCK_SCAN: return "scan";

    case LOCK_SNAP_SYNC: return "snap->sync";

    default: assert(0); return 0;
    }
  }


  // waiting
  static const uint64_t WAIT_RD          = (1<<0);  // to read
  static const uint64_t WAIT_WR          = (1<<1);  // to write
  static const uint64_t WAIT_XLOCK       = (1<<2);  // to xlock   (** dup)
  static const uint64_t WAIT_STABLE      = (1<<2);  // for a stable state
  static const int WAIT_BITS        = 3;
  static const uint64_t WAIT_ALL         = ((1<<WAIT_BITS)-1);


protected:
  // parent (what i lock)
  CObject *parent;

  // lock state
  __s16 state;

  //
  static const MutationRef null_xlock_by;
private:
  int num_stable_waiter;
  int num_rdlock;
  int num_client_lease;

  struct unstable_bits_t {
    // local state
    int num_wrlock, num_xlock;
    MutationRef xlock_by;
    client_t xlock_by_client;
    client_t excl_client;

    bool empty() {
      return
	num_wrlock == 0 &&
	num_xlock == 0 &&
	xlock_by.get() == NULL &&
	xlock_by_client == -1 &&
	excl_client == -1;
    }

    unstable_bits_t() : num_wrlock(0),
			num_xlock(0),
			xlock_by(),
			xlock_by_client(-1),
			excl_client(-1) {}
  };

  mutable unstable_bits_t *_unstable;

  bool have_more() const { return _unstable ? true : false; }
  unstable_bits_t *more() const {
    if (!_unstable)
      _unstable = new unstable_bits_t;
    return _unstable;
  }
  void try_clear_more() {
    if (_unstable && _unstable->empty()) {
      delete _unstable;
      _unstable = NULL;
    }
  }


public:

  bool has_stable_waiter() const { return num_stable_waiter; } 
  void inc_stable_waiter() { ++num_stable_waiter; }
  void dec_stable_waiter() { --num_stable_waiter; }

  client_t get_excl_client() const {
    return have_more() ? more()->excl_client : -1;
  }
  void set_excl_client(client_t c) {
    if (c < 0 && !have_more())
      return;  // default is -1
    more()->excl_client = c;
  }

  SimpleLock(CObject *o, LockType *lt) :
    type(lt),
    parent(o), 
    state(LOCK_SYNC),
    num_stable_waiter(0),
    num_rdlock(0),
    num_client_lease(0),
    _unstable(NULL)
  {}
  virtual ~SimpleLock() {
    delete _unstable;
  }

  virtual bool is_scatterlock() const {
    return false;
  }
  virtual bool is_locallock() const {
    return false;
  }

  // parent
  CObject *get_parent() { return parent; }
  int get_type() const { return type->type; }
  const sm_t* get_sm() const { return type->sm; }

  int get_wait_shift() const {
    switch (get_type()) {
    case CEPH_LOCK_DN:       return 8;
    case CEPH_LOCK_DVERSION: return 8 + 1*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_IAUTH:    return 8 + 2*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_ILINK:    return 8 + 3*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_IDFT:     return 8 + 4*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_IFILE:    return 8 + 5*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_IVERSION: return 8 + 6*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_IXATTR:   return 8 + 7*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_ISNAP:    return 8 + 8*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_INEST:    return 8 + 9*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_IFLOCK:   return 8 +10*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_IPOLICY:  return 8 +11*SimpleLock::WAIT_BITS;
    default:
      assert(0);
    }
  }

  int get_cap_shift() const {
    switch (get_type()) {
    case CEPH_LOCK_IAUTH: return CEPH_CAP_SAUTH;
    case CEPH_LOCK_ILINK: return CEPH_CAP_SLINK;
    case CEPH_LOCK_IFILE: return CEPH_CAP_SFILE;
    case CEPH_LOCK_IXATTR: return CEPH_CAP_SXATTR;
    default: return 0;
    }
  }
  int get_cap_mask() const {
    switch (get_type()) {
    case CEPH_LOCK_IFILE: return (1 << CEPH_CAP_FILE_BITS) - 1;
    default: return (1 << CEPH_CAP_SIMPLE_BITS) - 1;
    }
  }

  struct ptr_lt {
    bool operator()(const SimpleLock* l, const SimpleLock* r) const {
      // first sort by object type (dn < inode)
      if (!(l->type->type > CEPH_LOCK_DN) && (r->type->type > CEPH_LOCK_DN)) return true;
      if ((l->type->type > CEPH_LOCK_DN) == (r->type->type > CEPH_LOCK_DN)) {
	// then sort by object
	if (l->parent->is_lt(r->parent)) return true;
	if (l->parent == r->parent) {
	  // then sort by (inode) lock type
	  if (l->type->type < r->type->type) return true;
	}
      }
      return false;
    }
  };

  void add_waiter(uint64_t mask, MDSInternalContextBase *c) {
    parent->add_waiter((mask << get_wait_shift()) | CObject::WAIT_ORDERED, c);
  }
  void take_waiting(uint64_t mask, list<MDSInternalContextBase*>& ls) {
    parent->take_waiting(mask << get_wait_shift(), ls);
  }
  void finish_waiting(uint64_t mask) {
    parent->finish_waiting(mask << get_wait_shift());
  }
  bool is_waiting_for(uint64_t mask) const {
    return parent->is_waiting_for(mask << get_wait_shift());
  }

  // state
  int get_state() const { return state; }
  int set_state(int s) { 
    state = s; 
    //assert(!is_stable() || gather_set.size() == 0);  // gather should be empty in stable states.
    return s;
  }

  bool is_stable() const {
    return get_sm()->states[state].next == 0;
  }
  bool is_unstable_and_locked() const {
    if (is_stable())
      return false;
    return is_rdlocked() || is_wrlocked() || is_xlocked();
  }
  int get_next_state() {
    return get_sm()->states[state].next;
  }


  bool is_sync_and_unlocked() const {
    return
      get_state() == LOCK_SYNC &&
      !is_rdlocked() &&
      !is_leased() &&
      !is_wrlocked() &&
      !is_xlocked();
  }


  /*
  bool fw_rdlock_to_auth() {
    return get_sm()->states[state].can_rdlock == FW;
  }
  */
  bool req_rdlock_from_auth() {
    return get_sm()->states[state].can_rdlock == REQ;
  }

  virtual bool is_dirty() const { return false; }
  virtual bool is_stale() const { return false; }
  virtual bool is_flushing() const { return false; }
  virtual bool is_flushed() const { return false; }
  virtual void clear_flushed() { }

  // can_*
  bool can_lease(client_t client) const {
    return get_sm()->states[state].can_lease == ANY ||
      (get_sm()->states[state].can_lease == AUTH && parent->is_auth()) ||
      (get_sm()->states[state].can_lease == XCL && client >= 0 && get_xlock_by_client() == client);
  }
  bool can_read(client_t client) const {
    return get_sm()->states[state].can_read == ANY ||
      (get_sm()->states[state].can_read == AUTH && parent->is_auth()) ||
      (get_sm()->states[state].can_read == XCL && client >= 0 && get_xlock_by_client() == client);
  }
  bool can_read_projected(client_t client) const {
    return get_sm()->states[state].can_read_projected == ANY ||
      (get_sm()->states[state].can_read_projected == AUTH && parent->is_auth()) ||
      (get_sm()->states[state].can_read_projected == XCL && client >= 0 && get_xlock_by_client() == client);
  }
  bool can_rdlock(client_t client) const {
    return get_sm()->states[state].can_rdlock == ANY ||
      (get_sm()->states[state].can_rdlock == AUTH && parent->is_auth()) ||
      (get_sm()->states[state].can_rdlock == XCL && client >= 0 && get_xlock_by_client() == client);
  }
  bool can_wrlock(client_t client) const {
    return get_sm()->states[state].can_wrlock == ANY ||
      (get_sm()->states[state].can_wrlock == AUTH && parent->is_auth()) ||
      (get_sm()->states[state].can_wrlock == XCL && client >= 0 && (get_xlock_by_client() == client ||
								    get_excl_client() == client));
  }
  bool can_force_wrlock(client_t client) const {
    return get_sm()->states[state].can_force_wrlock == ANY ||
      (get_sm()->states[state].can_force_wrlock == AUTH && parent->is_auth()) ||
      (get_sm()->states[state].can_force_wrlock == XCL && client >= 0 && (get_xlock_by_client() == client ||
									  get_excl_client() == client));
  }
  bool can_xlock(client_t client) const {
    return get_sm()->states[state].can_xlock == ANY ||
      (get_sm()->states[state].can_xlock == AUTH && parent->is_auth()) ||
      (get_sm()->states[state].can_xlock == XCL && client >= 0 && get_xlock_by_client() == client);
  }

  // rdlock
  bool is_rdlocked() const { return num_rdlock > 0; }
  int get_rdlock() { 
    if (!num_rdlock)
      parent->get(CObject::PIN_LOCK);
    return ++num_rdlock; 
  }
  int put_rdlock() {
    assert(num_rdlock>0);
    --num_rdlock;
    if (num_rdlock == 0)
      parent->put(CObject::PIN_LOCK);
    return num_rdlock;
  }
  int get_num_rdlocks() const {
    return num_rdlock;
  }

  // wrlock
  void get_wrlock(bool force=false) {
    //assert(can_wrlock() || force);
    if (more()->num_wrlock == 0)
      parent->get(CObject::PIN_LOCK);
    ++more()->num_wrlock;
  }
  void put_wrlock() {
    --more()->num_wrlock;
    if (more()->num_wrlock == 0) {
      parent->put(CObject::PIN_LOCK);
      try_clear_more();
    }
  }
  bool is_wrlocked() const {
    return have_more() && more()->num_wrlock > 0;
  }
  int get_num_wrlocks() const {
    return have_more() ? more()->num_wrlock : 0;
  }

  // xlock
  void get_xlock(const MutationRef& who, client_t client) {
    assert(get_xlock_by() == null_xlock_by);
    assert(state == LOCK_XLOCK || is_locallock() ||
	   state == LOCK_LOCK /* if we are a slave */);
    parent->get(CObject::PIN_LOCK);
    more()->num_xlock++;
    more()->xlock_by = who; 
    more()->xlock_by_client = client;
  }
  void set_xlock_done() {
    assert(more()->xlock_by);
    assert(state == LOCK_XLOCK || is_locallock() ||
	   state == LOCK_LOCK /* if we are a slave */);
    if (!is_locallock())
      state = LOCK_XLOCKDONE;
    more()->xlock_by.reset();
  }
  void put_xlock() {
    assert(state == LOCK_XLOCK || state == LOCK_XLOCKDONE || is_locallock() ||
	   state == LOCK_LOCK /* if we are a master of a slave */);
    --more()->num_xlock;
    parent->put(CObject::PIN_LOCK);
    if (more()->num_xlock == 0) {
      more()->xlock_by.reset();
      more()->xlock_by_client = -1;
      try_clear_more();
    }
  }
  bool is_xlocked() const {
    return have_more() && more()->num_xlock > 0;
  }
  int get_num_xlocks() const {
    return have_more() ? more()->num_xlock : 0;
  }
  client_t get_xlock_by_client() const {
    return have_more() ? more()->xlock_by_client : -1;
  }
  bool is_xlocked_by_client(client_t c) const {
    return have_more() ? more()->xlock_by_client == c : false;
  }
  const MutationRef& get_xlock_by() const {

    return have_more() ? more()->xlock_by : null_xlock_by;
  }
  
  // lease
  void get_client_lease() {
    num_client_lease++;
  }
  void put_client_lease() {
    assert(num_client_lease > 0);
    num_client_lease--;
    if (num_client_lease == 0) {
      try_clear_more();
    }
  }
  bool is_leased() const {
    return num_client_lease > 0;
  }
  int get_num_client_lease() const {
    return num_client_lease;
  }

  bool is_used() const {
    return is_xlocked() || is_rdlocked() || is_wrlocked() || num_client_lease;
  }

  // caps
  bool is_loner_mode() const {
    return get_sm()->states[state].loner;
  }
  int gcaps_allowed_ever() const {
    return parent->is_auth() ? get_sm()->allowed_ever_auth : get_sm()->allowed_ever_replica;
  }
  int gcaps_allowed(int who, int s=-1) const {
    if (s < 0) s = state;
    if (parent->is_auth()) {
      if (get_xlock_by_client() >= 0 && who == CAP_XLOCKER)
	return get_sm()->states[s].xlocker_caps | get_sm()->states[s].caps; // xlocker always gets more
      else if (is_loner_mode() && who == CAP_ANY)
	return get_sm()->states[s].caps;
      else 
	return get_sm()->states[s].loner_caps | get_sm()->states[s].caps;  // loner always gets more
    } else 
      return get_sm()->states[s].replica_caps;
  }
  int gcaps_careful() const {
    if (get_num_wrlocks())
      return get_sm()->careful;
    return 0;
  }


  int gcaps_xlocker_mask(client_t client) const {
    if (client == get_xlock_by_client())
      return type->type == CEPH_LOCK_IFILE ? 0xf : (CEPH_CAP_GSHARED|CEPH_CAP_GEXCL);
    return 0;
  }

  void _print(ostream& out) const {
    out << get_lock_type_name(get_type()) << " ";
    out << get_state_name(get_state());
    if (num_client_lease)
      out << " l=" << num_client_lease;
    if (is_rdlocked()) 
      out << " r=" << get_num_rdlocks();
    if (is_wrlocked()) 
      out << " w=" << get_num_wrlocks();
    if (is_xlocked()) {
      out << " x=" << get_num_xlocks();
      if (get_xlock_by())
	out << " by " << get_xlock_by();
    }
    /*if (is_stable())
      out << " stable";
    else
      out << " unstable";
    */
  }

  virtual void print(ostream& out) const {
    out << "(";
    _print(out);
    out << ")";
  }

  /**
   * Write bare values (caller must be in an object section)
   * to formatter, or nothing if is_sync_and_unlocked.
   */
  void dump(Formatter *f) const;
};

inline ostream& operator<<(ostream& out, const SimpleLock& l) 
{
  l.print(out);
  return out;
}


#endif
