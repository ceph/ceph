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


#ifndef __SIMPLELOCK_H
#define __SIMPLELOCK_H

// -- lock types --
// see CEPH_LOCK_*

inline const char *get_lock_type_name(int t) {
  switch (t) {
  case CEPH_LOCK_DN: return "dn";
  case CEPH_LOCK_IVERSION: return "iversion";
  case CEPH_LOCK_IFILE: return "ifile";
  case CEPH_LOCK_IAUTH: return "iauth";
  case CEPH_LOCK_ILINK: return "ilink";
  case CEPH_LOCK_IDFT: return "idft";
  case CEPH_LOCK_INEST: return "inest";
  case CEPH_LOCK_IXATTR: return "ixattr";
  case CEPH_LOCK_ISNAP: return "isnap";
  case CEPH_LOCK_INO: return "ino";
  default: assert(0); return 0;
  }
}

class Mutation;

extern "C" {
#include "locks.h"
}


#define CAP_ANY     0
#define CAP_LONER   1
#define CAP_XLOCKER 2


class SimpleLock {
public:
  
  virtual const char *get_state_name(int n) {
    switch (n) {
    case LOCK_UNDEF: return "UNDEF";
    case LOCK_SYNC: return "sync";
    case LOCK_LOCK: return "lock";

    case LOCK_PREXLOCK: return "prexlock";
    case LOCK_XLOCK: return "xlock";
    case LOCK_XLOCKDONE: return "xlockdone";
    case LOCK_LOCK_XLOCK: return "lock->prexlock";

    case LOCK_SYNC_LOCK: return "sync->lock";
    case LOCK_LOCK_SYNC: return "lock->sync";
    case LOCK_REMOTEXLOCK: return "remote_xlock";
    case LOCK_EXCL: return "excl";
    case LOCK_EXCL_SYNC: return "excl->sync";
    case LOCK_EXCL_LOCK: return "excl->lock";
    case LOCK_SYNC_EXCL: return "sync->excl";
    case LOCK_LOCK_EXCL: return "lock->excl";      

    case LOCK_SYNC_MIX: return "sync->mix";
    case LOCK_SYNC_MIX2: return "sync->mix(2)";
    case LOCK_LOCK_TSYN: return "lock->tsyn";
      
    case LOCK_MIX_LOCK: return "mix->lock";
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

    default: assert(0); return 0;
    }
  }


  // waiting
  static const __u64 WAIT_RD          = (1<<0);  // to read
  static const __u64 WAIT_WR          = (1<<1);  // to write
  static const __u64 WAIT_XLOCK       = (1<<2);  // to xlock   (** dup)
  static const __u64 WAIT_STABLE      = (1<<2);  // for a stable state
  static const __u64 WAIT_REMOTEXLOCK = (1<<3);  // for a remote xlock
  static const int WAIT_BITS        = 4;
  static const __u64 WAIT_ALL         = ((1<<WAIT_BITS)-1);


  sm_t *sm;

protected:
  // parent (what i lock)
  MDSCacheObject *parent;
  int type;

  int get_wait_shift() {
    switch (type) {
    case CEPH_LOCK_DN:       return 8;
    case CEPH_LOCK_IAUTH:    return 5 +   SimpleLock::WAIT_BITS;
    case CEPH_LOCK_ILINK:    return 5 +   SimpleLock::WAIT_BITS;
    case CEPH_LOCK_IDFT:     return 5 + 2*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_IFILE:    return 5 + 3*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_IVERSION: return 5 + 4*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_IXATTR:   return 5 + 5*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_ISNAP:    return 5 + 6*SimpleLock::WAIT_BITS;
    case CEPH_LOCK_INEST:    return 5 + 7*SimpleLock::WAIT_BITS;
    default:
      assert(0);
    }
  }

  // lock state
  __s32 state;
  set<__s32> gather_set;  // auth+rep.  >= 0 is mds, < 0 is client
  int num_client_lease;

  // local state
  int num_rdlock, num_wrlock, num_xlock;
  Mutation *xlock_by;
  client_t xlock_by_client;
public:
  client_t excl_client;


public:
  SimpleLock(MDSCacheObject *o, int t) :
    parent(o), type(t),
    state(LOCK_SYNC), num_client_lease(0),
    num_rdlock(0), num_wrlock(0), num_xlock(0),
    xlock_by(0), xlock_by_client(-1), excl_client(-1) {
    switch (type) {
    case CEPH_LOCK_DN:
    case CEPH_LOCK_IAUTH:
    case CEPH_LOCK_ILINK:
    case CEPH_LOCK_IXATTR:
    case CEPH_LOCK_ISNAP:
      sm = &sm_simplelock;
      break;
    case CEPH_LOCK_IDFT:
    case CEPH_LOCK_INEST:
      sm = &sm_scatterlock;
      break;
    case CEPH_LOCK_IFILE:
      sm = &sm_filelock;
      break;
    default:
      sm = 0;
    }
  }
  virtual ~SimpleLock() {}

  // parent
  MDSCacheObject *get_parent() { return parent; }
  int get_type() { return type; }

  int get_cap_shift() {
    switch (type) {
    case CEPH_LOCK_IAUTH: return CEPH_CAP_SAUTH;
    case CEPH_LOCK_ILINK: return CEPH_CAP_SLINK;
    case CEPH_LOCK_IFILE: return CEPH_CAP_SFILE;
    case CEPH_LOCK_IXATTR: return CEPH_CAP_SXATTR;
    default: return 0;
    }
  }
  int get_cap_mask() {
    switch (type) {
    case CEPH_LOCK_IFILE: return 0xffff;
    default: return 0x3;
    }
  }

  struct ptr_lt {
    bool operator()(const SimpleLock* l, const SimpleLock* r) const {
      // first sort by object type (dn < inode)
      if ((l->type>CEPH_LOCK_DN) <  (r->type>CEPH_LOCK_DN)) return true;
      if ((l->type>CEPH_LOCK_DN) == (r->type>CEPH_LOCK_DN)) {
	// then sort by object
	if (l->parent->is_lt(r->parent)) return true;
	if (l->parent == r->parent) {
	  // then sort by (inode) lock type
	  if (l->type < r->type) return true;
	}
      }
      return false;
    }
  };

  void decode_locked_state(bufferlist& bl) {
    parent->decode_lock_state(type, bl);
  }
  void encode_locked_state(bufferlist& bl) {
    parent->encode_lock_state(type, bl);
  }
  void finish_waiters(__u64 mask, int r=0) {
    parent->finish_waiting(mask << get_wait_shift(), r);
  }
  void take_waiting(__u64 mask, list<Context*>& ls) {
    parent->take_waiting(mask << get_wait_shift(), ls);
  }
  void add_waiter(__u64 mask, Context *c) {
    parent->add_waiter(mask << get_wait_shift(), c);
  }
  bool is_waiter_for(__u64 mask) {
    return parent->is_waiter_for(mask << get_wait_shift());
  }
  
  

  // state
  int get_state() { return state; }
  int set_state(int s) { 
    state = s; 
    //assert(!is_stable() || gather_set.size() == 0);  // gather should be empty in stable states.
    return s;
  }
  void set_state_rejoin(int s, list<Context*>& waiters) {
    if (!is_stable()) {
      state = s;
      get_parent()->auth_unpin(this);
    } else {
      state = s;
    }
    take_waiting(SimpleLock::WAIT_ALL, waiters);
  }

  bool is_stable() {
    return !sm || sm->states[state].next == 0;
  }
  int get_next_state() {
    return sm->states[state].next;
  }

  bool fw_rdlock_to_auth() {
    return sm->states[state].can_rdlock == FW;
  }

  // gather set
  const set<int>& get_gather_set() { return gather_set; }
  void init_gather() {
    for (map<int,int>::const_iterator p = parent->replicas_begin(); 
	 p != parent->replicas_end(); 
	 ++p)
      gather_set.insert(p->first);
  }
  bool is_gathering() { return !gather_set.empty(); }
  bool is_gathering(int i) {
    return gather_set.count(i);
  }
  void clear_gather() {
    gather_set.clear();
  }
  void remove_gather(int i) {
    gather_set.erase(i);
  }



  virtual bool is_updated() { return false; }


  // can_*
  bool can_lease(client_t client) {
    return sm->states[state].can_lease == ANY ||
      (sm->states[state].can_lease == AUTH && parent->is_auth()) ||
      (sm->states[state].can_lease == XCL && client >= 0 && xlock_by_client == client);
  }
  bool can_read(client_t client) {
    return sm->states[state].can_read == ANY ||
      (sm->states[state].can_read == AUTH && parent->is_auth()) ||
      (sm->states[state].can_read == XCL && client >= 0 && xlock_by_client == client);
  }
  bool can_read_projected(client_t client) {
    return sm->states[state].can_read_projected == ANY ||
      (sm->states[state].can_read_projected == AUTH && parent->is_auth()) ||
      (sm->states[state].can_read_projected == XCL && client >= 0 && xlock_by_client == client);
  }
  bool can_rdlock(client_t client) {
    return sm->states[state].can_rdlock == ANY ||
      (sm->states[state].can_rdlock == AUTH && parent->is_auth()) ||
      (sm->states[state].can_rdlock == XCL && client >= 0 && xlock_by_client == client);
  }
  bool can_wrlock(client_t client) {
    return sm->states[state].can_wrlock == ANY ||
      (sm->states[state].can_wrlock == AUTH && parent->is_auth()) ||
      (sm->states[state].can_wrlock == XCL && client >= 0 && (xlock_by_client == client ||
							      excl_client == client));
  }
  bool can_xlock(client_t client) {
    return sm->states[state].can_xlock == ANY ||
      (sm->states[state].can_xlock == AUTH && parent->is_auth()) ||
      (sm->states[state].can_xlock == XCL && client >= 0 && xlock_by_client == client);
  }

  // rdlock
  bool is_rdlocked() { return num_rdlock > 0; }
  int get_rdlock() { 
    if (!num_rdlock) parent->get(MDSCacheObject::PIN_LOCK);
    return ++num_rdlock; 
  }
  int put_rdlock() {
    assert(num_rdlock>0);
    --num_rdlock;
    if (num_rdlock == 0) parent->put(MDSCacheObject::PIN_LOCK);
    return num_rdlock;
  }
  int get_num_rdlocks() { return num_rdlock; }

  // wrlock
  void get_wrlock(bool force=false) {
    //assert(can_wrlock() || force);
    if (num_wrlock == 0) parent->get(MDSCacheObject::PIN_LOCK);
    ++num_wrlock;
  }
  void put_wrlock() {
    --num_wrlock;
    if (num_wrlock == 0) parent->put(MDSCacheObject::PIN_LOCK);
  }
  bool is_wrlocked() { return num_wrlock > 0; }
  int get_num_wrlocks() { return num_wrlock; }

  // xlock
  void get_xlock(Mutation *who, client_t client) { 
    assert(xlock_by == 0);
    assert(state == LOCK_XLOCK);
    parent->get(MDSCacheObject::PIN_LOCK);
    num_xlock++;
    xlock_by = who; 
    xlock_by_client = client;
  }
  void set_xlock_done() {
    assert(xlock_by);
    assert(state == LOCK_XLOCK);
    state = LOCK_XLOCKDONE;
    xlock_by = 0;
  }
  void put_xlock() {
    assert(state == LOCK_XLOCK || state == LOCK_XLOCKDONE);
    --num_xlock;
    parent->put(MDSCacheObject::PIN_LOCK);
    if (num_xlock == 0) {
      xlock_by = 0;
      xlock_by_client = -1;
    }
  }
  bool is_xlocked() { return num_xlock > 0; }
  int get_num_xlocks() { return num_xlock; }
  bool is_xlocked_by_client(client_t c) {
    return xlock_by_client == c;
  }
  Mutation *get_xlocked_by() { return xlock_by; }
  
  // lease
  void get_client_lease() {
    num_client_lease++;
  }
  void put_client_lease() {
    assert(num_client_lease > 0);
    num_client_lease--;
  }
  bool is_leased() { return num_client_lease > 0; }
  int get_num_client_lease() {
    return num_client_lease;
  }

  bool is_used() {
    return is_xlocked() || is_rdlocked() || is_wrlocked() || num_client_lease;
  }

  // encode/decode
  void encode(bufferlist& bl) const {
    __u8 struct_v = 1;
    ::encode(struct_v, bl);
    ::encode(state, bl);
    ::encode(gather_set, bl);
  }
  void decode(bufferlist::iterator& p) {
    __u8 struct_v;
    ::decode(struct_v, p);
    ::decode(state, p);
    ::decode(gather_set, p);
  }
  void encode_state_for_replica(bufferlist& bl) const {
    __u32 s = get_replica_state();
    ::encode(s, bl);
  }
  void decode_state(bufferlist::iterator& p, bool is_new=true) {
    if (is_new)
      ::decode(state, p);
    else {
      __s32 blah;
      ::decode(blah, p);
    }
  }
  void decode_state_rejoin(bufferlist::iterator& p, list<Context*>& waiters) {
    __s32 s;
    ::decode(s, p);
    set_state_rejoin(s, waiters);
  }


  // caps
  bool is_loner_mode() {
    return sm->states[state].loner;
  }
  int gcaps_allowed_ever() {
    return parent->is_auth() ? sm->allowed_ever_auth : sm->allowed_ever_replica;
  }
  int gcaps_allowed(int who, int s=-1) {
    if (s < 0) s = state;
    if (parent->is_auth()) {
      if (xlock_by_client >= 0 && who == CAP_XLOCKER)
	return sm->states[s].xlocker_caps;
      else if (is_loner_mode() && who == CAP_ANY)
	return sm->states[s].caps;
      else 
	return sm->states[s].loner_caps | sm->states[s].caps;  // loner always gets more
    } else 
      return sm->states[s].replica_caps;
  }
  int gcaps_careful() {
    if (num_wrlock)
      return sm->careful;
    return 0;
  }


  int gcaps_xlocker_mask(client_t client) {
    if (client == xlock_by_client)
      return type == CEPH_LOCK_IFILE ? 0xffff : (CEPH_CAP_GSHARED|CEPH_CAP_GEXCL);
    return 0;
  }

  // simplelock specifics
  int get_replica_state() const {
    return sm->states[state].replica_state;
  }
  void export_twiddle() {
    clear_gather();
    state = get_replica_state();
  }

  /** replicate_relax
   * called on first replica creation.
   */
  void replicate_relax() {
    assert(parent->is_auth());
    assert(!parent->is_replicated());
    if (state == LOCK_LOCK && !is_used())
      state = LOCK_SYNC;
  }
  bool remove_replica(int from) {
    if (is_gathering(from)) {
      remove_gather(from);
      if (!is_gathering())
	return true;
    }
    return false;
  }
  bool do_import(int from, int to) {
    if (!is_stable()) {
      remove_gather(from);
      remove_gather(to);
      if (!is_gathering())
	return true;
    }
    if (!is_stable() && !is_gathering())
      return true;
    return false;
  }

  void _print(ostream& out) {
    out << get_lock_type_name(get_type()) << " ";
    out << get_state_name(get_state());
    if (!get_gather_set().empty())
      out << " g=" << get_gather_set();
    if (num_client_lease)
      out << " l=" << num_client_lease;
    if (is_rdlocked()) 
      out << " r=" << get_num_rdlocks();
    if (is_wrlocked()) 
      out << " w=" << get_num_wrlocks();
    if (is_xlocked()) {
      out << " x=" << get_num_xlocks();
      if (get_xlocked_by())
	out << " by " << get_xlocked_by();
    }
    /*if (is_stable())
      out << " stable";
    else
      out << " unstable";
    */
  }

  virtual void print(ostream& out) {
    out << "(";
    _print(out);
    out << ")";
  }
};
WRITE_CLASS_ENCODER(SimpleLock)

inline ostream& operator<<(ostream& out, SimpleLock& l) 
{
  l.print(out);
  return out;
}


#endif
