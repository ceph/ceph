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


class SimpleLock {
public:
  
  virtual const char *get_state_name(int n) {
    switch (n) {
    case LOCK_UNDEF: return "UNDEF";
    case LOCK_SYNC: return "sync";
    case LOCK_LOCK: return "lock";
    case LOCK_XLOCK: return "xlock";
    case LOCK_XLOCKDONE: return "xlockdone";
    case LOCK_SYNC_LOCK: return "sync->lock";
    case LOCK_LOCK_SYNC: return "lock->sync";
    case LOCK_REMOTEXLOCK: return "remote_xlock";
    case LOCK_EXCL: return "excl";
    case LOCK_EXCL_SYNC: return "excl->sync";
    case LOCK_EXCL_LOCK: return "excl->lock";
    case LOCK_SYNC_EXCL: return "sync->excl";
    case LOCK_LOCK_EXCL: return "lock->excl";      
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


  sm_state_t *states;

protected:
  // parent (what i lock)
  MDSCacheObject *parent;
  int type;
  int wait_shift;
  int cap_shift;

  // lock state
  __s32 state;
  set<__s32> gather_set;  // auth+rep.  >= 0 is mds, < 0 is client
  int num_client_lease;

  // local state
  int num_rdlock, num_wrlock, num_xlock;
  Mutation *xlock_by;
  int xlock_by_client;


public:
  SimpleLock(MDSCacheObject *o, int t, int ws, int cs) :
    states(sm_simplelock),
    parent(o), type(t), wait_shift(ws), cap_shift(cs),
    state(LOCK_SYNC), num_client_lease(0),
    num_rdlock(0), num_wrlock(0), num_xlock(0),
    xlock_by(0), xlock_by_client(-1) { }
  virtual ~SimpleLock() {}

  // parent
  MDSCacheObject *get_parent() { return parent; }
  int get_type() { return type; }

  int get_cap_shift() { return cap_shift; }

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
    parent->finish_waiting(mask << wait_shift, r);
  }
  void take_waiting(__u64 mask, list<Context*>& ls) {
    parent->take_waiting(mask << wait_shift, ls);
  }
  void add_waiter(__u64 mask, Context *c) {
    parent->add_waiter(mask << wait_shift, c);
  }
  bool is_waiter_for(__u64 mask) {
    return parent->is_waiter_for(mask << wait_shift);
  }
  
  

  // state
  int get_state() { return state; }
  int set_state(int s) { 
    state = s; 
    assert(!is_stable() || gather_set.size() == 0);  // gather should be empty in stable states.
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

  virtual bool is_stable() {
    return states[state].next == 0;
  }
  int get_next_state() {
    return states[state].next;
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


  // can_*
  bool can_lease(int client) {
    return states[state].can_lease == ANY ||
      (states[state].can_lease == AUTH && parent->is_auth()) ||
      (states[state].can_lease == XCL && client >= 0 && xlock_by_client == client);
  }
  bool can_read(int client) {
    return states[state].can_read == ANY ||
      (states[state].can_read == AUTH && parent->is_auth()) ||
      (states[state].can_read == XCL && client >= 0 && xlock_by_client == client);
  }
  bool can_read_projected(int client) {
    return states[state].can_read_projected == ANY ||
      (states[state].can_read_projected == AUTH && parent->is_auth()) ||
      (states[state].can_read_projected == XCL && client >= 0 && xlock_by_client == client);
  }
  bool can_rdlock(int client) {
    return states[state].can_rdlock == ANY ||
      (states[state].can_rdlock == AUTH && parent->is_auth()) ||
      (states[state].can_rdlock == XCL && client >= 0 && xlock_by_client == client);
  }
  bool can_xlock(int client) {
    return states[state].can_xlock == ANY ||
      (states[state].can_xlock == AUTH && parent->is_auth()) ||
      (states[state].can_xlock == XCL && client >= 0 && xlock_by_client == client);
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
  virtual bool can_wrlock() { assert(0); }
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
  void get_xlock(Mutation *who, int client) { 
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
  bool is_xlocked_by_client(int c) {
    return xlock_by_client == c;
  }
  Mutation *get_xlocked_by() { return xlock_by; }
  /*
  bool xlocker_is_done() { return state == LOCK_XLOCKDONE; }
  bool is_xlocked_by_other(Mutation *mdr) {
    return is_xlocked() && xlock_by != mdr;
  }
  */
  
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
    ::encode(state, bl);
    ::encode(gather_set, bl);
  }
  void decode(bufferlist::iterator& p) {
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
  virtual bool is_loner_mode() {
    return states[state].loner;
  }
  virtual int gcaps_allowed_ever() {
    if (!cap_shift) 
      return 0;  // none for this lock.
    return CEPH_CAP_GRDCACHE | CEPH_CAP_GEXCL;
  }
  virtual int gcaps_allowed(bool loner) {
    if (!cap_shift)
      return 0;
    if (parent->is_auth()) {
      if (is_loner_mode() && loner)
	return states[state].loner_caps;
      else
	return states[state].caps;
    } else 
      return states[state].replica_caps;
  }
  virtual int gcaps_careful() {
    if (num_wrlock)
      return CEPH_CAP_GRDCACHE | CEPH_CAP_GEXCL;
    return 0;
  }


  // simplelock specifics
  virtual int get_replica_state() const {
    return states[state].replica_state;
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

  virtual void _print(ostream& out) {
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
