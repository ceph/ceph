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
// NOTE: this also defines the lock ordering!
#define LOCK_OTYPE_DN       1

#define LOCK_OTYPE_IVERSION 2
#define LOCK_OTYPE_IFILE    3
#define LOCK_OTYPE_IAUTH    4
#define LOCK_OTYPE_ILINK    5
#define LOCK_OTYPE_IDIRFRAGTREE 6
#define LOCK_OTYPE_IDIR     7

//#define LOCK_OTYPE_DIR      7  // not used

inline const char *get_lock_type_name(int t) {
  switch (t) {
  case LOCK_OTYPE_DN: return "dn";
  case LOCK_OTYPE_IVERSION: return "iversion";
  case LOCK_OTYPE_IFILE: return "ifile";
  case LOCK_OTYPE_IAUTH: return "iauth";
  case LOCK_OTYPE_ILINK: return "ilink";
  case LOCK_OTYPE_IDIRFRAGTREE: return "idft";
  case LOCK_OTYPE_IDIR: return "idir";
  default: assert(0); return 0;
  }
}

// -- lock states --
// sync <-> lock
#define LOCK_UNDEF    0
//                               auth   rep
#define LOCK_SYNC     1  // AR   R .    R .
#define LOCK_LOCK     2  // AR   R W    . .
#define LOCK_GLOCKR  -3  // AR   R .    . .
#define LOCK_REMOTEXLOCK  -50    // on NON-auth

inline const char *get_simplelock_state_name(int n) {
  switch (n) {
  case LOCK_UNDEF: return "UNDEF";
  case LOCK_SYNC: return "sync";
  case LOCK_LOCK: return "lock";
  case LOCK_GLOCKR: return "glockr";
  case LOCK_REMOTEXLOCK: return "remote_xlock";
  default: assert(0); return 0;
  }
}

class MDRequest;

class SimpleLock {
public:
  static const int WAIT_RD          = (1<<0);  // to read
  static const int WAIT_WR          = (1<<1);  // to write
  static const int WAIT_XLOCK       = (1<<2);  // to xlock   (** dup)
  static const int WAIT_STABLE      = (1<<2);  // for a stable state
  static const int WAIT_REMOTEXLOCK = (1<<3);  // for a remote xlock
  static const int WAIT_BITS        = 4;
  static const int WAIT_ALL         = ((1<<WAIT_BITS)-1);

protected:
  // parent (what i lock)
  MDSCacheObject *parent;
  int type;
  int wait_offset;

  // lock state
  int state;
  set<int> gather_set;  // auth

  // local state
  int num_rdlock;
  MDRequest *xlock_by;

public:
  SimpleLock(MDSCacheObject *o, int t, int wo) :
    parent(o), type(t), wait_offset(wo),
    state(LOCK_SYNC), 
    num_rdlock(0), xlock_by(0) { }
  virtual ~SimpleLock() {}

  // parent
  MDSCacheObject *get_parent() { return parent; }
  int get_type() { return type; }

  struct ptr_lt {
    bool operator()(const SimpleLock* l, const SimpleLock* r) const {
      // first sort by object type (dn < inode)
      if ((l->type>LOCK_OTYPE_DN) <  (r->type>LOCK_OTYPE_DN)) return true;
      if ((l->type>LOCK_OTYPE_DN) == (r->type>LOCK_OTYPE_DN)) {
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
  void finish_waiters(int mask, int r=0) {
    parent->finish_waiting(mask << wait_offset, r);
  }
  void take_waiting(int mask, list<Context*>& ls) {
    parent->take_waiting(mask << wait_offset, ls);
  }
  void add_waiter(int mask, Context *c) {
    parent->add_waiter(mask << wait_offset, c);
  }
  bool is_waiter_for(int mask) {
    return parent->is_waiter_for(mask << wait_offset);
  }
  
  

  // state
  int get_state() { return state; }
  int set_state(int s) { 
    state = s; 
    assert(!is_stable() || gather_set.size() == 0);  // gather should be empty in stable states.
    return s;
  };
  bool is_stable() {
    return state >= 0;
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

  // ref counting
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

  void get_xlock(MDRequest *who) { 
    assert(xlock_by == 0);
    parent->get(MDSCacheObject::PIN_LOCK);
    xlock_by = who; 
  }
  void put_xlock() {
    assert(xlock_by);
    parent->put(MDSCacheObject::PIN_LOCK);
    xlock_by = 0;
  }
  bool is_xlocked() { return xlock_by ? true:false; }
  bool is_xlocked_by_other(MDRequest *mdr) {
    return is_xlocked() && xlock_by != mdr;
  }
  MDRequest *get_xlocked_by() { return xlock_by; }
  bool is_used() {
    return is_xlocked() || is_rdlocked();
  }

  // encode/decode
  void _encode(bufferlist& bl) {
    ::_encode_simple(state, bl);
    ::_encode_simple(gather_set, bl);
  }
  void _decode(bufferlist::iterator& p) {
    ::_decode_simple(state, p);
    ::_decode_simple(gather_set, p);
  }

  
  // simplelock specifics
  int get_replica_state() {
    switch (state) {
    case LOCK_LOCK:
    case LOCK_GLOCKR: 
      return LOCK_LOCK;
    case LOCK_SYNC:
      return LOCK_SYNC;
    default: 
      assert(0);
    }
    return 0;
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

  bool can_rdlock(MDRequest *mdr) {
    //if (state == LOCK_LOCK && mdr && xlock_by == mdr) return true; // xlocked by me.  (actually, is this right?)
    //if (state == LOCK_LOCK && !xlock_by && parent->is_auth()) return true;
    return (state == LOCK_SYNC);
  }
  bool can_xlock(MDRequest *mdr) {
    if (mdr && xlock_by == mdr) {
      assert(state == LOCK_LOCK);
      return true; // auth or replica!  xlocked by me.
    }
    if (state == LOCK_LOCK && parent->is_auth() && !xlock_by) return true;
    return false;
  }
  bool can_xlock_soon() {
    if (parent->is_auth())
      return (state == LOCK_GLOCKR);
    else
      return false;
  }

  virtual void print(ostream& out) {
    out << "(";
    out << get_lock_type_name(get_type()) << " ";
    out << get_simplelock_state_name(get_state());
    if (!get_gather_set().empty()) out << " g=" << get_gather_set();
    if (is_rdlocked()) 
      out << " r=" << get_num_rdlocks();
    if (is_xlocked())
      out << " x=" << get_xlocked_by();
    out << ")";
  }
};

inline ostream& operator<<(ostream& out, SimpleLock& l) 
{
  l.print(out);
  return out;
}


#endif
