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

// -- lock states --
// sync <-> lock
#define LOCK_UNDEF    0
//                               auth   rep
#define LOCK_SYNC     1  // AR   R .    R .
#define LOCK_LOCK     2  // AR   R W    . .
#define LOCK_SYNC_LOCK  -3  // AR   R .    . .
#define LOCK_REMOTEXLOCK  -50    // on NON-auth

inline const char *get_simplelock_state_name(int n) {
  switch (n) {
  case LOCK_UNDEF: return "UNDEF";
  case LOCK_SYNC: return "sync";
  case LOCK_LOCK: return "lock";
  case LOCK_SYNC_LOCK: return "sync->lock";
  case LOCK_REMOTEXLOCK: return "remote_xlock";
  default: assert(0); return 0;
  }
}

/*


          glockr
       <-/      ^--
  lock      -->    sync
       
    | ^-- glockc
    v
          ^
  clientdel
         

 */

class Mutation;


class SimpleLock {
public:
  static const __u64 WAIT_RD          = (1<<0);  // to read
  static const __u64 WAIT_WR          = (1<<1);  // to write
  static const __u64 WAIT_XLOCK       = (1<<2);  // to xlock   (** dup)
  static const __u64 WAIT_STABLE      = (1<<2);  // for a stable state
  static const __u64 WAIT_REMOTEXLOCK = (1<<3);  // for a remote xlock
  static const int WAIT_BITS        = 4;
  static const __u64 WAIT_ALL         = ((1<<WAIT_BITS)-1);

protected:
  // parent (what i lock)
  MDSCacheObject *parent;
  int type;
  int wait_offset;

  // lock state
  __s32 state;
  set<__s32> gather_set;  // auth+rep.  >= 0 is mds, < 0 is client
  int num_client_lease;

  // local state
  int num_rdlock, num_wrlock;
  Mutation *xlock_by;
  int xlock_by_client;


public:
  SimpleLock(MDSCacheObject *o, int t, int wo) :
    parent(o), type(t), wait_offset(wo),
    state(LOCK_SYNC), num_client_lease(0),
    num_rdlock(0), num_wrlock(0), xlock_by(0), xlock_by_client(-1) { }
  virtual ~SimpleLock() {}

  // parent
  MDSCacheObject *get_parent() { return parent; }
  int get_type() { return type; }

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
    parent->finish_waiting(mask << wait_offset, r);
  }
  void take_waiting(__u64 mask, list<Context*>& ls) {
    parent->take_waiting(mask << wait_offset, ls);
  }
  void add_waiter(__u64 mask, Context *c) {
    parent->add_waiter(mask << wait_offset, c);
  }
  bool is_waiter_for(__u64 mask) {
    return parent->is_waiter_for(mask << wait_offset);
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
  virtual bool can_wrlock() { return false; }
  void get_wrlock(bool force=false) {
    assert(can_wrlock() || force);
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
    parent->get(MDSCacheObject::PIN_LOCK);
    xlock_by = who; 
    xlock_by_client = client;
  }
  void put_xlock() {
    assert(xlock_by);
    parent->put(MDSCacheObject::PIN_LOCK);
    xlock_by = 0;
    xlock_by_client = -1;
  }
  bool is_xlocked() { return xlock_by ? true:false; }
  bool is_xlocked_by_other(Mutation *mdr) {
    return is_xlocked() && xlock_by != mdr;
  }
  Mutation *get_xlocked_by() { return xlock_by; }
  
  bool is_used() {
    return is_xlocked() || is_rdlocked() || num_client_lease;
  }

  void get_client_lease() {
    num_client_lease++;
  }
  void put_client_lease() {
    assert(num_client_lease > 0);
    num_client_lease--;
  }
  int get_num_client_lease() {
    return num_client_lease;
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


  // simplelock specifics
  virtual int get_replica_state() const {
    switch (state) {
    case LOCK_LOCK:
    case LOCK_SYNC_LOCK: 
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

  bool can_lease(int client=-1) {
    if (client >= 0 &&
	xlock_by &&
	xlock_by_client == client)
      return true;  // allow lease to xlocker... see simple_xlock_finish()
    return state == LOCK_SYNC;
  }
  bool can_rdlock(Mutation *mdr) {
    //if (state == LOCK_LOCK && mdr && xlock_by == mdr) return true; // xlocked by me.  (actually, is this right?)
    //if (state == LOCK_LOCK && !xlock_by && parent->is_auth()) return true;
    return (state == LOCK_SYNC);
  }
  bool can_xlock(Mutation *mdr) {
    if (mdr && xlock_by == mdr) {
      assert(state == LOCK_LOCK);
      return true; // auth or replica!  xlocked by me.
    }
    if (state == LOCK_LOCK && parent->is_auth() && !xlock_by) return true;
    return false;
  }
  bool can_xlock_soon() {
    if (parent->is_auth())
      return (state == LOCK_SYNC_LOCK);
    else
      return false;
  }

  virtual void print(ostream& out) {
    out << "(";
    out << get_lock_type_name(get_type()) << " ";
    out << get_simplelock_state_name(get_state());
    if (!get_gather_set().empty()) out << " g=" << get_gather_set();
    if (num_client_lease)
      out << " c=" << num_client_lease;
    if (is_rdlocked()) 
      out << " r=" << get_num_rdlocks();
    if (is_xlocked())
      out << " x=" << get_xlocked_by();
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
