// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
#ifndef __MDSTYPES_H
#define __MDSTYPES_H


#include <math.h>
#include <ostream>
#include <set>
#include <map>
using namespace std;

#include "config.h"
#include "common/DecayCounter.h"
#include "include/Context.h"

#include <cassert>

#include "include/frag.h"
#include "include/xlist.h"

#define MDS_REF_SET    // define me for improved debug output, sanity checking


#define MDS_PORT_CACHE   0x200
#define MDS_PORT_LOCKER  0x300
#define MDS_PORT_MIGRATOR 0x400


#define MAX_MDS                   0x100

#define MDS_INO_ROOT              1
#define MDS_INO_PGTABLE           2
#define MDS_INO_ANCHORTABLE       3

#define MDS_INO_LOG_OFFSET        (1*MAX_MDS)
#define MDS_INO_IDS_OFFSET        (2*MAX_MDS)
#define MDS_INO_CLIENTMAP_OFFSET  (3*MAX_MDS)
#define MDS_INO_SESSIONMAP_OFFSET (4*MAX_MDS)
#define MDS_INO_STRAY_OFFSET      (5*MAX_MDS)
#define MDS_INO_BASE              (6*MAX_MDS)

#define MDS_INO_STRAY(x) (MDS_INO_STRAY_OFFSET+((unsigned)x))
#define MDS_INO_IS_STRAY(i) ((i) >= MDS_INO_STRAY_OFFSET && (i) < MDS_INO_STRAY_OFFSET+MAX_MDS)

#define MDS_TRAVERSE_FORWARD       1
#define MDS_TRAVERSE_DISCOVER      2    // skips permissions checks etc.
#define MDS_TRAVERSE_DISCOVERXLOCK 3    // succeeds on (foreign?) null, xlocked dentries.
#define MDS_TRAVERSE_FAIL          4


struct metareqid_t {
  entity_name_t name;
  uint64_t tid;
  metareqid_t() : tid(0) {}
  //metareqid_t(int c, tid_t t) : tid(t) { name = entity_name_t::CLIENT(c); }
  metareqid_t(entity_name_t n, tid_t t) : name(n), tid(t) {}
};

inline ostream& operator<<(ostream& out, const metareqid_t& r) {
  return out << r.name << ":" << r.tid;
}

inline bool operator==(const metareqid_t& l, const metareqid_t& r) {
  return (l.name == r.name) && (l.tid == r.tid);
}
inline bool operator!=(const metareqid_t& l, const metareqid_t& r) {
  return (l.name != r.name) || (l.tid != r.tid);
}
inline bool operator<(const metareqid_t& l, const metareqid_t& r) {
  return (l.name < r.name) || 
    (l.name == r.name && l.tid < r.tid);
}
inline bool operator<=(const metareqid_t& l, const metareqid_t& r) {
  return (l.name < r.name) ||
    (l.name == r.name && l.tid <= r.tid);
}
inline bool operator>(const metareqid_t& l, const metareqid_t& r) { return !(l <= r); }
inline bool operator>=(const metareqid_t& l, const metareqid_t& r) { return !(l < r); }

namespace __gnu_cxx {
  template<> struct hash<metareqid_t> {
    size_t operator()(const metareqid_t &r) const { 
      hash<uint64_t> H;
      return H(r.name.num()) ^ H(r.name.type()) ^ H(r.tid);
    }
  };
}


// inode caps info for client reconnect
struct inode_caps_reconnect_t {
  int32_t wanted;
  int32_t issued;
  off_t size;
  utime_t mtime, atime;

  inode_caps_reconnect_t() {}
  inode_caps_reconnect_t(int w, int i) : 
    wanted(w), issued(i), size(0) {}
  inode_caps_reconnect_t(int w, int i, off_t sz, utime_t mt, utime_t at) : 
    wanted(w), issued(i), size(sz), mtime(mt), atime(at) {}
};


// ================================================================
// dir frag

struct dirfrag_t {
  inodeno_t ino;
  frag_t    frag;
  uint32_t  _pad;

  dirfrag_t() : ino(0), _pad(0) { }
  dirfrag_t(inodeno_t i, frag_t f) : ino(i), frag(f), _pad(0) { }
};

inline ostream& operator<<(ostream& out, const dirfrag_t df) {
  out << df.ino;
  if (!df.frag.is_root()) out << "." << df.frag;
  return out;
}
inline bool operator<(dirfrag_t l, dirfrag_t r) {
  if (l.ino < r.ino) return true;
  if (l.ino == r.ino && l.frag < r.frag) return true;
  return false;
}
inline bool operator==(dirfrag_t l, dirfrag_t r) {
  return l.ino == r.ino && l.frag == r.frag;
}

namespace __gnu_cxx {
  template<> struct hash<dirfrag_t> {
    size_t operator()(const dirfrag_t &df) const { 
      static rjhash<uint64_t> H;
      static rjhash<uint32_t> I;
      return H(df.ino) ^ I(df.frag);
    }
  };
}



// ================================================================

#define META_POP_IRD     0
#define META_POP_IWR     1
#define META_POP_READDIR 2
#define META_POP_FETCH   3
#define META_POP_STORE   4
#define META_NPOP        5

class inode_load_vec_t {
  static const int NUM = 2;
  DecayCounter vec[NUM];
public:
  DecayCounter &get(int t) { 
    assert(t < NUM);
    return vec[t]; 
  }
  void zero(utime_t now) {
    for (int i=0; i<NUM; i++) 
      vec[i].reset(now);
  }
};

class dirfrag_load_vec_t {
public:
  static const int NUM = 5;
  DecayCounter vec[NUM];
  DecayCounter &get(int t) { 
    assert(t < NUM);
    return vec[t]; 
  }
  void adjust(utime_t now, double d) {
    for (int i=0; i<NUM; i++) 
      vec[i].adjust(now, d);
  }
  void zero(utime_t now) {
    for (int i=0; i<NUM; i++) 
      vec[i].reset(now);
  }
  double meta_load(utime_t now) {
    return 
      1*vec[META_POP_IRD].get(now) + 
      2*vec[META_POP_IWR].get(now) +
      1*vec[META_POP_READDIR].get(now) +
      2*vec[META_POP_FETCH].get(now) +
      4*vec[META_POP_STORE].get(now);
  }
  double meta_load() {
    return 
      1*vec[META_POP_IRD].get_last() + 
      2*vec[META_POP_IWR].get_last() +
      1*vec[META_POP_READDIR].get_last() +
      2*vec[META_POP_FETCH].get_last() +
      4*vec[META_POP_STORE].get_last();
  }
};

inline dirfrag_load_vec_t& operator+=(dirfrag_load_vec_t& l, dirfrag_load_vec_t& r)
{
  utime_t now = g_clock.now();
  for (int i=0; i<dirfrag_load_vec_t::NUM; i++)
    l.vec[i].adjust(r.vec[i].get(now));
  return l;
}

inline dirfrag_load_vec_t& operator-=(dirfrag_load_vec_t& l, dirfrag_load_vec_t& r)
{
  utime_t now = g_clock.now();
  for (int i=0; i<dirfrag_load_vec_t::NUM; i++)
    l.vec[i].adjust(-r.vec[i].get(now));
  return l;
}

inline dirfrag_load_vec_t& operator*=(dirfrag_load_vec_t& l, double f)
{
  for (int i=0; i<dirfrag_load_vec_t::NUM; i++)
    l.vec[i].scale(f);
  return l;
}


inline ostream& operator<<(ostream& out, dirfrag_load_vec_t& dl)
{
  utime_t now = g_clock.now();
  return out << "[" << dl.vec[0].get(now) << "," << dl.vec[1].get(now) 
	     << " " << dl.meta_load(now)
	     << "]";
}




/* mds_load_t
 * mds load
 */

class mds_load_t {
 public:
  dirfrag_load_vec_t auth;
  dirfrag_load_vec_t all;

  double req_rate;
  double cache_hit_rate;
  double queue_len;

  double cpu_load_avg;

  mds_load_t() : 
    req_rate(0), cache_hit_rate(0), queue_len(0), cpu_load_avg(0) { 
  }
  
  double mds_load();  // defiend in MDBalancer.cc

};


inline ostream& operator<<( ostream& out, mds_load_t& load )
{
  return out << "mdsload<" << load.auth << "/" << load.all
             << ", req " << load.req_rate 
             << ", hr " << load.cache_hit_rate
             << ", qlen " << load.queue_len
	     << ", cpu " << load.cpu_load_avg
             << ">";
}

/*
inline mds_load_t& operator+=( mds_load_t& l, mds_load_t& r ) 
{
  l.root_pop += r.root_pop;
  l.req_rate += r.req_rate;
  l.queue_len += r.queue_len;
  return l;
}

inline mds_load_t operator/( mds_load_t& a, double d ) 
{
  mds_load_t r;
  r.root_pop = a.root_pop / d;
  r.req_rate = a.req_rate / d;
  r.queue_len = a.queue_len / d;
  return r;
}
*/


class load_spread_t {
public:
  static const int MAX = 4;
  int last[MAX];
  int p, n;
  DecayCounter count;

public:
  load_spread_t() : p(0), n(0) { 
    for (int i=0; i<MAX; i++) last[i] = -1;
  } 

  double hit(utime_t now, int who) {
    for (int i=0; i<n; i++)
      if (last[i] == who) 
	return count.get_last();

    // we're new(ish)
    last[p++] = who;
    if (n < MAX) n++;
    if (n == 1) return 0.0;

    if (p == MAX) p = 0;

    return count.hit(now);
  }
  double get(utime_t now) {
    return count.get(now);
  }
};



// ================================================================

//#define MDS_PIN_REPLICATED     1
//#define MDS_STATE_AUTH     (1<<0)

class MLock;
class SimpleLock;

class MDSCacheObject;

// -- authority delegation --
// directory authority types
//  >= 0 is the auth mds
#define CDIR_AUTH_PARENT   -1   // default
#define CDIR_AUTH_UNKNOWN  -2
#define CDIR_AUTH_DEFAULT   pair<int,int>(-1, -2)
#define CDIR_AUTH_UNDEF     pair<int,int>(-2, -2)
//#define CDIR_AUTH_ROOTINODE pair<int,int>( 0, -2)



// print hack
struct mdsco_db_line_prefix {
  MDSCacheObject *object;
  mdsco_db_line_prefix(MDSCacheObject *o) : object(o) {}
};
ostream& operator<<(ostream& out, mdsco_db_line_prefix o);

// printer
ostream& operator<<(ostream& out, MDSCacheObject &o);

class MDSCacheObjectInfo {
public:
  inodeno_t ino;
  dirfrag_t dirfrag;
  string dname;

  MDSCacheObjectInfo() : ino(0) {}

  void _encode(bufferlist& bl) const {
    ::_encode(ino, bl);
    ::_encode(dirfrag, bl);
    ::_encode(dname, bl);
  }
  void _decode(bufferlist& bl, int& off) {
    ::_decode(ino, bl, off);
    ::_decode(dirfrag, bl, off);
    ::_decode(dname, bl, off);
  }
  void _decode(bufferlist::iterator& p) {
    ::_decode_simple(ino, p);
    ::_decode_simple(dirfrag, p);
    ::_decode_simple(dname, p);
  }
};


class MDSCacheObject {
 public:
  // -- pins --
  const static int PIN_REPLICATED =  1000;
  const static int PIN_DIRTY      =  1001;
  const static int PIN_LOCK       = -1002;
  const static int PIN_REQUEST    = -1003;
  const static int PIN_WAITER     =  1004;
  const static int PIN_DIRTYSCATTERED = 1005;   // make this neg if we start using multiple scatterlocks?  
  static const int PIN_AUTHPIN    =  1006;
  static const int PIN_PTRWAITER  = -1007;
  const static int PIN_TEMPEXPORTING = 1008;  // temp pin between encode_ and finish_export

  const char *generic_pin_name(int p) {
    switch (p) {
    case PIN_REPLICATED: return "replicated";
    case PIN_DIRTY: return "dirty";
    case PIN_LOCK: return "lock";
    case PIN_REQUEST: return "request";
    case PIN_WAITER: return "waiter";
    case PIN_DIRTYSCATTERED: return "dirtyscattered";
    case PIN_AUTHPIN: return "authpin";
    case PIN_PTRWAITER: return "ptrwaiter";
    case PIN_TEMPEXPORTING: return "tempexporting";
    default: assert(0); return 0;
    }
  }

  // -- state --
  const static int STATE_AUTH      = (1<<30);
  const static int STATE_DIRTY     = (1<<29);
  const static int STATE_REJOINING = (1<<28);  // replica has not joined w/ primary copy

  // -- wait --
  const static int WAIT_SINGLEAUTH  = (1<<30);
  const static int WAIT_UNFREEZE    = (1<<29); // pka AUTHPINNABLE


  // ============================================
  // cons
 public:
  MDSCacheObject() :
    state(0), 
    ref(0),
    replica_nonce(0) {}
  virtual ~MDSCacheObject() {}

  // printing
  virtual void print(ostream& out) = 0;
  virtual ostream& print_db_line_prefix(ostream& out) { 
    return out << "mdscacheobject(" << this << ") "; 
  }
  
  // --------------------------------------------
  // state
 protected:
  unsigned state;     // state bits

 public:
  unsigned get_state() const { return state; }
  unsigned state_test(unsigned mask) const { return (state & mask); }
  void state_clear(unsigned mask) { state &= ~mask; }
  void state_set(unsigned mask) { state |= mask; }
  void state_reset(unsigned s) { state = s; }

  bool is_auth() const { return state_test(STATE_AUTH); }
  bool is_dirty() const { return state_test(STATE_DIRTY); }
  bool is_clean() const { return !is_dirty(); }
  bool is_rejoining() const { return state_test(STATE_REJOINING); }

  // --------------------------------------------
  // authority
  virtual pair<int,int> authority() = 0;
  bool is_ambiguous_auth() {
    return authority().second != CDIR_AUTH_UNKNOWN;
  }

  // --------------------------------------------
  // pins
protected:
  int      ref;       // reference count
#ifdef MDS_REF_SET
  multiset<int> ref_set;
#endif

 public:
  int get_num_ref() { return ref; }
  virtual const char *pin_name(int by) = 0;
  //bool is_pinned_by(int by) { return ref_set.count(by); }
  //multiset<int>& get_ref_set() { return ref_set; }

  virtual void last_put() {}
  virtual void bad_put(int by) {
#ifdef MDS_REF_SET
    assert(ref_set.count(by) > 0);
#endif
    assert(ref > 0);
  }
  void put(int by) {
#ifdef MDS_REF_SET
    if (ref == 0 || ref_set.count(by) == 0) {
#else
    if (ref == 0) {
#endif
      bad_put(by);
    } else {
      ref--;
#ifdef MDS_REF_SET
      ref_set.erase(ref_set.find(by));
      assert(ref == (int)ref_set.size());
#endif
      if (ref == 0)
	last_put();
    }
  }

  virtual void first_get() {}
  virtual void bad_get(int by) {
#ifdef MDS_REF_SET
    assert(by < 0 || ref_set.count(by) == 0);
#endif
    assert(0);
  }
  void get(int by) {
#ifdef MDS_REF_SET
    if (by >= 0 && ref_set.count(by)) {
      bad_get(by);
    } else {
#endif
      if (ref == 0) 
	first_get();
      ref++;
#ifdef MDS_REF_SET
      ref_set.insert(by);
      assert(ref == (int)ref_set.size());
    }
#endif
  }

  void print_pin_set(ostream& out) {
#ifdef MDS_REF_SET
    multiset<int>::iterator it = ref_set.begin();
    while (it != ref_set.end()) {
      out << " " << pin_name(*it);
      int last = *it;
      int c = 1;
      do {
	it++;
	if (it == ref_set.end()) break;
      } while (*it == last);
      if (c > 1)
	out << "*" << c;
    }
#endif
  }


  // --------------------------------------------
  // auth pins
  virtual bool can_auth_pin() = 0;
  virtual void auth_pin() = 0;
  virtual void auth_unpin() = 0;
  virtual bool is_frozen() = 0;


  // --------------------------------------------
  // replication
 protected:
  map<int,int> replica_map;      // [auth] mds -> nonce
  int          replica_nonce; // [replica] defined on replica

 public:
  bool is_replicated() { return !replica_map.empty(); }
  bool is_replica(int mds) { return replica_map.count(mds); }
  int num_replicas() { return replica_map.size(); }
  int add_replica(int mds) {
    if (replica_map.count(mds)) 
      return ++replica_map[mds];  // inc nonce
    if (replica_map.empty()) 
      get(PIN_REPLICATED);
    return replica_map[mds] = 1;
  }
  void add_replica(int mds, int nonce) {
    if (replica_map.empty()) 
      get(PIN_REPLICATED);
    replica_map[mds] = nonce;
  }
  int get_replica_nonce(int mds) {
    assert(replica_map.count(mds));
    return replica_map[mds];
  }
  void remove_replica(int mds) {
    assert(replica_map.count(mds));
    replica_map.erase(mds);
    if (replica_map.empty())
      put(PIN_REPLICATED);
  }
  void clear_replica_map() {
    if (!replica_map.empty())
      put(PIN_REPLICATED);
    replica_map.clear();
  }
  map<int,int>::iterator replicas_begin() { return replica_map.begin(); }
  map<int,int>::iterator replicas_end() { return replica_map.end(); }
  const map<int,int>& get_replicas() { return replica_map; }
  void list_replicas(set<int>& ls) {
    for (map<int,int>::const_iterator p = replica_map.begin();
	 p != replica_map.end();
	 ++p) 
      ls.insert(p->first);
  }

  int get_replica_nonce() { return replica_nonce;}
  void set_replica_nonce(int n) { replica_nonce = n; }


  // ---------------------------------------------
  // waiting
 protected:
  multimap<int, Context*>  waiting;

 public:
  bool is_waiter_for(int mask) {
    return waiting.count(mask) > 0;    // FIXME: not quite right.
  }
  virtual void add_waiter(int mask, Context *c) {
    if (waiting.empty())
      get(PIN_WAITER);
    waiting.insert(pair<int,Context*>(mask, c));
    pdout(10,g_conf.debug_mds) << (mdsco_db_line_prefix(this)) 
			       << "add_waiter " << hex << mask << dec << " " << c
			       << " on " << *this
			       << dendl;
    
  }
  virtual void take_waiting(int mask, list<Context*>& ls) {
    if (waiting.empty()) return;
    multimap<int,Context*>::iterator it = waiting.begin();
    while (it != waiting.end()) {
      if (it->first & mask) {
	ls.push_back(it->second);
	pdout(10,g_conf.debug_mds) << (mdsco_db_line_prefix(this))
				   << "take_waiting mask " << hex << mask << dec << " took " << it->second
				   << " tag " << it->first
				   << " on " << *this
				   << dendl;
	waiting.erase(it++);
      } else {
	pdout(10,g_conf.debug_mds) << "take_waiting mask " << hex << mask << dec << " SKIPPING " << it->second
				   << " tag " << it->first
				   << " on " << *this 
				   << dendl;
	it++;
      }
    }
    if (waiting.empty())
      put(PIN_WAITER);
  }
  void finish_waiting(int mask, int result = 0) {
    list<Context*> finished;
    take_waiting(mask, finished);
    finish_contexts(finished, result);
  }


  // ---------------------------------------------
  // locking
  // noop unless overloaded.
  virtual SimpleLock* get_lock(int type) { assert(0); return 0; }
  virtual void set_object_info(MDSCacheObjectInfo &info) { assert(0); }
  virtual void encode_lock_state(int type, bufferlist& bl) { assert(0); }
  virtual void decode_lock_state(int type, bufferlist& bl) { assert(0); }
  virtual void finish_lock_waiters(int type, int mask, int r=0) { assert(0); }
  virtual void add_lock_waiter(int type, int mask, Context *c) { assert(0); }
  virtual bool is_lock_waiting(int type, int mask) { assert(0); return false; }

  virtual void clear_dirty_scattered(int type) { assert(0); }

  // ---------------------------------------------
  // ordering
  virtual bool is_lt(const MDSCacheObject *r) const = 0;
  struct ptr_lt {
    bool operator()(const MDSCacheObject* l, const MDSCacheObject* r) const {
      return l->is_lt(r);
    }
  };

};

inline ostream& operator<<(ostream& out, MDSCacheObject &o) {
  o.print(out);
  return out;
}

inline ostream& operator<<(ostream& out, const MDSCacheObjectInfo &info) {
  if (info.ino) return out << info.ino;
  if (info.dname.length()) return out << info.dirfrag << "/" << info.dname;
  return out << info.dirfrag;
}

inline ostream& operator<<(ostream& out, mdsco_db_line_prefix o) {
  o.object->print_db_line_prefix(out);
  return out;
}


#endif
