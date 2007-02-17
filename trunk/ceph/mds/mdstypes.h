#ifndef __MDSTYPES_H
#define __MDSTYPES_H


#include <math.h>
#include <ostream>
#include <set>
#include <map>
using namespace std;

#include "config.h"
#include "common/DecayCounter.h"

#include <cassert>



// md ops
#define MDS_OP_STATFS   1

#define MDS_OP_STAT     100
#define MDS_OP_LSTAT    101
#define MDS_OP_UTIME    102
#define MDS_OP_CHMOD    103
#define MDS_OP_CHOWN    104  


#define MDS_OP_READDIR  200
#define MDS_OP_MKNOD    201
#define MDS_OP_LINK     202
#define MDS_OP_UNLINK   203
#define MDS_OP_RENAME   204

#define MDS_OP_MKDIR    220
#define MDS_OP_RMDIR    221
#define MDS_OP_SYMLINK  222

#define MDS_OP_OPEN     301
#define MDS_OP_TRUNCATE 306
#define MDS_OP_FSYNC    307
//#define MDS_OP_CLOSE    310
#define MDS_OP_RELEASE  308



// ================================================================

/* meta_load_t
 * hierarchical load for an inode/dir and it's children
 */
#define META_POP_IRD    0
#define META_POP_IWR    1
#define META_POP_DWR    2
//#define META_POP_LOG   3
//#define META_POP_FDIR  4
//#define META_POP_CDIR  4
#define META_NPOP      3

class meta_load_t {
 public:
  DecayCounter pop[META_NPOP];

  double meta_load() {
    return pop[META_POP_IRD].get() + 2*pop[META_POP_IWR].get();
  }

  void take(meta_load_t& other) {
    for (int i=0; i<META_NPOP; i++) {
      pop[i] = other.pop[i];
      other.pop[i].reset();
    }
  }
};

inline ostream& operator<<( ostream& out, meta_load_t& load )
{
  return out << "<rd " << load.pop[META_POP_IRD].get()
             << ", wr " << load.pop[META_POP_IWR].get()
             << ">";
}


inline meta_load_t& operator-=(meta_load_t& l, meta_load_t& r)
{
  for (int i=0; i<META_NPOP; i++)
    l.pop[i].adjust(- r.pop[i].get());
  return l;
}

inline meta_load_t& operator+=(meta_load_t& l, meta_load_t& r)
{
  for (int i=0; i<META_NPOP; i++)
    l.pop[i].adjust(r.pop[i].get());
  return l;
}



/* mds_load_t
 * mds load
 */

// popularity classes
#define MDS_POP_JUSTME  0   // just me (this dir or inode)
#define MDS_POP_NESTED  1   // me + children, auth or not
#define MDS_POP_CURDOM  2   // me + children in current auth domain
#define MDS_POP_ANYDOM  3   // me + children in any (nested) auth domain
//#define MDS_POP_DIRMOD  4   // just this dir, modifications only
#define MDS_NPOP        4

class mds_load_t {
 public:
  meta_load_t root;

  double req_rate;
  double cache_hit_rate;
  double queue_len;

  mds_load_t() : 
    req_rate(0), cache_hit_rate(0), queue_len(0) { }    

  double mds_load() {
    switch(g_conf.mds_bal_mode) {
    case 0: 
      return root.meta_load()
        + req_rate
        + 10.0*queue_len;

    case 1:
      return req_rate + 10.0*queue_len;
    }
    assert(0);
    return 0;
  }

};


inline ostream& operator<<( ostream& out, mds_load_t& load )
{
  return out << "mdsload<" << load.root
             << ", req " << load.req_rate 
             << ", hr " << load.cache_hit_rate
             << ", qlen " << load.queue_len
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


// ================================================================
// dir slices

struct dirslice_t {
  short hash_mask;
  short hash_val;
};



// ================================================================

#define MDS_PIN_REPLICATED 1

class MDSCacheObject {
 protected:
  unsigned state;     // state bits
  
  int      ref;       // reference count
  set<int> ref_set;

  map<int,int> replicas;      // [auth] mds -> nonce
  int          replica_nonce; // [replica] defined on replica

 public:
  MDSCacheObject() :
	state(0),
	ref(0),
	replica_nonce(0) {}
  virtual ~MDSCacheObject() {}
  
  // --------------------------------------------
  // state
  unsigned get_state() { return state; }
  void state_clear(unsigned mask) { state &= ~mask; }
  void state_set(unsigned mask) { state |= mask; }
  unsigned state_test(unsigned mask) { return state & mask; }
  void state_reset(unsigned s) { state = s; }

  // --------------------------------------------
  // pins
  int get_num_ref() { return ref; }
  bool is_pinned_by(int by) { return ref_set.count(by); }
  set<int>& get_ref_set() { return ref_set; }

  virtual void last_put() {}
  virtual void bad_put(int by) {
	assert(ref_set.count(by) == 1);
	assert(ref > 0);
  }
  void put(int by) {
    if (ref == 0 || ref_set.count(by) != 1) {
	  bad_put(by);
    } else {
	  ref--;
	  ref_set.erase(by);
	  assert(ref == (int)ref_set.size());
	  if (ref == 0)
		last_put();
	}
  }

  virtual void first_get() {}
  virtual void bad_get(int by) {
	assert(ref_set.count(by) == 0);
	assert(0);
  }
  void get(int by) {
    if (ref_set.count(by)) {
	  bad_get(by);
    } else {
	  if (ref == 0) 
		first_get();
	  ref++;
	  ref_set.insert(by);
	  assert(ref == (int)ref_set.size());
	}
  }



  // --------------------------------------------
  // replication
  bool is_replicated() { return !replicas.empty(); }
  bool is_replica(int mds) { return replicas.count(mds); }
  int num_replicas() { return replicas.size(); }
  int add_replica(int mds) {
	if (replicas.count(mds)) 
	  return ++replicas[mds];  // inc nonce
	if (replicas.empty()) 
	  get(MDS_PIN_REPLICATED);
	return replicas[mds] = 1;
  }
  void add_replica(int mds, int nonce) {
	if (replicas.empty()) 
	  get(MDS_PIN_REPLICATED);
	replicas[mds] = nonce;
  }
  int get_replica_nonce(int mds) {
	assert(replicas.count(mds));
	return replicas[mds];
  }
  void remove_replica(int mds) {
	assert(replicas.count(mds));
	replicas.erase(mds);
	if (replicas.empty())
	  put(MDS_PIN_REPLICATED);
  }
  void clear_replicas() {
	if (!replicas.empty())
	  put(MDS_PIN_REPLICATED);
	replicas.clear();
  }
  map<int,int>::iterator replicas_begin() { return replicas.begin(); }
  map<int,int>::iterator replicas_end() { return replicas.end(); }
  const map<int,int>& get_replicas() { return replicas; }

  int get_replica_nonce() { return replica_nonce;}
  void set_replica_nonce(int n) { replica_nonce = n; }
};


#endif
