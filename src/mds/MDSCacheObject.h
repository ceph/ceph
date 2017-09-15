#ifndef CEPH_MDSCACHEOBJECT_H
#define CEPH_MDSCACHEOBJECT_H

#include <ostream>

#include "common/config.h"

#include "include/Context.h"
#include "include/alloc_ptr.h"
#include "include/assert.h"
#include "include/mempool.h"
#include "include/types.h"
#include "include/xlist.h"

#include "mdstypes.h"

#define MDS_REF_SET      // define me for improved debug output, sanity checking
//#define MDS_AUTHPIN_SET  // define me for debugging auth pin leaks
//#define MDS_VERIFY_FRAGSTAT    // do (slow) sanity checking on frags


class MLock;
class SimpleLock;
class MDSCacheObject;
class MDSInternalContextBase;

/*
 * for metadata leases to clients
 */
struct ClientLease {
  client_t client;
  MDSCacheObject *parent;

  ceph_seq_t seq;
  utime_t ttl;
  xlist<ClientLease*>::item item_session_lease; // per-session list
  xlist<ClientLease*>::item item_lease;         // global list

  ClientLease(client_t c, MDSCacheObject *p) : 
    client(c), parent(p), seq(0),
    item_session_lease(this),
    item_lease(this) { }
};


// print hack
struct mdsco_db_line_prefix {
  MDSCacheObject *object;
  explicit mdsco_db_line_prefix(MDSCacheObject *o) : object(o) {}
};
std::ostream& operator<<(std::ostream& out, const mdsco_db_line_prefix& o);

// printer
std::ostream& operator<<(std::ostream& out, const MDSCacheObject &o);

class MDSCacheObject {
 public:
  // -- pins --
  const static int PIN_REPLICATED =  1000;
  const static int PIN_DIRTY      =  1001;
  const static int PIN_LOCK       = -1002;
  const static int PIN_REQUEST    = -1003;
  const static int PIN_WAITER     =  1004;
  const static int PIN_DIRTYSCATTERED = -1005;
  static const int PIN_AUTHPIN    =  1006;
  static const int PIN_PTRWAITER  = -1007;
  const static int PIN_TEMPEXPORTING = 1008;  // temp pin between encode_ and finish_export
  static const int PIN_CLIENTLEASE = 1009;
  static const int PIN_DISCOVERBASE = 1010;

  const char *generic_pin_name(int p) const {
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
    case PIN_CLIENTLEASE: return "clientlease";
    case PIN_DISCOVERBASE: return "discoverbase";
    default: ceph_abort(); return 0;
    }
  }

  // -- state --
  const static int STATE_AUTH      = (1<<30);
  const static int STATE_DIRTY     = (1<<29);
  const static int STATE_NOTIFYREF = (1<<28); // notify dropping ref drop through _put()
  const static int STATE_REJOINING = (1<<27);  // replica has not joined w/ primary copy
  const static int STATE_REJOINUNDEF = (1<<26);  // contents undefined.


  // -- wait --
  const static uint64_t WAIT_ORDERED	 = (1ull<<61);
  const static uint64_t WAIT_SINGLEAUTH  = (1ull<<60);
  const static uint64_t WAIT_UNFREEZE    = (1ull<<59); // pka AUTHPINNABLE


  // ============================================
  // cons
 public:
  MDSCacheObject() :
    state(0), 
    ref(0),
    auth_pins(0), nested_auth_pins(0),
    replica_nonce(0)
  {}
  virtual ~MDSCacheObject() {}

  // printing
  virtual void print(std::ostream& out) = 0;
  virtual std::ostream& print_db_line_prefix(std::ostream& out) { 
    return out << "mdscacheobject(" << this << ") "; 
  }
  
  // --------------------------------------------
  // state
 protected:
  __u32 state;     // state bits

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
  virtual mds_authority_t authority() const = 0;
  bool is_ambiguous_auth() const {
    return authority().second != CDIR_AUTH_UNKNOWN;
  }

  // --------------------------------------------
  // pins
protected:
  __s32      ref;       // reference count
#ifdef MDS_REF_SET
  mempool::mds_co::map<int,int> ref_map;
#endif

 public:
  int get_num_ref(int by = -1) const {
#ifdef MDS_REF_SET
    if (by >= 0) {
      if (ref_map.find(by) == ref_map.end()) {
	return 0;
      } else {
        return ref_map.find(by)->second;
      }
    }
#endif
    return ref;
  }
  virtual const char *pin_name(int by) const = 0;
  //bool is_pinned_by(int by) { return ref_set.count(by); }
  //multiset<int>& get_ref_set() { return ref_set; }

  virtual void last_put() {}
  virtual void bad_put(int by) {
#ifdef MDS_REF_SET
    assert(ref_map[by] > 0);
#endif
    assert(ref > 0);
  }
  virtual void _put() {}
  void put(int by) {
#ifdef MDS_REF_SET
    if (ref == 0 || ref_map[by] == 0) {
#else
    if (ref == 0) {
#endif
      bad_put(by);
    } else {
      ref--;
#ifdef MDS_REF_SET
      ref_map[by]--;
#endif
      if (ref == 0)
	last_put();
      if (state_test(STATE_NOTIFYREF))
	_put();
    }
  }

  virtual void first_get() {}
  virtual void bad_get(int by) {
#ifdef MDS_REF_SET
    assert(by < 0 || ref_map[by] == 0);
#endif
    ceph_abort();
  }
  void get(int by) {
    if (ref == 0)
      first_get();
    ref++;
#ifdef MDS_REF_SET
    if (ref_map.find(by) == ref_map.end())
      ref_map[by] = 0;
    ref_map[by]++;
#endif
  }

  void print_pin_set(std::ostream& out) const {
#ifdef MDS_REF_SET
    std::map<int, int>::const_iterator it = ref_map.begin();
    while (it != ref_map.end()) {
      out << " " << pin_name(it->first) << "=" << it->second;
      ++it;
    }
#else
    out << " nref=" << ref;
#endif
  }

  protected:
  int auth_pins;
  int nested_auth_pins;
#ifdef MDS_AUTHPIN_SET
  mempool::mds_co::multiset<void*> auth_pin_set;
#endif

  public:
  bool is_auth_pinned() const { return auth_pins || nested_auth_pins; }
  int get_num_auth_pins() const { return auth_pins; }
  int get_num_nested_auth_pins() const { return nested_auth_pins; }

  void dump_states(Formatter *f) const;
  void dump(Formatter *f) const;

  // --------------------------------------------
  // auth pins
  virtual bool can_auth_pin() const = 0;
  virtual void auth_pin(void *who) = 0;
  virtual void auth_unpin(void *who) = 0;
  virtual bool is_frozen() const = 0;
  virtual bool is_freezing() const = 0;
  virtual bool is_freezing_or_frozen() const {
    return is_frozen() || is_freezing();
  }


  // --------------------------------------------
  // replication (across mds cluster)
 protected:
  unsigned		replica_nonce; // [replica] defined on replica
  typedef compact_map<mds_rank_t,unsigned> replica_map_type;
  replica_map_type replica_map;   // [auth] mds -> nonce

 public:
  bool is_replicated() const { return !get_replicas().empty(); }
  bool is_replica(mds_rank_t mds) const { return get_replicas().count(mds); }
  int num_replicas() const { return get_replicas().size(); }
  unsigned add_replica(mds_rank_t mds) {
    if (get_replicas().count(mds))
      return ++get_replicas()[mds];  // inc nonce
    if (get_replicas().empty())
      get(PIN_REPLICATED);
    return get_replicas()[mds] = 1;
  }
  void add_replica(mds_rank_t mds, unsigned nonce) {
    if (get_replicas().empty())
      get(PIN_REPLICATED);
    get_replicas()[mds] = nonce;
  }
  unsigned get_replica_nonce(mds_rank_t mds) {
    assert(get_replicas().count(mds));
    return get_replicas()[mds];
  }
  void remove_replica(mds_rank_t mds) {
    assert(get_replicas().count(mds));
    get_replicas().erase(mds);
    if (get_replicas().empty()) {
      put(PIN_REPLICATED);
    }
  }
  void clear_replica_map() {
    if (!get_replicas().empty())
      put(PIN_REPLICATED);
    replica_map.clear();
  }
  replica_map_type& get_replicas() { return replica_map; }
  const replica_map_type& get_replicas() const { return replica_map; }
  void list_replicas(std::set<mds_rank_t>& ls) const {
    for (const auto &p : get_replicas()) {
      ls.insert(p.first);
    }
  }

  unsigned get_replica_nonce() const { return replica_nonce; }
  void set_replica_nonce(unsigned n) { replica_nonce = n; }


  // ---------------------------------------------
  // waiting
 private:
  alloc_ptr<mempool::mds_co::multimap<uint64_t, std::pair<uint64_t, MDSInternalContextBase*>>> waiting;
  static uint64_t last_wait_seq;

 public:
  bool is_waiter_for(uint64_t mask, uint64_t min=0) {
    if (!min) {
      min = mask;
      while (min & (min-1))  // if more than one bit is set
        min &= min-1;        //  clear LSB
    }
    if (waiting) {
      for (auto p = waiting->lower_bound(min); p != waiting->end(); ++p) {
        if (p->first & mask) return true;
        if (p->first > mask) return false;
      }
    }
    return false;
  }
  virtual void add_waiter(uint64_t mask, MDSInternalContextBase *c) {
    if (waiting->empty())
      get(PIN_WAITER);

    uint64_t seq = 0;
    if (mask & WAIT_ORDERED) {
      seq = ++last_wait_seq;
      mask &= ~WAIT_ORDERED;
    }
    waiting->insert(pair<uint64_t, pair<uint64_t, MDSInternalContextBase*> >(
			    mask,
			    pair<uint64_t, MDSInternalContextBase*>(seq, c)));
//    pdout(10,g_conf->debug_mds) << (mdsco_db_line_prefix(this)) 
//			       << "add_waiter " << hex << mask << dec << " " << c
//			       << " on " << *this
//			       << dendl;
    
  }
  virtual void take_waiting(uint64_t mask, std::list<MDSInternalContextBase*>& ls) {
    if (!waiting || waiting->empty()) return;

    // process ordered waiters in the same order that they were added.
    std::map<uint64_t, MDSInternalContextBase*> ordered_waiters;

    for (auto it = waiting->begin(); it != waiting->end(); ) {
      if (it->first & mask) {
	    if (it->second.first > 0) {
	      ordered_waiters.insert(it->second);
	    } else {
	      ls.push_back(it->second.second);
        }
//	pdout(10,g_conf->debug_mds) << (mdsco_db_line_prefix(this))
//				   << "take_waiting mask " << hex << mask << dec << " took " << it->second
//				   << " tag " << hex << it->first << dec
//				   << " on " << *this
//				   << dendl;
        waiting->erase(it++);
      } else {
//	pdout(10,g_conf->debug_mds) << "take_waiting mask " << hex << mask << dec << " SKIPPING " << it->second
//				   << " tag " << hex << it->first << dec
//				   << " on " << *this 
//				   << dendl;
	      ++it;
      }
    }
    for (auto it = ordered_waiters.begin(); it != ordered_waiters.end(); ++it) {
      ls.push_back(it->second);
    }
    if (waiting->empty()) {
      put(PIN_WAITER);
      waiting.release();
    }
  }
  void finish_waiting(uint64_t mask, int result = 0);

  // ---------------------------------------------
  // locking
  // noop unless overloaded.
  virtual SimpleLock* get_lock(int type) { ceph_abort(); return 0; }
  virtual void set_object_info(MDSCacheObjectInfo &info) { ceph_abort(); }
  virtual void encode_lock_state(int type, bufferlist& bl) { ceph_abort(); }
  virtual void decode_lock_state(int type, bufferlist& bl) { ceph_abort(); }
  virtual void finish_lock_waiters(int type, uint64_t mask, int r=0) { ceph_abort(); }
  virtual void add_lock_waiter(int type, uint64_t mask, MDSInternalContextBase *c) { ceph_abort(); }
  virtual bool is_lock_waiting(int type, uint64_t mask) { ceph_abort(); return false; }

  virtual void clear_dirty_scattered(int type) { ceph_abort(); }

  // ---------------------------------------------
  // ordering
  virtual bool is_lt(const MDSCacheObject *r) const = 0;
  struct ptr_lt {
    bool operator()(const MDSCacheObject* l, const MDSCacheObject* r) const {
      return l->is_lt(r);
    }
  };

};

inline std::ostream& operator<<(std::ostream& out, MDSCacheObject &o) {
  o.print(out);
  return out;
}

inline std::ostream& operator<<(std::ostream& out, const mdsco_db_line_prefix& o) {
  o.object->print_db_line_prefix(out);
  return out;
}

#endif
