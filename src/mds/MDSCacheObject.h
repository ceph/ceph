#ifndef CEPH_MDSCACHEOBJECT_H
#define CEPH_MDSCACHEOBJECT_H

#include <ostream>
#include <string_view>

#include "common/config.h"

#include "include/Context.h"
#include "include/ceph_assert.h"
#include "include/mempool.h"
#include "include/types.h"
#include "include/xlist.h"

#include "mdstypes.h"
#include "MDSContext.h"
#include "include/elist.h"

#define MDS_REF_SET      // define me for improved debug output, sanity checking
//#define MDS_AUTHPIN_SET  // define me for debugging auth pin leaks
//#define MDS_VERIFY_FRAGSTAT    // do (slow) sanity checking on frags

/*
 * for metadata leases to clients
 */
class MLock;
class SimpleLock;
class MDSCacheObject;
class MDSContext;

namespace ceph {
class Formatter;
}

struct ClientLease {
  ClientLease(client_t c, MDSCacheObject *p) :
    client(c), parent(p),
    item_session_lease(this),
    item_lease(this) { }
  ClientLease() = delete;

  client_t client;
  MDSCacheObject *parent;

  ceph_seq_t seq = 0;
  utime_t ttl;
  xlist<ClientLease*>::item item_session_lease; // per-session list
  xlist<ClientLease*>::item item_lease;         // global list
};

// print hack
struct mdsco_db_line_prefix {
  explicit mdsco_db_line_prefix(MDSCacheObject *o) : object(o) {}
  MDSCacheObject *object;
};

class MDSCacheObject {
 public:
  typedef mempool::mds_co::compact_map<mds_rank_t,unsigned> replica_map_type;

  /* Mask for waiters. It is 128 bits to accomodate lock waiters. Its layout:
   *
   *  Most-significant                 Least significant
   * [   SimpleLock 64 bits  |   MDSCacheObject 64 bits ]
   *
   * It is organized this way for simplicity not for compactness and because
   * the total bits will be >64 bits.
   */
  using waitmask_t = std::bitset<128>;

  struct ptr_lt {
    bool operator()(const MDSCacheObject* l, const MDSCacheObject* r) const {
      return l->is_lt(r);
    }
  };

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
  static const int PIN_SCRUBQUEUE = 1011;     // for scrub of inode and dir

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

  elist<MDSCacheObject*>::item item_scrub;   // for scrub inode or dir

  MDSCacheObject() {}
  virtual ~MDSCacheObject() {}

  std::string_view generic_pin_name(int p) const;

  // printing
  virtual void print(std::ostream& out) const = 0;
  virtual std::ostream& print_db_line_prefix(std::ostream& out) const {
    return out << "mdscacheobject(" << this << ") "; 
  }

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
  virtual bool is_ambiguous_auth() const {
    return authority().second != CDIR_AUTH_UNKNOWN;
  }

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
  virtual std::string_view pin_name(int by) const = 0;
  //bool is_pinned_by(int by) { return ref_set.count(by); }
  //multiset<int>& get_ref_set() { return ref_set; }

  virtual void last_put() {}
  virtual void bad_put(int by) {
#ifdef MDS_REF_SET
    ceph_assert(ref_map[by] > 0);
#endif
    ceph_assert(ref > 0);
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
    ceph_assert(by < 0 || ref_map[by] == 0);
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
    for(auto const &p : ref_map) {
      out << " " << pin_name(p.first) << "=" << p.second;
    }
#else
    out << " nref=" << ref;
#endif
  }

  int get_num_auth_pins() const { return auth_pins; }
#ifdef MDS_AUTHPIN_SET
  void print_authpin_set(std::ostream& out) const {
    out << " (" << auth_pin_set << ")";
  }
#endif

  void dump_states(ceph::Formatter *f) const;
  void dump(ceph::Formatter *f) const;

  // auth pins
  enum {
    // can_auth_pin() error codes
    ERR_NOT_AUTH = 1,
    ERR_EXPORTING_TREE,
    ERR_FRAGMENTING_DIR,
    ERR_EXPORTING_INODE,
  };
  virtual bool can_auth_pin(int *err_code=nullptr, bool bypassfreezing=false) const = 0;
  virtual void auth_pin(void *who) = 0;
  virtual void auth_unpin(void *who) = 0;
  virtual bool is_frozen() const = 0;
  virtual bool is_freezing() const = 0;
  virtual bool is_freezing_or_frozen() const {
    return is_frozen() || is_freezing();
  }

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
    ceph_assert(get_replicas().count(mds));
    return get_replicas()[mds];
  }
  void remove_replica(mds_rank_t mds) {
    ceph_assert(get_replicas().count(mds));
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

  /* A uint64_t mask is accepted to accomodate existing code that expects the
   * mask to actually be a 64 bit integer.
   */
  bool is_waiter_for(uint64_t mask) {
    return is_waiter_for(waitmask_t(mask));
  }
  bool is_waiter_for(waitmask_t mask);

  virtual void add_waiter(uint64_t mask, MDSContext *c) {
    add_waiter(waitmask_t(mask), c);
  }
  void add_waiter(waitmask_t mask, MDSContext *c) {
    if (waiting.empty())
      get(PIN_WAITER);

    waiter_seq_t seq = 0;
    if ((mask & waitmask_t(WAIT_ORDERED)).any()) {
      seq = ++last_wait_seq;
      mask &= ~waitmask_t(WAIT_ORDERED);
    } else {
      /* always at the front */
      seq = 0;
    }
    waiting.insert(std::pair<waiter_seq_t, waiter>(seq, waiter(mask, c)));
  }
  virtual void take_waiting(uint64_t mask, MDSContext::vec& ls) {
    take_waiting(waitmask_t(mask), ls);
  }
  void take_waiting(waitmask_t mask, MDSContext::vec& ls);
  void finish_waiting(uint64_t mask, int result = 0) {
    finish_waiting(waitmask_t(mask), result);
  }
  void finish_waiting(waitmask_t mask, int result = 0);

  // ---------------------------------------------
  // locking
  // noop unless overloaded.
  virtual SimpleLock* get_lock(int type) { ceph_abort(); return 0; }
  virtual void set_object_info(MDSCacheObjectInfo &info) { ceph_abort(); }
  virtual void encode_lock_state(int type, ceph::buffer::list& bl) { ceph_abort(); }
  virtual void decode_lock_state(int type, const ceph::buffer::list& bl) { ceph_abort(); }
  virtual void finish_lock_waiters(int type, uint64_t mask, int r=0) { ceph_abort(); }
  virtual void add_lock_waiter(int type, uint64_t mask, MDSContext *c) { ceph_abort(); }
  virtual bool is_lock_waiting(int type, uint64_t mask) { ceph_abort(); return false; }

  virtual void clear_dirty_scattered(int type) { ceph_abort(); }

  // ---------------------------------------------
  // ordering
  virtual bool is_lt(const MDSCacheObject *r) const = 0;

  // state
 protected:
  __u32 state = 0;     // state bits

  // pins
  __s32      ref = 0;       // reference count
#ifdef MDS_REF_SET
  mempool::mds_co::flat_map<int,int> ref_map;
#endif

  int auth_pins = 0;
#ifdef MDS_AUTHPIN_SET
  mempool::mds_co::multiset<void*> auth_pin_set;
#endif

  // replication (across mds cluster)
  unsigned replica_nonce = 0; // [replica] defined on replica
    replica_map_type replica_map;   // [auth] mds -> nonce

  // ---------------------------------------------
  // waiting
 private:
  using waiter_seq_t = uint64_t;
  struct waiter {
    waitmask_t mask;
    MDSContext* c;
  };
  mempool::mds_co::compact_multimap<waiter_seq_t, struct waiter> waiting;
  static waiter_seq_t last_wait_seq;
};

inline std::ostream& operator<<(std::ostream& out, const mdsco_db_line_prefix& o) {
  o.object->print_db_line_prefix(out);
  return out;
}
#endif
