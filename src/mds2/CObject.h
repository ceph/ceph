#ifndef CEPH_COBJECT_H  
#define CEPH_COBJECT_H
#include <atomic>
#include <boost/intrusive_ptr.hpp>
#include "common/Mutex.h"

#include "include/compact_map.h"
#include "include/compact_set.h"
#include "include/assert.h"
#include "common/debug.h"

#include "MDSContext.h"

//#define __MDS_REF_SET

class LogSegment;

class CObject {
protected:
#ifdef __MDS_REF_SET
  Mutex ref_map_lock;
  std::map<int,int> ref_map;
#endif
  std::atomic<int> ref;

  virtual void last_put() {}
  virtual void first_get(bool locked) {}
public:
  // -- pins --
  const static int PIN_DIRTY		= 1001;
  const static int PIN_INTRUSIVEPTR     = 1000;
  const static int PIN_LOCK		= -1002;
  const static int PIN_REQUEST		= -1003;
  const static int PIN_WAITER		= 1004;
  const static int PIN_DIRTYSCATTERED	= -1005;
  static const int PIN_PTRWAITER	= -1007;


  void get(int by, bool locked=false) {
    int old = std::atomic_fetch_add(&ref, 1);
    if (old == 0)
      first_get(locked);
#ifdef __MDS_REF_SET
    Mutex::Locker l(ref_map_lock);
    ref_map[by]++;
#endif
  }
  bool get_unless_zero(int by) {
    int old = std::atomic_load(&ref);
    while (old != 0 && !std::atomic_compare_exchange_weak(&ref, &old, old + 1));
    if (old == 0) 
      return false;
#ifdef __MDS_REF_SET
    Mutex::Locker l(ref_map_lock);
    ref_map[by]++;
#endif
    return true;
  }
  void put(int by) {
    int old = std::atomic_fetch_sub(&ref, 1);
    assert(old >= 1);
    if (old == 1)
      last_put();
#ifdef __MDS_REF_SET
    Mutex::Locker l(ref_map_lock);
    assert(--ref_map[by] >= 0);
#endif
  }
  int get_num_ref(int by=-1) const {
    return std::atomic_load(&ref);
  }

protected:
  std::atomic<uint64_t> state;
public:
  // -- state --
  const static uint64_t STATE_DIRTY	= (1ULL<<48);

  uint64_t get_state() const {
    return std::atomic_load(&state);
  }
  uint64_t state_test(uint64_t mask) const {
    return std::atomic_load(&state) & mask;
  }
  uint64_t state_set(uint64_t mask) {
    return std::atomic_fetch_or(&state, mask);
  }
  uint64_t state_clear(uint64_t mask) {
    return std::atomic_fetch_and(&state, ~mask);
  }
  void state_reset(uint64_t mask) {
    return std::atomic_store(&state, mask);
  }

  bool is_auth() const { return true; }
  bool is_dirty() const { return state_test(STATE_DIRTY); }
  bool is_clean() const { return !is_dirty(); }

protected:
  mutable Mutex mutex;
public:
  void mutex_lock() const { mutex.Lock(); }
  void mutex_unlock() const { mutex.Unlock(); }
  bool mutex_trylock() const { return mutex.TryLock(); }
  bool mutex_is_locked_by_me() const  { return mutex.is_locked_by_me(); }
  void mutex_assert_locked_by_me() const { assert(mutex.is_locked_by_me()); }

  class Locker {
    CObject *o;
  public:
    Locker(CObject *_o) : o(_o) { if (o) o->mutex_lock(); }
    ~Locker() { if(o) o->mutex_unlock(); }
  };

protected:
  compact_multimap<uint64_t, pair<uint64_t, MDSContextBase*> > waiting;
  uint64_t last_wait_seq;
public:
  // -- wait --
  const static uint64_t WAIT_ORDERED	= (1ull<<56);

  bool is_waiting_for(uint64_t mask, uint64_t min=0) {
    if (!min) {
      min = mask;
      while (min & (min-1))  // if more than one bit is set
        min &= min-1;        //  clear LSB
    }
    for (auto p = waiting.lower_bound(min);
         p != waiting.end();
         ++p) {
      if (p->first & mask) return true;
      if (p->first > mask) return false;
    }
    return false;
  }
  virtual void add_waiter(uint64_t mask, MDSContextBase *c) {
    if (waiting.empty())
      get(PIN_WAITER);

    uint64_t seq = 0;
    if (mask & WAIT_ORDERED) {
      seq = ++last_wait_seq;
      mask &= ~WAIT_ORDERED;
    }
    waiting.insert(pair<uint64_t, pair<uint64_t, MDSContextBase*> >(
		      mask,
		      pair<uint64_t, MDSContextBase*>(seq, c)));
  }
  virtual void take_waiting(uint64_t mask, list<MDSContextBase*>& ls) {
    if (waiting.empty())
      return;

    // process ordered waiters in the same order that they were added.
    std::map<uint64_t, MDSContextBase*> ordered_waiters;

    for (auto it = waiting.begin(); it != waiting.end(); ) {
      if (it->first & mask) {
	if (it->second.first > 0)
	  ordered_waiters.insert(it->second);
	else
	  ls.push_back(it->second.second);
	waiting.erase(it++);
      } else {
	++it;
      }
    }
    for (auto it = ordered_waiters.begin(); it != ordered_waiters.end(); ++it) {
      ls.push_back(it->second);
    }
    if (waiting.empty())
      put(PIN_WAITER);
  }
  void finish_waiting(uint64_t mask, int result = 0) {
    list<MDSContextBase*> finished;
    take_waiting(mask, finished);
    finish_contexts(g_ceph_context, finished, result);
  }

  virtual void mark_dirty_scattered(int type, LogSegment *ls) { assert(0); }
  virtual void clear_dirty_scattered(int type) { assert(0); }

  virtual bool is_lt(const CObject *r) const = 0;
  struct ptr_lt {
    bool operator()(const CObject* l, const CObject* r) const {
      return l->is_lt(r);
    }
  };

  CObject(const string &type_name) :
#ifdef __MDS_REF_SET
    ref_map_lock("CObject::ref_map_lock"),
#endif
    ref(ATOMIC_VAR_INIT(0)), state(ATOMIC_VAR_INIT(0)),
    mutex(type_name + "::mutex"),
    last_wait_seq(0) {}

  virtual ~CObject() {}
};

inline std::ostream& operator<<(std::ostream& out, CObject &o)
{
  return out;
}

class CInode;
class CDir;
class CDentry;

typedef boost::intrusive_ptr<CObject> CObjectRef;
typedef boost::intrusive_ptr<CInode> CInodeRef;
typedef boost::intrusive_ptr<CDir> CDirRef;
typedef boost::intrusive_ptr<CDentry> CDentryRef;

static inline void intrusive_ptr_add_ref(CObject *o)
{
  o->get(CObject::PIN_INTRUSIVEPTR);
}
static inline void intrusive_ptr_release(CObject *o)
{
  o->put(CObject::PIN_INTRUSIVEPTR);
}
extern void intrusive_ptr_add_ref(CInode *in);
extern void intrusive_ptr_release(CInode *in);
extern void intrusive_ptr_add_ref(CDir *dir);
extern void intrusive_ptr_release(CDir *dir);
extern void intrusive_ptr_add_ref(CDentry *dn);
extern void intrusive_ptr_release(CDentry *dn);

#endif
