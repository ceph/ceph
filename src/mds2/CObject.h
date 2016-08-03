#ifndef CEPH_COBJECT_H  
#define CEPH_COBJECT_H
#include <atomic>
#include "common/Mutex.h"
#include <boost/intrusive_ptr.hpp>

class CObject {
protected:

  std::atomic<int> ref;
  std::atomic<uint64_t> state;

  Mutex mutex;

  virtual void last_put() {}
  virtual void first_get() {}
public:
  // -- pins --
  const static int PIN_DIRTY		= 1001;
  static const int PIN_INTRUSIVEPTR     = 1000;
  const static int PIN_REQUEST		= -1003;

  // -- state --
  const static uint64_t STATE_DIRTY		= (1ULL<<48);

  void get(int by) {
    int old = std::atomic_fetch_add(&ref, 1);
    if (old == 0)
      first_get();
  }
  void put(int by) {
    int old = std::atomic_fetch_sub(&ref, 1);
    assert(old >= 1);
    if (old == 1)
      last_put();
  }
  int get_num_ref(int by=-1) {
    return std::atomic_load(&ref);
  }

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

  bool is_dirty() const { return state_test(STATE_DIRTY); }
  bool is_clean() const { return !is_dirty(); }

  void mutex_lock() { mutex.Lock(); }
  void mutex_unlock() { mutex.Unlock(); }
  bool mutex_trylock() { return mutex.TryLock(); }
  bool mutex_is_locked_by_me() const  { return mutex.is_locked_by_me(); }
  void mutex_assert_locked_by_me() const { assert(mutex.is_locked_by_me()); }

  virtual ~CObject() {}
  CObject(const string &type_name) :
    ref(ATOMIC_VAR_INIT(0)), state(ATOMIC_VAR_INIT(0)),
    mutex(type_name + "::mutex") {}
};

class CInode;
class CDir;
class CDentry;

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
#endif
