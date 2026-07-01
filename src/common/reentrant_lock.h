//
// Created by edrodrig on 5/22/26.
//

#ifndef CEPH_REENTRANT_LOCK_H
#define CEPH_REENTRANT_LOCK_H

#include <atomic>
#include <thread>

#include <common/ceph_mutex.h>
#include <include/ceph_assert.h>
#include <include/Context.h>

namespace ceph {

// Shared ownership tracking for locks wrapping a native ceph::mutex.
// Subclasses implement non-reentrant (TrackedLockImpl) or reentrant
// (ReentrantLockImpl) acquire/release policy.
template <class Mutex>
class LockOwnershipMixin {
protected:
  Mutex mtx;
  std::atomic<std::thread::id> owner{};

  void _establish_ownership() noexcept
  {
    owner.store(std::this_thread::get_id(), std::memory_order_release);
  }

  void _relinquish_ownership() noexcept
  {
    owner.store(std::thread::id{}, std::memory_order_release);
  }

  bool _caller_owns_lock(std::memory_order order = std::memory_order_acquire) const
  {
    return owner.load(order) == std::this_thread::get_id();
  }

public:
  LockOwnershipMixin() = default;

#if defined(CEPH_DEBUG_MUTEX) && defined(CEPH_LOCKSTAT)
  explicit LockOwnershipMixin(
      const lockstat_detail::LockStatTraits* traits,
      bool ld = true,
      bool bt = false) : mtx(traits, ld, bt) {}
#elif defined(CEPH_LOCKSTAT)
  explicit LockOwnershipMixin(
      const lockstat_detail::LockStatTraits* traits)
    : mtx(traits) {}
#else
  template <typename ...Args>
  explicit LockOwnershipMixin(Args&& ...args)
    : mtx(ceph::make_mutex(std::forward<Args>(args)...)) {}
#endif

  Mutex& native_mutex() { return mtx; }
  const Mutex& native_mutex() const { return mtx; }
};

// Non-reentrant mutex with ownership queries (is_locked_by_me).  Use when
// assertions or unique_unlock need to know the holder but recursion is not
// required yet.
template <class Mutex>
class TrackedLockImpl : public LockOwnershipMixin<Mutex> {
  std::atomic<int> held{0};

public:
  TrackedLockImpl() = default;

#if defined(CEPH_DEBUG_MUTEX) && defined(CEPH_LOCKSTAT)
  explicit TrackedLockImpl(
      const lockstat_detail::LockStatTraits* traits,
      bool ld = true,
      bool bt = false)
    : LockOwnershipMixin<Mutex>(traits, ld, bt) {}
#elif defined(CEPH_LOCKSTAT)
  explicit TrackedLockImpl(
      const lockstat_detail::LockStatTraits* traits)
    : LockOwnershipMixin<Mutex>(traits) {}
#else
  template <typename ...Args>
  explicit TrackedLockImpl(Args&& ...args)
    : LockOwnershipMixin<Mutex>(std::forward<Args>(args)...) {}
#endif

  void lock()
  {
    if (held.load(std::memory_order_acquire) > 0 &&
	this->_caller_owns_lock(std::memory_order_relaxed)) {
      ceph_abort_msg("TrackedLock is not reentrant");
    }
    this->mtx.lock();
    this->_establish_ownership();
    held.store(1, std::memory_order_relaxed);
  }

  bool try_lock()
  {
    if (held.load(std::memory_order_acquire) > 0 &&
	this->_caller_owns_lock(std::memory_order_relaxed)) {
      return false;
    }
    if (!this->mtx.try_lock()) {
      return false;
    }
    this->_establish_ownership();
    held.store(1, std::memory_order_relaxed);
    return true;
  }

  void unlock()
  {
    ceph_assert(is_locked_by_me());
    held.store(0, std::memory_order_relaxed);
    this->_relinquish_ownership();
    this->mtx.unlock();
  }

  bool is_locked() const
  {
    return held.load(std::memory_order_acquire) > 0;
  }

  bool is_locked_by_me() const
  {
    return held.load(std::memory_order_acquire) > 0 &&
      this->_caller_owns_lock(std::memory_order_relaxed);
  }

  operator bool() const
  {
    return is_locked_by_me();
  }

  int release_for_wait() noexcept
  {
    ceph_assert(is_locked_by_me());
    held.store(0, std::memory_order_relaxed);
    this->_relinquish_ownership();
    return 1;
  }

  void restore_after_wait(int saved) noexcept
  {
    ceph_assert(saved == 1);
    this->_establish_ownership();
    held.store(1, std::memory_order_relaxed);
  }
};

template <class Mutex>
class ReentrantLockImpl : public LockOwnershipMixin<Mutex> {
  std::atomic<int> recursion_count{0};

public:
  ReentrantLockImpl() = default;

#if defined(CEPH_DEBUG_MUTEX) && defined(CEPH_LOCKSTAT)
  explicit ReentrantLockImpl(
      const lockstat_detail::LockStatTraits* traits,
      bool ld = true,
      bool bt = false)
    : LockOwnershipMixin<Mutex>(traits, ld, bt) {}
#elif defined(CEPH_LOCKSTAT)
  explicit ReentrantLockImpl(
      const lockstat_detail::LockStatTraits* traits)
    : LockOwnershipMixin<Mutex>(traits) {}
#else
  template <typename ...Args>
  explicit ReentrantLockImpl(Args&& ...args)
    : LockOwnershipMixin<Mutex>(std::forward<Args>(args)...) {}
#endif

  void lock()
  {
    if (this->_caller_owns_lock(std::memory_order_acquire) &&
	recursion_count.load(std::memory_order_relaxed) > 0) {
      ++recursion_count;
      return;
    }

    this->mtx.lock();
    this->_establish_ownership();
    recursion_count.store(1, std::memory_order_relaxed);
  }

  bool try_lock()
  {
    if (this->_caller_owns_lock(std::memory_order_acquire) &&
	recursion_count.load(std::memory_order_relaxed) > 0) {
      ++recursion_count;
      return true;
    }

    if (!this->mtx.try_lock()) {
      return false;
    }

    this->_establish_ownership();
    recursion_count.store(1, std::memory_order_relaxed);
    return true;
  }

  void unlock()
  {
    ceph_assert(is_locked_by_me());

    if (--recursion_count == 0) {
      this->_relinquish_ownership();
      this->mtx.unlock();
    }
  }

  bool is_locked() const
  {
    return recursion_count.load(std::memory_order_acquire) > 0;
  }

  bool is_locked_by_me() const
  {
    if (recursion_count.load(std::memory_order_acquire) <= 0) {
      return false;
    }
    return this->_caller_owns_lock(std::memory_order_relaxed);
  }

  operator bool() const
  {
    return is_locked_by_me();
  }

  int release_for_wait() noexcept
  {
    ceph_assert(is_locked_by_me());
    int saved = recursion_count.load(std::memory_order_relaxed);
    this->_relinquish_ownership();
    recursion_count.store(0, std::memory_order_relaxed);
    return saved;
  }

  void restore_after_wait(int saved) noexcept
  {
    this->_establish_ownership();
    recursion_count.store(saved, std::memory_order_relaxed);
  }
};

using TrackedLock = TrackedLockImpl<ceph::mutex>;
using ReentrantLock = ReentrantLockImpl<ceph::mutex>;

#ifndef CEPH_LOCKSTAT
template <typename ...Args>
TrackedLock make_tracked(Args&& ...args)
{
  return TrackedLock(std::forward<Args>(args)...);
}

template <typename ...Args>
ReentrantLock make_reentrant(Args&& ...args)
{
  return ReentrantLock(std::forward<Args>(args)...);
}
#endif

} // namespace ceph

#if defined(CEPH_DEBUG_MUTEX) && defined(CEPH_LOCKSTAT)
#define make_tracked(name, ...) \
  TrackedLockImpl<ceph::mutex>(LOCKSTAT(name), ##__VA_ARGS__)
#define make_reentrant(name, ...) \
  ReentrantLockImpl<ceph::mutex>(LOCKSTAT(name), ##__VA_ARGS__)
#elif defined(CEPH_LOCKSTAT)
#define make_tracked(name, ...) \
  TrackedLockImpl<ceph::mutex>(LOCKSTAT(name))
#define make_reentrant(name, ...) \
  ReentrantLockImpl<ceph::mutex>(LOCKSTAT(name))
#endif

namespace std {

template <>
class unique_lock<ceph::ReentrantLock> {
public:
  using mutex_type = ceph::ReentrantLock;

  unique_lock() noexcept
    : _mutex(nullptr), _owns(false)
  {}

  explicit unique_lock(mutex_type& m)
    : unique_lock(m, defer_lock)
  {
    lock();
  }

  unique_lock(mutex_type& m, defer_lock_t) noexcept
    : _mutex(&m), _owns(false)
  {}

  unique_lock(mutex_type& m, adopt_lock_t) noexcept
    : _mutex(&m), _owns(true)
  {}

  unique_lock(const unique_lock&) = delete;
  unique_lock& operator=(const unique_lock&) = delete;
  unique_lock(unique_lock&&) = delete;
  unique_lock& operator=(unique_lock&&) = delete;

  ~unique_lock()
  {
    unlock();
  }

  void lock()
  {
    if (!_mutex) {
      return;
    }
    if (_owns && _mutex->is_locked_by_me()) {
      return;
    }
    _mutex->lock();
    _owns = true;
  }

  bool try_lock()
  {
    if (!_mutex) {
      return false;
    }
    if (_owns && _mutex->is_locked_by_me()) {
      return true;
    }
    if (!_mutex->try_lock()) {
      return false;
    }
    _owns = true;
    return true;
  }

  void unlock()
  {
    if (!_owns) {
      return;
    }
    _owns = false;
    if (_mutex && _mutex->is_locked_by_me()) {
      _mutex->unlock();
    }
  }

  mutex_type *release() noexcept
  {
    _owns = false;
    mutex_type *m = _mutex;
    _mutex = nullptr;
    return m;
  }

  bool owns_lock() const noexcept
  {
    return _owns && _mutex && _mutex->is_locked_by_me();
  }

  mutex_type *mutex() const noexcept
  {
    return _mutex;
  }

private:
  mutex_type *_mutex;
  bool _owns;
};

} // namespace std

namespace ceph {

template <typename LockType = ReentrantLock>
class reentrant_condition_variable_impl {
  ceph::condition_variable cv;

  struct Guard {
    LockType& rl;
    int saved;
    std::unique_lock<ceph::mutex> nl;

    explicit Guard(std::unique_lock<LockType>& lock)
      : rl(*lock.mutex()),
        saved(rl.release_for_wait()),
        nl(rl.native_mutex(), std::adopt_lock) {}

    ~Guard()
    {
      nl.release();
      rl.restore_after_wait(saved);
    }
  };

public:
  void wait(std::unique_lock<LockType>& lock)
  {
    Guard g(lock);
    cv.wait(g.nl);
  }

  template <class Predicate>
  void wait(std::unique_lock<LockType>& lock, Predicate pred)
  {
    while (!pred()) {
      wait(lock);
    }
  }

  template <class Rep, class Period>
  std::cv_status wait_for(std::unique_lock<LockType>& lock,
                          const std::chrono::duration<Rep, Period>& rel_time)
  {
    Guard g(lock);
    return cv.wait_for(g.nl, rel_time);
  }

  template <class Rep, class Period, class Predicate>
  bool wait_for(std::unique_lock<LockType>& lock,
                const std::chrono::duration<Rep, Period>& rel_time,
                Predicate pred)
  {
    return wait_until(lock,
                      std::chrono::steady_clock::now() + rel_time,
                      std::move(pred));
  }

  template <class Clock, class Duration>
  std::cv_status wait_until(std::unique_lock<LockType>& lock,
                            const std::chrono::time_point<Clock, Duration>& abs_time)
  {
    Guard g(lock);
    return cv.wait_until(g.nl, abs_time);
  }

  template <class Clock, class Duration, class Predicate>
  bool wait_until(std::unique_lock<LockType>& lock,
                  const std::chrono::time_point<Clock, Duration>& abs_time,
                  Predicate pred)
  {
    while (!pred()) {
      if (wait_until(lock, abs_time) == std::cv_status::timeout) {
        return pred();
      }
    }
    return true;
  }

  void notify_one() noexcept { cv.notify_one(); }
  void notify_all() noexcept { cv.notify_all(); }
  void notify_all_sloppy() noexcept
  {
#ifdef CEPH_DEBUG_MUTEX
    cv.notify_all(true);
#else
    cv.notify_all();
#endif
  }
};

using reentrant_condition_variable = reentrant_condition_variable_impl<ReentrantLock>;
using tracked_condition_variable = reentrant_condition_variable_impl<TrackedLock>;

inline void wait_for_reentrant_cond_broadcast(std::atomic<bool>& wake_complete)
{
  while (!wake_complete.load(std::memory_order_acquire)) {
    std::this_thread::yield();
  }
}

struct C_ReentrantLock : public Context {
  ceph::ReentrantLock *lock;
  Context *fin;
  C_ReentrantLock(ceph::ReentrantLock *l, Context *c) : lock(l), fin(c) {}
  ~C_ReentrantLock() override
  {
    delete fin;
  }
  void finish(int r) override
  {
    if (fin) {
      std::lock_guard l{*lock};
      fin->complete(r);
      fin = NULL;
    }
  }
};

struct C_TrackedLock : public Context {
  TrackedLock *lock;
  Context *fin;
  C_TrackedLock(TrackedLock *l, Context *c) : lock(l), fin(c) {}
  ~C_TrackedLock() override
  {
    delete fin;
  }
  void finish(int r) override
  {
    if (fin) {
      std::lock_guard l{*lock};
      fin->complete(r);
      fin = NULL;
    }
  }
};

class C_TrackedCond : public Context {
  tracked_condition_variable& cond;
  bool *done;
  int *rval;
public:
  C_TrackedCond(tracked_condition_variable &c, bool *d, int *r)
    : cond(c), done(d), rval(r)
  {
    *done = false;
  }
  void finish(int r) override
  {
    *done = true;
    *rval = r;
    cond.notify_all();
  }
};

template <typename LockType = ReentrantLock>
class C_ReentrantCond : public Context {
  reentrant_condition_variable_impl<LockType>& cond;
  std::atomic<bool> *done;
  std::atomic<bool> *wake_complete;
  int *rval;
public:
  C_ReentrantCond(reentrant_condition_variable_impl<LockType> &c,
                  std::atomic<bool> *d, int *r,
                  std::atomic<bool> *wc = nullptr)
    : cond(c), done(d), wake_complete(wc), rval(r)
  {
    done->store(false, std::memory_order_relaxed);
    if (wake_complete) {
      wake_complete->store(false, std::memory_order_relaxed);
    }
  }
  void finish(int r) override
  {
    *rval = r;
    done->store(true, std::memory_order_release);
    cond.notify_all_sloppy();
    if (wake_complete) {
      wake_complete->store(true, std::memory_order_release);
    }
  }
};

template <class Mutex>
class unique_unlock<TrackedLockImpl<Mutex>> {
public:
  explicit unique_unlock(TrackedLockImpl<Mutex>& tl)
    : m_lock(tl), m_saved(0), m_released(false)
  {
    release();
  }

  unique_unlock(TrackedLockImpl<Mutex>& tl, std::defer_lock_t)
    : m_lock(tl), m_saved(0), m_released(false)
  {}

  void release()
  {
    if (!m_released && m_lock.is_locked_by_me()) {
      m_saved = m_lock.release_for_wait();
      m_lock.native_mutex().unlock();
      m_released = true;
    }
  }

  bool released() const
  {
    return m_released;
  }

  void reacquire()
  {
    if (!m_released || m_saved <= 0) {
      return;
    }
    ceph_assert(!m_lock.is_locked_by_me());
    m_lock.native_mutex().lock();
    m_lock.restore_after_wait(m_saved);
    m_released = false;
    m_saved = 0;
  }

  void _abandon() noexcept
  {
    m_released = false;
    m_saved = 0;
  }

  ~unique_unlock() noexcept(false)
  {
    reacquire();
  }

  unique_unlock(const unique_unlock&) = delete;
  unique_unlock& operator=(const unique_unlock&) = delete;

private:
  TrackedLockImpl<Mutex>& m_lock;
  int m_saved;
  bool m_released;
};

template <class Mutex>
class unique_unlock<ReentrantLockImpl<Mutex>> {
public:
  explicit unique_unlock(ReentrantLockImpl<Mutex>& rl)
    : m_lock(rl), m_saved(0), m_released(false)
  {
    release();
  }

  unique_unlock(ReentrantLockImpl<Mutex>& rl, std::defer_lock_t)
    : m_lock(rl), m_saved(0), m_released(false)
  {}

  void release()
  {
    if (!m_released && m_lock.is_locked_by_me()) {
      m_saved = m_lock.release_for_wait();
      m_lock.native_mutex().unlock();
      m_released = true;
    }
  }

  bool released() const
  {
    return m_released;
  }

  void reacquire()
  {
    if (!m_released || m_saved <= 0) {
      return;
    }
    ceph_assert(!m_lock.is_locked_by_me());
    m_lock.native_mutex().lock();
    m_lock.restore_after_wait(m_saved);
    m_released = false;
    m_saved = 0;
  }

  void _abandon() noexcept
  {
    m_released = false;
    m_saved = 0;
  }

  ~unique_unlock() noexcept(false)
  {
    reacquire();
  }

  unique_unlock(const unique_unlock&) = delete;
  unique_unlock& operator=(const unique_unlock&) = delete;

private:
  ReentrantLockImpl<Mutex>& m_lock;
  int m_saved;
  bool m_released;
};

} // namespace ceph
#endif //CEPH_REENTRANT_LOCK_H