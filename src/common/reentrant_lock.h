//
// Created by edrodrig on 5/22/26.
//

#ifndef CEPH_REENTRANT_LOCK_H
#define CEPH_REENTRANT_LOCK_H

#include <atomic>
#include <thread>

#include <common/ceph_mutex.h>
#include <include/Context.h>

namespace ceph {

template <class Mutex>
class ReentrantLockImpl {
  Mutex mtx;                    // The real mutex for waiting
  std::atomic<std::thread::id> owner{};
  std::atomic<int> recursion_count{0};  // Or plain int + proper fencing

public:
  ReentrantLockImpl() = default;

#if defined(CEPH_DEBUG_MUTEX) && defined(CEPH_LOCKSTAT)
  explicit ReentrantLockImpl(
      const lockstat_detail::LockStatTraits* traits,
      bool ld = true,
      bool bt = false) : mtx(traits, ld, bt) {}
#elif defined(CEPH_LOCKSTAT)
  explicit ReentrantLockImpl(
      const lockstat_detail::LockStatTraits* traits)
    : mtx(traits) {}
#else
  template <typename ...Args>
  explicit ReentrantLockImpl(Args&& ...args)
    : mtx(ceph::make_mutex(std::forward<Args>(args)...)) {}
#endif

  void lock() {
    auto tid = std::this_thread::get_id();

    // Fast path: already owner
    if (owner.load(std::memory_order_acquire) == tid) {
      ++recursion_count;   // No atomic needed here (we own it)
      return;
    }

    // Slow path
    mtx.lock();              // Now we have exclusive access

    owner.store(tid, std::memory_order_release);
    recursion_count.store(1, std::memory_order_relaxed);
  }

  bool try_lock() {
    auto tid = std::this_thread::get_id();

    if (owner.load(std::memory_order_acquire) == tid) {
      ++recursion_count;
      return true;
    }

    if (!mtx.try_lock()) {
      return false;
    }

    owner.store(tid, std::memory_order_release);
    recursion_count.store(1, std::memory_order_relaxed);
    return true;
  }

  void unlock() {
    ceph_assert(owner == std::this_thread::get_id());
    // Assert or handle error in debug: owner == tid

    if (--recursion_count == 0) {
      owner.store({}, std::memory_order_release);
      mtx.unlock();
    }
  }

  Mutex& native_mutex() { return mtx; }  // For condition_variable

  bool is_locked() const {
    return (recursion_count > 0);
  }
  bool is_locked_by_me() const {
    if (recursion_count.load(std::memory_order_acquire) <= 0) {
      return false;
    }
    return owner.load(std::memory_order_relaxed) == std::this_thread::get_id();
  }

  operator bool() const {
    return is_locked_by_me();
  }

  // Called by reentrant_condition_variable before blocking: saves recursion
  // depth and marks the lock as released so other threads can acquire it.
  int release_for_wait() noexcept {
    ceph_assert(is_locked_by_me());
    int saved = recursion_count.load(std::memory_order_relaxed);
    owner.store({}, std::memory_order_release);
    recursion_count.store(0, std::memory_order_relaxed);
    return saved;
  }

  // Called by reentrant_condition_variable after waking: restores the saved
  // recursion depth and re-establishes ownership for the current thread.
  void restore_after_wait(int saved) noexcept {
    owner.store(std::this_thread::get_id(), std::memory_order_release);
    recursion_count.store(saved, std::memory_order_relaxed);
  }
};

using ReentrantLock = ReentrantLockImpl<ceph::mutex>;

#ifndef CEPH_LOCKSTAT
template <typename ...Args>
ReentrantLock make_reentrant(Args&& ...args) {
  return ReentrantLock(std::forward<Args>(args)...);
}
#endif

} // namespace ceph

// make_mutex is a macro when CEPH_LOCKSTAT is enabled; the lock name must be
// expanded at the call site, so make_reentrant is a macro too.
#if defined(CEPH_DEBUG_MUTEX) && defined(CEPH_LOCKSTAT)
#define make_reentrant(name, ...) \
  ReentrantLockImpl<ceph::mutex>(LOCKSTAT(name), ##__VA_ARGS__)
#elif defined(CEPH_LOCKSTAT)
#define make_reentrant(name, ...) \
  ReentrantLockImpl<ceph::mutex>(LOCKSTAT(name))
#endif

namespace std {

/**
 * One reentrant lock level per guard.  lock()/unlock() pair with
 * ReentrantLock::lock()/unlock(); a partner ceph::unique_unlock drops the
 * full recursion depth and restores it on scope exit.
 */
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

// condition_variable that accepts std::unique_lock<LockType> where LockType
// provides a reentrant lock interface (lock/unlock/release_for_wait/restore_after_wait).
//
// On wait, the lock's full recursion depth is saved and the underlying native
// mutex is handed to the real condition_variable.  On wakeup the recursion
// depth is restored, so the caller re-emerges holding the lock with the same
// count as before the wait.
template <typename LockType = ReentrantLock>
class reentrant_condition_variable_impl {
  ceph::condition_variable cv;

  // RAII helper: releases all recursion levels before a wait and restores
  // them afterwards.  Keeps a unique_lock on the native mutex so the
  // real condition_variable can atomically unlock+sleep.
  struct Guard {
    LockType& rl;
    int saved;
    std::unique_lock<ceph::mutex> nl;

    explicit Guard(std::unique_lock<LockType>& lock)
      : rl(*lock.mutex()),
        saved(rl.release_for_wait()),
        nl(rl.native_mutex(), std::adopt_lock) {}

    ~Guard() {
      nl.release();               // native mutex stays locked; rl takes it back
      rl.restore_after_wait(saved);
    }
  };

public:
  void wait(std::unique_lock<LockType>& lock) {
    Guard g(lock);
    cv.wait(g.nl);
  }

  template <class Predicate>
  void wait(std::unique_lock<LockType>& lock, Predicate pred) {
    while (!pred()) wait(lock);
  }

  template <class Rep, class Period>
  std::cv_status wait_for(std::unique_lock<LockType>& lock,
                          const std::chrono::duration<Rep, Period>& rel_time) {
    Guard g(lock);
    return cv.wait_for(g.nl, rel_time);
  }

  template <class Rep, class Period, class Predicate>
  bool wait_for(std::unique_lock<LockType>& lock,
                const std::chrono::duration<Rep, Period>& rel_time,
                Predicate pred) {
    return wait_until(lock,
                      std::chrono::steady_clock::now() + rel_time,
                      std::move(pred));
  }

  template <class Clock, class Duration>
  std::cv_status wait_until(std::unique_lock<LockType>& lock,
                            const std::chrono::time_point<Clock, Duration>& abs_time) {
    Guard g(lock);
    return cv.wait_until(g.nl, abs_time);
  }

  template <class Clock, class Duration, class Predicate>
  bool wait_until(std::unique_lock<LockType>& lock,
                  const std::chrono::time_point<Clock, Duration>& abs_time,
                  Predicate pred) {
    while (!pred()) {
      if (wait_until(lock, abs_time) == std::cv_status::timeout) {
        return pred();
      }
    }
    return true;
  }

  void notify_one() noexcept { cv.notify_one(); }
  void notify_all() noexcept { cv.notify_all(); }
  // Wake waiters without holding the associated lock (pthread-safe; skips
  // lockdep's waiter_mutex check).  Use only when the signaller cannot take
  // the waiter lock without risking deadlock.
  void notify_all_sloppy() noexcept {
#ifdef CEPH_DEBUG_MUTEX
    cv.notify_all(true);
#else
    cv.notify_all();
#endif
  }
};

using reentrant_condition_variable = reentrant_condition_variable_impl<ReentrantLock>;

// finish() may call notify_all_sloppy() from another thread.  Wait until that
// broadcast completes before destroying a stack-allocated cond.
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
  ~C_ReentrantLock() override {
    delete fin;
  }
  void finish(int r) override {
    if (fin) {
      std::lock_guard l{*lock};
      fin->complete(r);
      fin = NULL;
    }
  }
};

template <typename LockType = ReentrantLock>
class C_ReentrantCond : public Context {
  reentrant_condition_variable_impl<LockType>& cond;   ///< Cond to signal
  std::atomic<bool> *done;   ///< true if finish() has been called
  std::atomic<bool> *wake_complete;  ///< true after notify_all_sloppy()
  int *rval;    ///< return value
public:
  C_ReentrantCond(reentrant_condition_variable_impl<LockType> &c,
                  std::atomic<bool> *d, int *r,
                  std::atomic<bool> *wc = nullptr)
    : cond(c), done(d), wake_complete(wc), rval(r) {
    done->store(false, std::memory_order_relaxed);
    if (wake_complete) {
      wake_complete->store(false, std::memory_order_relaxed);
    }
  }
  void finish(int r) override {
    *rval = r;
    // finish() often runs from another thread that does not hold the
    // waiter's lock (e.g. cap grant on ms_dispatch waking get_caps).
    done->store(true, std::memory_order_release);
    cond.notify_all_sloppy();
    if (wake_complete) {
      wake_complete->store(true, std::memory_order_release);
    }
  }
};

// Specialization of unique_unlock for ReentrantLockImpl.
//
// Mirrors reentrant_condition_variable::Guard: saves the full recursion depth
// before releasing the underlying mutex and restores it after reacquiring.
// This ensures that code which temporarily drops the lock (e.g. to call
// blocking I/O) re-emerges holding the lock with the same recursion count.
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

  /** Restore recursion depth saved by release(); partner guards keep _owns. */
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

  // Call when a partner guard already restored the lock (e.g. inode ordering).
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
