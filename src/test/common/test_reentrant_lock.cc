// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <atomic>
#include <chrono>
#include <future>
#include <mutex>
#include <thread>

#include "common/reentrant_lock.h"
#include "gtest/gtest.h"

namespace {

ReentrantLock make_test_reentrant(const char *name)
{
  return ceph::make_reentrant(name);
}

TrackedLock make_test_tracked(const char *name)
{
  return ceph::make_tracked(name);
}

// Join helper threads even when a gtest ASSERT fails mid-test.
struct thread_joiner {
  std::thread& t;
  explicit thread_joiner(std::thread& thr) : t(thr) {}
  ~thread_joiner()
  {
    if (t.joinable()) {
      t.join();
    }
  }
  thread_joiner(const thread_joiner&) = delete;
  thread_joiner& operator=(const thread_joiner&) = delete;
};

// Avoid "destroying a locked mutex" when an ASSERT fails mid-test.
struct unlock_reentrant_on_exit {
  ReentrantLock& lock;
  explicit unlock_reentrant_on_exit(ReentrantLock& l) : lock(l) {}
  ~unlock_reentrant_on_exit()
  {
    while (lock.is_locked_by_me()) {
      lock.unlock();
    }
  }
};

struct unlock_tracked_on_exit {
  TrackedLock& lock;
  explicit unlock_tracked_on_exit(TrackedLock& l) : lock(l) {}
  ~unlock_tracked_on_exit()
  {
    while (lock.is_locked_by_me()) {
      lock.unlock();
    }
  }
};

struct unlock_mutex_on_exit {
  ceph::mutex& mtx;
  explicit unlock_mutex_on_exit(ceph::mutex& m) : mtx(m) {}
  ~unlock_mutex_on_exit()
  {
    if (ceph_mutex_is_locked_by_me(mtx)) {
      mtx.unlock();
    }
  }
};

bool wait_until(const std::atomic<bool>& flag,
                std::chrono::milliseconds timeout =
                  std::chrono::seconds(5))
{
  const auto deadline = std::chrono::steady_clock::now() + timeout;
  while (!flag.load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  return flag.load(std::memory_order_acquire);
}

template <typename Rep, typename Period>
bool wait_for_future(std::future<bool>& fut,
                     const std::chrono::duration<Rep, Period>& timeout)
{
  return fut.wait_for(timeout) == std::future_status::ready && fut.get();
}

} // namespace

TEST(TrackedLock, BasicLockUnlock)
{
  auto lock = make_test_tracked("tracked_basic");
  unlock_tracked_on_exit cleanup(lock);
  ASSERT_FALSE(lock.is_locked());
  ASSERT_FALSE(lock.is_locked_by_me());

  lock.lock();
  ASSERT_TRUE(lock.is_locked());
  ASSERT_TRUE(lock.is_locked_by_me());
  ASSERT_TRUE(static_cast<bool>(lock));

  lock.unlock();
  ASSERT_FALSE(lock.is_locked_by_me());
}

TEST(TrackedLock, NotReentrant)
{
  auto lock = make_test_tracked("tracked_non_reentrant");
  unlock_tracked_on_exit cleanup(lock);
  lock.lock();
  ASSERT_FALSE(lock.try_lock());
  lock.unlock();
}

TEST(TrackedLock, UniqueUnlock)
{
  auto lock = make_test_tracked("tracked_unique_unlock");
  unlock_tracked_on_exit cleanup(lock);
  lock.lock();
  {
    ceph::unique_unlock u(lock);
    ASSERT_TRUE(u.released());
    ASSERT_FALSE(lock.is_locked_by_me());
  }
  ASSERT_TRUE(lock.is_locked_by_me());
  lock.unlock();
}

TEST(ReentrantLock, BasicLockUnlock)
{
  auto lock = make_test_reentrant("basic");
  unlock_reentrant_on_exit cleanup(lock);
  ASSERT_FALSE(lock.is_locked());
  ASSERT_FALSE(lock.is_locked_by_me());

  lock.lock();
  ASSERT_TRUE(lock.is_locked());
  ASSERT_TRUE(lock.is_locked_by_me());
  ASSERT_TRUE(static_cast<bool>(lock));

  lock.unlock();
  ASSERT_FALSE(lock.is_locked_by_me());
}

TEST(ReentrantLock, RecursiveDepth)
{
  auto lock = make_test_reentrant("recursive");
  unlock_reentrant_on_exit cleanup(lock);
  lock.lock();
  lock.lock();
  lock.lock();
  ASSERT_TRUE(lock.is_locked_by_me());

  auto try_from_other = std::async(std::launch::async, [&lock]() {
    return lock.try_lock();
  });
  ASSERT_FALSE(try_from_other.get());

  lock.unlock();
  ASSERT_TRUE(lock.is_locked_by_me());
  lock.unlock();
  ASSERT_TRUE(lock.is_locked_by_me());
  lock.unlock();
  ASSERT_FALSE(lock.is_locked_by_me());

  auto try_after = std::async(std::launch::async, [&lock]() {
    if (!lock.try_lock()) {
      return false;
    }
    lock.unlock();
    return true;
  });
  ASSERT_TRUE(try_after.get());
}

TEST(ReentrantLock, TryLockRecursive)
{
  auto lock = make_test_reentrant("try_recursive");
  unlock_reentrant_on_exit cleanup(lock);
  ASSERT_TRUE(lock.try_lock());
  ASSERT_TRUE(lock.try_lock());
  lock.unlock();
  ASSERT_TRUE(lock.is_locked_by_me());
  lock.unlock();
  ASSERT_FALSE(lock.is_locked_by_me());
}

TEST(ReentrantLock, ReleaseForWaitRestore)
{
  auto lock = make_test_reentrant("release_restore");
  unlock_reentrant_on_exit cleanup(lock);
  lock.lock();
  lock.lock();

  const int saved = lock.release_for_wait();
  ASSERT_EQ(saved, 2);
  ASSERT_FALSE(lock.is_locked_by_me());
  lock.native_mutex().unlock();

  auto acquired = std::async(std::launch::async, [&lock]() {
    if (!lock.try_lock()) {
      return false;
    }
    lock.unlock();
    return true;
  });
  ASSERT_TRUE(acquired.get());

  lock.native_mutex().lock();
  lock.restore_after_wait(saved);
  ASSERT_TRUE(lock.is_locked_by_me());
  lock.unlock();
  lock.unlock();
  ASSERT_FALSE(lock.is_locked_by_me());
}

TEST(ReentrantLock, StdUniqueLockOneLevel)
{
  auto lock = make_test_reentrant("std_unique");
  unlock_reentrant_on_exit cleanup(lock);
  lock.lock();
  lock.lock();

  {
    std::unique_lock guard(lock);
    ASSERT_TRUE(guard.owns_lock());
    ASSERT_TRUE(lock.is_locked_by_me());
    guard.unlock();
    ASSERT_FALSE(guard.owns_lock());
    ASSERT_TRUE(lock.is_locked_by_me());
    guard.lock();
    ASSERT_TRUE(guard.owns_lock());
  }

  ASSERT_TRUE(lock.is_locked_by_me());
  lock.unlock();
  lock.unlock();
}

TEST(ReentrantLock, StdUniqueLockDeferAndAdopt)
{
  auto lock = make_test_reentrant("defer_adopt");
  unlock_reentrant_on_exit cleanup(lock);
  lock.lock();

  std::unique_lock defer(lock, std::defer_lock);
  ASSERT_FALSE(defer.owns_lock());
  defer.lock();
  ASSERT_TRUE(defer.owns_lock());
  defer.unlock();

  {
    std::unique_lock adopt(lock, std::adopt_lock);
    ASSERT_TRUE(adopt.owns_lock());
  }

  ASSERT_FALSE(lock.is_locked_by_me());
}

TEST(ReentrantUniqueUnlock, DropsFullRecursionDepth)
{
  auto lock = make_test_reentrant("full_drop");
  unlock_reentrant_on_exit cleanup(lock);
  lock.lock();
  lock.lock();
  lock.lock();

  std::promise<bool> other_done;
  auto other_result = other_done.get_future();

  std::thread other([&]() {
    bool acquired = false;
    const auto deadline = std::chrono::steady_clock::now() +
      std::chrono::seconds(5);
    while (!acquired && std::chrono::steady_clock::now() < deadline) {
      acquired = lock.try_lock();
      if (!acquired) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }
    if (acquired) {
      lock.unlock();
    }
    other_done.set_value(acquired);
  });
  thread_joiner join_other(other);

  {
    ceph::unique_unlock drop(lock);
    ASSERT_TRUE(drop.released());
    ASSERT_FALSE(lock.is_locked_by_me());
    // Wait until the helper has acquired and released before drop reacquires.
    ASSERT_TRUE(wait_for_future(other_result, std::chrono::seconds(5)));
  }

  ASSERT_TRUE(lock.is_locked_by_me());
  lock.unlock();
  lock.unlock();
  lock.unlock();
  ASSERT_FALSE(lock.is_locked_by_me());
}

TEST(ReentrantUniqueUnlock, DeferLockManualRelease)
{
  auto lock = make_test_reentrant("defer_release");
  unlock_reentrant_on_exit cleanup(lock);
  lock.lock();
  lock.lock();

  unique_unlock drop(lock, std::defer_lock);
  ASSERT_FALSE(drop.released());
  drop.release();
  ASSERT_TRUE(drop.released());
  ASSERT_FALSE(lock.is_locked_by_me());

  auto acquired = std::async(std::launch::async, [&lock]() {
    if (!lock.try_lock()) {
      return false;
    }
    lock.unlock();
    return true;
  });
  ASSERT_TRUE(acquired.get());

  drop.reacquire();
  ASSERT_FALSE(drop.released());
  ASSERT_TRUE(lock.is_locked_by_me());

  lock.unlock();
  lock.unlock();
}

TEST(ReentrantUniqueUnlock, AbandonAfterPartnerRestore)
{
  auto lock = make_test_reentrant("abandon");
  unlock_reentrant_on_exit cleanup(lock);
  lock.lock();
  lock.lock();

  unique_unlock drop(lock);
  lock.lock();
  drop._abandon();

  lock.unlock();
  ASSERT_FALSE(lock.is_locked_by_me());
}

TEST(MutexUniqueUnlock, ReleasesAndRestoresPlainMutex)
{
  ceph::mutex m = ceph::make_mutex("plain");
  unlock_mutex_on_exit cleanup(m);
  m.lock();

  std::promise<bool> other_done;
  auto other_result = other_done.get_future();

  std::thread other([&]() {
    bool acquired = false;
    const auto deadline = std::chrono::steady_clock::now() +
      std::chrono::seconds(5);
    while (!acquired && std::chrono::steady_clock::now() < deadline) {
      acquired = m.try_lock();
      if (!acquired) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }
    if (acquired) {
      m.unlock();
    }
    other_done.set_value(acquired);
  });
  thread_joiner join_other(other);

  {
    unique_unlock drop(m);
    ASSERT_TRUE(drop.released());
    ASSERT_TRUE(wait_for_future(other_result, std::chrono::seconds(5)));
  }

  ASSERT_TRUE(ceph_mutex_is_locked_by_me(m));
  m.unlock();
}

TEST(MutexUniqueUnlock, DeferLock)
{
  ceph::mutex m = ceph::make_mutex("plain_defer");
  unlock_mutex_on_exit cleanup(m);
  m.lock();

  {
    unique_unlock drop(m, std::defer_lock);
    ASSERT_FALSE(drop.released());
    drop.release();
    ASSERT_FALSE(ceph_mutex_is_locked_by_me(m));
  }

  ASSERT_TRUE(ceph_mutex_is_locked_by_me(m));
  m.unlock();
}

TEST(ReentrantCondVar, WaitPreservesRecursionDepth)
{
  ceph::reentrant_condition_variable cv;
  auto lock = make_test_reentrant("cond");
  unlock_reentrant_on_exit cleanup(lock);

  std::atomic<bool> ready{false};
  std::atomic<bool> waiter_done{false};
  std::thread waiter([&]() {
    lock.lock();
    std::unique_lock wl(lock);
    cv.wait(wl, [&]() { return ready.load(std::memory_order_acquire); });
    ASSERT_TRUE(lock.is_locked_by_me());
    wl.unlock();
    lock.unlock();
    waiter_done.store(true, std::memory_order_release);
  });
  thread_joiner join_waiter(waiter);

  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  lock.lock();
  ready.store(true, std::memory_order_release);
  cv.notify_all();
  lock.unlock();
  ASSERT_TRUE(wait_until(waiter_done));
  ASSERT_TRUE(waiter_done.load(std::memory_order_acquire));
}