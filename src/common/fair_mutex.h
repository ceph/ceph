// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-

#pragma once

#include "common/ceph_mutex.h"

#ifdef CEPH_DEBUG_MUTEX
#include <thread> // for std::this_thread::get_id()
#endif
#ifdef CEPH_LOCKSTAT
#include "lockstat.h"
#endif

#include <string>

namespace ceph {
/// a FIFO mutex
class fair_mutex
#ifdef CEPH_LOCKSTAT
  : public lockstat_detail::LockStat
#endif
{
public:
  fair_mutex(const std::string& name) :
#ifdef CEPH_LOCKSTAT
    lockstat_detail::LockStat(ceph::mutex::LockType, LOCKSTAT("fair_" + name)),
#endif
  mutex{ceph::make_mutex(name)}
  {}
  ~fair_mutex() = default;
  fair_mutex(const fair_mutex&) = delete;
  fair_mutex& operator=(const fair_mutex&) = delete;

  void lock()
  {
#ifdef CEPH_LOCKSTAT
    const auto wait_start_clock =
        unlikely(lockstat_detail::LockStat::is_lockstat_enabled())
            ? lockstat_detail::lockstat_clock::now()
            : lockstat_detail::lockstat_clock::zero();
#endif
    std::unique_lock lock(mutex);
    const unsigned my_id = next_id++;
    cond.wait(lock, [&] {
      return my_id == unblock_id;
    });
    _set_locked_by();
#ifdef CEPH_LOCKSTAT
    if (unlikely(wait_start_clock != lockstat_detail::lockstat_clock::zero())) {
      record_wait_time(
          lockstat_detail::lockstat_clock::now() - wait_start_clock,
          lockstat_detail::LockMode::WRITE);
    }
#endif
  }

  bool try_lock()
  {
#ifdef CEPH_LOCKSTAT
    const auto wait_start_clock =
        unlikely(lockstat_detail::LockStat::is_lockstat_enabled())
            ? lockstat_detail::lockstat_clock::now()
            : lockstat_detail::lockstat_clock::zero();
#endif
    std::lock_guard lock(mutex);
    if (is_locked()) {
      return false;
    }
    ++next_id;
#ifdef CEPH_LOCKSTAT
    if (unlikely(wait_start_clock != lockstat_detail::lockstat_clock::zero())) {
      record_wait_time(
          lockstat_detail::lockstat_clock::now() - wait_start_clock,
          lockstat_detail::LockMode::TRY_WRITE);
    }
#endif
    _set_locked_by();
    return true;
  }

  void unlock()
  {
    std::lock_guard lock(mutex);
    ++unblock_id;
    _reset_locked_by();
    cond.notify_all();
  }

  bool is_locked() const
  {
    return next_id != unblock_id;
  }

#ifdef CEPH_DEBUG_MUTEX
  bool is_locked_by_me() const {
    return is_locked() && locked_by == std::this_thread::get_id();
  }
private:
  void _set_locked_by() {
    locked_by = std::this_thread::get_id();
  }
  void _reset_locked_by() {
    locked_by = {};
  }
#else
  void _set_locked_by() {}
  void _reset_locked_by() {}
#endif

private:
  unsigned next_id = 0;
  unsigned unblock_id = 0;
  ceph::condition_variable cond;
  ceph::mutex mutex;
#ifdef CEPH_DEBUG_MUTEX
  std::thread::id locked_by = {};
#endif
};
} // namespace ceph
