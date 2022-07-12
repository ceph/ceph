// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <pthread.h>
#include <atomic>

#include "common/mutex_debug.h"

namespace ceph {

class shared_mutex_debug :
    public ceph::mutex_debug_detail::mutex_debugging_base
{
  pthread_rwlock_t rwlock;
  const bool track;
  std::atomic<unsigned> nrlock{0};

public:
  shared_mutex_debug(std::string group,
		     bool track_lock=true,
		     bool enable_lock_dep=true,
		     bool prioritize_write=false);
  // exclusive locking
  void lock();
  bool try_lock();
  void unlock();
  bool is_wlocked() const {
    return nlock > 0;
  }
  // shared locking
  void lock_shared();
  bool try_lock_shared();
  void unlock_shared();
  bool is_rlocked() const {
    return nrlock > 0;
  }
  // either of them
  bool is_locked() const {
    return nlock > 0 || nrlock > 0;
  }
private:
  // exclusive locking
  void _pre_unlock();
  void _post_lock();
  // shared locking
  void _pre_unlock_shared();
  void _post_lock_shared();
};

} // namespace ceph
