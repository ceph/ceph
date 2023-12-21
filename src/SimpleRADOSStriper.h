// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License version 2.1, as published by
 * the Free Software Foundation.  See file COPYING.
 *
 */

#ifndef _SIMPLERADOSSTRIPER_H
#define _SIMPLERADOSSTRIPER_H

#include <queue>
#include <string_view>
#include <thread>

#include "include/buffer.h"
#include "include/rados/librados.hpp"
#include "include/uuid.h"
#include "include/types.h"

#include "common/ceph_time.h"
#include "common/perf_counters.h"

class [[gnu::visibility("default")]] SimpleRADOSStriper
{
public:
  using aiocompletionptr = std::unique_ptr<librados::AioCompletion>;
  using clock = ceph::coarse_mono_clock;
  using time = ceph::coarse_mono_time;

  static inline const uint64_t object_size = 22; /* power of 2 */
  static inline const uint64_t min_growth = (1<<27); /* 128 MB */
  static int config_logger(CephContext* cct, std::string_view name, std::shared_ptr<PerfCounters>* l);

  SimpleRADOSStriper() = default;
  SimpleRADOSStriper(librados::IoCtx _ioctx, std::string _oid)
    : ioctx(std::move(_ioctx))
    , oid(std::move(_oid))
  {
    cookie.generate_random();
    auto r = librados::Rados(ioctx);
    myaddrs = r.get_addrs();
  }
  SimpleRADOSStriper(const SimpleRADOSStriper&) = delete;
  SimpleRADOSStriper& operator=(const SimpleRADOSStriper&) = delete;
  SimpleRADOSStriper& operator=(SimpleRADOSStriper&&) = delete;
  SimpleRADOSStriper(SimpleRADOSStriper&&) = delete;
  ~SimpleRADOSStriper();

  int create();
  int open();
  int remove();
  int stat(uint64_t* size);
  ssize_t write(const void* data, size_t len, uint64_t off);
  ssize_t read(void* data, size_t len, uint64_t off);
  int truncate(size_t size);
  int flush();
  int lock(uint64_t timeoutms);
  int unlock();
  int is_locked() const {
    return locked;
  }
  int print_lockers(std::ostream& out);
  void set_logger(std::shared_ptr<PerfCounters> l) {
    logger = std::move(l);
  }
  void set_lock_interval(std::chrono::milliseconds t) {
    lock_keeper_interval = t;
  }
  void set_lock_timeout(std::chrono::milliseconds t) {
    lock_keeper_timeout = t;
  }
  void set_blocklist_the_dead(bool b) {
    blocklist_the_dead = b;
  }

protected:
  struct extent {
    std::string soid;
    size_t len;
    size_t off;
  };

  ceph::bufferlist str2bl(std::string_view sv);
  ceph::bufferlist uint2bl(uint64_t v);
  int set_metadata(uint64_t new_size, bool update_size);
  int shrink_alloc(uint64_t a);
  int maybe_shrink_alloc();
  int wait_for_aios(bool block);
  int recover_lock();
  extent get_next_extent(uint64_t off, size_t len) const;
  extent get_first_extent() const {
    return get_next_extent(0, 0);
  }

private:
  static inline const char XATTR_EXCL[] = "striper.excl";
  static inline const char XATTR_SIZE[] = "striper.size";
  static inline const char XATTR_ALLOCATED[] = "striper.allocated";
  static inline const char XATTR_VERSION[] = "striper.version";
  static inline const char XATTR_LAYOUT_STRIPE_UNIT[] = "striper.layout.stripe_unit";
  static inline const char XATTR_LAYOUT_STRIPE_COUNT[] = "striper.layout.stripe_count";
  static inline const char XATTR_LAYOUT_OBJECT_SIZE[] = "striper.layout.object_size";
  static inline const std::string biglock = "striper.lock";
  static inline const std::string lockdesc = "SimpleRADOSStriper";

  void lock_keeper_main();

  librados::IoCtx ioctx;
  std::shared_ptr<PerfCounters> logger;
  std::string oid;
  std::thread lock_keeper;
  std::condition_variable lock_keeper_cvar;
  std::mutex lock_keeper_mutex;
  time last_renewal = clock::zero();
  std::chrono::milliseconds lock_keeper_interval{2000};
  std::chrono::milliseconds lock_keeper_timeout{30000};
  std::atomic<bool> blocklisted = false;
  bool shutdown = false;
  version_t version = 0;
  std::string exclusive_holder;
  uint64_t size = 0;
  uint64_t allocated = 0;
  uuid_d cookie{};
  bool locked = false;
  bool size_dirty = false;
  bool blocklist_the_dead = true;
  std::queue<aiocompletionptr> aios;
  int aios_failure = 0;
  std::string myaddrs;
};

#endif /* _SIMPLERADOSSTRIPER_H */
