// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

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
#include <utility>

#include "include/buffer.h"
#include "include/rados/librados.hpp"
#include "include/uuid.h"
#include "include/types.h"

#include <chrono>
#include "common/perf_counters.h"

class [[gnu::visibility("hidden")]] SimpleRADOSStriper
{
public:
  enum class LockLevel : int {
    None = 0,
    Shared = 1,
    Reserved = 2,
    Pending = 3,
    Exclusive = 4
  };

  using aiocompletionptr = std::unique_ptr<librados::AioCompletion>;
  using clock = std::chrono::steady_clock;
  using time = std::chrono::time_point<std::chrono::steady_clock>;

  static inline const uint64_t object_size = 22; /* power of 2 */
  static inline const uint64_t min_growth = (1<<27); /* 128 MB */
  static int config_logger(CephContext* cct, std::string_view name, std::shared_ptr<PerfCounters>* l);

  SimpleRADOSStriper() = default;
  SimpleRADOSStriper(librados::IoCtx _ioctx, std::string _oid)
    : ioctx(std::move(_ioctx))
    , oid(std::move(_oid))
  {
    cookie.generate_random();
    cookie_dstr = cookie.to_string().substr(0, 8);
    auto r = librados::Rados(ioctx);
    myaddrs = r.get_addrs();
  }
  SimpleRADOSStriper(const SimpleRADOSStriper&) = delete;
  SimpleRADOSStriper& operator=(const SimpleRADOSStriper&) = delete;
  SimpleRADOSStriper& operator=(SimpleRADOSStriper&&) = delete;
  SimpleRADOSStriper(SimpleRADOSStriper&&) = delete;
  ~SimpleRADOSStriper();

  void print(std::ostream&) const;

  int create();
  int open();
  int get_metadata();
  int remove();
  int stat(uint64_t* size);
  ssize_t write(const void* data, size_t len, uint64_t off);
  ssize_t read(void* data, size_t len, uint64_t off);
  int truncate(size_t size);
  int flush();
  int lock(LockLevel ilock);
  int unlock(LockLevel ilock);
  bool is_locked() const {
    return locked > LockLevel::None;
  }
  int check_reservation(bool* is_reserved);
  int print_lockers(std::ostream& out);
  void set_logger(std::shared_ptr<PerfCounters> l) {
    logger = std::move(l);
  }
  void set_lock_interval(std::chrono::milliseconds t) {
    lock_keeper_interval = t;
  }
  void set_acquire_timeout(std::chrono::milliseconds t) {
    acquire_timeout = t;
  }
  std::chrono::milliseconds get_acquire_timeout() const {
    return acquire_timeout;
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
  ceph::bufferlist state2bl(LockLevel l);
  int set_metadata(uint64_t new_size, bool update_size);
  int shrink_alloc(uint64_t a);
  int maybe_shrink_alloc();
  int wait_for_aios(bool block);
  int recover_lock();
  int _unlock(LockLevel ilock);
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
  static inline const char XATTR_STATE[] = "striper.state";
  static inline const char XATTR_WRITE_EPOCH[] = "striper.write_epoch";
  static inline const std::string primary = "striper.lock";
  static inline const std::string lockdesc = "SimpleRADOSStriper";

  void lock_keeper_main();

  librados::IoCtx ioctx;
  std::shared_ptr<PerfCounters> logger;
  std::string oid;
  std::thread lock_keeper;
  std::condition_variable lock_keeper_cvar;
  std::mutex lock_keeper_mutex;
  time last_renewal = time::min();
  std::chrono::milliseconds lock_keeper_interval{2000};
  std::chrono::milliseconds lock_keeper_timeout{30000};
  std::chrono::milliseconds acquire_timeout{0};
  std::atomic<bool> blocklisted = false;
  bool shutdown = false;
  version_t version = 0;
  std::string exclusive_holder;
  uint64_t write_epoch = 0;
  uint64_t size = 0;
  uint64_t allocated = 0;
  uuid_d cookie{};
  std::string cookie_dstr;
  LockLevel disk_state = LockLevel::None;

  uint64_t layout_stripe_unit = 0;
  uint64_t layout_stripe_count = 0;
  uint64_t layout_object_size = 0;

  /*
   * Tracks the current lock state as a logical escalation level:
   * None: No locks held.
   * Shared: A shared lock is held on the primary lock object. XATTR_STATE is "1".
   * Reserved: A shared lock is held on the primary lock object. XATTR_STATE is "2". XATTR_EXCL is set.
   * Pending: A shared lock is held on the primary lock object. XATTR_STATE is "3" (blocks new readers).
   * Exclusive: An exclusive lock is held on the primary lock object. XATTR_STATE is "4".
   * Ownership metadata is additionally written for dead-client recovery.
   */
  LockLevel locked = LockLevel::None;

  bool size_dirty = false;
  bool blocklist_the_dead = true;
  std::queue<aiocompletionptr> aios;
  int aios_failure = 0;
  std::string myaddrs;
};

#endif /* _SIMPLERADOSSTRIPER_H */
