// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=2 sw=2 expandtab ft=cpp

/*
 * GC thread/process synchronization for the CORTX Motr backend
 *
 * Copyright (C) 2022 Seagate Technology LLC and/or its Affiliates
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "motr/sync/motr_sync_impl.h"

std::string random_string(size_t length)
{
    auto randchar = []() -> char
    {
        const char charset[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
        const size_t max_index = (sizeof(charset) - 1);
        return charset[ rand() % max_index ];
    };
    std::string str(length,0);
    std::generate_n( str.begin(), length, randchar );
    return str;
}

void MotrLock::initialize(std::unique_ptr<MotrLockProvider>& lock_provider) {
  _lock_provider = std::move(lock_provider);
}

int MotrLock::lock(const std::string& lock_name,
                   MotrLockType lock_type,
                   utime_t lock_duration,
                   const std::string& locker_id = "") {
  if (!_lock_provider || locker_id.empty()) {
    return -EINVAL;
  }
  int rc = 0;
  motr_lock_info_t lock_obj;
  // First, try to write lock object
  struct motr_locker_info_t locker;
  locker.cookie = locker_id;
  locker.expiration = ceph_clock_now() + lock_duration;
  locker.description = "";
  // Insert lock entry
  lock_obj.lockers.insert(
      std::pair<std::string, struct motr_locker_info_t>(locker_id,
                                                        locker));
  rc = _lock_provider->write_lock(lock_name, &lock_obj, false);
  if (rc == 0) {
    // lock entry created successfully
    return rc;
  } else if (rc == -EEXIST) {
    // Failed to acquire lock object; possibly, already acquired by someone
    // Lock entry is present. Check if this is a stale/expired lock
    rc = _lock_provider->read_lock(lock_name, &lock_obj);
    if (rc == 0) {
      utime_t now = ceph_clock_now();
      auto iter = lock_obj.lockers.begin();
      while (iter != lock_obj.lockers.end()) {
        struct motr_locker_info_t &info = iter->second;
        if (!info.expiration.is_zero() && info.expiration < now) {
          // locker has expired; delete it
          iter = lock_obj.lockers.erase(iter);
        } else {
          ++iter;
        }
      }
      // remove the lock if no locker is left
      if (lock_obj.lockers.empty())
        _lock_provider->remove_lock(lock_name, locker_id);
    }
  }
  return -EBUSY;
}

int MotrLock::unlock(const std::string& lock_name,
                     MotrLockType lock_type, const std::string& locker_id) {
  return _lock_provider->remove_lock(lock_name, locker_id);
}

int MotrKVLockProvider::initialize(const DoutPrefixProvider* dpp,
                                   rgw::sal::MotrStore* _s,
                                   const std::string& lock_index_name) {
  int rc = 0;
  _dpp = dpp;
  _store = _s;
  _lock_index = lock_index_name;
  if (!_store || lock_index_name.empty()) {
    return -EINVAL;
  }
  rc = _store->create_motr_idx_by_name(lock_index_name);
  if (rc == -EEXIST) rc = 0;
  return rc;  
}

int MotrLock::check_lock(const std::string& lock_name,
                         const std::string& locker_id = "") {
  if (!_lock_provider || locker_id.empty()) {
    return -EINVAL;
  }
  motr_lock_info_t cnfm_lock_obj;
  int rc = _lock_provider->read_lock(lock_name, &cnfm_lock_obj);
  if (rc == 0) {
    auto iter = cnfm_lock_obj.lockers.begin();
    while (iter != cnfm_lock_obj.lockers.end()) {
      struct motr_locker_info_t &info = iter->second;
      if (info.cookie == locker_id) {
        // Same lock exists; this confirms lock object
        return rc;
      }
    }
  } else if (rc == -ENOENT) {
    // Looks like lock object is deleted by another caller
    // as part of the race condition 
    return -EBUSY;
  }
  return -EBUSY;
}

int MotrKVLockProvider::read_lock(const std::string& lock_name,
                                  motr_lock_info_t* lock_info) {
  if (!_store || _lock_index.empty() || !lock_info) {
    return -EINVAL;
  }
  int rc = 0;
  bufferlist bl;
  rc = _store->do_idx_op_by_name(_lock_index, M0_IC_GET, lock_name, bl);
  if (rc != 0) {
    return rc;
  }
  bufferlist::const_iterator bitr = bl.begin();
  lock_info->decode(bitr);
  return 0;
}

int MotrKVLockProvider::write_lock(const std::string& lock_name,
                                   motr_lock_info_t* lock_info, bool update) {
  if (!_store || _lock_index.empty() || !lock_info) {
    return -EINVAL;
  }
  bufferlist bl;
  lock_info->encode(bl);
  int rc =
      _store->do_idx_op_by_name(_lock_index, M0_IC_PUT, lock_name, bl, update);
  return rc;
}

int MotrKVLockProvider::remove_lock(const std::string& lock_name,
                                    const std::string& locker_id = "") {
  if (!_store || _lock_index.empty() || lock_name.empty()) {
    return -EINVAL;
  }
  motr_lock_info_t lock_info;
  bufferlist bl;
  int rc = 0;
  bool del_lock_entry = false;
  rc = read_lock(lock_name, &lock_info);
  if (rc != 0)
    return (rc == -ENOENT) ? 0 : rc;

  if (locker_id.empty()) {
    // this is exclusive lock
    del_lock_entry = true;
  } else {
    auto it = lock_info.lockers.find(locker_id);
    if (it != lock_info.lockers.end()) {
      lock_info.lockers.erase(it);
    }
    if (lock_info.lockers.empty()) {
      del_lock_entry = true;
    }
  }
  if (del_lock_entry) {
    // all lockers removed; now delete lock object
    rc = _store->do_idx_op_by_name(_lock_index, M0_IC_DEL, lock_name, bl);
  } else {
    // update the lock entry
    lock_info.encode(bl);
    rc = _store->do_idx_op_by_name(_lock_index, M0_IC_PUT, lock_name, bl);
  }
  return rc;
}

// Create a global instance of MotrLock that can be used by caller
// This needs to be called by a single main thread of the caller.
std::unique_ptr<MotrLockProvider> g_lock_provider = nullptr;
std::shared_ptr<MotrSync> g_motr_lock;
std::shared_ptr<MotrSync>& get_lock_instance(
    std::unique_ptr<MotrLockProvider>& lock_provider) {
  static bool initialize = false;
  std::mutex lock;
  std::lock_guard l(lock);
  if (!initialize && lock_provider) {
    g_motr_lock = std::make_shared<MotrLock>();
    g_motr_lock->initialize(lock_provider);
    initialize = true;
    return g_motr_lock;
  }
  return g_motr_lock;
}
