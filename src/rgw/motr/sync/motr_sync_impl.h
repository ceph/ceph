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

#ifndef __MOTR_LOCK__
#define __MOTR_LOCK__

/*
Usage:
 Approach 1.
  1. Instantiate one of the LockProvider Implementation.
      example:
      std::unique_ptr<MotrKVLockProvider> lock_provider;

  2. Initialize the lock provider with global lock index table
      example:
      lock_provider.initialize(pp, store, "motr.gc.index.locks");

      The caller can do this during the process/thread 
      start-up/initialization; if "initialize()" returns < 0, the
      caller is expected to stop further initialization if locking is esential
      in the system. In multi-process environment, each process can call
      "MotrKVLockProvider::initialize" during it's start-up, providing the same
      name for the global lock index.

  3. Instantiate one of the Locker Implmentations.
      example:
      std::unique_ptr<MotrLock> motr_lock;
  
  4. Initialize the Locker class with LockProvider Object.
      example:
      motr_lock.initialize(lock_provider);
  
  5. Lock the required resource using Locker object
      example:
      motr_lock.lock(resource_name, lock_type,
                     lock_duration, locker_id);

      Any -ve value returned by MotrLock::lock() indicates resource 
      can't be locked; a value 0 indicates success.

  6. Unlock the resource using the same Locker Object.
      exmaple:
      motr_lock.unlock(resource_name, lock_type, locker_id);
  
 Approach 2.
  1. Follow step (1) & step (2) in Approach 1.
  2. Call get_lock_instance(lock_provider), passing it lock provider instance
     created in step (1) above.
  3. Call get_lock_instance() wherever lock is needed.
    
Note: Presently, locking framework supports EXCLUSIVE lock; it means
caller needs to set MotrLockType::EXCLUSIVE while calling  MotrLock::lock(),
and do not pass locker_id.
*/

#include "rgw_sal_motr.h"
#include "motr/sync/motr_sync.h"

class MotrLock : public MotrSync {
private:
  std::unique_ptr<MotrLockProvider> _lock_provider;

public:
  virtual void initialize(
      std::unique_ptr<MotrLockProvider>& lock_provider) override;
  virtual int lock(const std::string& lock_name, MotrLockType lock_type,
                   utime_t lock_duration, const std::string& locker_id) override;
  virtual int unlock(const std::string& lock_name, MotrLockType lock_type,
                     const std::string& locker_id) override;
  virtual int check_lock(const std::string& lock_name, 
                         const std::string& locker_id) override;
};

class MotrKVLockProvider : public MotrLockProvider {
private:
  const DoutPrefixProvider* _dpp;
  rgw::sal::MotrStore* _store;
  std::string _lock_index;

public:
  virtual int initialize(const DoutPrefixProvider* dpp, 
                         rgw::sal::MotrStore* _s,
                         const std::string& lock_index_name) override;
  virtual int read_lock(const std::string& lock_name,
                        motr_lock_info_t* lock_info) override;
  virtual int write_lock(const std::string& lock_name,
                         motr_lock_info_t* lock_info,
                         bool update = false) override;
  virtual int remove_lock(const std::string& lock_name,
                          const std::string& locker_id) override;
};
extern std::unique_ptr<MotrLockProvider> g_lock_provider;
std::shared_ptr<MotrSync>& get_lock_instance(
    std::unique_ptr<MotrLockProvider>& lock_provider = g_lock_provider);
std::string random_string(size_t length);

#endif
