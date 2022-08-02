// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=2 sw=2 expandtab ft=cpp

/*
 * Abstract thread/process synchronization for the CORTX Motr backend
 *
 * Copyright (C) 2022 Seagate Technology LLC and/or its Affiliates
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#ifndef __MOTR_SYNCHROZATION__
#define __MOTR_SYNCHROZATION__

#include <unistd.h>

#include "include/types.h"
#include "include/utime.h"
#include "rgw_sal_motr.h"

// Define MOTR_EXCLUSIVE_RW_LOCK only when system needs exclusive read/write lock
#define MOTR_EXCLUSIVE_RW_LOCK 1

class MotrLockProvider;

struct motr_locker_info_t
{
  std::string cookie;        // unique id of caller
  utime_t expiration;         // expiration: non-zero means epoch of locker expiration
  std::string description;    // description: locker description, may be empty

  motr_locker_info_t() {}
  motr_locker_info_t(const utime_t& _e, std::string& _c, const std::string& _d)
                    : expiration(_e), cookie(_c), description(_d) {}
  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(1, 1, bl);
    encode(cookie, bl);
    encode(expiration, bl);
    encode(description, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    decode(cookie, bl);
    decode(expiration, bl);
    decode(description, bl);
    DECODE_FINISH(bl);
  }  
};
WRITE_CLASS_ENCODER(motr_locker_info_t)

struct motr_lock_info_t {
public:
  std::map<std::string, motr_locker_info_t> lockers;
  void encode(ceph::buffer::list &bl) const {
    ENCODE_START(1, 1, bl);
    encode(lockers, bl);
    ENCODE_FINISH(bl);
  }
  void decode(ceph::buffer::list::const_iterator &bl) {
    DECODE_START_LEGACY_COMPAT_LEN(1, 1, 1, bl);
    decode(lockers, bl);
    DECODE_FINISH(bl);
  }
};
WRITE_CLASS_ENCODER(motr_lock_info_t)

enum class MotrLockType {
  NONE                = 0,
  EXCLUSIVE           = 1,
  SHARED              = 2,
  EXCLUSIVE_EPHEMERAL = 3, /* lock object is removed @ unlock */
};

// Abstract interface for Motr named synchronization
class MotrSync {
public:
  virtual void initialize(std::shared_ptr<MotrLockProvider> lock_provider) = 0;
  virtual int lock(const std::string& lock_name, MotrLockType lock_type,
                   utime_t lock_duration, const std::string& locker_id) = 0;
  virtual int unlock(const std::string& lock_name, MotrLockType lock_type,
                     const std::string& locker_id) = 0;
};

// Abstract interface for entity that implements backend for lock objects
class MotrLockProvider {
public:
  virtual int read_lock(const std::string& lock_name,
                        motr_lock_info_t* lock_info) = 0;
  virtual int write_lock(const std::string& lock_name,
                         motr_lock_info_t* lock_info, bool update) = 0;
  virtual int remove_lock(const std::string& lock_name,
                          const std::string& locker_id) = 0;
};
#endif __MOTR_SYNCHROZATION__
