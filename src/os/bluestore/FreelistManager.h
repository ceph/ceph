// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_BLUESTORE_FREELISTMANAGER_H
#define CEPH_OS_BLUESTORE_FREELISTMANAGER_H

#include <string>
#include <map>
#include <mutex>
#include <ostream>
#include "kv/KeyValueDB.h"

class FreelistManager {
public:
  FreelistManager() {}
  virtual ~FreelistManager() {}

  static FreelistManager *create(
    string type,
    KeyValueDB *db,
    string prefix);

  static void setup_merge_operators(KeyValueDB *db);

  virtual int create(uint64_t size, KeyValueDB::Transaction txn) = 0;

  virtual int init() = 0;
  virtual void shutdown() = 0;

  virtual void dump() = 0;

  virtual void enumerate_reset() = 0;
  virtual bool enumerate_next(uint64_t *offset, uint64_t *length) = 0;

  virtual void allocate(
    uint64_t offset, uint64_t length,
    KeyValueDB::Transaction txn) = 0;
  virtual void release(
    uint64_t offset, uint64_t length,
    KeyValueDB::Transaction txn) = 0;

  virtual bool supports_parallel_transactions() {
    return false;
  }
};


#endif
