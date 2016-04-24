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

  static FreelistManager *create(string type);

  virtual int create(KeyValueDB::Transaction txn) {
    return 0;
  }

  virtual int init(KeyValueDB *kvdb, std::string prefix) = 0;
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
};


#endif
