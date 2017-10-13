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
  CephContext* cct;
  FreelistManager(CephContext* cct) : cct(cct) {}
  virtual ~FreelistManager() {}

  static FreelistManager *create(
    CephContext* cct,
    string type,
    KeyValueDB *db,
    string prefix);

  static void setup_merge_operators(KeyValueDB *db);

  virtual int create(uint64_t size, uint64_t min_alloc_size,
		     KeyValueDB::Transaction txn) = 0;

  virtual int init(uint64_t dev_size) = 0;
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
