// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_BLUESTORE_EXTENTFREELISTMANAGER_H
#define CEPH_OS_BLUESTORE_EXTENTFREELISTMANAGER_H

#include <string>
#include <map>
#include <mutex>
#include <ostream>
#include "FreelistManager.h"

#include "include/cpp-btree/btree_map.h"

class ExtentFreelistManager : public FreelistManager {
  std::string prefix;
  std::mutex lock;
  uint64_t total_free;

  typedef btree::btree_map<uint64_t,uint64_t> map_t;
  static const bool map_t_has_stable_iterators = false;

  map_t kv_free;    ///< mirrors our kv values in the db

  map_t::const_iterator enumerate_p;

  void _audit();
  void _dump();

public:
  ExtentFreelistManager() :
    total_free(0) {
  }

  int init(KeyValueDB *kvdb, std::string prefix) override;
  void shutdown() override;

  void dump() override;

  void enumerate_reset() override;
  bool enumerate_next(uint64_t *offset, uint64_t *length) override;

  void allocate(
    uint64_t offset, uint64_t length,
    KeyValueDB::Transaction txn) override;
  void release(
    uint64_t offset, uint64_t length,
    KeyValueDB::Transaction txn) override;
};


#endif
