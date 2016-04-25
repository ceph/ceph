// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_BLUESTORE_FREELISTMANAGER_H
#define CEPH_OS_BLUESTORE_FREELISTMANAGER_H

#include <string>
#include <map>
#include <mutex>
#include <ostream>
#include "kv/KeyValueDB.h"

#include "include/cpp-btree/btree_map.h"

class FreelistManager {
  std::string prefix;
  std::mutex lock;
  uint64_t total_free;

  typedef btree::btree_map<uint64_t,uint64_t> map_t;
  static const bool map_t_has_stable_iterators = false;

  map_t kv_free;    ///< mirrors our kv values in the db

  void _audit();
  void _dump();

public:
  FreelistManager() :
    total_free(0) {
  }

  int init(KeyValueDB *kvdb, std::string prefix);
  void shutdown();

  void dump();

  uint64_t get_total_free() {
    std::lock_guard<std::mutex> l(lock);
    return total_free;
  }

  const map_t& get_freelist() {
    return kv_free;
  }

  void allocate(
    uint64_t offset, uint64_t length,
    KeyValueDB::Transaction txn);
  void release(
    uint64_t offset, uint64_t length,
    KeyValueDB::Transaction txn);
};


#endif
