// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_BLUESTORE_FREELISTMANAGER_H
#define CEPH_OS_BLUESTORE_FREELISTMANAGER_H

#include <string>
#include <vector>
#include <mutex>
#include <ostream>
#include "kv/KeyValueDB.h"
#include "bluestore_types.h"
#include "zoned_types.h"

class FreelistManager {
public:
  CephContext* cct;
  FreelistManager(CephContext* cct) : cct(cct) {}
  virtual ~FreelistManager() {}

  static FreelistManager *create(
    CephContext* cct,
    std::string type,
    std::string prefix);

  static void setup_merge_operators(KeyValueDB *db, const std::string &type);

  virtual int create(uint64_t size, uint64_t granularity,
		     KeyValueDB::Transaction txn) = 0;

  virtual int init(KeyValueDB *kvdb, bool db_in_read_only,
    std::function<int(const std::string&, std::string*)> cfg_reader) = 0;
  virtual void sync(KeyValueDB* kvdb) = 0;
  virtual void shutdown() = 0;

  virtual void dump(KeyValueDB *kvdb) = 0;

  virtual void enumerate_reset() = 0;
  virtual bool enumerate_next(KeyValueDB *kvdb, uint64_t *offset, uint64_t *length) = 0;

  virtual void allocate(
    uint64_t offset, uint64_t length,
    KeyValueDB::Transaction txn) = 0;
  virtual void release(
    uint64_t offset, uint64_t length,
    KeyValueDB::Transaction txn) = 0;

  virtual uint64_t get_size() const = 0;
  virtual uint64_t get_alloc_units() const = 0;
  virtual uint64_t get_alloc_size() const = 0;

  virtual void get_meta(uint64_t target_size,
    std::vector<std::pair<string, string>>*) const = 0;

  virtual std::vector<zone_state_t> get_zone_states(KeyValueDB *kvdb) const {
    return {};
  }
};


#endif
