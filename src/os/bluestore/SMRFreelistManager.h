// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_BLUESTORE_SMRFREELISTMANAGER_H
#define CEPH_OS_BLUESTORE_SMRFREELISTMANAGER_H

#include <string>
#include <map>
#include <mutex>
#include <ostream>
#include <vector>
#include "FreelistManager.h"
#include "SMRZone.h"

#include "include/cpp-btree/btree_map.h"

typedef class SMRZone SMRZone;

class SMRFreelistManager : public FreelistManager {
  KeyValueDB *kvdb;
  std::string meta_prefix;
  std::mutex m_lock;
  ceph::shared_ptr<KeyValueDB::MergeOperator> merge_op;
  std::mutex lock;

  // Same as BitmapFreelistManager

  uint64_t size;            ///< size of device (bytes)
  uint64_t bytes_per_block; ///< bytes per block (bdev_block_size)
  uint64_t blocks_per_key;  ///< blocks (bits) per key/value pair
  uint64_t blocks;          ///< size of device (blocks, size rounded up)

  uint64_t block_mask;  ///< mask to convert byte offset to block offset
  uint64_t key_mask;    ///< mask to convert offset to key offset

  bufferlist all_set_bl;

  // SMR specific values
  uint64_t m_total_free;
  uint64_t m_num_zones;
  uint64_t m_num_random;
  std::vector<SMRZone> m_zone_list;

  typedef btree::btree_map<uint64_t,uint64_t> map_t;
  static const bool map_t_has_stable_iterators = false;

  map_t kv_free;    ///< mirrors our kv values in the db

  map_t::const_iterator enumerate_p;

  void _audit();
  void _dump();

  void _init_misc();
//  uint64_t getZones(std::string dev_name);

public:
  SMRFreelistManager(KeyValueDB *kvdb, string prefix, string bdev);

  int create(uint64_t size, KeyValueDB::Transaction txn) override{};

  int init() override;
  void shutdown() override {};

  void dump() override {};

  void enumerate_reset() override{};
  bool enumerate_next(uint64_t *offset, uint64_t *length) override{};

  void allocate(
    uint64_t offset, uint64_t length,
    KeyValueDB::Transaction txn) override{};
  void release(
    uint64_t offset, uint64_t length,
    KeyValueDB::Transaction txn) override{};
};


#endif
