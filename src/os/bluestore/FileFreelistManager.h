// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_BLUESTORE_FILEFREELISTMANAGER_H
#define CEPH_OS_BLUESTORE_FILEFREELISTMANAGER_H

#include "FreelistManager.h"

#include <string>
#include <mutex>

#include "common/ceph_mutex.h"
#include "include/buffer.h"
#include "kv/KeyValueDB.h"

#include "Allocator.h"

class FileFreelistManager : public FreelistManager {
  CephContext* cct;
  //std::string meta_prefix, bitmap_prefix;
  //std::shared_ptr<KeyValueDB::MergeOperator> merge_op;
  //ceph::mutex lock = ceph::make_mutex("FileFreelistManager::lock");

  uint64_t size;            ///< size of device (bytes)
  uint64_t bytes_per_block; ///< bytes per block (bdev_block_size)
  uint64_t blocks_per_key;  ///< blocks (bits) per key/value pair
  uint64_t bytes_per_key;   ///< bytes per key/value pair
  uint64_t blocks;          ///< size of device (blocks, size rounded up)

  //  uint64_t block_mask;  ///< mask to convert byte offset to block offset
  //uint64_t key_mask;    ///< mask to convert offset to key offset

  //ceph::buffer::list all_set_bl;

  //KeyValueDB::Iterator enumerate_p;
  //uint64_t enumerate_offset; ///< logical offset; position
  //ceph::buffer::list enumerate_bl;   ///< current key at enumerate_offset
  //int enumerate_bl_pos;      ///< bit position in enumerate_bl

  int _read_cfg(
    std::function<int(const std::string&, std::string*)> cfg_reader);

  int _expand(uint64_t new_size, KeyValueDB* db);

  void _sync(KeyValueDB* kvdb, bool read_only);

  uint64_t size_2_block_count(uint64_t target_size) const;

public:
  FileFreelistManager(ObjectStore* store);

  static void setup_merge_operator(KeyValueDB *db, std::string prefix);

  int create(uint64_t size, uint64_t granularity,
	     uint64_t zone_size, uint64_t first_sequential_zone,
	     KeyValueDB::Transaction txn) override;

  int init(KeyValueDB *kvdb, bool db_in_read_only,
    std::function<int(const std::string&, std::string*)> cfg_reader) override;
  int init_alloc(bool read_only) override;
  void shutdown() override;
  void sync(KeyValueDB* kvdb) override;

  void dump(KeyValueDB *kvdb) override;

  void enumerate_reset() override;
  bool enumerate_next(KeyValueDB *kvdb, uint64_t *offset, uint64_t *length) override;
  bool enumerate(KeyValueDB *kvdb,
		 std::function<bool(uint64_t offset, uint64_t length)> next_extent) override;

  void allocate(
    uint64_t offset, uint64_t length,
    KeyValueDB::Transaction txn) override;
  void release(
    uint64_t offset, uint64_t length,
    KeyValueDB::Transaction txn) override;

  inline uint64_t get_size() const override {
    return size;
  }
  inline uint64_t get_alloc_units() const override {
    return size / bytes_per_block;
  }
  inline uint64_t get_alloc_size() const override {
    return bytes_per_block;
  }
  void get_meta(uint64_t target_size,
    std::vector<std::pair<std::string, std::string>>*) const override;
  bool is_null_manager() const override {
    return true;
  }
};

#endif
