// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_OS_BLUESTORE_BITMAPFREELISTMANAGER_H
#define CEPH_OS_BLUESTORE_BITMAPFREELISTMANAGER_H

#include "FreelistManager.h"

#include <string>
#include <mutex>

#include "include/buffer.h"
#include "kv/KeyValueDB.h"

class BitmapFreelistManager : public FreelistManager {
  std::string meta_prefix, bitmap_prefix;
  KeyValueDB *kvdb;
  ceph::shared_ptr<KeyValueDB::MergeOperator> merge_op;
  std::mutex lock;

  uint64_t size;            ///< size of device (bytes)
  uint64_t bytes_per_block; ///< bytes per block (bdev_block_size)
  uint64_t blocks_per_key;  ///< blocks (bits) per key/value pair
  uint64_t bytes_per_key;   ///< bytes per key/value pair
  uint64_t blocks;          ///< size of device (blocks, size rounded up)

  uint64_t block_mask;  ///< mask to convert byte offset to block offset
  uint64_t key_mask;    ///< mask to convert offset to key offset

  bufferlist all_set_bl;

  KeyValueDB::Iterator enumerate_p;
  uint64_t enumerate_offset; ///< logical offset; position
  bufferlist enumerate_bl;   ///< current key at enumerate_offset
  int enumerate_bl_pos;      ///< bit position in enumerate_bl

  uint64_t get_offset(uint64_t key_off, int bit) {
    return key_off + bit * bytes_per_block;
  }

  void _init_misc();

  void _verify_range(uint64_t offset, uint64_t length, int val);
  void _xor(
    uint64_t offset, uint64_t length,
    KeyValueDB::Transaction txn);

public:
  BitmapFreelistManager(KeyValueDB *db, string meta_prefix,
			string bitmap_prefix);

  static void setup_merge_operator(KeyValueDB *db, string prefix);

  int create(uint64_t size, KeyValueDB::Transaction txn) override;

  int init() override;
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

  bool supports_parallel_transactions() override {
    return true;
  }
};

#endif
