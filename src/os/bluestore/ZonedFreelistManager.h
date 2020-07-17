// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

//
// A freelist manager for zoned devices.  This iteration just keeps the write
// pointer per zone.  Following iterations will add enough information to enable
// cleaning of zones.
//
// Copyright (C) 2020 Abutalib Aghayev
//

#ifndef CEPH_OS_BLUESTORE_ZONEDFREELISTMANAGER_H
#define CEPH_OS_BLUESTORE_ZONEDFREELISTMANAGER_H

#include "FreelistManager.h"

#include <string>
#include <mutex>

#include "common/ceph_mutex.h"
#include "include/buffer.h"
#include "kv/KeyValueDB.h"

using cfg_reader_t = std::function<int(const std::string&, std::string*)>;

class ZonedFreelistManager : public FreelistManager {
  std::string meta_prefix;    ///< device size, zone size, etc.
  std::string info_prefix;    ///< per zone write pointer, dead bytes
  mutable ceph::mutex lock = ceph::make_mutex("ZonedFreelistManager::lock");

  uint64_t size;	      ///< size of sequential region (bytes)
  uint64_t bytes_per_block;   ///< bytes per allocation unit (bytes)
  uint64_t zone_size;	      ///< size of a single zone (bytes)
  uint64_t num_zones;	      ///< number of sequential zones
  uint64_t starting_zone_num; ///< the first sequential zone number

  KeyValueDB::Iterator enumerate_p;
  uint64_t enumerate_zone_num;

  void write_zone_state_to_db(uint64_t zone_num,
			      const zone_state_t &zone_state,
			      KeyValueDB::Transaction txn);
  void load_zone_state_from_db(uint64_t zone_num,
			       zone_state_t &zone_state,
			       KeyValueDB::Iterator &it) const;

  void init_zone_states(KeyValueDB::Transaction txn);

  void increment_write_pointer(
      uint64_t zone, uint64_t length, KeyValueDB::Transaction txn);
  void increment_num_dead_bytes(
      uint64_t zone, uint64_t num_bytes, KeyValueDB::Transaction txn);

  int _read_cfg(cfg_reader_t cfg_reader);

public:
  ZonedFreelistManager(CephContext* cct,
		       std::string meta_prefix,
		       std::string info_prefix);

  static void setup_merge_operator(KeyValueDB *db, std::string prefix);

  int create(uint64_t size,
	     uint64_t granularity,
	     KeyValueDB::Transaction txn) override;

  int init(KeyValueDB *kvdb,
	   bool db_in_read_only,
	   cfg_reader_t cfg_reader) override;

  void shutdown() override;
  void sync(KeyValueDB* kvdb) override;
  void dump(KeyValueDB *kvdb) override;

  void enumerate_reset() override;
  bool enumerate_next(KeyValueDB *kvdb,
		      uint64_t *offset,
		      uint64_t *length) override;

  void allocate(uint64_t offset,
		uint64_t length,
		KeyValueDB::Transaction txn) override;

  void release(uint64_t offset,
	       uint64_t length,
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
		std::vector<std::pair<string, string>>*) const override;

  std::vector<zone_state_t> get_zone_states(KeyValueDB *kvdb) const override;
};

#endif
