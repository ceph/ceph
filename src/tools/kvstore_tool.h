// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iosfwd>
#include <memory>
#include <string>

#include "acconfig.h"
#include "include/buffer_fwd.h"
#ifdef WITH_BLUESTORE
#include "os/bluestore/BlueStore.h"
#endif

class KeyValueDB;

class StoreTool
{
#ifdef WITH_BLUESTORE
  struct Deleter {
    BlueStore *bluestore;
    Deleter()
      : bluestore(nullptr) {}
    Deleter(BlueStore *store)
      : bluestore(store) {}
    void operator()(KeyValueDB *db) {
      if (bluestore) {
	bluestore->umount();
	delete bluestore;
      } else {
	delete db;
      }
    }
  };
  std::unique_ptr<KeyValueDB, Deleter> db;
#else
  std::unique_ptr<KeyValueDB> db;
#endif

  const std::string store_path;

public:
  StoreTool(const std::string& type,
	    const std::string& path,
            bool read_only,
	    bool need_open_db = true,
	    bool need_stats = false);
  int load_bluestore(const std::string& path, bool read_only, bool need_open_db);
  uint32_t traverse(const std::string& prefix,
                    const bool do_crc,
                    const bool do_value_dump,
                    std::ostream *out);
  void list(const std::string& prefix,
	    const bool do_crc,
	    const bool do_value_dump);
  bool exists(const std::string& prefix);
  bool exists(const std::string& prefix, const std::string& key);
  ceph::bufferlist get(const std::string& prefix,
		       const std::string& key,
		       bool& exists);
  uint64_t get_size();
  bool set(const std::string& prefix,
	   const std::string& key,
	   ceph::bufferlist& val);
  bool rm(const std::string& prefix, const std::string& key);
  bool rm_prefix(const std::string& prefix);
  void print_summary(const uint64_t total_keys, const uint64_t total_size,
                     const uint64_t total_txs, const std::string& store_path,
                     const std::string& other_path, const int duration) const;
  int copy_store_to(const std::string& type, const std::string& other_path,
                    const int num_keys_per_tx, const std::string& other_type);
  void compact();
  void compact_prefix(const std::string& prefix);
  void compact_range(const std::string& prefix,
		     const std::string& start,
		     const std::string& end);
  int destructive_repair();

  int print_stats() const;
  int build_size_histogram(const std::string& prefix) const;
};
