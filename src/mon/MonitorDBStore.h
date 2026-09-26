// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2012 Inktank, Inc.
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/
#ifndef CEPH_MONITOR_DB_STORE_H
#define CEPH_MONITOR_DB_STORE_H

#include <algorithm>
#include <filesystem>
#include <set>
#include <map>
#include <string>
#include <boost/scoped_ptr.hpp>
#include <fstream>
#include "kv/KeyValueDB.h"

#include "include/buffer.h"
#include "include/encoding.h"
#include "include/types.h" // for version_t
#include "common/JSONFormatter.h"
#include "common/Finisher.h"
#include "common/strtol.h"
#include "common/PriorityCache.h"

#define dout_context g_ceph_context

class Context;

class MonitorDBStore
{
  std::string path;
  boost::scoped_ptr<KeyValueDB> db;
  bool do_dump;
  int dump_fd_binary;
  std::ofstream dump_fd_json;
  ceph::JSONFormatter dump_fmt;
  

  Finisher io_work;

  bool is_open;

 public:

  std::string get_devname();

  std::string get_path() {
    return path;
  }

  // returns the database store path
  static std::string get_store_path(const std::string& path) {
    return (std::filesystem::path(path) / "store.db").string();
  }

  std::shared_ptr<PriorityCache::PriCache> get_priority_cache() const {
    return db->get_priority_cache();
  }

  struct Op {
    uint8_t type;
    std::string prefix;
    std::string key, endkey;
    ceph::buffer::list bl;

    Op()
      : type(0) { }
    Op(int t, const std::string& p, const std::string& k)
      : type(t), prefix(p), key(k) { }
    Op(int t, const std::string& p, const std::string& k, const ceph::buffer::list& b)
      : type(t), prefix(p), key(k), bl(b) { }
    Op(int t, const std::string& p, const std::string& start, const std::string& end)
      : type(t), prefix(p), key(start), endkey(end) { }

    void encode(ceph::buffer::list& encode_bl) const;
    void decode(ceph::buffer::list::const_iterator& decode_bl);

    void dump(ceph::Formatter *f) const;

    int approx_size() const {
      return 6 + 1 +
	4 + prefix.size() +
	4 + key.size() +
	4 + endkey.size() +
	4 + bl.length();
    }

    static std::list<Op> generate_test_instances();
  };

  struct Transaction;
  typedef std::shared_ptr<Transaction> TransactionRef;
  struct Transaction {
    std::list<Op> ops;
    uint64_t bytes, keys;

    Transaction() : bytes(6 + 4 + 8*2), keys(0) {}

    enum {
      OP_PUT	= 1,
      OP_ERASE	= 2,
      OP_COMPACT = 3,
      OP_ERASE_RANGE = 4,
    };

    void put(const std::string& prefix, const std::string& key, const ceph::buffer::list& bl);
    void put(const std::string& prefix, version_t ver, const ceph::buffer::list& bl);
    void put(const std::string& prefix, const std::string& key, version_t ver);

    void erase(const std::string& prefix, const std::string& key);
    void erase(const std::string& prefix, version_t ver);

    void erase_range(const std::string& prefix, const std::string& begin,
		     const std::string& end);

    void compact_prefix(const std::string& prefix);
    void compact_range(const std::string& prefix, const std::string& start,
		       const std::string& end);

    void encode(ceph::buffer::list& bl) const;
    void decode(ceph::buffer::list::const_iterator& bl);

    static std::list<Transaction> generate_test_instances();

    void append(TransactionRef other);
    void append_from_encoded(ceph::buffer::list& bl);

    bool empty() {
      return (size() == 0);
    }

    size_t size() const {
      return ops.size();
    }
    uint64_t get_keys() const {
      return keys;
    }
    uint64_t get_bytes() const {
      return bytes;
    }

    void dump(ceph::Formatter *f, bool dump_val=false) const;
  };

  int apply_transaction(MonitorDBStore::TransactionRef t);

  struct C_DoTransaction;

  /**
   * queue transaction
   *
   * Queue a transaction to commit asynchronously.  Trigger a context
   * on completion (without any locks held).
   */
  void queue_transaction(MonitorDBStore::TransactionRef t,
			 Context *oncommit);

  /**
   * block and flush all io activity
   */
  void flush() {
    io_work.wait_for_empty();
  }

  class StoreIteratorImpl {
  protected:
    bool done;
    std::pair<std::string,std::string> last_key;
    ceph::buffer::list crc_bl;

    StoreIteratorImpl() : done(false) { }
    virtual ~StoreIteratorImpl() { }

    virtual bool _is_valid() = 0;

  public:
    __u32 crc();
    std::pair<std::string,std::string> get_last_key() {
      return last_key;
    }
    virtual bool has_next_chunk() {
      return !done && _is_valid();
    }
    virtual void get_chunk_tx(TransactionRef tx, uint64_t max_bytes,
			      uint64_t max_keys) = 0;
    virtual std::pair<std::string,std::string> get_next_key() = 0;
  };
  typedef std::shared_ptr<StoreIteratorImpl> Synchronizer;

  class WholeStoreIteratorImpl : public StoreIteratorImpl {
    KeyValueDB::WholeSpaceIterator iter;
    std::set<std::string> sync_prefixes;

  public:
    WholeStoreIteratorImpl(KeyValueDB::WholeSpaceIterator iter,
			   std::set<std::string> &prefixes)
      : StoreIteratorImpl(),
	iter(iter),
	sync_prefixes(prefixes)
    { }

    ~WholeStoreIteratorImpl() override { }

    /**
     * Obtain a chunk of the store
     *
     * @param bl	    Encoded transaction that will recreate the chunk
     * @param first_key	    Pair containing the first key to obtain, and that
     *			    will contain the first key in the chunk (that may
     *			    differ from the one passed on to the function)
     * @param last_key[out] Last key in the chunk
     */
    void get_chunk_tx(TransactionRef tx, uint64_t max_bytes,
		      uint64_t max_keys) override;

    std::pair<std::string,std::string> get_next_key() override;

    bool _is_valid() override {
      return iter->valid();
    }
  };

  Synchronizer get_synchronizer(std::pair<std::string,std::string> &key,
				std::set<std::string> &prefixes);

  KeyValueDB::Iterator get_iterator(const std::string &prefix);
  KeyValueDB::WholeSpaceIterator get_iterator();

  int get(const std::string& prefix, const std::string& key, ceph::buffer::list& bl);
  int get(const std::string& prefix, const version_t ver, ceph::buffer::list& bl);
  version_t get(const std::string& prefix, const std::string& key);

  bool exists(const std::string& prefix, const std::string& key);
  bool exists(const std::string& prefix, version_t ver);

  std::string combine_strings(const std::string& prefix, const std::string& value);
  std::string combine_strings(const std::string& prefix, const version_t ver);

  int clear_key(const std::string& prefix, const std::string& key);

  void clear(std::set<std::string>& prefixes);

  void _open(const std::string& kv_type);
  int open(std::ostream &out);
  int create_and_open(std::ostream &out);

  void close();

  /// @brief Creates a backup of the database under mon_backup_path.
  /// @return stats describing the created backup
  KeyValueDB::BackupStats backup();

  /// @brief Remove old backups in mon_backup_path according to the retention config.
  /// @return stats describing what was kept, deleted, and freed
  KeyValueDB::BackupCleanupStats backup_cleanup();

  /// @brief List all backup versions at backup_path.
  /// @param cct ceph context
  /// @param path path to the local mon data dir (used to discover the kv backend)
  /// @param backup_path path to the backup location
  /// @return list of BackupStats, one per backup
  static std::optional<std::vector<KeyValueDB::BackupStats>> list_backups(
    CephContext *cct, const std::string &path, const std::string &backup_path);

  /// @brief Restore the backup with the given version from backup_path into path.
  /// @param cct ceph context
  /// @param path path to the local mon data dir to restore into
  /// @param backup_path path to the backup location
  /// @param version version of the backup to restore (nullopt for latest)
  /// @return true on success
  static bool restore_backup(CephContext *cct, const std::string &path,
                             const std::string &backup_path,
                             const std::optional<uint32_t> &version);

  void compact() {
    db->compact();
  }

  void compact_async() {
    db->compact_async();
  }

  void compact_prefix(const std::string& prefix) {
    db->compact_prefix(prefix);
  }

  uint64_t get_estimated_size(std::map<std::string, uint64_t> &extras) {
    return db->get_estimated_size(extras);
  }

  /**
   * write_meta - write a simple configuration key out-of-band
   *
   * Write a simple key/value pair for basic store configuration
   * (e.g., a uuid or magic number) to an unopened/unmounted store.
   * The default implementation writes this to a plaintext file in the
   * path.
   *
   * A newline is appended.
   *
   * @param key key name (e.g., "fsid")
   * @param value value (e.g., a uuid rendered as a string)
   * @returns 0 for success, or an error code
   */
  int write_meta(const std::string& key,
		 const std::string& value) const;

  /**
   * read_meta - read a simple configuration key out-of-band
   *
   * Read a simple key value from an unopened/unmounted store.
   *
   * Trailing whitespace is stripped off.
   *
   * @param key key name
   * @param value pointer to value string
   * @returns 0 for success, or an error code
   */
  int read_meta(const std::string& key,
		std::string *value) const {
    return read_meta_path(key, value, path);
  }

  /**
   * read_meta_path - read a simple configuration key out-of-band
   *
   * Read a simple key value from a specified path store.
   *
   * Trailing whitespace is stripped off.
   *
   * @param key key name
   * @param value pointer to value string
   * @param path path to directory
   * @returns 0 for success, or an error code
   */
  static int read_meta_path(const std::string& key,
                            std::string *value,
                            const std::string& path);

  explicit MonitorDBStore(const std::string& path);
  ~MonitorDBStore();
};

WRITE_CLASS_ENCODER(MonitorDBStore::Op)
WRITE_CLASS_ENCODER(MonitorDBStore::Transaction)

#endif /* CEPH_MONITOR_DB_STORE_H */
