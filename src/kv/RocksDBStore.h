// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef ROCKS_DB_STORE_H
#define ROCKS_DB_STORE_H

#include "include/types.h"
#include "include/buffer_fwd.h"
#include "KeyValueDB.h"
#include <set>
#include <map>
#include <string>
#include <memory>
#include <boost/scoped_ptr.hpp>
#include "rocksdb/write_batch.h"
#include "rocksdb/perf_context.h"
#include "rocksdb/iostats_context.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "kv/rocksdb_cache/BinnedLRUCache.h"
#include <errno.h>
#include "common/errno.h"
#include "common/dout.h"
#include "include/ceph_assert.h"
#include "common/Formatter.h"
#include "common/Cond.h"
#include "common/ceph_context.h"
#include "common/PriorityCache.h"

class PerfCounters;

enum {
  l_rocksdb_first = 34300,
  l_rocksdb_gets,
  l_rocksdb_txns,
  l_rocksdb_txns_sync,
  l_rocksdb_get_latency,
  l_rocksdb_submit_latency,
  l_rocksdb_submit_sync_latency,
  l_rocksdb_compact,
  l_rocksdb_compact_range,
  l_rocksdb_compact_queue_merge,
  l_rocksdb_compact_queue_len,
  l_rocksdb_write_wal_time,
  l_rocksdb_write_memtable_time,
  l_rocksdb_write_delay_time,
  l_rocksdb_write_pre_and_post_process_time,
  l_rocksdb_last,
};

namespace rocksdb{
  class DB;
  class Env;
  class Cache;
  class FilterPolicy;
  class Snapshot;
  class Slice;
  class WriteBatch;
  class Iterator;
  class Logger;
  class ColumnFamilyHandle;
  struct Options;
  struct BlockBasedTableOptions;
  struct DBOptions;
  struct ColumnFamilyOptions;
}

extern rocksdb::Logger *create_rocksdb_ceph_logger();

/**
 * Uses RocksDB to implement the KeyValueDB interface
 */
class RocksDBStore : public KeyValueDB {
  CephContext *cct;
  PerfCounters *logger;
  string path;
  map<string,string> kv_options;
  void *priv;
  rocksdb::DB *db;
  rocksdb::Env *env;
  std::shared_ptr<rocksdb::Statistics> dbstats;
  rocksdb::BlockBasedTableOptions bbt_opts;
  string options_str;

  uint64_t cache_size = 0;
  bool set_cache_flag = false;

  bool must_close_default_cf = false;
  rocksdb::ColumnFamilyHandle *default_cf = nullptr;

  int submit_common(rocksdb::WriteOptions& woptions, KeyValueDB::Transaction t);

  int create_db_dir();
  int load_rocksdb_options(bool create_if_missing, rocksdb::Options& opt);

  // manage async compactions
  ceph::mutex compact_queue_lock =
    ceph::make_mutex("RocksDBStore::compact_thread_lock");
  ceph::condition_variable compact_queue_cond;
  list< pair<string,string> > compact_queue;
  bool compact_queue_stop;
  class CompactThread : public Thread {
    RocksDBStore *db;
  public:
    explicit CompactThread(RocksDBStore *d) : db(d) {}
    void *entry() override {
      db->compact_thread_entry();
      return NULL;
    }
    friend class RocksDBStore;
  } compact_thread;

  void compact_thread_entry();

  void compact_range(const string& start, const string& end);
  void compact_range_async(const string& start, const string& end);
  int tryInterpret(const string& key, const string& val, rocksdb::Options& opt);

public:
  /// compact the underlying rocksdb store
  bool compact_on_mount;
  bool disableWAL;
  const uint64_t delete_range_threshold;
  void compact() override;

  void compact_async() override {
    compact_range_async(string(), string());
  }

  int ParseOptionsFromString(const string& opt_str, rocksdb::Options& opt);
  static int ParseOptionsFromStringStatic(
    CephContext* cct,
    const string& opt_str,
    rocksdb::Options &opt,
    function<int(const string&, const string&, rocksdb::Options&)> interp);
  static int _test_init(const string& dir);
  int init(string options_str) override;
  /// compact rocksdb for all keys with a given prefix
  void compact_prefix(const string& prefix) override {
    compact_range(prefix, past_prefix(prefix));
  }
  void compact_prefix_async(const string& prefix) override {
    compact_range_async(prefix, past_prefix(prefix));
  }

  void compact_range(const string& prefix, const string& start, const string& end) override {
    compact_range(combine_strings(prefix, start), combine_strings(prefix, end));
  }
  void compact_range_async(const string& prefix, const string& start, const string& end) override {
    compact_range_async(combine_strings(prefix, start), combine_strings(prefix, end));
  }

  RocksDBStore(CephContext *c, const string &path, map<string,string> opt, void *p) :
    cct(c),
    logger(NULL),
    path(path),
    kv_options(opt),
    priv(p),
    db(NULL),
    env(static_cast<rocksdb::Env*>(p)),
    dbstats(NULL),
    compact_queue_stop(false),
    compact_thread(this),
    compact_on_mount(false),
    disableWAL(false),
    delete_range_threshold(cct->_conf.get_val<uint64_t>("rocksdb_delete_range_threshold"))
  {}

  ~RocksDBStore() override;

  static bool check_omap_dir(string &omap_dir);
  int open(ostream &out, const std::vector<ColumnFamily>& options = {}) override;
  /// Creates underlying db if missing and opens it
  int create_and_open(ostream &out, const std::vector<ColumnFamily>& new_cfs = {}) override;

  int open_read_only(ostream &out, const vector<ColumnFamily>& cfs = {}) override {
    return do_open(out, false, true, &cfs);
  }

  void close() override;

  int column_family_list(vector<std::string>& cf_names) override;
  int column_family_create(const std::string& name, const std::string& options) override;
  int column_family_delete(const std::string& name) override;
  KeyValueDB::ColumnFamilyHandle column_family_handle(const std::string& cf_name) override;

private:
  /*
   * Get merge operator for column family.
   */
  std::shared_ptr<rocksdb::MergeOperator> cf_get_merge_operator(const std::string& prefix);

  /*
   * Returns handle to mono column family.
   * Does not return handles for regular column family, even if name matches
   */
  rocksdb::ColumnFamilyHandle *cf_mono_get_handle(const std::string& cf_name) {
    auto iter = cf_mono_handles.find(cf_name);
    if (iter == cf_mono_handles.end())
      return nullptr;
    else
      return static_cast<rocksdb::ColumnFamilyHandle*>(iter->second.priv);
  }

  /*
   * Determines how 'prefix' should be handled.
   * It is a convenience function that allow to better structuralize conditions
   * in functions that operate on both mono column families and regular column families.
   *
   * Param:
   *  - cf [in] column family handle as requested by client
   *       [out] fixed column family handle after redirection
   *  - prefix [in] as requested by client
   * Result:
   *  true - we operate on mono column family, false - we operate on normal column family
   */
  bool cf_check_mode(rocksdb::ColumnFamilyHandle* &cf, const string &prefix) {
    if (cf != nullptr) {
      return false;
    }
    cf = get_cf_handle(prefix);
    if (cf != nullptr)
      return true;
    cf = default_cf;
    return false;
  }

private:
  rocksdb::Options rocksdb_options;
  int open_existing(rocksdb::Options& rocksdb_options);

  struct ColumnFamilyData {
      string options;                   ///< specific configure option string for this CF
      ColumnFamilyHandle handle;        ///< handle to column family
      ColumnFamilyData(const string &options, ColumnFamilyHandle handle = {nullptr})
        : options(options), handle(handle) {}
      ColumnFamilyData() {}
    };
  typedef std::string ColumnFamilyName;
  std::map<ColumnFamilyName, ColumnFamilyData> column_families;

  std::unordered_map<std::string, ColumnFamilyHandle> cf_mono_handles;

  void perf_counters_register();

  std::pair<std::string, ColumnFamilyHandle> get_cf_by_rocksdb_ID(uint32_t ID) const;
  friend class RocksWBHandler;
public:
  rocksdb::ColumnFamilyHandle *get_cf_handle(const std::string& cf_name) const {
    auto iter = cf_mono_handles.find(cf_name);
    if (iter == cf_mono_handles.end())
      return nullptr;
    else
      return static_cast<rocksdb::ColumnFamilyHandle*>(iter->second.priv);
  }

  int repair(std::ostream &out) override;
  void split_stats(const std::string &s, char delim, std::vector<std::string> &elems);
  void get_statistics(Formatter *f) override;

  PerfCounters *get_perf_counters() override
  {
    return logger;
  }

  bool get_property(
    const std::string &property,
    uint64_t *out) final;

  int64_t estimate_prefix_size(const string& prefix,
			       const string& key_prefix) override;

  class RocksDBTransactionImpl : public KeyValueDB::TransactionImpl {
  public:
    rocksdb::WriteBatch bat;
    RocksDBStore *db;

    explicit RocksDBTransactionImpl(RocksDBStore *_db);
  private:
    rocksdb::ColumnFamilyHandle *cf_handle;

    void put_bat(
      rocksdb::WriteBatch& bat,
      rocksdb::ColumnFamilyHandle *cf,
      const string &k,
      const bufferlist &to_set_bl);
  public:
    void set(
      const string &prefix,
      const string &k,
      const bufferlist &bl) override;
    void set(
      const string &prefix,
      const char *k,
      size_t keylen,
      const bufferlist &bl) override;
    void rmkey(
      const string &prefix,
      const string &k) override;
    void rmkey(
      const string &prefix,
      const char *k,
      size_t keylen) override;
    void rm_single_key(
      const string &prefix,
      const string &k) override;
    void rmkeys_by_prefix(
      const string &prefix
      ) override;
    void rm_range_keys(
      const string &prefix,
      const string &start,
      const string &end) override;
    void merge(
      const string& prefix,
      const string& k,
      const bufferlist &bl) override;
    void select(
        KeyValueDB::ColumnFamilyHandle column_family_handle) override;
  };

  KeyValueDB::Transaction get_transaction() override {
    return std::make_shared<RocksDBTransactionImpl>(this);
  }

  int submit_transaction(KeyValueDB::Transaction t) override;
  int submit_transaction_sync(KeyValueDB::Transaction t) override;
  int get(
    const string &prefix,
    const std::set<string> &key,
    std::map<string, bufferlist> *out
    ) override;
  int get(
    const string &prefix,
    const string &key,
    bufferlist *out
    ) override;
  int get(
    const string &prefix,
    const char *key,
    size_t keylen,
    bufferlist *out) override;
  int get(
    KeyValueDB::ColumnFamilyHandle cf_handle,
    const std::string &prefix,
    const std::set<std::string> &keys,
    std::map<std::string, bufferlist> *out) override;
  int get(
    KeyValueDB::ColumnFamilyHandle cf_handle,
    const string &prefix,
    const string &key,
    bufferlist *out
    ) override;

  class RocksDBWholeSpaceIteratorImpl :
    public KeyValueDB::WholeSpaceIteratorImpl {
  protected:
    rocksdb::Iterator *dbiter;
  public:
    explicit RocksDBWholeSpaceIteratorImpl(rocksdb::Iterator *iter) :
      dbiter(iter) { }
    //virtual ~RocksDBWholeSpaceIteratorImpl() { }
    ~RocksDBWholeSpaceIteratorImpl() override;

    int seek_to_first() override;
    int seek_to_first(const string &prefix) override;
    int seek_to_last() override;
    int seek_to_last(const string &prefix) override;
    int upper_bound(const string &prefix, const string &after) override;
    int lower_bound(const string &prefix, const string &to) override;
    bool valid() override;
    int next() override;
    int prev() override;
    string key() override;
    pair<string,string> raw_key() override;
    bool raw_key_is_prefixed(const string &prefix) override;
    bufferlist value() override;
    bufferptr value_as_ptr() override;
    int status() override;
    size_t key_size() override;
    size_t value_size() override;
  };

  Iterator get_iterator(const std::string& prefix) override;
  Iterator get_iterator_cf(ColumnFamilyHandle cfh, const std::string &prefix) override;

  /// Utility
  static string combine_strings(const string &prefix, const string &value) {
    string out = prefix;
    out.push_back(0);
    out.append(value);
    return out;
  }
  static void combine_strings(const string &prefix,
			      const char *key, size_t keylen,
			      string *out) {
    out->reserve(prefix.size() + 1 + keylen);
    *out = prefix;
    out->push_back(0);
    out->append(key, keylen);
  }

  static int split_key(rocksdb::Slice in, string *prefix, string *key);

  static string past_prefix(const string &prefix);

  class MergeOperatorRouter;
  class MergeOperatorLinker;
  class MergeOperatorAll;

  friend class MergeOperatorRouter;
  int set_merge_operator(
    const std::string& prefix,
    std::shared_ptr<KeyValueDB::MergeOperator> mop) override;

  virtual int64_t get_cache_usage() const override {
    return static_cast<int64_t>(bbt_opts.block_cache->GetUsage());
  }
  uint64_t get_estimated_size(map<string,uint64_t> &extra) override;
  virtual int64_t request_cache_bytes(
      PriorityCache::Priority pri, uint64_t cache_bytes) const override;
  virtual int64_t commit_cache_size() override;
  virtual std::string get_cache_name() const override {
    return "RocksDB Block Cache";
  }
  int set_cache_size(uint64_t s) override {
    cache_size = s;
    set_cache_flag = true;
    return 0;
  }
  int set_cache_capacity(int64_t capacity);
  int64_t get_cache_capacity();

  virtual std::shared_ptr<PriorityCache::PriCache> get_priority_cache() 
      const override {
    return dynamic_pointer_cast<PriorityCache::PriCache>(
        bbt_opts.block_cache);
  }

  WholeSpaceIterator get_wholespace_iterator() override;
  WholeSpaceIterator get_wholespace_iterator_cf(ColumnFamilyHandle cfh) override;
};

#endif
