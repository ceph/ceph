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
#include "rocksdb/db.h"
#include "kv/rocksdb_cache/BinnedLRUCache.h"
#include <errno.h>
#include "common/errno.h"
#include "common/dout.h"
#include "include/ceph_assert.h"
#include "include/common_fwd.h"
#include "common/Formatter.h"
#include "common/Cond.h"
#include "common/ceph_context.h"
#include "common/PriorityCache.h"
#include "common/pretty_binary.h"

enum {
  l_rocksdb_first = 34300,
  l_rocksdb_get_latency,
  l_rocksdb_submit_latency,
  l_rocksdb_submit_sync_latency,
  l_rocksdb_compact,
  l_rocksdb_compact_running,
  l_rocksdb_compact_completed,
  l_rocksdb_compact_lasted,
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

inline rocksdb::Slice make_slice(const std::optional<std::string>& bound) {
  if (bound) {
    return {*bound};
  } else {
    return {};
  }
}

/**
 * Uses RocksDB to implement the KeyValueDB interface
 */
class RocksDBStore : public KeyValueDB {
  CephContext *cct;
  PerfCounters *logger;
  std::string path;
  std::map<std::string,std::string> kv_options;
  void *priv;
  rocksdb::DB *db;
  rocksdb::Env *env;
  const rocksdb::Comparator* comparator;
  std::shared_ptr<rocksdb::Statistics> dbstats;
  rocksdb::BlockBasedTableOptions bbt_opts;
  std::string options_str;

  uint64_t cache_size = 0;
  bool set_cache_flag = false;
  friend class ShardMergeIteratorImpl;
  friend class CFIteratorImpl;
  friend class WholeMergeIteratorImpl;
  /*
   *  See RocksDB's definition of a column family(CF) and how to use it.
   *  The interfaces of KeyValueDB is extended, when a column family is created.
   *  Prefix will be the name of column family to use.
   */
public:
  struct ColumnFamily {
    std::string name;      //< name of this individual column family
    size_t shard_cnt; //< count of shards
    std::string options;   //< configure option string for this CF
    uint32_t hash_l;  //< first character to take for hash calc.
    uint32_t hash_h;  //< last character to take for hash calc.
    ColumnFamily(const std::string &name, size_t shard_cnt, const std::string &options,
		 uint32_t hash_l, uint32_t hash_h)
      : name(name), shard_cnt(shard_cnt), options(options), hash_l(hash_l), hash_h(hash_h) {}
  };
private:
  friend std::ostream& operator<<(std::ostream& out, const ColumnFamily& cf);

  bool must_close_default_cf = false;
  rocksdb::ColumnFamilyHandle *default_cf = nullptr;

  /// column families in use, name->handles
  struct prefix_shards {
    uint32_t hash_l;  //< first character to take for hash calc.
    uint32_t hash_h;  //< last character to take for hash calc.
    std::vector<rocksdb::ColumnFamilyHandle *> handles;
  };
  std::unordered_map<std::string, prefix_shards> cf_handles;
  typedef decltype(cf_handles)::iterator cf_handles_iterator;
  std::unordered_map<uint32_t, std::string> cf_ids_to_prefix;
  std::unordered_map<std::string, rocksdb::BlockBasedTableOptions> cf_bbt_opts;
  
  void add_column_family(const std::string& cf_name, uint32_t hash_l, uint32_t hash_h,
			 size_t shard_idx, rocksdb::ColumnFamilyHandle *handle);
  bool is_column_family(const std::string& prefix);
  std::string_view get_key_hash_view(const prefix_shards& shards, const char* key, const size_t keylen);
  rocksdb::ColumnFamilyHandle *get_key_cf(const prefix_shards& shards, const char* key, const size_t keylen);
  rocksdb::ColumnFamilyHandle *get_cf_handle(const std::string& prefix, const std::string& key);
  rocksdb::ColumnFamilyHandle *get_cf_handle(const std::string& prefix, const char* key, size_t keylen);
  rocksdb::ColumnFamilyHandle *check_cf_handle_bounds(const cf_handles_iterator& it, const IteratorBounds& bounds);

  int submit_common(rocksdb::WriteOptions& woptions, KeyValueDB::Transaction t);
  int install_cf_mergeop(const std::string &cf_name, rocksdb::ColumnFamilyOptions *cf_opt);
  int create_db_dir();
  int do_open(std::ostream &out, bool create_if_missing, bool open_readonly,
	      const std::string& cfs="");
  int load_rocksdb_options(bool create_if_missing, rocksdb::Options& opt);
public:
  static bool parse_sharding_def(const std::string_view text_def,
				std::vector<ColumnFamily>& sharding_def,
				char const* *error_position = nullptr,
				std::string *error_msg = nullptr);
  const rocksdb::Comparator* get_comparator() const {
    return comparator;
  }

private:
  static void sharding_def_to_columns(const std::vector<ColumnFamily>& sharding_def,
				      std::vector<std::string>& columns);
  int create_shards(const rocksdb::Options& opt,
		    const std::vector<ColumnFamily>& sharding_def);
  int apply_sharding(const rocksdb::Options& opt,
		     const std::string& sharding_text);
  int verify_sharding(const rocksdb::Options& opt,
		      std::vector<rocksdb::ColumnFamilyDescriptor>& existing_cfs,
		      std::vector<std::pair<size_t, RocksDBStore::ColumnFamily> >& existing_cfs_shard,
		      std::vector<rocksdb::ColumnFamilyDescriptor>& missing_cfs,
		      std::vector<std::pair<size_t, RocksDBStore::ColumnFamily> >& missing_cfs_shard);
  std::shared_ptr<rocksdb::Cache> create_block_cache(const std::string& cache_type, size_t cache_size, double cache_prio_high = 0.0);
  int split_column_family_options(const std::string& opts_str,
				  std::unordered_map<std::string, std::string>* column_opts_map,
				  std::string* block_cache_opt);
  int apply_block_cache_options(const std::string& column_name,
				const std::string& block_cache_opt,
				rocksdb::ColumnFamilyOptions* cf_opt);
  int update_column_family_options(const std::string& base_name,
				   const std::string& more_options,
				   rocksdb::ColumnFamilyOptions* cf_opt);
  // manage async compactions
  ceph::mutex compact_queue_lock =
    ceph::make_mutex("RocksDBStore::compact_thread_lock");
  ceph::condition_variable compact_queue_cond;
  std::list<std::pair<std::string,std::string>> compact_queue;
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

  void compact_range(const std::string& start, const std::string& end);
  void compact_range_async(const std::string& start, const std::string& end);
  int tryInterpret(const std::string& key, const std::string& val,
		   rocksdb::Options& opt);

public:
  /// compact the underlying rocksdb store
  bool compact_on_mount;
  bool disableWAL;
  uint64_t get_delete_range_threshold() const {
    return cct->_conf.get_val<uint64_t>("rocksdb_delete_range_threshold");
  }

  void compact() override;

  void compact_async() override {
    compact_range_async({}, {});
  }

  int ParseOptionsFromString(const std::string& opt_str, rocksdb::Options& opt);
  static int ParseOptionsFromStringStatic(
    CephContext* cct,
    const std::string& opt_str,
    rocksdb::Options &opt,
    std::function<int(const std::string&, const std::string&, rocksdb::Options&)> interp);
  static int _test_init(const std::string& dir);
  int init(std::string options_str) override;
  /// compact rocksdb for all keys with a given prefix
  void compact_prefix(const std::string& prefix) override {
    compact_range(prefix, past_prefix(prefix));
  }
  void compact_prefix_async(const std::string& prefix) override {
    compact_range_async(prefix, past_prefix(prefix));
  }

  void compact_range(const std::string& prefix, const std::string& start,
		     const std::string& end) override {
    compact_range(combine_strings(prefix, start), combine_strings(prefix, end));
  }
  void compact_range_async(const std::string& prefix, const std::string& start,
			   const std::string& end) override {
    compact_range_async(combine_strings(prefix, start), combine_strings(prefix, end));
  }

  RocksDBStore(CephContext *c, const std::string &path, std::map<std::string,std::string> opt, void *p) :
    cct(c),
    logger(NULL),
    path(path),
    kv_options(opt),
    priv(p),
    db(NULL),
    env(static_cast<rocksdb::Env*>(p)),
    comparator(nullptr),
    dbstats(NULL),
    compact_queue_stop(false),
    compact_thread(this),
    compact_on_mount(false),
    disableWAL(false)
  {}

  ~RocksDBStore() override;

  static bool check_omap_dir(std::string &omap_dir);
  /// Opens underlying db
  int open(std::ostream &out, const std::string& cfs="") override {
    return do_open(out, false, false, cfs);
  }
  /// Creates underlying db if missing and opens it
  int create_and_open(std::ostream &out,
		      const std::string& cfs="") override;

  int open_read_only(std::ostream &out, const std::string& cfs="") override {
    return do_open(out, false, true, cfs);
  }

  void close() override;

  int repair(std::ostream &out) override;
  void split_stats(const std::string &s, char delim, std::vector<std::string> &elems);
  void get_statistics(ceph::Formatter *f) override;

  PerfCounters *get_perf_counters() override
  {
    return logger;
  }

  bool get_property(
    const std::string &property,
    uint64_t *out) final;

  int64_t estimate_prefix_size(const std::string& prefix,
			       const std::string& key_prefix) override;
  struct RocksWBHandler;
  class RocksDBTransactionImpl : public KeyValueDB::TransactionImpl {
  public:
    rocksdb::WriteBatch bat;
    RocksDBStore *db;

    explicit RocksDBTransactionImpl(RocksDBStore *_db);
  private:
    void put_bat(
      rocksdb::WriteBatch& bat,
      rocksdb::ColumnFamilyHandle *cf,
      const std::string &k,
      const ceph::bufferlist &to_set_bl);
  public:
    size_t get_count() const override {
      return bat.Count();
    }
    size_t get_size_bytes() const override {
      return bat.GetDataSize();
    }
    void set(
      const std::string &prefix,
      const std::string &k,
      const ceph::bufferlist &bl) override;
    void set(
      const std::string &prefix,
      const char *k,
      size_t keylen,
      const ceph::bufferlist &bl) override;
    void rmkey(
      const std::string &prefix,
      const std::string &k) override;
    void rmkey(
      const std::string &prefix,
      const char *k,
      size_t keylen) override;
    void rm_single_key(
      const std::string &prefix,
      const std::string &k) override;
    void rmkeys_by_prefix(
      const std::string &prefix
      ) override;
    void rm_range_keys(
      const std::string &prefix,
      const std::string &start,
      const std::string &end) override;
    void merge(
      const std::string& prefix,
      const std::string& k,
      const ceph::bufferlist &bl) override;
  };

  KeyValueDB::Transaction get_transaction() override {
    return std::make_shared<RocksDBTransactionImpl>(this);
  }

  int submit_transaction(KeyValueDB::Transaction t) override;
  int submit_transaction_sync(KeyValueDB::Transaction t) override;
  int get(
    const std::string &prefix,
    const std::set<std::string> &key,
    std::map<std::string, ceph::bufferlist> *out
    ) override;
  int get(
    const std::string &prefix,
    const std::string &key,
    ceph::bufferlist *out
    ) override;
  int get(
    const std::string &prefix,
    const char *key,
    size_t keylen,
    ceph::bufferlist *out) override;


  class RocksDBWholeSpaceIteratorImpl :
    public KeyValueDB::WholeSpaceIteratorImpl {
  protected:
    rocksdb::Iterator *dbiter;
  public:
    explicit RocksDBWholeSpaceIteratorImpl(const RocksDBStore* db,
                                           rocksdb::ColumnFamilyHandle* cf,
                                           const KeyValueDB::IteratorOpts opts)
      {
        rocksdb::ReadOptions options = rocksdb::ReadOptions();
        if (opts & ITERATOR_NOCACHE)
          options.fill_cache=false;
        dbiter = db->db->NewIterator(options, cf);
    }
    ~RocksDBWholeSpaceIteratorImpl() override;

    int seek_to_first() override;
    int seek_to_first(const std::string &prefix) override;
    int seek_to_last() override;
    int seek_to_last(const std::string &prefix) override;
    int upper_bound(const std::string &prefix, const std::string &after) override;
    int lower_bound(const std::string &prefix, const std::string &to) override;
    bool valid() override;
    int next() override;
    int prev() override;
    std::string key() override;
    std::pair<std::string,std::string> raw_key() override;
    bool raw_key_is_prefixed(const std::string &prefix) override;
    ceph::bufferlist value() override;
    ceph::bufferptr value_as_ptr() override;
    int status() override;
    size_t key_size() override;
    size_t value_size() override;
  };

  Iterator get_iterator(const std::string& prefix, IteratorOpts opts = 0, IteratorBounds = IteratorBounds()) override;
private:
  /// this iterator spans single cf
  WholeSpaceIterator new_shard_iterator(rocksdb::ColumnFamilyHandle* cf);
  Iterator new_shard_iterator(rocksdb::ColumnFamilyHandle* cf,
			      const std::string& prefix, IteratorBounds bound);
public:
  /// Utility
  static std::string combine_strings(const std::string &prefix, const std::string &value) {
    std::string out = prefix;
    out.push_back(0);
    out.append(value);
    return out;
  }
  static void combine_strings(const std::string &prefix,
			      const char *key, size_t keylen,
			      std::string *out) {
    out->reserve(prefix.size() + 1 + keylen);
    *out = prefix;
    out->push_back(0);
    out->append(key, keylen);
  }

  static int split_key(rocksdb::Slice in, std::string *prefix, std::string *key);

  static std::string past_prefix(const std::string &prefix);

  class MergeOperatorRouter;
  class MergeOperatorLinker;
  friend class MergeOperatorRouter;
  int set_merge_operator(
    const std::string& prefix,
    std::shared_ptr<KeyValueDB::MergeOperator> mop) override;
  std::string assoc_name; ///< Name of associative operator

  uint64_t get_estimated_size(std::map<std::string,uint64_t> &extra) override {
    DIR *store_dir = opendir(path.c_str());
    if (!store_dir) {
      lderr(cct) << __func__ << " something happened opening the store: "
                 << cpp_strerror(errno) << dendl;
      return 0;
    }

    uint64_t total_size = 0;
    uint64_t sst_size = 0;
    uint64_t log_size = 0;
    uint64_t misc_size = 0;

    struct dirent *entry = NULL;
    while ((entry = readdir(store_dir)) != NULL) {
      std::string n(entry->d_name);

      if (n == "." || n == "..")
        continue;

      std::string fpath = path + '/' + n;
      struct stat s;
      int err = stat(fpath.c_str(), &s);
      if (err < 0)
	err = -errno;
      // we may race against rocksdb while reading files; this should only
      // happen when those files are being updated, data is being shuffled
      // and files get removed, in which case there's not much of a problem
      // as we'll get to them next time around.
      if (err == -ENOENT) {
	continue;
      }
      if (err < 0) {
        lderr(cct) << __func__ << " error obtaining stats for " << fpath
                   << ": " << cpp_strerror(err) << dendl;
        goto err;
      }

      size_t pos = n.find_last_of('.');
      if (pos == std::string::npos) {
        misc_size += s.st_size;
        continue;
      }

      std::string ext = n.substr(pos+1);
      if (ext == "sst") {
        sst_size += s.st_size;
      } else if (ext == "log") {
        log_size += s.st_size;
      } else {
        misc_size += s.st_size;
      }
    }

    total_size = sst_size + log_size + misc_size;

    extra["sst"] = sst_size;
    extra["log"] = log_size;
    extra["misc"] = misc_size;
    extra["total"] = total_size;

err:
    closedir(store_dir);
    return total_size;
  }

  virtual int64_t get_cache_usage() const override {
    return static_cast<int64_t>(bbt_opts.block_cache->GetUsage());
  }

  virtual int64_t get_cache_usage(std::string prefix) const override {
    auto it = cf_bbt_opts.find(prefix);
    if (it != cf_bbt_opts.end() && it->second.block_cache) {
      return static_cast<int64_t>(it->second.block_cache->GetUsage());
    }
    return -EINVAL;
  }

  int set_cache_size(uint64_t s) override {
    cache_size = s;
    set_cache_flag = true;
    return 0;
  }

  virtual std::shared_ptr<PriorityCache::PriCache>
      get_priority_cache() const override {
    return std::dynamic_pointer_cast<PriorityCache::PriCache>(
        bbt_opts.block_cache);
  }

  virtual std::shared_ptr<PriorityCache::PriCache>
      get_priority_cache(std::string prefix) const override {
    auto it = cf_bbt_opts.find(prefix);
    if (it != cf_bbt_opts.end()) {
      return std::dynamic_pointer_cast<PriorityCache::PriCache>(
          it->second.block_cache);
    }
    return nullptr;
  }

  WholeSpaceIterator get_wholespace_iterator(IteratorOpts opts = 0) override;
private:
  WholeSpaceIterator get_default_cf_iterator();

  using cf_deleter_t = std::function<void(rocksdb::ColumnFamilyHandle*)>;
  using columns_t = std::map<std::string,
			     std::unique_ptr<rocksdb::ColumnFamilyHandle,
					     cf_deleter_t>>;
  int prepare_for_reshard(const std::string& new_sharding,
			  columns_t& to_process_columns);
  int reshard_cleanup(const columns_t& current_columns);
public:
  struct resharding_ctrl {
    size_t bytes_per_iterator = 10000000; /// amount of data to process before refreshing iterator
    size_t keys_per_iterator =  10000;
    size_t bytes_per_batch =    1000000;  /// amount of data before submitting batch
    size_t keys_per_batch =     1000;
    bool   unittest_fail_after_first_batch = false;
    bool   unittest_fail_after_processing_column = false;
    bool   unittest_fail_after_successful_processing = false;
  };
  int reshard(const std::string& new_sharding, const resharding_ctrl* ctrl = nullptr);
  bool get_sharding(std::string& sharding);

};

#endif
