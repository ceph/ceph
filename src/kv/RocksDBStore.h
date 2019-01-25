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
  int install_cf_mergeop(const string &cf_name, rocksdb::ColumnFamilyOptions *cf_opt);
  int create_db_dir();
  int do_open(ostream &out, bool create_if_missing, bool open_readonly,
	      const vector<ColumnFamily>* cfs = nullptr);
  int load_rocksdb_options(bool create_if_missing, rocksdb::Options& opt);

  // manage async compactions
  Mutex compact_queue_lock;
  Cond compact_queue_cond;
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

public:
  /// compact the underlying rocksdb store
  bool compact_on_mount;
  bool disableWAL;
  bool enable_rmrange;
  void compact() override;

  void compact_async() override {
    compact_range_async(string(), string());
  }

  int tryInterpret(const string& key, const string& val, rocksdb::Options &opt);
  int ParseOptionsFromString(const string& opt_str, rocksdb::Options &opt);
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
    compact_queue_lock("RocksDBStore::compact_thread_lock"),
    compact_queue_stop(false),
    compact_thread(this),
    compact_on_mount(false),
    disableWAL(false),
    enable_rmrange(cct->_conf->rocksdb_enable_rmrange)
  {}

  ~RocksDBStore() override;

  static bool check_omap_dir(string &omap_dir);
  /// Opens underlying db
  int open(ostream &out, const vector<ColumnFamily>& cfs = {}) override {
    return do_open(out, false, false, &cfs);
  }
  /// Creates underlying db if missing and opens it
  int create_and_open(ostream &out,
		      const vector<ColumnFamily>& cfs = {}) override;

  int open_read_only(ostream &out, const vector<ColumnFamily>& cfs = {}) override {
    return do_open(out, false, true, &cfs);
  }

  void close() override;

  rocksdb::ColumnFamilyHandle *get_cf_handle(const std::string& cf_name) {
    auto iter = cf_handles.find(cf_name);
    if (iter == cf_handles.end())
      return nullptr;
    else
      return static_cast<rocksdb::ColumnFamilyHandle*>(iter->second);
  }
  int repair(std::ostream &out) override;
  void split_stats(const std::string &s, char delim, std::vector<std::string> &elems);
  void get_statistics(Formatter *f) override;

  PerfCounters *get_perf_counters() override
  {
    return logger;
  }

  int64_t estimate_prefix_size(const string& prefix) override;

  struct  RocksWBHandler: public rocksdb::WriteBatch::Handler {
    std::string seen ;
    int num_seen = 0;
    static string pretty_binary_string(const string& in) {
      char buf[10];
      string out;
      out.reserve(in.length() * 3);
      enum { NONE, HEX, STRING } mode = NONE;
      unsigned from = 0, i;
      for (i=0; i < in.length(); ++i) {
        if ((in[i] < 32 || (unsigned char)in[i] > 126) ||
          (mode == HEX && in.length() - i >= 4 &&
          ((in[i] < 32 || (unsigned char)in[i] > 126) ||
          (in[i+1] < 32 || (unsigned char)in[i+1] > 126) ||
          (in[i+2] < 32 || (unsigned char)in[i+2] > 126) ||
          (in[i+3] < 32 || (unsigned char)in[i+3] > 126)))) {

          if (mode == STRING) {
            out.append(in.substr(from, i - from));
            out.push_back('\'');
          }
          if (mode != HEX) {
            out.append("0x");
            mode = HEX;
          }
          if (in.length() - i >= 4) {
            // print a whole u32 at once
            snprintf(buf, sizeof(buf), "%08x",
                  (uint32_t)(((unsigned char)in[i] << 24) |
                            ((unsigned char)in[i+1] << 16) |
                            ((unsigned char)in[i+2] << 8) |
                            ((unsigned char)in[i+3] << 0)));
            i += 3;
          } else {
            snprintf(buf, sizeof(buf), "%02x", (int)(unsigned char)in[i]);
          }
          out.append(buf);
        } else {
          if (mode != STRING) {
            out.push_back('\'');
            mode = STRING;
            from = i;
          }
        }
      }
      if (mode == STRING) {
        out.append(in.substr(from, i - from));
        out.push_back('\'');
      }
      return out;
    }
    void Put(const rocksdb::Slice& key,
                    const rocksdb::Slice& value) override {
      string prefix ((key.ToString()).substr(0,1));
      string key_to_decode ((key.ToString()).substr(2,string::npos));
      uint64_t size = (value.ToString()).size();
      seen += "\nPut( Prefix = " + prefix + " key = " 
            + pretty_binary_string(key_to_decode) 
            + " Value size = " + std::to_string(size) + ")";
      num_seen++;
    }
    void SingleDelete(const rocksdb::Slice& key) override {
      string prefix ((key.ToString()).substr(0,1));
      string key_to_decode ((key.ToString()).substr(2,string::npos));
      seen += "\nSingleDelete(Prefix = "+ prefix + " Key = " 
            + pretty_binary_string(key_to_decode) + ")";
      num_seen++;
    }
    void Delete(const rocksdb::Slice& key) override {
      string prefix ((key.ToString()).substr(0,1));
      string key_to_decode ((key.ToString()).substr(2,string::npos));
      seen += "\nDelete( Prefix = " + prefix + " key = " 
            + pretty_binary_string(key_to_decode) + ")";

      num_seen++;
    }
    void Merge(const rocksdb::Slice& key,
                      const rocksdb::Slice& value) override {
      string prefix ((key.ToString()).substr(0,1));
      string key_to_decode ((key.ToString()).substr(2,string::npos));
      uint64_t size = (value.ToString()).size();
      seen += "\nMerge( Prefix = " + prefix + " key = " 
            + pretty_binary_string(key_to_decode) + " Value size = " 
            + std::to_string(size) + ")";

      num_seen++;
    }
    bool Continue() override { return num_seen < 50; }

  };

  class RocksDBTransactionImpl : public KeyValueDB::TransactionImpl {
  public:
    rocksdb::WriteBatch bat;
    RocksDBStore *db;

    explicit RocksDBTransactionImpl(RocksDBStore *_db);
  private:
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
  friend class MergeOperatorRouter;
  int set_merge_operator(
    const std::string& prefix,
    std::shared_ptr<KeyValueDB::MergeOperator> mop) override;
  string assoc_name; ///< Name of associative operator

  uint64_t get_estimated_size(map<string,uint64_t> &extra) override {
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
      string n(entry->d_name);

      if (n == "." || n == "..")
        continue;

      string fpath = path + '/' + n;
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
      if (pos == string::npos) {
        misc_size += s.st_size;
        continue;
      }

      string ext = n.substr(pos+1);
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
};



#endif
