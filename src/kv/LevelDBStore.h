// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef LEVEL_DB_STORE_H
#define LEVEL_DB_STORE_H

#include "include/types.h"
#include "include/buffer_fwd.h"
#include "KeyValueDB.h"
#include <set>
#include <map>
#include <string>
#include "include/memory.h"
#include <boost/scoped_ptr.hpp>
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"
#include "leveldb/slice.h"
#include "leveldb/cache.h"
#ifdef HAVE_LEVELDB_FILTER_POLICY
#include "leveldb/filter_policy.h"
#endif

#include <errno.h>
#include "common/errno.h"
#include "common/dout.h"
#include "include/assert.h"
#include "common/Formatter.h"
#include "common/Cond.h"

#include "common/ceph_context.h"

// reinclude our assert to clobber the system one
# include "include/assert.h"

class PerfCounters;

enum {
  l_leveldb_first = 34300,
  l_leveldb_gets,
  l_leveldb_txns,
  l_leveldb_get_latency,
  l_leveldb_submit_latency,
  l_leveldb_submit_sync_latency,
  l_leveldb_compact,
  l_leveldb_compact_range,
  l_leveldb_compact_queue_merge,
  l_leveldb_compact_queue_len,
  l_leveldb_last,
};

extern leveldb::Logger *create_leveldb_ceph_logger();

class CephLevelDBLogger;

/**
 * Uses LevelDB to implement the KeyValueDB interface
 */
class LevelDBStore : public KeyValueDB {
  CephContext *cct;
  PerfCounters *logger;
  CephLevelDBLogger *ceph_logger;
  string path;
  boost::scoped_ptr<leveldb::Cache> db_cache;
#ifdef HAVE_LEVELDB_FILTER_POLICY
  boost::scoped_ptr<const leveldb::FilterPolicy> filterpolicy;
#endif
  boost::scoped_ptr<leveldb::DB> db;

  int do_open(ostream &out, bool create_if_missing);

  // manage async compactions
  Mutex compact_queue_lock;
  Cond compact_queue_cond;
  list< pair<string,string> > compact_queue;
  bool compact_queue_stop;
  class CompactThread : public Thread {
    LevelDBStore *db;
  public:
    explicit CompactThread(LevelDBStore *d) : db(d) {}
    void *entry() override {
      db->compact_thread_entry();
      return NULL;
    }
    friend class LevelDBStore;
  } compact_thread;

  void compact_thread_entry();

  void compact_range(const string& start, const string& end) {
    leveldb::Slice cstart(start);
    leveldb::Slice cend(end);
    db->CompactRange(&cstart, &cend);
  }
  void compact_range_async(const string& start, const string& end);

public:
  /// compact the underlying leveldb store
  void compact() override;

  /// compact db for all keys with a given prefix
  void compact_prefix(const string& prefix) override {
    compact_range(prefix, past_prefix(prefix));
  }
  void compact_prefix_async(const string& prefix) override {
    compact_range_async(prefix, past_prefix(prefix));
  }
  void compact_range(const string& prefix,
		     const string& start, const string& end) override {
    compact_range(combine_strings(prefix, start), combine_strings(prefix, end));
  }
  void compact_range_async(const string& prefix,
			   const string& start, const string& end) override {
    compact_range_async(combine_strings(prefix, start),
			combine_strings(prefix, end));
  }


  /**
   * options_t: Holds options which are minimally interpreted
   * on initialization and then passed through to LevelDB.
   * We transform a couple of these into actual LevelDB
   * structures, but the rest are simply passed through unchanged. See
   * leveldb/options.h for more precise details on each.
   *
   * Set them after constructing the LevelDBStore, but before calling
   * open() or create_and_open().
   */
  struct options_t {
    uint64_t write_buffer_size; /// in-memory write buffer size
    int max_open_files; /// maximum number of files LevelDB can open at once
    uint64_t cache_size; /// size of extra decompressed cache to use
    uint64_t block_size; /// user data per block
    int bloom_size; /// number of bits per entry to put in a bloom filter
    bool compression_enabled; /// whether to use libsnappy compression or not

    // don't change these ones. No, seriously
    int block_restart_interval;
    bool error_if_exists;
    bool paranoid_checks;

    string log_file;

    options_t() :
      write_buffer_size(0), //< 0 means default
      max_open_files(0), //< 0 means default
      cache_size(0), //< 0 means no cache (default)
      block_size(0), //< 0 means default
      bloom_size(0), //< 0 means no bloom filter (default)
      compression_enabled(true), //< set to false for no compression
      block_restart_interval(0), //< 0 means default
      error_if_exists(false), //< set to true if you want to check nonexistence
      paranoid_checks(false) //< set to true if you want paranoid checks
    {}
  } options;

  LevelDBStore(CephContext *c, const string &path) :
    cct(c),
    logger(NULL),
    ceph_logger(NULL),
    path(path),
    db_cache(NULL),
#ifdef HAVE_LEVELDB_FILTER_POLICY
    filterpolicy(NULL),
#endif
    compact_queue_lock("LevelDBStore::compact_thread_lock"),
    compact_queue_stop(false),
    compact_thread(this),
    options()
  {}

  ~LevelDBStore() override;

  static int _test_init(const string& dir);
  int init(string option_str="") override;

  /// Opens underlying db
  int open(ostream &out) override {
    return do_open(out, false);
  }
  /// Creates underlying db if missing and opens it
  int create_and_open(ostream &out) override {
    return do_open(out, true);
  }

  void close() override;

  PerfCounters *get_perf_counters() override
  {
    return logger;
  }

  class LevelDBTransactionImpl : public KeyValueDB::TransactionImpl {
  public:
    leveldb::WriteBatch bat;
    LevelDBStore *db;
    explicit LevelDBTransactionImpl(LevelDBStore *db) : db(db) {}
    void set(
      const string &prefix,
      const string &k,
      const bufferlist &bl) override;
    using KeyValueDB::TransactionImpl::set;
    void rmkey(
      const string &prefix,
      const string &k) override;
    void rmkeys_by_prefix(
      const string &prefix
      ) override;
    virtual void rm_range_keys(
        const string &prefix,
        const string &start,
        const string &end) override;

    using KeyValueDB::TransactionImpl::rmkey;
  };

  KeyValueDB::Transaction get_transaction() override {
    return std::make_shared<LevelDBTransactionImpl>(this);
  }

  int submit_transaction(KeyValueDB::Transaction t) override;
  int submit_transaction_sync(KeyValueDB::Transaction t) override;
  int get(
    const string &prefix,
    const std::set<string> &key,
    std::map<string, bufferlist> *out
    ) override;

  int get(const string &prefix, 
    const string &key,   
    bufferlist *value) override;

  using KeyValueDB::get;

  class LevelDBWholeSpaceIteratorImpl :
    public KeyValueDB::WholeSpaceIteratorImpl {
  protected:
    boost::scoped_ptr<leveldb::Iterator> dbiter;
  public:
    explicit LevelDBWholeSpaceIteratorImpl(leveldb::Iterator *iter) :
      dbiter(iter) { }
    ~LevelDBWholeSpaceIteratorImpl() override { }

    int seek_to_first() override {
      dbiter->SeekToFirst();
      return dbiter->status().ok() ? 0 : -1;
    }
    int seek_to_first(const string &prefix) override {
      leveldb::Slice slice_prefix(prefix);
      dbiter->Seek(slice_prefix);
      return dbiter->status().ok() ? 0 : -1;
    }
    int seek_to_last() override {
      dbiter->SeekToLast();
      return dbiter->status().ok() ? 0 : -1;
    }
    int seek_to_last(const string &prefix) override {
      string limit = past_prefix(prefix);
      leveldb::Slice slice_limit(limit);
      dbiter->Seek(slice_limit);

      if (!dbiter->Valid()) {
        dbiter->SeekToLast();
      } else {
        dbiter->Prev();
      }
      return dbiter->status().ok() ? 0 : -1;
    }
    int upper_bound(const string &prefix, const string &after) override {
      lower_bound(prefix, after);
      if (valid()) {
	pair<string,string> key = raw_key();
	if (key.first == prefix && key.second == after)
	  next();
      }
      return dbiter->status().ok() ? 0 : -1;
    }
    int lower_bound(const string &prefix, const string &to) override {
      string bound = combine_strings(prefix, to);
      leveldb::Slice slice_bound(bound);
      dbiter->Seek(slice_bound);
      return dbiter->status().ok() ? 0 : -1;
    }
    bool valid() override {
      return dbiter->Valid();
    }
    int next() override {
      if (valid())
	dbiter->Next();
      return dbiter->status().ok() ? 0 : -1;
    }
    int prev() override {
      if (valid())
	dbiter->Prev();
      return dbiter->status().ok() ? 0 : -1;
    }
    string key() override {
      string out_key;
      split_key(dbiter->key(), 0, &out_key);
      return out_key;
    }
    pair<string,string> raw_key() override {
      string prefix, key;
      split_key(dbiter->key(), &prefix, &key);
      return make_pair(prefix, key);
    }
    bool raw_key_is_prefixed(const string &prefix) override {
      leveldb::Slice key = dbiter->key();
      if ((key.size() > prefix.length()) && (key[prefix.length()] == '\0')) {
        return memcmp(key.data(), prefix.c_str(), prefix.length()) == 0;
      } else {
        return false;
      }
    }
    bufferlist value() override {
      return to_bufferlist(dbiter->value());
    }

    bufferptr value_as_ptr() override {
      leveldb::Slice data = dbiter->value();
      return bufferptr(data.data(), data.size());
    }

    int status() override {
      return dbiter->status().ok() ? 0 : -1;
    }
  };

  /// Utility
  static string combine_strings(const string &prefix, const string &value);
  static int split_key(leveldb::Slice in, string *prefix, string *key);
  static bufferlist to_bufferlist(leveldb::Slice in);
  static string past_prefix(const string &prefix) {
    string limit = prefix;
    limit.push_back(1);
    return limit;
  }

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
      // we may race against leveldb while reading files; this should only
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


protected:
  WholeSpaceIterator _get_iterator() override {
    return std::make_shared<LevelDBWholeSpaceIteratorImpl>(
	db->NewIterator(leveldb::ReadOptions()));
  }

};

#endif
