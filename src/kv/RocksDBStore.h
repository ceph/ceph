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

#include <errno.h>
#include "common/errno.h"
#include "common/dout.h"
#include "include/assert.h"
#include "common/Formatter.h"
#include "common/Cond.h"

#include "common/ceph_context.h"
class PerfCounters;

enum {
  l_rocksdb_first = 34300,
  l_rocksdb_gets,
  l_rocksdb_txns,
  l_rocksdb_get_latency,
  l_rocksdb_submit_latency,
  l_rocksdb_submit_sync_latency,
  l_rocksdb_compact,
  l_rocksdb_compact_range,
  l_rocksdb_compact_queue_merge,
  l_rocksdb_compact_queue_len,
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
  struct Options;
}

extern rocksdb::Logger *create_rocksdb_ceph_logger();

/**
 * Uses RocksDB to implement the KeyValueDB interface
 */
class RocksDBStore : public KeyValueDB {
  CephContext *cct;
  PerfCounters *logger;
  string path;
  void *priv;
  rocksdb::DB *db;
  rocksdb::Env *env;
  string options_str;
  int do_open(ostream &out, bool create_if_missing);

  // manage async compactions
  Mutex compact_queue_lock;
  Cond compact_queue_cond;
  list< pair<string,string> > compact_queue;
  bool compact_queue_stop;
  class CompactThread : public Thread {
    RocksDBStore *db;
  public:
    explicit CompactThread(RocksDBStore *d) : db(d) {}
    void *entry() {
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
  void compact();

  int tryInterpret(const string key, const string val, rocksdb::Options &opt);
  int ParseOptionsFromString(const string opt_str, rocksdb::Options &opt);
  static int _test_init(const string& dir);
  int init(string options_str);
  /// compact rocksdb for all keys with a given prefix
  void compact_prefix(const string& prefix) {
    compact_range(prefix, past_prefix(prefix));
  }
  void compact_prefix_async(const string& prefix) {
    compact_range_async(prefix, past_prefix(prefix));
  }

  void compact_range(const string& prefix, const string& start, const string& end) {
    compact_range(combine_strings(prefix, start), combine_strings(prefix, end));
  }
  void compact_range_async(const string& prefix, const string& start, const string& end) {
    compact_range_async(combine_strings(prefix, start), combine_strings(prefix, end));
  }
  int get_info_log_level(string info_log_level);

  RocksDBStore(CephContext *c, const string &path, void *p) :
    cct(c),
    logger(NULL),
    path(path),
    priv(p),
    db(NULL),
    env(static_cast<rocksdb::Env*>(p)),
    compact_queue_lock("RocksDBStore::compact_thread_lock"),
    compact_queue_stop(false),
    compact_thread(this),
    compact_on_mount(false),
    disableWAL(false)
  {}

  ~RocksDBStore();

  static bool check_omap_dir(string &omap_dir);
  /// Opens underlying db
  int open(ostream &out) {
    return do_open(out, false);
  }
  /// Creates underlying db if missing and opens it
  int create_and_open(ostream &out);

  void close();

  class RocksDBTransactionImpl : public KeyValueDB::TransactionImpl {
  public:
    rocksdb::WriteBatch *bat;
    RocksDBStore *db;

    explicit RocksDBTransactionImpl(RocksDBStore *_db);
    ~RocksDBTransactionImpl();
    void set(
      const string &prefix,
      const string &k,
      const bufferlist &bl);
    void rmkey(
      const string &prefix,
      const string &k);
    void rmkeys_by_prefix(
      const string &prefix
      );
    void merge(
      const string& prefix,
      const string& k,
      const bufferlist &bl);
  };

  KeyValueDB::Transaction get_transaction() {
    return std::make_shared<RocksDBTransactionImpl>(this);
  }

  int submit_transaction(KeyValueDB::Transaction t);
  int submit_transaction_sync(KeyValueDB::Transaction t);
  int get(
    const string &prefix,
    const std::set<string> &key,
    std::map<string, bufferlist> *out
    );
  int get(
    const string &prefix,
    const string &key,
    bufferlist *out
    );

  class RocksDBWholeSpaceIteratorImpl :
    public KeyValueDB::WholeSpaceIteratorImpl {
  protected:
    rocksdb::Iterator *dbiter;
  public:
    explicit RocksDBWholeSpaceIteratorImpl(rocksdb::Iterator *iter) :
      dbiter(iter) { }
    //virtual ~RocksDBWholeSpaceIteratorImpl() { }
    ~RocksDBWholeSpaceIteratorImpl();

    int seek_to_first();
    int seek_to_first(const string &prefix);
    int seek_to_last();
    int seek_to_last(const string &prefix);
    int upper_bound(const string &prefix, const string &after);
    int lower_bound(const string &prefix, const string &to);
    bool valid();
    int next();
    int prev();
    string key();
    pair<string,string> raw_key();
    bool raw_key_is_prefixed(const string &prefix);
    bufferlist value();
    bufferptr value_as_ptr();
    int status();
  };

  class RocksDBSnapshotIteratorImpl : public RocksDBWholeSpaceIteratorImpl {
    rocksdb::DB *db;
    const rocksdb::Snapshot *snapshot;
  public:
    RocksDBSnapshotIteratorImpl(rocksdb::DB *db, const rocksdb::Snapshot *s,
				rocksdb::Iterator *iter) :
      RocksDBWholeSpaceIteratorImpl(iter), db(db), snapshot(s) { }

    ~RocksDBSnapshotIteratorImpl();
  };

  /// Utility
  static string combine_strings(const string &prefix, const string &value);
  static int split_key(rocksdb::Slice in, string *prefix, string *key);
  static bufferlist to_bufferlist(rocksdb::Slice in);
  static string past_prefix(const string &prefix);

  class MergeOperatorRouter;
  friend class MergeOperatorRouter;
  virtual int set_merge_operator(const std::string& prefix, std::shared_ptr<KeyValueDB::MergeOperator> mop);
  string assoc_name; // Name of associative operator
 
  virtual uint64_t get_estimated_size(map<string,uint64_t> &extra) {
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


protected:
  WholeSpaceIterator _get_iterator();

  WholeSpaceIterator _get_snapshot_iterator();

};



#endif
