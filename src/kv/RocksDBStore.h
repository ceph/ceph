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
  struct BlockBasedTableOptions;
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
  std::shared_ptr<rocksdb::Statistics> dbstats;
  rocksdb::BlockBasedTableOptions bbt_opts;
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
    dbstats(NULL),
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

  void split(const std::string &s, char delim, std::vector<std::string> &elems);
  std::vector<std::string> split(const std::string &s, char delim);
  void get_statistics(Formatter *f);

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
    virtual void Put(const rocksdb::Slice& key,
                    const rocksdb::Slice& value) override {
      string prefix ((key.ToString()).substr(0,1));
      string key_to_decode ((key.ToString()).substr(2,string::npos));
      uint64_t size = (value.ToString()).size();
      seen += "\nPut( Prefix = " + prefix + " key = " 
            + pretty_binary_string(key_to_decode) 
            + " Value size = " + std::to_string(size) + ")";
      num_seen++;
    }
    virtual void SingleDelete(const rocksdb::Slice& key) override {
      string prefix ((key.ToString()).substr(0,1));
      string key_to_decode ((key.ToString()).substr(2,string::npos));
      seen += "\nSingleDelete(Prefix = "+ prefix + " Key = " 
            + pretty_binary_string(key_to_decode) + ")";
      num_seen++;
    }
    virtual void Delete(const rocksdb::Slice& key) override {
      string prefix ((key.ToString()).substr(0,1));
      string key_to_decode ((key.ToString()).substr(2,string::npos));
      seen += "\nDelete( Prefix = " + prefix + " key = " 
            + pretty_binary_string(key_to_decode) + ")";

      num_seen++;
    }
    virtual void Merge(const rocksdb::Slice& key,
                      const rocksdb::Slice& value) override {
      string prefix ((key.ToString()).substr(0,1));
      string key_to_decode ((key.ToString()).substr(2,string::npos));
      uint64_t size = (value.ToString()).size();
      seen += "\nMerge( Prefix = " + prefix + " key = " 
            + pretty_binary_string(key_to_decode) + " Value size = " 
            + std::to_string(size) + ")";

      num_seen++;
    }
    virtual bool Continue() override { return num_seen < 50; }

  };


  class RocksDBTransactionImpl : public KeyValueDB::TransactionImpl {
  public:
    rocksdb::WriteBatch bat;
    RocksDBStore *db;

    explicit RocksDBTransactionImpl(RocksDBStore *_db);
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
