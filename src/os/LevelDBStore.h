// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef LEVEL_DB_STORE_H
#define LEVEL_DB_STORE_H

#include "include/types.h"
#include "include/buffer.h"
#include "KeyValueDB.h"
#include <set>
#include <map>
#include <string>
#include <tr1/memory>
#include <boost/scoped_ptr.hpp>
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"
#include "leveldb/slice.h"
#include "leveldb/cache.h"
#ifdef HAVE_LEVELDB_FILTER_POLICY
#include "leveldb/filter_policy.h"
#endif

#include "common/ceph_context.h"

class PerfCounters;

enum {
  l_leveldb_first = 34300,
  l_leveldb_gets,
  l_leveldb_txns,
  l_leveldb_compact,
  l_leveldb_compact_range,
  l_leveldb_compact_queue_merge,
  l_leveldb_compact_queue_len,
  l_leveldb_last,
};

/**
 * Uses LevelDB to implement the KeyValueDB interface
 */
class LevelDBStore : public KeyValueDB {
  CephContext *cct;
  PerfCounters *logger;
  string path;
  boost::scoped_ptr<leveldb::DB> db;
  boost::scoped_ptr<leveldb::Cache> db_cache;
#ifdef HAVE_LEVELDB_FILTER_POLICY
  boost::scoped_ptr<const leveldb::FilterPolicy> filterpolicy;
#endif

  int init(ostream &out, bool create_if_missing);

  // manage async compactions
  Mutex compact_queue_lock;
  Cond compact_queue_cond;
  list< pair<string,string> > compact_queue;
  bool compact_queue_stop;
  class CompactThread : public Thread {
    LevelDBStore *db;
  public:
    CompactThread(LevelDBStore *d) : db(d) {}
    void *entry() {
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
  void compact();

  /// compact leveldb for all keys with a given prefix
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

  ~LevelDBStore();

  /// Opens underlying db
  int open(ostream &out) {
    return init(out, false);
  }
  /// Creates underlying db if missing and opens it
  int create_and_open(ostream &out) {
    return init(out, true);
  }

  void close();

  class LevelDBTransactionImpl : public KeyValueDB::TransactionImpl {
  public:
    leveldb::WriteBatch bat;
    list<bufferlist> buffers;
    list<string> keys;
    LevelDBStore *db;

    LevelDBTransactionImpl(LevelDBStore *db) : db(db) {}
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
    return std::tr1::shared_ptr< LevelDBTransactionImpl >(
      new LevelDBTransactionImpl(this));
  }

  int submit_transaction(KeyValueDB::Transaction t);
  int submit_transaction_sync(KeyValueDB::Transaction t);
  int get(
    const string &prefix,
    const std::set<string> &key,
    std::map<string, bufferlist> *out
    );

  class LevelDBWholeSpaceIteratorImpl :
    public KeyValueDB::WholeSpaceIteratorImpl {
  protected:
    boost::scoped_ptr<leveldb::Iterator> dbiter;
  public:
    LevelDBWholeSpaceIteratorImpl(leveldb::Iterator *iter) :
      dbiter(iter) { }
    virtual ~LevelDBWholeSpaceIteratorImpl() { }

    int seek_to_first() {
      dbiter->SeekToFirst();
      return dbiter->status().ok() ? 0 : -1;
    }
    int seek_to_first(const string &prefix) {
      leveldb::Slice slice_prefix(prefix);
      dbiter->Seek(slice_prefix);
      return dbiter->status().ok() ? 0 : -1;
    }
    int seek_to_last() {
      dbiter->SeekToLast();
      return dbiter->status().ok() ? 0 : -1;
    }
    int seek_to_last(const string &prefix) {
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
    int upper_bound(const string &prefix, const string &after) {
      lower_bound(prefix, after);
      if (valid()) {
	pair<string,string> key = raw_key();
	if (key.first == prefix && key.second == after)
	  next();
      }
      return dbiter->status().ok() ? 0 : -1;
    }
    int lower_bound(const string &prefix, const string &to) {
      string bound = combine_strings(prefix, to);
      leveldb::Slice slice_bound(bound);
      dbiter->Seek(slice_bound);
      return dbiter->status().ok() ? 0 : -1;
    }
    bool valid() {
      return dbiter->Valid();
    }
    int next() {
      if (valid())
	dbiter->Next();
      return dbiter->status().ok() ? 0 : -1;
    }
    int prev() {
      if (valid())
	dbiter->Prev();
      return dbiter->status().ok() ? 0 : -1;
    }
    string key() {
      string out_key;
      split_key(dbiter->key(), 0, &out_key);
      return out_key;
    }
    pair<string,string> raw_key() {
      string prefix, key;
      split_key(dbiter->key(), &prefix, &key);
      return make_pair(prefix, key);
    }
    bufferlist value() {
      return to_bufferlist(dbiter->value());
    }
    int status() {
      return dbiter->status().ok() ? 0 : -1;
    }
  };

  class LevelDBSnapshotIteratorImpl : public LevelDBWholeSpaceIteratorImpl {
    leveldb::DB *db;
    const leveldb::Snapshot *snapshot;
  public:
    LevelDBSnapshotIteratorImpl(leveldb::DB *db, const leveldb::Snapshot *s,
				leveldb::Iterator *iter) :
      LevelDBWholeSpaceIteratorImpl(iter), db(db), snapshot(s) { }

    ~LevelDBSnapshotIteratorImpl() {
      assert(snapshot != NULL);
      db->ReleaseSnapshot(snapshot);
    }
  };

  /// Utility
  static string combine_strings(const string &prefix, const string &value);
  static int split_key(leveldb::Slice in, string *prefix, string *key);
  static bufferlist to_bufferlist(leveldb::Slice in);
  static bool in_prefix(const string &prefix, leveldb::Slice key) {
    return (key.compare(leveldb::Slice(past_prefix(prefix))) < 0) &&
      (key.compare(leveldb::Slice(prefix)) > 0);
  }
  static string past_prefix(const string &prefix) {
    string limit = prefix;
    limit.push_back(1);
    return limit;
  }

protected:
  WholeSpaceIterator _get_iterator() {
    return std::tr1::shared_ptr<KeyValueDB::WholeSpaceIteratorImpl>(
      new LevelDBWholeSpaceIteratorImpl(
	db->NewIterator(leveldb::ReadOptions())
      )
    );
  }

  WholeSpaceIterator _get_snapshot_iterator() {
    const leveldb::Snapshot *snapshot;
    leveldb::ReadOptions options;

    snapshot = db->GetSnapshot();
    options.snapshot = snapshot;

    return std::tr1::shared_ptr<KeyValueDB::WholeSpaceIteratorImpl>(
      new LevelDBSnapshotIteratorImpl(db.get(), snapshot,
	db->NewIterator(options))
    );
  }

};

#endif
