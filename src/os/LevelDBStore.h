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
#include "leveldb/write_batch.h"
#include "leveldb/slice.h"
#include "leveldb/cache.h"
#ifdef HAVE_LEVELDB_FILTER_POLICY
#include "leveldb/filter_policy.h"
#endif

/**
 * Uses LevelDB to implement the KeyValueDB interface
 */
class LevelDBStore : public KeyValueDB {
  string path;
  boost::scoped_ptr<leveldb::DB> db;
  boost::scoped_ptr<leveldb::Cache> db_cache;
#ifdef HAVE_LEVELDB_FILTER_POLICY
  boost::scoped_ptr<const leveldb::FilterPolicy> filterpolicy;
#endif

  int init(ostream &out, bool create_if_missing);

public:
  /// compact the underlying leveldb store
  void compact() {
    db->CompactRange(NULL, NULL);
  }

  /// compact leveldb for all keys with a given prefix
  void compact_prefix(const string& prefix) {
    // if we combine the prefix with key by adding a '\0' separator,
    // a char(1) will capture all such keys.
    string end = prefix;
    end += (char)1;
    leveldb::Slice cstart(prefix);
    leveldb::Slice cend(end);
    db->CompactRange(&cstart, &cend);
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

  LevelDBStore(const string &path) :
    path(path),
    db_cache(NULL),
#ifdef HAVE_LEVELDB_FILTER_POLICY
    filterpolicy(NULL),
#endif
    options()
  {}

  ~LevelDBStore() {}

  /// Opens underlying db
  int open(ostream &out) {
    return init(out, false);
  }
  /// Creates underlying db if missing and opens it
  int create_and_open(ostream &out) {
    return init(out, true);
  }

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

  int submit_transaction(KeyValueDB::Transaction t) {
    LevelDBTransactionImpl * _t =
      static_cast<LevelDBTransactionImpl *>(t.get());
    leveldb::Status s = db->Write(leveldb::WriteOptions(), &(_t->bat));
    return s.ok() ? 0 : -1;
  }

  int submit_transaction_sync(KeyValueDB::Transaction t) {
    LevelDBTransactionImpl * _t =
      static_cast<LevelDBTransactionImpl *>(t.get());
    leveldb::WriteOptions options;
    options.sync = true;
    leveldb::Status s = db->Write(options, &(_t->bat));
    return s.ok() ? 0 : -1;
  }

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
