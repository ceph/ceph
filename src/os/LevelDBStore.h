// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
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

/**
 * Uses LevelDB to implement the KeyValueDB interface
 */
class LevelDBStore : public KeyValueDB {
  string path;
  boost::scoped_ptr<leveldb::DB> db;
public:
  LevelDBStore(const string &path) : path(path) {}

  /// Opens underlying db
  int init(ostream &out);

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

  class LevelDBIteratorImpl : public KeyValueDB::IteratorImpl {
    boost::scoped_ptr<leveldb::Iterator> dbiter;
    const string prefix;
  public:
    LevelDBIteratorImpl(leveldb::Iterator *iter, const string &prefix) :
      dbiter(iter), prefix(prefix) {}
    int seek_to_first() {
      leveldb::Slice slice_prefix(prefix);
      dbiter->Seek(slice_prefix);
      return dbiter->status().ok() ? 0 : -1;
    }
    int seek_to_last() {
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
    int upper_bound(const string &after) {
      lower_bound(after);
      if (valid() && key() == after)
	next();
      return dbiter->status().ok() ? 0 : -1;
    }
    int lower_bound(const string &to) {
      string bound = combine_strings(prefix, to);
      leveldb::Slice slice_bound(bound);
      dbiter->Seek(slice_bound);
      return dbiter->status().ok() ? 0 : -1;
    }
    bool valid() {
      return dbiter->Valid() && in_prefix(prefix, dbiter->key());
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
    bufferlist value() {
      return to_bufferlist(dbiter->value());
    }
    int status() {
      return dbiter->status().ok() ? 0 : -1;
    }
  };
  Iterator get_iterator(const string &prefix) {
    return std::tr1::shared_ptr<LevelDBIteratorImpl>(
      new LevelDBIteratorImpl(
	db->NewIterator(leveldb::ReadOptions()),
	prefix));
  }


  /// Utility
  static string combine_strings(const string &prefix, const string &value);
  static int split_key(leveldb::Slice in, string *prefix, string *key);
  static bufferlist to_bufferlist(leveldb::Slice in);
  static bool in_prefix(const string &prefix, leveldb::Slice key) {
    return (key.compare(leveldb::Slice(past_prefix(prefix))) < 0) &&
      (key.compare(leveldb::Slice(prefix)) > 0);
  }
  static string past_prefix(const string prefix) {
    string limit = prefix;
    limit.push_back(1);
    return limit;
  }
};

#endif
