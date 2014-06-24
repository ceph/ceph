// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef KEY_VALUE_DB_H
#define KEY_VALUE_DB_H

#include "include/buffer.h"
#include <set>
#include <map>
#include <string>
#include "include/memory.h"
#include <boost/scoped_ptr.hpp>
#include "ObjectMap.h"

using std::string;
/**
 * Defines virtual interface to be implemented by key value store
 *
 * Kyoto Cabinet or LevelDB should implement this
 */
class KeyValueDB {
public:
  class TransactionImpl {
  public:
    /// Set Keys
    void set(
      const string &prefix,                 ///< [in] Prefix for keys
      const std::map<string, bufferlist> &to_set ///< [in] keys/values to set
    ) {
      std::map<string, bufferlist>::const_iterator it;
      for (it = to_set.begin(); it != to_set.end(); ++it)
	set(prefix, it->first, it->second);
    }

    /// Set Key
    virtual void set(
      const string &prefix,   ///< [in] Prefix for the key
      const string &k,	      ///< [in] Key to set
      const bufferlist &bl    ///< [in] Value to set
      ) = 0;


    /// Removes Keys
    void rmkeys(
      const string &prefix,   ///< [in] Prefix to search for
      const std::set<string> &keys ///< [in] Keys to remove
    ) {
      std::set<string>::const_iterator it;
      for (it = keys.begin(); it != keys.end(); ++it)
	rmkey(prefix, *it);
    }

    /// Remove Key
    virtual void rmkey(
      const string &prefix,   ///< [in] Prefix to search for
      const string &k	      ///< [in] Key to remove
      ) = 0;

    /// Removes keys beginning with prefix
    virtual void rmkeys_by_prefix(
      const string &prefix ///< [in] Prefix by which to remove keys
      ) = 0;

    virtual ~TransactionImpl() {}
  };
  typedef ceph::shared_ptr< TransactionImpl > Transaction;

  /// create a new instance
  static KeyValueDB *create(CephContext *cct, const string& type,
			    const string& dir);

  /// test whether we can successfully initialize; may have side effects (e.g., create)
  static bool test_init(const string& type, const string& dir);
  virtual int init() = 0;
  virtual int open(ostream &out) = 0;
  virtual int create_and_open(ostream &out) = 0;

  virtual Transaction get_transaction() = 0;
  virtual int submit_transaction(Transaction) = 0;
  virtual int submit_transaction_sync(Transaction t) {
    return submit_transaction(t);
  }

  /// Retrieve Keys
  virtual int get(
    const string &prefix,        ///< [in] Prefix for key
    const std::set<string> &key,      ///< [in] Key to retrieve
    std::map<string, bufferlist> *out ///< [out] Key value retrieved
    ) = 0;

  class WholeSpaceIteratorImpl {
  public:
    virtual int seek_to_first() = 0;
    virtual int seek_to_first(const string &prefix) = 0;
    virtual int seek_to_last() = 0;
    virtual int seek_to_last(const string &prefix) = 0;
    virtual int upper_bound(const string &prefix, const string &after) = 0;
    virtual int lower_bound(const string &prefix, const string &to) = 0;
    virtual bool valid() = 0;
    virtual int next() = 0;
    virtual int prev() = 0;
    virtual string key() = 0;
    virtual pair<string,string> raw_key() = 0;
    virtual bufferlist value() = 0;
    virtual int status() = 0;
    virtual ~WholeSpaceIteratorImpl() { }
  };
  typedef ceph::shared_ptr< WholeSpaceIteratorImpl > WholeSpaceIterator;

  class IteratorImpl : public ObjectMap::ObjectMapIteratorImpl {
    const string prefix;
    WholeSpaceIterator generic_iter;
  public:
    IteratorImpl(const string &prefix, WholeSpaceIterator iter) :
      prefix(prefix), generic_iter(iter) { }
    virtual ~IteratorImpl() { }

    int seek_to_first() {
      return generic_iter->seek_to_first(prefix);
    }
    int seek_to_last() {
      return generic_iter->seek_to_last(prefix);
    }
    int upper_bound(const string &after) {
      return generic_iter->upper_bound(prefix, after);
    }
    int lower_bound(const string &to) {
      return generic_iter->lower_bound(prefix, to);
    }
    bool valid() {
      if (!generic_iter->valid())
	return false;
      pair<string,string> raw_key = generic_iter->raw_key();
      return (raw_key.first == prefix);
    }
    int next() {
      if (valid())
	return generic_iter->next();
      return status();
    }
    int prev() {
      if (valid())
	return generic_iter->prev();
      return status();
    }
    string key() {
      return generic_iter->key();
    }
    bufferlist value() {
      return generic_iter->value();
    }
    int status() {
      return generic_iter->status();
    }
  };

  typedef ceph::shared_ptr< IteratorImpl > Iterator;

  WholeSpaceIterator get_iterator() {
    return _get_iterator();
  }

  Iterator get_iterator(const string &prefix) {
    return ceph::shared_ptr<IteratorImpl>(
      new IteratorImpl(prefix, get_iterator())
    );
  }

  WholeSpaceIterator get_snapshot_iterator() {
    return _get_snapshot_iterator();
  }

  Iterator get_snapshot_iterator(const string &prefix) {
    return ceph::shared_ptr<IteratorImpl>(
      new IteratorImpl(prefix, get_snapshot_iterator())
    );
  }

  virtual uint64_t get_estimated_size(map<string,uint64_t> &extra) = 0;

  virtual ~KeyValueDB() {}

  /// compact the underlying store
  virtual void compact() {}

  /// compact db for all keys with a given prefix
  virtual void compact_prefix(const string& prefix) {}
  /// compact db for all keys with a given prefix, async
  virtual void compact_prefix_async(const string& prefix) {}
  virtual void compact_range(const string& prefix,
			     const string& start, const string& end) {}
  virtual void compact_range_async(const string& prefix,
				   const string& start, const string& end) {}

protected:
  virtual WholeSpaceIterator _get_iterator() = 0;
  virtual WholeSpaceIterator _get_snapshot_iterator() = 0;
};

#endif
