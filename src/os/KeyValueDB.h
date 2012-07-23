// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef KEY_VALUE_DB_H
#define KEY_VALUE_DB_H

#include "include/buffer.h"
#include <set>
#include <map>
#include <string>
#include <tr1/memory>
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

    virtual ~TransactionImpl() {};
  };
  typedef std::tr1::shared_ptr< TransactionImpl > Transaction;

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

  class IteratorImpl : public ObjectMap::ObjectMapIteratorImpl {
  public:
    virtual int seek_to_first() = 0;
    virtual int seek_to_last() = 0;
    virtual int upper_bound(const string &after) = 0;
    virtual int lower_bound(const string &to) = 0;
    virtual bool valid() = 0;
    virtual int next() = 0;
    virtual int prev() = 0;
    virtual string key() = 0;
    virtual pair<string,string> raw_key() = 0;
    virtual bufferlist value() = 0;
    virtual int status() = 0;
    virtual ~IteratorImpl() {}
  };
  typedef std::tr1::shared_ptr< IteratorImpl > Iterator;
  virtual Iterator get_iterator(const string &prefix) = 0;

  virtual ~KeyValueDB() {}
};

#endif
