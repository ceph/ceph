// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef KEY_VALUE_DB_H
#define KEY_VALUE_DB_H

#include "include/buffer.h"
#include <ostream>
#include <set>
#include <map>
#include <string>
#include "include/memory.h"
#include <boost/scoped_ptr.hpp>
#include "include/encoding.h"

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
      const std::string &prefix,                 ///< [in] Prefix for keys
      const std::map<std::string, bufferlist> &to_set ///< [in] keys/values to set
    ) {
      std::map<std::string, bufferlist>::const_iterator it;
      for (it = to_set.begin(); it != to_set.end(); ++it)
	set(prefix, it->first, it->second);
    }

    /// Set Keys (via encoded bufferlist)
    void set(
      const std::string &prefix,      ///< [in] prefix
      bufferlist& to_set_bl     ///< [in] encoded key/values to set
      ) {
      bufferlist::iterator p = to_set_bl.begin();
      uint32_t num;
      ::decode(num, p);
      while (num--) {
	string key;
	bufferlist value;
	::decode(key, p);
	::decode(value, p);
	set(prefix, key, value);
      }
    }

    /// Set Key
    virtual void set(
      const std::string &prefix,   ///< [in] Prefix for the key
      const std::string &k,	      ///< [in] Key to set
      const bufferlist &bl    ///< [in] Value to set
      ) = 0;


    /// Removes Keys (via encoded bufferlist)
    void rmkeys(
      const std::string &prefix,   ///< [in] Prefix to search for
      bufferlist &keys_bl ///< [in] Keys to remove
    ) {
      bufferlist::iterator p = keys_bl.begin();
      uint32_t num;
      ::decode(num, p);
      while (num--) {
	string key;
	::decode(key, p);
	rmkey(prefix, key);
      }
    }

    /// Removes Keys
    void rmkeys(
      const std::string &prefix,   ///< [in] Prefix to search for
      const std::set<std::string> &keys ///< [in] Keys to remove
    ) {
      std::set<std::string>::const_iterator it;
      for (it = keys.begin(); it != keys.end(); ++it)
	rmkey(prefix, *it);
    }

    /// Remove Key
    virtual void rmkey(
      const std::string &prefix,   ///< [in] Prefix to search for
      const std::string &k	      ///< [in] Key to remove
      ) = 0;

    /// Remove Single Key which exists and was not overwritten.
    /// This API is only related to performance optimization, and should only be 
    /// re-implemented by log-insert-merge tree based keyvalue stores(such as RocksDB). 
    /// If a key is overwritten (by calling set multiple times), then the result
    /// of calling rm_single_key on this key is undefined.
    virtual void rm_single_key(
      const std::string &prefix,   ///< [in] Prefix to search for
      const std::string &k	      ///< [in] Key to remove
      ) { return rmkey(prefix, k);}

    /// Removes keys beginning with prefix
    virtual void rmkeys_by_prefix(
      const std::string &prefix ///< [in] Prefix by which to remove keys
      ) = 0;

    /// Merge value into key
    virtual void merge(
      const std::string &prefix,   ///< [in] Prefix ==> MUST match some established merge operator
      const std::string &key,      ///< [in] Key to be merged
      const bufferlist  &value     ///< [in] value to be merged into key
    ) { assert(0 == "Not implemented"); }

    virtual ~TransactionImpl() {}
  };
  typedef ceph::shared_ptr< TransactionImpl > Transaction;

  /// create a new instance
  static KeyValueDB *create(CephContext *cct, const std::string& type,
			    const std::string& dir,
			    void *p = NULL);

  /// test whether we can successfully initialize; may have side effects (e.g., create)
  static int test_init(const std::string& type, const std::string& dir);
  virtual int init(string option_str="") = 0;
  virtual int open(std::ostream &out) = 0;
  virtual int create_and_open(std::ostream &out) = 0;

  virtual Transaction get_transaction() = 0;
  virtual int submit_transaction(Transaction) = 0;
  virtual int submit_transaction_sync(Transaction t) {
    return submit_transaction(t);
  }

  /// Retrieve Keys
  virtual int get(
    const std::string &prefix,        ///< [in] Prefix for key
    const std::set<std::string> &key,      ///< [in] Key to retrieve
    std::map<std::string, bufferlist> *out ///< [out] Key value retrieved
    ) = 0;
  virtual int get(const std::string &prefix, ///< [in] prefix
		  const std::string &key,    ///< [in] key
		  bufferlist *value) {  ///< [out] value
    std::set<std::string> ks;
    ks.insert(key);
    std::map<std::string,bufferlist> om;
    int r = get(prefix, ks, &om);
    if (om.find(key) != om.end()) {
      *value = om[key];
    } else {
      *value = bufferlist();
      r = -ENOENT;
    }
    return r;
  }

  class GenericIteratorImpl {
  public:
    virtual int seek_to_first() = 0;
    virtual int upper_bound(const std::string &after) = 0;
    virtual int lower_bound(const std::string &to) = 0;
    virtual bool valid() = 0;
    virtual int next(bool validate=true) = 0;
    virtual std::string key() = 0;
    virtual bufferlist value() = 0;
    virtual int status() = 0;
    virtual ~GenericIteratorImpl() {}
  };

  class WholeSpaceIteratorImpl {
  public:
    virtual int seek_to_first() = 0;
    virtual int seek_to_first(const std::string &prefix) = 0;
    virtual int seek_to_last() = 0;
    virtual int seek_to_last(const std::string &prefix) = 0;
    virtual int upper_bound(const std::string &prefix, const std::string &after) = 0;
    virtual int lower_bound(const std::string &prefix, const std::string &to) = 0;
    virtual bool valid() = 0;
    virtual int next() = 0;
    virtual int prev() = 0;
    virtual std::string key() = 0;
    virtual std::pair<std::string,std::string> raw_key() = 0;
    virtual bool raw_key_is_prefixed(const std::string &prefix) = 0;
    virtual bufferlist value() = 0;
    virtual bufferptr value_as_ptr() {
      bufferlist bl = value();
      if (bl.length()) {
        return *bl.buffers().begin();
      } else {
        return bufferptr();
      }
    }
    virtual int status() = 0;
    virtual ~WholeSpaceIteratorImpl() { }
  };
  typedef ceph::shared_ptr< WholeSpaceIteratorImpl > WholeSpaceIterator;

  class IteratorImpl : public GenericIteratorImpl {
    const std::string prefix;
    WholeSpaceIterator generic_iter;
  public:
    IteratorImpl(const std::string &prefix, WholeSpaceIterator iter) :
      prefix(prefix), generic_iter(iter) { }
    virtual ~IteratorImpl() { }

    int seek_to_first() {
      return generic_iter->seek_to_first(prefix);
    }
    int seek_to_last() {
      return generic_iter->seek_to_last(prefix);
    }
    int upper_bound(const std::string &after) {
      return generic_iter->upper_bound(prefix, after);
    }
    int lower_bound(const std::string &to) {
      return generic_iter->lower_bound(prefix, to);
    }
    bool valid() {
      if (!generic_iter->valid())
	return false;
      return generic_iter->raw_key_is_prefixed(prefix);
    }
    // Note that next() and prev() shouldn't validate iters,
    // it's responsibility of caller to ensure they're valid.
    int next(bool validate=true) {
      if (validate) {
        if (valid())
          return generic_iter->next();
        return status();
      } else {
        return generic_iter->next();  
      }      
    }
    
    int prev(bool validate=true) {
      if (validate) {
        if (valid())
          return generic_iter->prev();
        return status();
      } else {
        return generic_iter->prev();  
      }      
    }
    std::string key() {
      return generic_iter->key();
    }
    std::pair<std::string, std::string> raw_key() {
      return generic_iter->raw_key();
    }
    bufferlist value() {
      return generic_iter->value();
    }
    bufferptr value_as_ptr() {
      return generic_iter->value_as_ptr();
    }
    int status() {
      return generic_iter->status();
    }
  };

  typedef ceph::shared_ptr< IteratorImpl > Iterator;

  WholeSpaceIterator get_iterator() {
    return _get_iterator();
  }

  Iterator get_iterator(const std::string &prefix) {
    return std::make_shared<IteratorImpl>(prefix, get_iterator());
  }

  WholeSpaceIterator get_snapshot_iterator() {
    return _get_snapshot_iterator();
  }

  Iterator get_snapshot_iterator(const std::string &prefix) {
    return std::make_shared<IteratorImpl>(prefix, get_snapshot_iterator());
  }

  virtual uint64_t get_estimated_size(std::map<std::string,uint64_t> &extra) = 0;
  virtual int get_statfs(struct statfs *buf) {
    return -EOPNOTSUPP;
  }

  virtual ~KeyValueDB() {}

  /// compact the underlying store
  virtual void compact() {}

  /// compact db for all keys with a given prefix
  virtual void compact_prefix(const std::string& prefix) {}
  /// compact db for all keys with a given prefix, async
  virtual void compact_prefix_async(const std::string& prefix) {}
  virtual void compact_range(const std::string& prefix,
			     const std::string& start, const std::string& end) {}
  virtual void compact_range_async(const std::string& prefix,
				   const std::string& start, const std::string& end) {}

  // See RocksDB merge operator definition, we support the basic
  // associative merge only right now.
  class MergeOperator {
    public:
    /// Merge into a key that doesn't exist
    virtual void merge_nonexistant(
      const char *rdata, size_t rlen,
      std::string *new_value) = 0;
    /// Merge into a key that does exist
    virtual void merge(
      const char *ldata, size_t llen,
      const char *rdata, size_t rlen,
      std::string *new_value) = 0;
    /// We use each operator name and each prefix to construct the overall RocksDB operator name for consistency check at open time.
    virtual string name() const = 0;

    virtual ~MergeOperator() {}
  };

  /// Setup one or more operators, this needs to be done BEFORE the DB is opened.
  virtual int set_merge_operator(const std::string& prefix,
				 std::shared_ptr<MergeOperator> mop) {
    return -EOPNOTSUPP;
  }

protected:
  /// List of matching prefixes and merge operators
  std::vector<std::pair<std::string,
			std::shared_ptr<MergeOperator> > > merge_ops;

  virtual WholeSpaceIterator _get_iterator() = 0;
  virtual WholeSpaceIterator _get_snapshot_iterator() = 0;
};

#endif
