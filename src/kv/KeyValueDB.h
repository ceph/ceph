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
  /*
   *  See RocksDB's definition of a column family(CF) and how to use it.
   *  The interfaces of KeyValueDB is extended, when a column family is created.
   *  Prefix will be the name of column family to use.
   */
  struct ColumnFamily {
    string name;      //< name of this individual column family
    string option;    //< configure option string for this CF
    ColumnFamily(const string &name, const string &option)
      : name(name), option(option) {}
  };

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

    /// Removes keys beginning with prefix
    virtual void rmkeys_by_prefix(
      const std::string &prefix ///< [in] Prefix by which to remove keys
      ) = 0;

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
  virtual int open(std::ostream &out, const std::vector<ColumnFamily>& cfs) {
    assert(0 == "Not implemented"); }
  virtual int create_and_open(std::ostream &out) = 0;
  // vector cfs contains column families to be created when db is created.
  virtual int create_and_open(std::ostream &out,
			      const std::vector<ColumnFamily>& cfs) {
    assert(0 == "Not implemented"); }

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

  /// When Column Family is used, WholeSpaceIterator is limited to the default column family,
  /// and user can still use multiple prefixes within the default CF.
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

  /// Iterator for column family which is created explicitly and not the default one.
  class ColumnFamilyIteratorImpl {
  public:
    virtual int seek_to_first() = 0;
    virtual int seek_to_last() = 0;
    virtual int upper_bound(const std::string &after) = 0;
    virtual int lower_bound(const std::string &to) = 0;
    virtual bool valid() = 0;
    virtual int next() = 0;
    virtual int prev() = 0;
    virtual std::string key() = 0;
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
    virtual ~ColumnFamilyIteratorImpl() { }
  };
  typedef ceph::shared_ptr< ColumnFamilyIteratorImpl > ColumnFamilyIterator;

  class IteratorImpl : public GenericIteratorImpl {
    const std::string prefix;
    WholeSpaceIterator generic_iter;
    ColumnFamilyIterator cf_iter;
  public:
    const bool is_cf;                       //is it for a explicit column family
    IteratorImpl(const std::string &prefix, WholeSpaceIterator iter) :
      prefix(prefix), generic_iter(iter), is_cf(false) { }
    IteratorImpl(const std::string &cf_name, ColumnFamilyIterator iter) :
      prefix(cf_name), cf_iter(iter), is_cf(true) { }
    virtual ~IteratorImpl() { }

     int seek_to_first() {
      return is_cf ? cf_iter->seek_to_first() :
                     generic_iter->seek_to_first(prefix);
     }
     int seek_to_last() {
      return is_cf ? cf_iter->seek_to_last() :
                     generic_iter->seek_to_last(prefix);
     }
     int upper_bound(const std::string &after) {
      return is_cf ? cf_iter->upper_bound(after) :
                     generic_iter->upper_bound(prefix, after);
     }
     int lower_bound(const std::string &to) {
      return is_cf ? cf_iter->lower_bound(to) :
                     generic_iter->lower_bound(prefix, to);
     }
    bool valid() {
      if (is_cf) {
	return cf_iter->valid();
      } else {
        if (!generic_iter->valid())
	  return false;
        return generic_iter->raw_key_is_prefixed(prefix);
      }
    }
    // Note that next() and prev() shouldn't validate iters,
    // it's responsibility of caller to ensure they're valid.
    int next(bool validate=true) {
      if (validate) {
        if (valid())
	  return is_cf ? cf_iter->next() : generic_iter->next();
	return status();
      } else {
	return is_cf ? cf_iter->next() : generic_iter->next();  
      }      
    }
    
    int prev(bool validate=true) {
      if (validate) {
        if (valid())
          return is_cf ? cf_iter->prev() : generic_iter->prev();
        return status();
      } else {
	return is_cf ? cf_iter->prev() : generic_iter->prev();
      }      
    }
    std::string key() {
      return is_cf ? cf_iter->key() : generic_iter->key();
    }
    // For column family, keys actually don't contain prefix;
    // to make interface compatible, prefix is added.
    std::pair<std::string, std::string> raw_key() {
      return is_cf ? make_pair(prefix, cf_iter->key()) : generic_iter->raw_key();
    }
    bufferlist value() {
      return is_cf ? cf_iter->value() : generic_iter->value();
    }
    bufferptr value_as_ptr() {
      return is_cf ? cf_iter->value_as_ptr() : generic_iter->value_as_ptr();
    }
    int status() {
      return is_cf ? cf_iter->status() : generic_iter->status();
    }
  };

  typedef ceph::shared_ptr< IteratorImpl > Iterator;

  WholeSpaceIterator get_iterator() {
    // a whole-space iterator cannot be used in combination with
    // column families because it is not smart enough to traverse
    // across all of them.
    assert(cf_handles.empty());
    return _get_iterator();
  }

  ColumnFamilyIterator get_cf_iterator(const std::string& cf_name) {
    return _get_cf_iterator(cf_name);
  }

  Iterator get_iterator(const std::string &prefix) {
    return is_column_family(prefix) ?
             std::make_shared<IteratorImpl>(prefix, get_cf_iterator(prefix)) :
             std::make_shared<IteratorImpl>(prefix, _get_iterator());
  }

  WholeSpaceIterator get_snapshot_iterator() {
    return _get_snapshot_iterator();
  }

  ColumnFamilyIterator get_cf_snapshot_iterator(const std::string& cf_name) {
    return _get_cf_snapshot_iterator(cf_name);
  }

  Iterator get_snapshot_iterator(const std::string &prefix) {
    return is_column_family(prefix) ?
             std::make_shared<IteratorImpl>(prefix, get_cf_snapshot_iterator(prefix)) :
             std::make_shared<IteratorImpl>(prefix, get_snapshot_iterator());
  }

  void add_column_family(const std::string& cf_name, void *handle) {
    cf_handles.insert(std::make_pair(cf_name, handle));
  }

  void *get_cf_handle(const std::string& cf_name) {
    std::unordered_map<std::string, void*>::const_iterator iter;
    iter = cf_handles.find(cf_name);
    if (iter == cf_handles.end())
      return nullptr;
    else
      return iter->second;
  }

  bool is_column_family(const std::string& prefix) {
    return cf_handles.count(prefix); 
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

protected:
  /// column families in use, name->handle
  std::unordered_map<std::string, void *> cf_handles;

  virtual WholeSpaceIterator _get_iterator() = 0;
  virtual WholeSpaceIterator _get_snapshot_iterator() = 0;
  virtual ColumnFamilyIterator _get_cf_iterator(const std::string& cf_name) {
    assert(0 == "Not implemented"); }
  virtual ColumnFamilyIterator _get_cf_snapshot_iterator(const std::string& cf_name) {
    assert(0 == "Not implemented"); }
};

#endif
