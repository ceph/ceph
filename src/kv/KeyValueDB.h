// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef KEY_VALUE_DB_H
#define KEY_VALUE_DB_H

#include "include/buffer.h"
#include <ostream>
#include <set>
#include <map>
#include <optional>
#include <string>
#include <boost/scoped_ptr.hpp>
#include "include/encoding.h"
#include "common/Formatter.h"
#include "common/perf_counters.h"
#include "common/PriorityCache.h"

/**
 * Defines virtual interface to be implemented by key value store
 *
 * Kyoto Cabinet should implement this
 */
class KeyValueDB {
public:
  class TransactionImpl {
  public:
    /// Set Keys
    void set(
      const std::string &prefix,                      ///< [in] Prefix for keys, or CF name
      const std::map<std::string, ceph::buffer::list> &to_set ///< [in] keys/values to set
    ) {
      for (auto it = to_set.cbegin(); it != to_set.cend(); ++it)
	set(prefix, it->first, it->second);
    }

    /// Set Keys (via encoded ceph::buffer::list)
    void set(
      const std::string &prefix,      ///< [in] prefix, or CF name
      ceph::buffer::list& to_set_bl           ///< [in] encoded key/values to set
      ) {
      using ceph::decode;
      auto p = std::cbegin(to_set_bl);
      uint32_t num;
      decode(num, p);
      while (num--) {
	std::string key;
	ceph::buffer::list value;
	decode(key, p);
	decode(value, p);
	set(prefix, key, value);
      }
    }

    /// Set Key
    virtual void set(
      const std::string &prefix,      ///< [in] Prefix or CF for the key
      const std::string &k,	      ///< [in] Key to set
      const ceph::buffer::list &bl            ///< [in] Value to set
      ) = 0;
    virtual void set(
      const std::string &prefix,
      const char *k,
      size_t keylen,
      const ceph::buffer::list& bl) {
      set(prefix, std::string(k, keylen), bl);
    }

    /// Removes Keys (via encoded ceph::buffer::list)
    void rmkeys(
      const std::string &prefix,     ///< [in] Prefix or CF to search for
      ceph::buffer::list &keys_bl            ///< [in] Keys to remove
    ) {
      using ceph::decode;
      auto p = std::cbegin(keys_bl);
      uint32_t num;
      decode(num, p);
      while (num--) {
	std::string key;
	decode(key, p);
	rmkey(prefix, key);
      }
    }

    /// Removes Keys
    void rmkeys(
      const std::string &prefix,        ///< [in] Prefix/CF to search for
      const std::set<std::string> &keys ///< [in] Keys to remove
    ) {
      for (auto it = keys.cbegin(); it != keys.cend(); ++it)
	rmkey(prefix, *it);
    }

    /// Remove Key
    virtual void rmkey(
      const std::string &prefix,       ///< [in] Prefix/CF to search for
      const std::string &k	       ///< [in] Key to remove
      ) = 0;
    virtual void rmkey(
      const std::string &prefix,   ///< [in] Prefix to search for
      const char *k,	      ///< [in] Key to remove
      size_t keylen
      ) {
      rmkey(prefix, std::string(k, keylen));
    }

    /// Remove Single Key which exists and was not overwritten.
    /// This API is only related to performance optimization, and should only be
    /// re-implemented by log-insert-merge tree based keyvalue stores(such as RocksDB).
    /// If a key is overwritten (by calling set multiple times), then the result
    /// of calling rm_single_key on this key is undefined.
    virtual void rm_single_key(
      const std::string &prefix,      ///< [in] Prefix/CF to search for
      const std::string &k	      ///< [in] Key to remove
      ) { return rmkey(prefix, k);}

    /// Removes keys beginning with prefix
    virtual void rmkeys_by_prefix(
      const std::string &prefix       ///< [in] Prefix/CF by which to remove keys
      ) = 0;

    virtual void rm_range_keys(
      const std::string &prefix,    ///< [in] Prefix by which to remove keys
      const std::string &start,     ///< [in] The start bound of remove keys
      const std::string &end        ///< [in] The start bound of remove keys
      ) = 0;

    /// Merge value into key
    virtual void merge(
      const std::string &prefix,   ///< [in] Prefix/CF ==> MUST match some established merge operator
      const std::string &key,      ///< [in] Key to be merged
      const ceph::buffer::list  &value     ///< [in] value to be merged into key
    ) { ceph_abort_msg("Not implemented"); }

    virtual ~TransactionImpl() {}
  };
  typedef std::shared_ptr< TransactionImpl > Transaction;

  /// create a new instance
  static KeyValueDB *create(CephContext *cct, const std::string& type,
			    const std::string& dir,
			    std::map<std::string,std::string> options = {},
			    void *p = NULL);

  /// test whether we can successfully initialize; may have side effects (e.g., create)
  static int test_init(const std::string& type, const std::string& dir);
  virtual int init(std::string option_str="") = 0;
  virtual int open(std::ostream &out, const std::string& cfs="") = 0;
  // std::vector cfs contains column families to be created when db is created.
  virtual int create_and_open(std::ostream &out, const std::string& cfs="") = 0;

  virtual int open_read_only(std::ostream &out, const std::string& cfs="") {
    return -ENOTSUP;
  }

  virtual void close() { }

  /// Try to repair K/V database. rocksdb requires that database must be not opened.
  virtual int repair(std::ostream &out) { return 0; }

  virtual Transaction get_transaction() = 0;
  virtual int submit_transaction(Transaction) = 0;
  virtual int submit_transaction_sync(Transaction t) {
    return submit_transaction(t);
  }

  /// Retrieve Keys
  virtual int get(
    const std::string &prefix,               ///< [in] Prefix/CF for key
    const std::set<std::string> &key,        ///< [in] Key to retrieve
    std::map<std::string, ceph::buffer::list> *out   ///< [out] Key value retrieved
    ) = 0;
  virtual int get(const std::string &prefix, ///< [in] prefix or CF name
		  const std::string &key,    ///< [in] key
		  ceph::buffer::list *value) {       ///< [out] value
    std::set<std::string> ks;
    ks.insert(key);
    std::map<std::string,ceph::buffer::list> om;
    int r = get(prefix, ks, &om);
    if (om.find(key) != om.end()) {
      *value = std::move(om[key]);
    } else {
      *value = ceph::buffer::list();
      r = -ENOENT;
    }
    return r;
  }
  virtual int get(const std::string &prefix,
		  const char *key, size_t keylen,
		  ceph::buffer::list *value) {
    return get(prefix, std::string(key, keylen), value);
  }

  // This superclass is used both by kv iterators *and* by the ObjectMap
  // omap iterator.  The class hierarchies are unfortunately tied together
  // by the legacy DBOjectMap implementation :(.
  class SimplestIteratorImpl {
  public:
    virtual int seek_to_first() = 0;
    virtual int upper_bound(const std::string &after) = 0;
    virtual int lower_bound(const std::string &to) = 0;
    virtual bool valid() = 0;
    virtual int next() = 0;
    virtual std::string key() = 0;
    virtual std::string tail_key() {
      return "";
    }
    virtual ceph::buffer::list value() = 0;
    virtual int status() = 0;
    virtual ~SimplestIteratorImpl() {}
  };

  class IteratorImpl : public SimplestIteratorImpl {
  public:
    virtual ~IteratorImpl() {}
    virtual int seek_to_last() = 0;
    virtual int prev() = 0;
    virtual std::pair<std::string, std::string> raw_key() = 0;
    virtual ceph::buffer::ptr value_as_ptr() {
      ceph::buffer::list bl = value();
      if (bl.length() == 1) {
        return *bl.buffers().begin();
      } else if (bl.length() == 0) {
        return ceph::buffer::ptr();
      } else {
	ceph_abort();
      }
    }
  };
  typedef std::shared_ptr< IteratorImpl > Iterator;

  // This is the low-level iterator implemented by the underlying KV store.
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
    virtual ceph::buffer::list value() = 0;
    virtual ceph::buffer::ptr value_as_ptr() {
      ceph::buffer::list bl = value();
      if (bl.length()) {
        return *bl.buffers().begin();
      } else {
        return ceph::buffer::ptr();
      }
    }
    virtual int status() = 0;
    virtual size_t key_size() {
      return 0;
    }
    virtual size_t value_size() {
      return 0;
    }
    virtual ~WholeSpaceIteratorImpl() { }
  };
  typedef std::shared_ptr< WholeSpaceIteratorImpl > WholeSpaceIterator;

private:
  // This class filters a WholeSpaceIterator by a prefix.
  // Performs as a dummy wrapper over WholeSpaceIterator
  // if prefix is empty
  class PrefixIteratorImpl : public IteratorImpl {
    const std::string prefix;
    WholeSpaceIterator generic_iter;
  public:
    PrefixIteratorImpl(const std::string &prefix, WholeSpaceIterator iter) :
      prefix(prefix), generic_iter(iter) { }
    ~PrefixIteratorImpl() override { }

    int seek_to_first() override {
      return prefix.empty() ?
	generic_iter->seek_to_first() :
	generic_iter->seek_to_first(prefix);
    }
    int seek_to_last() override {
      return prefix.empty() ?
	generic_iter->seek_to_last() :
	generic_iter->seek_to_last(prefix);
    }
    int upper_bound(const std::string &after) override {
      return generic_iter->upper_bound(prefix, after);
    }
    int lower_bound(const std::string &to) override {
      return generic_iter->lower_bound(prefix, to);
    }
    bool valid() override {
      if (!generic_iter->valid())
	return false;
      if (prefix.empty())
        return true;
      return prefix.empty() ?
	      true :
	      generic_iter->raw_key_is_prefixed(prefix);
    }
    int next() override {
      return generic_iter->next();
    }
    int prev() override {
      return generic_iter->prev();
    }
    std::string key() override {
      return generic_iter->key();
    }
    std::pair<std::string, std::string> raw_key() override {
      return generic_iter->raw_key();
    }
    ceph::buffer::list value() override {
      return generic_iter->value();
    }
    ceph::buffer::ptr value_as_ptr() override {
      return generic_iter->value_as_ptr();
    }
    int status() override {
      return generic_iter->status();
    }
  };
protected:
  Iterator make_iterator(const std::string &prefix, WholeSpaceIterator w_iter) {
    return std::make_shared<PrefixIteratorImpl>(
      prefix,
      w_iter);
  }
public:
  typedef uint32_t IteratorOpts;
  static const uint32_t ITERATOR_NOCACHE = 1;

  struct IteratorBounds {
    std::optional<std::string> lower_bound;
    std::optional<std::string> upper_bound;
  };

  virtual WholeSpaceIterator get_wholespace_iterator(IteratorOpts opts = 0) = 0;
  virtual Iterator get_iterator(const std::string &prefix, IteratorOpts opts = 0, IteratorBounds bounds = IteratorBounds()) {
    return make_iterator(prefix,
      get_wholespace_iterator(opts));
  }

  virtual uint64_t get_estimated_size(std::map<std::string,uint64_t> &extra) = 0;
  virtual int get_statfs(struct store_statfs_t *buf) {
    return -EOPNOTSUPP;
  }

  virtual int set_cache_size(uint64_t) {
    return -EOPNOTSUPP;
  }

  virtual int set_cache_high_pri_pool_ratio(double ratio) {
    return -EOPNOTSUPP;
  }

  virtual int64_t get_cache_usage() const {
    return -EOPNOTSUPP;
  }

  virtual int64_t get_cache_usage(std::string prefix) const {
    return -EOPNOTSUPP;
  }

  virtual std::shared_ptr<PriorityCache::PriCache> get_priority_cache() const {
    return nullptr;
  }

  virtual std::shared_ptr<PriorityCache::PriCache> get_priority_cache(std::string prefix) const {
    return nullptr;
  }



  virtual ~KeyValueDB() {}

  /// estimate space utilization for a prefix (in bytes)
  virtual int64_t estimate_prefix_size(const std::string& prefix,
				       const std::string& key_prefix) {
    return 0;
  }

  /// creates a kv database backup in directory path. Returns true on success
  virtual bool backup(const std::string& path) { return false; }

  /// restore from backup the specified backup version
  static bool restore_backup(CephContext *cct, const std::string &type, const std::string &path, const std::string &backup_location, const std::string& version);

  /// compact the underlying store
  virtual void compact() {}

  /// compact the underlying store in async mode
  virtual void compact_async() {}

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
    virtual void merge_nonexistent(
      const char *rdata, size_t rlen,
      std::string *new_value) = 0;
    /// Merge into a key that does exist
    virtual void merge(
      const char *ldata, size_t llen,
      const char *rdata, size_t rlen,
      std::string *new_value) = 0;
    /// We use each operator name and each prefix to construct the overall RocksDB operator name for consistency check at open time.
    virtual const char *name() const = 0;

    virtual ~MergeOperator() {}
  };

  /// Setup one or more operators, this needs to be done BEFORE the DB is opened.
  virtual int set_merge_operator(const std::string& prefix,
				 std::shared_ptr<MergeOperator> mop) {
    return -EOPNOTSUPP;
  }

  virtual void get_statistics(ceph::Formatter *f) {
    return;
  }

  /**
   * Return your perf counters if you have any.  Subclasses are not
   * required to implement this, and callers must respect a null return
   * value.
   */
  virtual PerfCounters *get_perf_counters() {
    return nullptr;
  }

  /**
   * Access implementation specific integral property corresponding
   * to passed property and prefic.
   * Return value is true if property is valid for prefix, populates out.
   */
  virtual bool get_property(
    const std::string &property,
    uint64_t *out) {
    return false;
  }
protected:
  /// List of matching prefixes/ColumnFamilies and merge operators
  std::vector<std::pair<std::string,
			std::shared_ptr<MergeOperator> > > merge_ops;

};

#endif
