// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef ZETA_SCALE_DB_H
#define ZETA_SCALE_DB_H

#include <boost/scoped_ptr.hpp>
#include <map>
#include <memory>
#include <ostream>
#include <set>
#include <string>

#include "KeyValueDB.h"
#include "include/buffer.h"
#include "include/encoding.h"
#include "include/memory.h"

#include "zs/api/zs.h"

using std::string;

#define MAX_CHUNK_COUNT 32

#define CEPH_ZS_CONTAINER_NAME "ceph_kv_store"
#define CEPH_ZS_LOG_CONTAINER_NAME "ceph_kv_log_store"

#define ZS_MINIMUM_DB_SIZE (64L * 1024 * 1024 * 1024)

class ZSStore : public KeyValueDB
{
  const char *default_cache_size = "1073741824";  // 1Gb

  CephContext *cct;
  void *priv;

  string db_path;
  string options;
  int dev_log_fd, dev_data_fd;

  ZS_cguid_t cguid, cguid_lc;

  int transaction_start();
  int transaction_commit();
  int transaction_rollback();
  int transaction_submit();

  int set_merge_operator(const std::string& prefix,
      std::shared_ptr<MergeOperator> mop) {
    merge_ops.push_back(std::make_pair(prefix,mop));
    return 0;
  }

  int _open(std::ostream &out);
  void SetPropertiesFromString(const string &opt_str);
  void close();
  int _get(const string &k, bufferlist *out);
  bool key_is_prefixed(const string &prefix, const string &full_key);

public:
  ZSStore(CephContext *c, const string &path, void *p)
      : cct(c),
	priv(p),
	db_path(path),
	dev_log_fd(-1),
	dev_data_fd(-1)
  {
  }

  ~ZSStore();

  static int _test_init(const string &dir) { return 0; };

  class ZSTransactionImpl : public KeyValueDB::TransactionImpl
  {
public:
    enum op_type { WRITE, DELETE, MERGE };

private:
    ZS_cguid_t cguid, cguid_lc;
    ZSStore *db;
    std::vector<std::pair<op_type, std::pair<std::string, bufferlist>>> ops;
    std::string pg_log_key;

public:
    const std::vector<std::pair<op_type, std::pair<std::string, bufferlist>>>
	&get_ops()
    {
	if(pg_log_key.length())
	    ops.push_back(make_pair(
				    DELETE, make_pair(pg_log_key, bufferlist())));
      return ops;
    };

    void set(const std::string &prefix, const std::string &key,
	     const bufferlist &val);
    void rmkey(const std::string &prefix, const std::string &k);
    void rmkeys_by_prefix(const std::string &prefix);

    void merge( const std::string &prefix, const std::string &key,
	const bufferlist  &value);

    ZSTransactionImpl(ZS_cguid_t _cguid, ZS_cguid_t _cguid_lc, ZSStore *_db)
	: cguid(_cguid), cguid_lc(_cguid_lc), db(_db){};
    ~ZSTransactionImpl(){};
  };

private:
  struct zs_map_comparator {
    /* BlueStore keys contain binary data and std::string is signed char, so
     * standard comparator only works for equality, not for < or > relation. */

    bool operator()(const std::string &a, const std::string &b) const
    {
      int len = a.length() < b.length() ? a.length() : b.length();

      int r = memcmp(a.c_str(), b.c_str(), len);

      if (!r)
	return a.length() < b.length();

      return r < 0;
    }
  };

  typedef std::map<std::string, bufferlist, zs_map_comparator> ZSMultiMap;

  ZSMultiMap write_ops;
  std::set<std::string> delete_ops;
  int _batch_set(const ZSMultiMap &ops);
  int _rmkey(const std::string &k);
  int _submit_transaction_sync(Transaction);
  bufferlist _merge(const std::string &key, const bufferlist &base,
      const bufferlist &value);

public:
  int init(string option_str = "")
  {
    options = option_str;
    return 0;
  }
  int _init(bool format);

  int do_open(std::ostream &out, int create);
  int open(std::ostream &out) { return do_open(out, 0); }
  int create_and_open(std::ostream &out)
  {
    return do_open(out, ZS_CTNR_CREATE);
  }

  KeyValueDB::Transaction get_transaction()
  {
    return std::shared_ptr<ZSTransactionImpl>(
	new ZSTransactionImpl(cguid, cguid_lc, this));
  }

  int submit_transaction(Transaction);
  int submit_transaction_sync(Transaction);

  int get(const std::string &prefix, const std::set<std::string> &key,
	  std::map<std::string, bufferlist> *out);

  class ZSWholeSpaceIteratorImpl : public KeyValueDB::WholeSpaceIteratorImpl
  {
    ZSStore *store;
    ZS_cguid_t cguid, cguid_lc;
    uint64_t snap_seqno;
    ZS_range_meta_t meta;
    ZS_range_data_t values[MAX_CHUNK_COUNT];
    int count;
    struct ZS_cursor *cursor;
    struct ZS_iterator *lc_it;
    bool direction;
    bufferlist value_bl;
    std::string cur_key;

    void invalidate()
    {
      for (int i = 0; i < count; i++) {
	free(values[i].key);
	free(values[i].data);
      }
      cur_key.clear();
      count = 0;
    }

    int split_key(const char *pkey, uint32_t pkey_len, string *prefix,
		  string *key);
    int seek(const char *start, uint32_t length, bool direction,
	     bool inclusive);
    void finish();

public:
    ZSWholeSpaceIteratorImpl(ZSStore *s, ZS_cguid_t _cguid,
			     ZS_cguid_t _cguid_lc, uint64_t ss)
	: store(s),
	  cguid(_cguid),
	  cguid_lc(_cguid_lc),
	  snap_seqno(ss),
	  cursor(NULL),
	  lc_it(NULL),
	  direction(true)
    {
      memset(&meta, 0, sizeof(meta));
      memset(&values, 0, sizeof(values));
      count = 0;
    }
    int seek_to_first(const std::string &k)
    {
      return seek(k.c_str(), k.length(), true, true);
    };
    int seek_to_last(const std::string &k)
    {
      if (!k.length())
	return seek(k.c_str(), k.length(), false, true);
      string kk(k);
      kk.append("\255");
      return seek(kk.c_str(), kk.length(), false, true);
    };
    int seek_to_first() { return seek(NULL, 0, true, true); };
    int seek_to_last() { return seek(NULL, 0, false, true); };
    int upper_bound(const std::string &prefix, const std::string &after);
    int lower_bound(const std::string &prefix, const std::string &to);
    bool valid() { return cur_key.length(); };
    int _next();
    int change_direction()
    {
      string k(cur_key);
      int r = seek(k.c_str(), k.length(), !direction, false);
      if (!r)
	r = _next();
      return r;
    }
    int next() { return direction ? _next() : change_direction(); };
    int prev() { return !direction ? _next() : change_direction(); };
    std::string key();
    std::pair<std::string, std::string> raw_key();
    bool raw_key_is_prefixed(const std::string &prefix);
    bufferlist value();
    int status() { return 0; };
    ~ZSWholeSpaceIteratorImpl();
  };

  uint64_t get_estimated_size(std::map<std::string, uint64_t> &extra)
  {
    // TODO: Implement
    return 0;
  };

  int get_statfs(store_statfs_t *buf) { return -EOPNOTSUPP; }
protected:
  WholeSpaceIterator _get_iterator()
  {
    return std::shared_ptr<KeyValueDB::WholeSpaceIteratorImpl>(
	new ZSWholeSpaceIteratorImpl(this, cguid, cguid_lc, 0));
  }

  WholeSpaceIterator _get_snapshot_iterator();
};

#endif
