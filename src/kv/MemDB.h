// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * In-memory crash non-safe keyvalue db
 * Author: Ramesh Chander, Ramesh.Chander@sandisk.com
 */

#ifndef CEPH_OS_BLUESTORE_MEMDB_H
#define CEPH_OS_BLUESTORE_MEMDB_H

#include "include/buffer.h"
#include <ostream>
#include <set>
#include <map>
#include <string>
#include <memory>
#include <boost/scoped_ptr.hpp>
#include "include/common_fwd.h"
#include "include/encoding.h"
#include "include/btree_map.h"
#include "KeyValueDB.h"
#include "osd/osd_types.h"

#define KEY_DELIM '\0' 


enum {
  l_memdb_first = 34440,
  l_memdb_gets,
  l_memdb_txns,
  l_memdb_get_latency,
  l_memdb_submit_latency,
  l_memdb_last,
};

class MemDB : public KeyValueDB
{
  typedef std::pair<std::pair<std::string, std::string>, ceph::bufferlist> ms_op_t;
  std::mutex m_lock;
  uint64_t m_total_bytes;
  uint64_t m_allocated_bytes;

  typedef std::map<std::string, ceph::bufferptr> mdb_map_t;
  typedef mdb_map_t::iterator mdb_iter_t;
  bool m_using_btree;

  mdb_map_t m_map;

  CephContext *m_cct;
  PerfCounters *logger;
  void* m_priv;
  std::string m_options;
  std::string m_db_path;

  int transaction_rollback(KeyValueDB::Transaction t);
  int _open(std::ostream &out);
  void close() override;
  bool _get(const std::string &prefix, const std::string &k, ceph::bufferlist *out);
  bool _get_locked(const std::string &prefix, const std::string &k, ceph::bufferlist *out);
  std::string _get_data_fn();
  void _encode(mdb_iter_t iter, ceph::bufferlist &bl);
  void _save();
  int _load();
  uint64_t iterator_seq_no;

public:
  MemDB(CephContext *c, const std::string &path, void *p) :
    m_total_bytes(0), m_allocated_bytes(0), m_using_btree(false),
    m_cct(c), logger(NULL), m_priv(p), m_db_path(path), iterator_seq_no(1)
  {
    //Nothing as of now
  }

  ~MemDB() override;
  int set_merge_operator(const std::string& prefix,
         std::shared_ptr<MergeOperator> mop) override;

  std::shared_ptr<MergeOperator> _find_merge_op(const std::string &prefix);

  static
  int _test_init(const std::string& dir) { return 0; };

  class MDBTransactionImpl : public KeyValueDB::TransactionImpl {
    public:
      enum op_type { WRITE = 1, MERGE = 2, DELETE = 3};
    private:

      std::vector<std::pair<op_type, ms_op_t>> ops;
      MemDB *m_db;

    bool key_is_prefixed(const std::string &prefix, const std::string& full_key);
    public:
      const std::vector<std::pair<op_type, ms_op_t>>&
        get_ops() { return ops; };

    void set(const std::string &prefix, const std::string &key,
	     const ceph::bufferlist &val) override;
    using KeyValueDB::TransactionImpl::set;
    void rmkey(const std::string &prefix, const std::string &k) override;
    using KeyValueDB::TransactionImpl::rmkey;
    void rmkeys_by_prefix(const std::string &prefix) override;
    void rm_range_keys(
      const std::string &prefix,
      const std::string &start,
      const std::string &end) override;

    void merge(const std::string &prefix, const std::string &key,
	       const ceph::bufferlist  &value) override;
    void clear() {
      ops.clear();
    }
    explicit MDBTransactionImpl(MemDB* _db) :m_db(_db)
    {
      ops.clear();
    }
    ~MDBTransactionImpl() override {};
  };

private:

  /*
   * Transaction states.
   */
  int _merge(const std::string &k, ceph::bufferptr &bl);
  int _merge(ms_op_t &op);
  int _setkey(ms_op_t &op);
  int _rmkey(ms_op_t &op);

public:

  int init(std::string option_str="") override { m_options = option_str; return 0; }
  int _init(bool format);

  int do_open(std::ostream &out, bool create);
  int open(std::ostream &out, const std::string& cfs="") override;
  int create_and_open(std::ostream &out, const std::string& cfs="") override;
  using KeyValueDB::create_and_open;

  KeyValueDB::Transaction get_transaction() override {
    return std::shared_ptr<MDBTransactionImpl>(new MDBTransactionImpl(this));
  }

  int submit_transaction(Transaction) override;
  int submit_transaction_sync(Transaction) override;

  int get(const std::string &prefix, const std::set<std::string> &key,
	  std::map<std::string, ceph::bufferlist> *out) override;

  int get(const std::string &prefix, const std::string &key,
          ceph::bufferlist *out) override;

  using KeyValueDB::get;

  class MDBWholeSpaceIteratorImpl : public KeyValueDB::WholeSpaceIteratorImpl {

      mdb_iter_t m_iter;
      std::pair<std::string, ceph::bufferlist> m_key_value;
      mdb_map_t *m_map_p;
      std::mutex *m_map_lock_p;
      uint64_t *global_seq_no;
      uint64_t this_seq_no;
      bool m_using_btree;

  public:
    MDBWholeSpaceIteratorImpl(mdb_map_t *btree_p, std::mutex *btree_lock_p,
                              uint64_t *iterator_seq_no, bool using_btree) {
      m_map_p = btree_p;
      m_map_lock_p = btree_lock_p;
      std::lock_guard<std::mutex> l(*m_map_lock_p);
      global_seq_no = iterator_seq_no;
      this_seq_no = *iterator_seq_no;
      m_using_btree = using_btree;
    }

    void fill_current();
    void free_last();


    int seek_to_first(const std::string &k) override;
    int seek_to_last(const std::string &k) override;

    int seek_to_first() override { return seek_to_first(std::string()); };
    int seek_to_last() override { return seek_to_last(std::string()); };

    int upper_bound(const std::string &prefix, const std::string &after) override;
    int lower_bound(const std::string &prefix, const std::string &to) override;
    bool valid() override;
    bool iterator_validate();

    int next() override;
    int prev() override;
    int status() override { return 0; };

    std::string key() override;
    std::pair<std::string,std::string> raw_key() override;
    bool raw_key_is_prefixed(const std::string &prefix) override;
    ceph::bufferlist value() override;
    ~MDBWholeSpaceIteratorImpl() override;
  };

  uint64_t get_estimated_size(std::map<std::string,uint64_t> &extra) override {
      std::lock_guard<std::mutex> l(m_lock);
      return m_allocated_bytes;
  };

  int get_statfs(struct store_statfs_t *buf) override {
    std::lock_guard<std::mutex> l(m_lock);
    buf->reset();
    buf->total = m_total_bytes;
    buf->allocated = m_allocated_bytes;
    buf->data_stored = m_total_bytes;
    return 0;
  }

  WholeSpaceIterator get_wholespace_iterator(IteratorOpts opts = 0) override {
    return std::shared_ptr<KeyValueDB::WholeSpaceIteratorImpl>(
      new MDBWholeSpaceIteratorImpl(&m_map, &m_lock, &iterator_seq_no, m_using_btree));
  }
};

#endif

