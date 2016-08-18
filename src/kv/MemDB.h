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
#include "include/memory.h"
#include <boost/scoped_ptr.hpp>
#include "include/encoding.h"
#include "include/cpp-btree/btree.h"
#include "include/cpp-btree/btree_map.h"
#include "include/encoding_btree.h"
#include "KeyValueDB.h"
#include "osd/osd_types.h"

using std::string;
#define KEY_DELIM '\0' 

class MemDB : public KeyValueDB
{
  typedef std::pair<std::pair<std::string, std::string>, bufferlist> ms_op_t;
  std::mutex m_lock;
  uint64_t m_total_bytes;
  uint64_t m_allocated_bytes;

  btree::btree_map<std::string, bufferptr> m_btree;
  CephContext *m_cct;
  void* m_priv;
  string m_options;
  string m_db_path;

  int transaction_rollback(KeyValueDB::Transaction t);
  int _open(ostream &out);
  void close();
  bool _get(const string &prefix, const string &k, bufferlist *out);
  bool _get_locked(const string &prefix, const string &k, bufferlist *out);
  std::string _get_data_fn();
  void _encode(btree::btree_map<string, bufferptr>:: iterator iter, bufferlist &bl);
  void _save();
  int _load();
  uint64_t iterator_seq_no;

public:
  MemDB(CephContext *c, const string &path, void *p) :
    m_cct(c), m_priv(p), m_db_path(path), iterator_seq_no(1)
  {
    //Nothing as of now
  }

  ~MemDB();
  virtual int set_merge_operator(const std::string& prefix,
         std::shared_ptr<MergeOperator> mop);

  std::shared_ptr<MergeOperator> _find_merge_op(std::string prefix);

  static
  int _test_init(const string& dir) { return 0; };

  class MDBTransactionImpl : public KeyValueDB::TransactionImpl {
    public:
      enum op_type { WRITE = 1, MERGE = 2, DELETE = 3};
    private:

      std::vector<std::pair<op_type, ms_op_t>> ops;
      MemDB *m_db;

      bool key_is_prefixed(const string &prefix, const string& full_key);
    public:
      const std::vector<std::pair<op_type, ms_op_t>>&
        get_ops() { return ops; };

    void set(const std::string &prefix, const std::string &key,
      const bufferlist &val);
    void rmkey(const std::string &prefix, const std::string &k);
    void rmkeys_by_prefix(const std::string &prefix);

    void merge(const std::string &prefix, const std::string &key, const bufferlist  &value);
    void clear() {
      ops.clear();
    }
    MDBTransactionImpl(MemDB* _db) :m_db(_db)
    {
      ops.clear();
    }
    ~MDBTransactionImpl() {};
  };

private:

  /*
   * Transaction states.
   */
  int _merge(const std::string &k, bufferptr &bl);
  int _merge(ms_op_t &op);
  int _setkey(ms_op_t &op);
  int _rmkey(ms_op_t &op);

public:

  int init(string option_str="") { m_options = option_str; return 0; }
  int _init(bool format);

  int do_open(ostream &out, bool create);
  int open(ostream &out) { return do_open(out, false); }
  int create_and_open(ostream &out) { return do_open(out, true); }

  KeyValueDB::Transaction get_transaction() {
    return std::shared_ptr<MDBTransactionImpl>(new MDBTransactionImpl(this));
  }

  int submit_transaction(Transaction);
  int submit_transaction_sync(Transaction);

  int get(const std::string &prefix, const std::set<std::string> &key,
    std::map<std::string, bufferlist> *out);

  int get(const std::string &prefix, const std::string &key,
          bufferlist *out) override;

  class MDBWholeSpaceIteratorImpl : public KeyValueDB::WholeSpaceIteratorImpl {

      btree::btree_map<string, bufferptr>::iterator m_iter;
      std::pair<string, bufferlist> m_key_value;
      btree::btree_map<std::string, bufferptr> *m_btree_p;
      std::mutex *m_btree_lock_p;
      uint64_t *global_seq_no;
      uint64_t this_seq_no;

  public:
    MDBWholeSpaceIteratorImpl(btree::btree_map<std::string, bufferptr> *btree_p,
                             std::mutex *btree_lock_p, uint64_t *iterator_seq_no) {
        m_btree_p = btree_p;
        m_btree_lock_p = btree_lock_p;
	std::lock_guard<std::mutex> l(*m_btree_lock_p);
	global_seq_no = iterator_seq_no;
	this_seq_no = *iterator_seq_no;
    }

    void fill_current();
    void free_last();


    int seek_to_first(const std::string &k);
    int seek_to_last(const std::string &k);

    int seek_to_first() { return seek_to_first(std::string()); };
    int seek_to_last() { return seek_to_last(std::string()); };

    int upper_bound(const std::string &prefix, const std::string &after);
    int lower_bound(const std::string &prefix, const std::string &to);
    bool valid();
    bool iterator_validate();

    int next();
    int prev();
    int status() { return 0; };

    std::string key();
    std::pair<std::string,std::string> raw_key();
    bool raw_key_is_prefixed(const std::string &prefix);
    bufferlist value();
    ~MDBWholeSpaceIteratorImpl();
  };

  uint64_t get_estimated_size(std::map<std::string,uint64_t> &extra) {
      std::lock_guard<std::mutex> l(m_lock);
      return m_allocated_bytes;
  };

  int get_statfs(struct store_statfs_t *buf) {
    std::lock_guard<std::mutex> l(m_lock);
    buf->reset();
    buf->total = m_total_bytes;
    buf->allocated = m_allocated_bytes;
    buf->stored = m_total_bytes;
    return 0;
  }

protected:

  WholeSpaceIterator _get_iterator() {
    return std::shared_ptr<KeyValueDB::WholeSpaceIteratorImpl>(
      new MDBWholeSpaceIteratorImpl(&m_btree, &m_lock, &iterator_seq_no));
  }

  WholeSpaceIterator _get_snapshot_iterator();
};

#endif

