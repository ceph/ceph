// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
* Ceph - scalable distributed file system
*
* Copyright (C) 2012 Inktank, Inc.
*
* This is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License version 2.1, as published by the Free Software
* Foundation. See file COPYING.
*/
#ifndef CEPH_MONITOR_DB_STORE_H
#define CEPH_MONITOR_DB_STORE_H

#include "include/types.h"
#include "include/buffer.h"
#include <set>
#include <map>
#include <string>
#include <boost/scoped_ptr.hpp>
#include <sstream>
#include <fstream>
#include "kv/KeyValueDB.h"

#include "include/assert.h"
#include "common/Formatter.h"
#include "common/Finisher.h"
#include "common/errno.h"
#include "common/debug.h"
#include "common/safe_io.h"

class MonitorDBStore
{
  string path;
  boost::scoped_ptr<KeyValueDB> db;
  bool do_dump;
  int dump_fd_binary;
  std::ofstream dump_fd_json;
  JSONFormatter dump_fmt;
  

  Finisher io_work;

  bool is_open;

 public:

  struct Op {
    uint8_t type;
    string prefix;
    string key, endkey;
    bufferlist bl;

    Op()
      : type(0) { }
    Op(int t, string p, string k)
      : type(t), prefix(p), key(k) { }
    Op(int t, const string& p, string k, bufferlist& b)
      : type(t), prefix(p), key(k), bl(b) { }
    Op(int t, const string& p, string start, string end)
      : type(t), prefix(p), key(start), endkey(end) { }

    void encode(bufferlist& encode_bl) const {
      ENCODE_START(2, 1, encode_bl);
      ::encode(type, encode_bl);
      ::encode(prefix, encode_bl);
      ::encode(key, encode_bl);
      ::encode(bl, encode_bl);
      ::encode(endkey, encode_bl);
      ENCODE_FINISH(encode_bl);
    }

    void decode(bufferlist::iterator& decode_bl) {
      DECODE_START(2, decode_bl);
      ::decode(type, decode_bl);
      ::decode(prefix, decode_bl);
      ::decode(key, decode_bl);
      ::decode(bl, decode_bl);
      if (struct_v >= 2)
	::decode(endkey, decode_bl);
      DECODE_FINISH(decode_bl);
    }

    void dump(Formatter *f) const {
      f->dump_int("type", type);
      f->dump_string("prefix", prefix);
      f->dump_string("key", key);
      if (endkey.length())
	f->dump_string("endkey", endkey);
    }

    static void generate_test_instances(list<Op*>& ls) {
      ls.push_back(new Op);
      // we get coverage here from the Transaction instances
    }
  };

  struct Transaction;
  typedef ceph::shared_ptr<Transaction> TransactionRef;
  struct Transaction {
    list<Op> ops;
    uint64_t bytes, keys;

    Transaction() : bytes(0), keys(0) {}

    enum {
      OP_PUT	= 1,
      OP_ERASE	= 2,
      OP_COMPACT = 3,
    };

    void put(string prefix, string key, bufferlist& bl) {
      ops.push_back(Op(OP_PUT, prefix, key, bl));
      ++keys;
      bytes += prefix.length() + key.length() + bl.length();
    }

    void put(string prefix, version_t ver, bufferlist& bl) {
      ostringstream os;
      os << ver;
      put(prefix, os.str(), bl);
    }

    void put(string prefix, string key, version_t ver) {
      bufferlist bl;
      ::encode(ver, bl);
      put(prefix, key, bl);
    }

    void erase(string prefix, string key) {
      ops.push_back(Op(OP_ERASE, prefix, key));
      ++keys;
      bytes += prefix.length() + key.length();
    }

    void erase(string prefix, version_t ver) {
      ostringstream os;
      os << ver;
      erase(prefix, os.str());
    }

    void compact_prefix(string prefix) {
      ops.push_back(Op(OP_COMPACT, prefix, string()));
    }

    void compact_range(string prefix, string start, string end) {
      ops.push_back(Op(OP_COMPACT, prefix, start, end));
    }

    void encode(bufferlist& bl) const {
      ENCODE_START(2, 1, bl);
      ::encode(ops, bl);
      ::encode(bytes, bl);
      ::encode(keys, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::iterator& bl) {
      DECODE_START(2, bl);
      ::decode(ops, bl);
      if (struct_v >= 2) {
	::decode(bytes, bl);
	::decode(keys, bl);
      }
      DECODE_FINISH(bl);
    }

    static void generate_test_instances(list<Transaction*>& ls) {
      ls.push_back(new Transaction);
      ls.push_back(new Transaction);
      bufferlist bl;
      bl.append("value");
      ls.back()->put("prefix", "key", bl);
      ls.back()->erase("prefix2", "key2");
      ls.back()->compact_prefix("prefix3");
      ls.back()->compact_range("prefix4", "from", "to");
    }

    void append(TransactionRef other) {
      ops.splice(ops.end(), other->ops);
      keys += other->keys;
      bytes += other->bytes;
    }

    void append_from_encoded(bufferlist& bl) {
      TransactionRef other(new Transaction);
      bufferlist::iterator it = bl.begin();
      other->decode(it);
      append(other);
    }

    bool empty() {
      return (size() == 0);
    }

    size_t size() const {
      return ops.size();
    }
    uint64_t get_keys() const {
      return keys;
    }
    uint64_t get_bytes() const {
      return bytes;
    }

    void dump(ceph::Formatter *f, bool dump_val=false) const {
      f->open_object_section("transaction");
      f->open_array_section("ops");
      list<Op>::const_iterator it;
      int op_num = 0;
      for (it = ops.begin(); it != ops.end(); ++it) {
	const Op& op = *it;
	f->open_object_section("op");
	f->dump_int("op_num", op_num++);
	switch (op.type) {
	case OP_PUT:
	  {
	    f->dump_string("type", "PUT");
	    f->dump_string("prefix", op.prefix);
	    f->dump_string("key", op.key);
	    f->dump_unsigned("length", op.bl.length());
	    if (dump_val) {
	      ostringstream os;
	      op.bl.hexdump(os);
	      f->dump_string("bl", os.str());
	    }
	  }
	  break;
	case OP_ERASE:
	  {
	    f->dump_string("type", "ERASE");
	    f->dump_string("prefix", op.prefix);
	    f->dump_string("key", op.key);
	  }
	  break;
	case OP_COMPACT:
	  {
	    f->dump_string("type", "COMPACT");
	    f->dump_string("prefix", op.prefix);
	    f->dump_string("start", op.key);
	    f->dump_string("end", op.endkey);
	  }
	  break;
	default:
	  {
	    f->dump_string("type", "unknown");
	    f->dump_unsigned("op_code", op.type);
	    break;
	  }
	}
	f->close_section();
      }
      f->close_section();
      f->dump_unsigned("num_keys", keys);
      f->dump_unsigned("num_bytes", bytes);
      f->close_section();
    }
  };

  int apply_transaction(MonitorDBStore::TransactionRef t) {
    KeyValueDB::Transaction dbt = db->get_transaction();

    if (do_dump) {
      if (!g_conf->mon_debug_dump_json) {
        bufferlist bl;
        t->encode(bl);
        bl.write_fd(dump_fd_binary);
      } else {
        t->dump(&dump_fmt, true);
        dump_fmt.flush(dump_fd_json);
        dump_fd_json.flush();
      }
    }

    list<pair<string, pair<string,string> > > compact;
    for (list<Op>::const_iterator it = t->ops.begin();
	 it != t->ops.end();
	 ++it) {
      const Op& op = *it;
      switch (op.type) {
      case Transaction::OP_PUT:
	dbt->set(op.prefix, op.key, op.bl);
	break;
      case Transaction::OP_ERASE:
	dbt->rmkey(op.prefix, op.key);
	break;
      case Transaction::OP_COMPACT:
	compact.push_back(make_pair(op.prefix, make_pair(op.key, op.endkey)));
	break;
      default:
	derr << __func__ << " unknown op type " << op.type << dendl;
	ceph_assert(0);
	break;
      }
    }
    int r = db->submit_transaction_sync(dbt);
    if (r >= 0) {
      while (!compact.empty()) {
	if (compact.front().second.first == string() &&
	    compact.front().second.second == string())
	  db->compact_prefix_async(compact.front().first);
	else
	  db->compact_range_async(compact.front().first, compact.front().second.first, compact.front().second.second);
	compact.pop_front();
      }
    } else {
      assert(0 == "failed to write to db");
    }
    return r;
  }

  struct C_DoTransaction : public Context {
    MonitorDBStore *store;
    MonitorDBStore::TransactionRef t;
    Context *oncommit;
    C_DoTransaction(MonitorDBStore *s, MonitorDBStore::TransactionRef t,
		    Context *f)
      : store(s), t(t), oncommit(f)
    {}
    void finish(int r) {
      /* The store serializes writes.  Each transaction is handled
       * sequentially by the io_work Finisher.  If a transaction takes longer
       * to apply its state to permanent storage, then no other transaction
       * will be handled meanwhile.
       *
       * We will now randomly inject random delays.  We can safely sleep prior
       * to applying the transaction as it won't break the model.
       */
      double delay_prob = g_conf->mon_inject_transaction_delay_probability;
      if (delay_prob && (rand() % 10000 < delay_prob * 10000.0)) {
        utime_t delay;
        double delay_max = g_conf->mon_inject_transaction_delay_max;
        delay.set_from_double(delay_max * (double)(rand() % 10000) / 10000.0);
        lsubdout(g_ceph_context, mon, 1)
          << "apply_transaction will be delayed for " << delay
          << " seconds" << dendl;
        delay.sleep();
      }
      int ret = store->apply_transaction(t);
      oncommit->complete(ret);
    }
  };

  /**
   * queue transaction
   *
   * Queue a transaction to commit asynchronously.  Trigger a context
   * on completion (without any locks held).
   */
  void queue_transaction(MonitorDBStore::TransactionRef t,
			 Context *oncommit) {
    io_work.queue(new C_DoTransaction(this, t, oncommit));
  }

  /**
   * block and flush all io activity
   */
  void flush() {
    io_work.wait_for_empty();
  }

  class StoreIteratorImpl {
  protected:
    bool done;
    pair<string,string> last_key;
    bufferlist crc_bl;

    StoreIteratorImpl() : done(false) { }
    virtual ~StoreIteratorImpl() { }

    bool add_chunk_entry(TransactionRef tx,
			 string &prefix,
			 string &key,
			 bufferlist &value,
			 uint64_t max) {
      TransactionRef tmp(new Transaction);
      bufferlist tmp_bl;
      tmp->put(prefix, key, value);
      tmp->encode(tmp_bl);

      bufferlist tx_bl;
      tx->encode(tx_bl);

      size_t len = tx_bl.length() + tmp_bl.length();

      if (!tx->empty() && (len > max)) {
	return false;
      }

      tx->append(tmp);
      last_key.first = prefix;
      last_key.second = key;

      if (g_conf->mon_sync_debug) {
	::encode(prefix, crc_bl);
	::encode(key, crc_bl);
	::encode(value, crc_bl);
      }

      return true;
    }

    virtual bool _is_valid() = 0;

  public:
    __u32 crc() {
      if (g_conf->mon_sync_debug)
	return crc_bl.crc32c(0);
      return 0;
    }
    pair<string,string> get_last_key() {
      return last_key;
    }
    virtual bool has_next_chunk() {
      return !done && _is_valid();
    }
    virtual void get_chunk_tx(TransactionRef tx, uint64_t max) = 0;
    virtual pair<string,string> get_next_key() = 0;
  };
  typedef ceph::shared_ptr<StoreIteratorImpl> Synchronizer;

  class WholeStoreIteratorImpl : public StoreIteratorImpl {
    KeyValueDB::WholeSpaceIterator iter;
    set<string> sync_prefixes;

  public:
    WholeStoreIteratorImpl(KeyValueDB::WholeSpaceIterator iter,
			   set<string> &prefixes)
      : StoreIteratorImpl(),
	iter(iter),
	sync_prefixes(prefixes)
    { }

    virtual ~WholeStoreIteratorImpl() { }

    /**
     * Obtain a chunk of the store
     *
     * @param bl	    Encoded transaction that will recreate the chunk
     * @param first_key	    Pair containing the first key to obtain, and that
     *			    will contain the first key in the chunk (that may
     *			    differ from the one passed on to the function)
     * @param last_key[out] Last key in the chunk
     */
    virtual void get_chunk_tx(TransactionRef tx, uint64_t max) {
      assert(done == false);
      assert(iter->valid() == true);

      while (iter->valid()) {
	string prefix(iter->raw_key().first);
	string key(iter->raw_key().second);
	if (sync_prefixes.count(prefix)) {
	  bufferlist value = iter->value();
	  if (!add_chunk_entry(tx, prefix, key, value, max))
	    return;
	}
	iter->next();
      }
      assert(iter->valid() == false);
      done = true;
    }

    virtual pair<string,string> get_next_key() {
      assert(iter->valid());

      for (; iter->valid(); iter->next()) {
        pair<string,string> r = iter->raw_key();
        if (sync_prefixes.count(r.first) > 0) {
          iter->next();
          return r;
        }
      }
      return pair<string,string>();
    }

    virtual bool _is_valid() {
      return iter->valid();
    }
  };

  Synchronizer get_synchronizer(pair<string,string> &key,
				set<string> &prefixes) {
    KeyValueDB::WholeSpaceIterator iter;
    iter = db->get_snapshot_iterator();

    if (!key.first.empty() && !key.second.empty())
      iter->upper_bound(key.first, key.second);
    else
      iter->seek_to_first();

    return ceph::shared_ptr<StoreIteratorImpl>(
	new WholeStoreIteratorImpl(iter, prefixes)
    );
  }

  KeyValueDB::Iterator get_iterator(const string &prefix) {
    assert(!prefix.empty());
    KeyValueDB::Iterator iter = db->get_snapshot_iterator(prefix);
    iter->seek_to_first();
    return iter;
  }

  KeyValueDB::WholeSpaceIterator get_iterator() {
    KeyValueDB::WholeSpaceIterator iter;
    iter = db->get_snapshot_iterator();
    iter->seek_to_first();
    return iter;
  }

  int get(const string& prefix, const string& key, bufferlist& bl) {
    assert(bl.length() == 0);
    return db->get(prefix, key, &bl);
  }

  int get(const string& prefix, const version_t ver, bufferlist& bl) {
    ostringstream os;
    os << ver;
    return get(prefix, os.str(), bl);
  }

  version_t get(const string& prefix, const string& key) {
    bufferlist bl;
    int err = get(prefix, key, bl);
    if (err < 0) {
      if (err == -ENOENT) // if key doesn't exist, assume its value is 0
        return 0;
      // we're not expecting any other negative return value, and we can't
      // just return a negative value if we're returning a version_t
      generic_dout(0) << "MonitorDBStore::get() error obtaining"
                      << " (" << prefix << ":" << key << "): "
                      << cpp_strerror(err) << dendl;
      assert(0 == "error obtaining key");
    }

    assert(bl.length());
    version_t ver;
    bufferlist::iterator p = bl.begin();
    ::decode(ver, p);
    return ver;
  }

  bool exists(const string& prefix, const string& key) {
    KeyValueDB::Iterator it = db->get_iterator(prefix);
    int err = it->lower_bound(key);
    if (err < 0)
      return false;

    return (it->valid() && it->key() == key);
  }

  bool exists(const string& prefix, version_t ver) {
    ostringstream os;
    os << ver;
    return exists(prefix, os.str());
  }

  string combine_strings(const string& prefix, const string& value) {
    string out = prefix;
    out.push_back('_');
    out.append(value);
    return out;
  }

  string combine_strings(const string& prefix, const version_t ver) {
    ostringstream os;
    os << ver;
    return combine_strings(prefix, os.str());
  }

  void clear(set<string>& prefixes) {
    set<string>::iterator iter;
    KeyValueDB::Transaction dbt = db->get_transaction();

    for (iter = prefixes.begin(); iter != prefixes.end(); ++iter) {
      dbt->rmkeys_by_prefix((*iter));
    }
    int r = db->submit_transaction_sync(dbt);
    assert(r >= 0);
  }

  void _open(string kv_type) {
    string::const_reverse_iterator rit;
    int pos = 0;
    for (rit = path.rbegin(); rit != path.rend(); ++rit, ++pos) {
      if (*rit != '/')
	break;
    }
    ostringstream os;
    os << path.substr(0, path.size() - pos) << "/store.db";
    string full_path = os.str();

    KeyValueDB *db_ptr = KeyValueDB::create(g_ceph_context,
					    kv_type,
					    full_path);
    if (!db_ptr) {
      derr << __func__ << " error initializing "
	   << kv_type << " db back storage in "
	   << full_path << dendl;
      assert(0 != "MonitorDBStore: error initializing keyvaluedb back storage");
    }
    db.reset(db_ptr);

    if (g_conf->mon_debug_dump_transactions) {
      if (!g_conf->mon_debug_dump_json) {
        dump_fd_binary = ::open(
          g_conf->mon_debug_dump_location.c_str(),
          O_CREAT|O_APPEND|O_WRONLY, 0644);
        if (dump_fd_binary < 0) {
          dump_fd_binary = -errno;
          derr << "Could not open log file, got "
               << cpp_strerror(dump_fd_binary) << dendl;
        }
      } else {
        dump_fmt.reset();
        dump_fmt.open_array_section("dump");
        dump_fd_json.open(g_conf->mon_debug_dump_location.c_str());
      }
      do_dump = true;
    }
    if (kv_type == "rocksdb")
      db->init(g_conf->mon_rocksdb_options);
    else
      db->init();
  }

  int open(ostream &out) {
    string kv_type;
    int r = read_meta("kv_backend", &kv_type);
    if (r < 0 || kv_type.length() == 0)
      kv_type = "leveldb";

    _open(kv_type);
    r = db->open(out);
    if (r < 0)
      return r;
    io_work.start();
    is_open = true;
    return 0;
  }

  int create_and_open(ostream &out) {
    // record the type before open
    string kv_type;
    int r = read_meta("kv_backend", &kv_type);
    if (r < 0) {
      kv_type = g_conf->mon_keyvaluedb;
      r = write_meta("kv_backend", kv_type);
      if (r < 0)
	return r;
    }
    _open(kv_type);
    r = db->create_and_open(out);
    if (r < 0)
      return r;
    io_work.start();
    is_open = true;
    return 0;
  }

  void close() {
    // there should be no work queued!
    io_work.stop();
    is_open = false;
  }

  void compact() {
    db->compact();
  }

  void compact_prefix(const string& prefix) {
    db->compact_prefix(prefix);
  }

  uint64_t get_estimated_size(map<string, uint64_t> &extras) {
    return db->get_estimated_size(extras);
  }

  /**
   * write_meta - write a simple configuration key out-of-band
   *
   * Write a simple key/value pair for basic store configuration
   * (e.g., a uuid or magic number) to an unopened/unmounted store.
   * The default implementation writes this to a plaintext file in the
   * path.
   *
   * A newline is appended.
   *
   * @param key key name (e.g., "fsid")
   * @param value value (e.g., a uuid rendered as a string)
   * @returns 0 for success, or an error code
   */
  int write_meta(const std::string& key,
		 const std::string& value) const {
    string v = value;
    v += "\n";
    int r = safe_write_file(path.c_str(), key.c_str(),
			    v.c_str(), v.length());
    if (r < 0)
      return r;
    return 0;
  }

  /**
   * read_meta - read a simple configuration key out-of-band
   *
   * Read a simple key value to an unopened/mounted store.
   *
   * Trailing whitespace is stripped off.
   *
   * @param key key name
   * @param value pointer to value string
   * @returns 0 for success, or an error code
   */
  int read_meta(const std::string& key,
		std::string *value) const {
    char buf[4096];
    int r = safe_read_file(path.c_str(), key.c_str(),
			   buf, sizeof(buf));
    if (r <= 0)
      return r;
    // drop trailing newlines
    while (r && isspace(buf[r-1])) {
      --r;
    }
    *value = string(buf, r);
    return 0;
  }

  explicit MonitorDBStore(const string& path)
    : path(path),
      db(0),
      do_dump(false),
      dump_fd_binary(-1),
      dump_fmt(true),
      io_work(g_ceph_context, "monstore", "fn_monstore"),
      is_open(false) {
  }
  ~MonitorDBStore() {
    assert(!is_open);
    if (do_dump) {
      if (!g_conf->mon_debug_dump_json) {
        ::close(dump_fd_binary);
      } else {
        dump_fmt.close_section();
        dump_fmt.flush(dump_fd_json);
        dump_fd_json.flush();
        dump_fd_json.close();
      }
    }
  }

};

WRITE_CLASS_ENCODER(MonitorDBStore::Op)
WRITE_CLASS_ENCODER(MonitorDBStore::Transaction)

#endif /* CEPH_MONITOR_DB_STORE_H */
