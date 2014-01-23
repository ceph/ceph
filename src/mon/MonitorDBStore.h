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
#include "os/KeyValueDB.h"
#include "os/LevelDBStore.h"

#include "include/assert.h"
#include "common/Formatter.h"
#include "common/errno.h"

class MonitorDBStore
{
  boost::scoped_ptr<LevelDBStore> db;
  bool do_dump;
  int dump_fd;

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

  struct Transaction {
    list<Op> ops;

    enum {
      OP_PUT	= 1,
      OP_ERASE	= 2,
      OP_COMPACT = 3,
    };

    void put(string prefix, string key, bufferlist& bl) {
      ops.push_back(Op(OP_PUT, prefix, key, bl));
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
      ENCODE_START(1, 1, bl);
      ::encode(ops, bl);
      ENCODE_FINISH(bl);
    }

    void decode(bufferlist::iterator& bl) {
      DECODE_START(1, bl);
      ::decode(ops, bl);
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

    void append(Transaction& other) {
      ops.splice(ops.end(), other.ops);
    }

    void append_from_encoded(bufferlist& bl) {
      Transaction other;
      bufferlist::iterator it = bl.begin();
      other.decode(it);
      append(other);
    }

    bool empty() {
      return (size() == 0);
    }

    bool size() {
      return ops.size();
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
      f->close_section();
    }
  };

  int apply_transaction(const MonitorDBStore::Transaction& t) {
    KeyValueDB::Transaction dbt = db->get_transaction();

    if (do_dump) {
      bufferlist bl;
      t.encode(bl);
      bl.write_fd(dump_fd);
    }

    list<pair<string, pair<string,string> > > compact;
    for (list<Op>::const_iterator it = t.ops.begin(); it != t.ops.end(); ++it) {
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
    }
    return r;
  }

  class StoreIteratorImpl {
  protected:
    bool done;
    pair<string,string> last_key;
    bufferlist crc_bl;

    StoreIteratorImpl() : done(false) { }
    virtual ~StoreIteratorImpl() { }

    bool add_chunk_entry(Transaction &tx,
			 string &prefix,
			 string &key,
			 bufferlist &value,
			 uint64_t max) {
      Transaction tmp;
      bufferlist tmp_bl;
      tmp.put(prefix, key, value);
      tmp.encode(tmp_bl);

      bufferlist tx_bl;
      tx.encode(tx_bl);

      size_t len = tx_bl.length() + tmp_bl.length();

      if (!tx.empty() && (len > max)) {
	return false;
      }

      tx.append(tmp);
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
    };
    virtual bool has_next_chunk() {
      return !done && _is_valid();
    }
    virtual void get_chunk_tx(Transaction &tx, uint64_t max) = 0;
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
    virtual void get_chunk_tx(Transaction &tx, uint64_t max) {
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
      pair<string,string> r = iter->raw_key();
      do {
	iter->next();
      } while (iter->valid() && sync_prefixes.count(iter->raw_key().first) == 0);
      return r;
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
    set<string> k;
    k.insert(key);
    map<string,bufferlist> out;

    db->get(prefix, k, &out);
    if (out.empty())
      return -ENOENT;
    bl.append(out[key]);

    return 0;
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
    db->submit_transaction_sync(dbt);
  }

  int open(ostream &out) {
    db->options.write_buffer_size = g_conf->mon_leveldb_write_buffer_size;
    db->options.cache_size = g_conf->mon_leveldb_cache_size;
    db->options.block_size = g_conf->mon_leveldb_block_size;
    db->options.bloom_size = g_conf->mon_leveldb_bloom_size;
    db->options.compression_enabled = g_conf->mon_leveldb_compression;
    db->options.max_open_files = g_conf->mon_leveldb_max_open_files;
    db->options.paranoid_checks = g_conf->mon_leveldb_paranoid;
    db->options.log_file = g_conf->mon_leveldb_log;
    return db->open(out);
  }

  int create_and_open(ostream &out) {
    return db->create_and_open(out);
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

  MonitorDBStore(const string& path) :
    db(0), do_dump(false), dump_fd(-1) {
    string::const_reverse_iterator rit;
    int pos = 0;
    for (rit = path.rbegin(); rit != path.rend(); ++rit, ++pos) {
      if (*rit != '/')
	break;
    }
    ostringstream os;
    os << path.substr(0, path.size() - pos) << "/store.db";
    string full_path = os.str();

    LevelDBStore *db_ptr = new LevelDBStore(g_ceph_context, full_path);
    if (!db_ptr) {
      derr << __func__ << " error initializing level db back storage in "
		<< full_path << dendl;
      assert(0 != "MonitorDBStore: error initializing level db back storage");
    }
    db.reset(db_ptr);

    if (g_conf->mon_debug_dump_transactions) {
      do_dump = true;
      dump_fd = ::open(
	g_conf->mon_debug_dump_location.c_str(),
	O_CREAT|O_APPEND|O_WRONLY, 0644);
      if (!dump_fd) {
	dump_fd = -errno;
	derr << "Could not open log file, got "
	     << cpp_strerror(dump_fd) << dendl;
      }
    }
  }
  MonitorDBStore(LevelDBStore *db_ptr) :
    db(0), do_dump(false), dump_fd(-1) {
    db.reset(db_ptr);
  }
  ~MonitorDBStore() {
    if (do_dump)
      ::close(dump_fd);
  }

};

WRITE_CLASS_ENCODER(MonitorDBStore::Op);
WRITE_CLASS_ENCODER(MonitorDBStore::Transaction);

#endif /* CEPH_MONITOR_DB_STORE_H */
