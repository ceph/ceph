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

class MonitorDBStore
{
  boost::scoped_ptr<LevelDBStore> db;

 public:

  struct Op {
    uint8_t type;
    string prefix;
    string key;
    bufferlist bl;

    Op() { }
    Op(int t, string p, string k)
      : type(t), prefix(p), key(k) { }
    Op(int t, const string& p, string k, bufferlist& b)
      : type(t), prefix(p), key(k), bl(b) { }

    void encode(bufferlist& encode_bl) const {
      ENCODE_START(1, 1, encode_bl);
      ::encode(type, encode_bl);
      ::encode(prefix, encode_bl);
      ::encode(key, encode_bl);
      ::encode(bl, encode_bl);
      ENCODE_FINISH(encode_bl);
    }

    void decode(bufferlist::iterator& decode_bl) {
      DECODE_START(1, decode_bl);
      ::decode(type, decode_bl);
      ::decode(prefix, decode_bl);
      ::decode(key, decode_bl);
      ::decode(bl, decode_bl);
      DECODE_FINISH(decode_bl);
    }
  };

  struct Transaction {
    list<Op> ops;

    enum {
      OP_PUT	= 1,
      OP_ERASE	= 2,
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
      return (ops.size() == 0);
    }

    void dump(ceph::Formatter *f) {
      f->open_object_section("transaction");
      f->open_array_section("ops");
      list<Op>::iterator it;
      int op_num = 0;
      for (it = ops.begin(); it != ops.end(); ++it) {
	Op& op = *it;
	f->open_object_section("op");
	f->dump_int("op_num", op_num++);
	switch (op.type) {
	case OP_PUT:
	  {
	    f->dump_string("type", "PUT");
	    f->dump_string("prefix", op.prefix);
	    f->dump_string("key", op.key);
	    ostringstream os;
	    op.bl.hexdump(os);
	    f->dump_unsigned("length", op.bl.length());
	    f->dump_string("bl", os.str());
	  }
	  break;
	case OP_ERASE:
	  {
	    f->dump_string("type", "ERASE");
	    f->dump_string("prefix", op.prefix);
	    f->dump_string("key", op.key);
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

  int apply_transaction(MonitorDBStore::Transaction& t) {
    KeyValueDB::Transaction dbt = db->get_transaction();

    for (list<Op>::iterator it = t.ops.begin(); it != t.ops.end(); ++it) {
      Op& op = *it;
      switch (op.type) {
      case Transaction::OP_PUT:
	dbt->set(op.prefix, op.key, op.bl);
	break;
      case Transaction::OP_ERASE:
	dbt->rmkey(op.prefix, op.key);
	break;
      default:
	derr << __func__ << " unknown op type " << op.type << dendl;
	ceph_assert(0);
	break;
      }
    }
    return db->submit_transaction_sync(dbt);
  }

  int get(const string& prefix, const string& key, bufferlist& bl) {
    set<string> k;
    k.insert(key);
    map<string,bufferlist> out;

    db->get(prefix, k, &out);
    if (!out.empty())
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
    get(prefix, key, bl);
    if (!bl.length()) // if key does not exist, assume its value is 0
      return 0;

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

  MonitorDBStore(const string& path) : db(0) {

    string::const_reverse_iterator rit;
    int pos = 0;
    for (rit = path.rbegin(); rit != path.rend(); ++rit, ++pos) {
      if (*rit != '/')
	break;
    }
    ostringstream os;
    os << path.substr(0, path.size() - pos) << "/store.db";

    string full_path = os.str();

    LevelDBStore *db_ptr = new LevelDBStore(full_path);
    if (!db_ptr) {
      cerr << __func__ << " error initializing level db back storage in "
	   << full_path << std::endl;
      assert(0 != "MonitorDBStore: error initializing level db back storage");
    }
    cout << __func__ << " initializing back storage in "
	 << full_path << std::endl;
    assert(!db_ptr->init(cerr));
    db.reset(db_ptr);
  }
  MonitorDBStore(LevelDBStore *db_ptr) {
    db.reset(db_ptr);
  }
  ~MonitorDBStore() { }

};

WRITE_CLASS_ENCODER(MonitorDBStore::Op);
WRITE_CLASS_ENCODER(MonitorDBStore::Transaction);

#endif /* CEPH_MONITOR_DB_STORE_H */
