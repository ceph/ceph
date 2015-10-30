// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 20015-2015 James Liu <james.liu@ssi.samsung.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */

#include <map>
#include <set>
#include <string>
#include "include/memory.h"

#include "os/KeyValueDB.h"
#include "include/buffer.h"
#include "include/Context.h"

using std::string;

class OmniKVStore : public KeyValueDB {
public:
  std::map<std::pair<string,string>,bufferlist> db;

  OmniKVStore(CephContext *c) { }
  OmniKVStore(OmniKVStore *db) : db(db->db) { }
  virtual ~OmniKVStore() { }

  virtual int init(string _opt) {
    return 0;
  }
  virtual int open(ostream &out) {
    return 0;
  }
  virtual int create_and_open(ostream &out) {
    return 0;
  }

  int get(
    const string &prefix,
    const std::set<string> &key,
    std::map<string, bufferlist> *out
    );

  int get_keys(
    const string &prefix,
    const std::set<string> &key,
    std::set<string> *out
    );

  int set(
    const string &prefix,
    const string &key,
    const bufferlist &bl
    );

  int rmkey(
    const string &prefix,
    const string &key
    );

  int rmkeys_by_prefix(
    const string &prefix
    );

  class KVSTransactionImpl_ : public TransactionImpl {
  public:
    list<Context *> on_commit;
    OmniKVStore *db;

    KVSTransactionImpl_(OmniKVStore *db) : db(db) {}


    struct SetOp : public Context {
      OmniKVStore *db;
      std::pair<string,string> key;
      bufferlist value;
      SetOp(OmniKVStore *db,
	    const std::pair<string,string> &key,
	    const bufferlist &value)
	: db(db), key(key), value(value) {}
      void finish(int r) {
	db->set(key.first, key.second, value);
      }
    };

    void set(const string &prefix, const string &k, const bufferlist& bl) {
      on_commit.push_back(new SetOp(db, std::make_pair(prefix, k), bl));
    }

    struct RmKeysOp : public Context {
      OmniKVStore *db;
      std::pair<string,string> key;
      RmKeysOp(OmniKVStore *db,
	       const std::pair<string,string> &key)
	: db(db), key(key) {}
      void finish(int r) {
	db->rmkey(key.first, key.second);
      }
    };

    void rmkey(const string &prefix, const string &key) {
      on_commit.push_back(new RmKeysOp(db, std::make_pair(prefix, key)));
    }

    struct RmKeysByPrefixOp : public Context {
      OmniKVStore *db;
      string prefix;
      RmKeysByPrefixOp(OmniKVStore *db,
		       const string &prefix)
	: db(db), prefix(prefix) {}
      void finish(int r) {
	db->rmkeys_by_prefix(prefix);
      }
    };
    void rmkeys_by_prefix(const string &prefix) {
      on_commit.push_back(new RmKeysByPrefixOp(db, prefix));
    }

    int complete() {
      for (list<Context *>::iterator i = on_commit.begin();
	   i != on_commit.end();
	   on_commit.erase(i++)) {
	(*i)->complete(0);
      }
      return 0;
    }

    ~KVSTransactionImpl_() {
      for (list<Context *>::iterator i = on_commit.begin();
	   i != on_commit.end();
	   on_commit.erase(i++)) {
	delete *i;
      }
    }
  };

  Transaction get_transaction() {
    return Transaction(new KVSTransactionImpl_(this));
  }

  int submit_transaction(Transaction trans) {
    return static_cast<KVSTransactionImpl_*>(trans.get())->complete();
  }

  int submit_transaction_sync(Transaction trans) {
  	return submit_transaction(trans);
  }

  uint64_t get_estimated_size(map<string,uint64_t> &extras) {
    uint64_t total_size = 0;

    for (map<pair<string,string>,bufferlist>::iterator p = db.begin();
         p != db.end(); ++p) {
      string prefix = p->first.first;
      bufferlist &bl = p->second;

      uint64_t sz = bl.length();
      total_size += sz;
      if (extras.count(prefix) == 0)
        extras[prefix] = 0;
      extras[prefix] += sz;
    }

    return total_size;
  }

private:
  bool exists_prefix(const string &prefix) {
    std::map<std::pair<string,string>,bufferlist>::iterator it;
    it = db.lower_bound(std::make_pair(prefix, ""));
    return ((it != db.end()) && ((*it).first.first == prefix));
  }

  friend class WholeSpaceMemIterator;

protected:
  WholeSpaceIterator _get_iterator();
  WholeSpaceIterator _get_snapshot_iterator();
};
