// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2015 SanDisk
 *
 * Author: Varada Kari<varada.kari@sandisk.com>
 *
 * This is free software; you can redistribute it and/or
 * modify it.
 */

/* 
 * Contains a generic interface to integrate any backend DB in form a shared
 * object. All needed interfaces should be implemented mentioned in
 * PluggableDBInterfaces.h and PluggableDBIterator.h in the layer which
 * interacts with the backend DB. 
 */
#ifndef PLUGGABLEDB_STORE_H
#define PLUGGABLEDB_STORE_H

#include "include/types.h"
#include "include/buffer.h"
#include "KeyValueDB.h"
#include <set>
#include <map>
#include <string>
#include "include/memory.h"
#include <tr1/memory>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/tss.hpp>
#include <tr1/unordered_map>
#include <errno.h>
#include "common/errno.h"
#include "common/dout.h"
#include "include/assert.h"
#include "common/Formatter.h"
#include "common/ceph_context.h"
#include "PluggableDBIterator.h"
#include "PluggableDBInterfaces.h"
#include <sys/stat.h>

class PerfCounters;

enum {
  l_PluggableDB_first = 34400,
  l_PluggableDB_gets,
  l_PluggableDB_txns,
  l_PluggableDB_last,
};

/**
 * Interface to implement proprietary KeyValue stores
 */
class PluggableDBStore : public KeyValueDB {
  CephContext *cct;
  PerfCounters *logger;
  bool init_done;
  static bool create_if_missing;
  void *library;
  int do_open(ostream &out, bool create_if_missing);
  int getosdid();
  PluggableDBIterator* DBGetIterator();
  value_t* DBGetValue(const string& key);
  int DBInit(const char*, int, map<string, string>*);
  void DBClose();
  uint64_t DBGetSize();
  int DBGetStatfs(struct statfs *buf);
  int DBSubmitTransaction(map<string, DBOp>& ops);
  int DBSubmitTransactionSync(map<string, DBOp>& ops);
  int load_symbols(const char *path);
  void load_dll(const char *path);
public:
  PluggableDBStore(CephContext *c);
  ~PluggableDBStore();

  static int _test_init();
  int init(string opt_str);

  // Opens underlying db
  int open(ostream &out) {
    return do_open(out, false);
  }
  // Creates underlying db if missing and opens it
  int create_and_open(ostream &out) {
    return do_open(out, true);
  }

  void close();

  class PluggableDBTransactionImpl : public KeyValueDB::TransactionImpl {
  public:
	map<string, DBOp> ops;
    PluggableDBStore *db;

    PluggableDBTransactionImpl(PluggableDBStore *db) : db(db) {}
    void set(
      const string &prefix,
      const string &k,
      const bufferlist &bl);
    void rmkey(
      const string &prefix,
      const string &k);
    void rmkeys_by_prefix(
      const string &prefix
      );
  };

  KeyValueDB::Transaction get_transaction() {
    return ceph::shared_ptr< PluggableDBTransactionImpl >(
      new PluggableDBTransactionImpl(this));
  }

  int submit_transaction(KeyValueDB::Transaction t);
  int submit_transaction_sync(KeyValueDB::Transaction t);
  int get(
    const string &prefix,
    const std::set<string> &key,
    std::map<string, bufferlist> *out
    );

  class PluggableDBWholeSpaceIteratorImpl :
    public KeyValueDB::WholeSpaceIteratorImpl {
    PluggableDBStore *store;
    PluggableDBIterator *iter;

  public:
    PluggableDBWholeSpaceIteratorImpl(PluggableDBStore *parent);
    virtual ~PluggableDBWholeSpaceIteratorImpl() {
       delete iter;
    }

    int seek_to_first() {
      return seek_to_first("");
    }
    int seek_to_first(const string &prefix);
    int seek_to_last();
    int seek_to_last(const string &prefix);
    int upper_bound(const string &prefix, const string &after);
    int lower_bound(const string &prefix, const string &to);
    bool valid();
    int next();
    int prev();
    string key();
    pair<string,string> raw_key();
    bufferlist value();
    int status();
  };

  // Utility
  static string combine_strings(const string &prefix, const string &value);
  static int split_key(string in_prefix, string *prefix, string *key);
  static bufferlist to_bufferlist(const string &in);
  virtual uint64_t get_estimated_size(map<string,uint64_t> &extra);
  virtual int get_statfs(struct statfs *buf);


protected:
  WholeSpaceIterator _get_iterator() {
    return ceph::shared_ptr<KeyValueDB::WholeSpaceIteratorImpl>(
								new PluggableDBWholeSpaceIteratorImpl(this));
  }

  // TODO: get a snapshot iterator, time being let us handle the AFS as
  // snapshot
  WholeSpaceIterator _get_snapshot_iterator() {
    return _get_iterator();
  }

};

#endif
