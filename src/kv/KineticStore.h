// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#ifndef KINETIC_STORE_H
#define KINETIC_STORE_H

#include "include/types.h"
#include "include/buffer_fwd.h"
#include "KeyValueDB.h"
#include <set>
#include <map>
#include <string>
#include "include/memory.h"
#include <kinetic/kinetic.h>

#include <errno.h>
#include "common/errno.h"
#include "common/dout.h"
#include "include/assert.h"
#include "common/Formatter.h"

#include "common/ceph_context.h"

class PerfCounters;

enum {
  l_kinetic_first = 34400,
  l_kinetic_gets,
  l_kinetic_txns,
  l_kinetic_last,
};

/**
 * Uses Kinetic to implement the KeyValueDB interface
 */
class KineticStore : public KeyValueDB {
  CephContext *cct;
  PerfCounters *logger;
  string host;
  int port;
  int user_id;
  string hmac_key;
  bool use_ssl;
  std::unique_ptr<kinetic::BlockingKineticConnection> kinetic_conn;

  int do_open(ostream &out, bool create_if_missing);

public:
  explicit KineticStore(CephContext *c);
  ~KineticStore();

  static int _test_init(CephContext *c);
  int init();

  /// Opens underlying db
  int open(ostream &out) {
    return do_open(out, false);
  }
  /// Creates underlying db if missing and opens it
  int create_and_open(ostream &out) {
    return do_open(out, true);
  }

  void close();

  enum KineticOpType {
    KINETIC_OP_WRITE,
    KINETIC_OP_DELETE,
  };

  struct KineticOp {
    KineticOpType type;
    std::string key;
    bufferlist data;
    KineticOp(KineticOpType type, const string &key) : type(type), key(key) {}
    KineticOp(KineticOpType type, const string &key, const bufferlist &data)
      : type(type), key(key), data(data) {}
  };

  class KineticTransactionImpl : public KeyValueDB::TransactionImpl {
  public:
    vector<KineticOp> ops;
    KineticStore *db;

    explicit KineticTransactionImpl(KineticStore *db) : db(db) {}
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
    return std::make_shared<KineticTransactionImpl>(this);
  }

  int submit_transaction(KeyValueDB::Transaction t);
  int submit_transaction_sync(KeyValueDB::Transaction t);
  int get(
    const string &prefix,
    const std::set<string> &key,
    std::map<string, bufferlist> *out
    );
  using KeyValueDB::get;

  class KineticWholeSpaceIteratorImpl :
    public KeyValueDB::WholeSpaceIteratorImpl {
    std::set<std::string> keys;
    std::set<std::string>::iterator keys_iter;
    kinetic::BlockingKineticConnection *kinetic_conn;
    kinetic::KineticStatus kinetic_status;
  public:
    explicit KineticWholeSpaceIteratorImpl(kinetic::BlockingKineticConnection *conn);
    virtual ~KineticWholeSpaceIteratorImpl() { }

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
    bool raw_key_is_prefixed(const string &prefix);
    bufferlist value();
    int status();
  };

  /// Utility
  static string combine_strings(const string &prefix, const string &value);
  static int split_key(string &in_prefix, string *prefix, string *key);
  static bufferlist to_bufferlist(const kinetic::KineticRecord &record);
  virtual uint64_t get_estimated_size(map<string,uint64_t> &extra) {
    // not used by the osd
    return 0;
  }


protected:
  WholeSpaceIterator _get_iterator() {
    return std::make_shared<KineticWholeSpaceIteratorImpl>(kinetic_conn.get());
  }

  // TODO: remove snapshots from interface
  WholeSpaceIterator _get_snapshot_iterator() {
    return _get_iterator();
  }

};

#endif
