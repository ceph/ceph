// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include <map>
#include <set>
#include <string>
#include <tr1/memory>

#include "os/KeyValueDB.h"
#include "include/buffer.h"
#include "include/Context.h"

using std::string;

class KeyValueDBMemory : public KeyValueDB {
public:
  std::map<string, std::map<string, bufferlist> > db;

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
    const std::map<string, bufferlist> &to_set
    );

  int rmkeys(
    const string &prefix,
    const std::set<string> &keys
    );

  int rmkeys_by_prefix(
    const string &prefix
    );

  class TransactionImpl_ : public TransactionImpl {
  public:
    list<Context *> on_commit;
    KeyValueDBMemory *db;

    TransactionImpl_(KeyValueDBMemory *db) : db(db) {}


    struct SetOp : public Context {
      KeyValueDBMemory *db;
      string prefix;
      std::map<string, bufferlist> to_set;
      SetOp(KeyValueDBMemory *db,
	    const string &prefix,
	    const std::map<string, bufferlist> &to_set)
	: db(db), prefix(prefix), to_set(to_set) {}
      void finish(int r) {
	db->set(prefix, to_set);
      }
    };
    void set(const string &prefix, const std::map<string, bufferlist> &to_set) {
      on_commit.push_back(new SetOp(db, prefix, to_set));
    }

    struct RmKeysOp : public Context {
      KeyValueDBMemory *db;
      string prefix;
      std::set<string> keys;
      RmKeysOp(KeyValueDBMemory *db,
	       const string &prefix,
	       const std::set<string> &keys)
	: db(db), prefix(prefix), keys(keys) {}
      void finish(int r) {
	db->rmkeys(prefix, keys);
      }
    };
    void rmkeys(const string &prefix, const std::set<string> &to_remove) {
      on_commit.push_back(new RmKeysOp(db, prefix, to_remove));
    }

    struct RmKeysByPrefixOp : public Context {
      KeyValueDBMemory *db;
      string prefix;
      RmKeysByPrefixOp(KeyValueDBMemory *db,
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
	(*i)->finish(0);
	delete *i;
      }
      return 0;
    }

    ~TransactionImpl_() {
      for (list<Context *>::iterator i = on_commit.begin();
	   i != on_commit.end();
	   on_commit.erase(i++)) {
	delete *i;
      }
    }
  };

  Transaction get_transaction() {
    return Transaction(new TransactionImpl_(this));
  }

  int submit_transaction(Transaction trans) {
    return static_cast<TransactionImpl_*>(trans.get())->complete();
  }

  friend class MemIterator;
  Iterator get_iterator(const string &prefix);
};
