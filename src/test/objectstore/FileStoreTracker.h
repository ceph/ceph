// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-

#ifndef FILESTORE_TRACKER_H
#define FILESTORE_TRACKER_H
#include "test/common/ObjectContents.h"
#include "os/filestore/FileStore.h"
#include "kv/KeyValueDB.h"
#include <boost/scoped_ptr.hpp>
#include <list>
#include <map>
#include "common/Mutex.h"

class FileStoreTracker {
  const static uint64_t SIZE = 4 * 1024;
  ObjectStore *store;
  KeyValueDB *db;
  Mutex lock;
  uint64_t restart_seq;

  struct OutTransaction {
    list<pair<pair<coll_t, string>, uint64_t> > *in_flight;
    ObjectStore::Transaction *t;
  };
public:
  FileStoreTracker(ObjectStore *store, KeyValueDB *db)
    : store(store), db(db),
      lock("Tracker Lock"), restart_seq(0) {}

  class Transaction {
    class Op {
    public:
      virtual void operator()(FileStoreTracker *harness,
			      OutTransaction *out) = 0;
      virtual ~Op() {};
    };
    list<Op*> ops;
    class Write : public Op {
    public:
      coll_t coll;
      string oid;
      Write(const coll_t &coll,
	    const string &oid)
	: coll(coll), oid(oid) {}
      void operator()(FileStoreTracker *harness,
		      OutTransaction *out) {
	harness->write(make_pair(coll, oid), out);
      }
    };
    class CloneRange : public Op {
    public:
      coll_t coll;
      string from;
      string to;
      CloneRange(const coll_t &coll,
		 const string &from,
		 const string &to)
	: coll(coll), from(from), to(to) {}
      void operator()(FileStoreTracker *harness,
		      OutTransaction *out) {
	harness->clone_range(make_pair(coll, from), make_pair(coll, to),
			     out);
      }
    };
    class Clone : public Op {
    public:
      coll_t coll;
      string from;
      string to;
      Clone(const coll_t &coll,
		 const string &from,
		 const string &to)
	: coll(coll), from(from), to(to) {}
      void operator()(FileStoreTracker *harness,
		      OutTransaction *out) {
	harness->clone(make_pair(coll, from), make_pair(coll, to),
			     out);
      }
    };
    class Remove: public Op {
    public:
      coll_t coll;
      string obj;
      Remove(const coll_t &coll,
	     const string &obj)
	: coll(coll), obj(obj) {}
      void operator()(FileStoreTracker *harness,
		      OutTransaction *out) {
	harness->remove(make_pair(coll, obj),
			out);
      }
    };
  public:
    void write(const coll_t &coll, const string &oid) {
      ops.push_back(new Write(coll, oid));
    }
    void clone_range(const coll_t &coll, const string &from,
		     const string &to) {
      ops.push_back(new CloneRange(coll, from, to));
    }
    void clone(const coll_t &coll, const string &from,
	       const string &to) {
      ops.push_back(new Clone(coll, from, to));
    }
    void remove(const coll_t &coll, const string &oid) {
      ops.push_back(new Remove(coll, oid));
    }
    friend class FileStoreTracker;
  };

  int init();
  void submit_transaction(Transaction &t);
  void verify(const coll_t &coll,
	      const string &from,
	      bool on_start = false);

private:
  ObjectContents get_current_content(const pair<coll_t, string> &obj);
  pair<uint64_t, uint64_t> get_valid_reads(const pair<coll_t, string> &obj);
  ObjectContents get_content(const pair<coll_t, string> &obj, uint64_t version);

  void committed(const pair<coll_t, string> &obj, uint64_t seq);
  void applied(const pair<coll_t, string> &obj, uint64_t seq);
  uint64_t set_content(const pair<coll_t, string> &obj, ObjectContents &content);

  // ObjectContents Operations
  void write(const pair<coll_t, string> &obj, OutTransaction *out);
  void remove(const pair<coll_t, string> &obj, OutTransaction *out);
  void clone_range(const pair<coll_t, string> &from,
		   const pair<coll_t, string> &to,
		   OutTransaction *out);
  void clone(const pair<coll_t, string> &from,
	     const pair<coll_t, string> &to,
	     OutTransaction *out);
  friend class OnApplied;
  friend class OnCommitted;
};

#endif
