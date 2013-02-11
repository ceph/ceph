// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software 
 * Foundation.  See file COPYING.
 * 
 */
#ifndef CEPH_OBJECTSTORE_H
#define CEPH_OBJECTSTORE_H

#include "include/Context.h"
#include "include/buffer.h"
#include "include/types.h"
#include "osd/osd_types.h"
#include "common/TrackedOp.h"
#include "ObjectMap.h"

#include <errno.h>
#include <sys/stat.h>
#include <vector>

#if defined(DARWIN) || defined(__FreeBSD__)
#include <sys/statvfs.h>
#else
#include <sys/vfs.h>    /* or <sys/statfs.h> */
#endif /* DARWIN */

using std::vector;
using std::string;

namespace ceph {
  class Formatter;
}

/*
 * low-level interface to the local OSD file system
 */

class Logger;

enum {
  l_os_first = 84000,
  l_os_jq_max_ops,
  l_os_jq_ops,
  l_os_j_ops,
  l_os_jq_max_bytes,
  l_os_jq_bytes,
  l_os_j_bytes,
  l_os_j_lat,
  l_os_j_wr,
  l_os_j_wr_bytes,
  l_os_oq_max_ops,
  l_os_oq_ops,
  l_os_ops,
  l_os_oq_max_bytes,
  l_os_oq_bytes,
  l_os_bytes,
  l_os_apply_lat,
  l_os_committing,
  l_os_commit,
  l_os_commit_len,
  l_os_commit_lat,
  l_os_j_full,
  l_os_last,
};


static inline void encode(const map<string,bufferptr> *attrset, bufferlist &bl) {
  ::encode(*attrset, bl);
}

class ObjectStore {
public:

  Logger *logger;

  /**
   * a sequencer orders transactions
   *
   * Any transactions queued under a given sequencer will be applied in
   * sequence.  Transactions queued under different sequencers may run
   * in parallel.
   */
  struct Sequencer_impl {
    virtual void flush() = 0;
    virtual ~Sequencer_impl() {}
  };
  struct Sequencer {
    string name;
    Sequencer_impl *p;

    Sequencer(string n)
      : name(n), p(NULL) {}
    ~Sequencer() {
      delete p;
    }

    /// return a unique string identifier for this sequencer
    const string& get_name() const {
      return name;
    }
    /// wait for any queued transactions on this sequencer to apply
    void flush() {
      if (p)
	p->flush();
    }
  };
  

  /*********************************
   * transaction
   */
  class Transaction {
  public:
    enum {
      OP_NOP =          0,
      OP_TOUCH =        9,   // cid, oid
      OP_WRITE =        10,  // cid, oid, offset, len, bl
      OP_ZERO =         11,  // cid, oid, offset, len
      OP_TRUNCATE =     12,  // cid, oid, len
      OP_REMOVE =       13,  // cid, oid
      OP_SETATTR =      14,  // cid, oid, attrname, bl
      OP_SETATTRS =     15,  // cid, oid, attrset
      OP_RMATTR =       16,  // cid, oid, attrname
      OP_CLONE =        17,  // cid, oid, newoid
      OP_CLONERANGE =   18,  // cid, oid, newoid, offset, len
      OP_CLONERANGE2 =  30,  // cid, oid, newoid, srcoff, len, dstoff

      OP_TRIMCACHE =    19,  // cid, oid, offset, len  **DEPRECATED**

      OP_MKCOLL =       20,  // cid
      OP_RMCOLL =       21,  // cid
      OP_COLL_ADD =     22,  // cid, oldcid, oid
      OP_COLL_REMOVE =  23,  // cid, oid
      OP_COLL_SETATTR = 24,  // cid, attrname, bl
      OP_COLL_RMATTR =  25,  // cid, attrname
      OP_COLL_SETATTRS = 26,  // cid, attrset
      OP_COLL_MOVE =    8,   // newcid, oldcid, oid

      OP_STARTSYNC =    27,  // start a sync 

      OP_RMATTRS =      28,  // cid, oid
      OP_COLL_RENAME =       29,  // cid, newcid

      OP_OMAP_CLEAR = 31,   // cid
      OP_OMAP_SETKEYS = 32, // cid, attrset
      OP_OMAP_RMKEYS = 33,  // cid, keyset
      OP_OMAP_SETHEADER = 34, // cid, header
      OP_SPLIT_COLLECTION = 35, // cid, bits, destination
      OP_SPLIT_COLLECTION2 = 36, /* cid, bits, destination
				    doesn't create the destination */
    };

  private:
    uint64_t ops;
    uint64_t pad_unused_bytes;
    uint32_t largest_data_len, largest_data_off, largest_data_off_in_tbl;
    bufferlist tbl;
    bool sobject_encoding;
    int64_t pool_override;
    bool use_pool_override;

  public:
    void set_pool_override(int64_t pool) {
      pool_override = pool;
    }

    void swap(Transaction& other) {
      std::swap(ops, other.ops);
      std::swap(largest_data_len, other.largest_data_len);
      std::swap(largest_data_off, other.largest_data_off);
      std::swap(largest_data_off_in_tbl, other.largest_data_off_in_tbl);
      tbl.swap(other.tbl);
    }

    void append(Transaction& other) {
      ops += other.ops;
      assert(pad_unused_bytes == 0);
      assert(other.pad_unused_bytes == 0);
      if (other.largest_data_len > largest_data_len) {
	largest_data_len = other.largest_data_len;
	largest_data_off = other.largest_data_off;
	largest_data_off_in_tbl = tbl.length() + other.largest_data_off_in_tbl;
      }
      tbl.append(other.tbl);
    }

    uint64_t get_encoded_bytes() {
      return 1 + 8 + 8 + 4 + 4 + 4 + 4 + tbl.length();
    }

    uint64_t get_num_bytes() {
      return get_encoded_bytes();
    }

    uint32_t get_data_length() {
      return largest_data_len;
    }
    uint32_t get_data_offset() {
      if (largest_data_off_in_tbl) {
	return largest_data_off_in_tbl +
	  sizeof(__u8) +  // encode struct_v
	  sizeof(__u8) +  // encode compat_v
	  sizeof(__u32) + // encode len
	  sizeof(ops) +
	  sizeof(pad_unused_bytes) +
	  sizeof(largest_data_len) +
	  sizeof(largest_data_off) +
	  sizeof(largest_data_off_in_tbl) +
	  sizeof(__u32);  // tbl length
      }
      return 0;  // none
    }
    int get_data_alignment() {
      if (!largest_data_len)
	return -1;
      return (largest_data_off - get_data_offset()) & ~CEPH_PAGE_MASK;
    }

    bool empty() {
      return !ops;
    }

    int get_num_ops() {
      return ops;
    }

    // ---- iterator ----
    class iterator {
      bufferlist::iterator p;
      bool sobject_encoding;
      int64_t pool_override;
      bool use_pool_override;

      iterator(Transaction *t)
	: p(t->tbl.begin()),
	  sobject_encoding(t->sobject_encoding),
	  pool_override(t->pool_override),
	  use_pool_override(t->use_pool_override) {}

      friend class Transaction;

    public:
      bool have_op() {
	return !p.end();
      }
      int get_op() {
	__u32 op;
	::decode(op, p);
	return op;
      }
      void get_bl(bufferlist& bl) {
	::decode(bl, p);
      }
      hobject_t get_oid() {
	hobject_t hoid;
	if (sobject_encoding) {
	  sobject_t soid;
	  ::decode(soid, p);
	  hoid.snap = soid.snap;
	  hoid.oid = soid.oid;
	} else {
	  ::decode(hoid, p);
	  if (use_pool_override && pool_override != -1 &&
	      hoid.pool == -1) {
	    hoid.pool = pool_override;
	  }
	}
	return hoid;
      }
      coll_t get_cid() {
	coll_t c;
	::decode(c, p);
	return c;
      }
      uint64_t get_length() {
	uint64_t len;
	::decode(len, p);
	return len;
      }
      string get_attrname() {
	string s;
	::decode(s, p);
	return s;
      }
      void get_attrset(map<string,bufferptr>& aset) {
	::decode(aset, p);
      }
      void get_attrset(map<string,bufferlist>& aset) {
	::decode(aset, p);
      }
      void get_keyset(set<string> &keys) {
	::decode(keys, p);
      }
      uint32_t get_u32() {
	uint32_t bits;
	::decode(bits, p);
	return bits;
      }
    };

    iterator begin() {
      return iterator(this);
    }
    // -----------------------------

    void start_sync() {
      __u32 op = OP_STARTSYNC;
      ::encode(op, tbl);
      ops++;
    }
    void nop() {
      __u32 op = OP_NOP;
      ::encode(op, tbl);
      ops++;
    }
    void touch(coll_t cid, const hobject_t& oid) {
      __u32 op = OP_TOUCH;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ops++;
    }
    void write(coll_t cid, const hobject_t& oid, uint64_t off, uint64_t len, const bufferlist& data) {
      __u32 op = OP_WRITE;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(off, tbl);
      ::encode(len, tbl);
      assert(len == data.length());
      if (data.length() > largest_data_len) {
	largest_data_len = data.length();
	largest_data_off = off;
	largest_data_off_in_tbl = tbl.length() + sizeof(__u32);  // we are about to 
      }
      ::encode(data, tbl);
      ops++;
    }
    void zero(coll_t cid, const hobject_t& oid, uint64_t off, uint64_t len) {
      __u32 op = OP_ZERO;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(off, tbl);
      ::encode(len, tbl);
      ops++;
    }
    void truncate(coll_t cid, const hobject_t& oid, uint64_t off) {
      __u32 op = OP_TRUNCATE;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(off, tbl);
      ops++;
    }
    void remove(coll_t cid, const hobject_t& oid) {
      __u32 op = OP_REMOVE;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ops++;
    }
    void setattr(coll_t cid, const hobject_t& oid, const char* name, bufferlist& val) {
      string n(name);
      setattr(cid, oid, n, val);
    }
    void setattr(coll_t cid, const hobject_t& oid, const string& s, bufferlist& val) {
      __u32 op = OP_SETATTR;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(s, tbl);
      ::encode(val, tbl);
      ops++;
    }
    void setattrs(coll_t cid, const hobject_t& oid, map<string,bufferptr>& attrset) {
      __u32 op = OP_SETATTRS;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(attrset, tbl);
      ops++;
    }
    void rmattr(coll_t cid, const hobject_t& oid, const char *name) {
      string n(name);
      rmattr(cid, oid, n);
    }
    void rmattr(coll_t cid, const hobject_t& oid, const string& s) {
      __u32 op = OP_RMATTR;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(s, tbl);
      ops++;
    }
    void rmattrs(coll_t cid, const hobject_t& oid) {
      __u32 op = OP_RMATTR;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ops++;
    }
    void clone(coll_t cid, const hobject_t& oid, hobject_t noid) {
      __u32 op = OP_CLONE;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(noid, tbl);
      ops++;
    }
    void clone_range(coll_t cid, const hobject_t& oid, hobject_t noid,
		     uint64_t srcoff, uint64_t srclen, uint64_t dstoff) {
      __u32 op = OP_CLONERANGE2;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(noid, tbl);
      ::encode(srcoff, tbl);
      ::encode(srclen, tbl);
      ::encode(dstoff, tbl);
      ops++;
    }
    void create_collection(coll_t cid) {
      __u32 op = OP_MKCOLL;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ops++;
    }
    void remove_collection(coll_t cid) {
      __u32 op = OP_RMCOLL;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ops++;
    }
    void collection_add(coll_t cid, coll_t ocid, const hobject_t& oid) {
      __u32 op = OP_COLL_ADD;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(ocid, tbl);
      ::encode(oid, tbl);
      ops++;
    }
    void collection_remove(coll_t cid, const hobject_t& oid) {
      __u32 op = OP_COLL_REMOVE;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ops++;
    }
    void collection_move(coll_t cid, coll_t oldcid, const hobject_t& oid) {
      collection_add(cid, oldcid, oid);
      collection_remove(oldcid, oid);
      return;
    }

    void collection_setattr(coll_t cid, const char* name, bufferlist& val) {
      string n(name);
      collection_setattr(cid, n, val);
    }
    void collection_setattr(coll_t cid, const string& name, bufferlist& val) {
      __u32 op = OP_COLL_SETATTR;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(name, tbl);
      ::encode(val, tbl);
      ops++;
    }

    void collection_rmattr(coll_t cid, const char* name) {
      string n(name);
      collection_rmattr(cid, n);
    }
    void collection_rmattr(coll_t cid, const string& name) {
      __u32 op = OP_COLL_RMATTR;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(name, tbl);
      ops++;
    }
    void collection_setattrs(coll_t cid, map<string,bufferptr>& aset) {
      __u32 op = OP_COLL_SETATTRS;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(aset, tbl);
      ops++;
    }
    void collection_rename(coll_t cid, coll_t ncid) {
      __u32 op = OP_COLL_RENAME;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(ncid, tbl);
      ops++;
    }

    /// Remove omap from hoid
    void omap_clear(
      coll_t cid,           ///< [in] Collection containing hoid
      const hobject_t &hoid ///< [in] Object from which to remove omap
      ) {
      __u32 op = OP_OMAP_CLEAR;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(hoid, tbl);
      ops++;
    }
    /// Set keys on hoid omap.  Replaces duplicate keys.
    void omap_setkeys(
      coll_t cid,                           ///< [in] Collection containing hoid
      const hobject_t &hoid,                ///< [in] Object to update
      const map<string, bufferlist> &attrset ///< [in] Replacement keys and values
      ) {
      __u32 op = OP_OMAP_SETKEYS;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(hoid, tbl);
      ::encode(attrset, tbl);
      ops++;
    }
    /// Remove keys from hoid omap
    void omap_rmkeys(
      coll_t cid,             ///< [in] Collection containing hoid
      const hobject_t &hoid,  ///< [in] Object from which to remove the omap
      const set<string> &keys ///< [in] Keys to clear
      ) {
      __u32 op = OP_OMAP_RMKEYS;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(hoid, tbl);
      ::encode(keys, tbl);
      ops++;
    }

    /// Set omap header
    void omap_setheader(
      coll_t cid,             ///< [in] Collection containing hoid
      const hobject_t &hoid,  ///< [in] Object from which to remove the omap
      const bufferlist &bl    ///< [in] Header value
      ) {
      __u32 op = OP_OMAP_SETHEADER;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(hoid, tbl);
      ::encode(bl, tbl);
      ops++;
    }

    /// Split collection based on given prefixes
    void split_collection(
      coll_t cid,
      uint32_t bits,
      uint32_t rem,
      coll_t destination) {
      __u32 op = OP_SPLIT_COLLECTION2;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(bits, tbl);
      ::encode(rem, tbl);
      ::encode(destination, tbl);
      ++ops;
    }

    // etc.
    Transaction() :
      ops(0), pad_unused_bytes(0), largest_data_len(0), largest_data_off(0), largest_data_off_in_tbl(0),
      sobject_encoding(false), pool_override(-1), use_pool_override(false) {}
    Transaction(bufferlist::iterator &dp) :
      ops(0), pad_unused_bytes(0), largest_data_len(0), largest_data_off(0), largest_data_off_in_tbl(0),
      sobject_encoding(false), pool_override(-1), use_pool_override(false) {
      decode(dp);
    }
    Transaction(bufferlist &nbl) :
      ops(0), pad_unused_bytes(0), largest_data_len(0), largest_data_off(0), largest_data_off_in_tbl(0),
      sobject_encoding(false), pool_override(-1), use_pool_override(false) {
      bufferlist::iterator dp = nbl.begin();
      decode(dp); 
    }

    void encode(bufferlist& bl) const {
      ENCODE_START(6, 5, bl);
      ::encode(ops, bl);
      ::encode(pad_unused_bytes, bl);
      ::encode(largest_data_len, bl);
      ::encode(largest_data_off, bl);
      ::encode(largest_data_off_in_tbl, bl);
      ::encode(tbl, bl);
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::iterator &bl) {
      DECODE_START_LEGACY_COMPAT_LEN(6, 5, 5, bl);
      DECODE_OLDEST(2);
      if (struct_v < 4)
	sobject_encoding = true;
      else
	sobject_encoding = false;
      ::decode(ops, bl);
      ::decode(pad_unused_bytes, bl);
      if (struct_v >= 3) {
	::decode(largest_data_len, bl);
	::decode(largest_data_off, bl);
	::decode(largest_data_off_in_tbl, bl);
      }
      ::decode(tbl, bl);
      DECODE_FINISH(bl);
      if (struct_v < 6) {
	use_pool_override = true;
      }
    }

    void dump(ceph::Formatter *f);
    static void generate_test_instances(list<Transaction*>& o);
  };

  struct C_DeleteTransaction : public Context {
    ObjectStore::Transaction *t;
    C_DeleteTransaction(ObjectStore::Transaction *tt) : t(tt) {}
    void finish(int r) {
      delete t;
    }
  };
  template<class T>
  struct C_DeleteTransactionHolder : public Context {
    ObjectStore::Transaction *t;
    T obj;
    C_DeleteTransactionHolder(ObjectStore::Transaction *tt, T &obj) :
      t(tt), obj(obj) {}
    void finish(int r) {
      delete t;
    }
  };

  // synchronous wrappers
  unsigned apply_transaction(Transaction& t, Context *ondisk=0) {
    list<Transaction*> tls;
    tls.push_back(&t);
    return apply_transactions(NULL, tls, ondisk);
  }
  unsigned apply_transaction(Sequencer *osr, Transaction& t, Context *ondisk=0) {
    list<Transaction*> tls;
    tls.push_back(&t);
    return apply_transactions(osr, tls, ondisk);
  }
  unsigned apply_transactions(list<Transaction*>& tls, Context *ondisk=0) {
    return apply_transactions(NULL, tls, ondisk);
  }
  unsigned apply_transactions(Sequencer *osr, list<Transaction*>& tls, Context *ondisk=0);

  virtual int queue_transaction(Sequencer *osr, Transaction* t) = 0;
  virtual int queue_transaction(Sequencer *osr, Transaction *t, Context *onreadable, Context *ondisk=0,
				Context *onreadable_sync=0,
				TrackedOpRef op = TrackedOpRef()) {
    list<Transaction*> tls;
    tls.push_back(t);
    return queue_transactions(osr, tls, onreadable, ondisk, onreadable_sync, op);
  }
  virtual int queue_transactions(Sequencer *osr, list<Transaction*>& tls, Context *onreadable, Context *ondisk=0,
				 Context *onreadable_sync=0,
				 TrackedOpRef op = TrackedOpRef()) = 0;

  int queue_transactions(
    Sequencer *osr,
    list<Transaction*>& tls,
    Context *onreadable,
    Context *oncommit,
    Context *onreadable_sync,
    Context *oncomplete,
    TrackedOpRef op);

  int queue_transaction(
    Sequencer *osr,
    Transaction* t,
    Context *onreadable,
    Context *oncommit,
    Context *onreadable_sync,
    Context *oncomplete,
    TrackedOpRef op) {
    list<Transaction*> tls;
    tls.push_back(t);
    return queue_transactions(
      osr, tls, onreadable, oncommit, onreadable_sync, oncomplete, op);
  }

 public:
  ObjectStore() : logger(NULL) {}
  virtual ~ObjectStore() {}

  // mgmt
  virtual int version_stamp_is_valid(uint32_t *version) { return 1; }
  virtual int update_version_stamp() = 0;
  virtual bool test_mount_in_use() = 0;
  virtual int mount() = 0;
  virtual int umount() = 0;
  virtual int get_max_object_name_length() = 0;
  virtual int mkfs() = 0;  // wipe
  virtual int mkjournal() = 0; // journal only

  virtual int statfs(struct statfs *buf) = 0;

  /**
   * get ideal min value for collection_list_partial()
   *
   * default to some arbitrary values; the implementation will override.
   */
  virtual int get_ideal_list_min() { return 32; }

  /**
   * get ideal max value for collection_list_partial()
   *
   * default to some arbitrary values; the implementation will override.
   */
  virtual int get_ideal_list_max() { return 64; }

  // objects
  virtual bool exists(coll_t cid, const hobject_t& oid) = 0;                   // useful?
  virtual int stat(coll_t cid, const hobject_t& oid, struct stat *st) = 0;     // struct stat?
  virtual int read(coll_t cid, const hobject_t& oid, uint64_t offset, size_t len, bufferlist& bl) = 0;
  virtual int fiemap(coll_t cid, const hobject_t& oid, uint64_t offset, size_t len, bufferlist& bl) = 0;

  virtual int getattr(coll_t cid, const hobject_t& oid, const char *name, bufferptr& value) = 0;
  int getattr(coll_t cid, const hobject_t& oid, const char *name, bufferlist& value) {
    bufferptr bp;
    int r = getattr(cid, oid, name, bp);
    if (bp.length())
      value.push_back(bp);
    return r;
  }
  virtual int getattrs(coll_t cid, const hobject_t& oid, map<string,bufferptr>& aset, bool user_only = false) {return 0;};

   
  // collections
  virtual int list_collections(vector<coll_t>& ls) = 0;
  virtual int collection_version_current(coll_t c, uint32_t *version) { 
    *version = 0;
    return 1;
  }
  virtual bool collection_exists(coll_t c) = 0;
  virtual int collection_getattr(coll_t cid, const char *name,
                                 void *value, size_t size) = 0;
  virtual int collection_getattr(coll_t cid, const char *name, bufferlist& bl) = 0;
  virtual int collection_getattrs(coll_t cid, map<string,bufferptr> &aset) = 0;
  virtual bool collection_empty(coll_t c) = 0;
  virtual int collection_list(coll_t c, vector<hobject_t>& o) = 0;

  /**
   * list partial contents of collection relative to a hash offset/position
   *
   * @param c collection
   * @param start list objects that sort >= this value
   * @param min return at least this many results, unless we reach the end
   * @param max return no more than this many results
   * @param snapid return no objects with snap < snapid
   * @param ls [out] result
   * @param next [out] next item sorts >= this value
   * @return zero on success, or negative error
   */
  virtual int collection_list_partial(coll_t c, hobject_t start,
				      int min, int max, snapid_t snap, 
				      vector<hobject_t> *ls, hobject_t *next) = 0;

  /**
   * list contents of a collection that fall in the range [start, end)
   *
   * @param c collection
   * @param start list object that sort >= this value
   * @param end list objects that sort < this value
   * @param snapid return no objects with snap < snapid
   * @param ls [out] result
   * @return zero on success, or negative error
   */
  virtual int collection_list_range(coll_t c, hobject_t start, hobject_t end,
                                    snapid_t seq, vector<hobject_t> *ls) = 0;

  /// OMAP
  /// Get omap contents
  virtual int omap_get(
    coll_t c,                ///< [in] Collection containing hoid
    const hobject_t &hoid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    map<string, bufferlist> *out /// < [out] Key to value map
    ) = 0;

  /// Get omap header
  virtual int omap_get_header(
    coll_t c,                ///< [in] Collection containing hoid
    const hobject_t &hoid,   ///< [in] Object containing omap
    bufferlist *header       ///< [out] omap header
    ) = 0;

  /// Get keys defined on hoid
  virtual int omap_get_keys(
    coll_t c,              ///< [in] Collection containing hoid
    const hobject_t &hoid, ///< [in] Object containing omap
    set<string> *keys      ///< [out] Keys defined on hoid
    ) = 0;

  /// Get key values
  virtual int omap_get_values(
    coll_t c,                    ///< [in] Collection containing hoid
    const hobject_t &hoid,       ///< [in] Object containing omap
    const set<string> &keys,     ///< [in] Keys to get
    map<string, bufferlist> *out ///< [out] Returned keys and values
    ) = 0;

  /// Filters keys into out which are defined on hoid
  virtual int omap_check_keys(
    coll_t c,                ///< [in] Collection containing hoid
    const hobject_t &hoid,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string> *out         ///< [out] Subset of keys defined on hoid
    ) = 0;

  /**
   * Returns an object map iterator
   *
   * Warning!  The returned iterator is an implicit lock on filestore
   * operations in c.  Do not use filestore methods on c while the returned
   * iterator is live.  (Filling in a transaction is no problem).
   *
   * @return iterator, null on error
   */
  virtual ObjectMap::ObjectMapIterator get_omap_iterator(
    coll_t c,              ///< [in] collection
    const hobject_t &hoid  ///< [in] object
    ) = 0;

  virtual void sync(Context *onsync) {}
  virtual void sync() {}
  virtual void flush() {}
  virtual void sync_and_flush() {}

  virtual int dump_journal(ostream& out) { return -EOPNOTSUPP; }

  virtual int snapshot(const string& name) { return -EOPNOTSUPP; }
    
  virtual void set_fsid(uuid_d u) = 0;
  virtual uuid_d get_fsid() = 0;
};


WRITE_CLASS_ENCODER(ObjectStore::Transaction)

ostream& operator<<(ostream& out, const ObjectStore::Sequencer& s);

#endif
