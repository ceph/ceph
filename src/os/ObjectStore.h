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
#include "common/WorkQueue.h"
#include "ObjectMap.h"

#include <errno.h>
#include <sys/stat.h>
#include <vector>

#if defined(DARWIN) || defined(__FreeBSD__)
#include <sys/statvfs.h>
#else
#include <sys/vfs.h>    /* or <sys/statfs.h> */
#endif /* DARWIN */

class CephContext;

using std::vector;
using std::string;

namespace ceph {
  class Formatter;
}

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
  l_os_j_full,
  l_os_committing,
  l_os_commit,
  l_os_commit_len,
  l_os_commit_lat,
  l_os_oq_max_ops,
  l_os_oq_ops,
  l_os_ops,
  l_os_oq_max_bytes,
  l_os_oq_bytes,
  l_os_bytes,
  l_os_apply_lat,
  l_os_queue_lat,
  l_os_last,
};


/*
 * low-level interface to the local OSD file system
 */

class Logger;


static inline void encode(const map<string,bufferptr> *attrset, bufferlist &bl) {
  ::encode(*attrset, bl);
}

// Flag bits
typedef uint32_t osflagbits_t;
const int SKIP_JOURNAL_REPLAY = 1 << 0;
const int SKIP_MOUNT_OMAP = 1 << 1;

class ObjectStore {
protected:
  string path;

public:
  /**
   * create - create an ObjectStore instance.
   *
   * This is invoked once at initialization time.
   *
   * @param type type of store. This is a string from the configuration file.
   * @param data path (or other descriptor) for data
   * @param journal path (or other descriptor) for journal (optional)
   * @param flags which filestores should check if applicable
   */
  static ObjectStore *create(CephContext *cct,
			     const string& type,
			     const string& data,
			     const string& journal,
			     osflagbits_t flag = 0);

  Logger *logger;

  /**
   * Fetch Object Store statistics.
   *
   * Currently only latency of write and apply times are measured.
   *
   * This appears to be called with nothing locked.
   */
  virtual objectstore_perf_stat_t get_cur_stats() = 0;

  /**
   * a sequencer orders transactions
   *
   * Any transactions queued under a given sequencer will be applied in
   * sequence.  Transactions queued under different sequencers may run
   * in parallel.
   *
   * Clients of ObjectStore create and maintain their own Sequencer objects.
   * When a list of transactions is queued the caller specifies a Sequencer to be used.
   *
   */

  /**
   * ABC for Sequencer implementation, private to the ObjectStore derived class.
   * created in ...::queue_transaction(s)
   */
  struct Sequencer_impl {
    virtual void flush() = 0;

    /**
     * Async flush_commit
     *
     * There are two cases:
     * 1) sequencer is currently idle: the method returns true and
     *    c is deleted
     * 2) sequencer is not idle: the method returns false and c is
     *    called asyncronously with a value of 0 once all transactions
     *    queued on this sequencer prior to the call have been applied
     *    and committed.
     */
    virtual bool flush_commit(
      Context *c ///< [in] context to call upon flush/commit
      ) = 0; ///< @return true if idle, false otherwise

    virtual ~Sequencer_impl() {}
  };

  /**
   * External (opaque) sequencer implementation
   */
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

    /// @see Sequencer_impl::flush_commit()
    bool flush_commit(Context *c) {
      if (!p) {
	delete c;
	return true;
      } else {
	return p->flush_commit(c);
      }
    }
  };

  /*********************************
   *
   * Object Contents and semantics
   *
   * All ObjectStore objects are identified as a named object
   * (ghobject_t and hobject_t) in a named collection (coll_t).
   * ObjectStore operations support the creation, mutation, deletion
   * and enumeration of objects within a collection.  Enumeration is
   * in sorted key order (where keys are sorted by hash). Object names
   * are globally unique.
   *
   * Each object has four distinct parts: byte data, xattrs, omap_header
   * and omap entries.
   *
   * The data portion of an object is conceptually equivalent to a
   * file in a file system. Random and Partial access for both read
   * and operations is required. The ability to have a sparse
   * implementation of the data portion of an object is beneficial for
   * some workloads, but not required. There is a system-wide limit on
   * the maximum size of an object, which is typically around 100 MB.
   *
   * Xattrs are equivalent to the extended attributes of file
   * systems. Xattrs are a set of key/value pairs.  Sub-value access
   * is not required. It is possible to enumerate the set of xattrs in
   * key order.  At the implementation level, xattrs are used
   * exclusively internal to Ceph and the implementer can expect the
   * total size of all of the xattrs on an object to be relatively
   * small, i.e., less than 64KB. Much of Ceph assumes that accessing
   * xattrs on temporally adjacent object accesses (recent past or
   * near future) is inexpensive.
   *
   * omap_header is a single blob of data. It can be read or written
   * in total.
   *
   * Omap entries are conceptually the same as xattrs
   * but in a different address space. In other words, you can have
   * the same key as an xattr and an omap entry and they have distinct
   * values. Enumeration of xattrs doesn't include omap entries and
   * vice versa. The size and access characteristics of omap entries
   * are very different from xattrs. In particular, the value portion
   * of an omap entry can be quite large (MBs).  More importantly, the
   * interface must support efficient range queries on omap entries even
   * when there are a large numbers of entries.
   *
   *********************************/

  /*******************************
   *
   * Collections
   *
   * A collection is simply a grouping of objects. Collections have
   * names (coll_t) and can be enumerated in order.  Like an
   * individual object, a collection also has a set of xattrs.
   *
   *
   */


  /*********************************
   * transaction
   *
   * A Transaction represents a sequence of primitive mutation
   * operations.
   *
   * Three events in the life of a Transaction result in
   * callbacks. Any Transaction can contain any number of callback
   * objects (Context) for any combination of the three classes of
   * callbacks:
   *
   *    on_applied_sync, on_applied, and on_commit.
   *
   * The "on_applied" and "on_applied_sync" callbacks are invoked when
   * the modifications requested by the Transaction are visible to
   * subsequent ObjectStore operations, i.e., the results are
   * readable. The only conceptual difference between on_applied and
   * on_applied_sync is the specific thread and locking environment in
   * which the callbacks operate.  "on_applied_sync" is called
   * directly by an ObjectStore execution thread. It is expected to
   * execute quickly and must not acquire any locks of the calling
   * environment. Conversely, "on_applied" is called from the separate
   * Finisher thread, meaning that it can contend for calling
   * environment locks. NB, on_applied and on_applied sync are
   * sometimes called on_readable and on_readable_sync.
   *
   * The "on_commit" callback is also called from the Finisher thread
   * and indicates that all of the mutations have been durably
   * committed to stable storage (i.e., are now software/hardware
   * crashproof).
   *
   * At the implementation level, each mutation primitive (and its
   * associated data) can be serialized to a single buffer.  That
   * serialization, however, does not copy any data, but (using the
   * bufferlist library) will reference the original buffers.  This
   * implies that the buffer that contains the data being submitted
   * must remain stable until the on_commit callback completes.  In
   * practice, bufferlist handles all of this for you and this
   * subtlety is only relevant if you are referencing an existing
   * buffer via buffer::raw_static.
   *
   * Some implementations of ObjectStore choose to implement their own
   * form of journaling that uses the serialized form of a
   * Transaction. This requires that the encode/decode logic properly
   * version itself and handle version upgrades that might change the
   * format of the encoded Transaction. This has already happened a
   * couple of times and the Transaction object contains some helper
   * variables that aid in this legacy decoding:
   *
   *   sobject_encoding detects an older/simpler version of oid
   *   present in pre-bobtail versions of ceph.  use_pool_override
   *   also detects a situation where the pool of an oid can be
   *   override for legacy operations/buffers.  For non-legacy
   *   implementation of ObjectStore, neither of these fields is
   *   relevant.
   *
   *
   * TRANSACTION ISOLATION
   *
   * Except as noted below, isolation is the responsibility of the
   * caller. In other words, if any storage element (storage element
   * == any of the four portions of an object as described above) is
   * altered by a transaction (including deletion), the caller
   * promises not to attempt to read that element while the
   * transaction is pending (here pending means from the time of
   * issuance until the "on_applied_sync" callback has been
   * received). Violations of isolation need not be detected by
   * ObjectStore and there is no corresponding error mechanism for
   * reporting an isolation violation (crashing would be the
   * appropriate way to report an isolation violation if detected).
   *
   * Enumeration operations may violate transaction isolation as
   * described above when a storage element is being created or
   * deleted as part of a transaction. In this case, ObjectStore is
   * allowed to consider the enumeration operation to either preceed
   * or follow the violating transaction element. In other words, the
   * presence/absence of the mutated element in the enumeration is
   * entirely at the discretion of ObjectStore. The arbitrary ordering
   * applies independently to each transaction element. For example,
   * if a transaction contains two mutating elements "create A" and
   * "delete B". And an enumeration operation is performed while this
   * transaction is pending. It is permissable for ObjectStore to
   * report any of the four possible combinations of the existance of
   * A and B.
   *
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
      OP_OMAP_RMKEYRANGE = 37,  // cid, oid, firstkey, lastkey
      OP_COLL_MOVE_RENAME = 38,   // oldcid, oldoid, newcid, newoid

      OP_SETALLOCHINT = 39,  // cid, oid, object_size, write_size
    };

  private:
    uint64_t ops;
    uint64_t pad_unused_bytes;
    uint32_t largest_data_len, largest_data_off, largest_data_off_in_tbl;
    bufferlist tbl;
    bool sobject_encoding;
    int64_t pool_override;
    bool use_pool_override;
    bool replica;
    bool tolerate_collection_add_enoent;

    list<Context *> on_applied;
    list<Context *> on_commit;
    list<Context *> on_applied_sync;

  public:
    void set_tolerate_collection_add_enoent() {
      tolerate_collection_add_enoent = true;
    }

    /* Operations on callback contexts */
    void register_on_applied(Context *c) {
      if (!c) return;
      on_applied.push_back(c);
    }
    void register_on_commit(Context *c) {
      if (!c) return;
      on_commit.push_back(c);
    }
    void register_on_applied_sync(Context *c) {
      if (!c) return;
      on_applied_sync.push_back(c);
    }
    void register_on_complete(Context *c) {
      if (!c) return;
      RunOnDeleteRef _complete(new RunOnDelete(c));
      register_on_applied(new ContainerContext<RunOnDeleteRef>(_complete));
      register_on_commit(new ContainerContext<RunOnDeleteRef>(_complete));
    }

    static void collect_contexts(
      list<Transaction *> &t,
      Context **out_on_applied,
      Context **out_on_commit,
      Context **out_on_applied_sync) {
      assert(out_on_applied);
      assert(out_on_commit);
      assert(out_on_applied_sync);
      list<Context *> on_applied, on_commit, on_applied_sync;
      for (list<Transaction *>::iterator i = t.begin();
	   i != t.end();
	   ++i) {
	on_applied.splice(on_applied.end(), (*i)->on_applied);
	on_commit.splice(on_commit.end(), (*i)->on_commit);
	on_applied_sync.splice(on_applied_sync.end(), (*i)->on_applied_sync);
      }
      *out_on_applied = C_Contexts::list_to_context(on_applied);
      *out_on_commit = C_Contexts::list_to_context(on_commit);
      *out_on_applied_sync = C_Contexts::list_to_context(on_applied_sync);
    }

    Context *get_on_applied() {
      return C_Contexts::list_to_context(on_applied);
    }
    Context *get_on_commit() {
      return C_Contexts::list_to_context(on_commit);
    }
    Context *get_on_applied_sync() {
      return C_Contexts::list_to_context(on_applied_sync);
    }

    /// For legacy transactions, provide the pool to override the encoded pool with
    void set_pool_override(int64_t pool) {
      pool_override = pool;
    }
    void set_replica() {
      replica = true;
    }
    bool get_replica() { return replica; }

    void swap(Transaction& other) {
      std::swap(ops, other.ops);
      std::swap(largest_data_len, other.largest_data_len);
      std::swap(largest_data_off, other.largest_data_off);
      std::swap(largest_data_off_in_tbl, other.largest_data_off_in_tbl);
      std::swap(on_applied, other.on_applied);
      std::swap(on_commit, other.on_commit);
      std::swap(on_applied_sync, other.on_applied_sync);
      tbl.swap(other.tbl);
    }

    /// Append the operations of the parameter to this Transaction. Those operations are removed from the parameter Transaction
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
      on_applied.splice(on_applied.end(), other.on_applied);
      on_commit.splice(on_commit.end(), other.on_commit);
      on_applied_sync.splice(on_applied_sync.end(), other.on_applied_sync);
    }

    /** Inquires about the Transaction as a whole. */

    /// How big is the encoded Transaction buffer?
    uint64_t get_encoded_bytes() {
      return 1 + 8 + 8 + 4 + 4 + 4 + 4 + tbl.length();
    }

    uint64_t get_num_bytes() {
      return get_encoded_bytes();
    }
    /// Size of largest data buffer to the "write" operation encountered so far
    uint32_t get_data_length() {
      return largest_data_len;
    }
    /// offset within the encoded buffer to the start of the first data buffer that's encoded
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
    /// offset of buffer as aligned to destination within object.
    int get_data_alignment() {
      if (!largest_data_len)
	return -1;
      return (largest_data_off - get_data_offset()) & ~CEPH_PAGE_MASK;
    }
    /// Is the Transaction empty (no operations)
    bool empty() {
      return !ops;
    }
    /// Number of operations in the transation
    int get_num_ops() {
      return ops;
    }

    /**
     * iterator
     *
     * Helper object to parse Transactions.
     *
     * ObjectStore instances use this object to step down the encoded
     * buffer decoding operation codes and parameters as we go.
     *
     */
    class iterator {
      bufferlist::iterator p;
      bool sobject_encoding;
      int64_t pool_override;
      bool use_pool_override;
      bool replica;
      bool _tolerate_collection_add_enoent;

      iterator(Transaction *t)
	: p(t->tbl.begin()),
	  sobject_encoding(t->sobject_encoding),
	  pool_override(t->pool_override),
	  use_pool_override(t->use_pool_override),
	  replica(t->replica),
	  _tolerate_collection_add_enoent(
	    t->tolerate_collection_add_enoent) {}

      friend class Transaction;

    public:
      bool tolerate_collection_add_enoent() const {
	return _tolerate_collection_add_enoent;
      }
      /// true if there are more operations left to be enumerated
      bool have_op() {
	return !p.end();
      }

      /* Decode the specified type of object from the input
       * stream. There is no checking that the encoded data is of the
       * correct type.
       */
      int get_op() {
	__u32 op;
	::decode(op, p);
	return op;
      }
      void get_bl(bufferlist& bl) {
	::decode(bl, p);
      }
      /// Get an oid, recognize various legacy forms and update them.
      ghobject_t get_oid() {
	ghobject_t oid;
	if (sobject_encoding) {
	  sobject_t soid;
	  ::decode(soid, p);
	  oid.hobj.snap = soid.snap;
	  oid.hobj.oid = soid.oid;
	  oid.generation = ghobject_t::NO_GEN;
	  oid.shard_id = ghobject_t::NO_SHARD;
	} else {
	  ::decode(oid, p);
	  if (use_pool_override && pool_override != -1 &&
	      !oid.hobj.is_max() && oid.hobj.pool == -1) {
	    oid.hobj.pool = pool_override;
	  }
	}
	return oid;
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
      string get_key() {
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
      bool get_replica() { return replica; }
    };

    iterator begin() {
      return iterator(this);
    }

    /**
     * Helper functions to encode the various mutation elements of a
     * transaction.  These are 1:1 with the operation codes (see
     * enumeration above).  These routines ensure that the
     * encoder/creator of a transaction gets the right data in the
     * right place. Sadly, there's no corresponding version nor any
     * form of seat belts for the decoder.
     */

    /// Commence a global file system sync operation.
    void start_sync() {
      __u32 op = OP_STARTSYNC;
      ::encode(op, tbl);
      ops++;
    }
    /// noop. 'nuf said
    void nop() {
      __u32 op = OP_NOP;
      ::encode(op, tbl);
      ops++;
    }
    /**
     * touch
     *
     * Ensure the existance of an object in a collection. Create an
     * empty object if necessary
     */
    void touch(coll_t cid, const ghobject_t& oid) {
      __u32 op = OP_TOUCH;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ops++;
    }
    /**
     * Write data to an offset within an object. If the object is too
     * small, it is expanded as needed.  It is possible to specify an
     * offset beyond the current end of an object and it will be
     * expanded as needed. Simple implementations of ObjectStore will
     * just zero the data between the old end of the object and the
     * newly provided data. More sophisticated implementations of
     * ObjectStore will omit the untouched data and store it as a
     * "hole" in the file.
     */
    void write(coll_t cid, const ghobject_t& oid, uint64_t off, uint64_t len,
	       const bufferlist& data) {
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
    /**
     * zero out the indicated byte range within an object. Some
     * ObjectStore instances may optimize this to release the
     * underlying storage space.
     */
    void zero(coll_t cid, const ghobject_t& oid, uint64_t off, uint64_t len) {
      __u32 op = OP_ZERO;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(off, tbl);
      ::encode(len, tbl);
      ops++;
    }
    /// Discard all data in the object beyond the specified size.
    void truncate(coll_t cid, const ghobject_t& oid, uint64_t off) {
      __u32 op = OP_TRUNCATE;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(off, tbl);
      ops++;
    }
    /// Remove an object. All four parts of the object are removed.
    void remove(coll_t cid, const ghobject_t& oid) {
      __u32 op = OP_REMOVE;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ops++;
    }
    /// Set an xattr of an object
    void setattr(coll_t cid, const ghobject_t& oid, const char* name, bufferlist& val) {
      string n(name);
      setattr(cid, oid, n, val);
    }
    /// Set an xattr of an object
    void setattr(coll_t cid, const ghobject_t& oid, const string& s, bufferlist& val) {
      __u32 op = OP_SETATTR;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(s, tbl);
      ::encode(val, tbl);
      ops++;
    }
    /// Set multiple xattrs of an object
    void setattrs(coll_t cid, const ghobject_t& oid, map<string,bufferptr>& attrset) {
      __u32 op = OP_SETATTRS;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(attrset, tbl);
      ops++;
    }
    /// Set multiple xattrs of an object
    void setattrs(coll_t cid, const ghobject_t& oid, map<string,bufferlist>& attrset) {
      __u32 op = OP_SETATTRS;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(attrset, tbl);
      ops++;
    }
    /// remove an xattr from an object
    void rmattr(coll_t cid, const ghobject_t& oid, const char *name) {
      string n(name);
      rmattr(cid, oid, n);
    }
    /// remove an xattr from an object
    void rmattr(coll_t cid, const ghobject_t& oid, const string& s) {
      __u32 op = OP_RMATTR;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(s, tbl);
      ops++;
    }
    /// remove all xattrs from an object
    void rmattrs(coll_t cid, const ghobject_t& oid) {
      __u32 op = OP_RMATTRS;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ops++;
    }
    /**
     * Clone an object into another object.
     *
     * Low-cost (e.g., O(1)) cloning (if supported) is best, but
     * fallback to an O(n) copy is allowed.  All four parts of the
     * object are cloned (data, xattrs, omap header, omap
     * entries).
     *
     * The destination named object may already exist in
     * which case its previous contents are discarded.
     */
    void clone(coll_t cid, const ghobject_t& oid, ghobject_t noid) {
      __u32 op = OP_CLONE;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(noid, tbl);
      ops++;
    }
    /**
     * Clone a byte range from one object to another.
     *
     * The data portion of the destination object receives a copy of a
     * portion of the data from the source object. None of the other
     * three parts of an object is copied from the source.
     */
    void clone_range(coll_t cid, const ghobject_t& oid, ghobject_t noid,
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
    /// Create the collection
    void create_collection(coll_t cid) {
      __u32 op = OP_MKCOLL;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ops++;
    }
    /// remove the collection, the collection must be empty
    void remove_collection(coll_t cid) {
      __u32 op = OP_RMCOLL;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ops++;
    }
    /**
     * Add object to another collection (DEPRECATED)
     *
     * The Object is added to the new collection. This is a virtual
     * add, we now have two names for the same object.  This is only
     * used for conversion of old stores to new stores and is not
     * needed for new implementations unless they expect to make use
     * of the conversion infrastructure.
     */
    void collection_add(coll_t cid, coll_t ocid, const ghobject_t& oid) {
      __u32 op = OP_COLL_ADD;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(ocid, tbl);
      ::encode(oid, tbl);
      ops++;
    }
    void collection_remove(coll_t cid, const ghobject_t& oid) {
      __u32 op = OP_COLL_REMOVE;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ops++;
    }
    void collection_move(coll_t cid, coll_t oldcid, const ghobject_t& oid) {
      collection_add(cid, oldcid, oid);
      collection_remove(oldcid, oid);
      return;
    }
    void collection_move_rename(coll_t oldcid, const ghobject_t& oldoid,
				coll_t cid, const ghobject_t& oid) {
      __u32 op = OP_COLL_MOVE_RENAME;
      ::encode(op, tbl);
      ::encode(oldcid, tbl);
      ::encode(oldoid, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ops++;
    }

    /// Set an xattr on a collection
    void collection_setattr(coll_t cid, const char* name, bufferlist& val) {
      string n(name);
      collection_setattr(cid, n, val);
    }
    /// Set an xattr on a collection
    void collection_setattr(coll_t cid, const string& name, bufferlist& val) {
      __u32 op = OP_COLL_SETATTR;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(name, tbl);
      ::encode(val, tbl);
      ops++;
    }

    /// Remove an xattr from a collection
    void collection_rmattr(coll_t cid, const char* name) {
      string n(name);
      collection_rmattr(cid, n);
    }
    /// Remove an xattr from a collection
    void collection_rmattr(coll_t cid, const string& name) {
      __u32 op = OP_COLL_RMATTR;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(name, tbl);
      ops++;
    }
    /// Set multiple xattrs on a collection
    void collection_setattrs(coll_t cid, map<string,bufferptr>& aset) {
      __u32 op = OP_COLL_SETATTRS;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(aset, tbl);
      ops++;
    }
    /// Set multiple xattrs on a collection
    void collection_setattrs(coll_t cid, map<string,bufferlist>& aset) {
      __u32 op = OP_COLL_SETATTRS;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(aset, tbl);
      ops++;
    }
    /// Change the name of a collection
    void collection_rename(coll_t cid, coll_t ncid) {
      __u32 op = OP_COLL_RENAME;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(ncid, tbl);
      ops++;
    }

    /// Remove omap from oid
    void omap_clear(
      coll_t cid,           ///< [in] Collection containing oid
      const ghobject_t &oid  ///< [in] Object from which to remove omap
      ) {
      __u32 op = OP_OMAP_CLEAR;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ops++;
    }
    /// Set keys on oid omap.  Replaces duplicate keys.
    void omap_setkeys(
      coll_t cid,                           ///< [in] Collection containing oid
      const ghobject_t &oid,                ///< [in] Object to update
      const map<string, bufferlist> &attrset ///< [in] Replacement keys and values
      ) {
      __u32 op = OP_OMAP_SETKEYS;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(attrset, tbl);
      ops++;
    }
    /// Remove keys from oid omap
    void omap_rmkeys(
      coll_t cid,             ///< [in] Collection containing oid
      const ghobject_t &oid,  ///< [in] Object from which to remove the omap
      const set<string> &keys ///< [in] Keys to clear
      ) {
      __u32 op = OP_OMAP_RMKEYS;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(keys, tbl);
      ops++;
    }

    /// Remove key range from oid omap
    void omap_rmkeyrange(
      coll_t cid,             ///< [in] Collection containing oid
      const ghobject_t &oid,  ///< [in] Object from which to remove the omap keys
      const string& first,    ///< [in] first key in range
      const string& last      ///< [in] first key past range, range is [first,last)
      ) {
      __u32 op = OP_OMAP_RMKEYRANGE;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(first, tbl);
      ::encode(last, tbl);
      ops++;
    }

    /// Set omap header
    void omap_setheader(
      coll_t cid,             ///< [in] Collection containing oid
      const ghobject_t &oid,  ///< [in] Object
      const bufferlist &bl    ///< [in] Header value
      ) {
      __u32 op = OP_OMAP_SETHEADER;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(bl, tbl);
      ops++;
    }

    /// Split collection based on given prefixes, objects matching the specified bits/rem are
    /// moved to the new collection
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

    void set_alloc_hint(
      coll_t cid,
      const ghobject_t &oid,
      uint64_t expected_object_size,
      uint64_t expected_write_size
    ) {
      __u32 op = OP_SETALLOCHINT;
      ::encode(op, tbl);
      ::encode(cid, tbl);
      ::encode(oid, tbl);
      ::encode(expected_object_size, tbl);
      ::encode(expected_write_size, tbl);
      ++ops;
    }

    // etc.
    Transaction() :
      ops(0), pad_unused_bytes(0), largest_data_len(0), largest_data_off(0), largest_data_off_in_tbl(0),
      sobject_encoding(false), pool_override(-1), use_pool_override(false),
      replica(false),
      tolerate_collection_add_enoent(false) {}

    Transaction(bufferlist::iterator &dp) :
      ops(0), pad_unused_bytes(0), largest_data_len(0), largest_data_off(0), largest_data_off_in_tbl(0),
      sobject_encoding(false), pool_override(-1), use_pool_override(false),
      replica(false),
      tolerate_collection_add_enoent(false) {
      decode(dp);
    }

    Transaction(bufferlist &nbl) :
      ops(0), pad_unused_bytes(0), largest_data_len(0), largest_data_off(0), largest_data_off_in_tbl(0),
      sobject_encoding(false), pool_override(-1), use_pool_override(false),
      replica(false),
      tolerate_collection_add_enoent(false) {
      bufferlist::iterator dp = nbl.begin();
      decode(dp);
    }

    void encode(bufferlist& bl) const {
      ENCODE_START(7, 5, bl);
      ::encode(ops, bl);
      ::encode(pad_unused_bytes, bl);
      ::encode(largest_data_len, bl);
      ::encode(largest_data_off, bl);
      ::encode(largest_data_off_in_tbl, bl);
      ::encode(tbl, bl);
      ::encode(tolerate_collection_add_enoent, bl);
      ENCODE_FINISH(bl);
    }
    void decode(bufferlist::iterator &bl) {
      DECODE_START_LEGACY_COMPAT_LEN(7, 5, 5, bl);
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
      if (struct_v < 6) {
	use_pool_override = true;
      }
      if (struct_v >= 7) {
	::decode(tolerate_collection_add_enoent, bl);
      }
      DECODE_FINISH(bl);
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

  int queue_transaction_and_cleanup(Sequencer *osr, Transaction* t,
				    ThreadPool::TPHandle *handle = NULL) {
    list<Transaction *> tls;
    tls.push_back(t);
    return queue_transactions(osr, tls, new C_DeleteTransaction(t),
	                      NULL, NULL, TrackedOpRef(), handle);
  }

  int queue_transaction(Sequencer *osr, Transaction *t, Context *onreadable, Context *ondisk=0,
				Context *onreadable_sync=0,
				TrackedOpRef op = TrackedOpRef(),
				ThreadPool::TPHandle *handle = NULL) {
    list<Transaction*> tls;
    tls.push_back(t);
    return queue_transactions(osr, tls, onreadable, ondisk, onreadable_sync,
	                      op, handle);
  }

  int queue_transactions(Sequencer *osr, list<Transaction*>& tls,
			 Context *onreadable, Context *ondisk=0,
			 Context *onreadable_sync=0,
			 TrackedOpRef op = TrackedOpRef(),
			 ThreadPool::TPHandle *handle = NULL) {
    assert(!tls.empty());
    tls.back()->register_on_applied(onreadable);
    tls.back()->register_on_commit(ondisk);
    tls.back()->register_on_applied_sync(onreadable_sync);
    return queue_transactions(osr, tls, op, handle);
  }

  virtual int queue_transactions(
    Sequencer *osr, list<Transaction*>& tls,
    TrackedOpRef op = TrackedOpRef(),
    ThreadPool::TPHandle *handle = NULL) = 0;


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
  ObjectStore(const std::string& path_) : path(path_), logger(NULL) {}
  virtual ~ObjectStore() {}

  // no copying
  ObjectStore(const ObjectStore& o);
  const ObjectStore& operator=(const ObjectStore& o);

  // mgmt
  virtual int version_stamp_is_valid(uint32_t *version) { return 1; }
  virtual int update_version_stamp() = 0;
  virtual bool test_mount_in_use() = 0;
  virtual int mount() = 0;
  virtual int umount() = 0;
  virtual int get_max_object_name_length() = 0;
  virtual int mkfs() = 0;  // wipe
  virtual int mkjournal() = 0; // journal only
  virtual void set_allow_sharded_objects() = 0;
  virtual bool get_allow_sharded_objects() = 0;

  virtual int statfs(struct statfs *buf) = 0;

  /**
   * get the most recent "on-disk format version" supported
   */
  virtual uint32_t get_target_version() = 0;

  /**
   * check the journal uuid/fsid, without opening
   */
  virtual int peek_journal_fsid(uuid_d *fsid) = 0;

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
  virtual int write_meta(const std::string& key,
			 const std::string& value);

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
  virtual int read_meta(const std::string& key,
			std::string *value);

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

  /**
   * Synchronous read operations
   */


  /**
   * exists -- Test for existance of object
   *
   * @param cid collection for object
   * @param oid oid of object
   * @returns true if object exists, false otherwise
   */
  virtual bool exists(coll_t cid, const ghobject_t& oid) = 0;                   // useful?

  /**
   * stat -- get information for an object
   *
   * @param cid collection for object
   * @param oid oid of object
   * @param st output information for the object
   * @param allow_eio if false, assert on -EIO operation failure
   * @returns 0 on success, negative error code on failure.
   */
  virtual int stat(
    coll_t cid,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio = false) = 0; // struct stat?

  /**
   * read -- read a byte range of data from an object
   *
   * Note: if reading from an offset past the end of the object, we
   * return 0 (not, say, -EINVAL).
   *
   * @param cid collection for object
   * @param oid oid of object
   * @param offset location offset of first byte to be read
   * @param len number of bytes to be read
   * @param bl output bufferlist
   * @param allow_eio if false, assert on -EIO operation failure
   * @returns number of bytes read on success, or negative error code on failure.
   */
   virtual int read(
    coll_t cid,
    const ghobject_t& oid,
    uint64_t offset,
    size_t len,
    bufferlist& bl,
    bool allow_eio = false) = 0;

  /**
   * fiemap -- get extent map of data of an object
   *
   * Returns an encoded map of the extents of an object's data portion
   * (map<offset,size>).
   *
   * A non-enlightend implementation is free to return the extent (offset, len)
   * as the sole extent.
   *
   * @param cid collection for object
   * @param oid oid of object
   * @param offset location offset of first byte to be read
   * @param len number of bytes to be read
   * @param bl output bufferlist for extent map information.
   * @returns 0 on success, negative error code on failure.
   */
  virtual int fiemap(coll_t cid, const ghobject_t& oid, uint64_t offset, size_t len, bufferlist& bl) = 0;

  /**
   * getattr -- get an xattr of an object
   *
   * @param cid collection for object
   * @param oid oid of object
   * @param name name of attr to read
   * @param value place to put output result.
   * @returns 0 on success, negative error code on failure.
   */
  virtual int getattr(coll_t cid, const ghobject_t& oid, const char *name, bufferptr& value) = 0;

  /**
   * getattr -- get an xattr of an object
   *
   * @param cid collection for object
   * @param oid oid of object
   * @param name name of attr to read
   * @param value place to put output result.
   * @returns 0 on success, negative error code on failure.
   */
  int getattr(coll_t cid, const ghobject_t& oid, const char *name, bufferlist& value) {
    bufferptr bp;
    int r = getattr(cid, oid, name, bp);
    if (bp.length())
      value.push_back(bp);
    return r;
  }
  int getattr(
    coll_t cid, const ghobject_t& oid,
    const string name, bufferlist& value) {
    bufferptr bp;
    int r = getattr(cid, oid, name.c_str(), bp);
    value.push_back(bp);
    return r;
  }

  /**
   * getattrs -- get all of the xattrs of an object
   *
   * @param cid collection for object
   * @param oid oid of object
   * @param aset place to put output result.
   * @param user_only true -> only user attributes are return else all attributes are returned
   * @returns 0 on success, negative error code on failure.
   */
  virtual int getattrs(coll_t cid, const ghobject_t& oid, map<string,bufferptr>& aset, bool user_only = false) = 0;

  /**
   * getattrs -- get all of the xattrs of an object
   *
   * @param cid collection for object
   * @param oid oid of object
   * @param aset place to put output result.
   * @param user_only true -> only user attributes are return else all attributes are returned
   * @returns 0 on success, negative error code on failure.
   */
  int getattrs(coll_t cid, const ghobject_t& oid, map<string,bufferlist>& aset, bool user_only = false) {
    map<string,bufferptr> bmap;
    int r = getattrs(cid, oid, bmap, user_only);
    for (map<string,bufferptr>::iterator i = bmap.begin();
	i != bmap.end();
	++i) {
      aset[i->first].append(i->second);
    }
    return r;
  }


  // collections

  /**
   * list_collections -- get all of the collections known to this ObjectStore
   *
   * @param ls list of the collections in sorted order.
   * @returns 0 on success, negative error code on failure.
   */
  virtual int list_collections(vector<coll_t>& ls) = 0;

  virtual int collection_version_current(coll_t c, uint32_t *version) {
    *version = 0;
    return 1;
  }
  /**
   * does a collection exist?
   *
   * @param c collection
   * @returns true if it exists, false otherwise
   */
  virtual bool collection_exists(coll_t c) = 0;
  /**
   * collection_getattr - get an xattr of a collection
   *
   * @param cid collection name
   * @param name xattr name
   * @param value pointer of buffer to receive value
   * @param size size of buffer to receive value
   * @returns 0 on success, negative error code on failure
   */
  virtual int collection_getattr(coll_t cid, const char *name,
	                         void *value, size_t size) = 0;
  /**
   * collection_getattr - get an xattr of a collection
   *
   * @param cid collection name
   * @param name xattr name
   * @param bl buffer to receive value
   * @returns 0 on success, negative error code on failure
   */
  virtual int collection_getattr(coll_t cid, const char *name, bufferlist& bl) = 0;
  /**
   * collection_getattrs - get all xattrs of a collection
   *
   * @param cid collection name
   * @param asert map of keys and buffers that contain the values
   * @returns 0 on success, negative error code on failure
   */
  virtual int collection_getattrs(coll_t cid, map<string,bufferptr> &aset) = 0;
  /**
   * is a collection empty?
   *
   * @param c collection
   * @returns true if empty, false otherwise
   */
  virtual bool collection_empty(coll_t c) = 0;

  /**
   * collection_list - get all objects of a collection in sorted order
   *
   * @param c collection name
   * @param o [out] list of objects
   * @returns 0 on success, negative error code on failure
   */
  virtual int collection_list(coll_t c, vector<ghobject_t>& o) = 0;

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
  virtual int collection_list_partial(coll_t c, ghobject_t start,
				      int min, int max, snapid_t snap,
				      vector<ghobject_t> *ls, ghobject_t *next) = 0;

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
  virtual int collection_list_range(coll_t c, ghobject_t start, ghobject_t end,
	                            snapid_t seq, vector<ghobject_t> *ls) = 0;

  //TODO: Remove
  int collection_list(coll_t c, vector<hobject_t>& o);

  int collection_list_partial(coll_t c, hobject_t start,
				      int min, int max, snapid_t snap,
				      vector<hobject_t> *ls, hobject_t *next);

  int collection_list_range(coll_t c, hobject_t start, hobject_t end,
	                            snapid_t seq, vector<hobject_t> *ls);

  /// OMAP
  /// Get omap contents
  virtual int omap_get(
    coll_t c,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    map<string, bufferlist> *out /// < [out] Key to value map
    ) = 0;

  /// Get omap header
  virtual int omap_get_header(
    coll_t c,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    bufferlist *header,      ///< [out] omap header
    bool allow_eio = false ///< [in] don't assert on eio
    ) = 0;

  /// Get keys defined on oid
  virtual int omap_get_keys(
    coll_t c,              ///< [in] Collection containing oid
    const ghobject_t &oid, ///< [in] Object containing omap
    set<string> *keys      ///< [out] Keys defined on oid
    ) = 0;

  /// Get key values
  virtual int omap_get_values(
    coll_t c,                    ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const set<string> &keys,     ///< [in] Keys to get
    map<string, bufferlist> *out ///< [out] Returned keys and values
    ) = 0;

  /// Filters keys into out which are defined on oid
  virtual int omap_check_keys(
    coll_t c,                ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const set<string> &keys, ///< [in] Keys to check
    set<string> *out         ///< [out] Subset of keys defined on oid
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
    const ghobject_t &oid  ///< [in] object
    ) = 0;

  virtual void sync(Context *onsync) {}
  virtual void sync() {}
  virtual void flush() {}
  virtual void sync_and_flush() {}

  virtual int dump_journal(ostream& out) { return -EOPNOTSUPP; }

  virtual int snapshot(const string& name) { return -EOPNOTSUPP; }

  /**
   * Set and get internal fsid for this instance. No external data is modified
   */
  virtual void set_fsid(uuid_d u) = 0;
  virtual uuid_d get_fsid() = 0;

  // DEBUG
  virtual void inject_data_error(const ghobject_t &oid) {}
  virtual void inject_mdata_error(const ghobject_t &oid) {}
};
WRITE_CLASS_ENCODER(ObjectStore::Transaction)

ostream& operator<<(ostream& out, const ObjectStore::Sequencer& s);

#endif
