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
#include "include/stringify.h"
#include "osd/osd_types.h"
#include "common/TrackedOp.h"
#include "common/WorkQueue.h"
#include "ObjectMap.h"

#include <errno.h>
#include <sys/stat.h>
#include <vector>
#include <map>

#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__sun)
#include <sys/statvfs.h>
#else
#include <sys/vfs.h>    /* or <sys/statfs.h> */
#endif

#define OPS_PER_PTR 32

class CephContext;

namespace ceph {
  class Formatter;
}

/*
 * low-level interface to the local OSD file system
 */

class Logger;
class ContextQueue;

static inline void encode(const std::map<std::string,ceph::buffer::ptr> *attrset, ceph::buffer::list &bl) {
  using ceph::encode;
  encode(*attrset, bl);
}

// this isn't the best place for these, but...
void decode_str_str_map_to_bl(ceph::buffer::list::const_iterator& p, ceph::buffer::list *out);
void decode_str_set_to_bl(ceph::buffer::list::const_iterator& p, ceph::buffer::list *out);

// Flag bits
typedef uint32_t osflagbits_t;
const int SKIP_JOURNAL_REPLAY = 1 << 0;
const int SKIP_MOUNT_OMAP = 1 << 1;

class ObjectStore {
protected:
  std::string path;

public:
  CephContext* cct;
  /**
   * create - create an ObjectStore instance.
   *
   * This is invoked once at initialization time.
   *
   * @param type type of store. This is a std::string from the configuration file.
   * @param data path (or other descriptor) for data
   * @param journal path (or other descriptor) for journal (optional)
   * @param flags which filestores should check if applicable
   */
  static ObjectStore *create(CephContext *cct,
			     const std::string& type,
			     const std::string& data,
			     const std::string& journal,
			     osflagbits_t flags = 0);

  /**
   * probe a block device to learn the uuid of the owning OSD
   *
   * @param cct cct
   * @param path path to device
   * @param fsid [out] osd uuid
   */
  static int probe_block_device_fsid(
    CephContext *cct,
    const std::string& path,
    uuid_d *fsid);

  /**
   * Fetch Object Store statistics.
   *
   * Currently only latency of write and apply times are measured.
   *
   * This appears to be called with nothing locked.
   */
  virtual objectstore_perf_stat_t get_cur_stats() = 0;

  /**
   * Fetch Object Store performance counters.
   *
   *
   * This appears to be called with nothing locked.
   */
  virtual const PerfCounters* get_perf_counters() const = 0;

  /**
   * a collection also orders transactions
   *
   * Any transactions queued under a given collection will be applied in
   * sequence.  Transactions queued under different collections may run
   * in parallel.
   *
   * ObjectStore users my get collection handles with open_collection() (or,
   * for bootstrapping a new collection, create_new_collection()).
   */
  struct CollectionImpl : public RefCountedObject {
    const coll_t cid;

    CollectionImpl(const coll_t& c)
      : RefCountedObject(NULL, 0),
	cid(c) {}

    /// wait for any queued transactions to apply
    // block until any previous transactions are visible.  specifically,
    // collection_list and collection_empty need to reflect prior operations.
    virtual void flush() = 0;

    /**
     * Async flush_commit
     *
     * There are two cases:
     * 1) collection is currently idle: the method returns true.  c is
     *    not touched.
     * 2) collection is not idle: the method returns false and c is
     *    called asynchronously with a value of 0 once all transactions
     *    queued on this collection prior to the call have been applied
     *    and committed.
     */
    virtual bool flush_commit(Context *c) = 0;

    const coll_t &get_cid() {
      return cid;
    }
  };
  typedef boost::intrusive_ptr<CollectionImpl> CollectionHandle;


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
   * and write operations is required. The ability to have a sparse
   * implementation of the data portion of an object is beneficial for
   * some workloads, but not required. There is a system-wide limit on
   * the maximum size of an object, which is typically around 100 MB.
   *
   * Xattrs are equivalent to the extended attributes of file
   * systems. Xattrs are a std::set of key/value pairs.  Sub-value access
   * is not required. It is possible to enumerate the std::set of xattrs in
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
   * individual object, a collection also has a std::set of xattrs.
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
   * environment locks. NB, on_applied and on_applied_sync are
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
   * ceph::buffer::list library) will reference the original buffers.  This
   * implies that the buffer that contains the data being submitted
   * must remain stable until the on_commit callback completes.  In
   * practice, ceph::buffer::list handles all of this for you and this
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
   *   overridden for legacy operations/buffers.  For non-legacy
   *   implementations of ObjectStore, neither of these fields are
   *   relevant.
   *
   *
   * TRANSACTION ISOLATION
   *
   * Except as noted above, isolation is the responsibility of the
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
   * allowed to consider the enumeration operation to either precede
   * or follow the violating transaction element. In other words, the
   * presence/absence of the mutated element in the enumeration is
   * entirely at the discretion of ObjectStore. The arbitrary ordering
   * applies independently to each transaction element. For example,
   * if a transaction contains two mutating elements "create A" and
   * "delete B". And an enumeration operation is performed while this
   * transaction is pending. It is permissible for ObjectStore to
   * report any of the four possible combinations of the existence of
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
      OP_COLL_HINT = 40, // cid, type, bl

      OP_TRY_RENAME = 41,   // oldcid, oldoid, newoid

      OP_COLL_SET_BITS = 42, // cid, bits

      OP_MERGE_COLLECTION = 43, // cid, destination
    };

    // Transaction hint type
    enum {
      COLL_HINT_EXPECTED_NUM_OBJECTS = 1,
    };

    struct Op {
      __le32 op;
      __le32 cid;
      __le32 oid;
      __le64 off;
      __le64 len;
      __le32 dest_cid;
      __le32 dest_oid;                  //OP_CLONE, OP_CLONERANGE
      __le64 dest_off;                  //OP_CLONERANGE
      union {
	struct {
	  __le32 hint_type;             //OP_COLL_HINT
	};
	struct {
	  __le32 alloc_hint_flags;      //OP_SETALLOCHINT
	};
      };
      __le64 expected_object_size;      //OP_SETALLOCHINT
      __le64 expected_write_size;       //OP_SETALLOCHINT
      __le32 split_bits;                //OP_SPLIT_COLLECTION2,OP_COLL_SET_BITS,
                                        //OP_MKCOLL
      __le32 split_rem;                 //OP_SPLIT_COLLECTION2
    } __attribute__ ((packed)) ;

    struct TransactionData {
      __le64 ops;
      __le32 largest_data_len;
      __le32 largest_data_off;
      __le32 largest_data_off_in_data_bl;
      __le32 fadvise_flags;

      TransactionData() noexcept :
        ops(0),
        largest_data_len(0),
        largest_data_off(0),
        largest_data_off_in_data_bl(0),
	fadvise_flags(0) { }

      // override default move operations to reset default values
      TransactionData(TransactionData&& other) noexcept :
        ops(other.ops),
        largest_data_len(other.largest_data_len),
        largest_data_off(other.largest_data_off),
        largest_data_off_in_data_bl(other.largest_data_off_in_data_bl),
        fadvise_flags(other.fadvise_flags) {
        other.ops = 0;
        other.largest_data_len = 0;
        other.largest_data_off = 0;
        other.largest_data_off_in_data_bl = 0;
        other.fadvise_flags = 0;
      }
      TransactionData& operator=(TransactionData&& other) noexcept {
        ops = other.ops;
        largest_data_len = other.largest_data_len;
        largest_data_off = other.largest_data_off;
        largest_data_off_in_data_bl = other.largest_data_off_in_data_bl;
        fadvise_flags = other.fadvise_flags;
        other.ops = 0;
        other.largest_data_len = 0;
        other.largest_data_off = 0;
        other.largest_data_off_in_data_bl = 0;
        other.fadvise_flags = 0;
        return *this;
      }

      TransactionData(const TransactionData& other) = default;
      TransactionData& operator=(const TransactionData& other) = default;

      void encode(ceph::buffer::list& bl) const {
        bl.append((char*)this, sizeof(TransactionData));
      }
      void decode(ceph::buffer::list::const_iterator &bl) {
        bl.copy(sizeof(TransactionData), (char*)this);
      }
    } __attribute__ ((packed)) ;

  private:
    TransactionData data;

    std::map<coll_t, __le32> coll_index;
    std::map<ghobject_t, __le32> object_index;

    __le32 coll_id {0};
    __le32 object_id {0};

    ceph::buffer::list data_bl;
    ceph::buffer::list op_bl;

    std::list<Context *> on_applied;
    std::list<Context *> on_commit;
    std::list<Context *> on_applied_sync;

  public:
    Transaction() = default;

    explicit Transaction(ceph::buffer::list::const_iterator &dp) {
      decode(dp);
    }
    explicit Transaction(ceph::buffer::list &nbl) {
      auto dp = nbl.cbegin();
      decode(dp);
    }

    // override default move operations to reset default values
    Transaction(Transaction&& other) noexcept :
      data(std::move(other.data)),
      coll_index(std::move(other.coll_index)),
      object_index(std::move(other.object_index)),
      coll_id(other.coll_id),
      object_id(other.object_id),
      data_bl(std::move(other.data_bl)),
      op_bl(std::move(other.op_bl)),
      on_applied(std::move(other.on_applied)),
      on_commit(std::move(other.on_commit)),
      on_applied_sync(std::move(other.on_applied_sync)) {
      other.coll_id = 0;
      other.object_id = 0;
    }

    Transaction& operator=(Transaction&& other) noexcept {
      data = std::move(other.data);
      coll_index = std::move(other.coll_index);
      object_index = std::move(other.object_index);
      coll_id = other.coll_id;
      object_id = other.object_id;
      data_bl = std::move(other.data_bl);
      op_bl = std::move(other.op_bl);
      on_applied = std::move(other.on_applied);
      on_commit = std::move(other.on_commit);
      on_applied_sync = std::move(other.on_applied_sync);
      other.coll_id = 0;
      other.object_id = 0;
      return *this;
    }

    Transaction(const Transaction& other) = default;
    Transaction& operator=(const Transaction& other) = default;

    // expose object_index for FileStore::Op's benefit
    const std::map<ghobject_t, __le32>& get_object_index() const {
      return object_index;
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
      RunOnDeleteRef _complete (std::make_shared<RunOnDelete>(c));
      register_on_applied(new ContainerContext<RunOnDeleteRef>(_complete));
      register_on_commit(new ContainerContext<RunOnDeleteRef>(_complete));
    }
    bool has_contexts() const {
      return
	!on_commit.empty() ||
	!on_applied.empty() ||
	!on_applied_sync.empty();
    }

    static void collect_contexts(
      std::vector<Transaction>& t,
      Context **out_on_applied,
      Context **out_on_commit,
      Context **out_on_applied_sync) {
      ceph_assert(out_on_applied);
      ceph_assert(out_on_commit);
      ceph_assert(out_on_applied_sync);
      std::list<Context *> on_applied, on_commit, on_applied_sync;
      for (auto& i : t) {
	on_applied.splice(on_applied.end(), i.on_applied);
	on_commit.splice(on_commit.end(), i.on_commit);
	on_applied_sync.splice(on_applied_sync.end(), i.on_applied_sync);
      }
      *out_on_applied = C_Contexts::list_to_context(on_applied);
      *out_on_commit = C_Contexts::list_to_context(on_commit);
      *out_on_applied_sync = C_Contexts::list_to_context(on_applied_sync);
    }
    static void collect_contexts(
      std::vector<Transaction>& t,
      std::list<Context*> *out_on_applied,
      std::list<Context*> *out_on_commit,
      std::list<Context*> *out_on_applied_sync) {
      ceph_assert(out_on_applied);
      ceph_assert(out_on_commit);
      ceph_assert(out_on_applied_sync);
      for (auto& i : t) {
	out_on_applied->splice(out_on_applied->end(), i.on_applied);
	out_on_commit->splice(out_on_commit->end(), i.on_commit);
	out_on_applied_sync->splice(out_on_applied_sync->end(),
				    i.on_applied_sync);
      }
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

    void set_fadvise_flags(uint32_t flags) {
      data.fadvise_flags = flags;
    }
    void set_fadvise_flag(uint32_t flag) {
      data.fadvise_flags = data.fadvise_flags | flag;
    }
    uint32_t get_fadvise_flags() { return data.fadvise_flags; }

    void swap(Transaction& other) noexcept {
      std::swap(data, other.data);
      std::swap(on_applied, other.on_applied);
      std::swap(on_commit, other.on_commit);
      std::swap(on_applied_sync, other.on_applied_sync);

      std::swap(coll_index, other.coll_index);
      std::swap(object_index, other.object_index);
      std::swap(coll_id, other.coll_id);
      std::swap(object_id, other.object_id);
      op_bl.swap(other.op_bl);
      data_bl.swap(other.data_bl);
    }

    void _update_op(Op* op,
      std::vector<__le32> &cm,
      std::vector<__le32> &om) {

      switch (op->op) {
      case OP_NOP:
        break;

      case OP_TOUCH:
      case OP_REMOVE:
      case OP_SETATTR:
      case OP_SETATTRS:
      case OP_RMATTR:
      case OP_RMATTRS:
      case OP_COLL_REMOVE:
      case OP_OMAP_CLEAR:
      case OP_OMAP_SETKEYS:
      case OP_OMAP_RMKEYS:
      case OP_OMAP_RMKEYRANGE:
      case OP_OMAP_SETHEADER:
      case OP_WRITE:
      case OP_ZERO:
      case OP_TRUNCATE:
      case OP_SETALLOCHINT:
        ceph_assert(op->cid < cm.size());
        ceph_assert(op->oid < om.size());
        op->cid = cm[op->cid];
        op->oid = om[op->oid];
        break;

      case OP_CLONERANGE2:
      case OP_CLONE:
        ceph_assert(op->cid < cm.size());
        ceph_assert(op->oid < om.size());
        ceph_assert(op->dest_oid < om.size());
        op->cid = cm[op->cid];
        op->oid = om[op->oid];
        op->dest_oid = om[op->dest_oid];
        break;

      case OP_MKCOLL:
      case OP_RMCOLL:
      case OP_COLL_SETATTR:
      case OP_COLL_RMATTR:
      case OP_COLL_SETATTRS:
      case OP_COLL_HINT:
      case OP_COLL_SET_BITS:
        ceph_assert(op->cid < cm.size());
        op->cid = cm[op->cid];
        break;

      case OP_COLL_ADD:
        ceph_assert(op->cid < cm.size());
        ceph_assert(op->oid < om.size());
        ceph_assert(op->dest_cid < om.size());
        op->cid = cm[op->cid];
        op->dest_cid = cm[op->dest_cid];
        op->oid = om[op->oid];
        break;

      case OP_COLL_MOVE_RENAME:
        ceph_assert(op->cid < cm.size());
        ceph_assert(op->oid < om.size());
        ceph_assert(op->dest_cid < cm.size());
        ceph_assert(op->dest_oid < om.size());
        op->cid = cm[op->cid];
        op->oid = om[op->oid];
        op->dest_cid = cm[op->dest_cid];
        op->dest_oid = om[op->dest_oid];
        break;

      case OP_TRY_RENAME:
        ceph_assert(op->cid < cm.size());
        ceph_assert(op->oid < om.size());
        ceph_assert(op->dest_oid < om.size());
        op->cid = cm[op->cid];
        op->oid = om[op->oid];
        op->dest_oid = om[op->dest_oid];
	break;

      case OP_SPLIT_COLLECTION2:
        ceph_assert(op->cid < cm.size());
	ceph_assert(op->dest_cid < cm.size());
        op->cid = cm[op->cid];
        op->dest_cid = cm[op->dest_cid];
        break;

      case OP_MERGE_COLLECTION:
        ceph_assert(op->cid < cm.size());
	ceph_assert(op->dest_cid < cm.size());
        op->cid = cm[op->cid];
        op->dest_cid = cm[op->dest_cid];
        break;

      default:
        ceph_abort_msg("Unknown OP");
      }
    }
    void _update_op_bl(
      ceph::buffer::list& bl,
      std::vector<__le32> &cm,
      std::vector<__le32> &om) {
      for (auto& bp : bl.buffers()) {
        ceph_assert(bp.length() % sizeof(Op) == 0);

        char* raw_p = const_cast<char*>(bp.c_str());
        char* raw_end = raw_p + bp.length();
        while (raw_p < raw_end) {
          _update_op(reinterpret_cast<Op*>(raw_p), cm, om);
          raw_p += sizeof(Op);
        }
      }
    }
    /// Append the operations of the parameter to this Transaction. Those operations are removed from the parameter Transaction
    void append(Transaction& other) {

      data.ops += other.data.ops;
      if (other.data.largest_data_len > data.largest_data_len) {
	data.largest_data_len = other.data.largest_data_len;
	data.largest_data_off = other.data.largest_data_off;
	data.largest_data_off_in_data_bl = data_bl.length() + other.data.largest_data_off_in_data_bl;
      }
      data.fadvise_flags |= other.data.fadvise_flags;
      on_applied.splice(on_applied.end(), other.on_applied);
      on_commit.splice(on_commit.end(), other.on_commit);
      on_applied_sync.splice(on_applied_sync.end(), other.on_applied_sync);

      //append coll_index & object_index
      std::vector<__le32> cm(other.coll_index.size());
      std::map<coll_t, __le32>::iterator coll_index_p;
      for (coll_index_p = other.coll_index.begin();
           coll_index_p != other.coll_index.end();
           ++coll_index_p) {
        cm[coll_index_p->second] = _get_coll_id(coll_index_p->first);
      }

      std::vector<__le32> om(other.object_index.size());
      std::map<ghobject_t, __le32>::iterator object_index_p;
      for (object_index_p = other.object_index.begin();
           object_index_p != other.object_index.end();
           ++object_index_p) {
        om[object_index_p->second] = _get_object_id(object_index_p->first);
      }      

      //the other.op_bl SHOULD NOT be changes during append operation,
      //we use additional ceph::buffer::list to avoid this problem
      ceph::buffer::list other_op_bl;
      {
        ceph::buffer::ptr other_op_bl_ptr(other.op_bl.length());
        other.op_bl.copy(0, other.op_bl.length(), other_op_bl_ptr.c_str());
        other_op_bl.append(std::move(other_op_bl_ptr));
      }

      //update other_op_bl with cm & om
      //When the other is appended to current transaction, all coll_index and
      //object_index in other.op_buffer should be updated by new index of the
      //combined transaction
      _update_op_bl(other_op_bl, cm, om);

      //append op_bl
      op_bl.append(other_op_bl);
      //append data_bl
      data_bl.append(other.data_bl);
    }

    /** Inquires about the Transaction as a whole. */

    /// How big is the encoded Transaction buffer?
    uint64_t get_encoded_bytes() {
      //layout: data_bl + op_bl + coll_index + object_index + data

      // coll_index size, object_index size and sizeof(transaction_data)
      // all here, so they may be computed at compile-time
      size_t final_size = sizeof(__u32) * 2 + sizeof(data);

      // coll_index second and object_index second
      final_size += (coll_index.size() + object_index.size()) * sizeof(__le32);

      // coll_index first
      for (auto p = coll_index.begin(); p != coll_index.end(); ++p) {
	final_size += p->first.encoded_size();
      }

      // object_index first
      for (auto p = object_index.begin(); p != object_index.end(); ++p) {
	final_size += p->first.encoded_size();
      }

      return data_bl.length() +
	op_bl.length() +
	final_size;
    }

    /// Retain old version for regression testing purposes
    uint64_t get_encoded_bytes_test() {
      using ceph::encode;
      //layout: data_bl + op_bl + coll_index + object_index + data
      ceph::buffer::list bl;
      encode(coll_index, bl);
      encode(object_index, bl);

      return data_bl.length() +
	op_bl.length() +
	bl.length() +
	sizeof(data);
    }

    uint64_t get_num_bytes() {
      return get_encoded_bytes();
    }
    /// Size of largest data buffer to the "write" operation encountered so far
    uint32_t get_data_length() {
      return data.largest_data_len;
    }
    /// offset within the encoded buffer to the start of the largest data buffer that's encoded
    uint32_t get_data_offset() {
      if (data.largest_data_off_in_data_bl) {
	return data.largest_data_off_in_data_bl +
	  sizeof(__u8) +      // encode struct_v
	  sizeof(__u8) +      // encode compat_v
	  sizeof(__u32) +     // encode len
	  sizeof(__u32);      // data_bl len
      }
      return 0;  // none
    }
    /// offset of buffer as aligned to destination within object.
    int get_data_alignment() {
      if (!data.largest_data_len)
	return 0;
      return (0 - get_data_offset()) & ~CEPH_PAGE_MASK;
    }
    /// Is the Transaction empty (no operations)
    bool empty() {
      return !data.ops;
    }
    /// Number of operations in the transaction
    int get_num_ops() {
      return data.ops;
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
      Transaction *t;

      uint64_t ops;
      char* op_buffer_p;

      ceph::buffer::list::const_iterator data_bl_p;

    public:
      std::vector<coll_t> colls;
      std::vector<ghobject_t> objects;

    private:
      explicit iterator(Transaction *t)
        : t(t),
	  data_bl_p(t->data_bl.cbegin()),
          colls(t->coll_index.size()),
          objects(t->object_index.size()) {

        ops = t->data.ops;
        op_buffer_p = t->op_bl.c_str();

        std::map<coll_t, __le32>::iterator coll_index_p;
        for (coll_index_p = t->coll_index.begin();
             coll_index_p != t->coll_index.end();
             ++coll_index_p) {
          colls[coll_index_p->second] = coll_index_p->first;
        }

        std::map<ghobject_t, __le32>::iterator object_index_p;
        for (object_index_p = t->object_index.begin();
             object_index_p != t->object_index.end();
             ++object_index_p) {
          objects[object_index_p->second] = object_index_p->first;
        }
      }

      friend class Transaction;

    public:

      bool have_op() {
        return ops > 0;
      }
      Op* decode_op() {
        ceph_assert(ops > 0);

        Op* op = reinterpret_cast<Op*>(op_buffer_p);
        op_buffer_p += sizeof(Op);
        ops--;

        return op;
      }
      std::string decode_string() {
	using ceph::decode;
        std::string s;
        decode(s, data_bl_p);
        return s;
      }
      void decode_bp(ceph::buffer::ptr& bp) {
	using ceph::decode;
        decode(bp, data_bl_p);
      }
      void decode_bl(ceph::buffer::list& bl) {
	using ceph::decode;
        decode(bl, data_bl_p);
      }
      void decode_attrset(std::map<std::string,ceph::buffer::ptr>& aset) {
	using ceph::decode;
        decode(aset, data_bl_p);
      }
      void decode_attrset(std::map<std::string,ceph::buffer::list>& aset) {
	using ceph::decode;
        decode(aset, data_bl_p);
      }
      void decode_attrset_bl(ceph::buffer::list *pbl) {
	decode_str_str_map_to_bl(data_bl_p, pbl);
      }
      void decode_keyset(std::set<std::string> &keys){
	using ceph::decode;
        decode(keys, data_bl_p);
      }
      void decode_keyset_bl(ceph::buffer::list *pbl){
        decode_str_set_to_bl(data_bl_p, pbl);
      }

      const ghobject_t &get_oid(__le32 oid_id) {
        ceph_assert(oid_id < objects.size());
        return objects[oid_id];
      }
      const coll_t &get_cid(__le32 cid_id) {
        ceph_assert(cid_id < colls.size());
        return colls[cid_id];
      }
      uint32_t get_fadvise_flags() const {
	return t->get_fadvise_flags();
      }
    };

    iterator begin() {
       return iterator(this);
    }

private:
    void _build_actions_from_tbl();

    /**
     * Helper functions to encode the various mutation elements of a
     * transaction.  These are 1:1 with the operation codes (see
     * enumeration above).  These routines ensure that the
     * encoder/creator of a transaction gets the right data in the
     * right place. Sadly, there's no corresponding version nor any
     * form of seat belts for the decoder.
     */
    Op* _get_next_op() {
      if (op_bl.get_append_buffer_unused_tail_length() < sizeof(Op)) {
        op_bl.reserve(sizeof(Op) * OPS_PER_PTR);
      }
      // append_hole ensures bptr merging. Even huge number of ops
      // shouldn't result in overpopulating bl::_buffers.
      char* const p = op_bl.append_hole(sizeof(Op)).c_str();
      memset(p, 0, sizeof(Op));
      return reinterpret_cast<Op*>(p);
    }
    __le32 _get_coll_id(const coll_t& coll) {
      std::map<coll_t, __le32>::iterator c = coll_index.find(coll);
      if (c != coll_index.end())
        return c->second;

      __le32 index_id = coll_id++;
      coll_index[coll] = index_id;
      return index_id;
    }
    __le32 _get_object_id(const ghobject_t& oid) {
      std::map<ghobject_t, __le32>::iterator o = object_index.find(oid);
      if (o != object_index.end())
        return o->second;

      __le32 index_id = object_id++;
      object_index[oid] = index_id;
      return index_id;
    }

public:
    /// noop. 'nuf said
    void nop() {
      Op* _op = _get_next_op();
      _op->op = OP_NOP;
      data.ops++;
    }
    /**
     * touch
     *
     * Ensure the existance of an object in a collection. Create an
     * empty object if necessary
     */
    void touch(const coll_t& cid, const ghobject_t& oid) {
      Op* _op = _get_next_op();
      _op->op = OP_TOUCH;
      _op->cid = _get_coll_id(cid);
      _op->oid = _get_object_id(oid);
      data.ops++;
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
     *
     * Note that a 0-length write does not affect the size of the object.
     */
    void write(const coll_t& cid, const ghobject_t& oid, uint64_t off, uint64_t len,
	       const ceph::buffer::list& write_data, uint32_t flags = 0) {
      using ceph::encode;
      uint32_t orig_len = data_bl.length();
      Op* _op = _get_next_op();
      _op->op = OP_WRITE;
      _op->cid = _get_coll_id(cid);
      _op->oid = _get_object_id(oid);
      _op->off = off;
      _op->len = len;
      encode(write_data, data_bl);

      ceph_assert(len == write_data.length());
      data.fadvise_flags = data.fadvise_flags | flags;
      if (write_data.length() > data.largest_data_len) {
	data.largest_data_len = write_data.length();
	data.largest_data_off = off;
	data.largest_data_off_in_data_bl = orig_len + sizeof(__u32);  // we are about to
      }
      data.ops++;
    }
    /**
     * zero out the indicated byte range within an object. Some
     * ObjectStore instances may optimize this to release the
     * underlying storage space.
     *
     * If the zero range extends beyond the end of the object, the object
     * size is extended, just as if we were writing a buffer full of zeros.
     * EXCEPT if the length is 0, in which case (just like a 0-length write)
     * we do not adjust the object size.
     */
    void zero(const coll_t& cid, const ghobject_t& oid, uint64_t off, uint64_t len) {
      Op* _op = _get_next_op();
      _op->op = OP_ZERO;
      _op->cid = _get_coll_id(cid);
      _op->oid = _get_object_id(oid);
      _op->off = off;
      _op->len = len;
      data.ops++;
    }
    /// Discard all data in the object beyond the specified size.
    void truncate(const coll_t& cid, const ghobject_t& oid, uint64_t off) {
      Op* _op = _get_next_op();
      _op->op = OP_TRUNCATE;
      _op->cid = _get_coll_id(cid);
      _op->oid = _get_object_id(oid);
      _op->off = off;
      data.ops++;
    }
    /// Remove an object. All four parts of the object are removed.
    void remove(const coll_t& cid, const ghobject_t& oid) {
      Op* _op = _get_next_op();
      _op->op = OP_REMOVE;
      _op->cid = _get_coll_id(cid);
      _op->oid = _get_object_id(oid);
      data.ops++;
    }
    /// Set an xattr of an object
    void setattr(const coll_t& cid, const ghobject_t& oid, const char* name, ceph::buffer::list& val) {
      std::string n(name);
      setattr(cid, oid, n, val);
    }
    /// Set an xattr of an object
    void setattr(const coll_t& cid, const ghobject_t& oid, const std::string& s, ceph::buffer::list& val) {
      using ceph::encode;
      Op* _op = _get_next_op();
      _op->op = OP_SETATTR;
      _op->cid = _get_coll_id(cid);
      _op->oid = _get_object_id(oid);
      encode(s, data_bl);
      encode(val, data_bl);
      data.ops++;
    }
    /// Set multiple xattrs of an object
    void setattrs(const coll_t& cid, const ghobject_t& oid, const std::map<std::string,ceph::buffer::ptr>& attrset) {
      using ceph::encode;
      Op* _op = _get_next_op();
      _op->op = OP_SETATTRS;
      _op->cid = _get_coll_id(cid);
      _op->oid = _get_object_id(oid);
      encode(attrset, data_bl);
      data.ops++;
    }
    /// Set multiple xattrs of an object
    void setattrs(const coll_t& cid, const ghobject_t& oid, const std::map<std::string,ceph::buffer::list>& attrset) {
      using ceph::encode;
      Op* _op = _get_next_op();
      _op->op = OP_SETATTRS;
      _op->cid = _get_coll_id(cid);
      _op->oid = _get_object_id(oid);
      encode(attrset, data_bl);
      data.ops++;
    }
    /// remove an xattr from an object
    void rmattr(const coll_t& cid, const ghobject_t& oid, const char *name) {
      std::string n(name);
      rmattr(cid, oid, n);
    }
    /// remove an xattr from an object
    void rmattr(const coll_t& cid, const ghobject_t& oid, const std::string& s) {
      using ceph::encode;
      Op* _op = _get_next_op();
      _op->op = OP_RMATTR;
      _op->cid = _get_coll_id(cid);
      _op->oid = _get_object_id(oid);
      encode(s, data_bl);
      data.ops++;
    }
    /// remove all xattrs from an object
    void rmattrs(const coll_t& cid, const ghobject_t& oid) {
      Op* _op = _get_next_op();
      _op->op = OP_RMATTRS;
      _op->cid = _get_coll_id(cid);
      _op->oid = _get_object_id(oid);
      data.ops++;
    }
    /**
     * Clone an object into another object.
     *
     * Low-cost (e.g., O(1)) cloning (if supported) is best, but
     * fallback to an O(n) copy is allowed.  All four parts of the
     * object are cloned (data, xattrs, omap header, omap
     * entries).
     *
     * The destination named object may already exist, in
     * which case its previous contents are discarded.
     */
    void clone(const coll_t& cid, const ghobject_t& oid,
	       const ghobject_t& noid) {
      Op* _op = _get_next_op();
      _op->op = OP_CLONE;
      _op->cid = _get_coll_id(cid);
      _op->oid = _get_object_id(oid);
      _op->dest_oid = _get_object_id(noid);
      data.ops++;
    }
    /**
     * Clone a byte range from one object to another.
     *
     * The data portion of the destination object receives a copy of a
     * portion of the data from the source object. None of the other
     * three parts of an object is copied from the source.
     *
     * The destination object size may be extended to the dstoff + len.
     *
     * The source range *must* overlap with the source object data. If it does
     * not the result is undefined.
     */
    void clone_range(const coll_t& cid, const ghobject_t& oid,
		     const ghobject_t& noid,
		     uint64_t srcoff, uint64_t srclen, uint64_t dstoff) {
      Op* _op = _get_next_op();
      _op->op = OP_CLONERANGE2;
      _op->cid = _get_coll_id(cid);
      _op->oid = _get_object_id(oid);
      _op->dest_oid = _get_object_id(noid);
      _op->off = srcoff;
      _op->len = srclen;
      _op->dest_off = dstoff;
      data.ops++;
    }

    /// Create the collection
    void create_collection(const coll_t& cid, int bits) {
      Op* _op = _get_next_op();
      _op->op = OP_MKCOLL;
      _op->cid = _get_coll_id(cid);
      _op->split_bits = bits;
      data.ops++;
    }

    /**
     * Give the collection a hint.
     *
     * @param cid  - collection id.
     * @param type - hint type.
     * @param hint - the hint payload, which contains the customized
     *               data along with the hint type.
     */
    void collection_hint(const coll_t& cid, uint32_t type, const ceph::buffer::list& hint) {
      using ceph::encode;
      Op* _op = _get_next_op();
      _op->op = OP_COLL_HINT;
      _op->cid = _get_coll_id(cid);
      _op->hint_type = type;
      encode(hint, data_bl);
      data.ops++;
    }

    /// remove the collection, the collection must be empty
    void remove_collection(const coll_t& cid) {
      Op* _op = _get_next_op();
      _op->op = OP_RMCOLL;
      _op->cid = _get_coll_id(cid);
      data.ops++;
    }
    void collection_move(const coll_t& cid, const coll_t &oldcid, const ghobject_t& oid)
      __attribute__ ((deprecated)) {
	// NOTE: we encode this as a fixed combo of ADD + REMOVE.  they
	// always appear together, so this is effectively a single MOVE.
	Op* _op = _get_next_op();
	_op->op = OP_COLL_ADD;
	_op->cid = _get_coll_id(oldcid);
	_op->oid = _get_object_id(oid);
	_op->dest_cid = _get_coll_id(cid);
	data.ops++;

	_op = _get_next_op();
	_op->op = OP_COLL_REMOVE;
	_op->cid = _get_coll_id(oldcid);
	_op->oid = _get_object_id(oid);
	data.ops++;
      }
    void collection_move_rename(const coll_t& oldcid, const ghobject_t& oldoid,
				const coll_t &cid, const ghobject_t& oid) {
      Op* _op = _get_next_op();
      _op->op = OP_COLL_MOVE_RENAME;
      _op->cid = _get_coll_id(oldcid);
      _op->oid = _get_object_id(oldoid);
      _op->dest_cid = _get_coll_id(cid);
      _op->dest_oid = _get_object_id(oid);
      data.ops++;
    }
    void try_rename(const coll_t &cid, const ghobject_t& oldoid,
                    const ghobject_t& oid) {
      Op* _op = _get_next_op();
      _op->op = OP_TRY_RENAME;
      _op->cid = _get_coll_id(cid);
      _op->oid = _get_object_id(oldoid);
      _op->dest_oid = _get_object_id(oid);
      data.ops++;
    }

    /// Remove omap from oid
    void omap_clear(
      const coll_t &cid,           ///< [in] Collection containing oid
      const ghobject_t &oid  ///< [in] Object from which to remove omap
      ) {
      Op* _op = _get_next_op();
      _op->op = OP_OMAP_CLEAR;
      _op->cid = _get_coll_id(cid);
      _op->oid = _get_object_id(oid);
      data.ops++;
    }
    /// Set keys on oid omap.  Replaces duplicate keys.
    void omap_setkeys(
      const coll_t& cid,                           ///< [in] Collection containing oid
      const ghobject_t &oid,                ///< [in] Object to update
      const std::map<std::string, ceph::buffer::list> &attrset ///< [in] Replacement keys and values
      ) {
      using ceph::encode;
      Op* _op = _get_next_op();
      _op->op = OP_OMAP_SETKEYS;
      _op->cid = _get_coll_id(cid);
      _op->oid = _get_object_id(oid);
      encode(attrset, data_bl);
      data.ops++;
    }

    /// Set keys on an oid omap (ceph::buffer::list variant).
    void omap_setkeys(
      const coll_t &cid,                           ///< [in] Collection containing oid
      const ghobject_t &oid,                ///< [in] Object to update
      const ceph::buffer::list &attrset_bl          ///< [in] Replacement keys and values
      ) {
      Op* _op = _get_next_op();
      _op->op = OP_OMAP_SETKEYS;
      _op->cid = _get_coll_id(cid);
      _op->oid = _get_object_id(oid);
      data_bl.append(attrset_bl);
      data.ops++;
    }

    /// Remove keys from oid omap
    void omap_rmkeys(
      const coll_t &cid,             ///< [in] Collection containing oid
      const ghobject_t &oid,  ///< [in] Object from which to remove the omap
      const std::set<std::string> &keys ///< [in] Keys to clear
      ) {
      using ceph::encode;
      Op* _op = _get_next_op();
      _op->op = OP_OMAP_RMKEYS;
      _op->cid = _get_coll_id(cid);
      _op->oid = _get_object_id(oid);
      encode(keys, data_bl);
      data.ops++;
    }

    /// Remove keys from oid omap
    void omap_rmkeys(
      const coll_t &cid,             ///< [in] Collection containing oid
      const ghobject_t &oid,  ///< [in] Object from which to remove the omap
      const ceph::buffer::list &keys_bl ///< [in] Keys to clear
      ) {
      Op* _op = _get_next_op();
      _op->op = OP_OMAP_RMKEYS;
      _op->cid = _get_coll_id(cid);
      _op->oid = _get_object_id(oid);
      data_bl.append(keys_bl);
      data.ops++;
    }

    /// Remove key range from oid omap
    void omap_rmkeyrange(
      const coll_t &cid,             ///< [in] Collection containing oid
      const ghobject_t &oid,  ///< [in] Object from which to remove the omap keys
      const std::string& first,    ///< [in] first key in range
      const std::string& last      ///< [in] first key past range, range is [first,last)
      ) {
        using ceph::encode;
	Op* _op = _get_next_op();
	_op->op = OP_OMAP_RMKEYRANGE;
	_op->cid = _get_coll_id(cid);
	_op->oid = _get_object_id(oid);
	encode(first, data_bl);
	encode(last, data_bl);
	data.ops++;
      }

    /// Set omap header
    void omap_setheader(
      const coll_t &cid,             ///< [in] Collection containing oid
      const ghobject_t &oid,  ///< [in] Object
      const ceph::buffer::list &bl    ///< [in] Header value
      ) {
      using ceph::encode;
      Op* _op = _get_next_op();
      _op->op = OP_OMAP_SETHEADER;
      _op->cid = _get_coll_id(cid);
      _op->oid = _get_object_id(oid);
      encode(bl, data_bl);
      data.ops++;
    }

    /// Split collection based on given prefixes, objects matching the specified bits/rem are
    /// moved to the new collection
    void split_collection(
      const coll_t &cid,
      uint32_t bits,
      uint32_t rem,
      const coll_t &destination) {
      Op* _op = _get_next_op();
      _op->op = OP_SPLIT_COLLECTION2;
      _op->cid = _get_coll_id(cid);
      _op->dest_cid = _get_coll_id(destination);
      _op->split_bits = bits;
      _op->split_rem = rem;
      data.ops++;
    }

    /// Merge collection into another.
    void merge_collection(
      coll_t cid,
      coll_t destination,
      uint32_t bits) {
      Op* _op = _get_next_op();
      _op->op = OP_MERGE_COLLECTION;
      _op->cid = _get_coll_id(cid);
      _op->dest_cid = _get_coll_id(destination);
      _op->split_bits = bits;
      data.ops++;
    }

    void collection_set_bits(
      const coll_t &cid,
      int bits) {
      Op* _op = _get_next_op();
      _op->op = OP_COLL_SET_BITS;
      _op->cid = _get_coll_id(cid);
      _op->split_bits = bits;
      data.ops++;
    }

    /// Set allocation hint for an object
    /// make 0 values(expected_object_size, expected_write_size) noops for all implementations
    void set_alloc_hint(
      const coll_t &cid,
      const ghobject_t &oid,
      uint64_t expected_object_size,
      uint64_t expected_write_size,
      uint32_t flags
    ) {
      Op* _op = _get_next_op();
      _op->op = OP_SETALLOCHINT;
      _op->cid = _get_coll_id(cid);
      _op->oid = _get_object_id(oid);
      _op->expected_object_size = expected_object_size;
      _op->expected_write_size = expected_write_size;
      _op->alloc_hint_flags = flags;
      data.ops++;
    }

    void encode(ceph::buffer::list& bl) const {
      //layout: data_bl + op_bl + coll_index + object_index + data
      ENCODE_START(9, 9, bl);
      encode(data_bl, bl);
      encode(op_bl, bl);
      encode(coll_index, bl);
      encode(object_index, bl);
      data.encode(bl);
      ENCODE_FINISH(bl);
    }

    void decode(ceph::buffer::list::const_iterator &bl) {
      DECODE_START(9, bl);
      DECODE_OLDEST(9);

      decode(data_bl, bl);
      decode(op_bl, bl);
      decode(coll_index, bl);
      decode(object_index, bl);
      data.decode(bl);
      coll_id = coll_index.size();
      object_id = object_index.size();

      DECODE_FINISH(bl);
    }

    void dump(ceph::Formatter *f);
    static void generate_test_instances(std::list<Transaction*>& o);
  };

  int queue_transaction(CollectionHandle& ch,
			Transaction&& t,
			TrackedOpRef op = TrackedOpRef(),
			ThreadPool::TPHandle *handle = NULL) {
    std::vector<Transaction> tls;
    tls.push_back(std::move(t));
    return queue_transactions(ch, tls, op, handle);
  }

  virtual int queue_transactions(
    CollectionHandle& ch, std::vector<Transaction>& tls,
    TrackedOpRef op = TrackedOpRef(),
    ThreadPool::TPHandle *handle = NULL) = 0;


 public:
  ObjectStore(CephContext* cct,
	      const std::string& path_) : path(path_), cct(cct) {}
  virtual ~ObjectStore() {}

  // no copying
  explicit ObjectStore(const ObjectStore& o) = delete;
  const ObjectStore& operator=(const ObjectStore& o) = delete;

  // versioning
  virtual int upgrade() {
    return 0;
  }

  virtual void get_db_statistics(ceph::Formatter *f) { }
  virtual void generate_db_histogram(ceph::Formatter *f) { }
  virtual int flush_cache(std::ostream *os = NULL) { return -1; }
  virtual void dump_perf_counters(ceph::Formatter *f) {}
  virtual void dump_cache_stats(ceph::Formatter *f) {}
  virtual void dump_cache_stats(std::ostream& os) {}

  virtual std::string get_type() = 0;

  // mgmt
  virtual bool test_mount_in_use() = 0;
  virtual int mount() = 0;
  virtual int umount() = 0;
  virtual int fsck(bool deep) {
    return -EOPNOTSUPP;
  }
  virtual int repair(bool deep) {
    return -EOPNOTSUPP;
  }

  virtual void set_cache_shards(unsigned num) { }

  /**
   * Returns 0 if the hobject is valid, -error otherwise
   *
   * Errors:
   * -ENAMETOOLONG: locator/namespace/name too large
   */
  virtual int validate_hobject_key(const hobject_t &obj) const = 0;

  virtual unsigned get_max_attr_name_length() = 0;
  virtual int mkfs() = 0;  // wipe
  virtual int mkjournal() = 0; // journal only
  virtual bool needs_journal() = 0;  //< requires a journal
  virtual bool wants_journal() = 0;  //< prefers a journal
  virtual bool allows_journal() = 0; //< allows a journal

  /// enumerate hardware devices (by 'devname', e.g., 'sda' as in /sys/block/sda)
  virtual int get_devices(std::set<std::string> *devls) {
    return -EOPNOTSUPP;
  }

  /// true if a txn is readable immediately after it is queued.
  virtual bool is_sync_onreadable() const {
    return true;
  }

  /**
   * is_rotational
   *
   * Check whether store is backed by a rotational (HDD) or non-rotational
   * (SSD) device.
   *
   * This must be usable *before* the store is mounted.
   *
   * @return true for HDD, false for SSD
   */
  virtual bool is_rotational() {
    return true;
  }

  /**
   * is_journal_rotational
   *
   * Check whether journal is backed by a rotational (HDD) or non-rotational
   * (SSD) device.
   *
   *
   * @return true for HDD, false for SSD
   */
  virtual bool is_journal_rotational() {
    return true;
  }

  virtual std::string get_default_device_class() {
    return is_rotational() ? "hdd" : "ssd";
  }

  virtual int get_numa_node(
    int *numa_node,
    std::set<int> *nodes,
    std::set<std::string> *failed) {
    return -EOPNOTSUPP;
  }


  virtual bool can_sort_nibblewise() {
    return false;   // assume a backend cannot, unless it says otherwise
  }

  virtual int statfs(struct store_statfs_t *buf,
		     osd_alert_list_t* alerts = nullptr) = 0;
  virtual int pool_statfs(uint64_t pool_id, struct store_statfs_t *buf) = 0;

  virtual void collect_metadata(std::map<std::string,string> *pm) { }

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
   * @param value value (e.g., a uuid rendered as a std::string)
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
   * @param value pointer to value std::string
   * @returns 0 for success, or an error code
   */
  virtual int read_meta(const std::string& key,
			std::string *value);

  /**
   * get ideal max value for collection_list()
   *
   * default to some arbitrary values; the implementation will override.
   */
  virtual int get_ideal_list_max() { return 64; }


  /**
   * get a collection handle
   *
   * Provide a trivial handle as a default to avoid converting legacy
   * implementations.
   */
  virtual CollectionHandle open_collection(const coll_t &cid) = 0;

  /**
   * get a collection handle for a soon-to-be-created collection
   *
   * This handle must be used by queue_transaction that includes a
   * create_collection call in order to become valid.  It will become the
   * reference to the created collection.
   */
  virtual CollectionHandle create_new_collection(const coll_t &cid) = 0;

  /**
   * std::set ContextQueue for a collection
   *
   * After that, oncommits of Transaction will queue into commit_queue.
   * And osd ShardThread will call oncommits.
   */
  virtual void set_collection_commit_queue(const coll_t &cid, ContextQueue *commit_queue) = 0;

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
  virtual bool exists(CollectionHandle& c, const ghobject_t& oid) = 0;
  /**
   * set_collection_opts -- std::set pool options for a collectioninformation for an object
   *
   * @param cid collection
   * @param opts new collection options
   * @returns 0 on success, negative error code on failure.
   */
  virtual int set_collection_opts(
    CollectionHandle& c,
    const pool_opts_t& opts) = 0;

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
    CollectionHandle &c,
    const ghobject_t& oid,
    struct stat *st,
    bool allow_eio = false) = 0;
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
   * @param bl output ceph::buffer::list
   * @param op_flags is CEPH_OSD_OP_FLAG_*
   * @returns number of bytes read on success, or negative error code on failure.
   */
   virtual int read(
     CollectionHandle &c,
     const ghobject_t& oid,
     uint64_t offset,
     size_t len,
     ceph::buffer::list& bl,
     uint32_t op_flags = 0) = 0;

  /**
   * fiemap -- get extent std::map of data of an object
   *
   * Returns an encoded std::map of the extents of an object's data portion
   * (std::map<offset,size>).
   *
   * A non-enlightened implementation is free to return the extent (offset, len)
   * as the sole extent.
   *
   * @param cid collection for object
   * @param oid oid of object
   * @param offset location offset of first byte to be read
   * @param len number of bytes to be read
   * @param bl output ceph::buffer::list for extent std::map information.
   * @returns 0 on success, negative error code on failure.
   */
   virtual int fiemap(CollectionHandle& c, const ghobject_t& oid,
		      uint64_t offset, size_t len, ceph::buffer::list& bl) = 0;
   virtual int fiemap(CollectionHandle& c, const ghobject_t& oid,
		      uint64_t offset, size_t len, std::map<uint64_t, uint64_t>& destmap) = 0;

  /**
   * getattr -- get an xattr of an object
   *
   * @param cid collection for object
   * @param oid oid of object
   * @param name name of attr to read
   * @param value place to put output result.
   * @returns 0 on success, negative error code on failure.
   */
  virtual int getattr(CollectionHandle &c, const ghobject_t& oid,
		      const char *name, ceph::buffer::ptr& value) = 0;

  /**
   * getattr -- get an xattr of an object
   *
   * @param cid collection for object
   * @param oid oid of object
   * @param name name of attr to read
   * @param value place to put output result.
   * @returns 0 on success, negative error code on failure.
   */
  int getattr(
    CollectionHandle &c, const ghobject_t& oid,
    const std::string& name, ceph::buffer::list& value) {
    ceph::buffer::ptr bp;
    int r = getattr(c, oid, name.c_str(), bp);
    value.push_back(bp);
    return r;
  }

  /**
   * getattrs -- get all of the xattrs of an object
   *
   * @param cid collection for object
   * @param oid oid of object
   * @param aset place to put output result.
   * @returns 0 on success, negative error code on failure.
   */
  virtual int getattrs(CollectionHandle &c, const ghobject_t& oid,
		       std::map<std::string,ceph::buffer::ptr>& aset) = 0;

  /**
   * getattrs -- get all of the xattrs of an object
   *
   * @param cid collection for object
   * @param oid oid of object
   * @param aset place to put output result.
   * @returns 0 on success, negative error code on failure.
   */
  int getattrs(CollectionHandle &c, const ghobject_t& oid,
	       std::map<std::string,ceph::buffer::list>& aset) {
    std::map<std::string,ceph::buffer::ptr> bmap;
    int r = getattrs(c, oid, bmap);
    for (auto i = bmap.begin(); i != bmap.end(); ++i) {
      aset[i->first].append(i->second);
    }
    return r;
  }


  // collections

  /**
   * list_collections -- get all of the collections known to this ObjectStore
   *
   * @param ls std::list of the collections in sorted order.
   * @returns 0 on success, negative error code on failure.
   */
  virtual int list_collections(std::vector<coll_t>& ls) = 0;

  /**
   * does a collection exist?
   *
   * @param c collection
   * @returns true if it exists, false otherwise
   */
  virtual bool collection_exists(const coll_t& c) = 0;

  /**
   * is a collection empty?
   *
   * @param c collection
   * @param empty true if the specified collection is empty, false otherwise
   * @returns 0 on success, negative error code on failure.
   */
  virtual int collection_empty(CollectionHandle& c, bool *empty) = 0;

  /**
   * return the number of significant bits of the coll_t::pgid.
   *
   * This should return what the last create_collection or split_collection
   * std::set.  A legacy backend may return -EAGAIN if the value is unavailable
   * (because we upgraded from an older version, e.g., FileStore).
   */
  virtual int collection_bits(CollectionHandle& c) = 0;


  /**
   * std::list contents of a collection that fall in the range [start, end) and no more than a specified many result
   *
   * @param c collection
   * @param start list object that sort >= this value
   * @param end list objects that sort < this value
   * @param max return no more than this many results
   * @param seq return no objects with snap < seq
   * @param ls [out] result
   * @param next [out] next item sorts >= this value
   * @return zero on success, or negative error
   */
  virtual int collection_list(CollectionHandle &c,
			      const ghobject_t& start, const ghobject_t& end,
			      int max,
			      std::vector<ghobject_t> *ls, ghobject_t *next) = 0;


  /// OMAP
  /// Get omap contents
  virtual int omap_get(
    CollectionHandle &c,     ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    ceph::buffer::list *header,      ///< [out] omap header
    std::map<std::string, ceph::buffer::list> *out /// < [out] Key to value std::map
    ) = 0;

  /// Get omap header
  virtual int omap_get_header(
    CollectionHandle &c,     ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    ceph::buffer::list *header,      ///< [out] omap header
    bool allow_eio = false ///< [in] don't assert on eio
    ) = 0;

  /// Get keys defined on oid
  virtual int omap_get_keys(
    CollectionHandle &c,   ///< [in] Collection containing oid
    const ghobject_t &oid, ///< [in] Object containing omap
    std::set<std::string> *keys      ///< [out] Keys defined on oid
    ) = 0;

  /// Get key values
  virtual int omap_get_values(
    CollectionHandle &c,         ///< [in] Collection containing oid
    const ghobject_t &oid,       ///< [in] Object containing omap
    const std::set<std::string> &keys,     ///< [in] Keys to get
    std::map<std::string, ceph::buffer::list> *out ///< [out] Returned keys and values
    ) = 0;

  /// Filters keys into out which are defined on oid
  virtual int omap_check_keys(
    CollectionHandle &c,     ///< [in] Collection containing oid
    const ghobject_t &oid,   ///< [in] Object containing omap
    const std::set<std::string> &keys, ///< [in] Keys to check
    std::set<std::string> *out         ///< [out] Subset of keys defined on oid
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
    CollectionHandle &c,   ///< [in] collection
    const ghobject_t &oid  ///< [in] object
    ) = 0;

  virtual int flush_journal() { return -EOPNOTSUPP; }

  virtual int dump_journal(std::ostream& out) { return -EOPNOTSUPP; }

  virtual int snapshot(const std::string& name) { return -EOPNOTSUPP; }

  /**
   * Set and get internal fsid for this instance. No external data is modified
   */
  virtual void set_fsid(uuid_d u) = 0;
  virtual uuid_d get_fsid() = 0;

  /**
  * Estimates additional disk space used by the specified amount of objects and caused by file allocation granularity and metadata store
  * - num objects - total (including witeouts) object count to measure used space for.
  */
  virtual uint64_t estimate_objects_overhead(uint64_t num_objects) = 0;


  // DEBUG
  virtual void inject_data_error(const ghobject_t &oid) {}
  virtual void inject_mdata_error(const ghobject_t &oid) {}

  virtual void compact() {}
  virtual bool has_builtin_csum() const {
    return false;
  }
};
WRITE_CLASS_ENCODER(ObjectStore::Transaction)
WRITE_CLASS_ENCODER(ObjectStore::Transaction::TransactionData)

std::ostream& operator<<(std::ostream& out, const ObjectStore::Transaction& tx);

#endif
