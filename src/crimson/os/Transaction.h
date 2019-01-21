// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include <map>

#include "include/int_types.h"
#include "include/buffer.h"
#include "osd/osd_types.h"

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
namespace ceph::os {
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

    void encode(bufferlist& bl) const {
      bl.append((char*)this, sizeof(TransactionData));
    }
    void decode(bufferlist::const_iterator &bl) {
      bl.copy(sizeof(TransactionData), (char*)this);
    }
  } __attribute__ ((packed)) ;

private:
  TransactionData data;

  std::map<coll_t, __le32> coll_index;
  std::map<ghobject_t, __le32> object_index;

  __le32 coll_id {0};
  __le32 object_id {0};

  bufferlist data_bl;
  bufferlist op_bl;

  std::list<Context *> on_applied;
  std::list<Context *> on_commit;
  std::list<Context *> on_applied_sync;

public:
  Transaction() = default;

  explicit Transaction(bufferlist::const_iterator &dp) {
    decode(dp);
  }
  explicit Transaction(bufferlist &nbl) {
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
  const map<ghobject_t, __le32>& get_object_index() const {
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

  static void collect_contexts(vector<Transaction>& t,
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
  static void collect_contexts(vector<Transaction>& t,
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
		  vector<__le32> &cm,
		  vector<__le32> &om) {

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
		     bufferlist& bl,
		     vector<__le32> &cm,
		     vector<__le32> &om) {
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
  /// Append the operations of the parameter to this Transaction. Those
  /// operations are removed from the parameter Transaction
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
    vector<__le32> cm(other.coll_index.size());
    map<coll_t, __le32>::iterator coll_index_p;
    for (coll_index_p = other.coll_index.begin();
	 coll_index_p != other.coll_index.end();
	 ++coll_index_p) {
      cm[coll_index_p->second] = _get_coll_id(coll_index_p->first);
    }
    
    vector<__le32> om(other.object_index.size());
    map<ghobject_t, __le32>::iterator object_index_p;
    for (object_index_p = other.object_index.begin();
	 object_index_p != other.object_index.end();
	 ++object_index_p) {
      om[object_index_p->second] = _get_object_id(object_index_p->first);
    }      
    
    //the other.op_bl SHOULD NOT be changes during append operation,
    //we use additional bufferlist to avoid this problem
    bufferlist other_op_bl;
    {
      bufferptr other_op_bl_ptr(other.op_bl.length());
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
    bufferlist bl;
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
  /// offset within the encoded buffer to the start of the largest data buffer
  /// that's encoded
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
    
    bufferlist::const_iterator data_bl_p;
    
  public:
    vector<coll_t> colls;
    vector<ghobject_t> objects;
    
  private:
    explicit iterator(Transaction *t)
      : t(t),
	data_bl_p(t->data_bl.cbegin()),
	colls(t->coll_index.size()),
	objects(t->object_index.size()) {
      
      ops = t->data.ops;
      op_buffer_p = t->op_bl.c_str();
      
      map<coll_t, __le32>::iterator coll_index_p;
      for (coll_index_p = t->coll_index.begin();
	   coll_index_p != t->coll_index.end();
	   ++coll_index_p) {
	colls[coll_index_p->second] = coll_index_p->first;
      }
      
      map<ghobject_t, __le32>::iterator object_index_p;
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
    string decode_string() {
      using ceph::decode;
      string s;
      decode(s, data_bl_p);
      return s;
    }
    void decode_bp(bufferptr& bp) {
      using ceph::decode;
      decode(bp, data_bl_p);
    }
    void decode_bl(bufferlist& bl) {
      using ceph::decode;
      decode(bl, data_bl_p);
    }
    void decode_attrset(map<string,bufferptr>& aset) {
      using ceph::decode;
      decode(aset, data_bl_p);
    }
    void decode_attrset(map<string,bufferlist>& aset) {
      using ceph::decode;
      decode(aset, data_bl_p);
    }
    void decode_attrset_bl(bufferlist *pbl);
    void decode_keyset(set<string> &keys){
      using ceph::decode;
      decode(keys, data_bl_p);
    }
    void decode_keyset_bl(bufferlist *pbl);
    
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

  static constexpr size_t OPS_PER_PTR = 32u;
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
    map<coll_t, __le32>::iterator c = coll_index.find(coll);
    if (c != coll_index.end())
      return c->second;
    
    __le32 index_id = coll_id++;
    coll_index[coll] = index_id;
    return index_id;
  }
  __le32 _get_object_id(const ghobject_t& oid) {
    map<ghobject_t, __le32>::iterator o = object_index.find(oid);
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
	     const bufferlist& write_data, uint32_t flags = 0) {
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
      // we are about to
      data.largest_data_off_in_data_bl = orig_len + sizeof(__u32);  
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
  void setattr(const coll_t& cid, const ghobject_t& oid,
	       const char* name,
	       bufferlist& val) {
    string n(name);
    setattr(cid, oid, n, val);
  }
  /// Set an xattr of an object
  void setattr(const coll_t& cid, const ghobject_t& oid,
	       const string& s, bufferlist& val) {
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
  void setattrs(const coll_t& cid, const ghobject_t& oid,
		const map<string,bufferptr>& attrset) {
    using ceph::encode;
    Op* _op = _get_next_op();
    _op->op = OP_SETATTRS;
    _op->cid = _get_coll_id(cid);
    _op->oid = _get_object_id(oid);
    encode(attrset, data_bl);
    data.ops++;
  }
  /// Set multiple xattrs of an object
  void setattrs(const coll_t& cid, const ghobject_t& oid,
		const map<string,bufferlist>& attrset) {
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
    string n(name);
    rmattr(cid, oid, n);
  }
  /// remove an xattr from an object
  void rmattr(const coll_t& cid, const ghobject_t& oid, const string& s) {
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
  void collection_hint(const coll_t& cid, uint32_t type, const bufferlist& hint) {
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
  void collection_move(const coll_t& cid, const coll_t &oldcid,
		       const ghobject_t& oid)
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
    const map<string, bufferlist> &attrset ///< [in] Replacement keys and values
    ) {
    using ceph::encode;
    Op* _op = _get_next_op();
    _op->op = OP_OMAP_SETKEYS;
    _op->cid = _get_coll_id(cid);
    _op->oid = _get_object_id(oid);
    encode(attrset, data_bl);
    data.ops++;
  }

    /// Set keys on an oid omap (bufferlist variant).
  void omap_setkeys(
    const coll_t &cid,                           ///< [in] Collection containing oid
    const ghobject_t &oid,                ///< [in] Object to update
    const bufferlist &attrset_bl          ///< [in] Replacement keys and values
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
    const set<string> &keys ///< [in] Keys to clear
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
    const bufferlist &keys_bl ///< [in] Keys to clear
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
    const string& first,    ///< [in] first key in range
    const string& last      ///< [in] first key past range, range is [first,last)
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
    const bufferlist &bl    ///< [in] Header value
    ) {
    using ceph::encode;
    Op* _op = _get_next_op();
    _op->op = OP_OMAP_SETHEADER;
    _op->cid = _get_coll_id(cid);
    _op->oid = _get_object_id(oid);
    encode(bl, data_bl);
    data.ops++;
  }

  /// Split collection based on given prefixes, objects matching the specified
  /// bits/rem are moved to the new collection
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

  void encode(bufferlist& bl) const {
    //layout: data_bl + op_bl + coll_index + object_index + data
    ENCODE_START(9, 9, bl);
    encode(data_bl, bl);
    encode(op_bl, bl);
    encode(coll_index, bl);
    encode(object_index, bl);
    data.encode(bl);
    ENCODE_FINISH(bl);
  }

  void decode(bufferlist::const_iterator &bl) {
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
};
}
