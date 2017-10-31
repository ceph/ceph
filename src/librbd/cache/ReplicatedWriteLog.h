// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG
#define CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG

//#if defined(HAVE_PMEM)
#include <libpmemobj.h>
//#endif
#include "common/RWLock.h"
#include "librbd/cache/ImageCache.h"
#include "librbd/cache/FileImageCache.h"
#include "librbd/Utils.h"
#include "librbd/cache/BlockGuard.h"
#include "librbd/cache/ImageWriteback.h"
#include "librbd/cache/file/Policy.h"
#include "librbd/BlockGuard.h"
#include <functional>
#include <list>

namespace librbd {

struct ImageCtx;

namespace cache {

static const uint32_t BLOCK_SIZE = 512;

namespace rwl {

/**** Write log entries ****/

static const uint64_t DEFAULT_POOL_SIZE = 10u<<30;
static const uint64_t MIN_POOL_SIZE = 1u<<20;
#define USABLE_SIZE (7.0 / 10)
static const uint8_t RWL_POOL_VERSION = 1;

POBJ_LAYOUT_BEGIN(rbd_rwl);
POBJ_LAYOUT_ROOT(rbd_rwl, struct WriteLogPoolRoot);
POBJ_LAYOUT_TOID(rbd_rwl, uint8_t);
POBJ_LAYOUT_TOID(rbd_rwl, struct WriteLogPmemEntry);
POBJ_LAYOUT_END(rbd_rwl);

struct WriteLogPmemEntry {
  uint64_t sync_gen_number;
  uint64_t write_sequence_number;
  uint64_t image_offset_bytes;
  uint64_t write_bytes;
  uint64_t first_pool_block;
  struct {
    uint8_t entry_valid :1; /* if 0, this entry is free */
    uint8_t sync_point :1;  /* No data. No write sequence
			       number. Marks sync point for this sync
			       gen number */
    uint8_t sequenced :1;   /* write sequence number is valid */
    uint8_t has_data :1;    /* first_pool_block field is valid */
    uint8_t unmap :1;       /* has_data will be 0 if this
			       is an unmap */
  };
  WriteLogPmemEntry(uint64_t image_offset_bytes, uint64_t write_bytes) 
    : sync_gen_number(0), write_sequence_number(0),
      image_offset_bytes(image_offset_bytes), write_bytes(write_bytes), first_pool_block(0),
      entry_valid(0), sync_point(0), sequenced(0), has_data(0), unmap(0) {
  }
  WriteLogPmemEntry() { WriteLogPmemEntry(0, 0); }
  BlockExtent block_extent() {
    return BlockExtent(image_offset_bytes / BLOCK_SIZE,
		       (image_offset_bytes + write_bytes) / BLOCK_SIZE);
  }
  friend std::ostream &operator<<(std::ostream &os,
				  const WriteLogPmemEntry &entry) {
    os << "entry_valid=" << (bool)entry.entry_valid << ", "
       << "sync_point=" << (bool)entry.sync_point << ", "
       << "sequenced=" << (bool)entry.sequenced << ", "
       << "has_data=" << (bool)entry.has_data << ", "
       << "unmap=" << (bool)entry.unmap << ", "
       << "sync_gen_number=" << entry.sync_gen_number << ", "
       << "write_sequence_number=" << entry.write_sequence_number << ", "
       << "image_offset_bytes=" << entry.image_offset_bytes << ", "
       << "write_bytes=" << entry.write_bytes << ", "
       << "first_pool_block=" << entry.first_pool_block;
    return os;
  };
};

struct WriteLogPoolRoot {
  union {
    struct {
      uint8_t layout_version;    /* Version of this structure (RWL_POOL_VERSION) */ 
    };
    uint64_t _u64;
  } header;
  TOID(uint8_t) data_blocks;	         /* contiguous array of blocks */
  TOID(struct WriteLogPmemEntry) log_entries;   /* contiguous array of log entries */
  uint64_t block_size;			         /* block size */
  uint64_t num_blocks;		         /* total data blocks */
  uint64_t num_log_entries;
  uint64_t valid_entry_hint;    /* Start looking here for the oldest valid entry */
  uint64_t free_entry_hint;     /* Start looking here for the next free entry */
};

class WriteLogEntry {
public:
  WriteLogPmemEntry ram_entry;
  uint64_t log_entry_index;
  WriteLogPmemEntry *pmem_entry;
  uint8_t *pmem_block;
  uint32_t referring_map_entries;
  /* TODO: occlusion by subsequent writes */
  /* TODO: flush state: portions flushed, in-progress flushes */
  WriteLogEntry(uint64_t image_offset_bytes, uint64_t write_bytes) 
    : ram_entry(image_offset_bytes, write_bytes), log_entry_index(0),
      pmem_entry(NULL), pmem_block(NULL), referring_map_entries(0) {
  }
  WriteLogEntry() {} 
  WriteLogEntry(const WriteLogEntry&) = delete;
  WriteLogEntry &operator=(const WriteLogEntry&) = delete;
  BlockExtent block_extent() { return ram_entry.block_extent(); }
  friend std::ostream &operator<<(std::ostream &os,
				  WriteLogEntry &entry) {
    os << "ram_entry=[" << entry.ram_entry << "], "
       << "log_entry_index=" << entry.log_entry_index << ", "
       << "pmem_entry=" << (void*)entry.pmem_entry << ", "
       << "pmem_block=" << (void*)entry.pmem_block;
    return os;
  };
};
  
typedef std::list<WriteLogEntry*> WriteLogEntries;
typedef std::unordered_map<uint64_t, WriteLogEntry*> BlockToWriteLogEntry;

/**** Write log entries end ****/

class SyncPoint {
public:
  CephContext *m_cct;
  const uint64_t m_sync_gen_num;
  uint64_t m_final_op_sequence_num;
  /* A sync point can't appear in the log until all the writes bearing
   * it and all the prior sync points have been appended and
   * persisted.
   *
   * Writes bearing this sync gen number and the prior sync point will
   * be sub-ops of this Gather. This sync point will not be appended
   * until all these complete. */
  C_Gather *m_prior_log_entries_persisted;
  int m_prior_log_entries_persisted_status;
  /* Signal this when this sync point is appended and persisted. This
   * is a sub-operation of the next sync point's
   * m_prior_log_entries_persisted Gather. */
  Context *m_on_sync_point_persisted;
  
  SyncPoint(CephContext *cct, uint64_t sync_gen_num);
  ~SyncPoint();
  SyncPoint(const SyncPoint&) = delete;
  SyncPoint &operator=(const SyncPoint&) = delete;
  friend std::ostream &operator<<(std::ostream &os,
				  SyncPoint &p) {
    os << "m_sync_gen_num=" << p.m_sync_gen_num << ", "
       << "m_final_op_sequence_num=" << p.m_final_op_sequence_num << ", "
       << "m_prior_log_entries_persisted=[" << p.m_prior_log_entries_persisted << "], "
       << "m_on_sync_point_persisted=[" << p.m_on_sync_point_persisted << "]";
    return os;
  };
};

class WriteLogOperationSet;
class WriteLogOperation {
public:
  WriteLogEntry *log_entry;
  bufferlist bl;
  Context *on_write_persist; /* Completion for things waiting on this write to persist */
  WriteLogOperation(WriteLogOperationSet *set, uint64_t image_offset_bytes, uint64_t write_bytes);
  ~WriteLogOperation() {
    if (NULL != log_entry) {
      delete(log_entry);
    }
  }
  WriteLogOperation(const WriteLogOperation&) = delete;
  WriteLogOperation &operator=(const WriteLogOperation&) = delete;
  friend std::ostream &operator<<(std::ostream &os,
				  WriteLogOperation &op) {
    os << "log_entry=[" << *op.log_entry << "], "
       << "bl=[" << op.bl << "]";
    return os;
  };
  void complete(int r);
};
typedef std::list<WriteLogOperation*> WriteLogOperations;

class WriteLogOperationSet {
public:
  CephContext *m_cct;
  BlockExtent m_extent; /* in blocks */
  Context *m_on_finish;
  bool m_persist_on_flush;
  BlockGuardCell *m_cell;
  C_Gather *m_extent_ops;
  Context *m_on_ops_persist;
  WriteLogOperations operations;
  void complete(int r) {
  }
  WriteLogOperationSet(CephContext *cct, SyncPoint *sync_point, bool persist_on_flush,
		       BlockExtent extent, Context *on_finish)
    : m_cct(cct), m_extent(extent), m_on_finish(on_finish), m_persist_on_flush(persist_on_flush) {
    m_on_ops_persist = sync_point->m_prior_log_entries_persisted->new_sub();
    m_extent_ops =
      new C_Gather(cct,
		   new FunctionContext( [this](int r) {
		       //ldout(m_cct, 6) << "m_extent_ops completed" << dendl;
		       m_on_ops_persist->complete(r);
		       m_on_finish->complete(r);
		       delete(this);
		     }));
  }
  WriteLogOperationSet(const WriteLogOperationSet&) = delete;
  ~WriteLogOperationSet() {
    /* TODO: Will m_extent_ops always delete itself? */
  }
  WriteLogOperationSet &operator=(const WriteLogOperationSet&) = delete;
  friend std::ostream &operator<<(std::ostream &os,
				  WriteLogOperationSet &s) {
    os << "m_extent=[" << s.m_extent.block_start << "," << s.m_extent.block_end << "] "
       << "m_on_finish=" << s.m_on_finish << ", "
       << "m_cell=" << (void*)s.m_cell << ", "
       << "m_extent_ops=[" << s.m_extent_ops << "]";
    return os;
  };
};

class GuardedRequestFunctionContext : public Context {
  std::atomic<bool> m_callback_invoked;
public:
  GuardedRequestFunctionContext(boost::function<void(BlockGuardCell*)> &&callback)
    : m_callback_invoked(false), m_callback(std::move(callback)) { }

  void finish(int r) override {
    if (r < 0) {
      bool initial = false;
      if (m_callback_invoked.compare_exchange_strong(initial, true)) {
	m_callback(NULL);
      }
    }
  }
  
  void acquired(BlockGuardCell *cell) {
    bool initial = false;
    if (m_callback_invoked.compare_exchange_strong(initial, true)) {
      m_callback(cell);
    }
  }
private:
  boost::function<void(BlockGuardCell*)> m_callback;
};

struct GuardedRequest {
  uint64_t first_block_num;
  uint64_t last_block_num;
  GuardedRequestFunctionContext *on_guard_acquire; /* Work to do when guard on range obtained */
  
  GuardedRequest(uint64_t first_block_num, uint64_t last_block_num, GuardedRequestFunctionContext *on_guard_acquire)
    : first_block_num(first_block_num), last_block_num(last_block_num), on_guard_acquire(on_guard_acquire) {
  }
};

typedef librbd::BlockGuard<GuardedRequest> WriteLogGuard;

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::rwl::WriteLogMap: " << this << " " \
                           <<  __func__ << ": "
/**
 * WriteLogMap: maps block extents to WriteLogEntries
 *
 */
/* A WriteLogMapEntry refers to a portion of a WriteLogEntry */
struct WriteLogMapEntry {
  uint64_t first_block_num;
  uint64_t last_block_num;
  WriteLogEntry *log_entry;
  
  WriteLogMapEntry(uint64_t first_block_num, uint64_t last_block_num, WriteLogEntry *log_entry)
    : first_block_num(first_block_num), last_block_num(last_block_num), log_entry(log_entry) {
    log_entry->referring_map_entries++;
  }
  ~WriteLogMapEntry() {
    log_entry->referring_map_entries--;
  }
};
typedef std::list<WriteLogMapEntry> WriteLogMapEntries;
class WriteLogMap {
private:
  struct WriteLogMapExtent;

public:
  WriteLogMap(CephContext *cct)
    : m_cct(cct), m_lock("librbd::cache::rwl::WriteLogMap::m_lock") {
  }

  WriteLogMap(const WriteLogMap&) = delete;
  WriteLogMap &operator=(const WriteLogMap&) = delete;

  /**
   * Add a write log entry to the map. Subsequent queries for blocks
   * within this log entry's extent will find this log entry. Portions
   * of prior write log entries overlapping with this log entry will
   * be replaced in the map by this log entry.
   *
   * The map_entries field of the log entry object will be updated to
   * contain this map entry.
   *
   * The map_entries fields of all log entries overlapping with this
   * entry will be updated to remove the regions that overlap with
   * this.
   */
  int add_entry(WriteLogEntry *log_entry) {
    Mutex::Locker locker(m_lock);
    const BlockExtent block_extent = log_entry->block_extent();
    ldout(m_cct, 20) << "block_start=" << block_extent.block_start << ", "
                     << "block_end=" << block_extent.block_end << ", "
                     << "free_slots=" << m_free_map_extents.size()
                     << dendl;
#if 0
    WriteLogMapExtent *detained_block_extent;
    auto it = m_detained_block_extents.find(block_extent);
    if (it != m_detained_block_extents.end()) {
      // request against an already detained block
      detained_block_extent = &(*it);
      if (map_entry != nullptr) {
        detained_block_extent->block_operations.emplace_back(
          std::move(*map_entry));
      }

      // alert the caller that the IO was detained
      return detained_block_extent->block_operations.size();
    } else {
      if (!m_free_map_extents.empty()) {
        detained_block_extent = &m_free_map_extents.front();
        detained_block_extent->block_operations.clear();
        m_free_map_extents.pop_front();
      } else {
        ldout(m_cct, 20) << "no free detained block cells" << dendl;
        m_detained_block_extent_pool.emplace_back();
        detained_block_extent = &m_detained_block_extent_pool.back();
      }

      detained_block_extent->block_extent = block_extent;
      m_detained_block_extents.insert(*detained_block_extent);
      return 0;
    }
#endif
  }

  /**
   * Remove any map entries that refer to the supplied write log
   * entry.
   */
  void remove_entry(WriteLogEntry *log_entry) {
    Mutex::Locker locker(m_lock);

#if 0
    assert(cell != nullptr);
    auto &detained_block_extent = reinterpret_cast<WriteLogMapExtent &>(
      *cell);
    ldout(m_cct, 20) << "block_start="
                     << detained_block_extent.block_extent.block_start << ", "
                     << "block_end="
                     << detained_block_extent.block_extent.block_end << ", "
                     << "pending_ops="
                     << (detained_block_extent.block_operations.empty() ?
                          0 : detained_block_extent.block_operations.size() - 1)
                     << dendl;

    *block_operations = std::move(detained_block_extent.block_operations);
    m_detained_block_extents.erase(detained_block_extent.block_extent);
    m_free_detained_block_extents.push_back(detained_block_extent);
#endif
  }

private:
  struct WriteLogMapExtent : public boost::intrusive::list_base_hook<>,
			     public boost::intrusive::set_base_hook<> {
    BlockExtent block_extent;
    WriteLogMapEntry map_entry;
  };

  struct WriteLogMapExtentKey {
    typedef BlockExtent type;
    const BlockExtent &operator()(const WriteLogMapExtent &value) {
      return value.block_extent;
    }
  };

  struct WriteLogMapExtentCompare {
    bool operator()(const BlockExtent &lhs,
                    const BlockExtent &rhs) const {
      // check for range overlap (lhs < rhs)
      if (lhs.block_end <= rhs.block_start) {
        return true;
      }
      return false;
    }
  };

  typedef std::deque<WriteLogMapExtent> WriteLogMapExtentsPool;
  typedef boost::intrusive::list<WriteLogMapExtent> WriteLogMapExtents;
  typedef boost::intrusive::set<
    WriteLogMapExtent,
    boost::intrusive::compare<WriteLogMapExtentCompare>,
    boost::intrusive::key_of_value<WriteLogMapExtentKey> >
      BlockExtentToWriteLogMapExtents;

  CephContext *m_cct;

  Mutex m_lock;
  WriteLogMapExtentsPool m_map_extent_pool;
  WriteLogMapExtents m_free_map_extents;
  BlockExtentToWriteLogMapExtents m_block_to_log_entry_map;
};

} // namespace rwl

using namespace librbd::cache::rwl;

struct C_WriteRequest;

/**
 * Prototype pmem-based, client-side, replicated write log
 */
template <typename ImageCtxT = librbd::ImageCtx>
class ReplicatedWriteLog : public ImageCache {
public:
  ReplicatedWriteLog(ImageCtx &image_ctx);
  ~ReplicatedWriteLog();
  ReplicatedWriteLog(const ReplicatedWriteLog&) = delete;
  ReplicatedWriteLog &operator=(const ReplicatedWriteLog&) = delete;

  /// client AIO methods
  void aio_read(Extents&& image_extents, ceph::bufferlist *bl,
                int fadvise_flags, Context *on_finish) override;
  void aio_write(Extents&& image_extents, ceph::bufferlist&& bl,
                 int fadvise_flags, Context *on_finish) override;
  void aio_discard(uint64_t offset, uint64_t length,
                   bool skip_partial_discard, Context *on_finish);
  void aio_flush(Context *on_finish) override;
  void aio_writesame(uint64_t offset, uint64_t length,
                     ceph::bufferlist&& bl,
                     int fadvise_flags, Context *on_finish) override;
  void aio_compare_and_write(Extents&& image_extents,
                             ceph::bufferlist&& cmp_bl, ceph::bufferlist&& bl,
                             uint64_t *mismatch_offset,int fadvise_flags,
                             Context *on_finish) override;

  /// internal state methods
  void init(Context *on_finish) override;
  void shut_down(Context *on_finish) override;

  void invalidate(Context *on_finish) override;
  void flush(Context *on_finish) override;

  void detain_guarded_request(GuardedRequest &&req);
  void release_guarded_request(BlockGuardCell *cell);
private:
  typedef std::function<void(uint64_t)> ReleaseBlock;
  typedef std::function<void(BlockGuard::BlockIO)> AppendDetainedBlock;
  typedef std::list<Context *> Contexts;

  const char* rwl_pool_layout_name = POBJ_LAYOUT_NAME(rbd_rwl);

  ImageCtxT &m_image_ctx;

  std::string m_log_pool_name;
  PMEMobjpool *m_log_pool;
  uint64_t m_log_pool_size;
  
  uint64_t m_total_log_entries;
  uint64_t m_total_blocks;
  uint64_t m_free_log_entries;
  uint64_t m_free_blocks;

  ImageWriteback<ImageCtxT> m_image_writeback;
  FileImageCache<ImageCtxT> m_image_cache;
  WriteLogGuard m_write_log_guard;
  
  /* When m_first_free_entry == m_last_free_entry, the log is empty */
  uint64_t m_first_free_entry;  /* Entries from here to m_first_valid_entry-1 are free */
  uint64_t m_first_valid_entry; /* Entries from here to m_first_free_entry-1 are valid */

  /* When m_next_free_block == m_last_free_bloock, all blocks are free */
  uint64_t m_next_free_block; /* Blocks from here to m_last_free_block are free */
  uint64_t m_last_free_block;
  
  uint64_t m_free_entry_hint;
  uint64_t m_valid_entry_hint;

  /* Starts at 0 for a new write log. Incremented on every flush. */
  uint64_t m_current_sync_gen;
  SyncPoint *m_current_sync_point;
  /* Starts at 0 on each sync gen increase. Incremented before applied
     to an operation */
  uint64_t m_last_op_sequence_num;

  bool m_persist_on_write_until_flush;
  bool m_persist_on_flush; /* If false, persist each write before completion */
  bool m_flush_seen;
  
  util::AsyncOpTracker m_async_op_tracker;

  mutable Mutex m_lock;
  BlockGuard::BlockIOs m_deferred_block_ios;
  BlockGuard::BlockIOs m_detained_block_ios;

  bool m_wake_up_scheduled = false;

  Contexts m_post_work_contexts;

  RWLock m_log_append_lock;

  WriteLogOperations m_ops_to_flush; /* Write ops neding flush in local log */

  WriteLogOperations m_ops_to_append; /* Write ops needing event append in local log */

  void wake_up();
  void process_work();

  bool is_work_available() const;
  void process_writeback_dirty_blocks();
  void process_detained_block_ios();
  void process_deferred_block_ios();

  void invalidate(Extents&& image_extents, Context *on_finish);

  void append_sync_point(SyncPoint *sync_point, int prior_write_status);
  void new_sync_point(void);

  void dispatch_aio_write(C_WriteRequest *write_req);
  void append_scheduled_ops(void);
  void schedule_append(WriteLogOperations &ops);
  void flush_then_append_scheduled_ops(void);
  void schedule_flush_and_append(WriteLogOperations &ops);
  void flush_pmem_blocks(WriteLogOperations &ops);
  void alloc_op_log_entries(WriteLogOperations &ops);
  int append_op_log_entries(WriteLogOperations &ops);
  void complete_op_log_entries(WriteLogOperations &ops, int r);
  void alloc_and_append_entries(WriteLogOperations &ops);
};

} // namespace cache
} // namespace librbd

extern template class librbd::cache::ReplicatedWriteLog<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG

/* Local Variables: */
/* eval: (c-set-offset 'innamespace 0) */
/* End: */
