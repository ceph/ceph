// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG
#define CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG

//#if defined(HAVE_PMEM)
//#include <libpmem.h>
#include <libpmemobj.h>
//#endif
#include "librbd/cache/ImageCache.h"
#include "librbd/cache/FileImageCache.h"
#include "librbd/Utils.h"
#include "librbd/cache/BlockGuard.h"
#include "librbd/cache/ImageWriteback.h"
#include "librbd/cache/file/Policy.h"
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
    uint8_t has_data :1; /* first_pool_block field is valid */
    uint8_t unmap :1;       /* has_data will be 0 if this
			       is an unmap */
  };
  WriteLogPmemEntry(uint64_t image_offset_bytes, uint64_t write_bytes) 
    : sync_gen_number(0), write_sequence_number(0),
      image_offset_bytes(image_offset_bytes), write_bytes(write_bytes), first_pool_block(0),
      entry_valid(0), sync_point(0), sequenced(0), has_data(0), unmap(0) {
  }
  WriteLogPmemEntry() { WriteLogPmemEntry(0, 0); }
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
  /* TODO: occlusion by subsequent writes */
  /* TODO: flush state: portions flushed, in-progress flushes */
  WriteLogEntry(uint64_t image_offset_bytes, uint64_t write_bytes) 
    : ram_entry(image_offset_bytes, write_bytes), log_entry_index(0), pmem_entry(NULL), pmem_block(NULL)  {
  }
  WriteLogEntry() {} 
  WriteLogEntry(const WriteLogEntry&) = delete;
  WriteLogEntry &operator=(const WriteLogEntry&) = delete;
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
  C_Gather *prior_log_entries_persisted;
  /* Signal this when this sync point is appended and persisted. This
   * is a sub-operation of the next sync point's
   * prior_log_entries_persisted Gather. */
  Context *on_sync_point_persisted;
  
  SyncPoint(CephContext *cct, uint64_t sync_gen_num);
  SyncPoint(const SyncPoint&) = delete;
  SyncPoint &operator=(const SyncPoint&) = delete;
  friend std::ostream &operator<<(std::ostream &os,
				  SyncPoint &p) {
    os << "m_sync_gen_num=" << p.m_sync_gen_num << ", "
       << "m_final_op_sequence_num=" << p.m_final_op_sequence_num << ", "
       << "prior_log_entries_persisted=[" << p.prior_log_entries_persisted << "], "
       << "on_sync_point_persisted=[" << p.on_sync_point_persisted << "]";
    return os;
  };
};
  
class WriteLogOperation {
public:
  WriteLogEntry *log_entry;
  bufferlist bl;
  Context *on_write_persist;
  WriteLogOperation(SyncPoint *sync_point, uint64_t image_offset_bytes, uint64_t write_bytes) 
    : log_entry(new WriteLogEntry(image_offset_bytes, write_bytes)) {
    on_write_persist = sync_point->prior_log_entries_persisted->new_sub();
  }
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
};
typedef std::list<WriteLogOperation*> WriteLogOperations;

} // namespace rwl

using namespace librbd::cache::rwl;

/**
 * Prototype pmem-based, client-side, replicated write log
 */
template <typename ImageCtxT = librbd::ImageCtx>
class ReplicatedWriteLog : public ImageCache {
public:
  //using typename FileImageCache<ImageCtxT>::Extents;
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
  //BlockGuard m_persist_pending_guard;
  BlockGuard m_block_guard;

  file::Policy *m_policy = nullptr;

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
  
  ReleaseBlock m_release_block;
  AppendDetainedBlock m_detain_block;

  util::AsyncOpTracker m_async_op_tracker;

  mutable Mutex m_lock;
  BlockGuard::BlockIOs m_deferred_block_ios;
  BlockGuard::BlockIOs m_detained_block_ios;

  bool m_wake_up_scheduled = false;

  Contexts m_post_work_contexts;

  void map_blocks(IOType io_type, Extents &&image_extents,
                  BlockGuard::C_BlockRequest *block_request);
  void map_block(bool detain_block, BlockGuard::BlockIO &&block_io);
  void release_block(uint64_t block);
  void append_detain_block(BlockGuard::BlockIO &block_io);

  void wake_up();
  void process_work();

  bool is_work_available() const;
  void process_writeback_dirty_blocks();
  void process_detained_block_ios();
  void process_deferred_block_ios();

  void invalidate(Extents&& image_extents, Context *on_finish);

  void new_sync_point(void);
};

} // namespace cache
} // namespace librbd

extern template class librbd::cache::ReplicatedWriteLog<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG

/* Local Variables: */
/* eval: (c-set-offset 'innamespace 0) */
/* End: */
