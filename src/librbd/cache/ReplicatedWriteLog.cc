// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "ReplicatedWriteLog.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include "librbd/cache/file/ImageStore.h"
#include "librbd/cache/file/JournalStore.h"
#include "librbd/cache/file/MetaStore.h"
#include "librbd/cache/file/StupidPolicy.h"
#include "librbd/cache/file/Types.h"
#include <map>
#include <vector>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::ReplicatedWriteLog: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {

using namespace librbd::cache::rwl;
using namespace librbd::cache::file;

BlockExtent block_extent(uint64_t offset_bytes, uint64_t length_bytes)
{
  return BlockExtent(offset_bytes / BLOCK_SIZE,
		     ((offset_bytes + length_bytes) / BLOCK_SIZE) - 1);
}

BlockExtent block_extent(ImageCache::Extent& image_extent)
{
  return block_extent(image_extent.first, image_extent.second);
}

namespace rwl {

SyncPoint::SyncPoint(CephContext *cct, uint64_t sync_gen_num)
  : m_cct(cct), m_sync_gen_num(sync_gen_num),
    m_prior_log_entries_persisted_status(0), m_on_sync_point_persisted(NULL) {
  m_prior_log_entries_persisted = new C_Gather(cct, NULL);
  ldout(cct, 6) << "sync point" << m_sync_gen_num << dendl;
  /* TODO: Connect m_prior_log_entries_persisted finisher to append this
     sync point and on persist complete call on_sync_point_persisted
     and delete this object */
}
SyncPoint::~SyncPoint() {
  /* TODO: will m_prior_log_entries_persisted always delete itself? */
}

WriteLogOperation::WriteLogOperation(WriteLogOperationSet *set, uint64_t image_offset_bytes, uint64_t write_bytes)
  : log_entry(new WriteLogEntry(image_offset_bytes, write_bytes)) {
  on_write_persist = set->m_extent_ops->new_sub();
}

/* Called when the write log operation is completed in all log replicas */
void WriteLogOperation::complete(int result) {
}

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
int WriteLogMap::add_entry(WriteLogEntry *log_entry) {
  Mutex::Locker locker(m_lock);
  add_entry_locked(log_entry);
}

int WriteLogMap::add_entries(WriteLogEntries &log_entries) {
  Mutex::Locker locker(m_lock);
  ldout(m_cct, 20) << dendl;
  for (auto &log_entry : log_entries) {
    add_entry_locked(log_entry);
  }
}
  
/**
 * Remove any map entries that refer to the supplied write log
 * entry.
 */
void WriteLogMap::remove_entry(WriteLogEntry *log_entry) {
  Mutex::Locker locker(m_lock);
  remove_entry_locked(log_entry);
}

int WriteLogMap::remove_entries(WriteLogEntries &log_entries) {
  Mutex::Locker locker(m_lock);
  ldout(m_cct, 20) << dendl;
  for (auto &log_entry : log_entries) {
    remove_entry_locked(log_entry);
  }
}

#if 0
/**
 * Returns the list of all write log entries that overlap the
 * specified block extent.
 */
WriteLogEntries WriteLogMap::find_entries(BlockExtent block_extent) {
  Mutex::Locker locker(m_lock);
  ldout(m_cct, 20) << dendl;
  return find_entries_locked(block_extent);
}
#endif

/**
 * Returns the list of all write log map entries that overlap the
 * specified block extent.
 */
WriteLogMapEntries WriteLogMap::find_map_entries(BlockExtent block_extent) {
  Mutex::Locker locker(m_lock);
  ldout(m_cct, 20) << dendl;
  return find_map_entries_locked(block_extent);
}
  
void WriteLogMap::add_entry_locked(WriteLogEntry *log_entry) {
  WriteLogMapEntry map_entry(log_entry);
  ldout(m_cct, 20) << "block_extent=" << map_entry.block_extent 
		   << dendl;
  assert(m_lock.is_locked_by_me());
  WriteLogMapEntries overlap_entries = find_map_entries_locked(map_entry.block_extent);
  if (overlap_entries.size()) {
    for (auto &entry : overlap_entries) {
      ldout(m_cct, 20) << entry << dendl;
      if (map_entry.block_extent.block_start <= entry.block_extent.block_start) {
	if (map_entry.block_extent.block_end >= entry.block_extent.block_end) {
	  ldout(m_cct, 20) << "map entry completely occluded by new log entry" << dendl;
	  remove_map_entry_locked(entry);
	} else {
	  assert(map_entry.block_extent.block_end < entry.block_extent.block_end);
	  /* The new entry occludes the beginning of the old entry */
	  BlockExtent adjusted_extent(map_entry.block_extent.block_end+1,
				      entry.block_extent.block_end);
	  adjust_map_entry_locked(entry, adjusted_extent);
	}
      } else {
	assert(map_entry.block_extent.block_start > entry.block_extent.block_start);
	/* The new entry occludes the end of the old entry */
	BlockExtent adjusted_extent(entry.block_extent.block_start,
				    map_entry.block_extent.block_start-1);
	adjust_map_entry_locked(entry, adjusted_extent);
      }
    }
  }
  add_map_entry_locked(map_entry);
}

void WriteLogMap::remove_entry_locked(WriteLogEntry *log_entry) {
  ldout(m_cct, 20) << "*log_entry=" << *log_entry << dendl;
  assert(m_lock.is_locked_by_me());
  
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

void WriteLogMap::add_map_entry_locked(WriteLogMapEntry &map_entry)
{
  m_block_to_log_entry_map.insert(map_entry);
}

void WriteLogMap::remove_map_entry_locked(WriteLogMapEntry &map_entry)
{
  auto it = m_block_to_log_entry_map.find(map_entry);
  assert(it != m_block_to_log_entry_map.end());

  WriteLogMapEntry erased = *it;
  m_block_to_log_entry_map.erase(it);
  erased.log_entry->referring_map_entries--;
  if (0 == erased.log_entry->referring_map_entries) {
    ldout(m_cct, 20) << "log entry has zero map entries: " << erased.log_entry << dendl;
  }
}

void WriteLogMap::adjust_map_entry_locked(WriteLogMapEntry &map_entry, BlockExtent &new_extent)
{
  auto it = m_block_to_log_entry_map.find(map_entry);
  assert(it != m_block_to_log_entry_map.end());

  WriteLogMapEntry adjusted = *it;
  m_block_to_log_entry_map.erase(it);

  m_block_to_log_entry_map.insert(WriteLogMapEntry(new_extent, adjusted.log_entry));
}

#if 0
WriteLogEntries WriteLogMap::find_entries_locked(BlockExtent block_extent) {
  WriteLogEntries overlaps;
  ldout(m_cct, 20) << "block_extent=" << block_extent << dendl;
  
  assert(m_lock.is_locked_by_me());
  WriteLogMapEntries map_entries = find_map_entries_locked(block_extent);
  for (auto &map_entry : map_entries) {
    overlaps.emplace_back(map_entry.log_entry);
  }
  return overlaps;
}
#endif

/**
 * TODO: Generalize this to do some arbitrary thing to each map
 * extent, instead of returning a list.
 */
WriteLogMapEntries WriteLogMap::find_map_entries_locked(BlockExtent &block_extent) {
  WriteLogMapEntries overlaps;
  
  ldout(m_cct, 20) << "block_extent=" << block_extent << dendl;
  assert(m_lock.is_locked_by_me());
  auto p = m_block_to_log_entry_map.equal_range(WriteLogMapEntry(block_extent));
  ldout(m_cct, 20) << "count=" << std::distance(p.first, p.second) << dendl;
  for ( auto i = p.first; i != p.second; ++i ) {
    WriteLogMapEntry entry = *i;
    overlaps.emplace_back(entry);
    ldout(m_cct, 20) << entry << dendl;
  }
  return overlaps;
}

bool is_block_aligned(const ImageCache::Extents &image_extents) {
  for (auto &extent : image_extents) {
    if (extent.first % BLOCK_SIZE != 0 || extent.second % BLOCK_SIZE != 0) {
      return false;
    }
  }
  return true;
}

struct C_BlockIORequest : public Context {
  CephContext *cct;
  C_BlockIORequest *next_block_request;

  C_BlockIORequest(CephContext *cct, C_BlockIORequest *next_block_request)
    : cct(cct), next_block_request(next_block_request) {
  }

  virtual void finish(int r) override {
    ldout(cct, 20) << "(" << get_name() << "): r=" << r << dendl;

    assert(NULL != next_block_request);
    if (r < 0) {
      // abort the chain of requests upon failure
      next_block_request->complete(r);
    } else {
      // execute next request in chain
      next_block_request->send();
    }
  }

  virtual void send() = 0;
  virtual const char *get_name() const = 0;
};

struct C_GuardedBlockIORequest : public C_BlockIORequest {
  BlockGuardCell *m_cell;

  C_GuardedBlockIORequest(CephContext *cct, C_BlockIORequest *next_block_request)
    : C_BlockIORequest(cct, next_block_request), m_cell(NULL) {
    ldout(cct, 6) << dendl;
  }

  virtual void send() = 0;
  virtual const char *get_name() const = 0;
};

} // namespace rwl

template <typename I>
ReplicatedWriteLog<I>::ReplicatedWriteLog(ImageCtx &image_ctx)
  : m_image_ctx(image_ctx), 
    m_log_pool(NULL), m_log_pool_size(DEFAULT_POOL_SIZE),
    m_total_log_entries(0), m_total_blocks(0),
    m_free_log_entries(0), m_free_blocks(0),
    m_image_writeback(image_ctx), m_image_cache(image_ctx),
    m_write_log_guard(image_ctx.cct),
    m_first_free_entry(0), m_first_valid_entry(0),
    m_next_free_block(0), m_last_free_block(0),
    m_free_entry_hint(0), m_valid_entry_hint(0),
    m_current_sync_gen(0), m_current_sync_point(NULL),
    m_last_op_sequence_num(0),
    m_persist_on_write_until_flush(true), m_persist_on_flush(false),
    m_flush_seen(false),
    m_lock("librbd::cache::ReplicatedWriteLog::m_lock"),
    m_log_append_lock("librbd::cache::ReplicatedWriteLog::m_log_append_lock"),
    m_blocks_to_log_entries(image_ctx.cct) {
}

template <typename I>
ReplicatedWriteLog<I>::~ReplicatedWriteLog() {
  //delete m_policy;
}

template <typename ExtentsType>
class ExtentsSummary {
public:
  uint64_t total_bytes;
  uint64_t first_image_byte;
  uint64_t last_image_byte;
  uint64_t first_block;
  uint64_t last_block;
  ExtentsSummary(const ExtentsType &extents) {
    total_bytes = 0;
    first_image_byte = 0;
    last_image_byte = 0;
    first_block = 0;
    last_block = 0;
    if (extents.empty()) return;
    first_image_byte = extents.front().first;
    last_image_byte = first_image_byte + extents.front().second;
    for (auto &extent : extents) {
      total_bytes += extent.second;
      if (extent.first < first_image_byte) {
	first_image_byte = extent.first;
      }
      if ((extent.first + extent.second) > last_image_byte) {
	last_image_byte = extent.first + extent.second;
      }
    }
    first_block = first_image_byte / BLOCK_SIZE;
    last_block = last_image_byte / BLOCK_SIZE;
  }
};
  
template <typename I>
void ReplicatedWriteLog<I>::aio_read(Extents &&image_extents, bufferlist *bl,
                                 int fadvise_flags, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "image_extents=" << image_extents << ", "
                 << "on_finish=" << on_finish << dendl;

  for (auto &extent : image_extents) {
    WriteLogMapEntries map_entries = m_blocks_to_log_entries.find_map_entries(block_extent(extent));
    for (auto &entry : map_entries) {
      ldout(cct, 20) << entry << dendl;
    }
  }
  
  if (m_image_ctx.persistent_cache_enabled) {
    m_image_cache.aio_read(std::move(image_extents), bl, fadvise_flags, on_finish);
  } else {
    m_image_writeback.aio_read(std::move(image_extents), bl, fadvise_flags, on_finish);
  }
  // TODO handle fadvise flags
  /*
  BlockGuard::C_BlockRequest *req = new C_ReadBlockRequest<I>(
    m_image_ctx, m_image_writeback, *m_image_store, m_release_block, bl,
    on_finish);
  map_blocks(IO_TYPE_READ, std::move(image_extents), req);
  */
}

template <typename I>
void ReplicatedWriteLog<I>::detain_guarded_request(GuardedRequest &&req)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;
  
  BlockGuardCell *cell;
  int r = m_write_log_guard.detain({req.first_block_num, req.last_block_num},
				   &req, &cell);
  if (r < 0) {
    lderr(cct) << "failed to detain guarded request: " << cpp_strerror(r)
		   << dendl;
    m_image_ctx.op_work_queue->queue(req.on_guard_acquire, r);
    return;
  } else if (r > 0) { 
    ldout(cct, 20) << "detaining guarded request due to in-flight requests: "
                   << "start=" << req.first_block_num << ", "
		   << "end=" << req.last_block_num << dendl;
    return;
  }

  ldout(cct, 20) << "in-flight request cell: " << cell << dendl;
  req.on_guard_acquire->acquired(cell);
}

template <typename I>
void ReplicatedWriteLog<I>::release_guarded_request(BlockGuardCell *cell)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "cell=" << cell << dendl;

  WriteLogGuard::BlockOperations block_ops;
  m_write_log_guard.release(cell, &block_ops);

  for (auto &op : block_ops) {
    detain_guarded_request(std::move(op));
  }
}

/**
 * This is the custodian of the BlockGuard cell for this write, and
 * the state information about the progress of this write. This object
 * lives until the write is persisted in all (live) log replicas.
 * User write op may be completed from here before the write
 * persists. Block guard is not released until the write persists
 * everywhere (this is how we guarantee to each log repica that they
 * will never see overlapping writes).
 */
struct C_WriteRequest : public C_GuardedBlockIORequest {
  ImageCache::Extents image_extents;
  bufferlist bl;
  int fadvise_flags;
  Context *user_req; /* User write request */
  Context *_on_finish; /* Block guard release */
  std::atomic<bool> m_user_req_completed;
  C_WriteRequest(CephContext *cct, ImageCache::Extents &&image_extents,
		 bufferlist&& bl, int fadvise_flags, Context *user_req)
    : C_GuardedBlockIORequest(cct, NULL), image_extents(std::move(image_extents)),
      bl(std::move(bl)), fadvise_flags(fadvise_flags),
      user_req(user_req), _on_finish(NULL), m_user_req_completed(false) {
  }

  void complete_user_request(int r) {
    bool initial = false;
    if (m_user_req_completed.compare_exchange_strong(initial, true)) {
      user_req->complete(r);
      // TODO: Make a WQ addressable from here
      //m_image_ctx.op_work_queue->queue(user_req, r)
    }
  }
  
  virtual void send() override {
    /* Should never be called */
    ldout(cct, 6) << dendl;
  }
  
  virtual void finish(int r) {
    ldout(cct, 6) << dendl;

    complete_user_request(r);
    _on_finish->complete(0);
  }
  
  virtual const char *get_name() const override {
    return "C_WriteRequest";
  }  
};

/*
 * Performs the log event append operation for all of the scheduled
 * events.
 */
template <typename I>
void ReplicatedWriteLog<I>::append_scheduled_ops(void)
{
  WriteLogOperations ops;
  {
    Mutex::Locker locker(m_lock);
    std::swap(ops, m_ops_to_append);
  }

  /* Ops subsequently scheduled for flush may finish before these,
   * which is fine. We're unconcerned with completion order until we
   * get to the log message append step. */
  if (ops.size()) {
    alloc_and_append_entries(ops);
  }
}

/*
 * Takes custody of ops. They'll all get their log entries appended,
 * and have their on_write_persist contexts completed once they and
 * all prior log entries are persisted everywhere.
 */
template <typename I>
void ReplicatedWriteLog<I>::schedule_append(WriteLogOperations &ops)
{
  {
    Mutex::Locker locker(m_lock);

    m_ops_to_append.splice(m_ops_to_append.end(), ops);
  }

  /* TODO: push this to a finisher */
  append_scheduled_ops();
}

/*
 * Performs the pme block flush on all scheduled ops, then schedules
 * the log event append operation for all of them.
 */
template <typename I>
void ReplicatedWriteLog<I>::flush_then_append_scheduled_ops(void)
{
  WriteLogOperations ops;
  {
    Mutex::Locker locker(m_lock);
    std::swap(ops, m_ops_to_flush);
  }

  /* Ops subsequently scheduled for flush may finish before these,
   * which is fine. We're unconcerned with completion order until we
   * get to the log message append step. */
  if (ops.size()) {
    flush_pmem_blocks(ops);
    schedule_append(ops);
  }
}

/*
 * Takes custody of ops. They'll all get their pmem blocks flushed,
 * then get their log entries appended.
 */
template <typename I>
void ReplicatedWriteLog<I>::schedule_flush_and_append(WriteLogOperations &ops)
{
  {
    Mutex::Locker locker(m_lock);

    m_ops_to_flush.splice(m_ops_to_flush.end(), ops);
  }

  /* TODO: push this to a finisher */
  flush_then_append_scheduled_ops();
}

/*
 * Flush the pmem regions for the data blocks of a set of operations
 */
template <typename I>
void ReplicatedWriteLog<I>::flush_pmem_blocks(WriteLogOperations &ops)
{
  for (auto &operation : ops) {
    pmemobj_flush(m_log_pool, operation->log_entry->pmem_block, operation->log_entry->ram_entry.write_bytes);
  }
  /* Drain once for all */
  pmemobj_drain(m_log_pool);
}

/*
 * Allocate the (already resrved) write log entries for a set of operations.
 *
 * Locking:
 * Acquires m_lock
 */
template <typename I>
void ReplicatedWriteLog<I>::alloc_op_log_entries(WriteLogOperations &ops)
{
  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);
  struct WriteLogPmemEntry *pmem_log_entries = D_RW(D_RW(pool_root)->log_entries);

  /* Allocate the (already reserved) log entries */
  {
    Mutex::Locker locker(m_lock);

    for (auto &operation : ops) {
      operation->log_entry->log_entry_index = m_first_free_entry;
      m_first_free_entry = (m_first_free_entry + 1) % m_total_log_entries;
      operation->log_entry->pmem_entry = &pmem_log_entries[operation->log_entry->log_entry_index];
      operation->log_entry->ram_entry.entry_valid = 1;
    }
  }
}

/*
 * Write and persist the (already allocated) write log entries for a
 * set of ops.
 */
template <typename I>
int ReplicatedWriteLog<I>::append_op_log_entries(WriteLogOperations &ops)
{
  CephContext *cct = m_image_ctx.cct;
  
  /* Write log entries */
  TX_BEGIN(m_log_pool) {
    for (auto &operation : ops) {
      TX_MEMCPY(operation->log_entry->pmem_entry,
		&operation->log_entry->ram_entry,
		sizeof(operation->log_entry->ram_entry));
    }
  } TX_ONCOMMIT {
  } TX_ONABORT {
    lderr(cct) << "failed to commit log entries (" << m_log_pool_name << ")" << dendl;
    return(-EIO);
  } TX_END;
  return 0;
}

/*
 * Complete a set of write ops with the result of append_op_entries.
 */
template <typename I>
void ReplicatedWriteLog<I>::complete_op_log_entries(WriteLogOperations &ops, int result)
{
  for (auto &operation : ops) {
    operation->on_write_persist->complete(result);
  }
}

/*
 * Allocate the (already resrved) write log entries for a set of operations, then
 * write and persist them all in a single transaction.
 *
 * Locking:
 * Acquires m_lock, m_log_append_lock
 */
template <typename I>
void ReplicatedWriteLog<I>::alloc_and_append_entries(WriteLogOperations &ops)
{
  int append_result = 0;
  {
    /*
     * We hold a read lock on m_log_append_lock across the sequence of
     * assigning a log entry location to each of these write log ops
     * and persistently writing it. A write lock is then unavailable
     * until this group of log messages is persisted. 
     */
    RWLock::RLocker appender_locker(m_log_append_lock);
    alloc_op_log_entries(ops);
    append_result = append_op_log_entries(ops);
  }
  {
    /* 
     * These log entries are persisted, but there may be log entries
     * positioned before them being persisted by another thread that
     * are not yet complete. If so, that thread will still hold the
     * read lock. 
     * 
     * We acquire (momentarily) a write lock on m_log_append_lock
     * before completing these write log ops to ensure we wait for
     * concurrent log append operations since they may appear before
     * these entries.
     */
    RWLock::WLocker completer_locker(m_log_append_lock);
  }
  complete_op_log_entries(ops, append_result);
}

template <typename I>
void ReplicatedWriteLog<I>::complete_write_req(C_WriteRequest *write_req, int result)
{
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 6) << "cell=" << write_req->m_cell << dendl;
  release_guarded_request(write_req->m_cell);
}

/**
 * Takes custody of write_req.
 *
 * Locking:
 * Acquires m_lock
 */
template <typename I>
void ReplicatedWriteLog<I>::dispatch_aio_write(C_WriteRequest *write_req)
{
  CephContext *cct = m_image_ctx.cct;
  
  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);

  ldout(cct,6) << "bl=[" << write_req->bl << "]" << dendl;
  uint8_t *pmem_blocks = D_RW(D_RW(pool_root)->data_blocks);

  ExtentsSummary<ImageCache::Extents> image_extents_summary(write_req->image_extents);

  WriteLogEntries log_entries;
  WriteLogOperationSet *set(new WriteLogOperationSet(
    cct, m_current_sync_point, m_persist_on_flush,
    BlockExtent(image_extents_summary.first_block,
		image_extents_summary.last_block), write_req));
  {
    uint64_t buffer_offset = 0;
    Mutex::Locker locker(m_lock);
    /* TODO: If there isn't space for this whole write, defer it */
    if (m_free_log_entries < write_req->image_extents.size()) {
      ldout(cct, 6) << "wait for " << write_req->image_extents.size() << " log entries" << dendl;
    } else if (m_free_blocks < image_extents_summary.total_bytes / BLOCK_SIZE) {
      ldout(cct, 6) << "wait for " << image_extents_summary.total_bytes / BLOCK_SIZE << " data blocks" << dendl;
    } else {
      for (auto &extent : write_req->image_extents) {
	/* operation->on_write_persist connected to m_prior_log_entries_persisted Gather */
	WriteLogOperation *operation =
	  new WriteLogOperation(set, extent.first, extent.second);
	set->operations.emplace_back(operation);
	log_entries.emplace_back(operation->log_entry);

	/* Reserve both log entry and data block space */
	m_free_log_entries--;
	m_free_blocks -= operation->log_entry->ram_entry.write_bytes / BLOCK_SIZE;

	/* Allocate data blocks */
	operation->log_entry->ram_entry.first_pool_block = m_next_free_block;
	operation->log_entry->ram_entry.has_data = 1;
	m_next_free_block = (m_next_free_block + 1) % m_total_blocks;
	operation->log_entry->pmem_block =
	  pmem_blocks + (operation->log_entry->ram_entry.first_pool_block * BLOCK_SIZE);
	operation->log_entry->ram_entry.sync_gen_number = m_current_sync_gen;
	if (set->m_persist_on_flush) {
	  /* Persist on flush. Sequence #0 is never used. */
	  operation->log_entry->ram_entry.write_sequence_number = 0;
	} else {
	  /* Persist on write */
	  operation->log_entry->ram_entry.write_sequence_number = ++m_last_op_sequence_num;
	  operation->log_entry->ram_entry.sequenced = 1;
	}
	operation->log_entry->ram_entry.sync_point = 0;
	operation->log_entry->ram_entry.unmap = 0;
	operation->bl.substr_of(write_req->bl, buffer_offset,
				operation->log_entry->ram_entry.write_bytes);
	buffer_offset += operation->log_entry->ram_entry.write_bytes;
	ldout(cct, 6) << "operation=[" << *operation << "]" << dendl;
      }
    }
  }

  write_req->_on_finish =
    new FunctionContext([this, write_req](int r) {
	complete_write_req(write_req, r);
      });

  m_blocks_to_log_entries.add_entries(log_entries);
  
  /* All extent ops subs created */
  set->m_extent_ops->activate();
  
  /* Write data */
  for (auto &operation : set->operations) {
    bufferlist::iterator i(&operation->bl);
    ldout(cct, 6) << operation->bl << dendl;
    i.copy((unsigned)operation->log_entry->ram_entry.write_bytes, (char*)operation->log_entry->pmem_block);
  }

  if (set->m_persist_on_flush) {
    /* 
     * We're done with the caller's buffer, and not guaranteeing
     * persistence until the next flush. The block guard for this
     * write_req will not be released until the write is persisted
     * everywhere.
     */
    write_req->complete_user_request(0);
  }
  
  WriteLogOperations ops_copy = set->operations;
  schedule_flush_and_append(ops_copy);
}

template <typename I>
void ReplicatedWriteLog<I>::aio_write(Extents &&image_extents,
                                  bufferlist&& bl,
                                  int fadvise_flags,
                                  Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  //ldout(cct, 20) << "image_extents=" << image_extents << ", "
  //               << "on_finish=" << on_finish << dendl;

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  // TODO: Handle unaligned IO. RMW must block overlapping writes (to
  // the limits of the read phase) and wait for prior overlaping
  // writes to complete.
  if (!is_block_aligned(image_extents)) {
    lderr(cct) << "unaligned write fails" << dendl;
    for (auto &extent : image_extents) {
      lderr(cct) << "start: " << extent.first << " length: " << extent.second << dendl;
    }
    on_finish->complete(-EINVAL);
    return;
  }

  ExtentsSummary<ImageCache::Extents> image_extents_summary(image_extents);

  C_WriteRequest *write_req =
    new C_WriteRequest(cct, std::move(image_extents), std::move(bl), fadvise_flags, on_finish);

  /* The lambda below will be called when the block guard for all
   * blocks affected by this write is obtained */
  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext([this, write_req](BlockGuardCell *cell) {
      CephContext *cct = m_image_ctx.cct;
      ldout(cct, 6) << "cell=" << cell << dendl;

      write_req->m_cell = cell;

      /* TODO: Defer write_req until log resources available */
      dispatch_aio_write(write_req);
    });

  GuardedRequest guarded(image_extents_summary.first_block,
			 image_extents_summary.last_block,
			 guarded_ctx);
  detain_guarded_request(std::move(guarded));
}

template <typename I>
void ReplicatedWriteLog<I>::aio_discard(uint64_t offset, uint64_t length,
                                    bool skip_partial_discard, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "offset=" << offset << ", "
                 << "length=" << length << ", "
                 << "on_finish=" << on_finish << dendl;

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  if (m_image_ctx.persistent_cache_enabled) {
    m_image_cache.aio_discard(offset, length, skip_partial_discard, on_finish);
  } else {
    m_image_writeback.aio_discard(offset, length, skip_partial_discard, on_finish);
  }
  /*
  if (!is_block_aligned({{offset, length}})) {
    // For clients that don't use LBA extents, re-align the discard request
    // to work with the cache
    ldout(cct, 20) << "aligning discard to block size" << dendl;

    // TODO: for non-aligned extents, invalidate the associated block-aligned
    // regions in the cache (if any), send the aligned extents to the cache
    // and the un-aligned extents directly to back to librbd
  }

  // TODO invalidate discard blocks until writethrough/back support added
  C_Gather *ctx = new C_Gather(cct, on_finish);
  Context *invalidate_done_ctx = ctx->new_sub();

  m_image_writeback.aio_discard(offset, length, skip_partial_discard, ctx->new_sub());

  ctx->activate();

  Context *invalidate_ctx = new FunctionContext(
    [this, offset, length, invalidate_done_ctx](int r) {
      invalidate({{offset, length}}, invalidate_done_ctx);
    });
  flush(invalidate_ctx);
  */
}

template <typename I>
void ReplicatedWriteLog<I>::aio_flush(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "on_finish=" << on_finish << dendl;

  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  if (!m_flush_seen) {
    Mutex::Locker locker(m_lock);
    if (!m_flush_seen) {
      ldout(cct, 5) << "flush seen" << dendl;
      m_flush_seen = true;
      if (!m_persist_on_flush && m_persist_on_write_until_flush) {
	m_persist_on_flush = true;
	ldout(cct, 5) << "now persisting on flush" << dendl;
      }
    }
  }
  
  if (m_image_ctx.persistent_cache_enabled) {
    m_image_cache.aio_flush(on_finish);
  } else {
    m_image_writeback.aio_flush(on_finish);
  }
  /*
  Context *ctx = new FunctionContext(
    [this, on_finish](int r) {
      if (r < 0) {
        on_finish->complete(r);
      }
      m_image_writeback.aio_flush(on_finish);
    });

  flush(ctx);
  */
}

template <typename I>
void ReplicatedWriteLog<I>::aio_writesame(uint64_t offset, uint64_t length,
                                      bufferlist&& bl, int fadvise_flags,
                                      Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "offset=" << offset << ", "
                 << "length=" << length << ", "
                 << "data_len=" << bl.length() << ", "
                 << "on_finish=" << on_finish << dendl;
  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  if (m_image_ctx.persistent_cache_enabled) {
    m_image_cache.aio_writesame(offset, length, std::move(bl), fadvise_flags, on_finish);
  } else {
    m_image_writeback.aio_writesame(offset, length, std::move(bl), fadvise_flags, on_finish);
  }

  /*
  bufferlist total_bl;

  uint64_t left = length;
  while(left) {
    total_bl.append(bl);
    left -= bl.length();
  }
  assert(length == total_bl.length());
  aio_write({{offset, length}}, std::move(total_bl), fadvise_flags, on_finish);
  */
}

template <typename I>
void ReplicatedWriteLog<I>::aio_compare_and_write(Extents &&image_extents,
                                                     bufferlist&& cmp_bl,
                                                     bufferlist&& bl,
                                                     uint64_t *mismatch_offset,
                                                     int fadvise_flags,
                                                     Context *on_finish) {

  // TODO:
  // Compare source may be RWL, image cache, or image.
  // Write will be to RWL

  //CephContext *cct = m_image_ctx.cct;

  //ldout(cct, 20) << "image_extents=" << image_extents << ", "
  //               << "on_finish=" << on_finish << dendl;

  if (m_image_ctx.persistent_cache_enabled) {
    m_image_cache.aio_compare_and_write(
      std::move(image_extents), std::move(cmp_bl), std::move(bl), mismatch_offset,
      fadvise_flags, on_finish);
  } else {
    m_image_writeback.aio_compare_and_write(
      std::move(image_extents), std::move(cmp_bl), std::move(bl), mismatch_offset,
      fadvise_flags, on_finish);
  }
}

/**
 * Called when the specified sync point can be appended to the log
 */
template <typename I>
void ReplicatedWriteLog<I>::append_sync_point(SyncPoint *sync_point, int status) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  assert(m_lock.is_locked_by_me());
  sync_point->m_prior_log_entries_persisted_status = status;
  // TODO: Write sync point, and complete this
}

/**
 * Begin a new sync point
 */
template <typename I>
void ReplicatedWriteLog<I>::new_sync_point(void) {
  CephContext *cct = m_image_ctx.cct;
  SyncPoint *old_sync_point = m_current_sync_point;
  SyncPoint *new_sync_point;
  ldout(cct, 20) << dendl;

  assert(m_lock.is_locked_by_me());

  if (NULL != old_sync_point) {
    new_sync_point = new SyncPoint(cct, ++m_current_sync_gen);
  } else {
    /* First sync point - don't advance gen number */
    new_sync_point = new SyncPoint(cct, m_current_sync_gen);
  }
  m_current_sync_point = new_sync_point;
  
  if (NULL != old_sync_point) {
    old_sync_point->m_final_op_sequence_num = m_last_op_sequence_num;
    /* Append of new sync point deferred until this sync point is persisted */
    old_sync_point->m_on_sync_point_persisted = m_current_sync_point->m_prior_log_entries_persisted->new_sub();
    /* This sync point will acquire no more sub-ops */
    old_sync_point->m_prior_log_entries_persisted->activate();
  }

  /* TODO: Make this part of the SyncPoint object. */
  new_sync_point->m_prior_log_entries_persisted->
    set_finisher(new FunctionContext([this, new_sync_point](int r) {
	  CephContext *cct = m_image_ctx.cct;
	  ldout(cct, 20) << "Prior log entries persisted for sync point =[" << new_sync_point << "]" << dendl;
	  append_sync_point(new_sync_point, r);
	}));
  
  if (NULL != old_sync_point) {
    ldout(cct,6) << "new sync point = [" << m_current_sync_point
		 << "], prior = [" << old_sync_point << "]" << dendl;
  } else {
    ldout(cct,6) << "first sync point = [" << m_current_sync_point
		 << "]" << dendl;
  }
}

template <typename I>
void ReplicatedWriteLog<I>::init(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;
  TOID(struct WriteLogPoolRoot) pool_root;

  ldout(cct,5) << "rbd_rwl_enabled:" << cct->_conf->get_val<bool>("rbd_rwl_enabled") << dendl;
  ldout(cct,5) << "rbd_rwl_size:" << cct->_conf->get_val<uint64_t>("rbd_rwl_size") << dendl;
  std::string rwl_path = cct->_conf->get_val<std::string>("rbd_rwl_path");
  ldout(cct,5) << "rbd_rwl_path:" << rwl_path << dendl;

  std::string log_pool_name = rwl_path + "/rbd-rwl." + m_image_ctx.id + ".pool";
  std::string log_poolset_name = rwl_path + "/rbd-rwl." + m_image_ctx.id + ".poolset";
  m_log_pool_size = ceph::max(cct->_conf->get_val<uint64_t>("rbd_rwl_size"), MIN_POOL_SIZE);
  //m_policy->set_block_count(m_image_ctx.size / BLOCK_SIZE);

  if (access(log_poolset_name.c_str(), F_OK) == 0) {
    m_log_pool_name = log_poolset_name;
  } else {
    m_log_pool_name = log_pool_name;
    lderr(cct) << "failed to open poolset" << log_poolset_name
	       << ":" << pmemobj_errormsg()
	       << ". Opening/creating simple/unreplicated pool" << dendl;
  }
  
  if (access(m_log_pool_name.c_str(), F_OK) != 0) {
    if ((m_log_pool =
	 pmemobj_create(m_log_pool_name.c_str(),
			rwl_pool_layout_name,
			m_log_pool_size,
			(S_IWUSR | S_IRUSR))) == NULL) {
      lderr(cct) << "failed to create pool (" << m_log_pool_name << ")"
                 << pmemobj_errormsg() << dendl;
      on_finish->complete(-1);
      return;
    }
    pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);

    /* new pool, calculate and store metadata */
    size_t effective_pool_size = (size_t)(m_log_pool_size * USABLE_SIZE);
    size_t small_write_size = BLOCK_SIZE + sizeof(struct WriteLogPmemEntry);
    uint64_t num_small_writes = (uint64_t)(effective_pool_size / small_write_size);
    m_valid_entry_hint = 0;
    m_free_entry_hint = 0;
    /* Log ring empty */
    m_first_free_entry = 0;
    m_first_valid_entry = 0;
    /* Block pool empty */
    m_next_free_block = 0;
    m_last_free_block = 0;
    TX_BEGIN(m_log_pool) {
      TX_ADD(pool_root);
      D_RW(pool_root)->header.layout_version = RWL_POOL_VERSION;
      D_RW(pool_root)->block_size = BLOCK_SIZE;
      D_RW(pool_root)->num_blocks = num_small_writes-1; // leave one free
      D_RW(pool_root)->num_log_entries = num_small_writes-1; // leave one free
      D_RW(pool_root)->data_blocks = TX_ALLOC(uint8_t, num_small_writes * BLOCK_SIZE);
      D_RW(pool_root)->log_entries = TX_ZALLOC(struct WriteLogPmemEntry, num_small_writes);
      D_RW(pool_root)->valid_entry_hint = m_valid_entry_hint;
      D_RW(pool_root)->free_entry_hint = m_free_entry_hint;
    } TX_ONCOMMIT {
      m_total_log_entries = D_RO(pool_root)->num_log_entries;
      m_free_log_entries = D_RO(pool_root)->num_log_entries;
      m_total_blocks = D_RO(pool_root)->num_blocks;
      m_free_blocks = D_RO(pool_root)->num_blocks;
    } TX_ONABORT {
      m_total_log_entries = 0;
      m_total_blocks = 0;
      m_free_log_entries = 0;
      m_free_blocks = 0;
      lderr(cct) << "failed to initialize pool (" << m_log_pool_name << ")" << dendl;
      on_finish->complete(-1);
      return;
    } TX_FINALLY {
    } TX_END;
  } else {
    if ((m_log_pool =
	 pmemobj_open(m_log_pool_name.c_str(),
		      rwl_pool_layout_name)) == NULL) {
      lderr(cct) << "failed to open pool (" << m_log_pool_name << "): "
                 << pmemobj_errormsg() << dendl;
      on_finish->complete(-1);
      return;
    }
    pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);
    if (D_RO(pool_root)->header.layout_version != RWL_POOL_VERSION) {
      lderr(cct) << "Pool layout version is " << D_RO(pool_root)->header.layout_version
		 << " expected " << RWL_POOL_VERSION << dendl;
      on_finish->complete(-1);
      return;
    }
    if (D_RO(pool_root)->block_size != BLOCK_SIZE) {
      lderr(cct) << "Pool block size is " << D_RO(pool_root)->block_size << " expected " << BLOCK_SIZE << dendl;
      on_finish->complete(-1);
      return;
    }
    m_total_log_entries = D_RO(pool_root)->num_log_entries;
    m_free_log_entries = D_RO(pool_root)->num_log_entries;
    m_total_blocks = D_RO(pool_root)->num_blocks;
    m_free_blocks = D_RO(pool_root)->num_blocks;
    m_valid_entry_hint = D_RO(pool_root)->valid_entry_hint;
    m_free_entry_hint = D_RO(pool_root)->free_entry_hint;
    /* TODO: Actually load all the log entries already persisted */
    /* TODO: Set m_current_sync_gen to the successor of the last one seen in the log */
    ldout(cct,5) << "pool " << m_log_pool_name << "has " << D_RO(pool_root)->num_log_entries <<
      " log entries and " << D_RO(pool_root)->num_blocks << " data blocks" << dendl;
    if (m_valid_entry_hint == m_free_entry_hint) {
      ldout(cct,5) << "write log is empy" << dendl;
    }
  }

  /* Start the sync point following the last one seen in the log */
  m_current_sync_point = new SyncPoint(cct, m_current_sync_gen);
  ldout(cct,6) << "new sync point = [" << m_current_sync_point << "]" << dendl;
  
  // chain the initialization of the meta, image, and journal stores
  Context *ctx = new FunctionContext(
    [this, on_finish](int r) {
      if (r >= 0) {
	// Init succeeded
      }
      on_finish->complete(r);
    });
  if (m_image_ctx.persistent_cache_enabled) {
    m_image_cache.init(ctx);
  } else {
    ctx->complete(0);
  }
}

template <typename I>
void ReplicatedWriteLog<I>::shut_down(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  // TODO flush all in-flight IO and pending writeback prior to shut down

  Context *ctx = new FunctionContext(
    [this, on_finish](int r) {
      if (NULL != m_log_pool) {
	pmemobj_close(m_log_pool);
      }
      on_finish->complete(r);
    });
  if (m_image_ctx.persistent_cache_enabled) {
    ctx = new FunctionContext(
      [this, ctx](int r) {
        if (r < 0) ctx->complete(r);
	m_image_cache.shut_down(ctx);
      });
  }
  ctx = new FunctionContext(
    [this, ctx](int r) {
      // flush writeback journal to OSDs
      if (r < 0) ctx->complete(r);
      flush(ctx);
    });

  {
    Mutex::Locker locker(m_lock);
    m_async_op_tracker.wait(m_image_ctx, ctx);
  }
}

template <typename I>
void ReplicatedWriteLog<I>::invalidate(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  Context *ctx = new FunctionContext(
    [this, on_finish](int r) {
      if (m_image_ctx.persistent_cache_enabled) {
	m_image_cache.invalidate(on_finish);
      } else {
	on_finish->complete(0);
      }
    });
  // TODO
  invalidate({{0, m_image_ctx.size}}, ctx);
}

template <typename I>
void ReplicatedWriteLog<I>::wake_up() {
  assert(m_lock.is_locked());
  if (m_wake_up_scheduled || m_async_op_tracker.is_waiting()) {
    return;
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  m_wake_up_scheduled = true;
  m_async_op_tracker.start_op();
  m_image_ctx.op_work_queue->queue(new FunctionContext(
    [this](int r) {
      process_work();
    }), 0);
}

template <typename I>
void ReplicatedWriteLog<I>::process_work() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  do {
    process_writeback_dirty_blocks();
    process_detained_block_ios();
    process_deferred_block_ios();

    // TODO
    Contexts post_work_contexts;
    {
      Mutex::Locker locker(m_lock);
      post_work_contexts.swap(m_post_work_contexts);
    }
    if (!post_work_contexts.empty()) {
      for (auto ctx : post_work_contexts) {
        //TODO: fix flush post work
        ctx->complete(0);
      }
      continue;
    }
  } while (false); // TODO need metric to only perform X amount of work per cycle

  // process delayed shut down request (if any)
  {
    Mutex::Locker locker(m_lock);
    m_wake_up_scheduled = false;
    m_async_op_tracker.finish_op();
  }
}

template <typename I>
bool ReplicatedWriteLog<I>::is_work_available() const {
  Mutex::Locker locker(m_lock);
  return (!m_detained_block_ios.empty() ||
          !m_deferred_block_ios.empty());
}

template <typename I>
void ReplicatedWriteLog<I>::process_writeback_dirty_blocks() {
#if 0
  CephContext *cct = m_image_ctx.cct;

  // TODO throttle the amount of in-flight writebacks
  while (true) {
    uint64_t tid = 0;
    uint64_t block;
    IOType io_type;
    bool demoted;
    int r = m_journal_store->get_writeback_event(&tid, &block, &io_type,
                                                 &demoted);
    //int r = m_policy->get_writeback_block(&block);
    if (r == -ENODATA || r == -EBUSY) {
      // nothing to writeback
      return;
    } else if (r < 0) {
      lderr(cct) << "failed to retrieve writeback block: "
                 << cpp_strerror(r) << dendl;
      return;
    }

    // block is now detained -- safe for writeback
    C_WritebackRequest<I> *req = new C_WritebackRequest<I>(
      m_image_ctx, m_image_writeback, *m_policy, *m_journal_store,
      *m_image_store, m_release_block, m_async_op_tracker, tid, block, io_type,
      demoted, BLOCK_SIZE);
    req->send();
  }
#endif
}

template <typename I>
void ReplicatedWriteLog<I>::process_detained_block_ios() {
  BlockGuard::BlockIOs block_ios;
  {
    Mutex::Locker locker(m_lock);
    std::swap(block_ios, m_detained_block_ios);
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "block_ios=" << block_ios.size() << dendl;
  for (auto &block_io : block_ios) {
    ldout(cct, 20) << "block_io=" << &block_io << dendl;
    //map_block(false, std::move(block_io));
  }
}

template <typename I>
void ReplicatedWriteLog<I>::process_deferred_block_ios() {
  BlockGuard::BlockIOs block_ios;
  {
    Mutex::Locker locker(m_lock);
    std::swap(block_ios, m_deferred_block_ios);
  }

  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "block_ios=" << block_ios.size() << dendl;
  for (auto &block_io : block_ios) {
    ldout(cct, 20) << "block_io=" << &block_io << dendl;
    //map_block(true, std::move(block_io));
  }
}

template <typename I>
void ReplicatedWriteLog<I>::invalidate(Extents&& image_extents,
                                   Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "image_extents=" << image_extents << dendl;

  // TODO - ensure sync with in-flight flushes
  for (auto &extent : image_extents) {
    uint64_t image_offset = extent.first;
    uint64_t image_length = extent.second;
    while (image_length > 0) {
      uint32_t block_start_offset = image_offset % BLOCK_SIZE;
      uint32_t block_end_offset = MIN(block_start_offset + image_length,
                                      BLOCK_SIZE);
      uint32_t block_length = block_end_offset - block_start_offset;

      image_offset += block_length;
      image_length -= block_length;
    }
  }

  on_finish->complete(0);
}

/*
 * Internal flush - will actually flush the RWL.
 *
 * User flushes should arrive at aio_flush(), and only flush prior
 * writes to all log replicas.
 */
template <typename I>
void ReplicatedWriteLog<I>::flush(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "**** NOT FLUSHED ****" << dendl;

  /* TODO: Actually flush */
  on_finish->complete(0);
}

} // namespace cache
} // namespace librbd

template class librbd::cache::ReplicatedWriteLog<librbd::ImageCtx>;

/* Local Variables: */
/* eval: (c-set-offset 'innamespace 0) */
/* End: */
