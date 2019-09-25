// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG_INTERNAL
#define CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG_INTERNAL

#include "ReplicatedWriteLog.h"
#include "ImageCache.h"
#include "librbd/ImageCtx.h"
#include "common/environment.h"
#include "common/hostname.h"
#include "common/ceph_context.h"
#include "common/config_proxy.h"
#include <map>
#include <vector>

namespace librbd {

namespace cache {

using namespace librbd::cache::rwl;

namespace rwl {

/* Defer a set of Contexts until destruct/exit. Used for deferring
 * work on a given thread until a required lock is dropped. */
class DeferredContexts {
private:
  std::vector<Context*> contexts;
public:
  ~DeferredContexts() {
    finish_contexts(nullptr, contexts, 0);
  }
  void add(Context* ctx) {
    contexts.push_back(ctx);
  }
};

/* Pmem structures */
POBJ_LAYOUT_BEGIN(rbd_rwl);
POBJ_LAYOUT_ROOT(rbd_rwl, struct WriteLogPoolRoot);
POBJ_LAYOUT_TOID(rbd_rwl, uint8_t);
POBJ_LAYOUT_TOID(rbd_rwl, struct WriteLogPmemEntry);
POBJ_LAYOUT_END(rbd_rwl);

struct WriteLogPmemEntry {
  uint64_t sync_gen_number = 0;
  uint64_t write_sequence_number = 0;
  uint64_t image_offset_bytes;
  uint64_t write_bytes;
  TOID(uint8_t) write_data;
  struct {
    uint8_t entry_valid :1; /* if 0, this entry is free */
    uint8_t sync_point :1;  /* No data. No write sequence number. Marks sync
			       point for this sync gen number */
    uint8_t sequenced :1;   /* write sequence number is valid */
    uint8_t has_data :1;    /* write_data field is valid (else ignore) */
    uint8_t discard :1;     /* has_data will be 0 if this is a discard */
    uint8_t writesame :1;   /* ws_datalen indicates length of data at write_bytes */
  };
  uint32_t ws_datalen = 0;  /* Length of data buffer (writesame only) */
  uint32_t entry_index = 0; /* For debug consistency check. Can be removed if
			     * we need the space */
  WriteLogPmemEntry(const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : image_offset_bytes(image_offset_bytes), write_bytes(write_bytes),
      entry_valid(0), sync_point(0), sequenced(0), has_data(0), discard(0), writesame(0) {
  }
  const BlockExtent block_extent();
  bool is_sync_point() {
    return sync_point;
  }
  bool is_discard() {
    return discard;
  }
  bool is_writesame() {
    return writesame;
  }
  bool is_write() {
    /* Log entry is a basic write */
    return !is_sync_point() && !is_discard() && !is_writesame();
  }
  bool is_writer() {
    /* Log entry is any type that writes data */
    return is_write() || is_discard() || is_writesame();
  }
  const uint64_t get_offset_bytes() { return image_offset_bytes; }
  const uint64_t get_write_bytes() { return write_bytes; }
  friend std::ostream &operator<<(std::ostream &os,
				  const WriteLogPmemEntry &entry) {
    os << "entry_valid=" << (bool)entry.entry_valid << ", "
       << "sync_point=" << (bool)entry.sync_point << ", "
       << "sequenced=" << (bool)entry.sequenced << ", "
       << "has_data=" << (bool)entry.has_data << ", "
       << "discard=" << (bool)entry.discard << ", "
       << "writesame=" << (bool)entry.writesame << ", "
       << "sync_gen_number=" << entry.sync_gen_number << ", "
       << "write_sequence_number=" << entry.write_sequence_number << ", "
       << "image_offset_bytes=" << entry.image_offset_bytes << ", "
       << "write_bytes=" << entry.write_bytes << ", "
       << "ws_datalen=" << entry.ws_datalen << ", "
       << "entry_index=" << entry.entry_index;
    return os;
  };
};

static_assert(sizeof(WriteLogPmemEntry) == 64);

struct WriteLogPoolRoot {
  union {
    struct {
      uint8_t layout_version;    /* Version of this structure (RWL_POOL_VERSION) */
    };
    uint64_t _u64;
  } header;
  TOID(struct WriteLogPmemEntry) log_entries;   /* contiguous array of log entries */
  uint64_t pool_size;
  uint64_t flushed_sync_gen;     /* All writing entries with this or a lower
				  * sync gen number are flushed. */
  uint32_t block_size;		 /* block size */
  uint32_t num_log_entries;
  uint32_t first_free_entry;     /* Entry following the newest valid entry */
  uint32_t first_valid_entry;    /* Index of the oldest valid entry in the log */
};

static const bool RWL_VERBOSE_LOGGING = false;

typedef ReplicatedWriteLog<ImageCtx>::Extent Extent;
typedef ReplicatedWriteLog<ImageCtx>::Extents Extents;

/*
 * A BlockExtent identifies a range by first and last.
 *
 * An Extent ("image extent") identifies a range by start and length.
 *
 * The ImageCache interface is defined in terms of image extents, and
 * requires no alignment of the beginning or end of the extent. We
 * convert between image and block extents here using a "block size"
 * of 1.
 */
const BlockExtent block_extent(const uint64_t offset_bytes, const uint64_t length_bytes)
{
  return BlockExtent(offset_bytes,
		     offset_bytes + length_bytes - 1);
}

const BlockExtent block_extent(const Extent& image_extent)
{
  return block_extent(image_extent.first, image_extent.second);
}

const Extent image_extent(const BlockExtent& block_extent)
{
  return Extent(block_extent.block_start,
		block_extent.block_end - block_extent.block_start + 1);
}

const BlockExtent WriteLogPmemEntry::block_extent() {
  return BlockExtent(librbd::cache::rwl::block_extent(image_offset_bytes, write_bytes));
}

class GenericLogEntry {
public:
  WriteLogPmemEntry ram_entry;
  WriteLogPmemEntry *pmem_entry = nullptr;
  uint32_t log_entry_index = 0;
  bool completed = false;
  GenericLogEntry(const uint64_t image_offset_bytes = 0, const uint64_t write_bytes = 0)
    : ram_entry(image_offset_bytes, write_bytes) {
  };
  virtual ~GenericLogEntry() { };
  GenericLogEntry(const GenericLogEntry&) = delete;
  GenericLogEntry &operator=(const GenericLogEntry&) = delete;
  virtual unsigned int write_bytes() = 0;
  bool is_sync_point() { return ram_entry.is_sync_point(); }
  bool is_discard() { return ram_entry.is_discard(); }
  bool is_writesame() { return ram_entry.is_writesame(); }
  bool is_write() { return ram_entry.is_write(); }
  bool is_writer() { return ram_entry.is_writer(); }
  virtual const GenericLogEntry* get_log_entry() = 0;
  virtual const SyncPointLogEntry* get_sync_point_log_entry() { return nullptr; }
  virtual const GeneralWriteLogEntry* get_gen_write_log_entry() { return nullptr; }
  virtual const WriteLogEntry* get_write_log_entry() { return nullptr; }
  virtual const WriteSameLogEntry* get_write_same_log_entry() { return nullptr; }
  virtual const DiscardLogEntry* get_discard_log_entry() { return nullptr; }
  virtual std::ostream &format(std::ostream &os) const {
    os << "ram_entry=[" << ram_entry << "], "
       << "pmem_entry=" << (void*)pmem_entry << ", "
       << "log_entry_index=" << log_entry_index << ", "
       << "completed=" << completed;
    return os;
  };
  friend std::ostream &operator<<(std::ostream &os,
				  const GenericLogEntry &entry) {
    return entry.format(os);
  }
};

class SyncPointLogEntry : public GenericLogEntry {
public:
  /* Writing entries using this sync gen number */
  std::atomic<unsigned int> m_writes = {0};
  /* Total bytes for all writing entries using this sync gen number */
  std::atomic<uint64_t> m_bytes = {0};
  /* Writing entries using this sync gen number that have completed */
  std::atomic<unsigned int> m_writes_completed = {0};
  /* Writing entries using this sync gen number that have completed flushing to the writeback interface */
  std::atomic<unsigned int> m_writes_flushed = {0};
  /* All writing entries using all prior sync gen numbers have been flushed */
  std::atomic<bool> m_prior_sync_point_flushed = {true};
  std::shared_ptr<SyncPointLogEntry> m_next_sync_point_entry = nullptr;
  SyncPointLogEntry(const uint64_t sync_gen_number) {
    ram_entry.sync_gen_number = sync_gen_number;
    ram_entry.sync_point = 1;
  };
  SyncPointLogEntry(const SyncPointLogEntry&) = delete;
  SyncPointLogEntry &operator=(const SyncPointLogEntry&) = delete;
  virtual inline unsigned int write_bytes() { return 0; }
  const GenericLogEntry* get_log_entry() override { return get_sync_point_log_entry(); }
  const SyncPointLogEntry* get_sync_point_log_entry() override { return this; }
  std::ostream &format(std::ostream &os) const {
    os << "(Sync Point) ";
    GenericLogEntry::format(os);
    os << ", "
       << "m_writes=" << m_writes << ", "
       << "m_bytes=" << m_bytes << ", "
       << "m_writes_completed=" << m_writes_completed << ", "
       << "m_writes_flushed=" << m_writes_flushed << ", "
       << "m_prior_sync_point_flushed=" << m_prior_sync_point_flushed << ", "
       << "m_next_sync_point_entry=" << m_next_sync_point_entry;
    return os;
  };
  friend std::ostream &operator<<(std::ostream &os,
				  const SyncPointLogEntry &entry) {
    return entry.format(os);
  }
};

class GeneralWriteLogEntry : public GenericLogEntry {
private:
  friend class WriteLogEntry;
  friend class DiscardLogEntry;
public:
  uint32_t referring_map_entries = 0;
  bool flushing = false;
  bool flushed = false; /* or invalidated */
  std::shared_ptr<SyncPointLogEntry> sync_point_entry;
  GeneralWriteLogEntry(std::shared_ptr<SyncPointLogEntry> sync_point_entry,
		       const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GenericLogEntry(image_offset_bytes, write_bytes), sync_point_entry(sync_point_entry) { }
  GeneralWriteLogEntry(const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GenericLogEntry(image_offset_bytes, write_bytes), sync_point_entry(nullptr) { }
  GeneralWriteLogEntry(const GeneralWriteLogEntry&) = delete;
  GeneralWriteLogEntry &operator=(const GeneralWriteLogEntry&) = delete;
  virtual inline unsigned int write_bytes() {
    /* The valid bytes in this ops data buffer. Discard and WS override. */
    return ram_entry.write_bytes;
  };
  virtual inline unsigned int bytes_dirty() {
    /* The bytes in the image this op makes dirty. Discard and WS override. */
    return write_bytes();
  };
  const BlockExtent block_extent() { return ram_entry.block_extent(); }
  const GenericLogEntry* get_log_entry() override { return get_gen_write_log_entry(); }
  const GeneralWriteLogEntry* get_gen_write_log_entry() override { return this; }
  uint32_t get_map_ref() { return(referring_map_entries); }
  void inc_map_ref() { referring_map_entries++; }
  void dec_map_ref() { referring_map_entries--; }
  std::ostream &format(std::ostream &os) const {
    GenericLogEntry::format(os);
    os << ", "
       << "sync_point_entry=[";
    if (sync_point_entry) {
      os << *sync_point_entry;
    } else {
      os << "nullptr";
    }
    os << "], "
       << "referring_map_entries=" << referring_map_entries << ", "
       << "flushing=" << flushing << ", "
       << "flushed=" << flushed;
    return os;
  };
  friend std::ostream &operator<<(std::ostream &os,
				  const GeneralWriteLogEntry &entry) {
    return entry.format(os);
  }
};

class WriteLogEntry : public GeneralWriteLogEntry {
protected:
  buffer::ptr pmem_bp;
  buffer::list pmem_bl;
  std::atomic<int> bl_refs = {0}; /* The refs held on pmem_bp by pmem_bl */

  void init_pmem_bp() {
    assert(!pmem_bp.get_raw());
    pmem_bp = buffer::ptr(buffer::create_static(this->write_bytes(), (char*)pmem_buffer));
  }

  /* Write same will override */
  virtual void init_bl(buffer::ptr &bp, buffer::list &bl) {
    bl.append(bp);
  }

  void init_pmem_bl() {
    pmem_bl.clear();
    init_pmem_bp();
    assert(pmem_bp.get_raw());
    int before_bl = pmem_bp.raw_nref();
    this->init_bl(pmem_bp, pmem_bl);
    int after_bl = pmem_bp.raw_nref();
    bl_refs = after_bl - before_bl;
  }

public:
  uint8_t *pmem_buffer = nullptr;
  WriteLogEntry(std::shared_ptr<SyncPointLogEntry> sync_point_entry,
		const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GeneralWriteLogEntry(sync_point_entry, image_offset_bytes, write_bytes) { }
  WriteLogEntry(const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GeneralWriteLogEntry(nullptr, image_offset_bytes, write_bytes) { }
  WriteLogEntry(const WriteLogEntry&) = delete;
  WriteLogEntry &operator=(const WriteLogEntry&) = delete;
  const BlockExtent block_extent();

  unsigned int reader_count() {
    if (pmem_bp.get_raw()) {
      return (pmem_bp.raw_nref() - bl_refs - 1);
    } else {
      return 0;
    }
  }

  /* Returns a ref to a bl containing bufferptrs to the entry pmem buffer */
  buffer::list &get_pmem_bl(Mutex &entry_bl_lock) {
    if (0 == bl_refs) {
      Mutex::Locker locker(entry_bl_lock);
      if (0 == bl_refs) {
	init_pmem_bl();
      }
      assert(0 != bl_refs);
    }
    return pmem_bl;
  };

  /* Constructs a new bl containing copies of pmem_bp */
  void copy_pmem_bl(Mutex &entry_bl_lock, bufferlist *out_bl) {
    this->get_pmem_bl(entry_bl_lock);
    /* pmem_bp is now initialized */
    buffer::ptr cloned_bp(pmem_bp.clone());
    out_bl->clear();
    this->init_bl(cloned_bp, *out_bl);
  }

  virtual const GenericLogEntry* get_log_entry() override { return get_write_log_entry(); }
  const WriteLogEntry* get_write_log_entry() override { return this; }
  std::ostream &format(std::ostream &os) const {
    os << "(Write) ";
    GeneralWriteLogEntry::format(os);
    os << ", "
       << "pmem_buffer=" << (void*)pmem_buffer << ", ";
    os << "pmem_bp=" << pmem_bp << ", ";
    os << "pmem_bl=" << pmem_bl << ", ";
    os << "bl_refs=" << bl_refs;
    return os;
  };
  friend std::ostream &operator<<(std::ostream &os,
				  const WriteLogEntry &entry) {
    return entry.format(os);
  }
};

class WriteSameLogEntry : public WriteLogEntry {
protected:
  void init_bl(buffer::ptr &bp, buffer::list &bl) override {
    for (uint64_t i = 0; i < ram_entry.write_bytes / ram_entry.ws_datalen; i++) {
      bl.append(bp);
    }
    int trailing_partial = ram_entry.write_bytes % ram_entry.ws_datalen;
    if (trailing_partial) {
      bl.append(bp, 0, trailing_partial);
    }
  };

public:
  WriteSameLogEntry(std::shared_ptr<SyncPointLogEntry> sync_point_entry,
		    const uint64_t image_offset_bytes, const uint64_t write_bytes,
		    const uint32_t data_length)
    : WriteLogEntry(sync_point_entry, image_offset_bytes, write_bytes) {
    ram_entry.writesame = 1;
    ram_entry.ws_datalen = data_length;
  };
  WriteSameLogEntry(const uint64_t image_offset_bytes, const uint64_t write_bytes,
		    const uint32_t data_length)
    : WriteLogEntry(nullptr, image_offset_bytes, write_bytes) {
    ram_entry.writesame = 1;
    ram_entry.ws_datalen = data_length;
  };
  WriteSameLogEntry(const WriteSameLogEntry&) = delete;
  WriteSameLogEntry &operator=(const WriteSameLogEntry&) = delete;
  virtual inline unsigned int write_bytes() override {
    /* The valid bytes in this ops data buffer. */
    return ram_entry.ws_datalen;
  };
  virtual inline unsigned int bytes_dirty() {
    /* The bytes in the image this op makes dirty. */
    return ram_entry.write_bytes;
  };
  const BlockExtent block_extent();
  const GenericLogEntry* get_log_entry() override { return get_write_same_log_entry(); }
  const WriteSameLogEntry* get_write_same_log_entry() override { return this; }
  std::ostream &format(std::ostream &os) const {
    os << "(WriteSame) ";
    WriteLogEntry::format(os);
    return os;
  };
  friend std::ostream &operator<<(std::ostream &os,
				  const WriteSameLogEntry &entry) {
    return entry.format(os);
  }
};

class DiscardLogEntry : public GeneralWriteLogEntry {
public:
  DiscardLogEntry(std::shared_ptr<SyncPointLogEntry> sync_point_entry,
		  const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GeneralWriteLogEntry(sync_point_entry, image_offset_bytes, write_bytes) {
    ram_entry.discard = 1;
  };
  DiscardLogEntry(const uint64_t image_offset_bytes, const uint64_t write_bytes)
    : GeneralWriteLogEntry(nullptr, image_offset_bytes, write_bytes) {
    ram_entry.discard = 1;
  };
  DiscardLogEntry(const DiscardLogEntry&) = delete;
  DiscardLogEntry &operator=(const DiscardLogEntry&) = delete;
  virtual inline unsigned int write_bytes() {
    /* The valid bytes in this ops data buffer. */
    return 0;
  };
  virtual inline unsigned int bytes_dirty() {
    /* The bytes in the image this op makes dirty. */
    return ram_entry.write_bytes;
  };
  const BlockExtent block_extent();
  const GenericLogEntry* get_log_entry() { return get_discard_log_entry(); }
  const DiscardLogEntry* get_discard_log_entry() { return this; }
  std::ostream &format(std::ostream &os) const {
    os << "(Discard) ";
    GeneralWriteLogEntry::format(os);
    return os;
  };
  friend std::ostream &operator<<(std::ostream &os,
				  const DiscardLogEntry &entry) {
    return entry.format(os);
  }
};

template <typename T>
SyncPoint<T>::SyncPoint(T &rwl, const uint64_t sync_gen_num)
  : rwl(rwl), log_entry(std::make_shared<SyncPointLogEntry>(sync_gen_num)) {
  m_prior_log_entries_persisted = new C_Gather(rwl.m_image_ctx.cct, nullptr);
  m_sync_point_persist = new C_Gather(rwl.m_image_ctx.cct, nullptr);
  m_on_sync_point_appending.reserve(MAX_WRITES_PER_SYNC_POINT + 2);
  m_on_sync_point_persisted.reserve(MAX_WRITES_PER_SYNC_POINT + 2);
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << "sync point " << sync_gen_num << dendl;
  }
}

template <typename T>
SyncPoint<T>::~SyncPoint() {
  assert(m_on_sync_point_appending.empty());
  assert(m_on_sync_point_persisted.empty());
  assert(!earlier_sync_point);
}

template <typename T>
std::ostream &SyncPoint<T>::format(std::ostream &os) const {
  os << "log_entry=[" << *log_entry << "], "
     << "earlier_sync_point=" << earlier_sync_point << ", "
     << "later_sync_point=" << later_sync_point << ", "
     << "m_final_op_sequence_num=" << m_final_op_sequence_num << ", "
     << "m_prior_log_entries_persisted=" << m_prior_log_entries_persisted << ", "
     << "m_prior_log_entries_persisted_complete=" << m_prior_log_entries_persisted_complete << ", "
     << "m_append_scheduled=" << m_append_scheduled << ", "
     << "m_appending=" << m_appending << ", "
     << "m_on_sync_point_appending=" << m_on_sync_point_appending.size() << ", "
     << "m_on_sync_point_persisted=" << m_on_sync_point_persisted.size() << "";
  return os;
};

template <typename T>
GenericLogOperation<T>::GenericLogOperation(T &rwl, const utime_t dispatch_time)
  : rwl(rwl), m_dispatch_time(dispatch_time) {
}

template <typename T>
SyncPointLogOperation<T>::SyncPointLogOperation(T &rwl,
						std::shared_ptr<SyncPoint<T>> sync_point,
						const utime_t dispatch_time)
  : GenericLogOperation<T>(rwl, dispatch_time), sync_point(sync_point) {
}

template <typename T>
SyncPointLogOperation<T>::~SyncPointLogOperation() { }

template <typename T>
void SyncPointLogOperation<T>::appending() {
  std::vector<Context*> appending_contexts;

  assert(sync_point);
  {
    Mutex::Locker locker(rwl.m_lock);
    if (!sync_point->m_appending) {
      ldout(rwl.m_image_ctx.cct, 20) << "Sync point op=[" << *this
				     << "] appending" << dendl;
      sync_point->m_appending = true;
    }
    appending_contexts.swap(sync_point->m_on_sync_point_appending);
  }
  for (auto &ctx : appending_contexts) {
    //rwl.m_work_queue.queue(ctx);
    ctx->complete(0);
  }
}

template <typename T>
void SyncPointLogOperation<T>::complete(int result) {
  std::vector<Context*> persisted_contexts;

  assert(sync_point);
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << "Sync point op =[" << *this
				   << "] completed" << dendl;
  }
  {
    Mutex::Locker locker(rwl.m_lock);
    /* Remove link from next sync point */
    assert(sync_point->later_sync_point);
    assert(sync_point->later_sync_point->earlier_sync_point ==
	   sync_point);
    sync_point->later_sync_point->earlier_sync_point = nullptr;
  }

  /* Do append now in case completion occurred before the
   * normal append callback executed, and to handle
   * on_append work that was queued after the sync point
   * entered the appending state. */
  appending();

  {
    Mutex::Locker locker(rwl.m_lock);
    /* The flush request that scheduled this op will be one of these
     * contexts */
    persisted_contexts.swap(sync_point->m_on_sync_point_persisted);
    rwl.handle_flushed_sync_point(sync_point->log_entry);
  }
  for (auto &ctx : persisted_contexts) {
    //rwl.m_work_queue.queue(ctx, result);
    ctx->complete(result);
  }
}

template <typename T>
GeneralWriteLogOperation<T>::GeneralWriteLogOperation(T &rwl,
						      std::shared_ptr<SyncPoint<T>> sync_point,
						      const utime_t dispatch_time)
  : GenericLogOperation<T>(rwl, dispatch_time),
  m_lock("librbd::cache::rwl::GeneralWriteLogOperation::m_lock"), sync_point(sync_point) {
}

template <typename T>
GeneralWriteLogOperation<T>::~GeneralWriteLogOperation() { }

template <typename T>
DiscardLogOperation<T>::DiscardLogOperation(T &rwl,
					    std::shared_ptr<SyncPoint<T>> sync_point,
					    const uint64_t image_offset_bytes,
					    const uint64_t write_bytes,
					    const utime_t dispatch_time)
  : GeneralWriteLogOperation<T>(rwl, sync_point, dispatch_time),
    log_entry(std::make_shared<DiscardLogEntry>(sync_point->log_entry, image_offset_bytes, write_bytes)) {
  on_write_append = sync_point->m_prior_log_entries_persisted->new_sub();
  on_write_persist = nullptr;
  log_entry->sync_point_entry->m_writes++;
  log_entry->sync_point_entry->m_bytes += write_bytes;
}

template <typename T>
DiscardLogOperation<T>::~DiscardLogOperation() { }

template <typename T>
WriteLogOperation<T>::WriteLogOperation(WriteLogOperationSet<T> &set,
					uint64_t image_offset_bytes, uint64_t write_bytes)
  : GeneralWriteLogOperation<T>(set.rwl, set.sync_point, set.m_dispatch_time),
    log_entry(std::make_shared<WriteLogEntry>(set.sync_point->log_entry, image_offset_bytes, write_bytes)) {
  on_write_append = set.m_extent_ops_appending->new_sub();
  on_write_persist = set.m_extent_ops_persist->new_sub();
  log_entry->sync_point_entry->m_writes++;
  log_entry->sync_point_entry->m_bytes += write_bytes;
}

template <typename T>
WriteLogOperation<T>::~WriteLogOperation() { }

template <typename T>
std::ostream &WriteLogOperation<T>::format(std::ostream &os) const {
  os << "(Write) ";
  GeneralWriteLogOperation<T>::format(os);
  os << ", ";
  if (log_entry) {
    os << "log_entry=[" << *log_entry << "], ";
  } else {
    os << "log_entry=nullptr, ";
  }
  os << "bl=[" << bl << "],"
     << "buffer_alloc=" << buffer_alloc;
  return os;
};

template <typename T>
WriteSameLogOperation<T>::WriteSameLogOperation(WriteLogOperationSet<T> &set, uint64_t image_offset_bytes,
						uint64_t write_bytes, uint32_t data_len)
  : WriteLogOperation<T>(set, image_offset_bytes, write_bytes) {
  auto ws_entry =
    std::make_shared<WriteSameLogEntry>(set.sync_point->log_entry, image_offset_bytes, write_bytes, data_len);
  log_entry = static_pointer_cast<WriteLogEntry>(ws_entry);
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << __func__ << " " << this << dendl;
  }
}

template <typename T>
WriteSameLogOperation<T>::~WriteSameLogOperation() {
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << __func__ << " " << this << dendl;
  }
}

/* Called when the write log operation is appending and its log position is guaranteed */
template <typename T>
void GeneralWriteLogOperation<T>::appending() {
  Context *on_append = nullptr;
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << __func__ << " " << this << dendl;
  }
  {
    Mutex::Locker locker(m_lock);
    on_append = on_write_append;
    on_write_append = nullptr;
  }
  if (on_append) {
    //rwl.m_work_queue.queue(on_append);
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 20) << __func__ << " " << this << " on_append=" << on_append << dendl;
    }
    on_append->complete(0);
  }
}

/* Called when the write log operation is completed in all log replicas */
template <typename T>
void GeneralWriteLogOperation<T>::complete(int result) {
  appending();
  Context *on_persist = nullptr;
  if (RWL_VERBOSE_LOGGING) {
    ldout(rwl.m_image_ctx.cct, 20) << __func__ << " " << this << dendl;
  }
  {
    Mutex::Locker locker(m_lock);
    on_persist = on_write_persist;
    on_write_persist = nullptr;
  }
  if (on_persist) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 20) << __func__ << " " << this << " on_persist=" << on_persist << dendl;
    }
    on_persist->complete(result);
  }
}

template <typename T>
std::ostream &DiscardLogOperation<T>::format(std::ostream &os) const {
  os << "(Discard) ";
  GeneralWriteLogOperation<T>::format(os);
  os << ", ";
  if (log_entry) {
    os << "log_entry=[" << *log_entry << "], ";
  } else {
    os << "log_entry=nullptr, ";
  }
  return os;
};

template <typename T>
WriteLogOperationSet<T>::WriteLogOperationSet(T &rwl, utime_t dispatched, std::shared_ptr<SyncPoint<T>> sync_point,
					      bool persist_on_flush, BlockExtent extent, Context *on_finish)
  : rwl(rwl), m_extent(extent), m_on_finish(on_finish),
    m_persist_on_flush(persist_on_flush), m_dispatch_time(dispatched), sync_point(sync_point) {
  m_on_ops_appending = sync_point->m_prior_log_entries_persisted->new_sub();
  m_on_ops_persist = nullptr;
  m_extent_ops_persist =
    new C_Gather(rwl.m_image_ctx.cct,
		 new FunctionContext( [this](int r) {
		     if (RWL_VERBOSE_LOGGING) {
		       ldout(this->rwl.m_image_ctx.cct,20) << __func__ << " " << this << " m_extent_ops_persist completed" << dendl;
		     }
		     if (m_on_ops_persist) {
		       m_on_ops_persist->complete(r);
		     }
		     m_on_finish->complete(r);
		   }));
  auto appending_persist_sub = m_extent_ops_persist->new_sub();
  m_extent_ops_appending =
    new C_Gather(rwl.m_image_ctx.cct,
		 new FunctionContext( [this, appending_persist_sub](int r) {
		     if (RWL_VERBOSE_LOGGING) {
		       ldout(this->rwl.m_image_ctx.cct, 20) << __func__ << " " << this << " m_extent_ops_appending completed" << dendl;
		     }
		     m_on_ops_appending->complete(r);
		     appending_persist_sub->complete(r);
		   }));
}

template <typename T>
WriteLogOperationSet<T>::~WriteLogOperationSet() { }

GuardedRequestFunctionContext::GuardedRequestFunctionContext(boost::function<void(GuardedRequestFunctionContext&)> &&callback)
  : m_callback(std::move(callback)){ }

GuardedRequestFunctionContext::~GuardedRequestFunctionContext(void) { }

void GuardedRequestFunctionContext::finish(int r) {
  assert(m_cell);
  m_callback(*this);
}

/**
 * A request that can be deferred in a BlockGuard to sequence
 * overlapping operations.
 */
template <typename T>
struct C_GuardedBlockIORequest : public SharedPtrContext {
private:
  std::atomic<bool> m_cell_released = {false};
  BlockGuardCell* m_cell = nullptr;
public:
  T &rwl;
  C_GuardedBlockIORequest(T &rwl)
    : rwl(rwl) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }
  ~C_GuardedBlockIORequest() {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
    assert(m_cell_released || !m_cell);
  }
  C_GuardedBlockIORequest(const C_GuardedBlockIORequest&) = delete;
  C_GuardedBlockIORequest &operator=(const C_GuardedBlockIORequest&) = delete;
  auto shared_from_this() {
    return shared_from(this);
  }

  virtual const char *get_name() const = 0;
  void set_cell(BlockGuardCell *cell) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 20) << this << " cell=" << cell << dendl;
    }
    assert(cell);
    assert(!m_cell);
    m_cell = cell;
  }
  BlockGuardCell *get_cell(void) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 20) << this << " cell=" << m_cell << dendl;
    }
    return m_cell;
  }

  void release_cell() {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 20) << this << " cell=" << m_cell << dendl;
    }
    assert(m_cell);
    bool initial = false;
    if (m_cell_released.compare_exchange_strong(initial, true)) {
      rwl.release_guarded_request(m_cell);
    } else {
      ldout(rwl.m_image_ctx.cct, 5) << "cell " << m_cell << " already released for " << this << dendl;
    }
  }
};

} // namespace rwl

class ReplicatedWriteLogInternal {
public:
  PMEMobjpool *m_log_pool = nullptr;
};

template <typename I>
ImageCacheStateRWL<I>::ImageCacheStateRWL(I *image_ctx): ImageCacheState<I>(image_ctx) {
  ldout(image_ctx->cct, 20) << "Initialize RWL cache state with config data. " << dendl;
  ConfigProxy &config = image_ctx->config;
  m_host = ceph_get_short_hostname();
  m_path = config.get_val<std::string>("rbd_rwl_path");
  m_size = config.get_val<uint64_t>("rbd_rwl_size");
  m_invalidate_on_flush = config.get_val<bool>("rbd_rwl_invalidate_on_flush");
  m_log_stats_on_close = config.get_val<bool>("rbd_rwl_log_stats_on_close");
  m_log_periodic_stats = config.get_val<bool>("rbd_rwl_log_periodic_stats");
  m_remove_on_close = config.get_val<bool>("rbd_rwl_remove_on_close");
}

template <typename I>
ImageCacheStateRWL<I>::ImageCacheStateRWL(I *image_ctx, JSONFormattable &f): ImageCacheState<I>(image_ctx, f) {
  ldout(image_ctx->cct, 20) << "Initialize RWL cache state with data from server side" << dendl;

  m_host = (string)f["rwl_host"];
  m_path = (string)f["rwl_path"];
  uint64_t rwl_size;
  std::istringstream iss(f["rwl_size"]);
  iss >> rwl_size;
  m_size = rwl_size;
  m_invalidate_on_flush = (bool)f["rwl_invalidate_on_flush"];

  // Others from config
  ConfigProxy &config = image_ctx->config;
  m_log_stats_on_close = config.get_val<bool>("rbd_rwl_log_stats_on_close");
  m_log_periodic_stats = config.get_val<bool>("rbd_rwl_log_periodic_stats");
  m_remove_on_close = config.get_val<bool>("rbd_rwl_remove_on_close");
}

template <typename I>
bool ImageCacheStateRWL<I>::is_valid() {
  if (this->m_present &&
      (m_host.compare(ceph_get_short_hostname()) != 0)) {
    auto cleanstring = "dirty";
    if (this->m_clean) {
      cleanstring = "clean";
    }

    if (!m_image_ctx->ignore_image_cache_init_failure) {
      lderr(m_image_ctx->cct) << "An image cache (RWL) remains on host " << m_host
                 << " which is " << cleanstring
                 << ". Flush/close the image there to remove the image cache" << dendl;
      return false;
    }
  }
  return true;
}

template <typename I>
std::string ImageCacheStateRWL<I>::get_json_list() {
  std::string json_str = ImageCacheState<I>::get_json_list();

  std::string invalidate_on_flush_str;
  if (m_invalidate_on_flush) {
    invalidate_on_flush_str = "1";
  } else {
    invalidate_on_flush_str = "0";
  }

  json_str += "\"rwl_host\": \"" + m_host + "\"," +
              "\"rwl_path\": \"" + m_path + "\"," +
              "\"rwl_size\": \"" + std::to_string(m_size) + "\"," +
              "\"rwl_invalidate_on_flush\": \"" + invalidate_on_flush_str + "\"";
  return json_str;
}

template <typename I>
ImageCacheType ImageCacheStateRWL<I>::get_image_cache_type() {
  return IMAGE_CACHE_TYPE_RWL;
}

template <typename I>
ReplicatedWriteLog<I>::ReplicatedWriteLog(I &image_ctx, ImageCacheState<I>* cache_state)
  : ImageCache<I>(cache_state),
    m_internal(new ReplicatedWriteLogInternal),
    rwl_pool_layout_name(POBJ_LAYOUT_NAME(rbd_rwl)),
    m_image_ctx(image_ctx),
    m_log_pool_config_size(DEFAULT_POOL_SIZE),
    m_image_writeback(image_ctx), m_write_log_guard(image_ctx.cct),
    m_timer_lock("librbd::cache::ReplicatedWriteLog::m_timer_lock",
	   false, true, true),
    m_log_retire_lock("librbd::cache::ReplicatedWriteLog::m_log_retire_lock",
		      false, true, true),
    m_entry_reader_lock("librbd::cache::ReplicatedWriteLog::m_entry_reader_lock"),
    m_deferred_dispatch_lock("librbd::cache::ReplicatedWriteLog::m_deferred_dispatch_lock",
			     false, true, true),
    m_log_append_lock("librbd::cache::ReplicatedWriteLog::m_log_append_lock",
		      false, true, true),
    m_lock("librbd::cache::ReplicatedWriteLog::m_lock",
	   false, true, true),
    m_blockguard_lock("librbd::cache::ReplicatedWriteLog::m_blockguard_lock",
	   false, true, true),
    m_entry_bl_lock("librbd::cache::ReplicatedWriteLog::m_entry_bl_lock",
	   false, true, true),
    m_persist_finisher(image_ctx.cct, "librbd::cache::ReplicatedWriteLog::m_persist_finisher", "pfin_rwl"),
    m_log_append_finisher(image_ctx.cct, "librbd::cache::ReplicatedWriteLog::m_log_append_finisher", "afin_rwl"),
    m_on_persist_finisher(image_ctx.cct, "librbd::cache::ReplicatedWriteLog::m_on_persist_finisher", "opfin_rwl"),
    m_blocks_to_log_entries(image_ctx.cct),
    m_timer(image_ctx.cct, m_timer_lock, true /* m_timer_lock held in callbacks */),
    m_thread_pool(image_ctx.cct, "librbd::cache::ReplicatedWriteLog::thread_pool", "tp_rwl",
		  /*image_ctx.cct->_conf.get_val<int64_t>("rbd_op_threads")*/ 4, // TODO: Add config value
		  /*"rbd_op_threads"*/""), //TODO: match above
    m_work_queue("librbd::cache::ReplicatedWriteLog::work_queue",
		 60, //image_ctx.cct->_conf.get_val<int64_t>("rbd_op_thread_timeout"),
		 &m_thread_pool),
    m_flush_on_close(!get_env_bool("RBD_RWL_NO_FLUSH_ON_CLOSE")),
    m_retire_on_close(m_flush_on_close && !get_env_bool("RBD_RWL_NO_RETIRE_ON_CLOSE"))
{
}

template <typename I>
void ReplicatedWriteLog<I>::start_workers() {
  m_thread_pool.start();
  if (use_finishers) {
    m_persist_finisher.start();
    m_log_append_finisher.start();
    m_on_persist_finisher.start();
  }
  m_timer.init();
}

template <typename I>
ReplicatedWriteLog<I>::~ReplicatedWriteLog() {
  ldout(m_image_ctx.cct, 15) << "enter" << dendl;
  {
    Mutex::Locker timer_locker(m_timer_lock);
    m_timer.shutdown();
    ldout(m_image_ctx.cct, 15) << "(destruct) acquiring locks that shouldn't still be held" << dendl;
    Mutex::Locker retire_locker(m_log_retire_lock);
    RWLock::WLocker reader_locker(m_entry_reader_lock);
    Mutex::Locker dispatch_locker(m_deferred_dispatch_lock);
    Mutex::Locker append_locker(m_log_append_lock);
    Mutex::Locker locker(m_lock);
    Mutex::Locker bg_locker(m_blockguard_lock);
    Mutex::Locker bl_locker(m_entry_bl_lock);
    ldout(m_image_ctx.cct, 15) << "(destruct) gratuitous locking complete" << dendl;
    m_thread_pool.stop();
    assert(m_deferred_ios.size() == 0);
    assert(m_ops_to_flush.size() == 0);
    assert(m_ops_to_append.size() == 0);
    assert(m_flush_ops_in_flight == 0);
    assert(m_unpublished_reserves == 0);
    if (m_flush_on_close) {
      assert(m_bytes_dirty == 0);
    }
    if (m_retire_on_close) {
      assert(m_bytes_cached == 0);
      assert(m_bytes_allocated == 0);
    }
    delete m_internal;
    m_internal = nullptr;

    delete m_cache_state;
    m_cache_state = nullptr;
  }
  ldout(m_image_ctx.cct, 15) << "exit" << dendl;
}

template <typename I>
const typename ImageCache<I>::Extent ReplicatedWriteLog<I>::whole_volume_extent(void) {
  return typename ImageCache<I>::Extent({0, ~0});
}

template <typename ExtentsType>
class ExtentsSummary {
public:
  uint64_t total_bytes;
  uint64_t first_image_byte;
  uint64_t last_image_byte;
  friend std::ostream &operator<<(std::ostream &os,
				  const ExtentsSummary &s) {
    os << "total_bytes=" << s.total_bytes << ", "
       << "first_image_byte=" << s.first_image_byte << ", "
       << "last_image_byte=" << s.last_image_byte << "";
    return os;
  };
  ExtentsSummary(const ExtentsType &extents) {
    total_bytes = 0;
    first_image_byte = 0;
    last_image_byte = 0;
    if (extents.empty()) return;
    /* These extents refer to image offsets between first_image_byte
     * and last_image_byte, inclusive, but we don't guarantee here
     * that they address all of those bytes. There may be gaps. */
    first_image_byte = extents.front().first;
    last_image_byte = first_image_byte + extents.front().second;
    for (auto &extent : extents) {
      /* Ignore zero length extents */
      if (extent.second) {
	total_bytes += extent.second;
	if (extent.first < first_image_byte) {
	  first_image_byte = extent.first;
	}
	if ((extent.first + extent.second) > last_image_byte) {
	  last_image_byte = extent.first + extent.second;
	}
      }
    }
  }
  const BlockExtent block_extent() {
    return BlockExtent(first_image_byte, last_image_byte);
  }
  const Extent image_extent() {
    return rwl::image_extent(block_extent());
  }
};

struct ImageExtentBuf : public Extent {
public:
  bufferlist m_bl;
  ImageExtentBuf(Extent extent, buffer::raw *buf = nullptr)
    : Extent(extent) {
    if (buf) {
      m_bl.append(buf);
    }
  }
  ImageExtentBuf(Extent extent, bufferlist bl)
    : Extent(extent), m_bl(bl) { }
};
typedef std::vector<ImageExtentBuf> ImageExtentBufs;

struct C_ReadRequest : public Context {
  CephContext *m_cct;
  Context *m_on_finish;
  Extents m_miss_extents; // move back to caller
  ImageExtentBufs m_read_extents;
  bufferlist m_miss_bl;
  bufferlist *m_out_bl;
  utime_t m_arrived_time;
  PerfCounters *m_perfcounter;

  C_ReadRequest(CephContext *cct, utime_t arrived, PerfCounters *perfcounter, bufferlist *out_bl, Context *on_finish)
    : m_cct(cct), m_on_finish(on_finish), m_out_bl(out_bl),
      m_arrived_time(arrived), m_perfcounter(perfcounter) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(m_cct, 99) << this << dendl;
    }
  }
  ~C_ReadRequest() {
    if (RWL_VERBOSE_LOGGING) {
      ldout(m_cct, 99) << this << dendl;
    }
  }

  virtual void finish(int r) override {
    if (RWL_VERBOSE_LOGGING) {
      ldout(m_cct, 20) << "(" << get_name() << "): r=" << r << dendl;
    }
    int hits = 0;
    int misses = 0;
    int hit_bytes = 0;
    int miss_bytes = 0;
    if (r >= 0) {
      /*
       * At this point the miss read has completed. We'll iterate through
       * m_read_extents and produce *m_out_bl by assembling pieces of m_miss_bl
       * and the individual hit extent bufs in the read extents that represent
       * hits.
       */
      uint64_t miss_bl_offset = 0;
      for (auto &extent : m_read_extents) {
	if (extent.m_bl.length()) {
	  /* This was a hit */
	  assert(extent.second == extent.m_bl.length());
	  ++hits;
	  hit_bytes += extent.second;
	  m_out_bl->claim_append(extent.m_bl);
	} else {
	  /* This was a miss. */
	  ++misses;
	  miss_bytes += extent.second;
	  bufferlist miss_extent_bl;
	  miss_extent_bl.substr_of(m_miss_bl, miss_bl_offset, extent.second);
	  /* Add this read miss bufferlist to the output bufferlist */
	  m_out_bl->claim_append(miss_extent_bl);
	  /* Consume these bytes in the read miss bufferlist */
	  miss_bl_offset += extent.second;
	}
      }
    }
    if (RWL_VERBOSE_LOGGING) {
      ldout(m_cct, 20) << "(" << get_name() << "): r=" << r << " bl=" << *m_out_bl << dendl;
    }
    utime_t now = ceph_clock_now();
    assert((int)m_out_bl->length() == hit_bytes + miss_bytes);
    m_on_finish->complete(r);
    m_perfcounter->inc(l_librbd_rwl_rd_bytes, hit_bytes + miss_bytes);
    m_perfcounter->inc(l_librbd_rwl_rd_hit_bytes, hit_bytes);
    m_perfcounter->tinc(l_librbd_rwl_rd_latency, now - m_arrived_time);
    if (!misses) {
      m_perfcounter->inc(l_librbd_rwl_rd_hit_req, 1);
      m_perfcounter->tinc(l_librbd_rwl_rd_hit_latency, now - m_arrived_time);
    } else {
      if (hits) {
	m_perfcounter->inc(l_librbd_rwl_rd_part_hit_req, 1);
      }
    }
  }

  virtual const char *get_name() const {
    return "C_ReadRequest";
  }
};

static const bool COPY_PMEM_FOR_READ = true;
template <typename I>
void ReplicatedWriteLog<I>::aio_read(Extents &&image_extents, bufferlist *bl,
				     int fadvise_flags, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  utime_t now = ceph_clock_now();
  if (ExtentsSummary<Extents>(image_extents).total_bytes == 0) {
    on_finish->complete(0);
    return;
  }
  C_ReadRequest *read_ctx = new C_ReadRequest(cct, now, m_perfcounter, bl, on_finish);
  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 20) << "name: " << m_image_ctx.name << " id: " << m_image_ctx.id
		   << "image_extents=" << image_extents << ", "
		   << "bl=" << bl << ", "
		   << "on_finish=" << on_finish << dendl;
  }

  assert(m_initialized);
  bl->clear();
  m_perfcounter->inc(l_librbd_rwl_rd_req, 1);

  // TODO handle fadvise flags

  /*
   * The strategy here is to look up all the WriteLogMapEntries that overlap
   * this read, and iterate through those to separate this read into hits and
   * misses. A new Extents object is produced here with Extents for each miss
   * region. The miss Extents is then passed on to the read cache below RWL. We
   * also produce an ImageExtentBufs for all the extents (hit or miss) in this
   * read. When the read from the lower cache layer completes, we iterate
   * through the ImageExtentBufs and insert buffers for each cache hit at the
   * appropriate spot in the bufferlist returned from below for the miss
   * read. The buffers we insert here refer directly to regions of various
   * write log entry data buffers.
   *
   * Locking: These buffer objects hold a reference on the write log entries
   * they refer to. Log entries can't be retired until there are no references.
   * The GeneralWriteLogEntry references are released by the buffer destructor.
   */
  for (auto &extent : image_extents) {
    uint64_t extent_offset = 0;
    RWLock::RLocker entry_reader_locker(m_entry_reader_lock);
    WriteLogMapEntries map_entries = m_blocks_to_log_entries.find_map_entries(block_extent(extent));
    for (auto &map_entry : map_entries) {
      Extent entry_image_extent(image_extent(map_entry.block_extent));
      /* If this map entry starts after the current image extent offset ... */
      if (entry_image_extent.first > extent.first + extent_offset) {
	/* ... add range before map_entry to miss extents */
	uint64_t miss_extent_start = extent.first + extent_offset;
	uint64_t miss_extent_length = entry_image_extent.first - miss_extent_start;
	Extent miss_extent(miss_extent_start, miss_extent_length);
	read_ctx->m_miss_extents.push_back(miss_extent);
	/* Add miss range to read extents */
	ImageExtentBuf miss_extent_buf(miss_extent);
	read_ctx->m_read_extents.push_back(miss_extent_buf);
	extent_offset += miss_extent_length;
      }
      assert(entry_image_extent.first <= extent.first + extent_offset);
      uint64_t entry_offset = 0;
      /* If this map entry starts before the current image extent offset ... */
      if (entry_image_extent.first < extent.first + extent_offset) {
	/* ... compute offset into log entry for this read extent */
	entry_offset = (extent.first + extent_offset) - entry_image_extent.first;
      }
      /* This read hit ends at the end of the extent or the end of the log
	 entry, whichever is less. */
      uint64_t entry_hit_length = min(entry_image_extent.second - entry_offset,
				      extent.second - extent_offset);
      Extent hit_extent(entry_image_extent.first, entry_hit_length);
      assert(map_entry.log_entry->is_writer());
      if (map_entry.log_entry->is_write() || map_entry.log_entry->is_writesame()) {
	/* Offset of the map entry into the log entry's buffer */
	uint64_t map_entry_buffer_offset = entry_image_extent.first - map_entry.log_entry->ram_entry.image_offset_bytes;
	/* Offset into the log entry buffer of this read hit */
	uint64_t read_buffer_offset = map_entry_buffer_offset + entry_offset;
	/* Create buffer object referring to pmem pool for this read hit */
	auto write_entry = static_pointer_cast<WriteLogEntry>(map_entry.log_entry);

	/* Make a bl for this hit extent. This will add references to the write_entry->pmem_bp */
	buffer::list hit_bl;
	if (COPY_PMEM_FOR_READ) {
	  buffer::list entry_bl_copy;
	  write_entry->copy_pmem_bl(m_entry_bl_lock, &entry_bl_copy);
	  entry_bl_copy.copy(read_buffer_offset, entry_hit_length, hit_bl);
	} else {
	  hit_bl.substr_of(write_entry->get_pmem_bl(m_entry_bl_lock), read_buffer_offset, entry_hit_length);
	}
	assert(hit_bl.length() == entry_hit_length);

	/* Add hit extent to read extents */
	ImageExtentBuf hit_extent_buf(hit_extent, hit_bl);
	read_ctx->m_read_extents.push_back(hit_extent_buf);
      } else if (map_entry.log_entry->is_discard()) {
	auto discard_entry = static_pointer_cast<DiscardLogEntry>(map_entry.log_entry);
	if (RWL_VERBOSE_LOGGING) {
	  ldout(cct, 20) << "read hit on discard entry: log_entry=" << *discard_entry << dendl;
	}
	/* Discards read as zero, so we'll construct a bufferlist of zeros */
	bufferlist zero_bl;
	zero_bl.append_zero(entry_hit_length);
	/* Add hit extent to read extents */
	ImageExtentBuf hit_extent_buf(hit_extent, zero_bl);
	read_ctx->m_read_extents.push_back(hit_extent_buf);
      } else {
	ldout(cct, 02) << "Reading from log entry=" << *map_entry.log_entry
		       << " unimplemented" << dendl;
	assert(false);
      }

      /* Exclude RWL hit range from buffer and extent */
      extent_offset += entry_hit_length;
      if (RWL_VERBOSE_LOGGING) {
	ldout(cct, 20) << map_entry << dendl;
      }
    }
    /* If the last map entry didn't consume the entire image extent ... */
    if (extent.second > extent_offset) {
      /* ... add the rest of this extent to miss extents */
      uint64_t miss_extent_start = extent.first + extent_offset;
      uint64_t miss_extent_length = extent.second - extent_offset;
      Extent miss_extent(miss_extent_start, miss_extent_length);
      read_ctx->m_miss_extents.push_back(miss_extent);
      /* Add miss range to read extents */
      ImageExtentBuf miss_extent_buf(miss_extent);
      read_ctx->m_read_extents.push_back(miss_extent_buf);
      extent_offset += miss_extent_length;
    }
  }

  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 20) << "miss_extents=" << read_ctx->m_miss_extents << ", "
		   << "miss_bl=" << read_ctx->m_miss_bl << dendl;
  }

  if (read_ctx->m_miss_extents.empty()) {
    /* All of this read comes from RWL */
    read_ctx->complete(0);
  } else {
    /* Pass the read misses on to the layer below RWL */
    m_image_writeback.aio_read(std::move(read_ctx->m_miss_extents), &read_ctx->m_miss_bl, fadvise_flags, read_ctx);
  }
}

template <typename I>
BlockGuardCell* ReplicatedWriteLog<I>::detain_guarded_request_helper(GuardedRequest &req)
{
  CephContext *cct = m_image_ctx.cct;
  BlockGuardCell *cell;

  assert(m_blockguard_lock.is_locked_by_me());
  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 20) << dendl;
  }

  int r = m_write_log_guard.detain(req.block_extent, &req, &cell);
  assert(r>=0);
  if (r > 0) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(cct, 20) << "detaining guarded request due to in-flight requests: "
		     << "req=" << req << dendl;
    }
    return nullptr;
  }

  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 20) << "in-flight request cell: " << cell << dendl;
  }
  return cell;
}

template <typename I>
BlockGuardCell* ReplicatedWriteLog<I>::detain_guarded_request_barrier_helper(GuardedRequest &req)
{
  BlockGuardCell *cell = nullptr;

  assert(m_blockguard_lock.is_locked_by_me());
  if (RWL_VERBOSE_LOGGING) {
    ldout(m_image_ctx.cct, 20) << dendl;
  }

  if (m_barrier_in_progress) {
    req.guard_ctx->m_state.queued = true;
    m_awaiting_barrier.push_back(req);
  } else {
    bool barrier = req.guard_ctx->m_state.barrier;
    if (barrier) {
      m_barrier_in_progress = true;
      req.guard_ctx->m_state.current_barrier = true;
    }
    cell = detain_guarded_request_helper(req);
    if (barrier) {
      /* Only non-null if the barrier acquires the guard now */
      m_barrier_cell = cell;
    }
  }

  return cell;
}

template <typename I>
void ReplicatedWriteLog<I>::detain_guarded_request(GuardedRequest &&req)
{
  BlockGuardCell *cell = nullptr;

  if (RWL_VERBOSE_LOGGING) {
    ldout(m_image_ctx.cct, 20) << dendl;
  }
  {
    Mutex::Locker locker(m_blockguard_lock);
    cell = detain_guarded_request_barrier_helper(req);
  }
  if (cell) {
    req.guard_ctx->m_cell = cell;
    req.guard_ctx->complete(0);
  }
}

template <typename I>
void ReplicatedWriteLog<I>::release_guarded_request(BlockGuardCell *released_cell)
{
  CephContext *cct = m_image_ctx.cct;
  WriteLogGuard::BlockOperations block_reqs;
  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 20) << "released_cell=" << released_cell << dendl;
  }

  {
    Mutex::Locker locker(m_blockguard_lock);
    m_write_log_guard.release(released_cell, &block_reqs);

    for (auto &req : block_reqs) {
      req.guard_ctx->m_state.detained = true;
      BlockGuardCell *detained_cell = detain_guarded_request_helper(req);
      if (detained_cell) {
	if (req.guard_ctx->m_state.current_barrier) {
	  /* The current barrier is acquiring the block guard, so now we know its cell */
	  m_barrier_cell = detained_cell;
	  /* detained_cell could be == released_cell here */
	  if (RWL_VERBOSE_LOGGING) {
	    ldout(cct, 20) << "current barrier cell=" << detained_cell << " req=" << req << dendl;
	  }
	}
	req.guard_ctx->m_cell = detained_cell;
	m_work_queue.queue(req.guard_ctx);
      }
    }

    if (m_barrier_in_progress && (released_cell == m_barrier_cell)) {
      if (RWL_VERBOSE_LOGGING) {
	ldout(cct, 20) << "current barrier released cell=" << released_cell << dendl;
      }
      /* The released cell is the current barrier request */
      m_barrier_in_progress = false;
      m_barrier_cell = nullptr;
      /* Move waiting requests into the blockguard. Stop if there's another barrier */
      while (!m_barrier_in_progress && !m_awaiting_barrier.empty()) {
	auto &req = m_awaiting_barrier.front();
	if (RWL_VERBOSE_LOGGING) {
	  ldout(cct, 20) << "submitting queued request to blockguard: " << req << dendl;
	}
	BlockGuardCell *detained_cell = detain_guarded_request_barrier_helper(req);
	if (detained_cell) {
	  req.guard_ctx->m_cell = detained_cell;
	  m_work_queue.queue(req.guard_ctx);
	}
	m_awaiting_barrier.pop_front();
      }
    }
  }

  if (RWL_VERBOSE_LOGGING) {
    ldout(cct, 20) << "exit" << dendl;
  }
}

struct WriteBufferAllocation {
  unsigned int allocation_size = 0;
  pobj_action buffer_alloc_action;
  TOID(uint8_t) buffer_oid = OID_NULL;
  bool allocated = false;
  utime_t allocation_lat;
};

struct WriteRequestResources {
  bool allocated = false;
  std::vector<WriteBufferAllocation> buffers;
};

/**
 * This is the custodian of the BlockGuard cell for this IO, and the
 * state information about the progress of this IO. This object lives
 * until the IO is persisted in all (live) log replicas.  User request
 * may be completed from here before the IO persists.
 */
template <typename T>
struct C_BlockIORequest : public C_GuardedBlockIORequest<T> {
  using C_GuardedBlockIORequest<T>::rwl;
  Extents m_image_extents;
  bufferlist bl;
  int fadvise_flags;
  Context *user_req; /* User write request */
  std::atomic<bool> m_user_req_completed = {false};
  std::atomic<bool> m_finish_called = {false};
  ExtentsSummary<Extents> m_image_extents_summary;
  utime_t m_arrived_time;
  utime_t m_allocated_time;               /* When allocation began */
  utime_t m_dispatched_time;              /* When dispatch began */
  utime_t m_user_req_completed_time;
  bool m_detained = false;                /* Detained in blockguard (overlapped with a prior IO) */
  std::atomic<bool> m_deferred = {false}; /* Deferred because this or a prior IO had to wait for write resources */
  bool m_waited_lanes = false;            /* This IO waited for free persist/replicate lanes */
  bool m_waited_entries = false;          /* This IO waited for free log entries */
  bool m_waited_buffers = false;          /* This IO waited for data buffers (pmemobj_reserve() failed) */
  friend std::ostream &operator<<(std::ostream &os,
				  const C_BlockIORequest<T> &req) {
    os << "m_image_extents=[" << req.m_image_extents << "], "
       << "m_image_extents_summary=[" << req.m_image_extents_summary << "], "
       << "bl=" << req.bl << ", "
       << "user_req=" << req.user_req << ", "
       << "m_user_req_completed=" << req.m_user_req_completed << ", "
       << "deferred=" << req.m_deferred << ", "
       << "detained=" << req.m_detained << ", "
       << "m_waited_lanes=" << req.m_waited_lanes << ", "
       << "m_waited_entries=" << req.m_waited_entries << ", "
       << "m_waited_buffers=" << req.m_waited_buffers << "";
    return os;
  };
  C_BlockIORequest(T &rwl, const utime_t arrived, Extents &&image_extents,
		   bufferlist&& bl, const int fadvise_flags, Context *user_req)
    : C_GuardedBlockIORequest<T>(rwl), m_image_extents(std::move(image_extents)),
      bl(std::move(bl)), fadvise_flags(fadvise_flags),
      user_req(user_req), m_image_extents_summary(m_image_extents), m_arrived_time(arrived) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
    /* Remove zero length image extents from input */
    for (auto it = m_image_extents.begin(); it != m_image_extents.end(); ) {
      if (0 == it->second) {
	it = m_image_extents.erase(it);
	continue;
      }
      ++it;
    }
  }

  virtual ~C_BlockIORequest() {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }

  auto shared_from_this() {
    return shared_from(this);
  }

  void complete_user_request(int r) {
    bool initial = false;
    if (m_user_req_completed.compare_exchange_strong(initial, true)) {
      if (RWL_VERBOSE_LOGGING) {
	ldout(rwl.m_image_ctx.cct, 15) << this << " completing user req" << dendl;
      }
      m_user_req_completed_time = ceph_clock_now();
      user_req->complete(r);
    } else {
      if (RWL_VERBOSE_LOGGING) {
	ldout(rwl.m_image_ctx.cct, 20) << this << " user req already completed" << dendl;
      }
    }
  }

  void finish(int r) {
    ldout(rwl.m_image_ctx.cct, 20) << this << dendl;

    complete_user_request(r);
    bool initial = false;
    if (m_finish_called.compare_exchange_strong(initial, true)) {
      if (RWL_VERBOSE_LOGGING) {
	ldout(rwl.m_image_ctx.cct, 15) << this << " finishing" << dendl;
      }
      finish_req(0);
    } else {
      ldout(rwl.m_image_ctx.cct, 20) << this << " already finished" << dendl;
      assert(0);
    }
  }

  virtual void finish_req(int r) = 0;

  virtual bool alloc_resources() = 0;

  void deferred() {
    bool initial = false;
    if (m_deferred.compare_exchange_strong(initial, true)) {
      deferred_handler();
    }
  }

  virtual void deferred_handler() = 0;

  virtual void dispatch()  = 0;

  virtual const char *get_name() const override {
    return "C_BlockIORequest";
  }
};

/**
 * This is the custodian of the BlockGuard cell for this write. Block
 * guard is not released until the write persists everywhere (this is
 * how we guarantee to each log replica that they will never see
 * overlapping writes).
 */
template <typename T>
struct C_WriteRequest : public C_BlockIORequest<T> {
  using C_BlockIORequest<T>::rwl;
  WriteRequestResources m_resources;
  unique_ptr<WriteLogOperationSet<T>> m_op_set = nullptr;
  bool m_do_early_flush = false;
  std::atomic<int> m_appended = {0};
  bool m_queued = false;
  friend std::ostream &operator<<(std::ostream &os,
				  const C_WriteRequest<T> &req) {
    os << (C_BlockIORequest<T>&)req
       << " m_resources.allocated=" << req.m_resources.allocated;
    if (req.m_op_set) {
       os << "m_op_set=" << *req.m_op_set;
    }
    return os;
  };

  C_WriteRequest(T &rwl, const utime_t arrived, Extents &&image_extents,
		 bufferlist&& bl, const int fadvise_flags, Context *user_req)
    : C_BlockIORequest<T>(rwl, arrived, std::move(image_extents), std::move(bl), fadvise_flags, user_req) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }

  ~C_WriteRequest() {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }

  template <typename... U>
  static inline std::shared_ptr<C_WriteRequest<T>> create(U&&... arg)
  {
    return SharedPtrContext::create<C_WriteRequest<T>>(std::forward<U>(arg)...);
  }

  auto shared_from_this() {
    return shared_from(this);
  }

  void blockguard_acquired(GuardedRequestFunctionContext &guard_ctx) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 20) << __func__ << " write_req=" << this << " cell=" << guard_ctx.m_cell << dendl;
    }

    assert(guard_ctx.m_cell);
    this->m_detained = guard_ctx.m_state.detained; /* overlapped */
    this->m_queued = guard_ctx.m_state.queued; /* queued behind at least one barrier */
    this->set_cell(guard_ctx.m_cell);
  }

  /* Common finish to plain write and compare-and-write (if it writes) */
  virtual void finish_req(int r) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 15) << "write_req=" << this << " cell=" << this->get_cell() << dendl;
    }

    /* Completed to caller by here (in finish(), which calls this) */
    utime_t now = ceph_clock_now();
    rwl.release_write_lanes(this);
    this->release_cell(); /* TODO: Consider doing this in appending state */
    update_req_stats(now);
  }

  /* Compare and write will override this */
  virtual void update_req_stats(utime_t &now) {
    for (auto &allocation : this->m_resources.buffers) {
      rwl.m_perfcounter->tinc(l_librbd_rwl_log_op_alloc_t, allocation.allocation_lat);
      rwl.m_perfcounter->hinc(l_librbd_rwl_log_op_alloc_t_hist,
			      allocation.allocation_lat.to_nsec(), allocation.allocation_size);
    }
    if (this->m_detained) {
      rwl.m_perfcounter->inc(l_librbd_rwl_wr_req_overlap, 1);
    }
    if (this->m_queued) {
      rwl.m_perfcounter->inc(l_librbd_rwl_wr_req_queued, 1);
    }
    if (this->m_deferred) {
      rwl.m_perfcounter->inc(l_librbd_rwl_wr_req_def, 1);
    }
    if (this->m_waited_lanes) {
      rwl.m_perfcounter->inc(l_librbd_rwl_wr_req_def_lanes, 1);
    }
    if (this->m_waited_entries) {
      rwl.m_perfcounter->inc(l_librbd_rwl_wr_req_def_log, 1);
    }
    if (this->m_waited_buffers) {
      rwl.m_perfcounter->inc(l_librbd_rwl_wr_req_def_buf, 1);
    }
    rwl.m_perfcounter->tinc(l_librbd_rwl_req_arr_to_all_t, this->m_allocated_time - this->m_arrived_time);
    rwl.m_perfcounter->tinc(l_librbd_rwl_req_all_to_dis_t, this->m_dispatched_time - this->m_allocated_time);
    rwl.m_perfcounter->tinc(l_librbd_rwl_req_arr_to_dis_t, this->m_dispatched_time - this->m_arrived_time);
    utime_t comp_latency = now - this->m_arrived_time;
    if (!(this->m_waited_entries || this->m_waited_buffers || this->m_deferred)) {
      rwl.m_perfcounter->tinc(l_librbd_rwl_nowait_req_arr_to_all_t, this->m_allocated_time - this->m_arrived_time);
      rwl.m_perfcounter->tinc(l_librbd_rwl_nowait_req_all_to_dis_t, this->m_dispatched_time - this->m_allocated_time);
      rwl.m_perfcounter->tinc(l_librbd_rwl_nowait_req_arr_to_dis_t, this->m_dispatched_time - this->m_arrived_time);
      rwl.m_perfcounter->tinc(l_librbd_rwl_nowait_wr_latency, comp_latency);
      rwl.m_perfcounter->hinc(l_librbd_rwl_nowait_wr_latency_hist, comp_latency.to_nsec(),
			      this->m_image_extents_summary.total_bytes);
      rwl.m_perfcounter->tinc(l_librbd_rwl_nowait_wr_caller_latency,
			      this->m_user_req_completed_time - this->m_arrived_time);
    }
    rwl.m_perfcounter->tinc(l_librbd_rwl_wr_latency, comp_latency);
    rwl.m_perfcounter->hinc(l_librbd_rwl_wr_latency_hist, comp_latency.to_nsec(),
			    this->m_image_extents_summary.total_bytes);
    rwl.m_perfcounter->tinc(l_librbd_rwl_wr_caller_latency, this->m_user_req_completed_time - this->m_arrived_time);
  }

  virtual bool alloc_resources() override;

  /* Plain writes will allocate one buffer per request extent */
  virtual void setup_buffer_resources(uint64_t &bytes_cached, uint64_t &bytes_dirtied) {
    for (auto &extent : this->m_image_extents) {
      m_resources.buffers.emplace_back();
      struct WriteBufferAllocation &buffer = m_resources.buffers.back();
      buffer.allocation_size = MIN_WRITE_ALLOC_SIZE;
      buffer.allocated = false;
      bytes_cached += extent.second;
      if (extent.second > buffer.allocation_size) {
	buffer.allocation_size = extent.second;
      }
    }
    bytes_dirtied = bytes_cached;
  }

  void deferred_handler() override { }

  void dispatch() override;

  virtual void setup_log_operations() {
    for (auto &extent : this->m_image_extents) {
      /* operation->on_write_persist connected to m_prior_log_entries_persisted Gather */
      auto operation =
	std::make_shared<WriteLogOperation<T>>(*m_op_set, extent.first, extent.second);
      m_op_set->operations.emplace_back(operation);
    }
  }

  virtual void schedule_append() {
    assert(++m_appended == 1);
    if (m_do_early_flush) {
      /* This caller is waiting for persist, so we'll use their thread to
       * expedite it */
      rwl.flush_pmem_buffer(this->m_op_set->operations);
      rwl.schedule_append(this->m_op_set->operations);
    } else {
      /* This is probably not still the caller's thread, so do the payload
       * flushing/replicating later. */
      rwl.schedule_flush_and_append(this->m_op_set->operations);
    }
  }

  const char *get_name() const override {
    return "C_WriteRequest";
  }
};

/**
 * This is the custodian of the BlockGuard cell for this
 * aio_flush. Block guard is released as soon as the new
 * sync point (if required) is created. Subsequent IOs can
 * proceed while this flush waits for prio IOs to complete
 * and any required sync points to be persisted.
 */
template <typename T>
struct C_FlushRequest : public C_BlockIORequest<T> {
  using C_BlockIORequest<T>::rwl;
  std::atomic<bool> m_log_entry_allocated = {false};
  bool m_internal = false;
  std::shared_ptr<SyncPoint<T>> to_append;
  std::shared_ptr<SyncPointLogOperation<T>> op;
  friend std::ostream &operator<<(std::ostream &os,
				  const C_FlushRequest<T> &req) {
    os << (C_BlockIORequest<T>&)req
       << " m_log_entry_allocated=" << req.m_log_entry_allocated;
    return os;
  };

  C_FlushRequest(T &rwl, const utime_t arrived, Extents &&image_extents,
		 bufferlist&& bl, const int fadvise_flags, Context *user_req)
    : C_BlockIORequest<T>(rwl, arrived, std::move(image_extents), std::move(bl), fadvise_flags, user_req) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }

  ~C_FlushRequest() {
  }

  template <typename... U>
  static inline std::shared_ptr<C_FlushRequest<T>> create(U&&... arg)
  {
    return SharedPtrContext::create<C_FlushRequest<T>>(std::forward<U>(arg)...);
  }

  auto shared_from_this() {
    return shared_from(this);
  }

  void finish_req(int r) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 20) << "flush_req=" << this
				     << " cell=" << this->get_cell() << dendl;
    }
    /* Block guard already released */
    assert(!this->get_cell());

    /* Completed to caller by here */
    utime_t now = ceph_clock_now();
    rwl.m_perfcounter->tinc(l_librbd_rwl_aio_flush_latency, now - this->m_arrived_time);
  }

  bool alloc_resources() override;

  void deferred_handler() override {
    rwl.m_perfcounter->inc(l_librbd_rwl_aio_flush_def, 1);
  }

  void dispatch() override;

  const char *get_name() const override {
    return "C_FlushRequest";
  }
};

/**
 * This is the custodian of the BlockGuard cell for this discard. As in the
 * case of write, the block guard is not released until the discard persists
 * everywhere.
 */
template <typename T>
struct C_DiscardRequest : public C_BlockIORequest<T> {
  using C_BlockIORequest<T>::rwl;
  std::atomic<bool> m_log_entry_allocated = {false};
  uint32_t m_discard_granularity_bytes;
  std::shared_ptr<DiscardLogOperation<T>> op;
  friend std::ostream &operator<<(std::ostream &os,
				  const C_DiscardRequest<T> &req) {
    os << (C_BlockIORequest<T>&)req
       << "m_discard_granularity_bytes=" << req.m_discard_granularity_bytes;
    if (req.op) {
      os << "op=[" << *req.op << "]";
    } else {
      os << "op=nullptr";
    }
    return os;
  };

  C_DiscardRequest(T &rwl, const utime_t arrived, Extents &&image_extents,
		   const uint32_t discard_granularity_bytes, Context *user_req)
    : C_BlockIORequest<T>(rwl, arrived, std::move(image_extents), bufferlist(), 0, user_req),
    m_discard_granularity_bytes(discard_granularity_bytes) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }

  ~C_DiscardRequest() {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }

  template <typename... U>
  static inline std::shared_ptr<C_DiscardRequest<T>> create(U&&... arg)
  {
    return SharedPtrContext::create<C_DiscardRequest<T>>(std::forward<U>(arg)...);
  }

  auto shared_from_this() {
    return shared_from(this);
  }

  void finish_req(int r) {}

  bool alloc_resources() override;

  void deferred_handler() override { }

  void dispatch() override;

  const char *get_name() const override {
    return "C_DiscardRequest";
  }
};

/**
 * This is the custodian of the BlockGuard cell for this compare and write. The
 * block guard is acquired before the read begins to guarantee atomicity of this
 * operation.  If this results in a write, the block guard will be released
 * when the write completes to all replicas.
 */
template <typename T>
struct C_CompAndWriteRequest : public C_WriteRequest<T> {
  using C_BlockIORequest<T>::rwl;
  bool m_compare_succeeded = false;
  uint64_t *m_mismatch_offset;
  bufferlist m_cmp_bl;
  bufferlist m_read_bl;
  friend std::ostream &operator<<(std::ostream &os,
				  const C_CompAndWriteRequest<T> &req) {
    os << (C_WriteRequest<T>&)req
       << "m_cmp_bl=" << req.m_cmp_bl << ", "
       << "m_read_bl=" << req.m_read_bl << ", "
       << "m_compare_succeeded=" << req.m_compare_succeeded << ", "
       << "m_mismatch_offset=" << req.m_mismatch_offset;
    return os;
  };

  C_CompAndWriteRequest(T &rwl, const utime_t arrived, Extents &&image_extents,
			bufferlist&& cmp_bl, bufferlist&& bl, uint64_t *mismatch_offset,
			int fadvise_flags, Context *user_req)
    : C_WriteRequest<T>(rwl, arrived, std::move(image_extents), std::move(bl), fadvise_flags, user_req),
    m_mismatch_offset(mismatch_offset), m_cmp_bl(std::move(cmp_bl)) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }

  ~C_CompAndWriteRequest() {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }

  template <typename... U>
  static inline std::shared_ptr<C_CompAndWriteRequest<T>> create(U&&... arg)
  {
    return SharedPtrContext::create<C_CompAndWriteRequest<T>>(std::forward<U>(arg)...);
  }

  auto shared_from_this() {
    return shared_from(this);
  }

  void finish_req(int r) override {
    if (m_compare_succeeded) {
      C_WriteRequest<T>::finish_req(r);
    } else {
      utime_t now = ceph_clock_now();
      update_req_stats(now);
    }
  }

  void update_req_stats(utime_t &now) override {
    /* Compare-and-write stats. Compare-and-write excluded from most write
     * stats because the read phase will make them look like slow writes in
     * those histograms. */
    if (!m_compare_succeeded) {
      rwl.m_perfcounter->inc(l_librbd_rwl_cmp_fails, 1);
    }
    utime_t comp_latency = now - this->m_arrived_time;
    rwl.m_perfcounter->tinc(l_librbd_rwl_cmp_latency, comp_latency);
  }

  /*
   * Compare and write doesn't implement alloc_resources(), deferred_handler(),
   * or dispatch(). We use the implementation in C_WriteRequest(), and only if the
   * compare phase succeeds and a write is actually performed.
   */

  const char *get_name() const override {
    return "C_CompAndWriteRequest";
  }
};

/**
 * This is the custodian of the BlockGuard cell for this write same.
 *
 * A writesame allocates and persists a data buffer like a write, but the
 * data buffer is usually much shorter than the write same.
 */
template <typename T>
struct C_WriteSameRequest : public C_WriteRequest<T> {
  using C_BlockIORequest<T>::rwl;
  friend std::ostream &operator<<(std::ostream &os,
				  const C_WriteSameRequest<T> &req) {
    os << (C_WriteRequest<T>&)req;
    return os;
  };

  C_WriteSameRequest(T &rwl, const utime_t arrived, Extents &&image_extents,
		     bufferlist&& bl, const int fadvise_flags, Context *user_req)
    : C_WriteRequest<T>(rwl, arrived, std::move(image_extents), std::move(bl), fadvise_flags, user_req) {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }

  ~C_WriteSameRequest() {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 99) << this << dendl;
    }
  }

  template <typename... U>
  static inline std::shared_ptr<C_WriteSameRequest<T>> create(U&&... arg)
  {
    return SharedPtrContext::create<C_WriteSameRequest<T>>(std::forward<U>(arg)...);
  }

  auto shared_from_this() {
    return shared_from(this);
  }

  /* Inherit finish_req() from C_WriteRequest */

  void update_req_stats(utime_t &now) override {
    /* Write same stats. Compare-and-write excluded from most write
     * stats because the read phase will make them look like slow writes in
     * those histograms. */
    ldout(rwl.m_image_ctx.cct, 01) << __func__ << " " << this->get_name() << " " << this << dendl;
    utime_t comp_latency = now - this->m_arrived_time;
    rwl.m_perfcounter->tinc(l_librbd_rwl_ws_latency, comp_latency);
  }

  /* Write sames will allocate one buffer, the size of the repeating pattern */
  void setup_buffer_resources(uint64_t &bytes_cached, uint64_t &bytes_dirtied) override {
    ldout(rwl.m_image_ctx.cct, 01) << __func__ << " " << this->get_name() << " " << this << dendl;
    assert(this->m_image_extents.size() == 1);
    bytes_dirtied += this->m_image_extents[0].second;
    auto pattern_length = this->bl.length();
    this->m_resources.buffers.emplace_back();
    struct WriteBufferAllocation &buffer = this->m_resources.buffers.back();
    buffer.allocation_size = MIN_WRITE_ALLOC_SIZE;
    buffer.allocated = false;
    bytes_cached += pattern_length;
    if (pattern_length > buffer.allocation_size) {
      buffer.allocation_size = pattern_length;
    }
  }

  virtual void setup_log_operations() {
    ldout(rwl.m_image_ctx.cct, 01) << __func__ << " " << this->get_name() << " " << this << dendl;
    /* Write same adds a single WS log op to the vector, corresponding to the single buffer item created above */
    assert(this->m_image_extents.size() == 1);
    auto extent = this->m_image_extents.front();
    /* operation->on_write_persist connected to m_prior_log_entries_persisted Gather */
    auto operation =
      std::make_shared<WriteSameLogOperation<T>>(*this->m_op_set.get(), extent.first, extent.second, this->bl.length());
    this->m_op_set->operations.emplace_back(operation);
  }

  /*
   * Write same doesn't implement alloc_resources(), deferred_handler(), or
   * dispatch(). We use the implementation in C_WriteRequest().
   */

  void schedule_append() {
    if (RWL_VERBOSE_LOGGING) {
      ldout(rwl.m_image_ctx.cct, 20) << __func__ << " " << this->get_name() << " " << this << dendl;
    }
    C_WriteRequest<T>::schedule_append();
  }

  const char *get_name() const override {
    return "C_WriteSameRequest";
  }
};

template <typename I>
void ReplicatedWriteLog<I>::perf_start(std::string name) {
  PerfCountersBuilder plb(m_image_ctx.cct, name, l_librbd_rwl_first, l_librbd_rwl_last);

  // Latency axis configuration for op histograms, values are in nanoseconds
  PerfHistogramCommon::axis_config_d op_hist_x_axis_config{
    "Latency (nsec)",
    PerfHistogramCommon::SCALE_LOG2, ///< Latency in logarithmic scale
    0,                               ///< Start at 0
    5000,                            ///< Quantization unit is 5usec
    16,                              ///< Ranges into the mS
  };

  // Op size axis configuration for op histogram y axis, values are in bytes
  PerfHistogramCommon::axis_config_d op_hist_y_axis_config{
    "Request size (bytes)",
    PerfHistogramCommon::SCALE_LOG2, ///< Request size in logarithmic scale
    0,                               ///< Start at 0
    512,                             ///< Quantization unit is 512 bytes
    16,                              ///< Writes up to >32k
  };

  // Num items configuration for op histogram y axis, values are in items
  PerfHistogramCommon::axis_config_d op_hist_y_axis_count_config{
    "Number of items",
    PerfHistogramCommon::SCALE_LINEAR, ///< Request size in linear scale
    0,                                 ///< Start at 0
    1,                                 ///< Quantization unit is 512 bytes
    32,                                ///< Writes up to >32k
  };

  plb.add_u64_counter(l_librbd_rwl_rd_req, "rd", "Reads");
  plb.add_u64_counter(l_librbd_rwl_rd_bytes, "rd_bytes", "Data size in reads");
  plb.add_time_avg(l_librbd_rwl_rd_latency, "rd_latency", "Latency of reads");

  plb.add_u64_counter(l_librbd_rwl_rd_hit_req, "hit_rd", "Reads completely hitting RWL");
  plb.add_u64_counter(l_librbd_rwl_rd_hit_bytes, "rd_hit_bytes", "Bytes read from RWL");
  plb.add_time_avg(l_librbd_rwl_rd_hit_latency, "hit_rd_latency", "Latency of read hits");

  plb.add_u64_counter(l_librbd_rwl_rd_part_hit_req, "part_hit_rd", "reads partially hitting RWL");

  plb.add_u64_counter(l_librbd_rwl_wr_req, "wr", "Writes");
  plb.add_u64_counter(l_librbd_rwl_wr_req_def, "wr_def", "Writes deferred for resources");
  plb.add_u64_counter(l_librbd_rwl_wr_req_def_lanes, "wr_def_lanes", "Writes deferred for lanes");
  plb.add_u64_counter(l_librbd_rwl_wr_req_def_log, "wr_def_log", "Writes deferred for log entries");
  plb.add_u64_counter(l_librbd_rwl_wr_req_def_buf, "wr_def_buf", "Writes deferred for buffers");
  plb.add_u64_counter(l_librbd_rwl_wr_req_overlap, "wr_overlap", "Writes overlapping with prior in-progress writes");
  plb.add_u64_counter(l_librbd_rwl_wr_req_queued, "wr_q_barrier", "Writes queued for prior barriers (aio_flush)");
  plb.add_u64_counter(l_librbd_rwl_wr_bytes, "wr_bytes", "Data size in writes");

  plb.add_u64_counter(l_librbd_rwl_log_ops, "log_ops", "Log appends");
  plb.add_u64_avg(l_librbd_rwl_log_op_bytes, "log_op_bytes", "Average log append bytes");

  plb.add_time_avg(l_librbd_rwl_req_arr_to_all_t, "req_arr_to_all_t", "Average arrival to allocation time (time deferred for overlap)");
  plb.add_time_avg(l_librbd_rwl_req_arr_to_dis_t, "req_arr_to_dis_t", "Average arrival to dispatch time (includes time deferred for overlaps and allocation)");
  plb.add_time_avg(l_librbd_rwl_req_all_to_dis_t, "req_all_to_dis_t", "Average allocation to dispatch time (time deferred for log resources)");
  plb.add_time_avg(l_librbd_rwl_wr_latency, "wr_latency", "Latency of writes (persistent completion)");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_wr_latency_hist, "wr_latency_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of write request latency (nanoseconds) vs. bytes written");
  plb.add_time_avg(l_librbd_rwl_wr_caller_latency, "caller_wr_latency", "Latency of write completion to caller");
  plb.add_time_avg(l_librbd_rwl_nowait_req_arr_to_all_t, "req_arr_to_all_nw_t", "Average arrival to allocation time (time deferred for overlap)");
  plb.add_time_avg(l_librbd_rwl_nowait_req_arr_to_dis_t, "req_arr_to_dis_nw_t", "Average arrival to dispatch time (includes time deferred for overlaps and allocation)");
  plb.add_time_avg(l_librbd_rwl_nowait_req_all_to_dis_t, "req_all_to_dis_nw_t", "Average allocation to dispatch time (time deferred for log resources)");
  plb.add_time_avg(l_librbd_rwl_nowait_wr_latency, "wr_latency_nw", "Latency of writes (persistent completion) not deferred for free space");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_nowait_wr_latency_hist, "wr_latency_nw_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of write request latency (nanoseconds) vs. bytes written for writes not deferred for free space");
  plb.add_time_avg(l_librbd_rwl_nowait_wr_caller_latency, "caller_wr_latency_nw", "Latency of write completion to callerfor writes not deferred for free space");
  plb.add_time_avg(l_librbd_rwl_log_op_alloc_t, "op_alloc_t", "Average buffer pmemobj_reserve() time");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_log_op_alloc_t_hist, "op_alloc_t_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of buffer pmemobj_reserve() time (nanoseconds) vs. bytes written");
  plb.add_time_avg(l_librbd_rwl_log_op_dis_to_buf_t, "op_dis_to_buf_t", "Average dispatch to buffer persist time");
  plb.add_time_avg(l_librbd_rwl_log_op_dis_to_app_t, "op_dis_to_app_t", "Average dispatch to log append time");
  plb.add_time_avg(l_librbd_rwl_log_op_dis_to_cmp_t, "op_dis_to_cmp_t", "Average dispatch to persist completion time");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_log_op_dis_to_cmp_t_hist, "op_dis_to_cmp_t_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of op dispatch to persist complete time (nanoseconds) vs. bytes written");

  plb.add_time_avg(l_librbd_rwl_log_op_buf_to_app_t, "op_buf_to_app_t", "Average buffer persist to log append time (write data persist/replicate + wait for append time)");
  plb.add_time_avg(l_librbd_rwl_log_op_buf_to_bufc_t, "op_buf_to_bufc_t", "Average buffer persist time (write data persist/replicate time)");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_log_op_buf_to_bufc_t_hist, "op_buf_to_bufc_t_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of write buffer persist time (nanoseconds) vs. bytes written");
  plb.add_time_avg(l_librbd_rwl_log_op_app_to_cmp_t, "op_app_to_cmp_t", "Average log append to persist complete time (log entry append/replicate + wait for complete time)");
  plb.add_time_avg(l_librbd_rwl_log_op_app_to_appc_t, "op_app_to_appc_t", "Average log append to persist complete time (log entry append/replicate time)");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_log_op_app_to_appc_t_hist, "op_app_to_appc_t_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of log append persist time (nanoseconds) (vs. op bytes)");

  plb.add_u64_counter(l_librbd_rwl_discard, "discard", "Discards");
  plb.add_u64_counter(l_librbd_rwl_discard_bytes, "discard_bytes", "Bytes discarded");
  plb.add_time_avg(l_librbd_rwl_discard_latency, "discard_lat", "Discard latency");

  plb.add_u64_counter(l_librbd_rwl_aio_flush, "aio_flush", "AIO flush (flush to RWL)");
  plb.add_u64_counter(l_librbd_rwl_aio_flush_def, "aio_flush_def", "AIO flushes deferred for resources");
  plb.add_time_avg(l_librbd_rwl_aio_flush_latency, "aio_flush_lat", "AIO flush latency");

  plb.add_u64_counter(l_librbd_rwl_ws,"ws", "Write Sames");
  plb.add_u64_counter(l_librbd_rwl_ws_bytes, "ws_bytes", "Write Same bytes to image");
  plb.add_time_avg(l_librbd_rwl_ws_latency, "ws_lat", "Write Same latency");

  plb.add_u64_counter(l_librbd_rwl_cmp, "cmp", "Compare and Write requests");
  plb.add_u64_counter(l_librbd_rwl_cmp_bytes, "cmp_bytes", "Compare and Write bytes compared/written");
  plb.add_time_avg(l_librbd_rwl_cmp_latency, "cmp_lat", "Compare and Write latecy");
  plb.add_u64_counter(l_librbd_rwl_cmp_fails, "cmp_fails", "Compare and Write compare fails");

  plb.add_u64_counter(l_librbd_rwl_flush, "flush", "Flush (flush RWL)");
  plb.add_u64_counter(l_librbd_rwl_invalidate_cache, "invalidate", "Invalidate RWL");
  plb.add_u64_counter(l_librbd_rwl_invalidate_discard_cache, "discard", "Discard and invalidate RWL");

  plb.add_time_avg(l_librbd_rwl_append_tx_t, "append_tx_lat", "Log append transaction latency");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_append_tx_t_hist, "append_tx_lat_histogram",
    op_hist_x_axis_config, op_hist_y_axis_count_config,
    "Histogram of log append transaction time (nanoseconds) vs. entries appended");
  plb.add_time_avg(l_librbd_rwl_retire_tx_t, "retire_tx_lat", "Log retire transaction latency");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_retire_tx_t_hist, "retire_tx_lat_histogram",
    op_hist_x_axis_config, op_hist_y_axis_count_config,
    "Histogram of log retire transaction time (nanoseconds) vs. entries retired");

  m_perfcounter = plb.create_perf_counters();
  m_image_ctx.cct->get_perfcounters_collection()->add(m_perfcounter);
}

template <typename I>
void ReplicatedWriteLog<I>::perf_stop() {
  assert(m_perfcounter);
  m_image_ctx.cct->get_perfcounters_collection()->remove(m_perfcounter);
  delete m_perfcounter;
}

template <typename I>
void ReplicatedWriteLog<I>::log_perf() {
  bufferlist bl;
  Formatter *f = Formatter::create("json-pretty");
  bl.append("Perf dump follows\n--- Begin perf dump ---\n");
  bl.append("{\n");
  stringstream ss;
  utime_t now = ceph_clock_now();
  ss << "\"test_time\": \"" << now << "\",";
  ss << "\"image\": \"" << m_image_ctx.name << "\",";
  bl.append(ss);
  bl.append("\"stats\": ");
  m_image_ctx.cct->get_perfcounters_collection()->dump_formatted(f, 0);
  f->flush(bl);
  bl.append(",\n\"histograms\": ");
  m_image_ctx.cct->get_perfcounters_collection()->dump_formatted_histograms(f, 0);
  f->flush(bl);
  delete f;
  bl.append("}\n--- End perf dump ---\n");
  bl.append('\0');
  ldout(m_image_ctx.cct, 1) << bl.c_str() << dendl;
}

template <typename I>
void ReplicatedWriteLog<I>::periodic_stats() {
  Mutex::Locker locker(m_lock);
  ldout(m_image_ctx.cct, 1) << "STATS: "
			    << "m_free_log_entries=" << m_free_log_entries << ", "
			    << "m_ops_to_flush=" << m_ops_to_flush.size() << ", "
			    << "m_ops_to_append=" << m_ops_to_append.size() << ", "
			    << "m_deferred_ios=" << m_deferred_ios.size() << ", "
			    << "m_log_entries=" << m_log_entries.size() << ", "
			    << "m_dirty_log_entries=" << m_dirty_log_entries.size() << ", "
			    << "m_bytes_allocated=" << m_bytes_allocated << ", "
			    << "m_bytes_cached=" << m_bytes_cached << ", "
			    << "m_bytes_dirty=" << m_bytes_dirty << ", "
			    << "bytes available=" << m_bytes_allocated_cap - m_bytes_allocated << ", "
			    << "m_current_sync_gen=" << m_current_sync_gen << ", "
			    << "m_flushed_sync_gen=" << m_flushed_sync_gen << ", "
			    << "m_flush_ops_in_flight=" << m_flush_ops_in_flight << ", "
			    << "m_flush_bytes_in_flight=" << m_flush_bytes_in_flight << ", "
			    << "m_async_flush_ops=" << m_async_flush_ops << ", "
			    << "m_async_append_ops=" << m_async_append_ops << ", "
			    << "m_async_complete_ops=" << m_async_complete_ops << ", "
			    << "m_async_null_flush_finish=" << m_async_null_flush_finish << ", "
			    << "m_async_process_work=" << m_async_process_work << ", "
			    << "m_async_op_tracker=[" << m_async_op_tracker << "]"
			    << dendl;
}

template <typename I>
void ReplicatedWriteLog<I>::arm_periodic_stats() {
  assert(m_timer_lock.is_locked());
  if (m_periodic_stats_enabled) {
    m_timer.add_event_after(LOG_STATS_INTERVAL_SECONDS, new FunctionContext(
      [this](int r) {
	/* m_timer_lock is held */
	periodic_stats();
	arm_periodic_stats();
      }));
  }
}

/*
 * Loads the log entries from an existing log.
 *
 * Creates the in-memory structures to represent the state of the
 * re-opened log.
 *
 * Finds the last appended sync point, and any sync points referred to
 * in log entries, but missing from the log. These missing sync points
 * are created and scheduled for append. Some rudimentary consistency
 * checking is done.
 *
 * Rebuilds the m_blocks_to_log_entries map, to make log entries
 * readable.
 *
 * Places all writes on the dirty entries list, which causes them all
 * to be flushed.
 *
 * TODO: Turn consistency check asserts into open failures.
 *
 * TODO: Writes referring to missing sync points must be discarded if
 * the replication mechanism doesn't guarantee all entries are
 * appended to all replicas in the same order, and that appends in
 * progress during a replica failure will be resolved by the
 * replication mechanism. PMDK pool replication guarantees this, so
 * discarding unsequenced writes referring to a missing sync point is
 * not yet implemented.
 *
 */
template <typename I>
void ReplicatedWriteLog<I>::load_existing_entries(DeferredContexts &later) {
  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_internal->m_log_pool, struct WriteLogPoolRoot);
  struct WriteLogPmemEntry *pmem_log_entries = D_RW(D_RW(pool_root)->log_entries);
  uint64_t entry_index = m_first_valid_entry;
  /* The map below allows us to find sync point log entries by sync
   * gen number, which is necessary so write entries can be linked to
   * their sync points. */
  std::map<uint64_t, std::shared_ptr<SyncPointLogEntry>> sync_point_entries;
  std::shared_ptr<SyncPointLogEntry> highest_existing_sync_point = nullptr;
  /* The map below tracks sync points referred to in writes but not
   * appearing in the sync_point_entries map.  We'll use this to
   * determine which sync points are missing and need to be
   * created. */
  std::map<uint64_t, bool> missing_sync_points;

  /*
   * Read the existing log entries. Construct an in-memory log entry
   * object of the appropriate type for each. Add these to the global
   * log entries list.
   *
   * Write entries will not link to their sync points yet. We'll do
   * that in the next pass. Here we'll accumulate a map of sync point
   * gen numbers that are referred to in writes but do not appearing in
   * the log.
   */
  while (entry_index != m_first_free_entry) {
    WriteLogPmemEntry *pmem_entry = &pmem_log_entries[entry_index];
    std::shared_ptr<GenericLogEntry> log_entry = nullptr;
    bool writer = pmem_entry->is_writer();

    assert(pmem_entry->entry_index == entry_index);
    if (pmem_entry->is_sync_point()) {
      ldout(m_image_ctx.cct, 20) << "Entry " << entry_index
				 << " is a sync point. pmem_entry=[" << *pmem_entry << "]" << dendl;
      auto sync_point_entry = std::make_shared<SyncPointLogEntry>(pmem_entry->sync_gen_number);
      log_entry = sync_point_entry;
      sync_point_entries[pmem_entry->sync_gen_number] = sync_point_entry;
      missing_sync_points.erase(pmem_entry->sync_gen_number);
      if (highest_existing_sync_point) {
	/* Sync points must appear in order */
	assert(pmem_entry->sync_gen_number > highest_existing_sync_point->ram_entry.sync_gen_number);
      }
      highest_existing_sync_point = sync_point_entry;
      m_current_sync_gen = pmem_entry->sync_gen_number;
    } else if (pmem_entry->is_write()) {
      ldout(m_image_ctx.cct, 20) << "Entry " << entry_index
				 << " is a write. pmem_entry=[" << *pmem_entry << "]" << dendl;
      auto write_entry =
	std::make_shared<WriteLogEntry>(nullptr, pmem_entry->image_offset_bytes, pmem_entry->write_bytes);
      write_entry->pmem_buffer = D_RW(pmem_entry->write_data);
      log_entry = write_entry;
    } else if (pmem_entry->is_writesame()) {
      ldout(m_image_ctx.cct, 20) << "Entry " << entry_index
				 << " is a write same. pmem_entry=[" << *pmem_entry << "]" << dendl;
      auto ws_entry =
	std::make_shared<WriteSameLogEntry>(nullptr, pmem_entry->image_offset_bytes,
					    pmem_entry->write_bytes, pmem_entry->ws_datalen);
      ws_entry->pmem_buffer = D_RW(pmem_entry->write_data);
      log_entry = ws_entry;
    } else if (pmem_entry->is_discard()) {
      ldout(m_image_ctx.cct, 20) << "Entry " << entry_index
				 << " is a discard. pmem_entry=[" << *pmem_entry << "]" << dendl;
      auto discard_entry =
	std::make_shared<DiscardLogEntry>(nullptr, pmem_entry->image_offset_bytes, pmem_entry->write_bytes);
      log_entry = discard_entry;
    } else {
      lderr(m_image_ctx.cct) << "Unexpected entry type in entry " << entry_index
			     << ", pmem_entry=[" << *pmem_entry << "]" << dendl;
      assert(false);
    }

    if (writer) {
      ldout(m_image_ctx.cct, 20) << "Entry " << entry_index
				 << " writes. pmem_entry=[" << *pmem_entry << "]" << dendl;
      if (highest_existing_sync_point) {
	/* Writes must precede the sync points they bear */
	assert(highest_existing_sync_point->ram_entry.sync_gen_number ==
	       highest_existing_sync_point->pmem_entry->sync_gen_number);
	assert(pmem_entry->sync_gen_number > highest_existing_sync_point->ram_entry.sync_gen_number);
      }
      if (!sync_point_entries[pmem_entry->sync_gen_number]) {
	missing_sync_points[pmem_entry->sync_gen_number] = true;
      }
    }

    log_entry->ram_entry = *pmem_entry;
    log_entry->pmem_entry = pmem_entry;
    log_entry->log_entry_index = entry_index;
    log_entry->completed = true;

    m_log_entries.push_back(log_entry);

    entry_index = (entry_index + 1) % m_total_log_entries;
  }

  /* Create missing sync points. These must not be appended until the
   * entry reload is complete and the write map is up to
   * date. Currently this is handled by the deferred contexts object
   * passed to new_sync_point(). These contexts won't be completed
   * until this function returns.  */
  for (auto &kv : missing_sync_points) {
    ldout(m_image_ctx.cct, 5) << "Adding sync point " << kv.first << dendl;
    if (0 == m_current_sync_gen) {
      /* The unlikely case where the log contains writing entries, but no sync
       * points (e.g. because they were all retired) */
      m_current_sync_gen = kv.first-1;
    }
    assert(kv.first == m_current_sync_gen+1);
    init_flush_new_sync_point(later);
    assert(kv.first == m_current_sync_gen);
    sync_point_entries[kv.first] = m_current_sync_point->log_entry;;
  }

  /*
   * Iterate over the log entries again (this time via the global
   * entries list), connecting write entries to their sync points and
   * updating the sync point stats.
   *
   * Add writes to the write log map.
   */
  std::shared_ptr<SyncPointLogEntry> previous_sync_point_entry = nullptr;
  for (auto &log_entry : m_log_entries)  {
    if (log_entry->ram_entry.is_writer()) {
      /* This entry is one of the types that write */
      auto gen_write_entry = static_pointer_cast<GeneralWriteLogEntry>(log_entry);
      auto sync_point_entry = sync_point_entries[gen_write_entry->ram_entry.sync_gen_number];
      if (!sync_point_entry) {
	lderr(m_image_ctx.cct) << "Sync point missing for entry=[" << *gen_write_entry << "]" << dendl;
	assert(false);
      } else {
	/* TODO: Discard unsequenced writes for sync points that
	 * didn't appear in the log (but were added above). This is
	 * optional if the replication mechanism guarantees
	 * persistence everywhere in the same order (which PMDK pool
	 * replication does). */
	gen_write_entry->sync_point_entry = sync_point_entry;
	sync_point_entry->m_writes++;
	sync_point_entry->m_bytes += gen_write_entry->ram_entry.write_bytes;
	sync_point_entry->m_writes_completed++;
	m_blocks_to_log_entries.add_log_entry(gen_write_entry);
	/* This entry is only dirty if its sync gen number is > the flushed
	 * sync gen number from the root object. */
	if (gen_write_entry->ram_entry.sync_gen_number > m_flushed_sync_gen) {
	  m_dirty_log_entries.push_back(log_entry);
	  m_bytes_dirty += gen_write_entry->bytes_dirty();
	} else {
	  gen_write_entry->flushed = true;
	  sync_point_entry->m_writes_flushed++;
	}
	if (log_entry->ram_entry.is_write()) {
	  /* This entry is a basic write */
	  uint64_t bytes_allocated = MIN_WRITE_ALLOC_SIZE;
	  if (gen_write_entry->ram_entry.write_bytes > bytes_allocated) {
	    bytes_allocated = gen_write_entry->ram_entry.write_bytes;
	  }
	  m_bytes_allocated += bytes_allocated;
	  m_bytes_cached += gen_write_entry->ram_entry.write_bytes;
	}
      }
    } else if (log_entry->ram_entry.is_sync_point()) {
      auto sync_point_entry = static_pointer_cast<SyncPointLogEntry>(log_entry);
      if (previous_sync_point_entry) {
	previous_sync_point_entry->m_next_sync_point_entry = sync_point_entry;
	if (previous_sync_point_entry->ram_entry.sync_gen_number > m_flushed_sync_gen) {
	  sync_point_entry->m_prior_sync_point_flushed = false;
	  assert(!previous_sync_point_entry->m_prior_sync_point_flushed ||
		 (0 == previous_sync_point_entry->m_writes) ||
		 (previous_sync_point_entry->m_writes > previous_sync_point_entry->m_writes_flushed));
	} else {
	  sync_point_entry->m_prior_sync_point_flushed = true;
	  assert(previous_sync_point_entry->m_prior_sync_point_flushed);
	  assert(previous_sync_point_entry->m_writes == previous_sync_point_entry->m_writes_flushed);
	}
	previous_sync_point_entry = sync_point_entry;
      } else {
	/* There are no previous sync points, so we'll consider them flushed */
	sync_point_entry->m_prior_sync_point_flushed = true;
      }
      ldout(m_image_ctx.cct, 10) << "Loaded to sync point=[" << *sync_point_entry << dendl;
    } else {
      lderr(m_image_ctx.cct) << "Unexpected entry type in entry=[" << *log_entry << "]" << dendl;
      assert(false);
    }
  }
  if (0 == m_current_sync_gen) {
    /* If a re-opened log was completely flushed, we'll have found no sync point entries here,
     * and not advanced m_current_sync_gen. Here we ensure it starts past the last flushed sync
     * point recorded in the log. */
    m_current_sync_gen = m_flushed_sync_gen;
  }
}

template <typename I>
void ReplicatedWriteLog<I>::rwl_init(Context *on_finish, DeferredContexts &later) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;
  TOID(struct WriteLogPoolRoot) pool_root;
  auto cache_state = dynamic_cast<ImageCacheStateRWL<I>*>(m_cache_state);

  Mutex::Locker locker(m_lock);
  assert(!m_initialized);
  ldout(cct,5) << "image name: " << m_image_ctx.name << " id: " << m_image_ctx.id << dendl;
  ldout(cct,5) << "rwl_size: " << cache_state->m_size << dendl;
  std::string rwl_path = cache_state->m_path;
  ldout(cct,5) << "rwl_path: " << rwl_path << dendl;
  if (!m_flush_on_close) {
    ldout(cct,5) << "NOT flushing on close" << dendl;
  }
  if (!m_retire_on_close) {
    ldout(cct,5) << "NOT retiring on close" << dendl;
  }

  std::string pool_name = m_image_ctx.md_ctx.get_pool_name();
  std::string log_pool_name = rwl_path + "/rbd-rwl." + pool_name + "." + m_image_ctx.id + ".pool";
  std::string log_poolset_name = rwl_path + "/rbd-rwl." + pool_name + "." + m_image_ctx.id + ".poolset";
  m_log_pool_config_size = max(cache_state->m_size, MIN_POOL_SIZE);

  if (access(log_poolset_name.c_str(), F_OK) == 0) {
    m_log_pool_name = log_poolset_name;
    m_log_is_poolset = true;
  } else {
    m_log_pool_name = log_pool_name;
    ldout(cct, 5) << "Poolset file " << log_poolset_name
		  << " not present (or can't open). Using unreplicated pool" << dendl;
  }

  if ((!cache_state->m_present) &&
    (access(m_log_pool_name.c_str(), F_OK) == 0)) {
      ldout(cct, 5) << "There's an existing pool/poolset file " << m_log_pool_name
         << ", While there's no cache in the image metatata." << dendl;
	    if (remove(m_log_pool_name.c_str()) != 0) {
	        lderr(cct) << "Failed to remove the pool/poolset file " << m_log_pool_name
              << dendl;
      } else {
        ldout(cct, 5) << "Removed the existing pool/poolset file." << dendl;
			}
	}

  if (access(m_log_pool_name.c_str(), F_OK) != 0) {
    if ((m_internal->m_log_pool =
	 pmemobj_create(m_log_pool_name.c_str(),
			rwl_pool_layout_name,
			m_log_pool_config_size,
			(S_IWUSR | S_IRUSR))) == NULL) {
      if (!m_image_ctx.ignore_image_cache_init_failure) {
        lderr(cct) << "failed to create pool (" << m_log_pool_name << ")"
                   << pmemobj_errormsg() << dendl;
      }
      m_present = false;
      m_clean = true;
      m_empty = true;
      /* TODO: filter/replace errnos that are meaningless to the caller */
      on_finish->complete(-errno);
      return;
    }
    m_present = true;
    m_clean = true;
    m_empty = true;
    pool_root = POBJ_ROOT(m_internal->m_log_pool, struct WriteLogPoolRoot);

    /* new pool, calculate and store metadata */
    size_t effective_pool_size = (size_t)(m_log_pool_config_size * USABLE_SIZE);
    size_t small_write_size = MIN_WRITE_ALLOC_SIZE + BLOCK_ALLOC_OVERHEAD_BYTES + sizeof(struct WriteLogPmemEntry);
    uint64_t num_small_writes = (uint64_t)(effective_pool_size / small_write_size);
    if (num_small_writes > MAX_LOG_ENTRIES) {
      num_small_writes = MAX_LOG_ENTRIES;
    }
    assert(num_small_writes > 2);
    m_log_pool_actual_size = m_log_pool_config_size;
    m_bytes_allocated_cap = effective_pool_size;
    /* Log ring empty */
    m_first_free_entry = 0;
    m_first_valid_entry = 0;
    TX_BEGIN(m_internal->m_log_pool) {
      TX_ADD(pool_root);
      D_RW(pool_root)->header.layout_version = RWL_POOL_VERSION;
      D_RW(pool_root)->log_entries =
	TX_ZALLOC(struct WriteLogPmemEntry,
		  sizeof(struct WriteLogPmemEntry) * num_small_writes);
      D_RW(pool_root)->pool_size = m_log_pool_actual_size;
      D_RW(pool_root)->flushed_sync_gen = m_flushed_sync_gen;
      D_RW(pool_root)->block_size = MIN_WRITE_ALLOC_SIZE;
      D_RW(pool_root)->num_log_entries = num_small_writes;
      D_RW(pool_root)->first_free_entry = m_first_free_entry;
      D_RW(pool_root)->first_valid_entry = m_first_valid_entry;
    } TX_ONCOMMIT {
      m_total_log_entries = D_RO(pool_root)->num_log_entries;
      m_free_log_entries = D_RO(pool_root)->num_log_entries - 1; // leave one free
    } TX_ONABORT {
      m_total_log_entries = 0;
      m_free_log_entries = 0;
      lderr(cct) << "failed to initialize pool (" << m_log_pool_name << ")" << dendl;
      on_finish->complete(-pmemobj_tx_errno());
      return;
    } TX_FINALLY {
    } TX_END;
  } else {
    m_present = true;
    /* Open existing pool */
    if ((m_internal->m_log_pool =
	 pmemobj_open(m_log_pool_name.c_str(),
		      rwl_pool_layout_name)) == NULL) {
      if (!m_image_ctx.ignore_image_cache_init_failure) {
        lderr(cct) << "failed to open pool (" << m_log_pool_name << "): "
		   << pmemobj_errormsg() << dendl;
      }
      on_finish->complete(-errno);
      return;
    }
    pool_root = POBJ_ROOT(m_internal->m_log_pool, struct WriteLogPoolRoot);
    if (D_RO(pool_root)->header.layout_version != RWL_POOL_VERSION) {
      if (!m_image_ctx.ignore_image_cache_init_failure) {
        lderr(cct) << "Pool layout version is " << D_RO(pool_root)->header.layout_version
	           << " expected " << RWL_POOL_VERSION << dendl;
      }
      on_finish->complete(-EINVAL);
      return;
    }
    if (D_RO(pool_root)->block_size != MIN_WRITE_ALLOC_SIZE) {
      if (!m_image_ctx.ignore_image_cache_init_failure) {
        lderr(cct) << "Pool block size is " << D_RO(pool_root)->block_size
                   << " expected " << MIN_WRITE_ALLOC_SIZE << dendl;
      }
      on_finish->complete(-EINVAL);
      return;
    }
    m_log_pool_actual_size = D_RO(pool_root)->pool_size;
    m_flushed_sync_gen = D_RO(pool_root)->flushed_sync_gen;
    m_total_log_entries = D_RO(pool_root)->num_log_entries;
    m_first_free_entry = D_RO(pool_root)->first_free_entry;
    m_first_valid_entry = D_RO(pool_root)->first_valid_entry;
    if (m_first_free_entry < m_first_valid_entry) {
      /* Valid entries wrap around the end of the ring, so first_free is lower
       * than first_valid.  If first_valid was == first_free+1, the entry at
       * first_free would be empty. The last entry is never used, so in
       * that case there would be zero free log entries. */
      m_free_log_entries = m_total_log_entries - (m_first_valid_entry - m_first_free_entry) -1;
    } else {
      /* first_valid is <= first_free. If they are == we have zero valid log
       * entries, and n-1 free log entries */
      m_free_log_entries = m_total_log_entries - (m_first_free_entry - m_first_valid_entry) -1;
    }
    size_t effective_pool_size = (size_t)(m_log_pool_config_size * USABLE_SIZE);
    m_bytes_allocated_cap = effective_pool_size;
    load_existing_entries(later);
    m_clean = m_dirty_log_entries.empty();
    m_empty = m_log_entries.empty();
  }

  ldout(cct,1) << "pool " << m_log_pool_name << " has " << m_total_log_entries
	       << " log entries, " << m_free_log_entries << " of which are free."
	       << " first_valid=" << m_first_valid_entry
	       << ", first_free=" << m_first_free_entry
	       << ", flushed_sync_gen=" << m_flushed_sync_gen
	       << ", current_sync_gen=" << m_current_sync_gen << dendl;
  if (m_first_free_entry == m_first_valid_entry) {
    ldout(cct,1) << "write log is empty" << dendl;
    m_empty = true;
  }

  /* Start the sync point following the last one seen in the
   * log. Flush the last sync point created during the loading of the
   * existing log entries. */
  init_flush_new_sync_point(later);
  ldout(cct,20) << "new sync point = [" << m_current_sync_point << "]" << dendl;

  m_initialized = true;
  // Start the thread
  start_workers();

  m_periodic_stats_enabled = cache_state->m_log_periodic_stats;
  /* Do these after we drop m_lock */
  later.add(new FunctionContext([this](int r) {
	if (m_periodic_stats_enabled) {
	  /* Log stats for the first time */
	  periodic_stats();
	  /* Arm periodic stats logging for the first time */
	  Mutex::Locker timer_locker(m_timer_lock);
	  arm_periodic_stats();
	}
      }));
  m_image_ctx.op_work_queue->queue(on_finish);
}

template <typename I>
void ReplicatedWriteLog<I>::update_image_cache_state(Context *on_finish) {
  m_cache_state->m_present = m_present;
  m_cache_state->m_empty = m_empty;
  m_cache_state->m_clean = m_clean;
  m_cache_state->write_image_cache_state(on_finish);
}

template <typename I>
void ReplicatedWriteLog<I>::clear_image_cache_state(Context *on_finish) {
  m_cache_state->m_present = false;
  m_cache_state->m_empty = true;
  m_cache_state->m_clean = true;
  m_cache_state->write_image_cache_state(on_finish);
}

template <typename I>
void ReplicatedWriteLog<I>::init(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;
  perf_start(m_image_ctx.id);

  assert(!m_initialized);

  Context *ctx = new FunctionContext(
    [this, on_finish](int r) {
      if (r >= 0) {
	update_image_cache_state(on_finish);
      } else {
        on_finish->complete(r);
      }
    });

  m_image_ctx.disable_zero_copy = true;
  DeferredContexts later;
  rwl_init(ctx, later);
}

template <typename I>
void ReplicatedWriteLog<I>::shut_down(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  ldout(cct,5) << "image name: " << m_image_ctx.name << " id: " << m_image_ctx.id << dendl;

  Context *ctx = new FunctionContext(
    [this, on_finish](int r) {
      ldout(m_image_ctx.cct, 6) << "shutdown complete" << dendl;
      m_image_ctx.op_work_queue->queue(on_finish, r);
    });
  ctx = new FunctionContext(
    [this, ctx](int r) {
      Context *next_ctx = ctx;
      if (r < 0) {
	/* Override on_finish status with this error */
	next_ctx = new FunctionContext(
	  [r, ctx](int _r) {
	    ctx->complete(r);
	  });
      }
      next_ctx->complete(0);
    });
  ctx = new FunctionContext(
    [this, ctx](int r) {
      Context *next_ctx = ctx;
      if (r < 0) {
	/* Override next_ctx status with this error */
	next_ctx = new FunctionContext(
	  [r, ctx](int _r) {
	    ctx->complete(r);
	  });
      }
      bool periodic_stats_enabled = m_periodic_stats_enabled;
      m_periodic_stats_enabled = false;
      {
	Mutex::Locker timer_locker(m_timer_lock);
	m_timer.shutdown();
      }

      if (periodic_stats_enabled) {
	/* Log stats one last time if they were enabled */
	periodic_stats();
      }
      auto cache_state = dynamic_cast<ImageCacheStateRWL<I>*>(m_cache_state);
      if (m_perfcounter && cache_state->m_log_stats_on_close) {
	log_perf();
      }
      {
	/* Much, but not all, of what's happening here is verifying
	   things are shut down as expected, and to make it visible
	   early (during testing) if they're not. */
	ldout(m_image_ctx.cct, 15) << "acquiring locks that shouldn't still be held" << dendl;
	Mutex::Locker timer_locker(m_timer_lock);
	Mutex::Locker retire_locker(m_log_retire_lock);
	RWLock::WLocker reader_locker(m_entry_reader_lock);
	Mutex::Locker dispatch_locker(m_deferred_dispatch_lock);
	Mutex::Locker append_locker(m_log_append_lock);
	Mutex::Locker locker(m_lock);
	Mutex::Locker bg_locker(m_blockguard_lock);
	Mutex::Locker bl_locker(m_entry_bl_lock);
	ldout(m_image_ctx.cct, 15) << "gratuitous locking complete" << dendl;
	if (use_finishers) {
	  ldout(m_image_ctx.cct, 6) << "stopping finishers" << dendl;
	  m_persist_finisher.wait_for_empty();
	  m_persist_finisher.stop();
	  m_log_append_finisher.wait_for_empty();
	  m_log_append_finisher.stop();
	  m_on_persist_finisher.wait_for_empty();
	  m_on_persist_finisher.stop();
	}
	{
	  assert(m_lock.is_locked());
	  if (m_flush_on_close) {
	    assert(m_dirty_log_entries.size() == 0);
	    m_clean = true;
	  }
	  else {
	    m_empty = m_log_entries.empty();
            m_clean = m_dirty_log_entries.empty();
	  }

	  if (m_retire_on_close) {
	    bool empty = true;
	    for (auto entry : m_log_entries) {
	      if (!entry->ram_entry.is_sync_point()) {
		empty = false; /* ignore sync points for emptiness */
	      }
	      if (entry->ram_entry.is_write() || entry->ram_entry.is_writesame()) {
		/* WS entry is also a Write entry */
		auto write_entry = static_pointer_cast<WriteLogEntry>(entry);
		m_blocks_to_log_entries.remove_log_entry(write_entry);
		assert(write_entry->referring_map_entries == 0);
		assert(write_entry->reader_count() == 0);
		assert(!write_entry->flushing);
	      }
	    }
	    m_empty = empty;
	  }
	  m_log_entries.clear();
	}
	if (m_internal->m_log_pool) {
	  ldout(m_image_ctx.cct, 6) << "closing pmem pool" << dendl;
	  pmemobj_close(m_internal->m_log_pool);
	}
	auto cache_state = dynamic_cast<ImageCacheStateRWL<I>*>(m_cache_state);
	if (m_clean && m_retire_on_close && cache_state->m_remove_on_close) {
	  if (m_log_is_poolset) {
	    ldout(m_image_ctx.cct, 5) << "Not removing poolset " << m_log_pool_name << dendl;
	  } else {
	    ldout(m_image_ctx.cct, 5) << "Removing empty pool file: " << m_log_pool_name << dendl;
	    if (remove(m_log_pool_name.c_str()) != 0) {
	      lderr(m_image_ctx.cct) << "failed to remove empty pool \"" << m_log_pool_name << "\": "
				     << pmemobj_errormsg() << dendl;
	    } else {
	      m_clean = true;
	      m_empty = true;
	      m_present = false;
	    }
	  }
	} else {
	  if (m_log_is_poolset) {
	    ldout(m_image_ctx.cct, 5) << "Not removing poolset " << m_log_pool_name << dendl;
	  } else {
	    ldout(m_image_ctx.cct, 5) << "Not removing pool file: " << m_log_pool_name << dendl;
	  }
	}
	if (m_perfcounter) {
	  perf_stop();
	}
      }
      next_ctx->complete(r);
    });
  if (m_first_free_entry == m_first_valid_entry) { //if the log entries are free.
    m_image_ctx.op_work_queue->queue(ctx, 0);
  } else {
    ctx = new FunctionContext(
      [this, ctx](int r) {
        ldout(m_image_ctx.cct, 6) << "done waiting for in flight operations (2)" << dendl;
        ldout(m_image_ctx.cct, 6) << "ctx:" << ctx << dendl;
        /* Get off of RWL WQ - thread pool about to be shut down */
        m_image_ctx.op_work_queue->queue(ctx, r);
    });
    ctx = new FunctionContext(
      [this, ctx](int r) {
        Context *next_ctx = ctx;
        if (r < 0) {
          /* Override next_ctx status with this error */
          next_ctx = new FunctionContext(
            [r, ctx](int _r) {
              ctx->complete(r);
            });
        }
        if (m_periodic_stats_enabled) {
          periodic_stats();
        }
        if (m_retire_on_close) {
          ldout(m_image_ctx.cct, 6) << "retiring entries" << dendl;
          while (retire_entries(MAX_ALLOC_PER_TRANSACTION)) { }
          ldout(m_image_ctx.cct, 6) << "waiting for internal async operations" << dendl;
        } else {
          {
            Mutex::Locker locker(m_lock);
            ldout(m_image_ctx.cct, 1) << "Not retiring " << m_log_entries.size() << " entries" << dendl;
          }
        }
        // Second op tracker wait after flush completion for process_work()
        {
          Mutex::Locker locker(m_lock);
          m_wake_up_enabled = false;
        }
        ldout(m_image_ctx.cct, 6) << "waiting for in flight operations (2)" << dendl;
        Mutex::Locker locker(m_lock);
	next_ctx = util::create_async_context_callback(m_image_ctx, next_ctx);
	m_async_op_tracker.wait_for_ops(next_ctx);
      });
    ctx = new FunctionContext(
      [this, ctx](int r) {
        Context *next_ctx = ctx;
        if (r < 0) {
          /* Override next_ctx status with this error */
          next_ctx = new FunctionContext(
            [r, ctx](int _r) {
              ctx->complete(r);
            });
        }
        {
          /* Sync with process_writeback_dirty_entries() */
          RWLock::WLocker entry_reader_wlocker(m_entry_reader_lock);
          m_shutting_down = true;
          /* Flush all writes to OSDs (unless disabled) and wait for all
             in-progress flush writes to complete */
          if (m_flush_on_close) {
            ldout(m_image_ctx.cct, 6) << "flushing" << dendl;
          } else {
            ldout(m_image_ctx.cct, 1) << "Not flushing " << m_dirty_log_entries.size() << " dirty entries" << dendl;
          }
          if (m_periodic_stats_enabled) {
            periodic_stats();
          }
        }
        flush_dirty_entries(next_ctx);
      });
    ctx = new FunctionContext(
      [this, ctx](int r) {
        /* Back to RWL WQ */
        ldout(m_image_ctx.cct, 6) << "done waiting for in flight operations" << dendl;
        m_work_queue.queue(ctx);
      });
    ctx = new FunctionContext(
      [this, ctx](int r) {
        Context *next_ctx = ctx;
        if (r < 0) {
          /* Override next_ctx status with this error */
          next_ctx = new FunctionContext(
            [r, ctx](int _r) {
              ctx->complete(r);
            });
        }
        ldout(m_image_ctx.cct, 6) << "waiting for in flight operations" << dendl;
        // Wait for in progress IOs to complete
        Mutex::Locker locker(m_lock);
        next_ctx = util::create_async_context_callback(m_image_ctx, next_ctx);
        m_async_op_tracker.wait_for_ops(next_ctx);
      });
    ctx = new FunctionContext(
      [this, ctx](int r) {
        ldout(m_image_ctx.cct, 6) << "Done internal_flush in shutdown" << dendl;
        m_work_queue.queue(ctx);
      });
    /* Complete all in-flight writes before shutting down */
    if (m_flush_on_close) {
      ldout(m_image_ctx.cct, 6) << "internal_flush in shutdown" << dendl;
      internal_flush(ctx, false, false);
    } else {
      aio_flush(librbd::io::FLUSH_SOURCE_USER, ctx);
    }
  }
}

} // namespace cache
} // namespace librbd


#endif // CEPH_LIBRBD_CACHE_REPLICATED_WRITE_LOG_INTERNAL

/* Local Variables: */
/* eval: (c-set-offset 'innamespace 0) */
/* End: */
