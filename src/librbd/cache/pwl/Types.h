// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_PWL_TYPES_H
#define CEPH_LIBRBD_CACHE_PWL_TYPES_H

#include "acconfig.h"

#ifdef WITH_RBD_RWL
#include "libpmemobj.h"
#endif

#include <vector>
#include "librbd/BlockGuard.h"
#include "librbd/io/Types.h"

namespace ceph {
class Formatter;
}

class Context;

enum {
  l_librbd_pwl_first = 26500,

  // All read requests
  l_librbd_pwl_rd_req,           // read requests
  l_librbd_pwl_rd_bytes,         // bytes read
  l_librbd_pwl_rd_latency,       // average req completion latency

  // Read requests completed from RWL (no misses)
  l_librbd_pwl_rd_hit_req,       // read requests
  l_librbd_pwl_rd_hit_bytes,     // bytes read
  l_librbd_pwl_rd_hit_latency,   // average req completion latency

  // Reed requests with hit and miss extents
  l_librbd_pwl_rd_part_hit_req,  // read ops

  // Per SyncPoint's LogEntry number and write bytes distribution
  l_librbd_pwl_syncpoint_hist,

  // All write requests
  l_librbd_pwl_wr_req,             // write requests
  l_librbd_pwl_wr_bytes,           // bytes written
  l_librbd_pwl_wr_req_def,         // write requests deferred for resources
  l_librbd_pwl_wr_req_def_lanes,   // write requests deferred for lanes
  l_librbd_pwl_wr_req_def_log,     // write requests deferred for log entries
  l_librbd_pwl_wr_req_def_buf,     // write requests deferred for buffer space
  l_librbd_pwl_wr_req_overlap,     // write requests detained for overlap
  l_librbd_pwl_wr_req_queued,      // write requests queued for prior barrier

  // Write log operations (1 .. n per request that appends to the log)
  l_librbd_pwl_log_ops,            // log append ops
  l_librbd_pwl_log_op_bytes,       // average bytes written per log op

  /*

   Req and op average latencies to the beginning of and over various phases:

   +------------------------------+------+-------------------------------+
   | Phase                        | Name | Description                   |
   +------------------------------+------+-------------------------------+
   | Arrive at RWL                | arr  |Arrives as a request           |
   +------------------------------+------+-------------------------------+
   | Allocate resources           | all  |time spent in block guard for  |
   |                              |      |overlap sequencing occurs      |
   |                              |      |before this point              |
   +------------------------------+------+-------------------------------+
   | Dispatch                     | dis  |Op lifetime begins here. time  |
   |                              |      |spent in allocation waiting for|
   |                              |      |resources occurs before this   |
   |                              |      |point                          |
   +------------------------------+------+-------------------------------+
   | Payload buffer persist and   | buf  |time spent queued for          |
   |replicate                     |      |replication occurs before here |
   +------------------------------+------+-------------------------------+
   | Payload buffer persist       | bufc |bufc - buf is just the persist |
   |complete                      |      |time                           |
   +------------------------------+------+-------------------------------+
   | Log append                   | app  |time spent queued for append   |
   |                              |      |occurs before here             |
   +------------------------------+------+-------------------------------+
   | Append complete              | appc |appc - app is just the time    |
   |                              |      |spent in the append operation  |
   +------------------------------+------+-------------------------------+
   | Complete                     | cmp  |write persisted, replicated,   |
   |                              |      |and globally visible           |
   +------------------------------+------+-------------------------------+

  */

  /* Request times */
  l_librbd_pwl_req_arr_to_all_t,   // arrival to allocation elapsed time - same as time deferred in block guard
  l_librbd_pwl_req_arr_to_dis_t,   // arrival to dispatch elapsed time
  l_librbd_pwl_req_all_to_dis_t,   // Time spent allocating or waiting to allocate resources
  l_librbd_pwl_wr_latency,         // average req (persist) completion latency
  l_librbd_pwl_wr_latency_hist,    // Histogram of write req (persist) completion latency vs. bytes written
  l_librbd_pwl_wr_caller_latency,  // average req completion (to caller) latency

  /* Request times for requests that never waited for space*/
  l_librbd_pwl_nowait_req_arr_to_all_t,   // arrival to allocation elapsed time - same as time deferred in block guard
  l_librbd_pwl_nowait_req_arr_to_dis_t,   // arrival to dispatch elapsed time
  l_librbd_pwl_nowait_req_all_to_dis_t,   // Time spent allocating or waiting to allocate resources
  l_librbd_pwl_nowait_wr_latency,         // average req (persist) completion latency
  l_librbd_pwl_nowait_wr_latency_hist,    // Histogram of write req (persist) completion latency vs. bytes written
  l_librbd_pwl_nowait_wr_caller_latency,  // average req completion (to caller) latency

  /* Log operation times */
  l_librbd_pwl_log_op_alloc_t,      // elapsed time of pmemobj_reserve()
  l_librbd_pwl_log_op_alloc_t_hist, // Histogram of elapsed time of pmemobj_reserve()

  l_librbd_pwl_log_op_dis_to_buf_t, // dispatch to buffer persist elapsed time
  l_librbd_pwl_log_op_dis_to_app_t, // dispatch to log append elapsed time
  l_librbd_pwl_log_op_dis_to_cmp_t, // dispatch to persist completion elapsed time
  l_librbd_pwl_log_op_dis_to_cmp_t_hist, // Histogram of dispatch to persist completion elapsed time

  l_librbd_pwl_log_op_buf_to_app_t, // data buf persist + append wait time
  l_librbd_pwl_log_op_buf_to_bufc_t,// data buf persist / replicate elapsed time
  l_librbd_pwl_log_op_buf_to_bufc_t_hist,// data buf persist time vs bytes histogram
  l_librbd_pwl_log_op_app_to_cmp_t, // log entry append + completion wait time
  l_librbd_pwl_log_op_app_to_appc_t, // log entry append / replicate elapsed time
  l_librbd_pwl_log_op_app_to_appc_t_hist, // log entry append time (vs. op bytes) histogram

  l_librbd_pwl_discard,
  l_librbd_pwl_discard_bytes,
  l_librbd_pwl_discard_latency,

  l_librbd_pwl_aio_flush,
  l_librbd_pwl_aio_flush_def,
  l_librbd_pwl_aio_flush_latency,
  l_librbd_pwl_ws,
  l_librbd_pwl_ws_bytes, // Bytes modified by write same, probably much larger than WS payload bytes
  l_librbd_pwl_ws_latency,

  l_librbd_pwl_cmp,
  l_librbd_pwl_cmp_bytes,
  l_librbd_pwl_cmp_latency,
  l_librbd_pwl_cmp_fails,

  l_librbd_pwl_internal_flush,
  l_librbd_pwl_writeback_latency,
  l_librbd_pwl_invalidate_cache,
  l_librbd_pwl_invalidate_discard_cache,

  l_librbd_pwl_append_tx_t,
  l_librbd_pwl_retire_tx_t,
  l_librbd_pwl_append_tx_t_hist,
  l_librbd_pwl_retire_tx_t_hist,

  l_librbd_pwl_last,
};

enum {
  WRITE_LOG_CACHE_ENTRY_VALID = 1U << 0,      /* if 0, this entry is free */
  WRITE_LOG_CACHE_ENTRY_SYNC_POINT = 1U << 1, /* No data. No write sequence number.
                                                 Marks sync point for this sync gen number */
  WRITE_LOG_CACHE_ENTRY_SEQUENCED = 1U << 2,  /* write sequence number is valid */
  WRITE_LOG_CACHE_ENTRY_HAS_DATA = 1U << 3,   /* write_data field is valid (else ignore) */
  WRITE_LOG_CACHE_ENTRY_DISCARD = 1U << 4,    /* has_data will be 0 if this is a discard */
  WRITE_LOG_CACHE_ENTRY_WRITESAME = 1U << 5,  /* ws_datalen indicates length of data at write_bytes */
};

namespace librbd {
namespace cache {
namespace pwl {

class ImageExtentBuf;

const int IN_FLIGHT_FLUSH_WRITE_LIMIT = 64;
const int IN_FLIGHT_FLUSH_BYTES_LIMIT = (1 * 1024 * 1024);

/* Limit work between sync points */
const uint64_t MAX_WRITES_PER_SYNC_POINT = 256;
const uint64_t MAX_BYTES_PER_SYNC_POINT = (1024 * 1024 * 8);

const uint32_t MIN_WRITE_ALLOC_SIZE = 512;
const uint32_t MIN_WRITE_ALLOC_SSD_SIZE = 4096;
const uint32_t LOG_STATS_INTERVAL_SECONDS = 5;

/**** Write log entries ****/
const unsigned long int MAX_ALLOC_PER_TRANSACTION = 8;
const unsigned long int MAX_FREE_PER_TRANSACTION = 1;
const unsigned int MAX_CONCURRENT_WRITES = (1024 * 1024);

const uint64_t DEFAULT_POOL_SIZE = 1u<<30;
const uint64_t MIN_POOL_SIZE = DEFAULT_POOL_SIZE;
const uint64_t POOL_SIZE_ALIGN = 1 << 20;
constexpr double USABLE_SIZE = (7.0 / 10);
const uint64_t BLOCK_ALLOC_OVERHEAD_BYTES = 16;
const uint8_t RWL_LAYOUT_VERSION = 1;
const uint8_t SSD_LAYOUT_VERSION = 1;
const uint64_t MAX_LOG_ENTRIES = (1024 * 1024);
const double AGGRESSIVE_RETIRE_HIGH_WATER = 0.75;
const double RETIRE_HIGH_WATER = 0.50;
const double RETIRE_LOW_WATER = 0.40;
const int RETIRE_BATCH_TIME_LIMIT_MS = 250;
const uint64_t CONTROL_BLOCK_MAX_LOG_ENTRIES = 32;
const uint64_t SPAN_MAX_DATA_LEN = (16 * 1024 * 1024);

/* offset of ring on SSD */
const uint64_t DATA_RING_BUFFER_OFFSET = 8192;

/* Defer a set of Contexts until destruct/exit. Used for deferring
 * work on a given thread until a required lock is dropped. */
class DeferredContexts {
private:
  std::vector<Context*> contexts;
public:
  ~DeferredContexts();
  void add(Context* ctx);
};

/* Pmem structures */
#ifdef WITH_RBD_RWL
POBJ_LAYOUT_BEGIN(rbd_pwl);
POBJ_LAYOUT_ROOT(rbd_pwl, struct WriteLogPoolRoot);
POBJ_LAYOUT_TOID(rbd_pwl, uint8_t);
POBJ_LAYOUT_TOID(rbd_pwl, struct WriteLogCacheEntry);
POBJ_LAYOUT_END(rbd_pwl);
#endif

struct WriteLogCacheEntry {
  uint64_t sync_gen_number = 0;
  uint64_t write_sequence_number = 0;
  uint64_t image_offset_bytes;
  uint64_t write_bytes;
  #ifdef WITH_RBD_RWL
  TOID(uint8_t) write_data;
  #endif
  #ifdef WITH_RBD_SSD_CACHE
  uint64_t write_data_pos = 0; /* SSD data offset */
  #endif
  uint8_t flags = 0;
  uint32_t ws_datalen = 0;  /* Length of data buffer (writesame only) */
  uint32_t entry_index = 0; /* For debug consistency check. Can be removed if
                             * we need the space */
  WriteLogCacheEntry(uint64_t image_offset_bytes=0, uint64_t write_bytes=0)
      : image_offset_bytes(image_offset_bytes), write_bytes(write_bytes) {}
  BlockExtent block_extent();
  uint64_t get_offset_bytes();
  uint64_t get_write_bytes();
  bool is_entry_valid() const {
    return flags & WRITE_LOG_CACHE_ENTRY_VALID;
  }
  bool is_sync_point() const {
    return flags & WRITE_LOG_CACHE_ENTRY_SYNC_POINT;
  }
  bool is_sequenced() const {
    return flags & WRITE_LOG_CACHE_ENTRY_SEQUENCED;
  }
  bool has_data() const {
    return flags & WRITE_LOG_CACHE_ENTRY_HAS_DATA;
  }
  bool is_discard() const {
    return flags & WRITE_LOG_CACHE_ENTRY_DISCARD;
  }
  bool is_writesame() const {
    return flags & WRITE_LOG_CACHE_ENTRY_WRITESAME;
  }
  bool is_write() const {
    /* Log entry is a basic write */
    return !is_sync_point() && !is_discard() && !is_writesame();
  }
  bool is_writer() const {
    /* Log entry is any type that writes data */
    return is_write() || is_discard() || is_writesame();
  }
  void set_entry_valid(bool flag) {
    if (flag) {
      flags |= WRITE_LOG_CACHE_ENTRY_VALID;
    } else {
      flags &= ~WRITE_LOG_CACHE_ENTRY_VALID;
    }
  }
  void set_sync_point(bool flag) {
    if (flag) {
      flags |= WRITE_LOG_CACHE_ENTRY_SYNC_POINT;
    } else {
      flags &= ~WRITE_LOG_CACHE_ENTRY_SYNC_POINT;
    }
  }
  void set_sequenced(bool flag) {
    if (flag) {
      flags |= WRITE_LOG_CACHE_ENTRY_SEQUENCED;
    } else {
      flags &= ~WRITE_LOG_CACHE_ENTRY_SEQUENCED;
    }
  }
  void set_has_data(bool flag) {
    if (flag) {
      flags |= WRITE_LOG_CACHE_ENTRY_HAS_DATA;
    } else {
      flags &= ~WRITE_LOG_CACHE_ENTRY_HAS_DATA;
    }
  }
  void set_discard(bool flag) {
    if (flag) {
      flags |= WRITE_LOG_CACHE_ENTRY_DISCARD;
    } else {
      flags &= ~WRITE_LOG_CACHE_ENTRY_DISCARD;
    }
  }
  void set_writesame(bool flag) {
    if (flag) {
      flags |= WRITE_LOG_CACHE_ENTRY_WRITESAME;
    } else {
      flags &= ~WRITE_LOG_CACHE_ENTRY_WRITESAME;
    }
  }
  friend std::ostream& operator<<(std::ostream& os,
                                  const WriteLogCacheEntry &entry);
  #ifdef WITH_RBD_SSD_CACHE
  DENC(WriteLogCacheEntry, v, p) {
    DENC_START(1, 1, p);
    denc(v.sync_gen_number, p);
    denc(v.write_sequence_number, p);
    denc(v.image_offset_bytes, p);
    denc(v.write_bytes, p);
    denc(v.write_data_pos, p);
    denc(v.flags, p);
    denc(v.ws_datalen, p);
    denc(v.entry_index, p);
    DENC_FINISH(p);
  }
  #endif
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<WriteLogCacheEntry*>& ls);
};

struct WriteLogPoolRoot {
  #ifdef WITH_RBD_RWL
  union {
    struct {
      uint8_t layout_version;
    };
    uint64_t _u64;
  } header;
  TOID(struct WriteLogCacheEntry) log_entries;   /* contiguous array of log entries */
  #endif
  #ifdef WITH_RBD_SSD_CACHE
  uint64_t layout_version = 0;
  uint64_t cur_sync_gen = 0;    /* TODO: remove it when changing disk format */
  #endif
  uint64_t pool_size;
  uint64_t flushed_sync_gen;    /* All writing entries with this or a lower
                                 * sync gen number are flushed. */
  uint32_t block_size;
  uint32_t num_log_entries;
  uint64_t first_free_entry;    /* The free entry following the latest valid
                                 * entry, which is going to be written */
  uint64_t first_valid_entry;   /* The oldest valid entry to be retired */

  #ifdef WITH_RBD_SSD_CACHE
  DENC(WriteLogPoolRoot, v, p) {
    DENC_START(1, 1, p);
    denc(v.layout_version, p);
    denc(v.cur_sync_gen, p);
    denc(v.pool_size, p);
    denc(v.flushed_sync_gen, p);
    denc(v.block_size, p);
    denc(v.num_log_entries, p);
    denc(v.first_free_entry, p);
    denc(v.first_valid_entry, p);
    DENC_FINISH(p);
  }
  #endif

  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<WriteLogPoolRoot*>& ls);
};

struct WriteBufferAllocation {
  unsigned int allocation_size = 0;
  #ifdef WITH_RBD_RWL
  pobj_action buffer_alloc_action;
  TOID(uint8_t) buffer_oid = OID_NULL;
  #endif
  bool allocated = false;
  utime_t allocation_lat;
};

static inline io::Extent image_extent(const BlockExtent& block_extent) {
  return io::Extent(block_extent.block_start,
                    block_extent.block_end - block_extent.block_start);
}

template <typename ExtentsType>
class ExtentsSummary {
public:
  uint64_t total_bytes;
  uint64_t first_image_byte;
  uint64_t last_image_byte;
  explicit ExtentsSummary(const ExtentsType &extents);
  friend std::ostream &operator<<(std::ostream &os,
                                  const ExtentsSummary &s) {
    os << "total_bytes=" << s.total_bytes
       << ", first_image_byte=" << s.first_image_byte
       << ", last_image_byte=" << s.last_image_byte;
    return os;
  }
  BlockExtent block_extent() {
    return BlockExtent(first_image_byte, last_image_byte);
  }
  io::Extent image_extent() {
    return librbd::cache::pwl::image_extent(block_extent());
  }
};

io::Extent whole_volume_extent();

BlockExtent block_extent(const io::Extent& image_extent);

Context * override_ctx(int r, Context *ctx);

class ImageExtentBuf : public io::Extent {
public:
  bufferlist m_bl;
  bool need_to_truncate;
  int truncate_offset;
  bool writesame;
  ImageExtentBuf() {}
  ImageExtentBuf(io::Extent extent,
                 bool need_to_truncate = false, uint64_t truncate_offset = 0,
                 bool writesame = false)
    : io::Extent(extent), need_to_truncate(need_to_truncate),
      truncate_offset(truncate_offset), writesame(writesame) {}
  ImageExtentBuf(io::Extent extent, bufferlist bl,
                 bool need_to_truncate = false, uint64_t truncate_offset = 0,
                 bool writesame = false)
    : io::Extent(extent), m_bl(bl), need_to_truncate(need_to_truncate),
      truncate_offset(truncate_offset), writesame(writesame) {}
};

std::string unique_lock_name(const std::string &name, void *address);

} // namespace pwl
} // namespace cache
} // namespace librbd

#ifdef WITH_RBD_SSD_CACHE
WRITE_CLASS_DENC(librbd::cache::pwl::WriteLogCacheEntry)
WRITE_CLASS_DENC(librbd::cache::pwl::WriteLogPoolRoot)
#endif

#endif // CEPH_LIBRBD_CACHE_PWL_TYPES_H
