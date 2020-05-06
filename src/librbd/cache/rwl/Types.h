// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_CACHE_RWL_TYPES_H
#define CEPH_LIBRBD_CACHE_RWL_TYPES_H

#include <vector>
#include <libpmemobj.h>
#include "librbd/BlockGuard.h"
#include "librbd/io/Types.h"

class Context;

enum {
  l_librbd_rwl_first = 26500,

  // All read requests
  l_librbd_rwl_rd_req,           // read requests
  l_librbd_rwl_rd_bytes,         // bytes read
  l_librbd_rwl_rd_latency,       // average req completion latency

  // Read requests completed from RWL (no misses)
  l_librbd_rwl_rd_hit_req,       // read requests
  l_librbd_rwl_rd_hit_bytes,     // bytes read
  l_librbd_rwl_rd_hit_latency,   // average req completion latency

  // Reed requests with hit and miss extents
  l_librbd_rwl_rd_part_hit_req,  // read ops

  // All write requests
  l_librbd_rwl_wr_req,             // write requests
  l_librbd_rwl_wr_req_def,         // write requests deferred for resources
  l_librbd_rwl_wr_req_def_lanes,   // write requests deferred for lanes
  l_librbd_rwl_wr_req_def_log,     // write requests deferred for log entries
  l_librbd_rwl_wr_req_def_buf,     // write requests deferred for buffer space
  l_librbd_rwl_wr_req_overlap,     // write requests detained for overlap
  l_librbd_rwl_wr_req_queued,      // write requests queued for prior barrier
  l_librbd_rwl_wr_bytes,           // bytes written

  // Write log operations (1 .. n per request that appends to the log)
  l_librbd_rwl_log_ops,            // log append ops
  l_librbd_rwl_log_op_bytes,       // average bytes written per log op

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
  l_librbd_rwl_req_arr_to_all_t,   // arrival to allocation elapsed time - same as time deferred in block guard
  l_librbd_rwl_req_arr_to_dis_t,   // arrival to dispatch elapsed time
  l_librbd_rwl_req_all_to_dis_t,   // Time spent allocating or waiting to allocate resources
  l_librbd_rwl_wr_latency,         // average req (persist) completion latency
  l_librbd_rwl_wr_latency_hist,    // Histogram of write req (persist) completion latency vs. bytes written
  l_librbd_rwl_wr_caller_latency,  // average req completion (to caller) latency

  /* Request times for requests that never waited for space*/
  l_librbd_rwl_nowait_req_arr_to_all_t,   // arrival to allocation elapsed time - same as time deferred in block guard
  l_librbd_rwl_nowait_req_arr_to_dis_t,   // arrival to dispatch elapsed time
  l_librbd_rwl_nowait_req_all_to_dis_t,   // Time spent allocating or waiting to allocate resources
  l_librbd_rwl_nowait_wr_latency,         // average req (persist) completion latency
  l_librbd_rwl_nowait_wr_latency_hist,    // Histogram of write req (persist) completion latency vs. bytes written
  l_librbd_rwl_nowait_wr_caller_latency,  // average req completion (to caller) latency

  /* Log operation times */
  l_librbd_rwl_log_op_alloc_t,      // elapsed time of pmemobj_reserve()
  l_librbd_rwl_log_op_alloc_t_hist, // Histogram of elapsed time of pmemobj_reserve()

  l_librbd_rwl_log_op_dis_to_buf_t, // dispatch to buffer persist elapsed time
  l_librbd_rwl_log_op_dis_to_app_t, // dispatch to log append elapsed time
  l_librbd_rwl_log_op_dis_to_cmp_t, // dispatch to persist completion elapsed time
  l_librbd_rwl_log_op_dis_to_cmp_t_hist, // Histogram of dispatch to persist completion elapsed time

  l_librbd_rwl_log_op_buf_to_app_t, // data buf persist + append wait time
  l_librbd_rwl_log_op_buf_to_bufc_t,// data buf persist / replicate elapsed time
  l_librbd_rwl_log_op_buf_to_bufc_t_hist,// data buf persist time vs bytes histogram
  l_librbd_rwl_log_op_app_to_cmp_t, // log entry append + completion wait time
  l_librbd_rwl_log_op_app_to_appc_t, // log entry append / replicate elapsed time
  l_librbd_rwl_log_op_app_to_appc_t_hist, // log entry append time (vs. op bytes) histogram

  l_librbd_rwl_discard,
  l_librbd_rwl_discard_bytes,
  l_librbd_rwl_discard_latency,

  l_librbd_rwl_aio_flush,
  l_librbd_rwl_aio_flush_def,
  l_librbd_rwl_aio_flush_latency,
  l_librbd_rwl_ws,
  l_librbd_rwl_ws_bytes, // Bytes modified by write same, probably much larger than WS payload bytes
  l_librbd_rwl_ws_latency,

  l_librbd_rwl_cmp,
  l_librbd_rwl_cmp_bytes,
  l_librbd_rwl_cmp_latency,
  l_librbd_rwl_cmp_fails,

  l_librbd_rwl_flush,
  l_librbd_rwl_invalidate_cache,
  l_librbd_rwl_invalidate_discard_cache,

  l_librbd_rwl_append_tx_t,
  l_librbd_rwl_retire_tx_t,
  l_librbd_rwl_append_tx_t_hist,
  l_librbd_rwl_retire_tx_t_hist,

  l_librbd_rwl_last,
};

namespace librbd {
namespace cache {
namespace rwl {

class ImageExtentBuf;
typedef std::vector<ImageExtentBuf> ImageExtentBufs;

const int IN_FLIGHT_FLUSH_WRITE_LIMIT = 64;
const int IN_FLIGHT_FLUSH_BYTES_LIMIT = (1 * 1024 * 1024);

/* Limit work between sync points */
const uint64_t MAX_WRITES_PER_SYNC_POINT = 256;
const uint64_t MAX_BYTES_PER_SYNC_POINT = (1024 * 1024 * 8);

const uint32_t MIN_WRITE_ALLOC_SIZE = 512;
const uint32_t LOG_STATS_INTERVAL_SECONDS = 5;

/**** Write log entries ****/
const unsigned long int MAX_ALLOC_PER_TRANSACTION = 8;
const unsigned long int MAX_FREE_PER_TRANSACTION = 1;
const unsigned int MAX_CONCURRENT_WRITES = 256;

const uint64_t DEFAULT_POOL_SIZE = 1u<<30;
const uint64_t MIN_POOL_SIZE = DEFAULT_POOL_SIZE;
constexpr double USABLE_SIZE = (7.0 / 10);
const uint64_t BLOCK_ALLOC_OVERHEAD_BYTES = 16;
const uint8_t RWL_POOL_VERSION = 1;
const uint64_t MAX_LOG_ENTRIES = (1024 * 1024);
const double AGGRESSIVE_RETIRE_HIGH_WATER = 0.75;
const double RETIRE_HIGH_WATER = 0.50;
const double RETIRE_LOW_WATER = 0.40;
const int RETIRE_BATCH_TIME_LIMIT_MS = 250;

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
  BlockExtent block_extent();
  uint64_t get_offset_bytes();
  uint64_t get_write_bytes();
  friend std::ostream& operator<<(std::ostream& os,
                                  const WriteLogPmemEntry &entry);
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
  uint32_t block_size;           /* block size */
  uint32_t num_log_entries;
  uint32_t first_free_entry;     /* Entry following the newest valid entry */
  uint32_t first_valid_entry;    /* Index of the oldest valid entry in the log */
};

struct WriteBufferAllocation {
  unsigned int allocation_size = 0;
  pobj_action buffer_alloc_action;
  TOID(uint8_t) buffer_oid = OID_NULL;
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
  template <typename U>
  friend std::ostream &operator<<(std::ostream &os,
                                  const ExtentsSummary<U> &s);
  BlockExtent block_extent() {
    return BlockExtent(first_image_byte, last_image_byte);
  }
  io::Extent image_extent() {
    return image_extent(block_extent());
  }
};

io::Extent whole_volume_extent();

BlockExtent block_extent(const io::Extent& image_extent);

Context * override_ctx(int r, Context *ctx);

class ImageExtentBuf : public io::Extent {
public:
  bufferlist m_bl;
  ImageExtentBuf(io::Extent extent)
    : io::Extent(extent) { }
  ImageExtentBuf(io::Extent extent, bufferlist bl)
    : io::Extent(extent), m_bl(bl) { }
};

} // namespace rwl
} // namespace cache
} // namespace librbd

#endif // CEPH_LIBRBD_CACHE_RWL_TYPES_H
