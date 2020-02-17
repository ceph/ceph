// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <libpmemobj.h>
#include "ReplicatedWriteLog.h"
#include "include/buffer.h"
#include "include/Context.h"
#include "include/ceph_assert.h"
#include "common/deleter.h"
#include "common/dout.h"
#include "common/environment.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "common/Timer.h"
#include "common/perf_counters.h"
#include "librbd/ImageCtx.h"
#include "librbd/cache/rwl/ImageCacheState.h"
#include "librbd/cache/rwl/LogEntry.h"
#include "librbd/cache/rwl/Types.h"
#include <map>
#include <vector>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::ReplicatedWriteLog: " << this << " " \
                           <<  __func__ << ": "

const uint32_t MIN_WRITE_ALLOC_SIZE = 512;
const uint32_t LOG_STATS_INTERVAL_SECONDS = 5;

/**** Write log entries ****/
const uint64_t DEFAULT_POOL_SIZE = 1u<<30;
const uint64_t MIN_POOL_SIZE = DEFAULT_POOL_SIZE;
constexpr double USABLE_SIZE = (7.0 / 10);
const uint64_t BLOCK_ALLOC_OVERHEAD_BYTES = 16;
const uint8_t RWL_POOL_VERSION = 1;
const uint64_t MAX_LOG_ENTRIES = (1024 * 1024);

namespace librbd {
namespace cache {

using namespace librbd::cache::rwl;

typedef ReplicatedWriteLog<ImageCtx>::Extents Extents;

template <typename I>
ReplicatedWriteLog<I>::ReplicatedWriteLog(I &image_ctx, librbd::cache::rwl::ImageCacheState<I>* cache_state)
  : m_cache_state(cache_state),
    m_rwl_pool_layout_name(POBJ_LAYOUT_NAME(rbd_rwl)),
    m_image_ctx(image_ctx),
    m_log_pool_config_size(DEFAULT_POOL_SIZE),
    m_image_writeback(image_ctx),
    m_lock("librbd::cache::ReplicatedWriteLog::m_lock",
           true, true),
    m_thread_pool(image_ctx.cct, "librbd::cache::ReplicatedWriteLog::thread_pool", "tp_rwl",
                  4,
                  ""),
    m_work_queue("librbd::cache::ReplicatedWriteLog::work_queue",
                 image_ctx.config.template get_val<uint64_t>("rbd_op_thread_timeout"),
                 &m_thread_pool)
{
  CephContext *cct = m_image_ctx.cct;
  ImageCtx::get_timer_instance(cct, &m_timer, &m_timer_lock);
}

template <typename I>
ReplicatedWriteLog<I>::~ReplicatedWriteLog() {
  ldout(m_image_ctx.cct, 15) << "enter" << dendl;
  {
    std::lock_guard timer_locker(*m_timer_lock);
    std::lock_guard locker(m_lock);
    m_timer->cancel_event(m_timer_ctx);
    m_thread_pool.stop();
    m_log_pool = nullptr;
    delete m_cache_state;
    m_cache_state = nullptr;
  }
  ldout(m_image_ctx.cct, 15) << "exit" << dendl;
}

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

  plb.add_time_avg(
      l_librbd_rwl_req_arr_to_all_t, "req_arr_to_all_t",
      "Average arrival to allocation time (time deferred for overlap)");
  plb.add_time_avg(
      l_librbd_rwl_req_arr_to_dis_t, "req_arr_to_dis_t",
      "Average arrival to dispatch time (includes time deferred for overlaps and allocation)");
  plb.add_time_avg(
      l_librbd_rwl_req_all_to_dis_t, "req_all_to_dis_t",
      "Average allocation to dispatch time (time deferred for log resources)");
  plb.add_time_avg(
      l_librbd_rwl_wr_latency, "wr_latency",
      "Latency of writes (persistent completion)");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_wr_latency_hist, "wr_latency_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of write request latency (nanoseconds) vs. bytes written");
  plb.add_time_avg(
      l_librbd_rwl_wr_caller_latency, "caller_wr_latency",
      "Latency of write completion to caller");
  plb.add_time_avg(
      l_librbd_rwl_nowait_req_arr_to_all_t, "req_arr_to_all_nw_t",
      "Average arrival to allocation time (time deferred for overlap)");
  plb.add_time_avg(
      l_librbd_rwl_nowait_req_arr_to_dis_t, "req_arr_to_dis_nw_t",
      "Average arrival to dispatch time (includes time deferred for overlaps and allocation)");
  plb.add_time_avg(
      l_librbd_rwl_nowait_req_all_to_dis_t, "req_all_to_dis_nw_t",
      "Average allocation to dispatch time (time deferred for log resources)");
  plb.add_time_avg(
      l_librbd_rwl_nowait_wr_latency, "wr_latency_nw",
      "Latency of writes (persistent completion) not deferred for free space");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_nowait_wr_latency_hist, "wr_latency_nw_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of write request latency (nanoseconds) vs. bytes written for writes not deferred for free space");
  plb.add_time_avg(
      l_librbd_rwl_nowait_wr_caller_latency, "caller_wr_latency_nw",
      "Latency of write completion to callerfor writes not deferred for free space");
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

  plb.add_time_avg(
      l_librbd_rwl_log_op_buf_to_app_t, "op_buf_to_app_t",
      "Average buffer persist to log append time (write data persist/replicate + wait for append time)");
  plb.add_time_avg(
      l_librbd_rwl_log_op_buf_to_bufc_t, "op_buf_to_bufc_t",
      "Average buffer persist time (write data persist/replicate time)");
  plb.add_u64_counter_histogram(
    l_librbd_rwl_log_op_buf_to_bufc_t_hist, "op_buf_to_bufc_t_bytes_histogram",
    op_hist_x_axis_config, op_hist_y_axis_config,
    "Histogram of write buffer persist time (nanoseconds) vs. bytes written");
  plb.add_time_avg(
      l_librbd_rwl_log_op_app_to_cmp_t, "op_app_to_cmp_t",
      "Average log append to persist complete time (log entry append/replicate + wait for complete time)");
  plb.add_time_avg(
      l_librbd_rwl_log_op_app_to_appc_t, "op_app_to_appc_t",
      "Average log append to persist complete time (log entry append/replicate time)");
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
  ceph_assert(m_perfcounter);
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
  std::lock_guard locker(m_lock);
  ldout(m_image_ctx.cct, 1) << "STATS: "
                            << "m_free_log_entries=" << m_free_log_entries << ", "
                            << "m_log_entries=" << m_log_entries.size() << ", "
                            << "m_dirty_log_entries=" << m_dirty_log_entries.size() << ", "
                            << "m_bytes_allocated=" << m_bytes_allocated << ", "
                            << "m_bytes_cached=" << m_bytes_cached << ", "
                            << "m_bytes_dirty=" << m_bytes_dirty << ", "
                            << "bytes available=" << m_bytes_allocated_cap - m_bytes_allocated << ", "
                            << "m_current_sync_gen=" << m_current_sync_gen << ", "
                            << "m_flushed_sync_gen=" << m_flushed_sync_gen << ", "
                            << dendl;
}

template <typename I>
void ReplicatedWriteLog<I>::arm_periodic_stats() {
  ceph_assert(m_timer_lock->is_locked());
  if (m_periodic_stats_enabled) {
    m_timer_ctx = new LambdaContext(
      [this](int r) {
        /* m_timer_lock is held */
        periodic_stats();
        arm_periodic_stats();
      });
    m_timer->add_event_after(LOG_STATS_INTERVAL_SECONDS, m_timer_ctx);
  }
}

template <typename I>
void ReplicatedWriteLog<I>::rwl_init(Context *on_finish, DeferredContexts &later) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;
  TOID(struct WriteLogPoolRoot) pool_root;
  ceph_assert(m_cache_state);
  std::lock_guard locker(m_lock);
  ceph_assert(!m_initialized);
  ldout(cct,5) << "image name: " << m_image_ctx.name << " id: " << m_image_ctx.id << dendl;
  ldout(cct,5) << "rwl_size: " << m_cache_state->size << dendl;
  std::string rwl_path = m_cache_state->path;
  ldout(cct,5) << "rwl_path: " << rwl_path << dendl;

  std::string pool_name = m_image_ctx.md_ctx.get_pool_name();
  std::string log_pool_name = rwl_path + "/rbd-rwl." + pool_name + "." + m_image_ctx.id + ".pool";
  std::string log_poolset_name = rwl_path + "/rbd-rwl." + pool_name + "." + m_image_ctx.id + ".poolset";
  m_log_pool_config_size = max(m_cache_state->size, MIN_POOL_SIZE);

  if (access(log_poolset_name.c_str(), F_OK) == 0) {
    m_log_pool_name = log_poolset_name;
    m_log_is_poolset = true;
  } else {
    m_log_pool_name = log_pool_name;
    ldout(cct, 5) << "Poolset file " << log_poolset_name
                  << " not present (or can't open). Using unreplicated pool" << dendl;
  }

  if ((!m_cache_state->present) &&
      (access(m_log_pool_name.c_str(), F_OK) == 0)) {
    ldout(cct, 5) << "There's an existing pool/poolset file " << m_log_pool_name
                  << ", While there's no cache in the image metatata." << dendl;
    if (remove(m_log_pool_name.c_str()) != 0) {
      lderr(cct) << "Failed to remove the pool/poolset file " << m_log_pool_name
                 << dendl;
      on_finish->complete(-errno);
      return;
    } else {
      ldout(cct, 5) << "Removed the existing pool/poolset file." << dendl;
    }
  }

  if (access(m_log_pool_name.c_str(), F_OK) != 0) {
    if ((m_log_pool =
         pmemobj_create(m_log_pool_name.c_str(),
                        m_rwl_pool_layout_name,
                        m_log_pool_config_size,
                        (S_IWUSR | S_IRUSR))) == NULL) {
      lderr(cct) << "failed to create pool (" << m_log_pool_name << ")"
                   << pmemobj_errormsg() << dendl;
      m_cache_state->present = false;
      m_cache_state->clean = true;
      m_cache_state->empty = true;
      /* TODO: filter/replace errnos that are meaningless to the caller */
      on_finish->complete(-errno);
      return;
    }
    m_cache_state->present = true;
    m_cache_state->clean = true;
    m_cache_state->empty = true;
    pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);

    /* new pool, calculate and store metadata */
    size_t effective_pool_size = (size_t)(m_log_pool_config_size * USABLE_SIZE);
    size_t small_write_size = MIN_WRITE_ALLOC_SIZE + BLOCK_ALLOC_OVERHEAD_BYTES + sizeof(struct WriteLogPmemEntry);
    uint64_t num_small_writes = (uint64_t)(effective_pool_size / small_write_size);
    if (num_small_writes > MAX_LOG_ENTRIES) {
      num_small_writes = MAX_LOG_ENTRIES;
    }
    if (num_small_writes <= 2) {
      lderr(cct) << "num_small_writes needs to > 2" << dendl;
      on_finish->complete(-EINVAL);
      return;
    }
    m_log_pool_actual_size = m_log_pool_config_size;
    m_bytes_allocated_cap = effective_pool_size;
    /* Log ring empty */
    m_first_free_entry = 0;
    m_first_valid_entry = 0;
    TX_BEGIN(m_log_pool) {
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
    // TODO: load existed cache. This will be covered in later PR.
  }

  ldout(cct,1) << "pool " << m_log_pool_name << " has " << m_total_log_entries
               << " log entries, " << m_free_log_entries << " of which are free."
               << " first_valid=" << m_first_valid_entry
               << ", first_free=" << m_first_free_entry
               << ", flushed_sync_gen=" << m_flushed_sync_gen
               << ", current_sync_gen=" << m_current_sync_gen << dendl;
  if (m_first_free_entry == m_first_valid_entry) {
    ldout(cct,1) << "write log is empty" << dendl;
    m_cache_state->empty = true;
  }

  // TODO: Will init sync point, this will be covered in later PR.
  //  init_flush_new_sync_point(later);

  m_initialized = true;
  // Start the thread
  m_thread_pool.start();

  m_periodic_stats_enabled = m_cache_state->log_periodic_stats;
  /* Do these after we drop m_lock */
  later.add(new LambdaContext([this](int r) {
        if (m_periodic_stats_enabled) {
          /* Log stats for the first time */
          periodic_stats();
          /* Arm periodic stats logging for the first time */
          std::lock_guard timer_locker(*m_timer_lock);
          arm_periodic_stats();
        }
      }));
  m_image_ctx.op_work_queue->queue(on_finish, 0);
}

template <typename I>
void ReplicatedWriteLog<I>::update_image_cache_state(Context *on_finish) {
  m_cache_state->write_image_cache_state(on_finish);
}

template <typename I>
void ReplicatedWriteLog<I>::init(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;
  perf_start(m_image_ctx.id);

  ceph_assert(!m_initialized);

  Context *ctx = new LambdaContext(
    [this, on_finish](int r) {
      if (r >= 0) {
        update_image_cache_state(on_finish);
      } else {
        on_finish->complete(r);
      }
    });

  DeferredContexts later;
  rwl_init(ctx, later);
}

template <typename I>
void ReplicatedWriteLog<I>::shut_down(Context *on_finish) {
  // TODO: This is cover in later PR.
  on_finish->complete(0);
}

template <typename I>
void ReplicatedWriteLog<I>::aio_read(Extents&& image_extents,
                                     ceph::bufferlist* bl,
                                     int fadvise_flags, Context *on_finish) {
}

template <typename I>
void ReplicatedWriteLog<I>::aio_write(Extents &&image_extents,
                                      bufferlist&& bl,
                                      int fadvise_flags,
                                      Context *on_finish) {
}

template <typename I>
void ReplicatedWriteLog<I>::aio_discard(uint64_t offset, uint64_t length,
                                        uint32_t discard_granularity_bytes, Context *on_finish) {
}

template <typename I>
void ReplicatedWriteLog<I>::aio_flush(Context *on_finish) {
}

template <typename I>
void ReplicatedWriteLog<I>::aio_writesame(uint64_t offset, uint64_t length,
                                          bufferlist&& bl, int fadvise_flags,
                                          Context *on_finish) {
}

template <typename I>
void ReplicatedWriteLog<I>::aio_compare_and_write(Extents &&image_extents,
                                                  bufferlist&& cmp_bl,
                                                  bufferlist&& bl,
                                                  uint64_t *mismatch_offset,
                                                  int fadvise_flags,
                                                  Context *on_finish) {
}

template <typename I>
void ReplicatedWriteLog<I>::flush(Context *on_finish) {
}

template <typename I>
void ReplicatedWriteLog<I>::invalidate(Context *on_finish) {
}

} // namespace cache
} // namespace librbd

template class librbd::cache::ReplicatedWriteLog<librbd::ImageCtx>;
template class librbd::cache::ImageCache<librbd::ImageCtx>;

