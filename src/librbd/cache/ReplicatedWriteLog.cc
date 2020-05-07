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

#undef dout_subsys
#define dout_subsys ceph_subsys_rbd_rwl
#undef dout_prefix
#define dout_prefix *_dout << "librbd::cache::ReplicatedWriteLog: " << this << " " \
                           <<  __func__ << ": "

namespace librbd {
namespace cache {

using namespace librbd::cache::rwl;

typedef ReplicatedWriteLog<ImageCtx>::Extent Extent;
typedef ReplicatedWriteLog<ImageCtx>::Extents Extents;

const unsigned long int OPS_APPENDED_TOGETHER = MAX_ALLOC_PER_TRANSACTION;

template <typename I>
ReplicatedWriteLog<I>::ReplicatedWriteLog(I &image_ctx, librbd::cache::rwl::ImageCacheState<I>* cache_state)
  : m_cache_state(cache_state),
    m_rwl_pool_layout_name(POBJ_LAYOUT_NAME(rbd_rwl)),
    m_image_ctx(image_ctx),
    m_log_pool_config_size(DEFAULT_POOL_SIZE),
    m_image_writeback(image_ctx), m_write_log_guard(image_ctx.cct),
    m_log_retire_lock(ceph::make_mutex(util::unique_lock_name(
      "librbd::cache::ReplicatedWriteLog::m_log_retire_lock", this))),
    m_entry_reader_lock("librbd::cache::ReplicatedWriteLog::m_entry_reader_lock"),
    m_deferred_dispatch_lock(ceph::make_mutex(util::unique_lock_name(
      "librbd::cache::ReplicatedWriteLog::m_deferred_dispatch_lock", this))),
    m_log_append_lock(ceph::make_mutex(util::unique_lock_name(
      "librbd::cache::ReplicatedWriteLog::m_log_append_lock", this))),
    m_lock(ceph::make_mutex(util::unique_lock_name(
      "librbd::cache::ReplicatedWriteLog::m_lock", this))),
    m_blockguard_lock(ceph::make_mutex(util::unique_lock_name(
      "librbd::cache::ReplicatedWriteLog::m_blockguard_lock", this))),
    m_blocks_to_log_entries(image_ctx.cct),
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
    ceph_assert(m_deferred_ios.size() == 0);
    ceph_assert(m_ops_to_flush.size() == 0);
    ceph_assert(m_ops_to_append.size() == 0);
    ceph_assert(m_flush_ops_in_flight == 0);

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
  ceph_assert(ceph_mutex_is_locked(*m_timer_lock));
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
               << ", m_current_sync_gen=" << m_current_sync_gen << dendl;
  if (m_first_free_entry == m_first_valid_entry) {
    ldout(cct,1) << "write log is empty" << dendl;
    m_cache_state->empty = true;
  }

  /* Start the sync point following the last one seen in the
   * log. Flush the last sync point created during the loading of the
   * existing log entries. */
  init_flush_new_sync_point(later);
  ldout(cct,20) << "new sync point = [" << m_current_sync_point << "]" << dendl;

  m_initialized = true;
  // Start the thread
  m_thread_pool.start();

  m_periodic_stats_enabled = m_cache_state->log_periodic_stats;
  /* Do these after we drop lock */
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
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  ldout(cct,5) << "image name: " << m_image_ctx.name << " id: " << m_image_ctx.id << dendl;

  Context *ctx = new LambdaContext(
    [this, on_finish](int r) {
      ldout(m_image_ctx.cct, 6) << "shutdown complete" << dendl;
      m_image_ctx.op_work_queue->queue(on_finish, r);
    });
  ctx = new LambdaContext(
    [this, ctx](int r) {
      Context *next_ctx = override_ctx(r, ctx);
      bool periodic_stats_enabled = m_periodic_stats_enabled;
      m_periodic_stats_enabled = false;

      if (periodic_stats_enabled) {
        /* Log stats one last time if they were enabled */
        periodic_stats();
      }
      {
        std::lock_guard locker(m_lock);
        ceph_assert(m_dirty_log_entries.size() == 0);
        m_cache_state->clean = true;
        m_log_entries.clear();
        if (m_log_pool) {
          ldout(m_image_ctx.cct, 6) << "closing pmem pool" << dendl;
          pmemobj_close(m_log_pool);
        }
        if (m_cache_state->clean) {
          if (m_log_is_poolset) {
            ldout(m_image_ctx.cct, 5) << "Not removing poolset " << m_log_pool_name << dendl;
          } else {
            ldout(m_image_ctx.cct, 5) << "Removing empty pool file: " << m_log_pool_name << dendl;
            if (remove(m_log_pool_name.c_str()) != 0) {
              lderr(m_image_ctx.cct) << "failed to remove empty pool \"" << m_log_pool_name << "\": "
                                     << pmemobj_errormsg() << dendl;
            } else {
              m_cache_state->clean = true;
              m_cache_state->empty = true;
              m_cache_state->present = false;
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
      update_image_cache_state(next_ctx);
    });
  if (m_first_free_entry == m_first_valid_entry) { //if the log entries are free.
    m_image_ctx.op_work_queue->queue(ctx, 0);
  } else {
    ctx = new LambdaContext(
      [this, ctx](int r) {
        Context *next_ctx = override_ctx(r, ctx);
        {
          /* Sync with process_writeback_dirty_entries() */
          RWLock::WLocker entry_reader_wlocker(m_entry_reader_lock);
          m_shutting_down = true;
          /* Flush all writes to OSDs (unless disabled) and wait for all
             in-progress flush writes to complete */
          ldout(m_image_ctx.cct, 6) << "flushing" << dendl;
          if (m_periodic_stats_enabled) {
            periodic_stats();
          }
        }
        flush_dirty_entries(next_ctx);
      });
    ctx = new LambdaContext(
      [this, ctx](int r) {
        Context *next_ctx = override_ctx(r, ctx);
        ldout(m_image_ctx.cct, 6) << "waiting for in flight operations" << dendl;
        // Wait for in progress IOs to complete
        next_ctx = util::create_async_context_callback(m_image_ctx, next_ctx);
        m_async_op_tracker.wait_for_ops(next_ctx);
      });
    ctx = new LambdaContext(
      [this, ctx](int r) {
        ldout(m_image_ctx.cct, 6) << "Done internal_flush in shutdown" << dendl;
        m_work_queue.queue(ctx, r);
      });
    /* Complete all in-flight writes before shutting down */
    ldout(m_image_ctx.cct, 6) << "internal_flush in shutdown" << dendl;
    internal_flush(false, ctx);
  }
}

template <typename I>
void ReplicatedWriteLog<I>::aio_read(Extents&& image_extents,
                                     ceph::bufferlist* bl,
                                     int fadvise_flags, Context *on_finish) {
  // TODO: handle writesame and discard case in later PRs
  CephContext *cct = m_image_ctx.cct;
  utime_t now = ceph_clock_now();
  C_ReadRequest *read_ctx = new C_ReadRequest(cct, now, m_perfcounter, bl, on_finish);
  ldout(cct, 20) << "name: " << m_image_ctx.name << " id: " << m_image_ctx.id
                 << "image_extents=" << image_extents << ", "
                 << "bl=" << bl << ", "
                 << "on_finish=" << on_finish << dendl;

  ceph_assert(m_initialized);
  bl->clear();
  m_perfcounter->inc(l_librbd_rwl_rd_req, 1);

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
   * The GenericWriteLogEntry references are released by the buffer destructor.
   */
  for (auto &extent : image_extents) {
    uint64_t extent_offset = 0;
    RWLock::RLocker entry_reader_locker(m_entry_reader_lock);
    WriteLogMapEntries map_entries = m_blocks_to_log_entries.find_map_entries(block_extent(extent));
    for (auto &map_entry : map_entries) {
      Extent entry_image_extent(rwl::image_extent(map_entry.block_extent));
      /* If this map entry starts after the current image extent offset ... */
      if (entry_image_extent.first > extent.first + extent_offset) {
        /* ... add range before map_entry to miss extents */
        uint64_t miss_extent_start = extent.first + extent_offset;
        uint64_t miss_extent_length = entry_image_extent.first - miss_extent_start;
        Extent miss_extent(miss_extent_start, miss_extent_length);
        read_ctx->miss_extents.push_back(miss_extent);
        /* Add miss range to read extents */
        ImageExtentBuf miss_extent_buf(miss_extent);
        read_ctx->read_extents.push_back(miss_extent_buf);
        extent_offset += miss_extent_length;
      }
      ceph_assert(entry_image_extent.first <= extent.first + extent_offset);
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
      if (0 == map_entry.log_entry->write_bytes() && 0 < map_entry.log_entry->bytes_dirty()) {
        /* discard log entry */
        auto discard_entry = map_entry.log_entry;
        ldout(cct, 20) << "read hit on discard entry: log_entry=" << *discard_entry << dendl;
        /* Discards read as zero, so we'll construct a bufferlist of zeros */
        bufferlist zero_bl;
        zero_bl.append_zero(entry_hit_length);
        /* Add hit extent to read extents */
        ImageExtentBuf hit_extent_buf(hit_extent, zero_bl);
        read_ctx->read_extents.push_back(hit_extent_buf);
      } else {
        /* write and writesame log entry */
        /* Offset of the map entry into the log entry's buffer */
        uint64_t map_entry_buffer_offset = entry_image_extent.first - map_entry.log_entry->ram_entry.image_offset_bytes;
        /* Offset into the log entry buffer of this read hit */
        uint64_t read_buffer_offset = map_entry_buffer_offset + entry_offset;
        /* Create buffer object referring to pmem pool for this read hit */
        auto write_entry = map_entry.log_entry;

        /* Make a bl for this hit extent. This will add references to the write_entry->pmem_bp */
        buffer::list hit_bl;

        buffer::list entry_bl_copy;
        write_entry->copy_pmem_bl(&entry_bl_copy);
        entry_bl_copy.begin(read_buffer_offset).copy(entry_hit_length, hit_bl);

        ceph_assert(hit_bl.length() == entry_hit_length);

        /* Add hit extent to read extents */
        ImageExtentBuf hit_extent_buf(hit_extent, hit_bl);
        read_ctx->read_extents.push_back(hit_extent_buf);
      }
      /* Exclude RWL hit range from buffer and extent */
      extent_offset += entry_hit_length;
      ldout(cct, 20) << map_entry << dendl;
    }
    /* If the last map entry didn't consume the entire image extent ... */
    if (extent.second > extent_offset) {
      /* ... add the rest of this extent to miss extents */
      uint64_t miss_extent_start = extent.first + extent_offset;
      uint64_t miss_extent_length = extent.second - extent_offset;
      Extent miss_extent(miss_extent_start, miss_extent_length);
      read_ctx->miss_extents.push_back(miss_extent);
      /* Add miss range to read extents */
      ImageExtentBuf miss_extent_buf(miss_extent);
      read_ctx->read_extents.push_back(miss_extent_buf);
      extent_offset += miss_extent_length;
    }
  }

  ldout(cct, 20) << "miss_extents=" << read_ctx->miss_extents << ", "
                 << "miss_bl=" << read_ctx->miss_bl << dendl;

  if (read_ctx->miss_extents.empty()) {
    /* All of this read comes from RWL */
    read_ctx->complete(0);
  } else {
    /* Pass the read misses on to the layer below RWL */
    m_image_writeback.aio_read(std::move(read_ctx->miss_extents), &read_ctx->miss_bl, fadvise_flags, read_ctx);
  }
}

template <typename I>
void ReplicatedWriteLog<I>::aio_write(Extents &&image_extents,
                                      bufferlist&& bl,
                                      int fadvise_flags,
                                      Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 20) << "aio_write" << dendl;

  utime_t now = ceph_clock_now();
  m_perfcounter->inc(l_librbd_rwl_wr_req, 1);

  ceph_assert(m_initialized);

  auto *write_req =
    new C_WriteRequestT(*this, now, std::move(image_extents), std::move(bl), fadvise_flags,
                        m_lock, m_perfcounter, on_finish);
  m_perfcounter->inc(l_librbd_rwl_wr_bytes, write_req->image_extents_summary.total_bytes);

  /* The lambda below will be called when the block guard for all
   * blocks affected by this write is obtained */
  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext([this, write_req](GuardedRequestFunctionContext &guard_ctx) {
      write_req->blockguard_acquired(guard_ctx);
      alloc_and_dispatch_io_req(write_req);
    });

  detain_guarded_request(write_req, guarded_ctx, false);
}

template <typename I>
void ReplicatedWriteLog<I>::aio_discard(uint64_t offset, uint64_t length,
                                        uint32_t discard_granularity_bytes,
                                        Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 20) << dendl;

  utime_t now = ceph_clock_now();
  m_perfcounter->inc(l_librbd_rwl_discard, 1);
  Extents discard_extents = {{offset, length}};

  ceph_assert(m_initialized);

  auto *discard_req =
    new C_DiscardRequestT(*this, now, std::move(discard_extents), discard_granularity_bytes,
                          m_lock, m_perfcounter, on_finish);

  /* The lambda below will be called when the block guard for all
   * blocks affected by this write is obtained */
  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext([this, discard_req](GuardedRequestFunctionContext &guard_ctx) {
      discard_req->blockguard_acquired(guard_ctx);
      alloc_and_dispatch_io_req(discard_req);
    });

  detain_guarded_request(discard_req, guarded_ctx, false);
}

/**
 * Aio_flush completes when all previously completed writes are
 * flushed to persistent cache. We make a best-effort attempt to also
 * defer until all in-progress writes complete, but we may not know
 * about all of the writes the application considers in-progress yet,
 * due to uncertainty in the IO submission workq (multiple WQ threads
 * may allow out-of-order submission).
 *
 * This flush operation will not wait for writes deferred for overlap
 * in the block guard.
 */
template <typename I>
void ReplicatedWriteLog<I>::aio_flush(io::FlushSource flush_source, Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << "on_finish=" << on_finish << " flush_source=" << flush_source << dendl;

  if (io::FLUSH_SOURCE_SHUTDOWN == flush_source || io::FLUSH_SOURCE_INTERNAL == flush_source) {
    internal_flush(false, on_finish);
    return;
  }
  m_perfcounter->inc(l_librbd_rwl_aio_flush, 1);

  /* May be called even if initialization fails */
  if (!m_initialized) {
    ldout(cct, 05) << "never initialized" << dendl;
    /* Deadlock if completed here */
    m_image_ctx.op_work_queue->queue(on_finish, 0);
    return;
  }

  {
    std::shared_lock image_locker(m_image_ctx.image_lock);
    if (m_image_ctx.snap_id != CEPH_NOSNAP || m_image_ctx.read_only) {
      on_finish->complete(-EROFS);
      return;
    }
  }

  auto flush_req = make_flush_req(on_finish);

  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext([this, flush_req](GuardedRequestFunctionContext &guard_ctx) {
      ldout(m_image_ctx.cct, 20) << "flush_req=" << flush_req << " cell=" << guard_ctx.cell << dendl;
      ceph_assert(guard_ctx.cell);
      flush_req->detained = guard_ctx.state.detained;
      /* We don't call flush_req->set_cell(), because the block guard will be released here */
      {
        DeferredContexts post_unlock; /* Do these when the lock below is released */
        std::lock_guard locker(m_lock);

        if (!m_persist_on_flush && m_persist_on_write_until_flush) {
          m_persist_on_flush = true;
          ldout(m_image_ctx.cct, 5) << "now persisting on flush" << dendl;
        }

        /*
         * Create a new sync point if there have been writes since the last
         * one.
         *
         * We do not flush the caches below the RWL here.
         */
        flush_new_sync_point_if_needed(flush_req, post_unlock);
      }

      release_guarded_request(guard_ctx.cell);
    });

  detain_guarded_request(flush_req, guarded_ctx, true);
}

template <typename I>
void ReplicatedWriteLog<I>::aio_writesame(uint64_t offset, uint64_t length,
                                          bufferlist&& bl, int fadvise_flags,
                                          Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 20) << "aio_writesame" << dendl;

  utime_t now = ceph_clock_now();
  Extents ws_extents = {{offset, length}};
  m_perfcounter->inc(l_librbd_rwl_ws, 1);
  ceph_assert(m_initialized);

  /* A write same request is also a write request. The key difference is the
   * write same data buffer is shorter than the extent of the request. The full
   * extent will be used in the block guard, and appear in
   * m_blocks_to_log_entries_map. The data buffer allocated for the WS is only
   * as long as the length of the bl here, which is the pattern that's repeated
   * in the image for the entire length of this WS. Read hits and flushing of
   * write sames are different than normal writes. */
  auto *ws_req =
    new C_WriteSameRequestT(*this, now, std::move(ws_extents), std::move(bl),
                            fadvise_flags, m_lock, m_perfcounter, on_finish);
  m_perfcounter->inc(l_librbd_rwl_ws_bytes, ws_req->image_extents_summary.total_bytes);

  /* The lambda below will be called when the block guard for all
   * blocks affected by this write is obtained */
  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext([this, ws_req](GuardedRequestFunctionContext &guard_ctx) {
      ws_req->blockguard_acquired(guard_ctx);
      alloc_and_dispatch_io_req(ws_req);
    });

  detain_guarded_request(ws_req, guarded_ctx, false);
}

template <typename I>
void ReplicatedWriteLog<I>::aio_compare_and_write(Extents &&image_extents,
                                                  bufferlist&& cmp_bl,
                                                  bufferlist&& bl,
                                                  uint64_t *mismatch_offset,
                                                  int fadvise_flags,
                                                  Context *on_finish) {
  ldout(m_image_ctx.cct, 20) << dendl;

  utime_t now = ceph_clock_now();
  m_perfcounter->inc(l_librbd_rwl_cmp, 1);
  ceph_assert(m_initialized);

  /* A compare and write request is also a write request. We only allocate
   * resources and dispatch this write request if the compare phase
   * succeeds. */
  auto *cw_req =
    new C_CompAndWriteRequestT(*this, now, std::move(image_extents), std::move(cmp_bl), std::move(bl),
                               mismatch_offset, fadvise_flags, m_lock, m_perfcounter, on_finish);
  m_perfcounter->inc(l_librbd_rwl_cmp_bytes, cw_req->image_extents_summary.total_bytes);

  /* The lambda below will be called when the block guard for all
   * blocks affected by this write is obtained */
  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext([this, cw_req](GuardedRequestFunctionContext &guard_ctx) {
      cw_req->blockguard_acquired(guard_ctx);

      auto read_complete_ctx = new LambdaContext(
        [this, cw_req](int r) {
          ldout(m_image_ctx.cct, 20) << "name: " << m_image_ctx.name << " id: " << m_image_ctx.id
                                     << "cw_req=" << cw_req << dendl;

          /* Compare read_bl to cmp_bl to determine if this will produce a write */
          if (cw_req->cmp_bl.contents_equal(cw_req->read_bl)) {
            /* Compare phase succeeds. Begin write */
            ldout(m_image_ctx.cct, 5) << " cw_req=" << cw_req << " compare matched" << dendl;
            cw_req->compare_succeeded = true;
            *cw_req->mismatch_offset = 0;
            /* Continue with this request as a write. Blockguard release and
             * user request completion handled as if this were a plain
             * write. */
            alloc_and_dispatch_io_req(cw_req);
          } else {
            /* Compare phase fails. Comp-and write ends now. */
            ldout(m_image_ctx.cct, 15) << " cw_req=" << cw_req << " compare failed" << dendl;
            /* Bufferlist doesn't tell us where they differed, so we'll have to determine that here */
            ceph_assert(cw_req->read_bl.length() == cw_req->cmp_bl.length());
            uint64_t bl_index = 0;
            for (bl_index = 0; bl_index < cw_req->cmp_bl.length(); bl_index++) {
              if (cw_req->cmp_bl[bl_index] != cw_req->read_bl[bl_index]) {
                ldout(m_image_ctx.cct, 15) << " cw_req=" << cw_req << " mismatch at " << bl_index << dendl;
                break;
              }
            }
            cw_req->compare_succeeded = false;
            *cw_req->mismatch_offset = bl_index;
            cw_req->complete_user_request(-EILSEQ);
            cw_req->release_cell();
            cw_req->complete(0);
          }
        });

      /* Read phase of comp-and-write must read through RWL */
      Extents image_extents_copy = cw_req->image_extents;
      aio_read(std::move(image_extents_copy), &cw_req->read_bl, cw_req->fadvise_flags, read_complete_ctx);
    });

  detain_guarded_request(cw_req, guarded_ctx, false);
}

template <typename I>
void ReplicatedWriteLog<I>::flush(Context *on_finish) {
  internal_flush(false, on_finish);
}

template <typename I>
void ReplicatedWriteLog<I>::invalidate(Context *on_finish) {
  internal_flush(true, on_finish);
}

template <typename I>
CephContext *ReplicatedWriteLog<I>::get_context() {
  return m_image_ctx.cct;
}

template <typename I>
BlockGuardCell* ReplicatedWriteLog<I>::detain_guarded_request_helper(GuardedRequest &req)
{
  CephContext *cct = m_image_ctx.cct;
  BlockGuardCell *cell;

  ceph_assert(ceph_mutex_is_locked_by_me(m_blockguard_lock));
  ldout(cct, 20) << dendl;

  int r = m_write_log_guard.detain(req.block_extent, &req, &cell);
  ceph_assert(r>=0);
  if (r > 0) {
    ldout(cct, 20) << "detaining guarded request due to in-flight requests: "
                   << "req=" << req << dendl;
    return nullptr;
  }

  ldout(cct, 20) << "in-flight request cell: " << cell << dendl;
  return cell;
}

template <typename I>
BlockGuardCell* ReplicatedWriteLog<I>::detain_guarded_request_barrier_helper(
  GuardedRequest &req)
{
  BlockGuardCell *cell = nullptr;

  ceph_assert(ceph_mutex_is_locked_by_me(m_blockguard_lock));
  ldout(m_image_ctx.cct, 20) << dendl;

  if (m_barrier_in_progress) {
    req.guard_ctx->state.queued = true;
    m_awaiting_barrier.push_back(req);
  } else {
    bool barrier = req.guard_ctx->state.barrier;
    if (barrier) {
      m_barrier_in_progress = true;
      req.guard_ctx->state.current_barrier = true;
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
void ReplicatedWriteLog<I>::detain_guarded_request(
  C_BlockIORequestT *request,
  GuardedRequestFunctionContext *guarded_ctx,
  bool is_barrier)
{
  BlockExtent extent;
  if (request) {
    extent = request->image_extents_summary.block_extent();
  } else {
    extent = block_extent(whole_volume_extent());
  }
  auto req = GuardedRequest(extent, guarded_ctx, is_barrier);
  BlockGuardCell *cell = nullptr;

  ldout(m_image_ctx.cct, 20) << dendl;
  {
    std::lock_guard locker(m_blockguard_lock);
    cell = detain_guarded_request_barrier_helper(req);
  }
  if (cell) {
    req.guard_ctx->cell = cell;
    req.guard_ctx->complete(0);
  }
}

template <typename I>
void ReplicatedWriteLog<I>::release_guarded_request(BlockGuardCell *released_cell)
{
  CephContext *cct = m_image_ctx.cct;
  WriteLogGuard::BlockOperations block_reqs;
  ldout(cct, 20) << "released_cell=" << released_cell << dendl;

  {
    std::lock_guard locker(m_blockguard_lock);
    m_write_log_guard.release(released_cell, &block_reqs);

    for (auto &req : block_reqs) {
      req.guard_ctx->state.detained = true;
      BlockGuardCell *detained_cell = detain_guarded_request_helper(req);
      if (detained_cell) {
        if (req.guard_ctx->state.current_barrier) {
          /* The current barrier is acquiring the block guard, so now we know its cell */
          m_barrier_cell = detained_cell;
          /* detained_cell could be == released_cell here */
          ldout(cct, 20) << "current barrier cell=" << detained_cell << " req=" << req << dendl;
        }
        req.guard_ctx->cell = detained_cell;
        m_work_queue.queue(req.guard_ctx);
      }
    }

    if (m_barrier_in_progress && (released_cell == m_barrier_cell)) {
      ldout(cct, 20) << "current barrier released cell=" << released_cell << dendl;
      /* The released cell is the current barrier request */
      m_barrier_in_progress = false;
      m_barrier_cell = nullptr;
      /* Move waiting requests into the blockguard. Stop if there's another barrier */
      while (!m_barrier_in_progress && !m_awaiting_barrier.empty()) {
        auto &req = m_awaiting_barrier.front();
        ldout(cct, 20) << "submitting queued request to blockguard: " << req << dendl;
        BlockGuardCell *detained_cell = detain_guarded_request_barrier_helper(req);
        if (detained_cell) {
          req.guard_ctx->cell = detained_cell;
          m_work_queue.queue(req.guard_ctx);
        }
        m_awaiting_barrier.pop_front();
      }
    }
  }

  ldout(cct, 20) << "exit" << dendl;
}

/*
 * Performs the log event append operation for all of the scheduled
 * events.
 */
template <typename I>
void ReplicatedWriteLog<I>::append_scheduled_ops(void)
{
  GenericLogOperations ops;
  int append_result = 0;
  bool ops_remain = false;
  bool appending = false; /* true if we set m_appending */
  ldout(m_image_ctx.cct, 20) << dendl;
  do {
    ops.clear();

    {
      std::lock_guard locker(m_lock);
      if (!appending && m_appending) {
        /* Another thread is appending */
        ldout(m_image_ctx.cct, 15) << "Another thread is appending" << dendl;
        return;
      }
      if (m_ops_to_append.size()) {
        appending = true;
        m_appending = true;
        auto last_in_batch = m_ops_to_append.begin();
        unsigned int ops_to_append = m_ops_to_append.size();
        if (ops_to_append > OPS_APPENDED_TOGETHER) {
          ops_to_append = OPS_APPENDED_TOGETHER;
        }
        std::advance(last_in_batch, ops_to_append);
        ops.splice(ops.end(), m_ops_to_append, m_ops_to_append.begin(), last_in_batch);
        ops_remain = true; /* Always check again before leaving */
        ldout(m_image_ctx.cct, 20) << "appending " << ops.size() << ", "
                                   << m_ops_to_append.size() << " remain" << dendl;
      } else {
        ops_remain = false;
        if (appending) {
          appending = false;
          m_appending = false;
        }
      }
    }

    if (ops.size()) {
      std::lock_guard locker(m_log_append_lock);
      alloc_op_log_entries(ops);
      append_result = append_op_log_entries(ops);
    }

    int num_ops = ops.size();
    if (num_ops) {
      /* New entries may be flushable. Completion will wake up flusher. */
      complete_op_log_entries(std::move(ops), append_result);
    }
  } while (ops_remain);
}

template <typename I>
void ReplicatedWriteLog<I>::enlist_op_appender()
{
  m_async_append_ops++;
  m_async_op_tracker.start_op();
  Context *append_ctx = new LambdaContext([this](int r) {
      append_scheduled_ops();
      m_async_append_ops--;
      m_async_op_tracker.finish_op();
    });
  m_work_queue.queue(append_ctx);
}

/*
 * Takes custody of ops. They'll all get their log entries appended,
 * and have their on_write_persist contexts completed once they and
 * all prior log entries are persisted everywhere.
 */
template <typename I>
void ReplicatedWriteLog<I>::schedule_append(GenericLogOperations &ops)
{
  bool need_finisher;
  GenericLogOperationsVector appending;

  std::copy(std::begin(ops), std::end(ops), std::back_inserter(appending));
  {
    std::lock_guard locker(m_lock);

    need_finisher = m_ops_to_append.empty() && !m_appending;
    m_ops_to_append.splice(m_ops_to_append.end(), ops);
  }

  if (need_finisher) {
    enlist_op_appender();
  }

  for (auto &op : appending) {
    op->appending();
  }
}

template <typename I>
void ReplicatedWriteLog<I>::schedule_append(GenericLogOperationsVector &ops)
{
  GenericLogOperations to_append(ops.begin(), ops.end());

  schedule_append(to_append);
}

template <typename I>
void ReplicatedWriteLog<I>::schedule_append(GenericLogOperationSharedPtr op)
{
  GenericLogOperations to_append { op };

  schedule_append(to_append);
}

const unsigned long int ops_flushed_together = 4;
/*
 * Performs the pmem buffer flush on all scheduled ops, then schedules
 * the log event append operation for all of them.
 */
template <typename I>
void ReplicatedWriteLog<I>::flush_then_append_scheduled_ops(void)
{
  GenericLogOperations ops;
  bool ops_remain = false;
  ldout(m_image_ctx.cct, 20) << dendl;
  do {
    {
      ops.clear();
      std::lock_guard locker(m_lock);
      if (m_ops_to_flush.size()) {
        auto last_in_batch = m_ops_to_flush.begin();
        unsigned int ops_to_flush = m_ops_to_flush.size();
        if (ops_to_flush > ops_flushed_together) {
          ops_to_flush = ops_flushed_together;
        }
        ldout(m_image_ctx.cct, 20) << "should flush " << ops_to_flush << dendl;
        std::advance(last_in_batch, ops_to_flush);
        ops.splice(ops.end(), m_ops_to_flush, m_ops_to_flush.begin(), last_in_batch);
        ops_remain = !m_ops_to_flush.empty();
        ldout(m_image_ctx.cct, 20) << "flushing " << ops.size() << ", "
                                   << m_ops_to_flush.size() << " remain" << dendl;
      } else {
        ops_remain = false;
      }
    }
    if (ops_remain) {
      enlist_op_flusher();
    }

    /* Ops subsequently scheduled for flush may finish before these,
     * which is fine. We're unconcerned with completion order until we
     * get to the log message append step. */
    if (ops.size()) {
      flush_pmem_buffer(ops);
      schedule_append(ops);
    }
  } while (ops_remain);
  append_scheduled_ops();
}

template <typename I>
void ReplicatedWriteLog<I>::enlist_op_flusher()
{
  m_async_flush_ops++;
  m_async_op_tracker.start_op();
  Context *flush_ctx = new LambdaContext([this](int r) {
      flush_then_append_scheduled_ops();
      m_async_flush_ops--;
      m_async_op_tracker.finish_op();
    });
  m_work_queue.queue(flush_ctx);
}

/*
 * Takes custody of ops. They'll all get their pmem blocks flushed,
 * then get their log entries appended.
 */
template <typename I>
void ReplicatedWriteLog<I>::schedule_flush_and_append(GenericLogOperationsVector &ops)
{
  GenericLogOperations to_flush(ops.begin(), ops.end());
  bool need_finisher;
  ldout(m_image_ctx.cct, 20) << dendl;
  {
    std::lock_guard locker(m_lock);

    need_finisher = m_ops_to_flush.empty();
    m_ops_to_flush.splice(m_ops_to_flush.end(), to_flush);
  }

  if (need_finisher) {
    enlist_op_flusher();
  }
}

/*
 * Flush the pmem regions for the data blocks of a set of operations
 *
 * V is expected to be GenericLogOperations<I>, or GenericLogOperationsVector<I>
 */
template <typename I>
template <typename V>
void ReplicatedWriteLog<I>::flush_pmem_buffer(V& ops)
{
  for (auto &operation : ops) {
    operation->flush_pmem_buf_to_cache(m_log_pool);
  }

  /* Drain once for all */
  pmemobj_drain(m_log_pool);

  utime_t now = ceph_clock_now();
  for (auto &operation : ops) {
    if (operation->reserved_allocated()) {
      operation->buf_persist_comp_time = now;
    } else {
      ldout(m_image_ctx.cct, 20) << "skipping non-write op: " << *operation << dendl;
    }
  }
}

/*
 * Allocate the (already reserved) write log entries for a set of operations.
 *
 * Locking:
 * Acquires lock
 */
template <typename I>
void ReplicatedWriteLog<I>::alloc_op_log_entries(GenericLogOperations &ops)
{
  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);
  struct WriteLogPmemEntry *pmem_log_entries = D_RW(D_RW(pool_root)->log_entries);

  ceph_assert(ceph_mutex_is_locked_by_me(m_log_append_lock));

  /* Allocate the (already reserved) log entries */
  std::lock_guard locker(m_lock);

  for (auto &operation : ops) {
    uint32_t entry_index = m_first_free_entry;
    m_first_free_entry = (m_first_free_entry + 1) % m_total_log_entries;
    auto &log_entry = operation->get_log_entry();
    log_entry->log_entry_index = entry_index;
    log_entry->ram_entry.entry_index = entry_index;
    log_entry->pmem_entry = &pmem_log_entries[entry_index];
    log_entry->ram_entry.entry_valid = 1;
    m_log_entries.push_back(log_entry);
    ldout(m_image_ctx.cct, 20) << "operation=[" << *operation << "]" << dendl;
  }
}

/*
 * Flush the persistent write log entries set of ops. The entries must
 * be contiguous in persistent memory.
 */
template <typename I>
void ReplicatedWriteLog<I>::flush_op_log_entries(GenericLogOperationsVector &ops)
{
  if (ops.empty()) {
    return;
  }

  if (ops.size() > 1) {
    ceph_assert(ops.front()->get_log_entry()->pmem_entry < ops.back()->get_log_entry()->pmem_entry);
  }

  ldout(m_image_ctx.cct, 20) << "entry count=" << ops.size() << " "
                             << "start address="
                             << ops.front()->get_log_entry()->pmem_entry << " "
                             << "bytes="
                             << ops.size() * sizeof(*(ops.front()->get_log_entry()->pmem_entry))
                             << dendl;
  pmemobj_flush(m_log_pool,
                ops.front()->get_log_entry()->pmem_entry,
                ops.size() * sizeof(*(ops.front()->get_log_entry()->pmem_entry)));
}

/*
 * Write and persist the (already allocated) write log entries and
 * data buffer allocations for a set of ops. The data buffer for each
 * of these must already have been persisted to its reserved area.
 */
template <typename I>
int ReplicatedWriteLog<I>::append_op_log_entries(GenericLogOperations &ops)
{
  CephContext *cct = m_image_ctx.cct;
  GenericLogOperationsVector entries_to_flush;
  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);
  int ret = 0;

  ceph_assert(ceph_mutex_is_locked_by_me(m_log_append_lock));

  if (ops.empty()) {
    return 0;
  }
  entries_to_flush.reserve(OPS_APPENDED_TOGETHER);

  /* Write log entries to ring and persist */
  utime_t now = ceph_clock_now();
  for (auto &operation : ops) {
    if (!entries_to_flush.empty()) {
      /* Flush these and reset the list if the current entry wraps to the
       * tail of the ring */
      if (entries_to_flush.back()->get_log_entry()->log_entry_index >
          operation->get_log_entry()->log_entry_index) {
        ldout(m_image_ctx.cct, 20) << "entries to flush wrap around the end of the ring at "
                                   << "operation=[" << *operation << "]" << dendl;
        flush_op_log_entries(entries_to_flush);
        entries_to_flush.clear();
        now = ceph_clock_now();
      }
    }
    ldout(m_image_ctx.cct, 20) << "Copying entry for operation at index="
                               << operation->get_log_entry()->log_entry_index << " "
                               << "from " << &operation->get_log_entry()->ram_entry << " "
                               << "to " << operation->get_log_entry()->pmem_entry << " "
                               << "operation=[" << *operation << "]" << dendl;
    ldout(m_image_ctx.cct, 05) << "APPENDING: index="
                               << operation->get_log_entry()->log_entry_index << " "
                               << "operation=[" << *operation << "]" << dendl;
    operation->log_append_time = now;
    *operation->get_log_entry()->pmem_entry = operation->get_log_entry()->ram_entry;
    ldout(m_image_ctx.cct, 20) << "APPENDING: index="
                               << operation->get_log_entry()->log_entry_index << " "
                               << "pmem_entry=[" << *operation->get_log_entry()->pmem_entry
                               << "]" << dendl;
    entries_to_flush.push_back(operation);
  }
  flush_op_log_entries(entries_to_flush);

  /* Drain once for all */
  pmemobj_drain(m_log_pool);

  /*
   * Atomically advance the log head pointer and publish the
   * allocations for all the data buffers they refer to.
   */
  utime_t tx_start = ceph_clock_now();
  TX_BEGIN(m_log_pool) {
    D_RW(pool_root)->first_free_entry = m_first_free_entry;
    for (auto &operation : ops) {
      if (operation->reserved_allocated()) {
        auto write_op = (std::shared_ptr<WriteLogOperation>&) operation;
        pmemobj_tx_publish(&write_op->buffer_alloc->buffer_alloc_action, 1);
      } else {
        ldout(m_image_ctx.cct, 20) << "skipping non-write op: " << *operation << dendl;
      }
    }
  } TX_ONCOMMIT {
  } TX_ONABORT {
    lderr(cct) << "failed to commit " << ops.size()
               << " log entries (" << m_log_pool_name << ")" << dendl;
    ceph_assert(false);
    ret = -EIO;
  } TX_FINALLY {
  } TX_END;

  utime_t tx_end = ceph_clock_now();
  m_perfcounter->tinc(l_librbd_rwl_append_tx_t, tx_end - tx_start);
  m_perfcounter->hinc(
    l_librbd_rwl_append_tx_t_hist, utime_t(tx_end - tx_start).to_nsec(), ops.size());
  for (auto &operation : ops) {
    operation->log_append_comp_time = tx_end;
  }

  return ret;
}

/*
 * Complete a set of write ops with the result of append_op_entries.
 */
template <typename I>
void ReplicatedWriteLog<I>::complete_op_log_entries(GenericLogOperations &&ops,
                                                    const int result)
{
  GenericLogEntries dirty_entries;
  int published_reserves = 0;
  ldout(m_image_ctx.cct, 20) << __func__ << ": completing" << dendl;
  for (auto &op : ops) {
    utime_t now = ceph_clock_now();
    auto log_entry = op->get_log_entry();
    log_entry->completed = true;
    if (op->is_writing_op()) {
      op->mark_log_entry_completed();
      dirty_entries.push_back(log_entry);
    }
    if (op->reserved_allocated()) {
      published_reserves++;
    }
    op->complete(result);
    m_perfcounter->tinc(l_librbd_rwl_log_op_dis_to_app_t,
                        op->log_append_time - op->dispatch_time);
    m_perfcounter->tinc(l_librbd_rwl_log_op_dis_to_cmp_t, now - op->dispatch_time);
    m_perfcounter->hinc(l_librbd_rwl_log_op_dis_to_cmp_t_hist,
                        utime_t(now - op->dispatch_time).to_nsec(),
                        log_entry->ram_entry.write_bytes);
    utime_t app_lat = op->log_append_comp_time - op->log_append_time;
    m_perfcounter->tinc(l_librbd_rwl_log_op_app_to_appc_t, app_lat);
    m_perfcounter->hinc(l_librbd_rwl_log_op_app_to_appc_t_hist, app_lat.to_nsec(),
                      log_entry->ram_entry.write_bytes);
    m_perfcounter->tinc(l_librbd_rwl_log_op_app_to_cmp_t, now - op->log_append_time);
  }

  {
    std::lock_guard locker(m_lock);
    m_unpublished_reserves -= published_reserves;
    m_dirty_log_entries.splice(m_dirty_log_entries.end(), dirty_entries);

    /* New entries may be flushable */
    wake_up();
  }
}

/**
 * Dispatch as many deferred writes as possible
 */
template <typename I>
void ReplicatedWriteLog<I>::dispatch_deferred_writes(void)
{
  C_BlockIORequestT *front_req = nullptr;     /* req still on front of deferred list */
  C_BlockIORequestT *allocated_req = nullptr; /* req that was allocated, and is now off the list */
  bool allocated = false; /* front_req allocate succeeded */
  bool cleared_dispatching_flag = false;

  /* If we can't become the dispatcher, we'll exit */
  {
    std::lock_guard locker(m_lock);
    if (m_dispatching_deferred_ops ||
        !m_deferred_ios.size()) {
      return;
    }
    m_dispatching_deferred_ops = true;
  }

  /* There are ops to dispatch, and this should be the only thread dispatching them */
  {
    std::lock_guard deferred_dispatch(m_deferred_dispatch_lock);
    do {
      {
        std::lock_guard locker(m_lock);
        ceph_assert(m_dispatching_deferred_ops);
        if (allocated) {
          /* On the 2..n-1 th time we get lock, front_req->alloc_resources() will
           * have succeeded, and we'll need to pop it off the deferred ops list
           * here. */
          ceph_assert(front_req);
          ceph_assert(!allocated_req);
          m_deferred_ios.pop_front();
          allocated_req = front_req;
          front_req = nullptr;
          allocated = false;
        }
        ceph_assert(!allocated);
        if (!allocated && front_req) {
          /* front_req->alloc_resources() failed on the last iteration. We'll stop dispatching. */
          front_req = nullptr;
          ceph_assert(!cleared_dispatching_flag);
          m_dispatching_deferred_ops = false;
          cleared_dispatching_flag = true;
        } else {
          ceph_assert(!front_req);
          if (m_deferred_ios.size()) {
            /* New allocation candidate */
            front_req = m_deferred_ios.front();
          } else {
            ceph_assert(!cleared_dispatching_flag);
            m_dispatching_deferred_ops = false;
            cleared_dispatching_flag = true;
          }
        }
      }
      /* Try allocating for front_req before we decide what to do with allocated_req
       * (if any) */
      if (front_req) {
        ceph_assert(!cleared_dispatching_flag);
        allocated = front_req->alloc_resources();
      }
      if (allocated_req && front_req && allocated) {
        /* Push dispatch of the first allocated req to a wq */
        m_work_queue.queue(new LambdaContext(
          [this, allocated_req](int r) {
            allocated_req->dispatch();
          }), 0);
        allocated_req = nullptr;
      }
      ceph_assert(!(allocated_req && front_req && allocated));

      /* Continue while we're still considering the front of the deferred ops list */
    } while (front_req);
    ceph_assert(!allocated);
  }
  ceph_assert(cleared_dispatching_flag);

  /* If any deferred requests were allocated, the last one will still be in allocated_req */
  if (allocated_req) {
    allocated_req->dispatch();
  }
}

/**
 * Returns the lanes used by this write, and attempts to dispatch the next
 * deferred write
 */
template <typename I>
void ReplicatedWriteLog<I>::release_write_lanes(C_BlockIORequestT *req)
{
  {
    std::lock_guard locker(m_lock);
    m_free_lanes += req->image_extents.size();
  }
  dispatch_deferred_writes();
}

/**
 * Attempts to allocate log resources for a write. Write is dispatched if
 * resources are available, or queued if they aren't.
 */
template <typename I>
void ReplicatedWriteLog<I>::alloc_and_dispatch_io_req(C_BlockIORequestT *req)
{
  bool dispatch_here = false;

  {
    /* If there are already deferred writes, queue behind them for resources */
    {
      std::lock_guard locker(m_lock);
      dispatch_here = m_deferred_ios.empty();
    }
    if (dispatch_here) {
      dispatch_here = req->alloc_resources();
    }
    if (dispatch_here) {
      ldout(m_image_ctx.cct, 20) << "dispatching" << dendl;
      req->dispatch();
    } else {
      req->deferred();
      {
        std::lock_guard locker(m_lock);
        m_deferred_ios.push_back(req);
      }
      ldout(m_image_ctx.cct, 20) << "deferred IOs: " << m_deferred_ios.size() << dendl;
      dispatch_deferred_writes();
    }
  }
}

template <typename I>
bool ReplicatedWriteLog<I>::alloc_resources(C_BlockIORequestT *req) {
  bool alloc_succeeds = true;
  bool no_space = false;
  uint64_t bytes_allocated = 0;
  uint64_t bytes_cached = 0;
  uint64_t bytes_dirtied = 0;
  uint64_t num_lanes = 0;
  uint64_t num_unpublished_reserves = 0;
  uint64_t num_log_entries = 0;

  // Setup buffer, and get all the number of required resources
  req->setup_buffer_resources(bytes_cached, bytes_dirtied, bytes_allocated,
                              num_lanes, num_log_entries, num_unpublished_reserves);

  {
    std::lock_guard locker(m_lock);
    if (m_free_lanes < num_lanes) {
      req->set_io_waited_for_lanes(true);
      ldout(m_image_ctx.cct, 20) << "not enough free lanes (need "
                                 <<  num_lanes
                                 << ", have " << m_free_lanes << ") "
                                 << *req << dendl;
      alloc_succeeds = false;
      /* This isn't considered a "no space" alloc fail. Lanes are a throttling mechanism. */
    }
    if (m_free_log_entries < num_log_entries) {
      req->set_io_waited_for_entries(true);
      ldout(m_image_ctx.cct, 20) << "not enough free entries (need "
                                 << num_log_entries
                                 << ", have " << m_free_log_entries << ") "
                                 << *req << dendl;
      alloc_succeeds = false;
      no_space = true; /* Entries must be retired */
    }
    /* Don't attempt buffer allocate if we've exceeded the "full" threshold */
    if (m_bytes_allocated + bytes_allocated > m_bytes_allocated_cap) {
      if (!req->has_io_waited_for_buffers()) {
        req->set_io_waited_for_entries(true);
        ldout(m_image_ctx.cct, 1) << "Waiting for allocation cap (cap="
                                  << m_bytes_allocated_cap
                                  << ", allocated=" << m_bytes_allocated
                                  << ") in write [" << *req << "]" << dendl;
      }
      alloc_succeeds = false;
      no_space = true; /* Entries must be retired */
    }
  }

  std::vector<WriteBufferAllocation>& buffers = req->get_resources_buffers();
  if (alloc_succeeds) {
    for (auto &buffer : buffers) {
      utime_t before_reserve = ceph_clock_now();
      buffer.buffer_oid = pmemobj_reserve(m_log_pool,
                                          &buffer.buffer_alloc_action,
                                          buffer.allocation_size,
                                          0 /* Object type */);
      buffer.allocation_lat = ceph_clock_now() - before_reserve;
      if (TOID_IS_NULL(buffer.buffer_oid)) {
        if (!req->has_io_waited_for_buffers()) {
          req->set_io_waited_for_entries(true);
        }
        ldout(m_image_ctx.cct, 5) << "can't allocate all data buffers: "
                                  << pmemobj_errormsg() << ". "
                                  << *req << dendl;
        alloc_succeeds = false;
        no_space = true; /* Entries need to be retired */
        break;
      } else {
        buffer.allocated = true;
      }
      ldout(m_image_ctx.cct, 20) << "Allocated " << buffer.buffer_oid.oid.pool_uuid_lo
                                 << "." << buffer.buffer_oid.oid.off
                                 << ", size=" << buffer.allocation_size << dendl;
    }
  }

  if (alloc_succeeds) {
    std::lock_guard locker(m_lock);
    /* We need one free log entry per extent (each is a separate entry), and
     * one free "lane" for remote replication. */
    if ((m_free_lanes >= num_lanes) &&
        (m_free_log_entries >= num_log_entries)) {
      m_free_lanes -= num_lanes;
      m_free_log_entries -= num_log_entries;
      m_unpublished_reserves += num_unpublished_reserves;
      m_bytes_allocated += bytes_allocated;
      m_bytes_cached += bytes_cached;
      m_bytes_dirty += bytes_dirtied;
    } else {
      alloc_succeeds = false;
    }
  }

  if (!alloc_succeeds) {
    /* On alloc failure, free any buffers we did allocate */
    for (auto &buffer : buffers) {
      if (buffer.allocated) {
        pmemobj_cancel(m_log_pool, &buffer.buffer_alloc_action, 1);
      }
    }
    if (no_space) {
      /* Expedite flushing and/or retiring */
      std::lock_guard locker(m_lock);
      m_alloc_failed_since_retire = true;
      m_last_alloc_fail = ceph_clock_now();
    }
  }

  req->set_allocated(alloc_succeeds);

  return alloc_succeeds;
}

template <typename I>
C_FlushRequest<ReplicatedWriteLog<I>>* ReplicatedWriteLog<I>::make_flush_req(Context *on_finish) {
  utime_t flush_begins = ceph_clock_now();
  bufferlist bl;
  auto *flush_req =
    new C_FlushRequestT(*this, flush_begins, Extents({whole_volume_extent()}),
                        std::move(bl), 0, m_lock, m_perfcounter, on_finish);

  return flush_req;
}

template <typename I>
void ReplicatedWriteLog<I>::wake_up() {
  CephContext *cct = m_image_ctx.cct;
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  if (!m_wake_up_enabled) {
    // wake_up is disabled during shutdown after flushing completes
    ldout(m_image_ctx.cct, 6) << "deferred processing disabled" << dendl;
    return;
  }

  if (m_wake_up_requested && m_wake_up_scheduled) {
    return;
  }

  ldout(cct, 20) << dendl;

  /* Wake-up can be requested while it's already scheduled */
  m_wake_up_requested = true;

  /* Wake-up cannot be scheduled if it's already scheduled */
  if (m_wake_up_scheduled) {
    return;
  }
  m_wake_up_scheduled = true;
  m_async_process_work++;
  m_async_op_tracker.start_op();
  m_work_queue.queue(new LambdaContext(
    [this](int r) {
      process_work();
      m_async_op_tracker.finish_op();
      m_async_process_work--;
    }), 0);
}

template <typename I>
void ReplicatedWriteLog<I>::process_work() {
  CephContext *cct = m_image_ctx.cct;
  int max_iterations = 4;
  bool wake_up_requested = false;
  uint64_t aggressive_high_water_bytes = m_bytes_allocated_cap * AGGRESSIVE_RETIRE_HIGH_WATER;
  uint64_t high_water_bytes = m_bytes_allocated_cap * RETIRE_HIGH_WATER;
  uint64_t low_water_bytes = m_bytes_allocated_cap * RETIRE_LOW_WATER;
  uint64_t aggressive_high_water_entries = m_total_log_entries * AGGRESSIVE_RETIRE_HIGH_WATER;
  uint64_t high_water_entries = m_total_log_entries * RETIRE_HIGH_WATER;
  uint64_t low_water_entries = m_total_log_entries * RETIRE_LOW_WATER;

  ldout(cct, 20) << dendl;

  do {
    {
      std::lock_guard locker(m_lock);
      m_wake_up_requested = false;
    }
    if (m_alloc_failed_since_retire || m_invalidating ||
        m_bytes_allocated > high_water_bytes ||
        (m_log_entries.size() > high_water_entries)) {
      int retired = 0;
      utime_t started = ceph_clock_now();
      ldout(m_image_ctx.cct, 10) << "alloc_fail=" << m_alloc_failed_since_retire
                                 << ", allocated > high_water="
                                 << (m_bytes_allocated > high_water_bytes)
                                 << ", allocated_entries > high_water="
                                 << (m_log_entries.size() > high_water_entries)
                                 << dendl;
      while (m_alloc_failed_since_retire || m_invalidating ||
            (m_bytes_allocated > high_water_bytes) ||
            (m_log_entries.size() > high_water_entries) ||
            (((m_bytes_allocated > low_water_bytes) || (m_log_entries.size() > low_water_entries)) &&
            (utime_t(ceph_clock_now() - started).to_msec() < RETIRE_BATCH_TIME_LIMIT_MS))) {
        if (!retire_entries((m_shutting_down || m_invalidating ||
           (m_bytes_allocated > aggressive_high_water_bytes) ||
           (m_log_entries.size() > aggressive_high_water_entries))
            ? MAX_ALLOC_PER_TRANSACTION
            : MAX_FREE_PER_TRANSACTION)) {
          break;
        }
        retired++;
        dispatch_deferred_writes();
        process_writeback_dirty_entries();
      }
      ldout(m_image_ctx.cct, 10) << "Retired " << retired << " times" << dendl;
    }
    dispatch_deferred_writes();
    process_writeback_dirty_entries();

    {
      std::lock_guard locker(m_lock);
      wake_up_requested = m_wake_up_requested;
    }
  } while (wake_up_requested && --max_iterations > 0);

  {
    std::lock_guard locker(m_lock);
    m_wake_up_scheduled = false;
    /* Reschedule if it's still requested */
    if (m_wake_up_requested) {
      wake_up();
    }
  }
}

template <typename I>
bool ReplicatedWriteLog<I>::can_flush_entry(std::shared_ptr<GenericLogEntry> log_entry) {
  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 20) << "" << dendl;
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  if (m_invalidating) {
    return true;
  }

  /* For OWB we can flush entries with the same sync gen number (write between
   * aio_flush() calls) concurrently. Here we'll consider an entry flushable if
   * its sync gen number is <= the lowest sync gen number carried by all the
   * entries currently flushing.
   *
   * If the entry considered here bears a sync gen number lower than a
   * previously flushed entry, the application had to have submitted the write
   * bearing the higher gen number before the write with the lower gen number
   * completed. So, flushing these concurrently is OK.
   *
   * If the entry considered here bears a sync gen number higher than a
   * currently flushing entry, the write with the lower gen number may have
   * completed to the application before the write with the higher sync gen
   * number was submitted, and the application may rely on that completion
   * order for volume consistency. In this case the entry will not be
   * considered flushable until all the entries bearing lower sync gen numbers
   * finish flushing.
   */

  if (m_flush_ops_in_flight &&
      (log_entry->ram_entry.sync_gen_number > m_lowest_flushing_sync_gen)) {
    return false;
  }

  return (log_entry->can_writeback() &&
         (m_flush_ops_in_flight <= IN_FLIGHT_FLUSH_WRITE_LIMIT) &&
         (m_flush_bytes_in_flight <= IN_FLIGHT_FLUSH_BYTES_LIMIT));
}

template <typename I>
Context* ReplicatedWriteLog<I>::construct_flush_entry_ctx(std::shared_ptr<GenericLogEntry> log_entry) {
  CephContext *cct = m_image_ctx.cct;
  bool invalidating = m_invalidating; // snapshot so we behave consistently

  ldout(cct, 20) << "" << dendl;
  ceph_assert(m_entry_reader_lock.is_locked());
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  if (!m_flush_ops_in_flight ||
      (log_entry->ram_entry.sync_gen_number < m_lowest_flushing_sync_gen)) {
    m_lowest_flushing_sync_gen = log_entry->ram_entry.sync_gen_number;
  }
  m_flush_ops_in_flight += 1;
  /* For write same this is the bytes affected bt the flush op, not the bytes transferred */
  m_flush_bytes_in_flight += log_entry->ram_entry.write_bytes;

  /* Flush write completion action */
  Context *ctx = new LambdaContext(
    [this, log_entry, invalidating](int r) {
      {
        std::lock_guard locker(m_lock);
        if (r < 0) {
          lderr(m_image_ctx.cct) << "failed to flush log entry"
                                 << cpp_strerror(r) << dendl;
          m_dirty_log_entries.push_front(log_entry);
        } else {
          ceph_assert(m_bytes_dirty >= log_entry->bytes_dirty());
          log_entry->set_flushed(true);
          m_bytes_dirty -= log_entry->bytes_dirty();
          sync_point_writer_flushed(log_entry->get_sync_point_entry());
          ldout(m_image_ctx.cct, 20) << "flushed: " << log_entry
                                     << " invalidating=" << invalidating
                                     << dendl;
        }
        m_flush_ops_in_flight -= 1;
        m_flush_bytes_in_flight -= log_entry->ram_entry.write_bytes;
        wake_up();
      }
    });
  /* Flush through lower cache before completing */
  ctx = new LambdaContext(
    [this, ctx](int r) {
      if (r < 0) {
        lderr(m_image_ctx.cct) << "failed to flush log entry"
                               << cpp_strerror(r) << dendl;
        ctx->complete(r);
      } else {
        m_image_writeback.aio_flush(io::FLUSH_SOURCE_WRITEBACK, ctx);
      }
    });

  if (invalidating) {
    return ctx;
  }
  return new LambdaContext(
    [this, log_entry, ctx](int r) {
      m_image_ctx.op_work_queue->queue(new LambdaContext(
        [this, log_entry, ctx](int r) {
          ldout(m_image_ctx.cct, 15) << "flushing:" << log_entry
                                     << " " << *log_entry << dendl;
          log_entry->writeback(m_image_writeback, ctx);
        }), 0);
    });
}

template <typename I>
void ReplicatedWriteLog<I>::process_writeback_dirty_entries() {
  CephContext *cct = m_image_ctx.cct;
  bool all_clean = false;
  int flushed = 0;

  ldout(cct, 20) << "Look for dirty entries" << dendl;
  {
    DeferredContexts post_unlock;
    std::shared_lock entry_reader_locker(m_entry_reader_lock);
    while (flushed < IN_FLIGHT_FLUSH_WRITE_LIMIT) {
      std::lock_guard locker(m_lock);
      if (m_shutting_down) {
        ldout(cct, 5) << "Flush during shutdown supressed" << dendl;
        /* Do flush complete only when all flush ops are finished */
        all_clean = !m_flush_ops_in_flight;
        break;
      }
      if (m_dirty_log_entries.empty()) {
        ldout(cct, 20) << "Nothing new to flush" << dendl;
        /* Do flush complete only when all flush ops are finished */
        all_clean = !m_flush_ops_in_flight;
        break;
      }
      auto candidate = m_dirty_log_entries.front();
      bool flushable = can_flush_entry(candidate);
      if (flushable) {
        post_unlock.add(construct_flush_entry_ctx(candidate));
        flushed++;
        m_dirty_log_entries.pop_front();
      } else {
        ldout(cct, 20) << "Next dirty entry isn't flushable yet" << dendl;
        break;
      }
    }
  }

  if (all_clean) {
    /* All flushing complete, drain outside lock */
    Contexts flush_contexts;
    {
      std::lock_guard locker(m_lock);
      flush_contexts.swap(m_flush_complete_contexts);
    }
    finish_contexts(m_image_ctx.cct, flush_contexts, 0);
  }
}

/**
 * Update/persist the last flushed sync point in the log
 */
template <typename I>
void ReplicatedWriteLog<I>::persist_last_flushed_sync_gen()
{
  TOID(struct WriteLogPoolRoot) pool_root;
  pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);
  uint64_t flushed_sync_gen;

  std::lock_guard append_locker(m_log_append_lock);
  {
    std::lock_guard locker(m_lock);
    flushed_sync_gen = m_flushed_sync_gen;
  }

  if (D_RO(pool_root)->flushed_sync_gen < flushed_sync_gen) {
    ldout(m_image_ctx.cct, 15) << "flushed_sync_gen in log updated from "
                               << D_RO(pool_root)->flushed_sync_gen << " to "
                               << flushed_sync_gen << dendl;
    TX_BEGIN(m_log_pool) {
      D_RW(pool_root)->flushed_sync_gen = flushed_sync_gen;
    } TX_ONCOMMIT {
    } TX_ONABORT {
      lderr(m_image_ctx.cct) << "failed to commit update of flushed sync point" << dendl;
      ceph_assert(false);
    } TX_FINALLY {
    } TX_END;
  }
}

/* Returns true if the specified SyncPointLogEntry is considered flushed, and
 * the log will be updated to reflect this. */
template <typename I>
bool ReplicatedWriteLog<I>::handle_flushed_sync_point(std::shared_ptr<SyncPointLogEntry> log_entry)
{
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  ceph_assert(log_entry);

  if ((log_entry->writes_flushed == log_entry->writes) &&
      log_entry->completed && log_entry->prior_sync_point_flushed &&
      log_entry->next_sync_point_entry) {
    ldout(m_image_ctx.cct, 20) << "All writes flushed up to sync point="
                               << *log_entry << dendl;
    log_entry->next_sync_point_entry->prior_sync_point_flushed = true;
    /* Don't move the flushed sync gen num backwards. */
    if (m_flushed_sync_gen < log_entry->ram_entry.sync_gen_number) {
      m_flushed_sync_gen = log_entry->ram_entry.sync_gen_number;
    }
    m_async_op_tracker.start_op();
    m_work_queue.queue(new LambdaContext(
      [this, log_entry](int r) {
        bool handled_by_next;
        {
          std::lock_guard locker(m_lock);
          handled_by_next = handle_flushed_sync_point(log_entry->next_sync_point_entry);
        }
        if (!handled_by_next) {
          persist_last_flushed_sync_gen();
        }
        m_async_op_tracker.finish_op();
      }));
    return true;
  }
  return false;
}

template <typename I>
void ReplicatedWriteLog<I>::sync_point_writer_flushed(std::shared_ptr<SyncPointLogEntry> log_entry)
{
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  ceph_assert(log_entry);
  log_entry->writes_flushed++;

  /* If this entry might be completely flushed, look closer */
  if ((log_entry->writes_flushed == log_entry->writes) && log_entry->completed) {
    ldout(m_image_ctx.cct, 15) << "All writes flushed for sync point="
                               << *log_entry << dendl;
    handle_flushed_sync_point(log_entry);
  }
}

/* Make a new sync point and flush the previous during initialization, when there may or may
 * not be a previous sync point */
template <typename I>
void ReplicatedWriteLog<I>::init_flush_new_sync_point(DeferredContexts &later) {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  ceph_assert(!m_initialized); /* Don't use this after init */

  if (!m_current_sync_point) {
    /* First sync point since start */
    new_sync_point(later);
  } else {
    flush_new_sync_point(nullptr, later);
  }
}

/**
 * Begin a new sync point
 */
template <typename I>
void ReplicatedWriteLog<I>::new_sync_point(DeferredContexts &later) {
  CephContext *cct = m_image_ctx.cct;
  std::shared_ptr<SyncPoint> old_sync_point = m_current_sync_point;
  std::shared_ptr<SyncPoint> new_sync_point;
  ldout(cct, 20) << dendl;

  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  /* The first time this is called, if this is a newly created log,
   * this makes the first sync gen number we'll use 1. On the first
   * call for a re-opened log m_current_sync_gen will be the highest
   * gen number from all the sync point entries found in the re-opened
   * log, and this advances to the next sync gen number. */
  ++m_current_sync_gen;

  new_sync_point = std::make_shared<SyncPoint>(m_current_sync_gen, cct);
  m_current_sync_point = new_sync_point;

  /* If this log has been re-opened, old_sync_point will initially be
   * nullptr, but m_current_sync_gen may not be zero. */
  if (old_sync_point) {
    new_sync_point->setup_earlier_sync_point(old_sync_point, m_last_op_sequence_num);
    /* This sync point will acquire no more sub-ops. Activation needs
     * to acquire m_lock, so defer to later*/
    later.add(new LambdaContext(
      [this, old_sync_point](int r) {
        old_sync_point->prior_persisted_gather_activate();
      }));
  }

  new_sync_point->prior_persisted_gather_set_finisher();

  if (old_sync_point) {
    ldout(cct,6) << "new sync point = [" << *m_current_sync_point
                 << "], prior = [" << *old_sync_point << "]" << dendl;
  } else {
    ldout(cct,6) << "first sync point = [" << *m_current_sync_point
                 << "]" << dendl;
  }
}

template <typename I>
void ReplicatedWriteLog<I>::flush_new_sync_point(C_FlushRequestT *flush_req,
                                                 DeferredContexts &later) {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  if (!flush_req) {
    m_async_null_flush_finish++;
    m_async_op_tracker.start_op();
    Context *flush_ctx = new LambdaContext([this](int r) {
      m_async_null_flush_finish--;
      m_async_op_tracker.finish_op();
    });
    flush_req = make_flush_req(flush_ctx);
    flush_req->internal = true;
  }

  /* Add a new sync point. */
  new_sync_point(later);
  std::shared_ptr<SyncPoint> to_append = m_current_sync_point->earlier_sync_point;
  ceph_assert(to_append);

  /* This flush request will append/persist the (now) previous sync point */
  flush_req->to_append = to_append;

  /* When the m_sync_point_persist Gather completes this sync point can be
   * appended.  The only sub for this Gather is the finisher Context for
   * m_prior_log_entries_persisted, which records the result of the Gather in
   * the sync point, and completes. TODO: Do we still need both of these
   * Gathers?*/
  Context * ctx = new LambdaContext([this, flush_req](int r) {
    ldout(m_image_ctx.cct, 20) << "Flush req=" << flush_req
                               << " sync point =" << flush_req->to_append
                               << ". Ready to persist." << dendl;
    alloc_and_dispatch_io_req(flush_req);
  });
  to_append->persist_gather_set_finisher(ctx);

  /* The m_sync_point_persist Gather has all the subs it will ever have, and
   * now has its finisher. If the sub is already complete, activation will
   * complete the Gather. The finisher will acquire m_lock, so we'll activate
   * this when we release m_lock.*/
  later.add(new LambdaContext([this, to_append](int r) {
    to_append->persist_gather_activate();
  }));

  /* The flush request completes when the sync point persists */
  to_append->add_in_on_persisted_ctxs(flush_req);
}

template <typename I>
void ReplicatedWriteLog<I>::flush_new_sync_point_if_needed(C_FlushRequestT *flush_req,
                                                           DeferredContexts &later) {
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));

  /* If there have been writes since the last sync point ... */
  if (m_current_sync_point->log_entry->writes) {
    flush_new_sync_point(flush_req, later);
  } else {
    /* There have been no writes to the current sync point. */
    if (m_current_sync_point->earlier_sync_point) {
      /* If previous sync point hasn't completed, complete this flush
       * with the earlier sync point. No alloc or dispatch needed. */
      m_current_sync_point->earlier_sync_point->on_sync_point_persisted.push_back(flush_req);
    } else {
      /* The previous sync point has already completed and been
       * appended. The current sync point has no writes, so this flush
       * has nothing to wait for. This flush completes now. */
      later.add(flush_req);
    }
  }
}

/*
 * RWL internal flush - will actually flush the RWL.
 *
 * User flushes should arrive at aio_flush(), and only flush prior
 * writes to all log replicas.
 *
 * Librbd internal flushes will arrive at flush(invalidate=false,
 * discard=false), and traverse the block guard to ensure in-flight writes are
 * flushed.
 */
template <typename I>
void ReplicatedWriteLog<I>::flush_dirty_entries(Context *on_finish) {
  CephContext *cct = m_image_ctx.cct;
  bool all_clean;
  bool flushing;
  bool stop_flushing;

  {
    std::lock_guard locker(m_lock);
    flushing = (0 != m_flush_ops_in_flight);
    all_clean = m_dirty_log_entries.empty();
    stop_flushing = (m_shutting_down);
  }

  if (!flushing && (all_clean || stop_flushing)) {
    /* Complete without holding m_lock */
    if (all_clean) {
      ldout(cct, 20) << "no dirty entries" << dendl;
    } else {
      ldout(cct, 5) << "flush during shutdown suppressed" << dendl;
    }
    on_finish->complete(0);
  } else {
    if (all_clean) {
      ldout(cct, 5) << "flush ops still in progress" << dendl;
    } else {
      ldout(cct, 20) << "dirty entries remain" << dendl;
    }
    std::lock_guard locker(m_lock);
    /* on_finish can't be completed yet */
    m_flush_complete_contexts.push_back(new LambdaContext(
      [this, on_finish](int r) {
        flush_dirty_entries(on_finish);
      }));
    wake_up();
  }
}

template <typename I>
void ReplicatedWriteLog<I>::internal_flush(bool invalidate, Context *on_finish) {
  ldout(m_image_ctx.cct, 20) << "invalidate=" << invalidate << dendl;

  if (m_perfcounter) {
    if (invalidate) {
      m_perfcounter->inc(l_librbd_rwl_invalidate_cache, 1);
    } else {
      m_perfcounter->inc(l_librbd_rwl_flush, 1);
    }
  }

  /* May be called even if initialization fails */
  if (!m_initialized) {
    ldout(m_image_ctx.cct, 05) << "never initialized" << dendl;
    /* Deadlock if completed here */
    m_image_ctx.op_work_queue->queue(on_finish, 0);
    return;
  }

  /* Flush/invalidate must pass through block guard to ensure all layers of
   * cache are consistently flush/invalidated. This ensures no in-flight write leaves
   * some layers with valid regions, which may later produce inconsistent read
   * results. */
  GuardedRequestFunctionContext *guarded_ctx =
    new GuardedRequestFunctionContext(
      [this, on_finish, invalidate](GuardedRequestFunctionContext &guard_ctx) {
        DeferredContexts on_exit;
        ldout(m_image_ctx.cct, 20) << "cell=" << guard_ctx.cell << dendl;
        ceph_assert(guard_ctx.cell);

        Context *ctx = new LambdaContext(
          [this, cell=guard_ctx.cell, invalidate, on_finish](int r) {
            std::lock_guard locker(m_lock);
            m_invalidating = false;
            ldout(m_image_ctx.cct, 6) << "Done flush/invalidating (invalidate="
                                      << invalidate << ")" << dendl;
            if (m_log_entries.size()) {
              ldout(m_image_ctx.cct, 1) << "m_log_entries.size()="
                                        << m_log_entries.size() << ", "
                                        << "front()=" << *m_log_entries.front()
                                        << dendl;
            }
            if (invalidate) {
              ceph_assert(m_log_entries.size() == 0);
            }
            ceph_assert(m_dirty_log_entries.size() == 0);
            m_image_ctx.op_work_queue->queue(on_finish, r);
            release_guarded_request(cell);
            });
        ctx = new LambdaContext(
          [this, ctx, invalidate](int r) {
            Context *next_ctx = ctx;
            if (r < 0) {
              /* Override on_finish status with this error */
              next_ctx = new LambdaContext([r, ctx](int _r) {
                ctx->complete(r);
              });
            }
            if (invalidate) {
              {
                std::lock_guard locker(m_lock);
                ceph_assert(m_dirty_log_entries.size() == 0);
                ceph_assert(!m_invalidating);
                ldout(m_image_ctx.cct, 6) << "Invalidating" << dendl;
                m_invalidating = true;
              }
              /* Discards all RWL entries */
              while (retire_entries(MAX_ALLOC_PER_TRANSACTION)) { }
              next_ctx->complete(0);
            } else {
              {
                std::lock_guard locker(m_lock);
                ceph_assert(m_dirty_log_entries.size() == 0);
                ceph_assert(!m_invalidating);
              }
              m_image_writeback.aio_flush(io::FLUSH_SOURCE_WRITEBACK, next_ctx);
            }
          });
        ctx = new LambdaContext(
          [this, ctx](int r) {
            flush_dirty_entries(ctx);
          });
        std::lock_guard locker(m_lock);
        /* Even if we're throwing everything away, but we want the last entry to
         * be a sync point so we can cleanly resume.
         *
         * Also, the blockguard only guarantees the replication of this op
         * can't overlap with prior ops. It doesn't guarantee those are all
         * completed and eligible for flush & retire, which we require here.
         */
        auto flush_req = make_flush_req(ctx);
        flush_new_sync_point_if_needed(flush_req, on_exit);
      });
  detain_guarded_request(nullptr, guarded_ctx, true);
}

template <typename I>
void ReplicatedWriteLog<I>::add_into_log_map(GenericWriteLogEntries &log_entries) {
  m_blocks_to_log_entries.add_log_entries(log_entries);
}

template <typename I>
bool ReplicatedWriteLog<I>::can_retire_entry(std::shared_ptr<GenericLogEntry> log_entry) {
  CephContext *cct = m_image_ctx.cct;

  ldout(cct, 20) << dendl;
  ceph_assert(ceph_mutex_is_locked_by_me(m_lock));
  return log_entry->can_retire();
}

/**
 * Retire up to MAX_ALLOC_PER_TRANSACTION of the oldest log entries
 * that are eligible to be retired. Returns true if anything was
 * retired.
 */
template <typename I>
bool ReplicatedWriteLog<I>::retire_entries(const unsigned long int frees_per_tx) {
  CephContext *cct = m_image_ctx.cct;
  GenericLogEntriesVector retiring_entries;
  uint32_t initial_first_valid_entry;
  uint32_t first_valid_entry;

  std::lock_guard retire_locker(m_log_retire_lock);
  ldout(cct, 20) << "Look for entries to retire" << dendl;
  {
    /* Entry readers can't be added while we hold m_entry_reader_lock */
    RWLock::WLocker entry_reader_locker(m_entry_reader_lock);
    std::lock_guard locker(m_lock);
    initial_first_valid_entry = m_first_valid_entry;
    first_valid_entry = m_first_valid_entry;
    auto entry = m_log_entries.front();
    while (!m_log_entries.empty() &&
           retiring_entries.size() < frees_per_tx &&
           can_retire_entry(entry)) {
      if (entry->log_entry_index != first_valid_entry) {
        lderr(cct) << "Retiring entry index (" << entry->log_entry_index
                   << ") and first valid log entry index (" << first_valid_entry
                   << ") must be ==." << dendl;
      }
      ceph_assert(entry->log_entry_index == first_valid_entry);
      first_valid_entry = (first_valid_entry + 1) % m_total_log_entries;
      m_log_entries.pop_front();
      retiring_entries.push_back(entry);
      /* Remove entry from map so there will be no more readers */
      if ((entry->write_bytes() > 0) || (entry->bytes_dirty() > 0)) {
        auto gen_write_entry = static_pointer_cast<GenericWriteLogEntry>(entry);
        if (gen_write_entry) {
          m_blocks_to_log_entries.remove_log_entry(gen_write_entry);
        }
      }
      entry = m_log_entries.front();
    }
  }

  if (retiring_entries.size()) {
    ldout(cct, 20) << "Retiring " << retiring_entries.size() << " entries" << dendl;
    TOID(struct WriteLogPoolRoot) pool_root;
    pool_root = POBJ_ROOT(m_log_pool, struct WriteLogPoolRoot);

    utime_t tx_start;
    utime_t tx_end;
    /* Advance first valid entry and release buffers */
    {
      uint64_t flushed_sync_gen;
      std::lock_guard append_locker(m_log_append_lock);
      {
        std::lock_guard locker(m_lock);
        flushed_sync_gen = m_flushed_sync_gen;
      }

      tx_start = ceph_clock_now();
      TX_BEGIN(m_log_pool) {
        if (D_RO(pool_root)->flushed_sync_gen < flushed_sync_gen) {
          ldout(m_image_ctx.cct, 20) << "flushed_sync_gen in log updated from "
                                     << D_RO(pool_root)->flushed_sync_gen << " to "
                                     << flushed_sync_gen << dendl;
          D_RW(pool_root)->flushed_sync_gen = flushed_sync_gen;
        }
        D_RW(pool_root)->first_valid_entry = first_valid_entry;
        for (auto &entry: retiring_entries) {
          if (entry->write_bytes()) {
            ldout(cct, 20) << "Freeing " << entry->ram_entry.write_data.oid.pool_uuid_lo
                           << "." << entry->ram_entry.write_data.oid.off << dendl;
            TX_FREE(entry->ram_entry.write_data);
          } else {
            ldout(cct, 20) << "Retiring non-write: " << *entry << dendl;
          }
        }
      } TX_ONCOMMIT {
      } TX_ONABORT {
        lderr(cct) << "failed to commit free of" << retiring_entries.size() << " log entries (" << m_log_pool_name << ")" << dendl;
        ceph_assert(false);
      } TX_FINALLY {
      } TX_END;
      tx_end = ceph_clock_now();
    }
    m_perfcounter->tinc(l_librbd_rwl_retire_tx_t, tx_end - tx_start);
    m_perfcounter->hinc(l_librbd_rwl_retire_tx_t_hist, utime_t(tx_end - tx_start).to_nsec(), retiring_entries.size());

    /* Update runtime copy of first_valid, and free entries counts */
    {
      std::lock_guard locker(m_lock);

      ceph_assert(m_first_valid_entry == initial_first_valid_entry);
      m_first_valid_entry = first_valid_entry;
      m_free_log_entries += retiring_entries.size();
      for (auto &entry: retiring_entries) {
        if (entry->write_bytes()) {
          ceph_assert(m_bytes_cached >= entry->write_bytes());
          m_bytes_cached -= entry->write_bytes();
          uint64_t entry_allocation_size = entry->write_bytes();
          if (entry_allocation_size < MIN_WRITE_ALLOC_SIZE) {
            entry_allocation_size = MIN_WRITE_ALLOC_SIZE;
          }
          ceph_assert(m_bytes_allocated >= entry_allocation_size);
          m_bytes_allocated -= entry_allocation_size;
        }
      }
      m_alloc_failed_since_retire = false;
      wake_up();
    }
  } else {
    ldout(cct, 20) << "Nothing to retire" << dendl;
    return false;
  }
  return true;
}

} // namespace cache
} // namespace librbd

template class librbd::cache::ReplicatedWriteLog<librbd::ImageCtx>;
template class librbd::cache::ImageCache<librbd::ImageCtx>;
