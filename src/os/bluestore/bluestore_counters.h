// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2014 Red Hat
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_BLUESTORE_COUNTERS_H
#define CEPH_BLUESTORE_COUNTERS_H

#include "common/perf_counters.h"

PERF_COUNTERS_ADD_TIME_AVG(l_bluestore_kv_flush_lat,
  "kv_flush_lat", "Average kv_thread flush latency", "fl_l",
  PerfCountersBuilder::PRIO_INTERESTING);

PERF_COUNTERS_ADD_TIME_AVG(l_bluestore_kv_commit_lat,
  "kv_commit_lat", "Average kv_thread commit latency");

PERF_COUNTERS_ADD_TIME_AVG(l_bluestore_kv_sync_lat,
  "kv_sync_lat", "Average kv_sync thread latency", "ks_l",
  PerfCountersBuilder::PRIO_INTERESTING);

PERF_COUNTERS_ADD_TIME_AVG(l_bluestore_kv_final_lat,
  "kv_final_lat", "Average kv_finalize thread latency", "kf_l",
  PerfCountersBuilder::PRIO_INTERESTING);

PERF_COUNTERS_ADD_TIME_AVG(l_bluestore_state_prepare_lat,
  "state_prepare_lat", "Average prepare state latency");

PERF_COUNTERS_ADD_TIME_AVG(l_bluestore_state_aio_wait_lat,
  "state_aio_wait_lat", "Average aio_wait state latency", "io_l",
  PerfCountersBuilder::PRIO_INTERESTING);

PERF_COUNTERS_ADD_TIME_AVG(l_bluestore_state_io_done_lat,
  "state_io_done_lat", "Average io_done state latency");

PERF_COUNTERS_ADD_TIME_AVG(l_bluestore_state_kv_queued_lat,
  "state_kv_queued_lat", "Average kv_queued state latency");

PERF_COUNTERS_ADD_TIME_AVG(l_bluestore_state_kv_committing_lat,
  "state_kv_commiting_lat", "Average kv_commiting state latency");

PERF_COUNTERS_ADD_TIME_AVG(l_bluestore_state_kv_done_lat,
  "state_kv_done_lat", "Average kv_done state latency");

PERF_COUNTERS_ADD_TIME_AVG(l_bluestore_state_deferred_queued_lat,
  "state_deferred_queued_lat", "Average deferred_queued state latency");

PERF_COUNTERS_ADD_TIME_AVG(l_bluestore_state_deferred_aio_wait_lat,
  "state_deferred_aio_wait_lat", "Average aio_wait state latency");

PERF_COUNTERS_ADD_TIME_AVG(l_bluestore_state_deferred_cleanup_lat,
  "state_deferred_cleanup_lat", "Average cleanup state latency");

PERF_COUNTERS_ADD_TIME_AVG(l_bluestore_state_finishing_lat,
  "state_finishing_lat", "Average finishing state latency");

PERF_COUNTERS_ADD_TIME_AVG(l_bluestore_state_done_lat,
  "state_done_lat", "Average done state latency");

PERF_COUNTERS_ADD_TIME_AVG(l_bluestore_throttle_lat,
  "throttle_lat", "Average submit throttle latency", "th_l",
  PerfCountersBuilder::PRIO_CRITICAL);

PERF_COUNTERS_ADD_TIME_AVG(l_bluestore_submit_lat,
  "submit_lat", "Average submit latency", "s_l",
  PerfCountersBuilder::PRIO_CRITICAL);

PERF_COUNTERS_ADD_TIME_AVG(l_bluestore_commit_lat,
  "commit_lat", "Average commit latency", "c_l",
  PerfCountersBuilder::PRIO_CRITICAL);

PERF_COUNTERS_ADD_TIME_AVG(l_bluestore_read_lat,
  "read_lat", "Average read latency", "r_l",
  PerfCountersBuilder::PRIO_CRITICAL);

PERF_COUNTERS_ADD_TIME_AVG(l_bluestore_read_onode_meta_lat,
  "read_onode_meta_lat", "Average read onode metadata latency");

PERF_COUNTERS_ADD_TIME_AVG(l_bluestore_read_wait_aio_lat,
  "read_wait_aio_lat", "Average read latency");

PERF_COUNTERS_ADD_TIME_AVG(l_bluestore_compress_lat,
  "compress_lat", "Average compress latency");

PERF_COUNTERS_ADD_TIME_AVG(l_bluestore_decompress_lat,
  "decompress_lat", "Average decompress latency");

PERF_COUNTERS_ADD_TIME_AVG(l_bluestore_csum_lat,
  "csum_lat", "Average checksum latency");

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_compress_success_count,
  "compress_success_count", "Sum for beneficial compress ops");

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_compress_rejected_count,
  "compress_rejected_count", "Sum for compress ops rejected due to low "
  "net gain of space");

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_write_pad_bytes,
  "write_pad_bytes", "Sum for write-op padded bytes", nullptr, 0, UNIT_BYTES);

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_deferred_write_ops,
  "deferred_write_ops", "Sum for deferred write op");

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_deferred_write_bytes,
  "deferred_write_bytes", "Sum for deferred write bytes", "def", 0, UNIT_BYTES);

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_write_penalty_read_ops,
  "write_penalty_read_ops", "Sum for write penalty read ops");


PERF_COUNTERS_ADD_U64(l_bluestore_allocated,
  "bluestore_allocated", "Sum for allocated bytes");

PERF_COUNTERS_ADD_U64(l_bluestore_stored,
  "bluestore_stored", "Sum for stored bytes");

PERF_COUNTERS_ADD_U64(l_bluestore_compressed,
  "bluestore_compressed", "Sum for stored compressed bytes");

PERF_COUNTERS_ADD_U64(l_bluestore_compressed_allocated,
  "bluestore_compressed_allocated",
  "Sum for bytes allocated for compressed data");

PERF_COUNTERS_ADD_U64(l_bluestore_compressed_original,
  "bluestore_compressed_original",
  "Sum for original bytes that were compressed");

PERF_COUNTERS_ADD_U64_SETABLE(l_bluestore_onodes,
  "bluestore_onodes", "Number of onodes in cache");

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_onode_hits,
  "bluestore_onode_hits", "Sum for onode-lookups hit in the cache");

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_onode_misses,
  "bluestore_onode_misses", "Sum for onode-lookups missed in the cache");

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_onode_shard_hits,
  "bluestore_onode_shard_hits", "Sum for onode-shard lookups hit in the cache");

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_onode_shard_misses,
  "bluestore_onode_shard_misses", "Sum for onode-shard lookups missed "
  "in the cache");

PERF_COUNTERS_ADD_U64_SETABLE(l_bluestore_extents,
  "bluestore_extents", "Number of extents in cache");

PERF_COUNTERS_ADD_U64_SETABLE(l_bluestore_blobs,
  "bluestore_blobs", "Number of blobs in cache");

PERF_COUNTERS_ADD_U64_SETABLE(l_bluestore_buffers,
  "bluestore_buffers", "Number of buffers in cache");

PERF_COUNTERS_ADD_U64_SETABLE(l_bluestore_buffer_bytes,
  "bluestore_buffer_bytes", "Number of buffer bytes in cache",
  nullptr, 0, UNIT_BYTES);

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_buffer_hit_bytes,
  "bluestore_buffer_hit_bytes", "Sum for bytes of read hit in the cache",
  nullptr, 0, UNIT_BYTES);

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_buffer_miss_bytes,
  "bluestore_buffer_miss_bytes", "Sum for bytes of read missed in the cache",
  nullptr, 0, UNIT_BYTES);

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_write_big,
  "bluestore_write_big", "Large aligned writes into fresh blobs");

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_write_big_bytes,
  "bluestore_write_big_bytes", "Large aligned writes into fresh blobs (bytes)",
  nullptr, 0, UNIT_BYTES);

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_write_big_blobs,
  "bluestore_write_big_blobs", "Large aligned writes into fresh blobs (blobs)");

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_write_small,
  "bluestore_write_small", "Small writes into existing or sparse small blobs");

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_write_small_bytes,
  "bluestore_write_small_bytes", "Small writes into existing or sparse small "
  "blobs (bytes)", nullptr, 0, UNIT_BYTES);

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_write_small_unused,
  "bluestore_write_small_unused", "Small writes into unused portion of "
  "existing blob");

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_write_small_deferred,
  "bluestore_write_small_deferred", "Small overwrites using deferred");

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_write_small_pre_read,
  "bluestore_write_small_pre_read", "Small writes that required we read some "
  "data (possibly cached) to fill out the block");

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_write_small_new,
  "bluestore_write_small_new", "Small write into new (sparse) blob");

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_txc,
  "bluestore_txc", "Transactions committed");

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_onode_reshard,
  "bluestore_onode_reshard", "Onode extent map reshard events");

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_blob_split,
  "bluestore_blob_split", "Sum for blob splitting due to resharding");

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_extent_compress,
  "bluestore_extent_compress", "Sum for extents that have been removed "
  "due to compression");

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_gc_merged,
  "bluestore_gc_merged", "Sum for extents that have been merged due to "
  "garbage collection");

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_read_eio,
  "bluestore_read_eio", "Read EIO errors propagated to high level callers");

PERF_COUNTERS_ADD_U64_COUNTER(l_bluestore_reads_with_retries,
  "bluestore_reads_with_retries",
  "Read operations that required at least one retry due to failed checksum validation");


PERF_COUNTERS_ADD_U64_SETABLE(l_bluestore_fragmentation,
  "bluestore_fragmentation_micros", "How fragmented bluestore free space is "
  "(free extents / max possible number of free extents) * 1000");

using bluestore_perf_counters_t = ceph::perf_counters_t<
  l_bluestore_kv_flush_lat,
  l_bluestore_kv_commit_lat,
  l_bluestore_kv_sync_lat,
  l_bluestore_kv_final_lat,
  l_bluestore_state_prepare_lat,
  l_bluestore_state_aio_wait_lat,
  l_bluestore_state_io_done_lat,
  l_bluestore_state_kv_queued_lat,
  l_bluestore_state_kv_committing_lat,
  l_bluestore_state_kv_done_lat,
  l_bluestore_state_deferred_queued_lat,
  l_bluestore_state_deferred_aio_wait_lat,
  l_bluestore_state_deferred_cleanup_lat,
  l_bluestore_state_finishing_lat,
  l_bluestore_state_done_lat,
  l_bluestore_throttle_lat,
  l_bluestore_submit_lat,
  l_bluestore_commit_lat,
  l_bluestore_read_lat,
  l_bluestore_read_onode_meta_lat,
  l_bluestore_read_wait_aio_lat,
  l_bluestore_compress_lat,
  l_bluestore_decompress_lat,
  l_bluestore_csum_lat,

  l_bluestore_compress_success_count,
  l_bluestore_compress_rejected_count,
  l_bluestore_write_pad_bytes,
  l_bluestore_deferred_write_ops,
  l_bluestore_deferred_write_bytes,
  l_bluestore_write_penalty_read_ops,

  l_bluestore_allocated,
  l_bluestore_stored,
  l_bluestore_compressed,
  l_bluestore_compressed_allocated,
  l_bluestore_compressed_original,
  l_bluestore_onodes,

  l_bluestore_onode_hits,
  l_bluestore_onode_misses,
  l_bluestore_onode_shard_hits,
  l_bluestore_onode_shard_misses,

  l_bluestore_extents,
  l_bluestore_blobs,
  l_bluestore_buffers,
  l_bluestore_buffer_bytes,
  l_bluestore_buffer_hit_bytes,
  l_bluestore_buffer_miss_bytes,

  l_bluestore_write_big,
  l_bluestore_write_big_bytes,
  l_bluestore_write_big_blobs,
  l_bluestore_write_small,
  l_bluestore_write_small_bytes,
  l_bluestore_write_small_unused,
  l_bluestore_write_small_deferred,
  l_bluestore_write_small_pre_read,
  l_bluestore_write_small_new,
  l_bluestore_txc,
  l_bluestore_onode_reshard,
  l_bluestore_blob_split,
  l_bluestore_extent_compress,
  l_bluestore_gc_merged,
  l_bluestore_read_eio,
  l_bluestore_reads_with_retries,

  l_bluestore_fragmentation>;

#endif // CEPH_OSD_BLUESTORE_H
