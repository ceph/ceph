// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "include/buffer.h"
#include "include/denc.h"

#include "crimson/os/seastore/async_cleaner.h"
#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/segment_manager_group.h"
#include "crimson/os/seastore/ordering_handle.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/osd/exceptions.h"
#include "segment_allocator.h"
#include "crimson/os/seastore/segment_seq_allocator.h"
#include "record_submitter.h"

namespace crimson::os::seastore::journal {
/**
 * Manages stream of atomically written records to a SegmentManager.
 */
class SegmentedJournal : public Journal {
public:
  SegmentedJournal(
      SegmentProvider &segment_provider,
      JournalTrimmer &trimmer);
  ~SegmentedJournal() {}

  JournalTrimmer &get_trimmer() final {
    return trimmer;
  }

  writer_stats_t get_writer_stats() const final {
    return record_submitter.get_stats();
  }

  open_for_mkfs_ret open_for_mkfs() final;

  open_for_mount_ret open_for_mount() final;

  close_ertr::future<> close() final;

  submit_record_ret submit_record(
    record_t &&record,
    OrderingHandle &handle) final;

  seastar::future<> flush(OrderingHandle &handle) final;

  replay_ret replay(delta_handler_t &&delta_handler) final;

  void set_write_pipeline(WritePipeline *_write_pipeline) final {
    write_pipeline = _write_pipeline;
  }

  backend_type_t get_type() final {
    return backend_type_t::SEGMENTED;
  }
  seastar::future<> finish_commit(transaction_type_t type) {
    return seastar::now();
  }

private:
  submit_record_ret do_submit_record(
    record_t &&record,
    OrderingHandle &handle
  );

  SegmentSeqAllocatorRef segment_seq_allocator;
  SegmentAllocator journal_segment_allocator;
  RecordSubmitter record_submitter;
  SegmentManagerGroup &sm_group;
  JournalTrimmer &trimmer;
  WritePipeline* write_pipeline = nullptr;

  /// return ordered vector of segments to replay
  using replay_segments_t = std::vector<
    std::pair<journal_seq_t, segment_header_t>>;
  using prep_replay_segments_fut = replay_ertr::future<
    replay_segments_t>;
  prep_replay_segments_fut prep_replay_segments(
    std::vector<std::pair<segment_id_t, segment_header_t>> segments);

  /// scan the last segment for tail deltas
  using scan_last_segment_ertr = replay_ertr;
  scan_last_segment_ertr::future<> scan_last_segment(
      const segment_id_t&, const segment_header_t&);

  struct replay_stats_t {
    std::size_t num_record_groups = 0;
    std::size_t num_records = 0;
    std::size_t num_alloc_deltas = 0;
    std::size_t num_dirty_deltas = 0;
  };

  /// replays records starting at start through end of segment
  replay_ertr::future<>
  replay_segment(
    journal_seq_t start,             ///< [in] starting addr, seq
    segment_header_t header,         ///< [in] segment header
    delta_handler_t &delta_handler,  ///< [in] processes deltas in order
    replay_stats_t &stats            ///< [out] replay stats
  );
};

}
