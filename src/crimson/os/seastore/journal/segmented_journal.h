// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "include/buffer.h"
#include "include/denc.h"

#include "crimson/os/seastore/segment_cleaner.h"
#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/extent_reader.h"
#include "crimson/os/seastore/ordering_handle.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/osd/exceptions.h"
#include "segment_allocator.h"

namespace crimson::os::seastore::journal {

/**
 * Manages stream of atomically written records to a SegmentManager.
 */
class SegmentedJournal : public Journal {
public:
  SegmentedJournal(
    SegmentManager &segment_manager,
    ExtentReader& scanner,
    SegmentProvider& cleaner);
  ~SegmentedJournal() {}

  open_for_write_ret open_for_write() final;

  close_ertr::future<> close() final;

  submit_record_ret submit_record(
    record_t &&record,
    OrderingHandle &handle) final;

  seastar::future<> flush(OrderingHandle &handle) final;

  replay_ret replay(delta_handler_t &&delta_handler) final;

  void set_write_pipeline(WritePipeline *_write_pipeline) final {
    write_pipeline = _write_pipeline;
  }

private:
  submit_record_ret do_submit_record(
    record_t &&record,
    OrderingHandle &handle
  );

  SegmentProvider& segment_provider;
  SegmentAllocator journal_segment_allocator;
  RecordSubmitter record_submitter;
  ExtentReader& scanner;
  WritePipeline* write_pipeline = nullptr;

  /// read journal segment headers from scanner
  using find_journal_segments_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using find_journal_segments_ret_bare = std::vector<
    std::pair<segment_id_t, segment_header_t>>;
  using find_journal_segments_ret = find_journal_segments_ertr::future<
    find_journal_segments_ret_bare>;
  find_journal_segments_ret find_journal_segments();

  /// return ordered vector of segments to replay
  using replay_segments_t = std::vector<
    std::pair<journal_seq_t, segment_header_t>>;
  using prep_replay_segments_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  using prep_replay_segments_fut = prep_replay_segments_ertr::future<
    replay_segments_t>;
  prep_replay_segments_fut prep_replay_segments(
    std::vector<std::pair<segment_id_t, segment_header_t>> segments);

  /// replays records starting at start through end of segment
  replay_ertr::future<>
  replay_segment(
    journal_seq_t start,             ///< [in] starting addr, seq
    segment_header_t header,         ///< [in] segment header
    delta_handler_t &delta_handler   ///< [in] processes deltas in order
  );
};

}
