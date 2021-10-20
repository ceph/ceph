// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/log.h"

#include <boost/intrusive_ptr.hpp>

#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "include/buffer.h"
#include "include/denc.h"

#include "crimson/common/log.h"
#include "crimson/os/seastore/extent_reader.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/ordering_handle.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/osd/exceptions.h"

namespace crimson::os::seastore {

class SegmentProvider;
class SegmentedAllocator;

/**
 * Manages stream of atomically written records to a SegmentManager.
 */
class Journal {
public:
  Journal(SegmentManager &segment_manager, ExtentReader& scanner);

  /**
   * Sets the SegmentProvider.
   *
   * Not provided in constructor to allow the provider to not own
   * or construct the Journal (TransactionManager).
   *
   * Note, Journal does not own this ptr, user must ensure that
   * *provider outlives Journal.
   */
  void set_segment_provider(SegmentProvider *provider) {
    segment_provider = provider;
  }

  /**
   * initializes journal for new writes -- must run prior to calls
   * to submit_record.  Should be called after replay if not a new
   * Journal.
   */
  using open_for_write_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  using open_for_write_ret = open_for_write_ertr::future<journal_seq_t>;
  open_for_write_ret open_for_write();

  /**
   * close journal
   *
   * TODO: should probably flush and disallow further writes
   */
  using close_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  close_ertr::future<> close() {
    return (
      current_journal_segment ?
      current_journal_segment->close() :
      Segment::close_ertr::now()
    ).handle_error(
      close_ertr::pass_further{},
      crimson::ct_error::assert_all{
	"Error during Journal::close()"
      }
    ).finally([this] {
      current_journal_segment.reset();
      reset_soft_state();
    });
  }

  /**
   * submit_record
   *
   * @param write record and returns offset of first block and seq
   */
  using submit_record_ertr = crimson::errorator<
    crimson::ct_error::erange,
    crimson::ct_error::input_output_error
    >;
  using submit_record_ret = submit_record_ertr::future<
    std::pair<paddr_t, journal_seq_t>
    >;
  submit_record_ret submit_record(
    record_t &&record,
    OrderingHandle &handle
  ) {
    assert(write_pipeline);
    auto rsize = get_encoded_record_length(
      record, segment_manager.get_block_size());
    auto total = rsize.mdlength + rsize.dlength;
    if (total > max_record_length()) {
      auto &logger = crimson::get_logger(ceph_subsys_seastore);
      logger.error(
	"Journal::submit_record: record size {} exceeds max {}",
	total,
	max_record_length()
      );
      return crimson::ct_error::erange::make();
    }
    auto roll = needs_roll(total)
      ? roll_journal_segment().safe_then([](auto){})
      : roll_journal_segment_ertr::now();
    return roll.safe_then(
      [this, rsize, record=std::move(record), &handle]() mutable {
	auto seq = next_journal_segment_seq - 1;
	return write_record(
	  rsize, std::move(record),
	  handle
	).safe_then([rsize, seq](auto addr) {
	  return std::make_pair(
	    addr.add_offset(rsize.mdlength),
	    journal_seq_t{seq, addr});
	});
      });
  }

  /**
   * Read deltas and pass to delta_handler
   *
   * record_block_start (argument to delta_handler) is the start of the
   * of the first block in the record
   */
  using replay_ertr = SegmentManager::read_ertr;
  using replay_ret = replay_ertr::future<>;
  using delta_handler_t = std::function<
    replay_ret(journal_seq_t seq,
	       paddr_t record_block_base,
	       const delta_info_t&)>;
  replay_ret replay(
    std::vector<std::pair<segment_id_t, segment_header_t>>&& segment_headers,
    delta_handler_t &&delta_handler);

  void set_write_pipeline(WritePipeline *_write_pipeline) {
    write_pipeline = _write_pipeline;
  }

private:
  SegmentProvider *segment_provider = nullptr;
  SegmentManager &segment_manager;

  segment_seq_t next_journal_segment_seq = 0;
  segment_nonce_t current_segment_nonce = 0;

  SegmentRef current_journal_segment;
  segment_off_t written_to = 0;
  segment_off_t committed_to = 0;

  ExtentReader& scanner;
  WritePipeline *write_pipeline = nullptr;

  void reset_soft_state() {
    next_journal_segment_seq = 0;
    current_segment_nonce = 0;
    written_to = 0;
    committed_to = 0;
  }

  /// prepare segment for writes, writes out segment header
  using initialize_segment_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  initialize_segment_ertr::future<segment_seq_t> initialize_segment(
    Segment &segment);


  /// do record write
  using write_record_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using write_record_ret = write_record_ertr::future<paddr_t>;
  write_record_ret write_record(
    record_size_t rsize,
    record_t &&record,
    OrderingHandle &handle);

  /// close current segment and initialize next one
  using roll_journal_segment_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  roll_journal_segment_ertr::future<segment_seq_t> roll_journal_segment();

  /// returns true iff current segment has insufficient space
  bool needs_roll(segment_off_t length) const;

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

  /// attempts to decode deltas from bl, return nullopt if unsuccessful
  std::optional<std::vector<delta_info_t>> try_decode_deltas(
    record_header_t header,
    const bufferlist &bl);

private:

  /// replays records starting at start through end of segment
  replay_ertr::future<>
  replay_segment(
    journal_seq_t start,             ///< [in] starting addr, seq
    segment_header_t header,         ///< [in] segment header
    delta_handler_t &delta_handler   ///< [in] processes deltas in order
  );

  extent_len_t max_record_length() const;
  friend class crimson::os::seastore::SegmentedAllocator;
};
using JournalRef = std::unique_ptr<Journal>;

}

namespace crimson::os::seastore {

inline extent_len_t Journal::max_record_length() const {
  return segment_manager.get_segment_size() -
    p2align(ceph::encoded_sizeof_bounded<segment_header_t>(),
	    size_t(segment_manager.get_block_size()));
}

}
