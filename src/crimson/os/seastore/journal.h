// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <optional>

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
   * Gets the current journal segment sequence.
   */
  segment_seq_t get_segment_seq() const {
    return journal_segment_manager.get_segment_seq();
  }

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
    journal_segment_manager.set_segment_provider(provider);
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
  open_for_write_ret open_for_write() {
    return journal_segment_manager.open();
  }

  /**
   * close journal
   *
   * TODO: should probably flush and disallow further writes
   */
  using close_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  close_ertr::future<> close() {
    return journal_segment_manager.close();
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
      record, journal_segment_manager.get_block_size());
    auto total = rsize.mdlength + rsize.dlength;
    auto max_record_length = journal_segment_manager.get_max_write_length();
    if (total > max_record_length) {
      auto &logger = crimson::get_logger(ceph_subsys_seastore);
      logger.error(
	"Journal::submit_record: record size {} exceeds max {}",
	total,
	max_record_length
      );
      return crimson::ct_error::erange::make();
    }
    auto roll = journal_segment_manager.needs_roll(total)
      ? journal_segment_manager.roll()
      : JournalSegmentManager::roll_ertr::now();
    return roll.safe_then(
      [this, rsize, record=std::move(record), &handle]() mutable {
	auto seq = journal_segment_manager.get_segment_seq();
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
  class JournalSegmentManager {
  public:
    JournalSegmentManager(SegmentManager&);

    using base_ertr = crimson::errorator<
        crimson::ct_error::input_output_error>;
    extent_len_t get_max_write_length() const {
      return segment_manager.get_segment_size() -
             p2align(ceph::encoded_sizeof_bounded<segment_header_t>(),
                     size_t(segment_manager.get_block_size()));
    }

    segment_off_t get_block_size() const {
      return segment_manager.get_block_size();
    }

    segment_nonce_t get_nonce() const {
      return current_segment_nonce;
    }

    segment_off_t get_committed_to() const {
      assert(committed_to.segment_seq ==
             get_segment_seq());
      return committed_to.offset.offset;
    }

    segment_seq_t get_segment_seq() const {
      return next_journal_segment_seq - 1;
    }

    void set_segment_provider(SegmentProvider* provider) {
      segment_provider = provider;
    }

    void set_segment_seq(segment_seq_t current_seq) {
      next_journal_segment_seq = (current_seq + 1);
    }

    using open_ertr = base_ertr;
    using open_ret = open_ertr::future<journal_seq_t>;
    open_ret open() {
      return roll().safe_then([this] {
        return get_current_write_seq();
      });
    }

    using close_ertr = base_ertr;
    close_ertr::future<> close();

    // returns true iff the current segment has insufficient space
    bool needs_roll(std::size_t length) const {
      auto write_capacity = current_journal_segment->get_write_capacity();
      return length + written_to > std::size_t(write_capacity);
    }

    // close the current segment and initialize next one
    using roll_ertr = base_ertr;
    roll_ertr::future<> roll();

    // write the buffer, return the write start
    // May be called concurrently, writes may complete in any order.
    using write_ertr = base_ertr;
    using write_ret = write_ertr::future<journal_seq_t>;
    write_ret write(ceph::bufferlist to_write);

    // mark write committed in order
    void mark_committed(const journal_seq_t& new_committed_to);

  private:
    journal_seq_t get_current_write_seq() const {
      assert(current_journal_segment);
      return journal_seq_t{
        get_segment_seq(),
        {current_journal_segment->get_segment_id(), written_to}
      };
    }

    void reset() {
      next_journal_segment_seq = 0;
      current_segment_nonce = 0;
      current_journal_segment.reset();
      written_to = 0;
      committed_to = {};
    }

    // prepare segment for writes, writes out segment header
    using initialize_segment_ertr = base_ertr;
    initialize_segment_ertr::future<> initialize_segment(Segment&);

    SegmentProvider* segment_provider;
    SegmentManager& segment_manager;

    segment_seq_t next_journal_segment_seq;
    segment_nonce_t current_segment_nonce;

    SegmentRef current_journal_segment;
    segment_off_t written_to;
    // committed_to may be in a previous journal segment
    journal_seq_t committed_to;
  };

  SegmentProvider* segment_provider = nullptr;
  JournalSegmentManager journal_segment_manager;
  ExtentReader& scanner;
  WritePipeline *write_pipeline = nullptr;

  /// do record write
  using write_record_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using write_record_ret = write_record_ertr::future<paddr_t>;
  write_record_ret write_record(
    record_size_t rsize,
    record_t &&record,
    OrderingHandle &handle);

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
};
using JournalRef = std::unique_ptr<Journal>;

}
