// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/log.h"

#include <boost/intrusive_ptr.hpp>

#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "include/buffer.h"
#include "include/denc.h"

#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/osd/exceptions.h"

namespace crimson::os::seastore {

using journal_seq_t = uint64_t;
static constexpr journal_seq_t NO_DELTAS =
  std::numeric_limits<journal_seq_t>::max();

/**
 * Segment header
 *
 * Every segment contains and encode segment_header_t in the first block.
 * Our strategy for finding the journal replay point is:
 * 1) Find the segment with the highest journal_segment_seq
 * 2) Scan forward from committed_journal_lb to find the most recent
 *    journal_commit_lb record
 * 3) Replay starting at the most recent found journal_commit_lb record
 */
struct segment_header_t {
  segment_seq_t journal_segment_seq;
  segment_id_t physical_segment_id; // debugging

  paddr_t journal_replay_lb;

  DENC(segment_header_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.journal_segment_seq, p);
    denc(v.physical_segment_id, p);
    denc(v.journal_replay_lb, p);
    DENC_FINISH(p);
  }
};

struct record_header_t {
  // Fixed portion
  extent_len_t  mdlength;       // block aligned, length of metadata
  extent_len_t  dlength;        // block aligned, length of data
  journal_seq_t seq;            // current journal seqid
  checksum_t    full_checksum;  // checksum for full record (TODO)
  size_t deltas;                // number of deltas
  size_t extents;               // number of extents

  DENC(record_header_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.mdlength, p);
    denc(v.dlength, p);
    denc(v.seq, p);
    denc(v.full_checksum, p);
    denc(v.deltas, p);
    denc(v.extents, p);
    DENC_FINISH(p);
  }
};

/**
 * Callback interface for managing available segments
 */
class JournalSegmentProvider {
public:
  using get_segment_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using get_segment_ret = get_segment_ertr::future<segment_id_t>;
  virtual get_segment_ret get_segment() = 0;

  /* TODO: we'll want to use this to propogate information about segment contents */
  virtual void put_segment(segment_id_t segment) = 0;

  virtual ~JournalSegmentProvider() {}
};

/**
 * Manages stream of atomically written records to a SegmentManager.
 */
class Journal {
public:
  Journal(SegmentManager &segment_manager);

  /**
   * Sets the JournalSegmentProvider.
   *
   * Not provided in constructor to allow the provider to not own
   * or construct the Journal (TransactionManager).
   *
   * Note, Journal does not own this ptr, user must ensure that
   * *provider outlives Journal.
   */
  void set_segment_provider(JournalSegmentProvider *provider) {
    segment_provider = provider;
  }

  /**
   * initializes journal for new writes -- must run prior to calls
   * to submit_record.  Should be called after replay if not a new
   * Journal.
   */
  using init_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  init_ertr::future<> open_for_write();

  /**
   * close journal
   *
   * TODO: should probably flush and disallow further writes
   */
  using close_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  close_ertr::future<> close() { return close_ertr::now(); }

  /**
   * write_record
   *
   * @param write record and returns offset of first block
   */
  using submit_record_ertr = crimson::errorator<
    crimson::ct_error::erange,
    crimson::ct_error::input_output_error
    >;
  using submit_record_ret = submit_record_ertr::future<paddr_t>;
  submit_record_ret submit_record(record_t &&record) {
    auto rsize = get_encoded_record_length(record);
    auto total = rsize.mdlength + rsize.dlength;
    if (total > max_record_length) {
      return crimson::ct_error::erange::make();
    }
    auto roll = needs_roll(total)
      ? roll_journal_segment()
      : roll_journal_segment_ertr::now();
    return roll.safe_then(
      [this, rsize, record=std::move(record)]() mutable {
	auto ret = next_record_addr();
	return write_record(rsize, std::move(record)
	).safe_then([this, ret] {
	  return ret.add_offset(block_size);
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
    replay_ret(paddr_t record_start, const delta_info_t&)>;
  replay_ret replay(delta_handler_t &&delta_handler);

private:
  const extent_len_t block_size;
  const extent_len_t max_record_length;

  JournalSegmentProvider *segment_provider = nullptr;
  SegmentManager &segment_manager;

  paddr_t current_replay_point;

  segment_seq_t current_journal_segment_seq = 0;

  SegmentRef current_journal_segment;
  segment_off_t written_to = 0;

  segment_id_t next_journal_segment_seq = NULL_SEG_ID;
  journal_seq_t current_journal_seq = 0;

  /// prepare segment for writes, writes out segment header
  using initialize_segment_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  initialize_segment_ertr::future<> initialize_segment(
    Segment &segment);

  struct record_size_t {
    extent_len_t mdlength = 0;
    extent_len_t dlength = 0;

    record_size_t(
      extent_len_t mdlength,
      extent_len_t dlength)
      : mdlength(mdlength), dlength(dlength) {}
  };

  /**
   * Return <mdlength, dlength> pair denoting length of
   * metadata and blocks respectively.
   */
  record_size_t get_encoded_record_length(
    const record_t &record) const;

  /// create encoded record bl
  ceph::bufferlist encode_record(
    record_size_t rsize,
    record_t &&record);

  /// do record write
  using write_record_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  write_record_ertr::future<> write_record(
    record_size_t rsize,
    record_t &&record);

  /// close current segment and initialize next one
  using roll_journal_segment_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  roll_journal_segment_ertr::future<> roll_journal_segment();

  /// returns true iff current segment has insufficient space
  bool needs_roll(segment_off_t length) const;

  /// returns next record addr
  paddr_t next_record_addr() const;

  /// return ordered vector of segments to replay
  using find_replay_segments_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  using find_replay_segments_fut =
    find_replay_segments_ertr::future<std::vector<paddr_t>>;
  find_replay_segments_fut find_replay_segments();

  /// read record metadata for record starting at start
  using read_record_metadata_ertr = replay_ertr;
  using read_record_metadata_ret = read_record_metadata_ertr::future<
    std::optional<std::pair<record_header_t, bufferlist>>
    >;
  read_record_metadata_ret read_record_metadata(
    paddr_t start);

  /// attempts to decode deltas from bl, return nullopt if unsuccessful
  std::optional<std::vector<delta_info_t>> try_decode_deltas(
    record_header_t header,
    bufferlist &bl);

  /// replays records starting at start through end of segment
  replay_ertr::future<>
  replay_segment(
    paddr_t start,                 ///< [in] starting addr
    delta_handler_t &delta_handler ///< [in] processes deltas in order
  );
};

}
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::segment_header_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::record_header_t)
