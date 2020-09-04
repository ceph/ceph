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

/**
 * Segment header
 *
 * Every segment contains and encode segment_header_t in the first block.
 * Our strategy for finding the journal replay point is:
 * 1) Find the segment with the highest journal_segment_seq
 * 2) Replay starting at record located at that segment's journal_tail
 */
struct segment_header_t {
  segment_seq_t journal_segment_seq;
  segment_id_t physical_segment_id; // debugging

  journal_seq_t journal_tail;

  DENC(segment_header_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.journal_segment_seq, p);
    denc(v.physical_segment_id, p);
    denc(v.journal_tail, p);
    DENC_FINISH(p);
  }
};

struct record_header_t {
  // Fixed portion
  extent_len_t  mdlength;       // block aligned, length of metadata
  extent_len_t  dlength;        // block aligned, length of data
  checksum_t    full_checksum;  // checksum for full record (TODO)
  size_t deltas;                // number of deltas
  size_t extents;               // number of extents

  DENC(record_header_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.mdlength, p);
    denc(v.dlength, p);
    denc(v.full_checksum, p);
    denc(v.deltas, p);
    denc(v.extents, p);
    DENC_FINISH(p);
  }
};

struct extent_info_t {
  extent_types_t type = extent_types_t::NONE;
  laddr_t addr = L_ADDR_NULL;
  extent_len_t len = 0;

  extent_info_t() = default;
  extent_info_t(const extent_t &et)
    : type(et.type), addr(et.addr), len(et.bl.length()) {}

  DENC(extent_info_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.type, p);
    denc(v.addr, p);
    denc(v.len, p);
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

  virtual void close_segment(segment_id_t) {}

  virtual void set_journal_segment(
    segment_id_t segment,
    segment_seq_t seq) {}

  virtual journal_seq_t get_journal_tail_target() const = 0;
  virtual void update_journal_tail_committed(journal_seq_t tail_committed) = 0;

  virtual void init_mark_segment_closed(
    segment_id_t segment, segment_seq_t seq) {}

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
  close_ertr::future<> close() { return close_ertr::now(); }

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
  submit_record_ret submit_record(record_t &&record) {
    auto rsize = get_encoded_record_length(record);
    auto total = rsize.mdlength + rsize.dlength;
    if (total > max_record_length) {
      return crimson::ct_error::erange::make();
    }
    auto roll = needs_roll(total)
      ? roll_journal_segment().safe_then([](auto){})
      : roll_journal_segment_ertr::now();
    return roll.safe_then(
      [this, rsize, record=std::move(record)]() mutable {
	return write_record(rsize, std::move(record)
	).safe_then([this, rsize](auto addr) {
	  return std::make_pair(
	    addr.add_offset(rsize.mdlength),
	    get_journal_seq(addr));
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
  replay_ret replay(delta_handler_t &&delta_handler);

  /**
   * scan_extents
   *
   * Scans records beginning at addr until the first record boundary after
   * addr + bytes_to_read.
   *
   * Returns <next_addr, list<extent, extent_info>>
   * next_addr will be P_ADDR_NULL if no further extents exist in segment.
   * If addr.offset == 0, scan will adjust to first record in segment.
   */
  using scan_extents_ertr = SegmentManager::read_ertr;
  using scan_extents_ret_bare = std::pair<
    paddr_t,
    std::list<std::pair<paddr_t, extent_info_t>>
    >;
  using scan_extents_ret = scan_extents_ertr::future<scan_extents_ret_bare>;
  scan_extents_ret scan_extents(
    paddr_t addr,
    extent_len_t bytes_to_read
  );


private:
  const extent_len_t block_size;
  const extent_len_t max_record_length;

  JournalSegmentProvider *segment_provider = nullptr;
  SegmentManager &segment_manager;

  segment_seq_t current_journal_segment_seq = 0;

  SegmentRef current_journal_segment;
  segment_off_t written_to = 0;

  segment_id_t next_journal_segment_seq = NULL_SEG_ID;

  journal_seq_t get_journal_seq(paddr_t addr) {
    return journal_seq_t{current_journal_segment_seq, addr};
  }

  /// prepare segment for writes, writes out segment header
  using initialize_segment_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  initialize_segment_ertr::future<segment_seq_t> initialize_segment(
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
  using write_record_ret = write_record_ertr::future<paddr_t>;
  write_record_ret write_record(
    record_size_t rsize,
    record_t &&record);

  /// close current segment and initialize next one
  using roll_journal_segment_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  roll_journal_segment_ertr::future<segment_seq_t> roll_journal_segment();

  /// returns true iff current segment has insufficient space
  bool needs_roll(segment_off_t length) const;

  /// return ordered vector of segments to replay
  using find_replay_segments_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  using find_replay_segments_fut = find_replay_segments_ertr::future<
    std::vector<journal_seq_t>>;
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

  /// attempts to decode extent infos from bl, return nullopt if unsuccessful
  std::optional<std::vector<extent_info_t>> try_decode_extent_infos(
    record_header_t header,
    bufferlist &bl);

  /**
   * scan_segment
   *
   * Scans bytes_to_read forward from addr to the first record after
   * addr+bytes_to_read invoking delta_handler and extent_info_handler
   * on deltas and extent_infos respectively.  deltas, extent_infos
   * will only be decoded if the corresponding handler is included.
   *
   * @return next address to read from, P_ADDR_NULL if segment complete
   */
  using scan_segment_ertr = SegmentManager::read_ertr;
  using scan_segment_ret = scan_segment_ertr::future<paddr_t>;
  using delta_scan_handler_t = std::function<
    replay_ret(paddr_t record_start,
	       paddr_t record_block_base,
	       const delta_info_t&)>;
  using extent_handler_t = std::function<
    scan_segment_ertr::future<>(paddr_t addr,
				const extent_info_t &info)>;
  scan_segment_ret scan_segment(
    paddr_t addr,
    extent_len_t bytes_to_read,
    delta_scan_handler_t *delta_handler,
    extent_handler_t *extent_info_handler
  );

  /// replays records starting at start through end of segment
  replay_ertr::future<>
  replay_segment(
    journal_seq_t start,             ///< [in] starting addr, seq
    delta_handler_t &delta_handler   ///< [in] processes deltas in order
  );

};

}
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::segment_header_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::record_header_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::extent_info_t)
