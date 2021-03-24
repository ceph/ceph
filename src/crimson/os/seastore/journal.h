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
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/ordering_handle.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/osd/exceptions.h"

namespace crimson::os::seastore {

using segment_nonce_t = uint32_t;


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
  segment_nonce_t segment_nonce;

  DENC(segment_header_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.journal_segment_seq, p);
    denc(v.physical_segment_id, p);
    denc(v.journal_tail, p);
    denc(v.segment_nonce, p);
    DENC_FINISH(p);
  }
};
std::ostream &operator<<(std::ostream &out, const segment_header_t &header);

struct record_header_t {
  // Fixed portion
  extent_len_t  mdlength;       // block aligned, length of metadata
  extent_len_t  dlength;        // block aligned, length of data
  uint32_t deltas;                // number of deltas
  uint32_t extents;               // number of extents
  segment_nonce_t segment_nonce;// nonce of containing segment
  segment_off_t committed_to;   // records in this segment prior to committed_to
                                // have been fully written
  checksum_t data_crc;          // crc of data payload


  DENC(record_header_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.mdlength, p);
    denc(v.dlength, p);
    denc(v.deltas, p);
    denc(v.extents, p);
    denc(v.segment_nonce, p);
    denc(v.committed_to, p);
    denc(v.data_crc, p);
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
std::ostream &operator<<(std::ostream &out, const extent_info_t &header);

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

  virtual segment_seq_t get_seq(segment_id_t id) { return 0; }

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
  submit_record_ret submit_record(
    record_t &&record,
    OrderingHandle &handle
  ) {
    assert(write_pipeline);
    auto rsize = get_encoded_record_length(record);
    auto total = rsize.mdlength + rsize.dlength;
    if (total > max_record_length) {
      auto &logger = crimson::get_logger(ceph_subsys_filestore);
      logger.error(
	"Journal::submit_record: record size {} exceeds max {}",
	total,
	max_record_length
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
  replay_ret replay(delta_handler_t &&delta_handler);

  /**
   * scan_extents
   *
   * Scans records beginning at addr until the first record boundary after
   * addr + bytes_to_read.
   *
   * Returns list<extent, extent_info>
   * cursor.is_complete() will be true when no further extents exist in segment.
   */
  class scan_valid_records_cursor;
  using scan_extents_cursor = scan_valid_records_cursor;
  using scan_extents_ertr = SegmentManager::read_ertr;
  using scan_extents_ret_bare = std::list<std::pair<paddr_t, extent_info_t>>;
  using scan_extents_ret = scan_extents_ertr::future<scan_extents_ret_bare>;
  scan_extents_ret scan_extents(
    scan_extents_cursor &cursor,
    extent_len_t bytes_to_read
  );

  void set_write_pipeline(WritePipeline *_write_pipeline) {
    write_pipeline = _write_pipeline;
  }

private:
  const extent_len_t block_size;
  const extent_len_t max_record_length;

  JournalSegmentProvider *segment_provider = nullptr;
  SegmentManager &segment_manager;

  segment_seq_t next_journal_segment_seq = 0;
  segment_nonce_t current_segment_nonce = 0;

  SegmentRef current_journal_segment;
  segment_off_t written_to = 0;
  segment_off_t committed_to = 0;

  WritePipeline *write_pipeline = nullptr;

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

  /// validate embedded metadata checksum
  static bool validate_metadata(const bufferlist &bl);

  /// read and validate data
  using read_validate_data_ertr = SegmentManager::read_ertr;
  using read_validate_data_ret = read_validate_data_ertr::future<bool>;
  read_validate_data_ret read_validate_data(
    paddr_t record_base,
    const record_header_t &header  ///< caller must ensure lifetime through
                                   ///  future resolution
  );


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

  using read_segment_header_ertr = crimson::errorator<
    crimson::ct_error::enoent,
    crimson::ct_error::enodata,
    crimson::ct_error::input_output_error
    >;
  using read_segment_header_ret = read_segment_header_ertr::future<
    segment_header_t>;
  read_segment_header_ret read_segment_header(segment_id_t segment);

  /// return ordered vector of segments to replay
  using replay_segments_t = std::vector<
    std::pair<journal_seq_t, segment_header_t>>;
  using find_replay_segments_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  using find_replay_segments_fut = find_replay_segments_ertr::future<
    replay_segments_t>;
  find_replay_segments_fut find_replay_segments();

  /// attempts to decode deltas from bl, return nullopt if unsuccessful
  std::optional<std::vector<delta_info_t>> try_decode_deltas(
    record_header_t header,
    const bufferlist &bl);

  /// attempts to decode extent infos from bl, return nullopt if unsuccessful
  std::optional<std::vector<extent_info_t>> try_decode_extent_infos(
    record_header_t header,
    const bufferlist &bl);

  /// read record metadata for record starting at start
  using read_validate_record_metadata_ertr = replay_ertr;
  using read_validate_record_metadata_ret =
    read_validate_record_metadata_ertr::future<
      std::optional<std::pair<record_header_t, bufferlist>>
    >;
  read_validate_record_metadata_ret read_validate_record_metadata(
    paddr_t start,
    segment_nonce_t nonce);

public:
  /// scan segment for end incrementally
  struct scan_valid_records_cursor {
    bool last_valid_header_found = false;
    paddr_t offset;
    paddr_t last_committed;

    struct found_record_t {
      paddr_t offset;
      record_header_t header;
      bufferlist mdbuffer;

      found_record_t(
	paddr_t offset,
	const record_header_t &header,
	const bufferlist &mdbuffer)
	: offset(offset), header(header), mdbuffer(mdbuffer) {}
    };
    std::deque<found_record_t> pending_records;

    bool is_complete() const {
      return last_valid_header_found && pending_records.empty();
    }

    paddr_t get_offset() const {
      return offset;
    }

    scan_valid_records_cursor(
      paddr_t offset)
      : offset(offset) {}
  };
private:

  using scan_valid_records_ertr = SegmentManager::read_ertr;
  using scan_valid_records_ret = scan_valid_records_ertr::future<
    size_t>;
  using found_record_handler_t = std::function<
    scan_valid_records_ertr::future<>(
      paddr_t record_block_base,
      // callee may assume header and bl will remain valid until
      // returned future resolves
      const record_header_t &header,
      const bufferlist &bl)>;
  scan_valid_records_ret scan_valid_records(
    scan_valid_records_cursor &cursor, ///< [in, out] cursor, updated during call
    segment_nonce_t nonce,             ///< [in] nonce for segment
    size_t budget,                     ///< [in] max budget to use
    found_record_handler_t &handler    ///< [in] handler for records
  ); ///< @return used budget

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
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::segment_header_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::record_header_t)
WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::extent_info_t)
