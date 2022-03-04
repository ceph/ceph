// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include "crimson/common/errorator.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/logging.h"

namespace crimson::os::seastore {

class SegmentCleaner;
class TransactionManager;

class ExtentReader {
public:
  seastore_off_t get_block_size() const {
    assert(segment_managers.size());
    // assume all segment managers have the same block size
    return segment_managers[0]->get_block_size();
  }

  using read_ertr = SegmentManager::read_ertr;
  ExtentReader() {
    segment_managers.resize(DEVICE_ID_MAX, nullptr);
  }
  using read_segment_header_ertr = crimson::errorator<
    crimson::ct_error::enoent,
    crimson::ct_error::enodata,
    crimson::ct_error::input_output_error
    >;
  using read_segment_header_ret = read_segment_header_ertr::future<
    segment_header_t>;
  read_segment_header_ret read_segment_header(segment_id_t segment);

  using read_segment_tail_ertr = read_segment_header_ertr;
  using read_segment_tail_ret = read_segment_tail_ertr::future<
    segment_tail_t>;
  read_segment_tail_ret  read_segment_tail(segment_id_t segment);

  struct commit_info_t {
    mod_time_point_t commit_time;
    record_commit_type_t commit_type;
  };

  /**
   * scan_extents
   *
   * Scans records beginning at addr until the first record boundary after
   * addr + bytes_to_read.
   *
   * Returns list<extent, extent_info>
   * cursor.is_complete() will be true when no further extents exist in segment.
   */
  using scan_extents_cursor = scan_valid_records_cursor;
  using scan_extents_ertr = read_ertr::extend<crimson::ct_error::enodata>;
  using scan_extents_ret_bare =
    std::list<std::pair<paddr_t, std::pair<commit_info_t, extent_info_t>>>;
  using scan_extents_ret = scan_extents_ertr::future<scan_extents_ret_bare>;
  scan_extents_ret scan_extents(
    scan_extents_cursor &cursor,
    extent_len_t bytes_to_read
  );

  using scan_valid_records_ertr = read_ertr::extend<crimson::ct_error::enodata>;
  using scan_valid_records_ret = scan_valid_records_ertr::future<
    size_t>;
  using found_record_handler_t = std::function<
    scan_valid_records_ertr::future<>(
      record_locator_t record_locator,
      // callee may assume header and bl will remain valid until
      // returned future resolves
      const record_group_header_t &header,
      const bufferlist &mdbuf)>;
  scan_valid_records_ret scan_valid_records(
    scan_valid_records_cursor &cursor, ///< [in, out] cursor, updated during call
    segment_nonce_t nonce,             ///< [in] nonce for segment
    size_t budget,                     ///< [in] max budget to use
    found_record_handler_t &handler    ///< [in] handler for records
  ); ///< @return used budget

  void add_segment_manager(SegmentManager* segment_manager) {
    assert(!segment_managers[segment_manager->get_device_id()] ||
      segment_manager == segment_managers[segment_manager->get_device_id()]);
    segment_managers[segment_manager->get_device_id()] = segment_manager;
  }

  read_ertr::future<> read(
    paddr_t addr,
    size_t len,
    ceph::bufferptr &out) {
    assert(segment_managers[addr.get_device_id()]);
    return segment_managers[addr.get_device_id()]->read(addr, len, out);
  }

private:
  std::vector<SegmentManager*> segment_managers;

  std::vector<SegmentManager*>& get_segment_managers() {
    return segment_managers;
  }
  /// read record metadata for record starting at start
  using read_validate_record_metadata_ertr = read_ertr;
  using read_validate_record_metadata_ret =
    read_validate_record_metadata_ertr::future<
      std::optional<std::pair<record_group_header_t, bufferlist>>
    >;
  read_validate_record_metadata_ret read_validate_record_metadata(
    paddr_t start,
    segment_nonce_t nonce);

  /// read and validate data
  using read_validate_data_ertr = read_ertr;
  using read_validate_data_ret = read_validate_data_ertr::future<bool>;
  read_validate_data_ret read_validate_data(
    paddr_t record_base,
    const record_group_header_t &header  ///< caller must ensure lifetime through
                                         ///  future resolution
  );

  using consume_record_group_ertr = scan_valid_records_ertr;
  consume_record_group_ertr::future<> consume_next_records(
      scan_valid_records_cursor& cursor,
      found_record_handler_t& handler,
      std::size_t& budget_used);

  friend class TransactionManager;
};

using ExtentReaderRef = std::unique_ptr<ExtentReader>;

} // namespace crimson::os::seastore
