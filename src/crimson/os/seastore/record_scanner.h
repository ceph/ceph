// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include "crimson/common/errorator.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/segment_manager.h"


namespace crimson::os::seastore {

class RecordScanner {
public:
  using read_ertr = SegmentManager::read_ertr;
  using scan_valid_records_ertr = read_ertr;
  using scan_valid_records_ret = scan_valid_records_ertr::future<>;
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

  device_off_t get_segment_off(paddr_t addr) const {
    if (addr.get_addr_type() == paddr_types_t::SEGMENT) {
      auto& seg_addr = addr.as_seg_paddr();
      return seg_addr.get_segment_off();
    }
    assert(addr.get_addr_type() == paddr_types_t::RANDOM_BLOCK);
    auto& blk_addr = addr.as_blk_paddr();
    return blk_addr.get_device_off();
  }

protected:
  /// read record metadata for record starting at start
  using read_validate_record_metadata_ertr = read_ertr;
  using read_validate_record_metadata_ret =
    read_validate_record_metadata_ertr::future<
      std::optional<std::pair<record_group_header_t, bufferlist>>
    >;
  read_validate_record_metadata_ret read_validate_record_metadata(
    scan_valid_records_cursor &cursor,
    segment_nonce_t nonce);

  /// read and validate data
  using read_validate_data_ertr = read_ertr;
  using read_validate_data_ret = read_validate_data_ertr::future<bool>;
  read_validate_data_ret read_validate_data(
    paddr_t record_base,
    const record_group_header_t &header  ///< caller must ensure lifetime through
                                         ///  future resolution
  );

  virtual bool is_record_segment_seq_invalid(scan_valid_records_cursor &cursor,
    record_group_header_t &h) = 0;

  virtual int64_t get_segment_end_offset(paddr_t addr) = 0;

  using read_ret = read_ertr::future<bufferptr>;
  virtual read_ret read(paddr_t start, size_t len) = 0;

  using consume_record_group_ertr = scan_valid_records_ertr;
  consume_record_group_ertr::future<> consume_next_records(
      scan_valid_records_cursor& cursor,
      found_record_handler_t& handler,
      std::size_t& budget_used);

  virtual void initialize_cursor(scan_valid_records_cursor &cursor) = 0;

  virtual ~RecordScanner() {}

};

}
