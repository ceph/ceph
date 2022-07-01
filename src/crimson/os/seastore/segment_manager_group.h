// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include <set>

#include "crimson/common/errorator.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/segment_manager.h"

namespace crimson::os::seastore {

class SegmentManagerGroup {
public:
  SegmentManagerGroup() {
    segment_managers.resize(DEVICE_ID_MAX, nullptr);
  }

  const std::set<device_id_t>& get_device_ids() const {
    return device_ids;
  }

  std::vector<SegmentManager*> get_segment_managers() const {
    assert(device_ids.size());
    std::vector<SegmentManager*> ret;
    for (auto& device_id : device_ids) {
      auto segment_manager = segment_managers[device_id];
      assert(segment_manager->get_device_id() == device_id);
      ret.emplace_back(segment_manager);
    }
    return ret;
  }

  void add_segment_manager(SegmentManager* segment_manager) {
    auto device_id = segment_manager->get_device_id();
    ceph_assert(!has_device(device_id));
    segment_managers[device_id] = segment_manager;
    device_ids.insert(device_id);
  }

  void reset() {
    segment_managers.clear();
    segment_managers.resize(DEVICE_ID_MAX, nullptr);
    device_ids.clear();
  }

  /**
   * get device info
   *
   * Assume all segment managers share the same following information.
   */
  seastore_off_t get_block_size() const {
    assert(device_ids.size());
    return segment_managers[*device_ids.begin()]->get_block_size();
  }

  seastore_off_t get_segment_size() const {
    assert(device_ids.size());
    return segment_managers[*device_ids.begin()]->get_segment_size();
  }

  const seastore_meta_t &get_meta() const {
    assert(device_ids.size());
    return segment_managers[*device_ids.begin()]->get_meta();
  }

  std::size_t get_rounded_header_length() const {
    return p2roundup(
      ceph::encoded_sizeof_bounded<segment_header_t>(),
      (std::size_t)get_block_size());
  }

  std::size_t get_rounded_tail_length() const {
    return p2roundup(
      ceph::encoded_sizeof_bounded<segment_tail_t>(),
      (std::size_t)get_block_size());
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

  using read_ertr = SegmentManager::read_ertr;
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

  /*
   * read journal segment headers
   */
  using find_journal_segment_headers_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using find_journal_segment_headers_ret_bare = std::vector<
    std::pair<segment_id_t, segment_header_t>>;
  using find_journal_segment_headers_ret = find_journal_segment_headers_ertr::future<
    find_journal_segment_headers_ret_bare>;
  find_journal_segment_headers_ret find_journal_segment_headers();

  using open_ertr = SegmentManager::open_ertr;
  open_ertr::future<SegmentRef> open(segment_id_t id) {
    assert(has_device(id.device_id()));
    return segment_managers[id.device_id()]->open(id);
  }

  using release_ertr = SegmentManager::release_ertr;
  release_ertr::future<> release_segment(segment_id_t id) {
    assert(has_device(id.device_id()));
    return segment_managers[id.device_id()]->release(id);
  }

private:
  bool has_device(device_id_t id) const {
    assert(id <= DEVICE_ID_MAX_VALID);
    return device_ids.count(id) >= 1;
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

  std::vector<SegmentManager*> segment_managers;
  std::set<device_id_t> device_ids;
};

using SegmentManagerGroupRef = std::unique_ptr<SegmentManagerGroup>;

} // namespace crimson::os::seastore
