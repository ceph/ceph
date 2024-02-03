// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include <set>

#include "crimson/common/errorator.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/record_scanner.h"

namespace crimson::os::seastore {

class SegmentManagerGroup : public RecordScanner {
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
    if (!device_ids.empty()) {
      auto existing_id = *device_ids.begin();
      ceph_assert(segment_managers[existing_id]->get_device_type()
                  == segment_manager->get_device_type());
    }
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
  extent_len_t get_block_size() const {
    assert(device_ids.size());
    return segment_managers[*device_ids.begin()]->get_block_size();
  }

  segment_off_t get_segment_size() const {
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

  void initialize_cursor(scan_valid_records_cursor &cursor) final;

  read_ret read(paddr_t start, size_t len) final;

  bool is_record_segment_seq_invalid(scan_valid_records_cursor &cursor,
    record_group_header_t &header) final {
    return false;
  }

  int64_t get_segment_end_offset(paddr_t addr) final {
    auto& seg_addr = addr.as_seg_paddr();
    auto& segment_manager = *segment_managers[seg_addr.get_segment_id().device_id()];
    return static_cast<int64_t>(segment_manager.get_segment_size());
  }

  std::vector<SegmentManager*> segment_managers;
  std::set<device_id_t> device_ids;
};

using SegmentManagerGroupRef = std::unique_ptr<SegmentManagerGroup>;

} // namespace crimson::os::seastore
