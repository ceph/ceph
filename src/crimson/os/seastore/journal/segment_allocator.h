// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include <optional>
#include <seastar/core/circular_buffer.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/shared_future.hh>

#include "include/buffer.h"

#include "crimson/common/errorator.h"
#include "crimson/os/seastore/segment_manager_group.h"
#include "crimson/os/seastore/segment_seq_allocator.h"
#include "crimson/os/seastore/journal/record_submitter.h"
#include "crimson/os/seastore/async_cleaner.h"

namespace crimson::os::seastore {
  class SegmentProvider;
  class JournalTrimmer;
}

namespace crimson::os::seastore::journal {

/**
 * SegmentAllocator
 *
 * Maintain an available segment for writes.
 */
class SegmentAllocator : public JournalAllocator {

 public:
  // SegmentAllocator specific methods
  SegmentAllocator(JournalTrimmer *trimmer,
                   data_category_t category,
                   rewrite_gen_t gen,
                   SegmentProvider &sp,
                   SegmentSeqAllocator &ssa);

  segment_id_t get_segment_id() const {
    assert(can_write());
    return current_segment->get_segment_id();
  }

  extent_len_t get_max_write_length() const {
    return sm_group.get_segment_size() -
           sm_group.get_rounded_header_length() -
           sm_group.get_rounded_tail_length();
  }

 public:
  // overriding methods
  const std::string& get_name() const final {
    return print_name;
  }

  extent_len_t get_block_size() const final {
    return sm_group.get_block_size();
  }

  bool can_write() const final {
    return !!current_segment;
  }

  segment_nonce_t get_nonce() const final {
    assert(can_write());
    return current_segment_nonce;
  }

  // returns true iff the current segment has insufficient space
  bool needs_roll(std::size_t length) const final {
    assert(can_write());
    assert(current_segment->get_write_capacity() ==
           sm_group.get_segment_size());
    auto write_capacity = current_segment->get_write_capacity() -
                          sm_group.get_rounded_tail_length();
    return length + written_to > std::size_t(write_capacity);
  }

  // open for write and generate the correct print name
  open_ret open(bool is_mkfs) final;

  // close the current segment and initialize next one
  roll_ertr::future<> roll() final;

  // write the buffer, return the write result
  //
  // May be called concurrently, but writes may complete in any order.
  // If rolling/opening, no write is allowed.
  write_ret write(ceph::bufferlist&& to_write) final;

  using close_ertr = base_ertr;
  close_ertr::future<> close() final;

  void update_modify_time(record_t& record) final {
    segment_provider.update_modify_time(
      get_segment_id(),
      record.modify_time,
      record.extents.size());
  }

 private:
  open_ret do_open(bool is_mkfs);

  void reset() {
    current_segment.reset();
    written_to = 0;

    current_segment_nonce = 0;
  }

  using close_segment_ertr = base_ertr;
  close_segment_ertr::future<> close_segment();

  // device id is not available during construction,
  // so generate the print_name later.
  std::string print_name;
  const segment_type_t type; // JOURNAL or OOL
  const data_category_t category;
  const rewrite_gen_t gen;
  SegmentProvider &segment_provider;
  SegmentManagerGroup &sm_group;
  SegmentRef current_segment;
  segment_off_t written_to;
  SegmentSeqAllocator &segment_seq_allocator;
  segment_nonce_t current_segment_nonce;
  JournalTrimmer *trimmer;
};

}
