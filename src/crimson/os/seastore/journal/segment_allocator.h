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
class SegmentAllocator {
  using base_ertr = crimson::errorator<
      crimson::ct_error::input_output_error>;

 public:
  SegmentAllocator(JournalTrimmer *trimmer,
                   data_category_t category,
                   rewrite_gen_t gen,
                   SegmentProvider &sp,
                   SegmentSeqAllocator &ssa);

  const std::string& get_name() const {
    return print_name;
  }

  SegmentProvider &get_provider() {
    return segment_provider;
  }

  extent_len_t get_block_size() const {
    return sm_group.get_block_size();
  }

  extent_len_t get_max_write_length() const {
    return sm_group.get_segment_size() -
           sm_group.get_rounded_header_length() -
           sm_group.get_rounded_tail_length();
  }

  bool can_write() const {
    return !!current_segment;
  }

  segment_id_t get_segment_id() const {
    assert(can_write());
    return current_segment->get_segment_id();
  }

  segment_nonce_t get_nonce() const {
    assert(can_write());
    return current_segment_nonce;
  }

  segment_off_t get_written_to() const {
    assert(can_write());
    return written_to;
  }

  // returns true iff the current segment has insufficient space
  bool needs_roll(std::size_t length) const {
    assert(can_write());
    assert(current_segment->get_write_capacity() ==
           sm_group.get_segment_size());
    auto write_capacity = current_segment->get_write_capacity() -
                          sm_group.get_rounded_tail_length();
    return length + written_to > std::size_t(write_capacity);
  }

  // open for write and generate the correct print name
  using open_ertr = base_ertr;
  using open_ret = open_ertr::future<journal_seq_t>;
  open_ret open(bool is_mkfs);

  // close the current segment and initialize next one
  using roll_ertr = base_ertr;
  roll_ertr::future<> roll();

  // write the buffer, return the write result
  //
  // May be called concurrently, but writes may complete in any order.
  // If rolling/opening, no write is allowed.
  using write_ertr = base_ertr;
  using write_ret = write_ertr::future<write_result_t>;
  write_ret write(ceph::bufferlist&& to_write);

  using close_ertr = base_ertr;
  close_ertr::future<> close();

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
