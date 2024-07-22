// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <memory>

#include "crimson/os/seastore/ordering_handle.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/segment_seq_allocator.h"
#include "crimson/os/seastore/cached_extent.h"

namespace crimson::os::seastore {

namespace random_block_device {
class RBMDevice;
}

class SegmentManagerGroup;
class SegmentProvider;
class JournalTrimmer;

class Journal {
public:
  virtual JournalTrimmer &get_trimmer() = 0;

  virtual writer_stats_t get_writer_stats() const = 0;

  /**
   * initializes journal for mkfs writes -- must run prior to calls
   * to submit_record.
   */
  using open_for_mkfs_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  using open_for_mkfs_ret = open_for_mkfs_ertr::future<journal_seq_t>;
  virtual open_for_mkfs_ret open_for_mkfs() = 0;

  /**
   * initializes journal for new writes -- must run prior to calls
   * to submit_record.  Should be called after replay if not a new
   * Journal.
   */
  using open_for_mount_ertr = open_for_mkfs_ertr;
  using open_for_mount_ret = open_for_mkfs_ret;
  virtual open_for_mount_ret open_for_mount() = 0;

  /// close journal
  using close_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  virtual close_ertr::future<> close() = 0;

  /**
   * submit_record
   *
   * write record with the ordering handle
   */
  using submit_record_ertr = crimson::errorator<
    crimson::ct_error::erange,
    crimson::ct_error::input_output_error
    >;
  using submit_record_ret = submit_record_ertr::future<
    record_locator_t
    >;
  virtual submit_record_ret submit_record(
    record_t &&record,
    OrderingHandle &handle
  ) = 0;

  /**
   * flush
   *
   * Wait for all outstanding IOs on handle to commit.
   * Note, flush() machinery must go through the same pipeline
   * stages and locks as submit_record.
   */
  virtual seastar::future<> flush(OrderingHandle &handle) = 0;

  /// sets write pipeline reference
  virtual void set_write_pipeline(WritePipeline *_write_pipeline) = 0;

  /**
   * Read deltas and pass to delta_handler
   *
   * record_block_start (argument to delta_handler) is the start of the
   * of the first block in the record
   */
  using replay_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange>;
  using replay_ret = replay_ertr::future<>;
  using delta_handler_t = std::function<
    replay_ertr::future<std::pair<bool, CachedExtentRef>>(
      const record_locator_t&,
      const delta_info_t&,
      const journal_seq_t&, // dirty_tail
      const journal_seq_t&, // alloc_tail
      sea_time_point modify_time)>;
  virtual replay_ret replay(
    delta_handler_t &&delta_handler) = 0;

  virtual seastar::future<> finish_commit(
    transaction_type_t type) = 0;

  virtual ~Journal() {}

  virtual backend_type_t get_type() = 0;
};
using JournalRef = std::unique_ptr<Journal>;

namespace journal {

JournalRef make_segmented(
  SegmentProvider &provider,
  JournalTrimmer &trimmer);

JournalRef make_circularbounded(
  JournalTrimmer &trimmer,
  crimson::os::seastore::random_block_device::RBMDevice* device,
  std::string path);

}

}
