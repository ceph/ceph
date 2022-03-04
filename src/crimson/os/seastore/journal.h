// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <memory>

#include "crimson/os/seastore/ordering_handle.h"
#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore {

namespace nvme_device {
class NVMeBlockDevice;
}

class SegmentManager;
class ExtentReader;
class SegmentProvider;

class Journal {
public:
  /**
   * initializes journal for new writes -- must run prior to calls
   * to submit_record.  Should be called after replay if not a new
   * Journal.
   */
  using open_for_write_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  using open_for_write_ret = open_for_write_ertr::future<journal_seq_t>;
  virtual open_for_write_ret open_for_write() = 0;

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
    replay_ret(const record_locator_t&,
	       const delta_info_t&,
	       seastar::lowres_system_clock::time_point last_modified)>;
  virtual replay_ret replay(
    delta_handler_t &&delta_handler) = 0;

  virtual ~Journal() {}
};
using JournalRef = std::unique_ptr<Journal>;

namespace journal {

JournalRef make_segmented(
  SegmentManager &sm,
  ExtentReader &reader,
  SegmentProvider &provider);

}

}
