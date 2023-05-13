// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/common/log.h"

#include <boost/intrusive_ptr.hpp>

#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "include/buffer.h"
#include "include/denc.h"

#include "crimson/osd/exceptions.h"
#include "crimson/os/seastore/journal.h"
#include "include/uuid.h"
#include "crimson/os/seastore/random_block_manager.h"
#include "crimson/os/seastore/random_block_manager/rbm_device.h"
#include <list>
#include "crimson/os/seastore/journal/record_submitter.h"
#include "crimson/os/seastore/journal/circular_journal_space.h"

namespace crimson::os::seastore::journal {

using RBMDevice = random_block_device::RBMDevice;

/**
 * CircularBoundedJournal
 *
 * 
 * CircularBoundedJournal (CBJournal) is the journal that works like circular
 * queue. With CBJournal, Seastore will append some of the records if the size
 * of the record is small (most likely metadata), at which point the head
 * (written_to) will be moved. Then, eventually, Seastore applies the records
 * in CBjournal to RBM (TODO).
 *
 * - Commit time
 * After submit_record is done, written_to is increased(this in-memory value)
 * ---written_to represents where the new record will be appended. Note that
 * applied_to is not changed here.
 *
 * - Replay time
 * At replay time, CBJournal begins to replay records in CBjournal by reading
 * records from dirty_tail. Then, CBJournal examines whether the records is valid
 * one by one, at which point written_to is recovered
 * if the valid record is founded. Note that applied_to is stored
 * permanently when the apply work---applying the records in CBJournal to RBM---
 * is done by CBJournal (TODO).
 *
 * TODO: apply records from CircularBoundedJournal to RandomBlockManager
 *
 */

constexpr uint64_t DEFAULT_BLOCK_SIZE = 4096;

class CircularBoundedJournal : public Journal {
public:
  CircularBoundedJournal(
      JournalTrimmer &trimmer, RBMDevice* device, const std::string &path);
  ~CircularBoundedJournal() {}

  JournalTrimmer &get_trimmer() final {
    return trimmer;
  }

  open_for_mkfs_ret open_for_mkfs() final;

  open_for_mount_ret open_for_mount() final;

  close_ertr::future<> close() final;

  journal_type_t get_type() final {
    return journal_type_t::RANDOM_BLOCK;
  }

  submit_record_ret submit_record(
    record_t &&record,
    OrderingHandle &handle
  ) final;

  seastar::future<> flush(
    OrderingHandle &handle
  ) final {
    // TODO
    return seastar::now();
  }

  replay_ret replay(delta_handler_t &&delta_handler) final;

  rbm_abs_addr get_rbm_addr(journal_seq_t seq) const {
    return convert_paddr_to_abs_addr(seq.offset);
  }

  /**
   *
   * CircularBoundedJournal write
   *
   * NVMe will support a large block write (< 512KB) with atomic write unit command.
   * With this command, we expect that the most of incoming data can be stored
   * as a single write call, which has lower overhead than existing
   * way that uses a combination of system calls such as write() and sync().
   *
   */

  seastar::future<> update_journal_tail(
    journal_seq_t dirty,
    journal_seq_t alloc) {
    return cjs.update_journal_tail(dirty, alloc);
  }
  journal_seq_t get_dirty_tail() const {
    return cjs.get_dirty_tail();
  }
  journal_seq_t get_alloc_tail() const {
    return cjs.get_alloc_tail();
  }

  using read_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange>;
  using read_record_ertr = read_ertr;
  using read_record_ret = read_record_ertr::future<
	std::optional<std::pair<record_group_header_t, bufferlist>>
	>;
  /*
   * read_record
   *
   * read record from given address
   *
   * @param paddr_t to read
   * @param expected_seq
   *
   */
  read_record_ret read_record(paddr_t offset, segment_seq_t expected_seq);

  read_record_ret return_record(record_group_header_t& header, bufferlist bl);

  void set_write_pipeline(WritePipeline *_write_pipeline) final {
    write_pipeline = _write_pipeline;
  }

  device_id_t get_device_id() const {
    return cjs.get_device_id();
  }
  extent_len_t get_block_size() const {
    return cjs.get_block_size();
  }

  rbm_abs_addr get_journal_end() const {
    return cjs.get_journal_end();
  }

  void set_written_to(journal_seq_t seq) {
    cjs.set_written_to(seq);
  }

  journal_seq_t get_written_to() {
    return cjs.get_written_to();
  }

  rbm_abs_addr get_records_start() const {
    return cjs.get_records_start();
  }

  seastar::future<> finish_commit(transaction_type_t type) final;

  using cbj_delta_handler_t = std::function<
  replay_ertr::future<bool>(
    const record_locator_t&,
    const delta_info_t&,
    sea_time_point modify_time)>;

  Journal::replay_ret scan_valid_record_delta(
    cbj_delta_handler_t &&delta_handler,
    journal_seq_t tail);

  submit_record_ret do_submit_record(record_t &&record, OrderingHandle &handle);

  // Test interfaces
  
  CircularJournalSpace& get_cjs() {
    return cjs;
  }

private:
  JournalTrimmer &trimmer;
  std::string path;
  WritePipeline *write_pipeline = nullptr;
  /**
   * initialized
   *
   * true after open_device_read_header, set to false in close().
   * Indicates that device is open and in-memory header is valid.
   */
  bool initialized = false;

  // start address where the newest record will be written
  // should be in range [get_records_start(), get_journal_end())
  // written_to.segment_seq is circulation seq to track 
  // the sequence to written records
  CircularJournalSpace cjs;
  RecordSubmitter record_submitter; 
};

}

