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

class JournalAllocator {
public:
  using base_ertr = crimson::errorator<
      crimson::ct_error::input_output_error>;
  virtual const std::string& get_name() const = 0;
  
  virtual void update_modify_time(record_t& record) = 0;

  virtual extent_len_t get_block_size() const = 0;

  using close_ertr = base_ertr;
  virtual close_ertr::future<> close() = 0;

  virtual segment_nonce_t get_nonce() const  = 0;

  using write_ertr = base_ertr;
  using write_ret = write_ertr::future<write_result_t>;
  virtual write_ret write(ceph::bufferlist&& to_write) = 0;

  virtual bool can_write() const = 0;
  
  using roll_ertr = base_ertr;
  virtual roll_ertr::future<> roll() = 0;

  virtual bool needs_roll(std::size_t length) const = 0;

  using open_ertr = base_ertr;
  using open_ret = open_ertr::future<journal_seq_t>;
  virtual open_ret open(bool is_mkfs) = 0;

};

/**
 * RecordBatch
 *
 * Maintain a batch of records for submit.
 */
class RecordBatch {
  enum class state_t {
    EMPTY = 0,
    PENDING,
    SUBMITTING
  };

public:
  RecordBatch() = default;
  RecordBatch(RecordBatch&&) = delete;
  RecordBatch(const RecordBatch&) = delete;
  RecordBatch& operator=(RecordBatch&&) = delete;
  RecordBatch& operator=(const RecordBatch&) = delete;

  bool is_empty() const {
    return state == state_t::EMPTY;
  }

  bool is_pending() const {
    return state == state_t::PENDING;
  }

  bool is_submitting() const {
    return state == state_t::SUBMITTING;
  }

  std::size_t get_index() const {
    return index;
  }

  std::size_t get_num_records() const {
    return pending.get_size();
  }

  std::size_t get_batch_capacity() const {
    return batch_capacity;
  }

  const record_group_size_t& get_submit_size() const {
    assert(state != state_t::EMPTY);
    return pending.size;
  }

  bool needs_flush() const {
    assert(state != state_t::SUBMITTING);
    assert(pending.get_size() <= batch_capacity);
    if (state == state_t::EMPTY) {
      return false;
    } else {
      assert(state == state_t::PENDING);
      return (pending.get_size() >= batch_capacity ||
              pending.size.get_encoded_length() > batch_flush_size);
    }
  }

  const record_group_t& get_record_group() const {
    return pending;
  }

  struct evaluation_t {
    record_group_size_t submit_size;
    bool is_full;
  };
  evaluation_t evaluate_submit(
      const record_size_t& rsize,
      extent_len_t block_size) const {
    assert(!needs_flush());
    auto submit_size = pending.size.get_encoded_length_after(
        rsize, block_size);
    bool is_full = submit_size.get_encoded_length() > batch_flush_size;
    return {submit_size, is_full};
  }

  void initialize(std::size_t i,
                  std::size_t _batch_capacity,
                  std::size_t _batch_flush_size) {
    ceph_assert(_batch_capacity > 0);
    index = i;
    batch_capacity = _batch_capacity;
    batch_flush_size = _batch_flush_size;
    pending.reserve(batch_capacity);
  }

  // Add to the batch, the future will be resolved after the batch is
  // written.
  //
  // Set write_result_t::write_length to 0 if the record is not the first one
  // in the batch.
  using add_pending_ertr = JournalAllocator::write_ertr;
  using add_pending_ret = add_pending_ertr::future<record_locator_t>;
  add_pending_ret add_pending(
      const std::string& name,
      record_t&&,
      extent_len_t block_size);

  // Encode the batched records for write.
  ceph::bufferlist encode_batch(
      const journal_seq_t& committed_to,
      segment_nonce_t segment_nonce);

  // Set the write result and reset for reuse
  using maybe_result_t = std::optional<write_result_t>;
  void set_result(maybe_result_t maybe_write_end_seq);

  // The fast path that is equivalent to submit a single record as a batch.
  //
  // Essentially, equivalent to the combined logic of:
  // add_pending(), encode_batch() and set_result() above without
  // the intervention of the shared io_promise.
  //
  // Note the current RecordBatch can be reused afterwards.
  ceph::bufferlist submit_pending_fast(
      record_group_t&&,
      extent_len_t block_size,
      const journal_seq_t& committed_to,
      segment_nonce_t segment_nonce);

private:
  record_group_size_t get_encoded_length_after(
      const record_t& record,
      extent_len_t block_size) const {
    return pending.size.get_encoded_length_after(
        record.size, block_size);
  }

  state_t state = state_t::EMPTY;
  std::size_t index = 0;
  std::size_t batch_capacity = 0;
  std::size_t batch_flush_size = 0;

  record_group_t pending;
  std::size_t submitting_size = 0;
  extent_len_t submitting_length = 0;
  extent_len_t submitting_mdlength = 0;

  struct promise_result_t {
    write_result_t write_result;
    extent_len_t mdlength;
  };
  using maybe_promise_result_t = std::optional<promise_result_t>;
  std::optional<seastar::shared_promise<maybe_promise_result_t> > io_promise;
};

/**
 * RecordSubmitter
 *
 * Submit records concurrently with RecordBatch with SegmentAllocator.
 *
 * Configurations and controls:
 * - io_depth: the io-depth limit to SegmentAllocator;
 * - batch_capacity: the number limit of records in a RecordBatch;
 * - batch_flush_size: the bytes threshold to force flush a RecordBatch to
 *   control the maximum latency;
 * - preferred_fullness: the fullness threshold to flush a RecordBatch;
 */
class RecordSubmitter {
  enum class state_t {
    IDLE = 0, // outstanding_io == 0
    PENDING,  // outstanding_io <  io_depth_limit
    FULL      // outstanding_io == io_depth_limit
    // OVERFLOW: outstanding_io >  io_depth_limit is impossible
  };

  using base_ertr = crimson::errorator<
      crimson::ct_error::input_output_error>;

public:
  RecordSubmitter(std::size_t io_depth,
                  std::size_t batch_capacity,
                  std::size_t batch_flush_size,
                  double preferred_fullness,
		  JournalAllocator&);

  const std::string& get_name() const {
    return journal_allocator.get_name();
  }

  journal_seq_t get_committed_to() const {
    return committed_to;
  }

  // whether is available to submit a record
  bool is_available() const;

  // get the stats since last_stats
  writer_stats_t get_stats() const;

  // wait for available if cannot submit, should check is_available() again
  // when the future is resolved.
  using wa_ertr = base_ertr;
  wa_ertr::future<> wait_available();

  // when available, check for the submit action
  // according to the pending record size
  enum class action_t {
    ROLL,
    SUBMIT_FULL,
    SUBMIT_NOT_FULL
  };
  action_t check_action(const record_size_t&) const;

  // when available, roll the segment if needed
  using roll_segment_ertr = base_ertr;
  roll_segment_ertr::future<> roll_segment();

  // when available, submit the record if possible
  using submit_ertr = base_ertr;
  using submit_ret = submit_ertr::future<record_locator_t>;
  submit_ret submit(record_t&&, bool with_atomic_roll_segment=false);

  void update_committed_to(const journal_seq_t& new_committed_to) {
    assert(new_committed_to != JOURNAL_SEQ_NULL);
    assert(committed_to == JOURNAL_SEQ_NULL ||
           committed_to <= new_committed_to);
    committed_to = new_committed_to;
  }

  // open for write, generate the correct print name, and register metrics
  using open_ertr = base_ertr;
  using open_ret = open_ertr::future<journal_seq_t>;
  open_ret open(bool is_mkfs);

  using close_ertr = base_ertr;
  close_ertr::future<> close();

private:
  void update_state();

  void increment_io() {
    ++num_outstanding_io;
    stats.io_depth_stats.increment(num_outstanding_io);
    update_state();
  }

  void decrement_io_with_flush();

  void pop_free_batch() {
    assert(p_current_batch == nullptr);
    assert(!free_batch_ptrs.empty());
    p_current_batch = free_batch_ptrs.front();
    assert(p_current_batch->is_empty());
    assert(p_current_batch == &batches[p_current_batch->get_index()]);
    free_batch_ptrs.pop_front();
  }

  void account_submission(const record_group_t&);

  using maybe_result_t = RecordBatch::maybe_result_t;
  void finish_submit_batch(RecordBatch*, maybe_result_t);

  void flush_current_batch();

  state_t state = state_t::IDLE;
  std::size_t num_outstanding_io = 0;
  std::size_t io_depth_limit;
  double preferred_fullness;

  JournalAllocator& journal_allocator;
  // committed_to may be in a previous journal segment
  journal_seq_t committed_to = JOURNAL_SEQ_NULL;

  std::unique_ptr<RecordBatch[]> batches;
  // should not be nullptr after constructed
  RecordBatch* p_current_batch = nullptr;
  seastar::circular_buffer<RecordBatch*> free_batch_ptrs;

  // blocked for rolling or lack of resource
  std::optional<seastar::shared_promise<> > wait_available_promise;
  bool has_io_error = false;
  // when needs flush but io depth is full,
  // wait for decrement_io_with_flush()
  std::optional<seastar::promise<> > wait_unfull_flush_promise;

  writer_stats_t stats;
  mutable writer_stats_t last_stats;

  seastar::metrics::metric_group metrics;
};

}
