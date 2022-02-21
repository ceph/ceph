// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <optional>

#include <seastar/core/circular_buffer.hh>
#include <seastar/core/future.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/shared_future.hh>

#include "include/ceph_assert.h"
#include "include/buffer.h"
#include "include/denc.h"

#include "crimson/os/seastore/segment_cleaner.h"
#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/extent_reader.h"
#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/ordering_handle.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/osd/exceptions.h"

namespace crimson::os::seastore::journal {

/**
 * Manages stream of atomically written records to a SegmentManager.
 */
class SegmentedJournal : public Journal {
public:
  SegmentedJournal(
    SegmentManager &segment_manager,
    ExtentReader& scanner,
    SegmentProvider& cleaner);
  ~SegmentedJournal() {}

  open_for_write_ret open_for_write() final;

  close_ertr::future<> close() final;

  submit_record_ret submit_record(
    record_t &&record,
    OrderingHandle &handle
  ) final {
    return record_submitter.submit(std::move(record), handle);
  }

  seastar::future<> flush(OrderingHandle &handle) final {
    return record_submitter.flush(handle);
  }

  replay_ret replay(delta_handler_t &&delta_handler) final;

  void set_write_pipeline(WritePipeline* write_pipeline) final {
    record_submitter.set_write_pipeline(write_pipeline);
  }

private:
  class JournalSegmentManager {
  public:
    JournalSegmentManager(SegmentManager&, SegmentProvider&);

    using base_ertr = crimson::errorator<
        crimson::ct_error::input_output_error>;
    extent_len_t get_max_write_length() const {
      return segment_manager.get_segment_size() -
             p2align(ceph::encoded_sizeof_bounded<segment_header_t>(),
                     size_t(segment_manager.get_block_size()));
    }

    device_id_t get_device_id() const {
      return segment_manager.get_device_id();
    }

    device_segment_id_t get_num_segments() const {
      return segment_manager.get_num_segments();
    }

    seastore_off_t get_block_size() const {
      return segment_manager.get_block_size();
    }

    segment_nonce_t get_nonce() const {
      return current_segment_nonce;
    }

    journal_seq_t get_committed_to() const {
      return committed_to;
    }

    segment_seq_t get_segment_seq() const {
      return next_journal_segment_seq - 1;
    }

    void set_segment_seq(segment_seq_t current_seq) {
      next_journal_segment_seq = (current_seq + 1);
    }

    using open_ertr = base_ertr;
    using open_ret = open_ertr::future<journal_seq_t>;
    open_ret open();

    using close_ertr = base_ertr;
    close_ertr::future<> close();

    // returns true iff the current segment has insufficient space
    bool needs_roll(std::size_t length) const {
      auto write_capacity = current_journal_segment->get_write_capacity();
      return length + written_to > std::size_t(write_capacity);
    }

    // close the current segment and initialize next one
    using roll_ertr = base_ertr;
    roll_ertr::future<> roll();

    // write the buffer, return the write result
    // May be called concurrently, writes may complete in any order.
    using write_ertr = base_ertr;
    using write_ret = write_ertr::future<write_result_t>;
    write_ret write(ceph::bufferlist to_write);

    // mark write committed in order
    void mark_committed(const journal_seq_t& new_committed_to);
    
  private:
    journal_seq_t get_current_write_seq() const {
      assert(current_journal_segment);
      return journal_seq_t{
        get_segment_seq(),
        paddr_t::make_seg_paddr(current_journal_segment->get_segment_id(),
	  written_to)
      };
    }

    void reset() {
      next_journal_segment_seq = 0;
      current_segment_nonce = 0;
      current_journal_segment.reset();
      written_to = 0;
      committed_to = {};
    }

    // prepare segment for writes, writes out segment header
    using initialize_segment_ertr = base_ertr;
    initialize_segment_ertr::future<> initialize_segment(Segment&);

    SegmentProvider& segment_provider;
    SegmentManager& segment_manager;

    segment_seq_t next_journal_segment_seq;
    segment_nonce_t current_segment_nonce;

    SegmentRef current_journal_segment;
    seastore_off_t written_to;
    // committed_to may be in a previous journal segment
    journal_seq_t committed_to;
  };

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

    // return the expected write sizes if allows to batch,
    // otherwise, return nullopt
    std::optional<record_group_size_t> can_batch(
        const record_t& record,
        extent_len_t block_size) const {
      assert(state != state_t::SUBMITTING);
      if (pending.get_size() >= batch_capacity ||
          (pending.get_size() > 0 &&
           pending.size.get_encoded_length() > batch_flush_size)) {
        assert(state == state_t::PENDING);
        return std::nullopt;
      }
      return get_encoded_length_after(record, block_size);
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
    using add_pending_ertr = JournalSegmentManager::write_ertr;
    using add_pending_ret = add_pending_ertr::future<record_locator_t>;
    add_pending_ret add_pending(
        record_t&&,
        OrderingHandle&,
        extent_len_t block_size);

    // Encode the batched records for write.
    std::pair<ceph::bufferlist, record_group_size_t> encode_batch(
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
    std::pair<ceph::bufferlist, record_group_size_t> submit_pending_fast(
        record_t&&,
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
    seastore_off_t submitting_length = 0;
    seastore_off_t submitting_mdlength = 0;

    struct promise_result_t {
      write_result_t write_result;
      seastore_off_t mdlength;
    };
    using maybe_promise_result_t = std::optional<promise_result_t>;
    std::optional<seastar::shared_promise<maybe_promise_result_t> > io_promise;
  };

  class RecordSubmitter {
    enum class state_t {
      IDLE = 0, // outstanding_io == 0
      PENDING,  // outstanding_io <  io_depth_limit
      FULL      // outstanding_io == io_depth_limit
      // OVERFLOW: outstanding_io >  io_depth_limit is impossible
    };

    struct grouped_io_stats {
      uint64_t num_io = 0;
      uint64_t num_io_grouped = 0;

      void increment(uint64_t num_grouped_io) {
        ++num_io;
        num_io_grouped += num_grouped_io;
      }
    };

  public:
    RecordSubmitter(std::size_t io_depth,
                    std::size_t batch_capacity,
                    std::size_t batch_flush_size,
                    double preferred_fullness,
                    JournalSegmentManager&);

    grouped_io_stats get_record_batch_stats() const {
      return stats.record_batch_stats;
    }

    grouped_io_stats get_io_depth_stats() const {
      return stats.io_depth_stats;
    }

    uint64_t get_record_group_padding_bytes() const {
      return stats.record_group_padding_bytes;
    }

    uint64_t get_record_group_metadata_bytes() const {
      return stats.record_group_metadata_bytes;
    }

    uint64_t get_record_group_data_bytes() const {
      return stats.record_group_data_bytes;
    }

    void reset_stats() {
      stats = {};
    }

    void set_write_pipeline(WritePipeline *_write_pipeline) {
      write_pipeline = _write_pipeline;
    }

    using submit_ret = Journal::submit_record_ret;
    submit_ret submit(record_t&&, OrderingHandle&);
    seastar::future<> flush(OrderingHandle &handle);

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

    void account_submission(std::size_t, const record_group_size_t&);

    using maybe_result_t = RecordBatch::maybe_result_t;
    void finish_submit_batch(RecordBatch*, maybe_result_t);

    void flush_current_batch();

    using submit_pending_ertr = JournalSegmentManager::write_ertr;
    using submit_pending_ret = submit_pending_ertr::future<
      record_locator_t>;
    submit_pending_ret submit_pending(
        record_t&&, OrderingHandle &handle, bool flush);

    using do_submit_ret = submit_pending_ret;
    do_submit_ret do_submit(
        record_t&&, OrderingHandle&);

    state_t state = state_t::IDLE;
    std::size_t num_outstanding_io = 0;
    std::size_t io_depth_limit;
    double preferred_fullness;

    WritePipeline* write_pipeline = nullptr;
    JournalSegmentManager& journal_segment_manager;
    std::unique_ptr<RecordBatch[]> batches;
    std::size_t current_batch_index;
    // should not be nullptr after constructed
    RecordBatch* p_current_batch = nullptr;
    seastar::circular_buffer<RecordBatch*> free_batch_ptrs;
    std::optional<seastar::promise<> > wait_submit_promise;

    struct {
      grouped_io_stats record_batch_stats;
      grouped_io_stats io_depth_stats;
      uint64_t record_group_padding_bytes = 0;
      uint64_t record_group_metadata_bytes = 0;
      uint64_t record_group_data_bytes = 0;
    } stats;
  };

  SegmentProvider& segment_provider;
  JournalSegmentManager journal_segment_manager;
  RecordSubmitter record_submitter;
  ExtentReader& scanner;
  seastar::metrics::metric_group metrics;

  /// read journal segment headers from scanner
  using find_journal_segments_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  using find_journal_segments_ret_bare = std::vector<
    std::pair<segment_id_t, segment_header_t>>;
  using find_journal_segments_ret = find_journal_segments_ertr::future<
    find_journal_segments_ret_bare>;
  find_journal_segments_ret find_journal_segments();

  /// return ordered vector of segments to replay
  using replay_segments_t = std::vector<
    std::pair<journal_seq_t, segment_header_t>>;
  using prep_replay_segments_ertr = crimson::errorator<
    crimson::ct_error::input_output_error
    >;
  using prep_replay_segments_fut = prep_replay_segments_ertr::future<
    replay_segments_t>;
  prep_replay_segments_fut prep_replay_segments(
    std::vector<std::pair<segment_id_t, segment_header_t>> segments);

  /// replays records starting at start through end of segment
  replay_ertr::future<>
  replay_segment(
    journal_seq_t start,             ///< [in] starting addr, seq
    segment_header_t header,         ///< [in] segment header
    delta_handler_t &delta_handler   ///< [in] processes deltas in order
  );

  void register_metrics();
};

}
