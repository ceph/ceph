// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "record_submitter.h"

#include <fmt/format.h>
#include <fmt/os.h>

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/async_cleaner.h"

SET_SUBSYS(seastore_journal);

namespace crimson::os::seastore::journal {

RecordBatch::add_pending_ret
RecordBatch::add_pending(
  const std::string& name,
  record_t&& record,
  extent_len_t block_size)
{
  LOG_PREFIX(RecordBatch::add_pending);
  auto new_size = get_encoded_length_after(record, block_size);
  auto dlength_offset = pending.size.dlength;
  TRACE("{} batches={}, write_size={}, dlength_offset={} ...",
        name,
        pending.get_size() + 1,
        new_size.get_encoded_length(),
        dlength_offset);
  assert(state != state_t::SUBMITTING);
  assert(evaluate_submit(record.size, block_size).submit_size == new_size);

  pending.push_back(
      std::move(record), block_size);
  assert(pending.size == new_size);
  if (state == state_t::EMPTY) {
    assert(!io_promise.has_value());
    io_promise = seastar::shared_promise<maybe_promise_result_t>();
  } else {
    assert(io_promise.has_value());
  }
  state = state_t::PENDING;

  return io_promise->get_shared_future(
  ).then([dlength_offset, FNAME, &name
         ](auto maybe_promise_result) -> add_pending_ret {
    if (!maybe_promise_result.has_value()) {
      ERROR("{} write failed", name);
      return crimson::ct_error::input_output_error::make();
    }
    auto write_result = maybe_promise_result->write_result;
    auto submit_result = record_locator_t{
      write_result.start_seq.offset.add_offset(
          maybe_promise_result->mdlength + dlength_offset),
      write_result
    };
    TRACE("{} write finish with {}", name, submit_result);
    return add_pending_ret(
      add_pending_ertr::ready_future_marker{},
      submit_result);
  });
}

ceph::bufferlist RecordBatch::encode_batch(
  const journal_seq_t& committed_to,
  segment_nonce_t segment_nonce)
{
  assert(state == state_t::PENDING);
  assert(pending.get_size() > 0);
  assert(io_promise.has_value());

  state = state_t::SUBMITTING;
  submitting_size = pending.get_size();
  submitting_length = pending.size.get_encoded_length();
  submitting_mdlength = pending.size.get_mdlength();
  auto bl = encode_records(pending, committed_to, segment_nonce);
  // Note: pending is cleared here
  assert(bl.length() == submitting_length);
  return bl;
}

void RecordBatch::set_result(
  maybe_result_t maybe_write_result)
{
  maybe_promise_result_t result;
  if (maybe_write_result.has_value()) {
    assert(maybe_write_result->length == submitting_length);
    result = promise_result_t{
      *maybe_write_result,
      submitting_mdlength
    };
  }
  assert(state == state_t::SUBMITTING);
  assert(io_promise.has_value());

  state = state_t::EMPTY;
  submitting_size = 0;
  submitting_length = 0;
  submitting_mdlength = 0;
  io_promise->set_value(result);
  io_promise.reset();
}

ceph::bufferlist
RecordBatch::submit_pending_fast(
  record_group_t&& group,
  extent_len_t block_size,
  const journal_seq_t& committed_to,
  segment_nonce_t segment_nonce)
{
  assert(group.get_size() == 1);
  auto& record = group.records[0];
  auto new_size = get_encoded_length_after(record, block_size);
  std::ignore = new_size;
  assert(state == state_t::EMPTY);
  assert(evaluate_submit(record.size, block_size).submit_size == new_size);
  assert(group.size == new_size);
  auto bl = encode_records(group, committed_to, segment_nonce);
  // Note: group is cleared here
  assert(bl.length() == new_size.get_encoded_length());
  return bl;
}

RecordSubmitter::RecordSubmitter(
  std::size_t io_depth,
  std::size_t batch_capacity,
  std::size_t batch_flush_size,
  double preferred_fullness,
  JournalAllocator& ja)
  : io_depth_limit{io_depth},
    preferred_fullness{preferred_fullness},
    journal_allocator{ja},
    batches(new RecordBatch[io_depth + 1])
{
  LOG_PREFIX(RecordSubmitter);
  INFO("{} io_depth_limit={}, batch_capacity={}, batch_flush_size={}, "
       "preferred_fullness={}",
       get_name(), io_depth, batch_capacity,
       batch_flush_size, preferred_fullness);
  ceph_assert(io_depth > 0);
  ceph_assert(batch_capacity > 0);
  ceph_assert(preferred_fullness >= 0 &&
              preferred_fullness <= 1);
  free_batch_ptrs.reserve(io_depth + 1);
  for (std::size_t i = 0; i <= io_depth; ++i) {
    batches[i].initialize(i, batch_capacity, batch_flush_size);
    free_batch_ptrs.push_back(&batches[i]);
  }
  pop_free_batch();
}

bool RecordSubmitter::is_available() const
{
  auto ret = !wait_available_promise.has_value() &&
             !has_io_error;
#ifndef NDEBUG
  if (ret) {
    // unconditional invariants
    ceph_assert(journal_allocator.can_write());
    ceph_assert(p_current_batch != nullptr);
    ceph_assert(!p_current_batch->is_submitting());
    // the current batch accepts a further write
    ceph_assert(!p_current_batch->needs_flush());
    if (!p_current_batch->is_empty()) {
      auto submit_length =
        p_current_batch->get_submit_size().get_encoded_length();
      ceph_assert(!journal_allocator.needs_roll(submit_length));
    }
    // I'm not rolling
  }
#endif
  return ret;
}

writer_stats_t RecordSubmitter::get_stats() const
{
  writer_stats_t ret = stats;
  ret.minus(last_stats);
  last_stats = stats;
  return ret;
}

RecordSubmitter::wa_ertr::future<>
RecordSubmitter::wait_available()
{
  LOG_PREFIX(RecordSubmitter::wait_available);
  assert(!is_available());
  if (has_io_error) {
    ERROR("{} I/O is failed before wait", get_name());
    return crimson::ct_error::input_output_error::make();
  }
  return wait_available_promise->get_shared_future(
  ).then([FNAME, this]() -> wa_ertr::future<> {
    if (has_io_error) {
      ERROR("{} I/O is failed after wait", get_name());
      return crimson::ct_error::input_output_error::make();
    }
    return wa_ertr::now();
  });
}

RecordSubmitter::action_t
RecordSubmitter::check_action(
  const record_size_t& rsize) const
{
  assert(is_available());
  auto eval = p_current_batch->evaluate_submit(
      rsize, journal_allocator.get_block_size());
  if (journal_allocator.needs_roll(eval.submit_size.get_encoded_length())) {
    return action_t::ROLL;
  } else if (eval.is_full) {
    return action_t::SUBMIT_FULL;
  } else {
    return action_t::SUBMIT_NOT_FULL;
  }
}

RecordSubmitter::roll_segment_ertr::future<>
RecordSubmitter::roll_segment()
{
  LOG_PREFIX(RecordSubmitter::roll_segment);
  ceph_assert(p_current_batch->needs_flush() ||
              is_available());
  // #1 block concurrent submissions due to rolling
  wait_available_promise = seastar::shared_promise<>();
  ceph_assert(!wait_unfull_flush_promise.has_value());
  return [FNAME, this] {
    if (p_current_batch->is_pending()) {
      if (state == state_t::FULL) {
        DEBUG("{} wait flush ...", get_name());
        wait_unfull_flush_promise = seastar::promise<>();
        return wait_unfull_flush_promise->get_future();
      } else { // IDLE/PENDING
        DEBUG("{} flush", get_name());
        flush_current_batch();
        return seastar::now();
      }
    } else {
      assert(p_current_batch->is_empty());
      return seastar::now();
    }
  }().then_wrapped([FNAME, this](auto fut) {
    if (fut.failed()) {
      ERROR("{} rolling is skipped unexpectedly, available", get_name());
      has_io_error = true;
      wait_available_promise->set_value();
      wait_available_promise.reset();
      return roll_segment_ertr::now();
    } else {
      // start rolling in background
      std::ignore = journal_allocator.roll(
      ).safe_then([FNAME, this] {
        // good
        DEBUG("{} rolling done, available", get_name());
        assert(!has_io_error);
        wait_available_promise->set_value();
        wait_available_promise.reset();
      }).handle_error(
        crimson::ct_error::all_same_way([FNAME, this](auto e) {
          ERROR("{} got error {}, available", get_name(), e);
          has_io_error = true;
          wait_available_promise->set_value();
          wait_available_promise.reset();
          return seastar::now();
        })
      ).handle_exception([FNAME, this](auto e) {
        ERROR("{} got exception {}, available", get_name(), e);
        has_io_error = true;
        wait_available_promise->set_value();
        wait_available_promise.reset();
      });
      // wait for background rolling
      return wait_available();
    }
  });
}

RecordSubmitter::submit_ret
RecordSubmitter::submit(
    record_t&& record,
    bool with_atomic_roll_segment)
{
  LOG_PREFIX(RecordSubmitter::submit);
  ceph_assert(is_available());
  assert(check_action(record.size) != action_t::ROLL);
  journal_allocator.update_modify_time(record);
  auto eval = p_current_batch->evaluate_submit(
      record.size, journal_allocator.get_block_size());
  bool needs_flush = (
      state == state_t::IDLE ||
      eval.submit_size.get_fullness() > preferred_fullness ||
      // RecordBatch::needs_flush()
      eval.is_full ||
      p_current_batch->get_num_records() + 1 >=
        p_current_batch->get_batch_capacity());
  if (p_current_batch->is_empty() &&
      needs_flush &&
      state != state_t::FULL) {
    // fast path with direct write
    increment_io();
    auto block_size = journal_allocator.get_block_size();
    auto rg = record_group_t(std::move(record), block_size);
    account_submission(rg);
    assert(stats.record_batch_stats.num_io ==
           stats.io_depth_stats.num_io);
    record_group_size_t sizes = rg.size;
    auto to_write = p_current_batch->submit_pending_fast(
      std::move(rg),
      block_size,
      get_committed_to(),
      journal_allocator.get_nonce());
    DEBUG("{} fast submit {}, committed_to={}, outstanding_io={} ...",
          get_name(), sizes, get_committed_to(), num_outstanding_io);
    return journal_allocator.write(std::move(to_write)
    ).safe_then([mdlength = sizes.get_mdlength()](auto write_result) {
      return record_locator_t{
        write_result.start_seq.offset.add_offset(mdlength),
        write_result
      };
    }).finally([this] {
      decrement_io_with_flush();
    });
  }
  // indirect batched write
  auto write_fut = p_current_batch->add_pending(
    get_name(),
    std::move(record),
    journal_allocator.get_block_size());
  if (needs_flush) {
    if (state == state_t::FULL) {
      // #2 block concurrent submissions due to lack of resource
      DEBUG("{} added with {} pending, outstanding_io={}, unavailable, wait flush ...",
            get_name(),
            p_current_batch->get_num_records(),
            num_outstanding_io);
      if (with_atomic_roll_segment) {
        // wait_available_promise and wait_unfull_flush_promise
        // need to be delegated to the follow-up atomic roll_segment();
        assert(p_current_batch->is_pending());
      } else {
        wait_available_promise = seastar::shared_promise<>();
        ceph_assert(!wait_unfull_flush_promise.has_value());
        wait_unfull_flush_promise = seastar::promise<>();
        // flush and mark available in background
        std::ignore = wait_unfull_flush_promise->get_future(
        ).finally([FNAME, this] {
          DEBUG("{} flush done, available", get_name());
          wait_available_promise->set_value();
          wait_available_promise.reset();
        });
      }
    } else {
      DEBUG("{} added pending, flush", get_name());
      flush_current_batch();
    }
  } else {
    // will flush later
    DEBUG("{} added with {} pending, outstanding_io={}",
          get_name(),
          p_current_batch->get_num_records(),
          num_outstanding_io);
    assert(!p_current_batch->needs_flush());
  }
  return write_fut;
}

RecordSubmitter::open_ret
RecordSubmitter::open(bool is_mkfs)
{
  return journal_allocator.open(is_mkfs
  ).safe_then([this](journal_seq_t ret) {
    LOG_PREFIX(RecordSubmitter::open);
    DEBUG("{} register metrics", get_name());
    stats = {};
    last_stats = {};
    namespace sm = seastar::metrics;
    std::vector<sm::label_instance> label_instances;
    label_instances.push_back(sm::label_instance("submitter", get_name()));
    metrics.add_group(
      "journal",
      {
        sm::make_counter(
          "record_num",
          stats.record_batch_stats.num_io_grouped,
          sm::description("total number of records submitted"),
          label_instances
        ),
        sm::make_counter(
          "io_num",
          stats.io_depth_stats.num_io,
          sm::description("total number of io submitted"),
          label_instances
        ),
        sm::make_counter(
          "io_depth_num",
          stats.io_depth_stats.num_io_grouped,
          sm::description("total number of io depth"),
          label_instances
        ),
        sm::make_counter(
          "record_group_padding_bytes",
          stats.record_group_padding_bytes,
          sm::description("bytes of metadata padding when write record groups"),
          label_instances
        ),
        sm::make_counter(
          "record_group_metadata_bytes",
          stats.record_group_metadata_bytes,
          sm::description("bytes of raw metadata when write record groups"),
          label_instances
        ),
        sm::make_counter(
          "record_group_data_bytes",
          stats.record_group_data_bytes,
          sm::description("bytes of data when write record groups"),
          label_instances
        ),
      }
    );
    return ret;
  });
}

RecordSubmitter::close_ertr::future<>
RecordSubmitter::close()
{
  committed_to = JOURNAL_SEQ_NULL;
  ceph_assert(state == state_t::IDLE);
  ceph_assert(num_outstanding_io == 0);
  ceph_assert(p_current_batch != nullptr);
  ceph_assert(p_current_batch->is_empty());
  ceph_assert(!wait_available_promise.has_value());
  has_io_error = false;
  ceph_assert(!wait_unfull_flush_promise.has_value());
  metrics.clear();
  return journal_allocator.close();
}

void RecordSubmitter::update_state()
{
  if (num_outstanding_io == 0) {
    state = state_t::IDLE;
  } else if (num_outstanding_io < io_depth_limit) {
    state = state_t::PENDING;
  } else if (num_outstanding_io == io_depth_limit) {
    state = state_t::FULL;
  } else {
    ceph_abort("fatal error: io-depth overflow");
  }
}

void RecordSubmitter::decrement_io_with_flush()
{
  LOG_PREFIX(RecordSubmitter::decrement_io_with_flush);
  assert(num_outstanding_io > 0);
  auto prv_state = state;
  --num_outstanding_io;
  update_state();

  if (prv_state == state_t::FULL) {
    if (wait_unfull_flush_promise.has_value()) {
      DEBUG("{} flush, resolve wait_unfull_flush_promise", get_name());
      assert(!p_current_batch->is_empty());
      assert(wait_available_promise.has_value());
      flush_current_batch();
      wait_unfull_flush_promise->set_value();
      wait_unfull_flush_promise.reset();
      return;
    }
  } else {
    ceph_assert(!wait_unfull_flush_promise.has_value());
  }

  auto needs_flush = (
      !p_current_batch->is_empty() && (
        state == state_t::IDLE ||
        p_current_batch->get_submit_size().get_fullness() > preferred_fullness ||
        p_current_batch->needs_flush()
      ));
  if (needs_flush) {
    DEBUG("{} flush", get_name());
    flush_current_batch();
  }
}

void RecordSubmitter::account_submission(
  const record_group_t& rg)
{
  stats.record_group_padding_bytes +=
    (rg.size.get_mdlength() - rg.size.get_raw_mdlength());
  stats.record_group_metadata_bytes += rg.size.get_raw_mdlength();
  stats.record_group_data_bytes += rg.size.dlength;
  stats.record_batch_stats.increment(rg.get_size());

  for (const record_t& r : rg.records) {
    auto src = r.type;
    assert(is_modify_transaction(src));
    auto& trans_stats = get_by_src(stats.stats_by_src, src);
    ++(trans_stats.num_records);
    trans_stats.metadata_bytes += r.size.get_raw_mdlength();
    trans_stats.data_bytes += r.size.dlength;
  }
}

void RecordSubmitter::finish_submit_batch(
  RecordBatch* p_batch,
  maybe_result_t maybe_result)
{
  assert(p_batch->is_submitting());
  p_batch->set_result(maybe_result);
  free_batch_ptrs.push_back(p_batch);
  decrement_io_with_flush();
}

void RecordSubmitter::flush_current_batch()
{
  LOG_PREFIX(RecordSubmitter::flush_current_batch);
  RecordBatch* p_batch = p_current_batch;
  assert(p_batch->is_pending());
  p_current_batch = nullptr;
  pop_free_batch();

  increment_io();
  auto num = p_batch->get_num_records();
  const auto& rg = p_batch->get_record_group();
  assert(rg.get_size() == num);
  record_group_size_t sizes = rg.size;
  account_submission(rg);
  assert(stats.record_batch_stats.num_io ==
         stats.io_depth_stats.num_io);
  auto to_write = p_batch->encode_batch(
    get_committed_to(), journal_allocator.get_nonce());
  // Note: rg is cleared
  DEBUG("{} {} records, {}, committed_to={}, outstanding_io={} ...",
        get_name(), num, sizes, get_committed_to(), num_outstanding_io);
  std::ignore = journal_allocator.write(std::move(to_write)
  ).safe_then([this, p_batch, FNAME, num, sizes](auto write_result) {
    TRACE("{} {} records, {}, write done with {}",
          get_name(), num, sizes, write_result);
    finish_submit_batch(p_batch, write_result);
  }).handle_error(
    crimson::ct_error::all_same_way([this, p_batch, FNAME, num, sizes](auto e) {
      ERROR("{} {} records, {}, got error {}",
            get_name(), num, sizes, e);
      finish_submit_batch(p_batch, std::nullopt);
      return seastar::now();
    })
  ).handle_exception([this, p_batch, FNAME, num, sizes](auto e) {
    ERROR("{} {} records, {}, got exception {}",
          get_name(), num, sizes, e);
    finish_submit_batch(p_batch, std::nullopt);
  });
}

}
