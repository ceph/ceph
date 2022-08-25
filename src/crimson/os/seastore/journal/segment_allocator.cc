// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "segment_allocator.h"

#include <fmt/format.h>

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/async_cleaner.h"

SET_SUBSYS(seastore_journal);

namespace crimson::os::seastore::journal {

SegmentAllocator::SegmentAllocator(
  JournalTrimmer *trimmer,
  data_category_t category,
  reclaim_gen_t gen,
  SegmentProvider &sp,
  SegmentSeqAllocator &ssa)
  : print_name{fmt::format("{}_G{}", category, gen)},
    type{trimmer == nullptr ?
         segment_type_t::OOL :
         segment_type_t::JOURNAL},
    category{category},
    gen{gen},
    segment_provider{sp},
    sm_group{*sp.get_segment_manager_group()},
    segment_seq_allocator(ssa),
    trimmer{trimmer}
{
  reset();
}

SegmentAllocator::open_ret
SegmentAllocator::do_open(bool is_mkfs)
{
  LOG_PREFIX(SegmentAllocator::do_open);
  ceph_assert(!current_segment);
  segment_seq_t new_segment_seq =
    segment_seq_allocator.get_and_inc_next_segment_seq();
  auto meta = sm_group.get_meta();
  current_segment_nonce = ceph_crc32c(
    new_segment_seq,
    reinterpret_cast<const unsigned char *>(meta.seastore_id.bytes()),
    sizeof(meta.seastore_id.uuid));
  auto new_segment_id = segment_provider.allocate_segment(
      new_segment_seq, type, category, gen);
  ceph_assert(new_segment_id != NULL_SEG_ID);
  return sm_group.open(new_segment_id
  ).handle_error(
    open_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in SegmentAllocator::do_open open"
    }
  ).safe_then([this, is_mkfs, FNAME, new_segment_seq](auto sref) {
    // initialize new segment
    segment_id_t segment_id = sref->get_segment_id();
    journal_seq_t dirty_tail;
    journal_seq_t alloc_tail;
    if (type == segment_type_t::JOURNAL) {
      dirty_tail = trimmer->get_dirty_tail();
      alloc_tail = trimmer->get_alloc_tail();
      if (is_mkfs) {
        ceph_assert(dirty_tail == JOURNAL_SEQ_NULL);
        ceph_assert(alloc_tail == JOURNAL_SEQ_NULL);
        auto mkfs_seq = journal_seq_t{
          new_segment_seq,
          paddr_t::make_seg_paddr(segment_id, 0)
        };
        dirty_tail = mkfs_seq;
        alloc_tail = mkfs_seq;
      } else {
        ceph_assert(dirty_tail != JOURNAL_SEQ_NULL);
        ceph_assert(alloc_tail != JOURNAL_SEQ_NULL);
      }
    } else { // OOL
      ceph_assert(!is_mkfs);
      dirty_tail = JOURNAL_SEQ_NULL;
      alloc_tail = JOURNAL_SEQ_NULL;
    }
    auto header = segment_header_t{
      new_segment_seq,
      segment_id,
      dirty_tail,
      alloc_tail,
      current_segment_nonce,
      type,
      category,
      gen};
    INFO("{} writing header {}", print_name, header);

    auto header_length = get_block_size();
    bufferlist bl;
    encode(header, bl);
    bufferptr bp(ceph::buffer::create_page_aligned(header_length));
    bp.zero();
    auto iter = bl.cbegin();
    iter.copy(bl.length(), bp.c_str());
    bl.clear();
    bl.append(bp);

    ceph_assert(sref->get_write_ptr() == 0);
    assert((unsigned)header_length == bl.length());
    written_to = header_length;
    auto new_journal_seq = journal_seq_t{
      new_segment_seq,
      paddr_t::make_seg_paddr(segment_id, written_to)};
    segment_provider.update_segment_avail_bytes(
        type, new_journal_seq.offset);
    return sref->write(0, std::move(bl)
    ).handle_error(
      open_ertr::pass_further{},
      crimson::ct_error::assert_all{
        "Invalid error in SegmentAllocator::do_open write"
      }
    ).safe_then([this,
                 FNAME,
                 new_journal_seq,
                 sref=std::move(sref)]() mutable {
      ceph_assert(!current_segment);
      current_segment = std::move(sref);
      DEBUG("{} rolled new segment id={}",
            print_name, current_segment->get_segment_id());
      ceph_assert(new_journal_seq.segment_seq ==
        segment_provider.get_seg_info(current_segment->get_segment_id()).seq);
      return new_journal_seq;
    });
  });
}

SegmentAllocator::open_ret
SegmentAllocator::open(bool is_mkfs)
{
  LOG_PREFIX(SegmentAllocator::open);
  auto& device_ids = sm_group.get_device_ids();
  ceph_assert(device_ids.size());
  std::ostringstream oss;
  for (auto& device_id : device_ids) {
    oss << device_id_printer_t{device_id} << "_";
  }
  oss << fmt::format("{}_G{}", category, gen);
  print_name = oss.str();

  DEBUG("{}", print_name);
  return do_open(is_mkfs);
}

SegmentAllocator::roll_ertr::future<>
SegmentAllocator::roll()
{
  ceph_assert(can_write());
  return close_segment().safe_then([this] {
    return do_open(false).discard_result();
  });
}

SegmentAllocator::write_ret
SegmentAllocator::write(ceph::bufferlist&& to_write)
{
  LOG_PREFIX(SegmentAllocator::write);
  assert(can_write());
  auto write_length = to_write.length();
  auto write_start_offset = written_to;
  auto write_start_seq = journal_seq_t{
    segment_provider.get_seg_info(current_segment->get_segment_id()).seq,
    paddr_t::make_seg_paddr(
      current_segment->get_segment_id(), write_start_offset)
  };
  TRACE("{} {}~{}", print_name, write_start_seq, write_length);
  assert(write_length > 0);
  assert((write_length % get_block_size()) == 0);
  assert(!needs_roll(write_length));

  auto write_result = write_result_t{
    write_start_seq,
    static_cast<seastore_off_t>(write_length)
  };
  written_to += write_length;
  segment_provider.update_segment_avail_bytes(
    type,
    paddr_t::make_seg_paddr(
      current_segment->get_segment_id(), written_to)
  );
  return current_segment->write(
    write_start_offset, std::move(to_write)
  ).handle_error(
    write_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in SegmentAllocator::write"
    }
  ).safe_then([write_result, cs=current_segment] {
    return write_result;
  });
}

SegmentAllocator::close_ertr::future<>
SegmentAllocator::close()
{
  return [this] {
    LOG_PREFIX(SegmentAllocator::close);
    if (current_segment) {
      DEBUG("{} close current segment", print_name);
      return close_segment();
    } else {
      INFO("{} no current segment", print_name);
      return close_segment_ertr::now();
    }
  }().finally([this] {
    reset();
  });
}

SegmentAllocator::close_segment_ertr::future<>
SegmentAllocator::close_segment()
{
  LOG_PREFIX(SegmentAllocator::close_segment);
  assert(can_write());
  // Note: make sure no one can access the current segment once closing
  auto seg_to_close = std::move(current_segment);
  auto close_segment_id = seg_to_close->get_segment_id();
  segment_provider.close_segment(close_segment_id);
  auto close_seg_info = segment_provider.get_seg_info(close_segment_id);
  ceph_assert((close_seg_info.modify_time == NULL_TIME &&
               close_seg_info.num_extents == 0) ||
              (close_seg_info.modify_time != NULL_TIME &&
               close_seg_info.num_extents != 0));
  auto tail = segment_tail_t{
    close_seg_info.seq,
    close_segment_id,
    current_segment_nonce,
    type,
    timepoint_to_mod(close_seg_info.modify_time),
    close_seg_info.num_extents};
  ceph::bufferlist bl;
  encode(tail, bl);
  INFO("{} close segment {}, written_to={}",
       print_name,
       tail,
       written_to);

  bufferptr bp(ceph::buffer::create_page_aligned(get_block_size()));
  bp.zero();
  auto iter = bl.cbegin();
  iter.copy(bl.length(), bp.c_str());
  bl.clear();
  bl.append(bp);

  assert(bl.length() == sm_group.get_rounded_tail_length());
  return seg_to_close->write(
    sm_group.get_segment_size() - sm_group.get_rounded_tail_length(),
    bl
  ).safe_then([seg_to_close=std::move(seg_to_close)] {
    return seg_to_close->close();
  }).handle_error(
    close_segment_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in SegmentAllocator::close_segment"
    }
  );
}

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

std::pair<ceph::bufferlist, record_group_size_t>
RecordBatch::encode_batch(
  const journal_seq_t& committed_to,
  segment_nonce_t segment_nonce)
{
  assert(state == state_t::PENDING);
  assert(pending.get_size() > 0);
  assert(io_promise.has_value());

  state = state_t::SUBMITTING;
  submitting_size = pending.get_size();
  auto gsize = pending.size;
  submitting_length = gsize.get_encoded_length();
  submitting_mdlength = gsize.get_mdlength();
  auto bl = encode_records(pending, committed_to, segment_nonce);
  // Note: pending is cleared here
  assert(bl.length() == (std::size_t)submitting_length);
  return std::make_pair(bl, gsize);
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

std::pair<ceph::bufferlist, record_group_size_t>
RecordBatch::submit_pending_fast(
  record_t&& record,
  extent_len_t block_size,
  const journal_seq_t& committed_to,
  segment_nonce_t segment_nonce)
{
  auto new_size = get_encoded_length_after(record, block_size);
  std::ignore = new_size;
  assert(state == state_t::EMPTY);
  assert(evaluate_submit(record.size, block_size).submit_size == new_size);

  auto group = record_group_t(std::move(record), block_size);
  auto size = group.size;
  assert(size == new_size);
  auto bl = encode_records(group, committed_to, segment_nonce);
  assert(bl.length() == size.get_encoded_length());
  return std::make_pair(std::move(bl), size);
}

RecordSubmitter::RecordSubmitter(
  std::size_t io_depth,
  std::size_t batch_capacity,
  std::size_t batch_flush_size,
  double preferred_fullness,
  SegmentAllocator& sa)
  : io_depth_limit{io_depth},
    preferred_fullness{preferred_fullness},
    segment_allocator{sa},
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
    // invariants when available
    ceph_assert(segment_allocator.can_write());
    ceph_assert(p_current_batch != nullptr);
    ceph_assert(!p_current_batch->is_submitting());
    ceph_assert(!p_current_batch->needs_flush());
    if (!p_current_batch->is_empty()) {
      auto submit_length =
        p_current_batch->get_submit_size().get_encoded_length();
      ceph_assert(!segment_allocator.needs_roll(submit_length));
    }
  }
#endif
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
      rsize, segment_allocator.get_block_size());
  if (segment_allocator.needs_roll(eval.submit_size.get_encoded_length())) {
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
  assert(is_available());
  // #1 block concurrent submissions due to rolling
  wait_available_promise = seastar::shared_promise<>();
  assert(!wait_unfull_flush_promise.has_value());
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
      std::ignore = segment_allocator.roll(
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
RecordSubmitter::submit(record_t&& record)
{
  LOG_PREFIX(RecordSubmitter::submit);
  assert(is_available());
  assert(check_action(record.size) != action_t::ROLL);
  segment_allocator.get_provider().update_modify_time(
      segment_allocator.get_segment_id(),
      record.modify_time,
      record.extents.size());
  auto eval = p_current_batch->evaluate_submit(
      record.size, segment_allocator.get_block_size());
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
    auto [to_write, sizes] = p_current_batch->submit_pending_fast(
      std::move(record),
      segment_allocator.get_block_size(),
      committed_to,
      segment_allocator.get_nonce());
    DEBUG("{} fast submit {}, committed_to={}, outstanding_io={} ...",
          get_name(), sizes, committed_to, num_outstanding_io);
    account_submission(1, sizes);
    return segment_allocator.write(std::move(to_write)
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
    segment_allocator.get_block_size());
  if (needs_flush) {
    if (state == state_t::FULL) {
      // #2 block concurrent submissions due to lack of resource
      DEBUG("{} added with {} pending, outstanding_io={}, unavailable, wait flush ...",
            get_name(),
            p_current_batch->get_num_records(),
            num_outstanding_io);
      wait_available_promise = seastar::shared_promise<>();
      assert(!wait_unfull_flush_promise.has_value());
      wait_unfull_flush_promise = seastar::promise<>();
      // flush and mark available in background
      std::ignore = wait_unfull_flush_promise->get_future(
      ).finally([FNAME, this] {
        DEBUG("{} flush done, available", get_name());
        wait_available_promise->set_value();
        wait_available_promise.reset();
      });
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
  return segment_allocator.open(is_mkfs
  ).safe_then([this](journal_seq_t ret) {
    LOG_PREFIX(RecordSubmitter::open);
    DEBUG("{} register metrics", get_name());
    stats = {};
    namespace sm = seastar::metrics;
    std::vector<sm::label_instance> label_instances;
    label_instances.push_back(sm::label_instance("submitter", get_name()));
    metrics.add_group(
      "journal",
      {
        sm::make_counter(
          "record_num",
          stats.record_batch_stats.num_io,
          sm::description("total number of records submitted"),
          label_instances
        ),
        sm::make_counter(
          "record_batch_num",
          stats.record_batch_stats.num_io_grouped,
          sm::description("total number of records batched"),
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
  assert(state == state_t::IDLE);
  assert(num_outstanding_io == 0);
  committed_to = JOURNAL_SEQ_NULL;
  assert(p_current_batch != nullptr);
  assert(p_current_batch->is_empty());
  assert(!wait_available_promise.has_value());
  has_io_error = false;
  assert(!wait_unfull_flush_promise.has_value());
  metrics.clear();
  return segment_allocator.close();
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
    assert(!wait_unfull_flush_promise.has_value());
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
  std::size_t num,
  const record_group_size_t& size)
{
  stats.record_group_padding_bytes +=
    (size.get_mdlength() - size.get_raw_mdlength());
  stats.record_group_metadata_bytes += size.get_raw_mdlength();
  stats.record_group_data_bytes += size.dlength;
  stats.record_batch_stats.increment(num);
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
  auto [to_write, sizes] = p_batch->encode_batch(
    committed_to, segment_allocator.get_nonce());
  DEBUG("{} {} records, {}, committed_to={}, outstanding_io={} ...",
        get_name(), num, sizes, committed_to, num_outstanding_io);
  account_submission(num, sizes);
  std::ignore = segment_allocator.write(std::move(to_write)
  ).safe_then([this, p_batch, FNAME, num, sizes=sizes](auto write_result) {
    TRACE("{} {} records, {}, write done with {}",
          get_name(), num, sizes, write_result);
    finish_submit_batch(p_batch, write_result);
  }).handle_error(
    crimson::ct_error::all_same_way([this, p_batch, FNAME, num, sizes=sizes](auto e) {
      ERROR("{} {} records, {}, got error {}",
            get_name(), num, sizes, e);
      finish_submit_batch(p_batch, std::nullopt);
    })
  ).handle_exception([this, p_batch, FNAME, num, sizes=sizes](auto e) {
    ERROR("{} {} records, {}, got exception {}",
          get_name(), num, sizes, e);
    finish_submit_batch(p_batch, std::nullopt);
  });
}

}
