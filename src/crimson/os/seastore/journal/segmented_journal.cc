// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include <boost/iterator/counting_iterator.hpp>

#include "include/intarith.h"

#include "segmented_journal.h"

#include "crimson/common/config_proxy.h"
#include "crimson/os/seastore/logging.h"

SET_SUBSYS(seastore_journal);

/*
 * format:
 * - H<handle-addr> information
 *
 * levels:
 * - INFO:  major initiation, closing, rolling and replay operations
 * - DEBUG: INFO details, major submit operations
 * - TRACE: DEBUG details
 */

namespace crimson::os::seastore::journal {

SegmentedJournal::SegmentedJournal(
  SegmentManager &segment_manager,
  ExtentReader &scanner,
  SegmentProvider &segment_provider)
  : segment_provider(segment_provider),
    journal_segment_allocator("JOURNAL",
                              segment_type_t::JOURNAL,
                              segment_provider,
                              segment_manager),
    record_submitter(crimson::common::get_conf<uint64_t>(
                       "seastore_journal_iodepth_limit"),
                     crimson::common::get_conf<uint64_t>(
                       "seastore_journal_batch_capacity"),
                     crimson::common::get_conf<Option::size_t>(
                       "seastore_journal_batch_flush_size"),
                     crimson::common::get_conf<double>(
                       "seastore_journal_batch_preferred_fullness"),
                     journal_segment_allocator),
    scanner(scanner)
{
  register_metrics();
}

SegmentedJournal::open_for_write_ret SegmentedJournal::open_for_write()
{
  LOG_PREFIX(Journal::open_for_write);
  INFO("device_id={}", journal_segment_allocator.get_device_id());
  return journal_segment_allocator.open();
}

SegmentedJournal::close_ertr::future<> SegmentedJournal::close()
{
  LOG_PREFIX(Journal::close);
  INFO("closing, committed_to={}",
       record_submitter.get_committed_to());
  metrics.clear();
  return journal_segment_allocator.close();
}

SegmentedJournal::prep_replay_segments_fut
SegmentedJournal::prep_replay_segments(
  std::vector<std::pair<segment_id_t, segment_header_t>> segments)
{
  LOG_PREFIX(Journal::prep_replay_segments);
  if (segments.empty()) {
    ERROR("no journal segments for replay");
    return crimson::ct_error::input_output_error::make();
  }
  std::sort(
    segments.begin(),
    segments.end(),
    [](const auto &lt, const auto &rt) {
      return lt.second.journal_segment_seq <
	rt.second.journal_segment_seq;
    });

  journal_segment_allocator.set_next_segment_seq(
    segments.rbegin()->second.journal_segment_seq + 1);
  std::for_each(
    segments.begin(),
    segments.end(),
    [this, FNAME](auto &seg)
  {
    if (seg.first != seg.second.physical_segment_id ||
        seg.first.device_id() != journal_segment_allocator.get_device_id() ||
        seg.second.get_type() != segment_type_t::JOURNAL) {
      ERROR("illegal journal segment for replay -- {}", seg.second);
      ceph_abort();
    }
  });

  auto journal_tail = segments.rbegin()->second.journal_tail;
  segment_provider.update_journal_tail_committed(journal_tail);
  auto replay_from = journal_tail.offset;
  auto from = segments.begin();
  if (replay_from != P_ADDR_NULL) {
    from = std::find_if(
      segments.begin(),
      segments.end(),
      [&replay_from](const auto &seg) -> bool {
	auto& seg_addr = replay_from.as_seg_paddr();
	return seg.first == seg_addr.get_segment_id();
      });
    if (from->second.journal_segment_seq != journal_tail.segment_seq) {
      ERROR("journal_tail {} does not match {}",
            journal_tail, from->second);
      ceph_abort();
    }
  } else {
    replay_from = paddr_t::make_seg_paddr(
      from->first,
      journal_segment_allocator.get_block_size());
  }

  auto num_segments = segments.end() - from;
  INFO("{} segments to replay, from {}",
       num_segments, replay_from);
  auto ret = replay_segments_t(num_segments);
  std::transform(
    from, segments.end(), ret.begin(),
    [this](const auto &p) {
      auto ret = journal_seq_t{
	p.second.journal_segment_seq,
	paddr_t::make_seg_paddr(
	  p.first,
	  journal_segment_allocator.get_block_size())
      };
      return std::make_pair(ret, p.second);
    });
  ret[0].first.offset = replay_from;
  return prep_replay_segments_fut(
    prep_replay_segments_ertr::ready_future_marker{},
    std::move(ret));
}

SegmentedJournal::replay_ertr::future<>
SegmentedJournal::replay_segment(
  journal_seq_t seq,
  segment_header_t header,
  delta_handler_t &handler)
{
  LOG_PREFIX(Journal::replay_segment);
  INFO("starting at {} -- {}", seq, header);
  return seastar::do_with(
    scan_valid_records_cursor(seq),
    ExtentReader::found_record_handler_t([=, &handler](
      record_locator_t locator,
      const record_group_header_t& header,
      const bufferlist& mdbuf)
      -> ExtentReader::scan_valid_records_ertr::future<>
    {
      auto maybe_record_deltas_list = try_decode_deltas(
          header, mdbuf, locator.record_block_base);
      if (!maybe_record_deltas_list) {
        // This should be impossible, we did check the crc on the mdbuf
        ERROR("unable to decode deltas for record {} at {}",
              header, locator);
        return crimson::ct_error::input_output_error::make();
      }

      return seastar::do_with(
        std::move(*maybe_record_deltas_list),
        [write_result=locator.write_result,
         this,
         FNAME,
         &handler](auto& record_deltas_list)
      {
        return crimson::do_for_each(
          record_deltas_list,
          [write_result,
           this,
           FNAME,
           &handler](record_deltas_t& record_deltas)
        {
          auto locator = record_locator_t{
            record_deltas.record_block_base,
            write_result
          };
          DEBUG("processing {} deltas at block_base {}",
                record_deltas.deltas.size(),
                locator);
          return crimson::do_for_each(
            record_deltas.deltas,
            [locator,
             this,
             FNAME,
             &handler](auto &p)
          {
	    auto& commit_time = p.first;
	    auto& delta = p.second;
            /* The journal may validly contain deltas for extents in
             * since released segments.  We can detect those cases by
             * checking whether the segment in question currently has a
             * sequence number > the current journal segment seq. We can
             * safetly skip these deltas because the extent must already
             * have been rewritten.
             */
            if (delta.paddr != P_ADDR_NULL) {
              auto& seg_addr = delta.paddr.as_seg_paddr();
              auto delta_paddr_segment_seq = segment_provider.get_seq(seg_addr.get_segment_id());
              auto delta_paddr_segment_type = segment_seq_to_type(delta_paddr_segment_seq);
              auto locator_segment_seq = locator.write_result.start_seq.segment_seq;
              if (delta_paddr_segment_type == segment_type_t::NULL_SEG ||
                  (delta_paddr_segment_type == segment_type_t::JOURNAL &&
                   delta_paddr_segment_seq > locator_segment_seq)) {
                SUBDEBUG(seastore_cache,
                         "delta is obsolete, delta_paddr_segment_seq={}, locator_segment_seq={} -- {}",
                         segment_seq_printer_t{delta_paddr_segment_seq},
                         segment_seq_printer_t{locator_segment_seq},
                         delta);
                return replay_ertr::now();
              }
            }
	    return handler(
	      locator,
	      delta,
	      seastar::lowres_system_clock::time_point(
		seastar::lowres_system_clock::duration(commit_time)));
          });
        });
      });
    }),
    [=](auto &cursor, auto &dhandler) {
      return scanner.scan_valid_records(
	cursor,
	header.segment_nonce,
	std::numeric_limits<size_t>::max(),
	dhandler).safe_then([](auto){}
      ).handle_error(
	replay_ertr::pass_further{},
	crimson::ct_error::assert_all{
	  "shouldn't meet with any other error other replay_ertr"
	}
      );
    }
  );
}

SegmentedJournal::find_journal_segments_ret
SegmentedJournal::find_journal_segments()
{
  return seastar::do_with(
    find_journal_segments_ret_bare{},
    [this](auto &ret) -> find_journal_segments_ret {
      return crimson::do_for_each(
	boost::counting_iterator<device_segment_id_t>(0),
	boost::counting_iterator<device_segment_id_t>(
	  journal_segment_allocator.get_num_segments()),
	[this, &ret](device_segment_id_t d_segment_id) {
	  segment_id_t segment_id{
	    journal_segment_allocator.get_device_id(),
	    d_segment_id};
	  return scanner.read_segment_header(
	    segment_id
	  ).safe_then([segment_id, &ret](auto &&header) {
	    if (header.get_type() == segment_type_t::JOURNAL) {
	      ret.emplace_back(std::make_pair(segment_id, std::move(header)));
	    }
	  }).handle_error(
	    crimson::ct_error::enoent::handle([](auto) {
	      return find_journal_segments_ertr::now();
	    }),
	    crimson::ct_error::enodata::handle([](auto) {
	      return find_journal_segments_ertr::now();
	    }),
	    crimson::ct_error::input_output_error::pass_further{}
	  );
	}).safe_then([&ret]() mutable {
	  return find_journal_segments_ret{
	    find_journal_segments_ertr::ready_future_marker{},
	    std::move(ret)};
	});
    });
}

SegmentedJournal::replay_ret SegmentedJournal::replay(
  delta_handler_t &&delta_handler)
{
  LOG_PREFIX(Journal::replay);
  return find_journal_segments(
  ).safe_then([this, FNAME, delta_handler=std::move(delta_handler)]
    (auto &&segment_headers) mutable -> replay_ret {
    INFO("got {} segments", segment_headers.size());
    return seastar::do_with(
      std::move(delta_handler), replay_segments_t(),
      [this, segment_headers=std::move(segment_headers)]
      (auto &handler, auto &segments) mutable -> replay_ret {
	return prep_replay_segments(std::move(segment_headers)
	).safe_then([this, &handler, &segments](auto replay_segs) mutable {
	  segments = std::move(replay_segs);
	  return crimson::do_for_each(segments, [this, &handler](auto i) mutable {
	    return replay_segment(i.first, i.second, handler);
	  });
	});
      });
  });
}

seastar::future<> SegmentedJournal::flush(OrderingHandle &handle)
{
  LOG_PREFIX(SegmentedJournal::flush);
  DEBUG("H{} flush ...", (void*)&handle);
  assert(write_pipeline);
  return handle.enter(write_pipeline->device_submission
  ).then([this, &handle] {
    return handle.enter(write_pipeline->finalize);
  }).then([FNAME, &handle] {
    DEBUG("H{} flush done", (void*)&handle);
  });
}

SegmentedJournal::submit_record_ret
SegmentedJournal::do_submit_record(
  record_t &&record,
  OrderingHandle &handle)
{
  LOG_PREFIX(SegmentedJournal::do_submit_record);
  if (!record_submitter.is_available()) {
    DEBUG("H{} wait ...", (void*)&handle);
    return record_submitter.wait_available(
    ).safe_then([this, record=std::move(record), &handle]() mutable {
      return do_submit_record(std::move(record), handle);
    });
  }
  auto action = record_submitter.check_action(record.size);
  if (action == RecordSubmitter::action_t::ROLL) {
    DEBUG("H{} roll, unavailable ...", (void*)&handle);
    return record_submitter.roll_segment(
    ).safe_then([this, record=std::move(record), &handle]() mutable {
      return do_submit_record(std::move(record), handle);
    });
  } else { // SUBMIT_FULL/NOT_FULL
    DEBUG("H{} submit {} ...",
          (void*)&handle,
          action == RecordSubmitter::action_t::SUBMIT_FULL ?
          "FULL" : "NOT_FULL");
    auto submit_fut = record_submitter.submit(std::move(record));
    return handle.enter(write_pipeline->device_submission
    ).then([submit_fut=std::move(submit_fut)]() mutable {
      return std::move(submit_fut);
    }).safe_then([FNAME, this, &handle](record_locator_t result) {
      return handle.enter(write_pipeline->finalize
      ).then([FNAME, this, result, &handle] {
        DEBUG("H{} finish with {}", (void*)&handle, result);
        auto new_committed_to = result.write_result.get_end_seq();
        record_submitter.update_committed_to(new_committed_to);
        return result;
      });
    });
  }
}

SegmentedJournal::submit_record_ret
SegmentedJournal::submit_record(
    record_t &&record,
    OrderingHandle &handle)
{
  LOG_PREFIX(SegmentedJournal::submit_record);
  DEBUG("H{} {} start ...", (void*)&handle, record);
  assert(write_pipeline);
  auto expected_size = record_group_size_t(
      record.size,
      journal_segment_allocator.get_block_size()
  ).get_encoded_length();
  auto max_record_length = journal_segment_allocator.get_max_write_length();
  if (expected_size > max_record_length) {
    ERROR("H{} {} exceeds max record size {}",
          (void*)&handle, record, max_record_length);
    return crimson::ct_error::erange::make();
  }

  return do_submit_record(std::move(record), handle);
}

void SegmentedJournal::register_metrics()
{
  LOG_PREFIX(Journal::register_metrics);
  DEBUG("");
  record_submitter.reset_stats();
  namespace sm = seastar::metrics;
  metrics.add_group(
    "journal",
    {
      sm::make_counter(
        "record_num",
        [this] {
          return record_submitter.get_record_batch_stats().num_io;
        },
        sm::description("total number of records submitted")
      ),
      sm::make_counter(
        "record_batch_num",
        [this] {
          return record_submitter.get_record_batch_stats().num_io_grouped;
        },
        sm::description("total number of records batched")
      ),
      sm::make_counter(
        "io_num",
        [this] {
          return record_submitter.get_io_depth_stats().num_io;
        },
        sm::description("total number of io submitted")
      ),
      sm::make_counter(
        "io_depth_num",
        [this] {
          return record_submitter.get_io_depth_stats().num_io_grouped;
        },
        sm::description("total number of io depth")
      ),
      sm::make_counter(
        "record_group_padding_bytes",
        [this] {
          return record_submitter.get_record_group_padding_bytes();
        },
        sm::description("bytes of metadata padding when write record groups")
      ),
      sm::make_counter(
        "record_group_metadata_bytes",
        [this] {
          return record_submitter.get_record_group_metadata_bytes();
        },
        sm::description("bytes of raw metadata when write record groups")
      ),
      sm::make_counter(
        "record_group_data_bytes",
        [this] {
          return record_submitter.get_record_group_data_bytes();
        },
        sm::description("bytes of data when write record groups")
      ),
    }
  );
}

SegmentedJournal::RecordBatch::add_pending_ret
SegmentedJournal::RecordBatch::add_pending(
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
SegmentedJournal::RecordBatch::encode_batch(
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

void SegmentedJournal::RecordBatch::set_result(
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
SegmentedJournal::RecordBatch::submit_pending_fast(
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
  return std::make_pair(bl, size);
}

SegmentedJournal::RecordSubmitter::RecordSubmitter(
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

bool SegmentedJournal::RecordSubmitter::is_available() const
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

SegmentedJournal::RecordSubmitter::wa_ertr::future<>
SegmentedJournal::RecordSubmitter::wait_available()
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

SegmentedJournal::RecordSubmitter::action_t
SegmentedJournal::RecordSubmitter::check_action(
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

SegmentedJournal::RecordSubmitter::roll_segment_ertr::future<>
SegmentedJournal::RecordSubmitter::roll_segment()
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

SegmentedJournal::RecordSubmitter::submit_ret
SegmentedJournal::RecordSubmitter::submit(record_t&& record)
{
  LOG_PREFIX(RecordSubmitter::submit);
  assert(is_available());
  assert(check_action(record.size) != action_t::ROLL);
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
    return segment_allocator.write(to_write
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

void SegmentedJournal::RecordSubmitter::update_state()
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

void SegmentedJournal::RecordSubmitter::decrement_io_with_flush()
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

void SegmentedJournal::RecordSubmitter::account_submission(
  std::size_t num,
  const record_group_size_t& size)
{
  stats.record_group_padding_bytes +=
    (size.get_mdlength() - size.get_raw_mdlength());
  stats.record_group_metadata_bytes += size.get_raw_mdlength();
  stats.record_group_data_bytes += size.dlength;
}

void SegmentedJournal::RecordSubmitter::finish_submit_batch(
  RecordBatch* p_batch,
  maybe_result_t maybe_result)
{
  assert(p_batch->is_submitting());
  p_batch->set_result(maybe_result);
  free_batch_ptrs.push_back(p_batch);
  decrement_io_with_flush();
}

void SegmentedJournal::RecordSubmitter::flush_current_batch()
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
  std::ignore = segment_allocator.write(to_write
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
