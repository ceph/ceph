// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include <boost/iterator/counting_iterator.hpp>

#include "include/intarith.h"

#include "crimson/common/config_proxy.h"
#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/segment_cleaner.h"

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

namespace crimson::os::seastore {

segment_nonce_t generate_nonce(
  segment_seq_t seq,
  const seastore_meta_t &meta)
{
  return ceph_crc32c(
    seq,
    reinterpret_cast<const unsigned char *>(meta.seastore_id.bytes()),
    sizeof(meta.seastore_id.uuid));
}

Journal::Journal(
  SegmentManager& segment_manager,
  ExtentReader& scanner)
  : journal_segment_manager(segment_manager),
    record_submitter(crimson::common::get_conf<uint64_t>(
                       "seastore_journal_iodepth_limit"),
                     crimson::common::get_conf<uint64_t>(
                       "seastore_journal_batch_capacity"),
                     crimson::common::get_conf<Option::size_t>(
                       "seastore_journal_batch_flush_size"),
                     crimson::common::get_conf<double>(
                       "seastore_journal_batch_preferred_fullness"),
                     journal_segment_manager),
    scanner(scanner)
{
  register_metrics();
}

Journal::open_for_write_ret Journal::open_for_write()
{
  LOG_PREFIX(Journal::open_for_write);
  INFO("device_id={}", journal_segment_manager.get_device_id());
  return journal_segment_manager.open();
}

Journal::close_ertr::future<> Journal::close()
{
  LOG_PREFIX(Journal::close);
  INFO("closing");
  metrics.clear();
  return journal_segment_manager.close();
}

Journal::prep_replay_segments_fut
Journal::prep_replay_segments(
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

  journal_segment_manager.set_segment_seq(
    segments.rbegin()->second.journal_segment_seq);
  std::for_each(
    segments.begin(),
    segments.end(),
    [this, FNAME](auto &seg)
  {
    if (seg.first != seg.second.physical_segment_id ||
        seg.first.device_id() != journal_segment_manager.get_device_id() ||
        seg.second.out_of_line == true) {
      ERROR("illegal journal segment for replay -- {}", seg.second);
      ceph_abort();
    }
    segment_provider->init_mark_segment_closed(
      seg.first,
      seg.second.journal_segment_seq,
      false);
  });

  auto journal_tail = segments.rbegin()->second.journal_tail;
  segment_provider->update_journal_tail_committed(journal_tail);
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
      journal_segment_manager.get_block_size());
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
	  journal_segment_manager.get_block_size())
      };
      return std::make_pair(ret, p.second);
    });
  ret[0].first.offset = replay_from;
  return prep_replay_segments_fut(
    prep_replay_segments_ertr::ready_future_marker{},
    std::move(ret));
}

Journal::replay_ertr::future<>
Journal::replay_segment(
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
             &handler](delta_info_t& delta)
          {
            /* The journal may validly contain deltas for extents in
             * since released segments.  We can detect those cases by
             * checking whether the segment in question currently has a
             * sequence number > the current journal segment seq. We can
             * safetly skip these deltas because the extent must already
             * have been rewritten.
             *
             * Note, this comparison exploits the fact that
             * SEGMENT_SEQ_NULL is a large number.
             */
            auto& seg_addr = delta.paddr.as_seg_paddr();
            if (delta.paddr != P_ADDR_NULL &&
                (segment_provider->get_seq(seg_addr.get_segment_id()) >
                 locator.write_result.start_seq.segment_seq)) {
              return replay_ertr::now();
            } else {
              return handler(locator, delta);
            }
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

Journal::replay_ret Journal::replay(
  std::vector<std::pair<segment_id_t, segment_header_t>>&& segment_headers,
  delta_handler_t &&delta_handler)
{
  LOG_PREFIX(Journal::replay);
  INFO("got {} segments", segment_headers.size());
  return seastar::do_with(
    std::move(delta_handler), replay_segments_t(),
    [this, segment_headers=std::move(segment_headers)]
    (auto &handler, auto &segments) mutable -> replay_ret
  {
    return prep_replay_segments(std::move(segment_headers)
    ).safe_then([this, &handler, &segments](auto replay_segs) mutable {
      segments = std::move(replay_segs);
      return crimson::do_for_each(segments, [this, &handler](auto i) mutable {
        return replay_segment(i.first, i.second, handler);
      });
    });
  });
}

void Journal::register_metrics()
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

Journal::JournalSegmentManager::JournalSegmentManager(
  SegmentManager& segment_manager)
  : segment_manager{segment_manager}
{
  reset();
}

Journal::JournalSegmentManager::open_ret
Journal::JournalSegmentManager::open()
{
  return roll().safe_then([this] {
    return get_current_write_seq();
  });
}

Journal::JournalSegmentManager::close_ertr::future<>
Journal::JournalSegmentManager::close()
{
  LOG_PREFIX(JournalSegmentManager::close);
  if (current_journal_segment) {
    INFO("segment_id={}, seq={}, "
         "written_to={}, committed_to={}, nonce={}",
         current_journal_segment->get_segment_id(),
         get_segment_seq(),
         written_to,
         committed_to,
         current_segment_nonce);
  } else {
    INFO("no current journal segment");
  }

  return (
    current_journal_segment ?
    current_journal_segment->close() :
    Segment::close_ertr::now()
  ).handle_error(
    close_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in JournalSegmentManager::close()"
    }
  ).finally([this] {
    reset();
  });
}

Journal::JournalSegmentManager::roll_ertr::future<>
Journal::JournalSegmentManager::roll()
{
  LOG_PREFIX(JournalSegmentManager::roll);
  auto old_segment_id = current_journal_segment ?
    current_journal_segment->get_segment_id() :
    NULL_SEG_ID;
  if (current_journal_segment) {
    INFO("closing segment {}, seq={}, "
         "written_to={}, committed_to={}, nonce={}",
         old_segment_id,
         get_segment_seq(),
         written_to,
         committed_to,
         current_segment_nonce);
  }

  return (
    current_journal_segment ?
    current_journal_segment->close() :
    Segment::close_ertr::now()
  ).safe_then([this] {
    return segment_provider->get_segment(get_device_id());
  }).safe_then([this](auto segment) {
    return segment_manager.open(segment);
  }).safe_then([this](auto sref) {
    current_journal_segment = sref;
    return initialize_segment(*current_journal_segment);
  }).safe_then([this, old_segment_id] {
    if (old_segment_id != NULL_SEG_ID) {
      segment_provider->close_segment(old_segment_id);
    }
    segment_provider->set_journal_segment(
      current_journal_segment->get_segment_id(),
      get_segment_seq());
  }).handle_error(
    roll_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in JournalSegmentManager::roll"
    }
  );
}

Journal::JournalSegmentManager::write_ret
Journal::JournalSegmentManager::write(ceph::bufferlist to_write)
{
  LOG_PREFIX(JournalSegmentManager::write);
  auto write_length = to_write.length();
  auto write_start_seq = get_current_write_seq();
  TRACE("{}~{}", write_start_seq, write_length);
  assert(write_length > 0);
  assert((write_length % segment_manager.get_block_size()) == 0);
  assert(!needs_roll(write_length));

  auto write_start_offset = written_to;
  written_to += write_length;
  auto write_result = write_result_t{
    write_start_seq,
    static_cast<seastore_off_t>(write_length)
  };
  return current_journal_segment->write(
    write_start_offset, to_write
  ).handle_error(
    write_ertr::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error in JournalSegmentManager::write"
    }
  ).safe_then([write_result] {
    return write_result;
  });
}

void Journal::JournalSegmentManager::mark_committed(
  const journal_seq_t& new_committed_to)
{
  LOG_PREFIX(JournalSegmentManager::mark_committed);
  TRACE("{} => {}", committed_to, new_committed_to);
  assert(committed_to == journal_seq_t() ||
         committed_to <= new_committed_to);
  committed_to = new_committed_to;
}

Journal::JournalSegmentManager::initialize_segment_ertr::future<>
Journal::JournalSegmentManager::initialize_segment(Segment& segment)
{
  LOG_PREFIX(JournalSegmentManager::initialize_segment);
  auto new_tail = segment_provider->get_journal_tail_target();
  // write out header
  ceph_assert(segment.get_write_ptr() == 0);
  bufferlist bl;

  segment_seq_t seq = next_journal_segment_seq++;
  current_segment_nonce = generate_nonce(
    seq, segment_manager.get_meta());
  auto header = segment_header_t{
    seq,
    segment.get_segment_id(),
    new_tail,
    current_segment_nonce,
    false};
  INFO("writing {} ...", header);
  encode(header, bl);

  bufferptr bp(
    ceph::buffer::create_page_aligned(
      segment_manager.get_block_size()));
  bp.zero();
  auto iter = bl.cbegin();
  iter.copy(bl.length(), bp.c_str());
  bl.clear();
  bl.append(bp);

  written_to = 0;
  return write(bl
  ).safe_then([this, new_tail](auto) {
    segment_provider->update_journal_tail_committed(new_tail);
  });
}

Journal::RecordBatch::add_pending_ret
Journal::RecordBatch::add_pending(
  record_t&& record,
  OrderingHandle& handle,
  extent_len_t block_size)
{
  LOG_PREFIX(RecordBatch::add_pending);
  auto new_size = get_encoded_length_after(record, block_size);
  auto dlength_offset = pending.size.dlength;
  TRACE("H{} batches={}, write_size={}, dlength_offset={} ...",
        (void*)&handle,
        pending.get_size() + 1,
        new_size.get_encoded_length(),
        dlength_offset);
  assert(state != state_t::SUBMITTING);
  assert(can_batch(record, block_size).value() == new_size);

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
  ).then([dlength_offset, FNAME, &handle
         ](auto maybe_promise_result) -> add_pending_ret {
    if (!maybe_promise_result.has_value()) {
      ERROR("H{} write failed", (void*)&handle);
      return crimson::ct_error::input_output_error::make();
    }
    auto write_result = maybe_promise_result->write_result;
    auto submit_result = record_locator_t{
      write_result.start_seq.offset.add_offset(
          maybe_promise_result->mdlength + dlength_offset),
      write_result
    };
    TRACE("H{} write finish with {}", (void*)&handle, submit_result);
    return add_pending_ret(
      add_pending_ertr::ready_future_marker{},
      submit_result);
  });
}

std::pair<ceph::bufferlist, record_group_size_t>
Journal::RecordBatch::encode_batch(
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

void Journal::RecordBatch::set_result(
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
Journal::RecordBatch::submit_pending_fast(
  record_t&& record,
  extent_len_t block_size,
  const journal_seq_t& committed_to,
  segment_nonce_t segment_nonce)
{
  auto new_size = get_encoded_length_after(record, block_size);
  std::ignore = new_size;
  assert(state == state_t::EMPTY);
  assert(can_batch(record, block_size).value() == new_size);

  auto group = record_group_t(std::move(record), block_size);
  auto size = group.size;
  assert(size == new_size);
  auto bl = encode_records(group, committed_to, segment_nonce);
  assert(bl.length() == size.get_encoded_length());
  return std::make_pair(bl, size);
}

Journal::RecordSubmitter::RecordSubmitter(
  std::size_t io_depth,
  std::size_t batch_capacity,
  std::size_t batch_flush_size,
  double preferred_fullness,
  JournalSegmentManager& jsm)
  : io_depth_limit{io_depth},
    preferred_fullness{preferred_fullness},
    journal_segment_manager{jsm},
    batches(new RecordBatch[io_depth + 1])
{
  LOG_PREFIX(RecordSubmitter);
  INFO("Journal::RecordSubmitter: io_depth_limit={}, "
       "batch_capacity={}, batch_flush_size={}, "
       "preferred_fullness={}",
       io_depth, batch_capacity,
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

Journal::RecordSubmitter::submit_ret
Journal::RecordSubmitter::submit(
  record_t&& record,
  OrderingHandle& handle)
{
  LOG_PREFIX(RecordSubmitter::submit);
  DEBUG("H{} {} start ...", (void*)&handle, record);
  assert(write_pipeline);
  auto expected_size = record_group_size_t(
      record.size,
      journal_segment_manager.get_block_size()
  ).get_encoded_length();
  auto max_record_length = journal_segment_manager.get_max_write_length();
  if (expected_size > max_record_length) {
    ERROR("H{} {} exceeds max record size {}",
          (void*)&handle, record, max_record_length);
    return crimson::ct_error::erange::make();
  }

  return do_submit(std::move(record), handle);
}

seastar::future<> Journal::RecordSubmitter::flush(OrderingHandle &handle)
{
  LOG_PREFIX(RecordSubmitter::flush);
  DEBUG("H{} flush", (void*)&handle);
  return handle.enter(write_pipeline->device_submission
  ).then([this, &handle] {
    return handle.enter(write_pipeline->finalize);
  }).then([FNAME, &handle] {
    DEBUG("H{} flush done", (void*)&handle);
  });
}

void Journal::RecordSubmitter::update_state()
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

void Journal::RecordSubmitter::decrement_io_with_flush()
{
  LOG_PREFIX(RecordSubmitter::decrement_io_with_flush);
  assert(num_outstanding_io > 0);
  --num_outstanding_io;
#ifndef NDEBUG
  auto prv_state = state;
#endif
  update_state();

  if (wait_submit_promise.has_value()) {
    DEBUG("wait resolved");
    assert(prv_state == state_t::FULL);
    wait_submit_promise->set_value();
    wait_submit_promise.reset();
  }

  if (!p_current_batch->is_empty()) {
    TRACE("flush");
    flush_current_batch();
  }
}

void Journal::RecordSubmitter::account_submission(
  std::size_t num,
  const record_group_size_t& size)
{
  stats.record_group_padding_bytes +=
    (size.get_mdlength() - size.get_raw_mdlength());
  stats.record_group_metadata_bytes += size.get_raw_mdlength();
  stats.record_group_data_bytes += size.dlength;
}

void Journal::RecordSubmitter::finish_submit_batch(
  RecordBatch* p_batch,
  maybe_result_t maybe_result)
{
  assert(p_batch->is_submitting());
  p_batch->set_result(maybe_result);
  free_batch_ptrs.push_back(p_batch);
  decrement_io_with_flush();
}

void Journal::RecordSubmitter::flush_current_batch()
{
  LOG_PREFIX(RecordSubmitter::flush_current_batch);
  RecordBatch* p_batch = p_current_batch;
  assert(p_batch->is_pending());
  p_current_batch = nullptr;
  pop_free_batch();

  increment_io();
  auto num = p_batch->get_num_records();
  auto committed_to = journal_segment_manager.get_committed_to();
  auto [to_write, sizes] = p_batch->encode_batch(
    committed_to, journal_segment_manager.get_nonce());
  DEBUG("{} records, {}, committed_to={}, outstanding_io={} ...",
        num, sizes, committed_to, num_outstanding_io);
  account_submission(num, sizes);
  std::ignore = journal_segment_manager.write(to_write
  ).safe_then([this, p_batch, FNAME, num, sizes=sizes](auto write_result) {
    TRACE("{} records, {}, write done with {}", num, sizes, write_result);
    finish_submit_batch(p_batch, write_result);
  }).handle_error(
    crimson::ct_error::all_same_way([this, p_batch, FNAME, num, sizes=sizes](auto e) {
      ERROR("{} records, {}, got error {}", num, sizes, e);
      finish_submit_batch(p_batch, std::nullopt);
    })
  ).handle_exception([this, p_batch, FNAME, num, sizes=sizes](auto e) {
    ERROR("{} records, {}, got exception {}", num, sizes, e);
    finish_submit_batch(p_batch, std::nullopt);
  });
}

Journal::RecordSubmitter::submit_pending_ret
Journal::RecordSubmitter::submit_pending(
  record_t&& record,
  OrderingHandle& handle,
  bool flush)
{
  LOG_PREFIX(RecordSubmitter::submit_pending);
  assert(!p_current_batch->is_submitting());
  stats.record_batch_stats.increment(
      p_current_batch->get_num_records() + 1);
  bool do_flush = (flush || state == state_t::IDLE);
  auto write_fut = [this, do_flush, FNAME, record=std::move(record), &handle]() mutable {
    if (do_flush && p_current_batch->is_empty()) {
      // fast path with direct write
      increment_io();
      auto committed_to = journal_segment_manager.get_committed_to();
      auto [to_write, sizes] = p_current_batch->submit_pending_fast(
        std::move(record),
        journal_segment_manager.get_block_size(),
        committed_to,
        journal_segment_manager.get_nonce());
      DEBUG("H{} fast submit {}, committed_to={}, outstanding_io={} ...",
            (void*)&handle, sizes, committed_to, num_outstanding_io);
      account_submission(1, sizes);
      return journal_segment_manager.write(to_write
      ).safe_then([mdlength = sizes.get_mdlength()](auto write_result) {
        return record_locator_t{
          write_result.start_seq.offset.add_offset(mdlength),
          write_result
        };
      }).finally([this] {
        decrement_io_with_flush();
      });
    } else {
      // indirect write with or without the existing pending records
      auto write_fut = p_current_batch->add_pending(
        std::move(record),
        handle,
        journal_segment_manager.get_block_size());
      if (do_flush) {
        DEBUG("H{} added pending and flush", (void*)&handle);
        flush_current_batch();
      } else {
        DEBUG("H{} added with {} pending",
              (void*)&handle, p_current_batch->get_num_records());
      }
      return write_fut;
    }
  }();
  return handle.enter(write_pipeline->device_submission
  ).then([write_fut=std::move(write_fut)]() mutable {
    return std::move(write_fut);
  }).safe_then([this, FNAME, &handle](auto submit_result) {
    return handle.enter(write_pipeline->finalize
    ).then([this, FNAME, submit_result, &handle] {
      DEBUG("H{} finish with {}", (void*)&handle, submit_result);
      journal_segment_manager.mark_committed(
          submit_result.write_result.get_end_seq());
      return submit_result;
    });
  });
}

Journal::RecordSubmitter::do_submit_ret
Journal::RecordSubmitter::do_submit(
  record_t&& record,
  OrderingHandle& handle)
{
  LOG_PREFIX(RecordSubmitter::do_submit);
  TRACE("H{} outstanding_io={}/{} ...",
        (void*)&handle, num_outstanding_io, io_depth_limit);
  assert(!p_current_batch->is_submitting());
  if (state <= state_t::PENDING) {
    // can increment io depth
    assert(!wait_submit_promise.has_value());
    auto maybe_new_size = p_current_batch->can_batch(
        record, journal_segment_manager.get_block_size());
    if (!maybe_new_size.has_value() ||
        (maybe_new_size->get_encoded_length() >
         journal_segment_manager.get_max_write_length())) {
      TRACE("H{} flush", (void*)&handle);
      assert(p_current_batch->is_pending());
      flush_current_batch();
      return do_submit(std::move(record), handle);
    } else if (journal_segment_manager.needs_roll(
          maybe_new_size->get_encoded_length())) {
      if (p_current_batch->is_pending()) {
        TRACE("H{} flush and roll", (void*)&handle);
        flush_current_batch();
      } else {
        TRACE("H{} roll", (void*)&handle);
      }
      return journal_segment_manager.roll(
      ).safe_then([this, record=std::move(record), &handle]() mutable {
        return do_submit(std::move(record), handle);
      });
    } else {
      bool flush = (maybe_new_size->get_fullness() > preferred_fullness ?
                    true : false);
      return submit_pending(std::move(record), handle, flush);
    }
  }

  assert(state == state_t::FULL);
  // cannot increment io depth
  auto maybe_new_size = p_current_batch->can_batch(
      record, journal_segment_manager.get_block_size());
  if (!maybe_new_size.has_value() ||
      (maybe_new_size->get_encoded_length() >
       journal_segment_manager.get_max_write_length()) ||
      journal_segment_manager.needs_roll(
        maybe_new_size->get_encoded_length())) {
    if (!wait_submit_promise.has_value()) {
      wait_submit_promise = seastar::promise<>();
    }
    DEBUG("H{} wait ...", (void*)&handle);
    return wait_submit_promise->get_future(
    ).then([this, record=std::move(record), &handle]() mutable {
      return do_submit(std::move(record), handle);
    });
  } else {
    return submit_pending(std::move(record), handle, false);
  }
}

}
