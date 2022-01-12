// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include <boost/iterator/counting_iterator.hpp>

#include "crimson/common/config_proxy.h"
#include "crimson/os/seastore/journal.h"

#include "include/intarith.h"
#include "crimson/os/seastore/segment_cleaner.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore_journal);
  }
}

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

Journal::prep_replay_segments_fut
Journal::prep_replay_segments(
  std::vector<std::pair<segment_id_t, segment_header_t>> segments)
{
  logger().debug(
    "Journal::prep_replay_segments: have {} segments",
    segments.size());
  if (segments.empty()) {
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
    [this](auto &seg) {
      segment_provider->init_mark_segment_closed(
	seg.first,
	seg.second.journal_segment_seq,
	false);
    });

  auto journal_tail = segments.rbegin()->second.journal_tail;
  segment_provider->update_journal_tail_committed(journal_tail);
  auto replay_from = journal_tail.offset;
  logger().debug(
    "Journal::prep_replay_segments: journal_tail={}",
    journal_tail);
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
      logger().error(
	"Journal::prep_replay_segments: journal_tail {} does not match {}",
	journal_tail,
	from->second);
      assert(0 == "invalid");
    }
  } else {
    replay_from = paddr_t::make_seg_paddr(
      from->first,
      journal_segment_manager.get_block_size());
  }
  auto ret = replay_segments_t(segments.end() - from);
  std::transform(
    from, segments.end(), ret.begin(),
    [this](const auto &p) {
      auto ret = journal_seq_t{
	p.second.journal_segment_seq,
	paddr_t::make_seg_paddr(
	  p.first,
	  journal_segment_manager.get_block_size())
      };
      logger().debug(
	"Journal::prep_replay_segments: replaying from  {}",
	ret);
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
  logger().debug("Journal::replay_segment: starting at {}", seq);
  return seastar::do_with(
    scan_valid_records_cursor(seq),
    ExtentReader::found_record_handler_t([=, &handler](
      record_locator_t locator,
      const record_group_header_t& header,
      const bufferlist& mdbuf)
      -> ExtentReader::scan_valid_records_ertr::future<>
    {
      logger().debug("Journal::replay_segment: decoding {} records",
                     header.records);
      auto maybe_record_deltas_list = try_decode_deltas(
          header, mdbuf, locator.record_block_base);
      if (!maybe_record_deltas_list) {
        // This should be impossible, we did check the crc on the mdbuf
        logger().error(
          "Journal::replay_segment: unable to decode deltas for record {}",
          locator.record_block_base);
        return crimson::ct_error::input_output_error::make();
      }

      return seastar::do_with(
        std::move(*maybe_record_deltas_list),
        [write_result=locator.write_result,
         this,
         &handler](auto& record_deltas_list)
      {
        return crimson::do_for_each(
          record_deltas_list,
          [write_result,
           this,
           &handler](record_deltas_t& record_deltas)
        {
          logger().debug("Journal::replay_segment: decoded {} deltas at block_base {}",
                         record_deltas.deltas.size(),
                         record_deltas.record_block_base);
          auto locator = record_locator_t{
            record_deltas.record_block_base,
            write_result
          };
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
  return seastar::do_with(
    std::move(delta_handler), replay_segments_t(),
    [this, segment_headers=std::move(segment_headers)]
    (auto &handler, auto &segments) mutable -> replay_ret {
      return prep_replay_segments(std::move(segment_headers)).safe_then(
        [this, &handler, &segments](auto replay_segs) mutable {
          logger().debug("Journal::replay: found {} segments", replay_segs.size());
          segments = std::move(replay_segs);
          return crimson::do_for_each(segments, [this, &handler](auto i) mutable {
            return replay_segment(i.first, i.second, handler);
          });
        });
    });
}

void Journal::register_metrics()
{
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

Journal::JournalSegmentManager::close_ertr::future<>
Journal::JournalSegmentManager::close()
{
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
  auto old_segment_id = current_journal_segment ?
    current_journal_segment->get_segment_id() :
    NULL_SEG_ID;

  return (
    current_journal_segment ?
    current_journal_segment->close() :
    Segment::close_ertr::now()
  ).safe_then([this] {
    return segment_provider->get_segment(segment_manager.get_device_id());
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
  auto write_length = to_write.length();
  auto write_start_seq = get_current_write_seq();
  logger().debug(
    "JournalSegmentManager::write: write_start {} => {}, length={}",
    write_start_seq,
    write_start_seq.offset.as_seg_paddr().get_segment_off() + write_length,
    write_length);
  assert(write_length > 0);
  assert((write_length % segment_manager.get_block_size()) == 0);
  assert(!needs_roll(write_length));

  auto write_start_offset = written_to;
  written_to += write_length;
  auto write_result = write_result_t{
    write_start_seq,
    static_cast<segment_off_t>(write_length)
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
  logger().debug(
    "JournalSegmentManager::mark_committed: committed_to {} => {}",
    committed_to, new_committed_to);
  assert(committed_to == journal_seq_t() ||
         committed_to <= new_committed_to);
  committed_to = new_committed_to;
}

Journal::JournalSegmentManager::initialize_segment_ertr::future<>
Journal::JournalSegmentManager::initialize_segment(Segment& segment)
{
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
  logger().debug(
    "JournalSegmentManager::initialize_segment: segment_id {} journal_tail_target {}, header {}",
    segment.get_segment_id(),
    new_tail,
    header);
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
  extent_len_t block_size)
{
  auto new_size = get_encoded_length_after(record, block_size);
  logger().debug(
    "Journal::RecordBatch::add_pending: batches={}, write_size={}",
    pending.get_size() + 1,
    new_size.get_encoded_length());
  assert(state != state_t::SUBMITTING);
  assert(can_batch(record, block_size).value() == new_size);

  auto dlength_offset = pending.current_dlength;
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
  ).then([dlength_offset
         ](auto maybe_promise_result) -> add_pending_ret {
    if (!maybe_promise_result.has_value()) {
      return crimson::ct_error::input_output_error::make();
    }
    auto write_result = maybe_promise_result->write_result;
    auto submit_result = record_locator_t{
      write_result.start_seq.offset.add_offset(
          maybe_promise_result->mdlength + dlength_offset),
      write_result
    };
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
  logger().debug(
    "Journal::RecordBatch::encode_batch: batches={}, committed_to={}",
    pending.get_size(),
    committed_to);
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
    logger().debug(
      "Journal::RecordBatch::set_result: batches={}, write_start {} + {}",
      submitting_size,
      maybe_write_result->start_seq,
      maybe_write_result->length);
    assert(maybe_write_result->length == submitting_length);
    result = promise_result_t{
      *maybe_write_result,
      submitting_mdlength
    };
  } else {
    logger().error(
      "Journal::RecordBatch::set_result: batches={}, write is failed!",
      submitting_size);
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
  logger().debug(
    "Journal::RecordBatch::submit_pending_fast: write_size={}",
    new_size.get_encoded_length());
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
  logger().info("Journal::RecordSubmitter: io_depth_limit={}, "
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
  assert(write_pipeline);
  auto expected_size = record_group_size_t(
      record.size,
      journal_segment_manager.get_block_size()
  ).get_encoded_length();
  auto max_record_length = journal_segment_manager.get_max_write_length();
  if (expected_size > max_record_length) {
    logger().error(
      "Journal::RecordSubmitter::submit: record size {} exceeds max {}",
      expected_size,
      max_record_length
    );
    return crimson::ct_error::erange::make();
  }

  return do_submit(std::move(record), handle);
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

void Journal::RecordSubmitter::account_submission(
  std::size_t num,
  const record_group_size_t& size)
{
  logger().debug("Journal::RecordSubmitter: submitting {} records, "
                 "mdsize={}, dsize={}, fillness={}",
                 num,
                 size.get_raw_mdlength(),
                 size.dlength,
                 ((double)(size.get_raw_mdlength() + size.dlength) /
                  (size.get_mdlength() + size.dlength)));
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
  RecordBatch* p_batch = p_current_batch;
  assert(p_batch->is_pending());
  p_current_batch = nullptr;
  pop_free_batch();

  increment_io();
  auto num = p_batch->get_num_records();
  auto [to_write, sizes] = p_batch->encode_batch(
    journal_segment_manager.get_committed_to(),
    journal_segment_manager.get_nonce());
  account_submission(num, sizes);
  std::ignore = journal_segment_manager.write(to_write
  ).safe_then([this, p_batch](auto write_result) {
    finish_submit_batch(p_batch, write_result);
  }).handle_error(
    crimson::ct_error::all_same_way([this, p_batch](auto e) {
      logger().error(
        "Journal::RecordSubmitter::flush_current_batch: got error {}",
        e);
      finish_submit_batch(p_batch, std::nullopt);
    })
  ).handle_exception([this, p_batch](auto e) {
    logger().error(
      "Journal::RecordSubmitter::flush_current_batch: got exception {}",
      e);
    finish_submit_batch(p_batch, std::nullopt);
  });
}

Journal::RecordSubmitter::submit_pending_ret
Journal::RecordSubmitter::submit_pending(
  record_t&& record,
  OrderingHandle& handle,
  bool flush)
{
  assert(!p_current_batch->is_submitting());
  stats.record_batch_stats.increment(
      p_current_batch->get_num_records() + 1);
  bool do_flush = (flush || state == state_t::IDLE);
  auto write_fut = [this, do_flush, record=std::move(record)]() mutable {
    if (do_flush && p_current_batch->is_empty()) {
      // fast path with direct write
      increment_io();
      auto [to_write, sizes] = p_current_batch->submit_pending_fast(
        std::move(record),
        journal_segment_manager.get_block_size(),
        journal_segment_manager.get_committed_to(),
        journal_segment_manager.get_nonce());
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
        std::move(record), journal_segment_manager.get_block_size());
      if (do_flush) {
        flush_current_batch();
      }
      return write_fut;
    }
  }();
  return handle.enter(write_pipeline->device_submission
  ).then([write_fut=std::move(write_fut)]() mutable {
    return std::move(write_fut);
  }).safe_then([this, &handle](auto submit_result) {
    return handle.enter(write_pipeline->finalize
    ).then([this, submit_result] {
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
  assert(!p_current_batch->is_submitting());
  if (state <= state_t::PENDING) {
    // can increment io depth
    assert(!wait_submit_promise.has_value());
    auto maybe_new_size = p_current_batch->can_batch(
        record, journal_segment_manager.get_block_size());
    if (!maybe_new_size.has_value() ||
        (maybe_new_size->get_encoded_length() >
         journal_segment_manager.get_max_write_length())) {
      assert(p_current_batch->is_pending());
      flush_current_batch();
      return do_submit(std::move(record), handle);
    } else if (journal_segment_manager.needs_roll(
          maybe_new_size->get_encoded_length())) {
      if (p_current_batch->is_pending()) {
        flush_current_batch();
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
    return wait_submit_promise->get_future(
    ).then([this, record=std::move(record), &handle]() mutable {
      return do_submit(std::move(record), handle);
    });
  } else {
    return submit_pending(std::move(record), handle, false);
  }
}

}
