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
    return crimson::get_logger(ceph_subsys_seastore);
  }
}

namespace crimson::os::seastore {

std::ostream &operator<<(std::ostream &out, const segment_header_t &header)
{
  return out << "segment_header_t("
	     << "segment_seq=" << header.journal_segment_seq
	     << ", physical_segment_id=" << header.physical_segment_id
	     << ", journal_tail=" << header.journal_tail
	     << ", segment_nonce=" << header.segment_nonce
	     << ", out-of-line=" << header.out_of_line
	     << ")";
}


std::ostream &operator<<(std::ostream &out, const extent_info_t &info)
{
  return out << "extent_info_t("
	     << " type: " << info.type
	     << " addr: " << info.addr
	     << " len: " << info.len
	     << ")";
}

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
      (segment_off_t)journal_segment_manager.get_block_size());
  }
  auto ret = replay_segments_t(segments.end() - from);
  std::transform(
    from, segments.end(), ret.begin(),
    [this](const auto &p) {
      auto ret = journal_seq_t{
	p.second.journal_segment_seq,
	paddr_t::make_seg_paddr(
	  p.first,
	  (segment_off_t)journal_segment_manager.get_block_size())};
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

std::optional<std::vector<delta_info_t>> Journal::try_decode_deltas(
  record_header_t header,
  const bufferlist &bl)
{
  auto bliter = bl.cbegin();
  bliter += ceph::encoded_sizeof_bounded<record_header_t>();
  bliter += sizeof(checksum_t) /* crc */;
  bliter += header.extents  * ceph::encoded_sizeof_bounded<extent_info_t>();
  logger().debug("Journal::try_decode_deltas: decoding {} deltas", header.deltas);
  std::vector<delta_info_t> deltas(header.deltas);
  for (auto &&i : deltas) {
    try {
      decode(i, bliter);
    } catch (ceph::buffer::error &e) {
      return std::nullopt;
    }
  }
  return deltas;
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
    ExtentReader::found_record_handler_t(
      [=, &handler](paddr_t base,
		    const record_header_t &header,
		    const bufferlist &mdbuf) {
	auto deltas = try_decode_deltas(
	  header,
	  mdbuf);
	if (!deltas) {
	  // This should be impossible, we did check the crc on the mdbuf
	  logger().error(
	    "Journal::replay_segment: unable to decode deltas for record {}",
	    base);
	  assert(deltas);
	}

	return seastar::do_with(
	  std::move(*deltas),
	  [=](auto &deltas) {
	    return crimson::do_for_each(
	      deltas,
	      [=](auto &delta) {
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
		     seq.segment_seq)) {
		  return replay_ertr::now();
		} else {
		  auto offsets = submit_result_t{
		    base.add_offset(header.mdlength),
		    write_result_t{
		      journal_seq_t{seq.segment_seq, base},
		      static_cast<segment_off_t>(header.mdlength + header.dlength)
		    }
		  };
		  return handler(
		    offsets,
		    delta);
		}
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
      );;
    });
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
  const record_size_t& rsize)
{
  logger().debug(
    "Journal::RecordBatch::add_pending: batches={}, write_size={}",
    records.size() + 1,
    get_encoded_length(rsize));
  assert(state != state_t::SUBMITTING);
  assert(can_batch(rsize));

  auto block_start_offset = encoded_length + rsize.mdlength;
  records.push_back(std::move(record));
  record_sizes.push_back(rsize);
  auto new_encoded_length = get_encoded_length(rsize);
  assert(new_encoded_length < MAX_SEG_OFF);
  encoded_length = new_encoded_length;
  if (state == state_t::EMPTY) {
    assert(!io_promise.has_value());
    io_promise = seastar::shared_promise<maybe_result_t>();
  } else {
    assert(io_promise.has_value());
  }
  state = state_t::PENDING;

  return io_promise->get_shared_future(
  ).then([block_start_offset
         ](auto maybe_write_result) -> add_pending_ret {
    if (!maybe_write_result.has_value()) {
      return crimson::ct_error::input_output_error::make();
    }
    auto submit_result = submit_result_t{
      maybe_write_result->start_seq.offset.add_offset(block_start_offset),
      *maybe_write_result
    };
    return add_pending_ret(
      add_pending_ertr::ready_future_marker{},
      submit_result);
  });
}

ceph::bufferlist Journal::RecordBatch::encode_records(
  size_t block_size,
  const journal_seq_t& committed_to,
  segment_nonce_t segment_nonce)
{
  logger().debug(
    "Journal::RecordBatch::encode_records: batches={}, committed_to={}",
    records.size(),
    committed_to);
  assert(state == state_t::PENDING);
  assert(records.size());
  assert(records.size() == record_sizes.size());
  assert(io_promise.has_value());

  state = state_t::SUBMITTING;
  ceph::bufferlist bl;
  std::size_t i = 0;
  do {
    auto record_bl = encode_record(
        record_sizes[i],
        std::move(records[i]),
        block_size,
        committed_to,
        segment_nonce);
    bl.claim_append(record_bl);
  } while ((++i) < records.size());
  assert(bl.length() == (std::size_t)encoded_length);
  return bl;
}

void Journal::RecordBatch::set_result(
  maybe_result_t maybe_write_result)
{
  if (maybe_write_result.has_value()) {
    logger().debug(
      "Journal::RecordBatch::set_result: batches={}, write_start {} + {}",
      records.size(),
      maybe_write_result->start_seq,
      maybe_write_result->length);
    assert(maybe_write_result->length == encoded_length);
  } else {
    logger().error(
      "Journal::RecordBatch::set_result: batches={}, write is failed!",
      records.size());
  }
  assert(state == state_t::SUBMITTING);
  assert(io_promise.has_value());

  state = state_t::EMPTY;
  encoded_length = 0;
  records.clear();
  record_sizes.clear();
  io_promise->set_value(maybe_write_result);
  io_promise.reset();
}

ceph::bufferlist Journal::RecordBatch::submit_pending_fast(
  record_t&& record,
  const record_size_t& rsize,
  size_t block_size,
  const journal_seq_t& committed_to,
  segment_nonce_t segment_nonce)
{
  logger().debug(
    "Journal::RecordBatch::submit_pending_fast: write_size={}",
    get_encoded_length(rsize));
  assert(state == state_t::EMPTY);
  assert(can_batch(rsize));

  auto bl = encode_record(
      rsize,
      std::move(record),
      block_size,
      committed_to,
      segment_nonce);
  assert(bl.length() == get_encoded_length(rsize));
  return bl;
}

Journal::RecordSubmitter::RecordSubmitter(
  std::size_t io_depth,
  std::size_t batch_capacity,
  std::size_t batch_flush_size,
  JournalSegmentManager& jsm)
  : io_depth_limit{io_depth},
    journal_segment_manager{jsm},
    batches(new RecordBatch[io_depth + 1])
{
  logger().info("Journal::RecordSubmitter: io_depth_limit={}, "
                "batch_capacity={}, batch_flush_size={}",
                io_depth, batch_capacity, batch_flush_size);
  assert(io_depth > 0);
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
  auto rsize = get_encoded_record_length(
    record, journal_segment_manager.get_block_size());
  auto total = rsize.mdlength + rsize.dlength;
  auto max_record_length = journal_segment_manager.get_max_write_length();
  if (total > max_record_length) {
    logger().error(
      "Journal::RecordSubmitter::submit: record size {} exceeds max {}",
      total,
      max_record_length
    );
    return crimson::ct_error::erange::make();
  }

  return do_submit(std::move(record), rsize, handle);
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
  ceph::bufferlist to_write = p_batch->encode_records(
    journal_segment_manager.get_block_size(),
    journal_segment_manager.get_committed_to(),
    journal_segment_manager.get_nonce());
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
  const record_size_t& rsize,
  OrderingHandle& handle,
  bool flush)
{
  assert(!p_current_batch->is_submitting());
  record_batch_stats.increment(
      p_current_batch->get_num_records() + 1);
  auto write_fut = [this, flush, record=std::move(record), &rsize]() mutable {
    if (flush && p_current_batch->is_empty()) {
      // fast path with direct write
      increment_io();
      ceph::bufferlist to_write = p_current_batch->submit_pending_fast(
        std::move(record),
        rsize,
        journal_segment_manager.get_block_size(),
        journal_segment_manager.get_committed_to(),
        journal_segment_manager.get_nonce());
      return journal_segment_manager.write(to_write
      ).safe_then([rsize](auto write_result) {
        return submit_result_t{
          write_result.start_seq.offset.add_offset(rsize.mdlength),
          write_result
        };
      }).finally([this] {
        decrement_io_with_flush();
      });
    } else {
      // indirect write with or without the existing pending records
      auto write_fut = p_current_batch->add_pending(
        std::move(record), rsize);
      if (flush) {
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
  const record_size_t& rsize,
  OrderingHandle& handle)
{
  assert(!p_current_batch->is_submitting());
  if (state <= state_t::PENDING) {
    // can increment io depth
    assert(!wait_submit_promise.has_value());
    auto batched_size = p_current_batch->can_batch(rsize);
    if (batched_size == 0 ||
        batched_size > journal_segment_manager.get_max_write_length()) {
      assert(p_current_batch->is_pending());
      flush_current_batch();
      return do_submit(std::move(record), rsize, handle);
    } else if (journal_segment_manager.needs_roll(batched_size)) {
      if (p_current_batch->is_pending()) {
        flush_current_batch();
      }
      return journal_segment_manager.roll(
      ).safe_then([this, record=std::move(record), rsize, &handle]() mutable {
        return do_submit(std::move(record), rsize, handle);
      });
    } else {
      return submit_pending(std::move(record), rsize, handle, true);
    }
  }

  assert(state == state_t::FULL);
  // cannot increment io depth
  auto batched_size = p_current_batch->can_batch(rsize);
  if (batched_size == 0 ||
      batched_size > journal_segment_manager.get_max_write_length() ||
      journal_segment_manager.needs_roll(batched_size)) {
    if (!wait_submit_promise.has_value()) {
      wait_submit_promise = seastar::promise<>();
    }
    return wait_submit_promise->get_future(
    ).then([this, record=std::move(record), rsize, &handle]() mutable {
      return do_submit(std::move(record), rsize, handle);
    });
  } else {
    return submit_pending(std::move(record), rsize, handle, false);
  }
}

}
