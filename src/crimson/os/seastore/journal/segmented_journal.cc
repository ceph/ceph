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
}

SegmentedJournal::open_for_write_ret SegmentedJournal::open_for_write()
{
  return record_submitter.open();
}

SegmentedJournal::close_ertr::future<> SegmentedJournal::close()
{
  LOG_PREFIX(Journal::close);
  INFO("closing, committed_to={}",
       record_submitter.get_committed_to());
  return record_submitter.close();
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

}
