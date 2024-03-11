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
  SegmentProvider &segment_provider,
  JournalTrimmer &trimmer)
  : segment_seq_allocator(
      new SegmentSeqAllocator(segment_type_t::JOURNAL)),
    journal_segment_allocator(&trimmer,
                              data_category_t::METADATA,
                              INLINE_GENERATION,
                              segment_provider,
                              *segment_seq_allocator),
    record_submitter(crimson::common::get_conf<uint64_t>(
                       "seastore_journal_iodepth_limit"),
                     crimson::common::get_conf<uint64_t>(
                       "seastore_journal_batch_capacity"),
                     crimson::common::get_conf<Option::size_t>(
                       "seastore_journal_batch_flush_size"),
                     crimson::common::get_conf<double>(
                       "seastore_journal_batch_preferred_fullness"),
                     journal_segment_allocator),
    sm_group(*segment_provider.get_segment_manager_group()),
    trimmer{trimmer}
{
}

SegmentedJournal::open_for_mkfs_ret
SegmentedJournal::open_for_mkfs()
{
  return record_submitter.open(true);
}

SegmentedJournal::open_for_mount_ret
SegmentedJournal::open_for_mount()
{
  return record_submitter.open(false);
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
      return lt.second.segment_seq <
	rt.second.segment_seq;
    });

  segment_seq_allocator->set_next_segment_seq(
    segments.rbegin()->second.segment_seq + 1);
  std::for_each(
    segments.begin(),
    segments.end(),
    [FNAME](auto &seg)
  {
    if (seg.first != seg.second.physical_segment_id ||
        seg.second.get_type() != segment_type_t::JOURNAL) {
      ERROR("illegal journal segment for replay -- {}", seg.second);
      ceph_abort();
    }
  });

  auto last_segment_id = segments.rbegin()->first;
  auto last_header = segments.rbegin()->second;
  return scan_last_segment(last_segment_id, last_header
  ).safe_then([this, FNAME, segments=std::move(segments)] {
    INFO("dirty_tail={}, alloc_tail={}",
         trimmer.get_dirty_tail(),
         trimmer.get_alloc_tail());
    auto journal_tail = trimmer.get_journal_tail();
    auto journal_tail_paddr = journal_tail.offset;
    ceph_assert(journal_tail != JOURNAL_SEQ_NULL);
    ceph_assert(journal_tail_paddr != P_ADDR_NULL);
    auto from = std::find_if(
      segments.begin(),
      segments.end(),
      [&journal_tail_paddr](const auto &seg) -> bool {
        auto& seg_addr = journal_tail_paddr.as_seg_paddr();
        return seg.first == seg_addr.get_segment_id();
      });
    if (from->second.segment_seq != journal_tail.segment_seq) {
      ERROR("journal_tail {} does not match {}",
            journal_tail, from->second);
      ceph_abort();
    }

    auto num_segments = segments.end() - from;
    INFO("{} segments to replay", num_segments);
    auto ret = replay_segments_t(num_segments);
    std::transform(
      from, segments.end(), ret.begin(),
      [this](const auto &p) {
        auto ret = journal_seq_t{
          p.second.segment_seq,
          paddr_t::make_seg_paddr(
            p.first,
            sm_group.get_block_size())
        };
        return std::make_pair(ret, p.second);
      });
    ret[0].first.offset = journal_tail_paddr;
    return prep_replay_segments_fut(
      replay_ertr::ready_future_marker{},
      std::move(ret));
  });
}

SegmentedJournal::scan_last_segment_ertr::future<>
SegmentedJournal::scan_last_segment(
  const segment_id_t &segment_id,
  const segment_header_t &segment_header)
{
  LOG_PREFIX(SegmentedJournal::scan_last_segment);
  assert(segment_id == segment_header.physical_segment_id);
  trimmer.update_journal_tails(
      segment_header.dirty_tail, segment_header.alloc_tail);
  auto seq = journal_seq_t{
    segment_header.segment_seq,
    paddr_t::make_seg_paddr(segment_id, 0)
  };
  INFO("scanning journal tail deltas -- {}", segment_header);
  return seastar::do_with(
    scan_valid_records_cursor(seq),
    SegmentManagerGroup::found_record_handler_t(
      [FNAME, this](
        record_locator_t locator,
        const record_group_header_t& record_group_header,
        const bufferlist& mdbuf
      ) -> SegmentManagerGroup::scan_valid_records_ertr::future<>
    {
      DEBUG("decoding {} at {}", record_group_header, locator);
      bool has_tail_delta = false;
      auto maybe_headers = try_decode_record_headers(
          record_group_header, mdbuf);
      if (!maybe_headers) {
        // This should be impossible, we did check the crc on the mdbuf
        ERROR("unable to decode headers from {} at {}",
              record_group_header, locator);
        ceph_abort();
      }
      for (auto &record_header : *maybe_headers) {
        ceph_assert(is_valid_transaction(record_header.type));
        if (is_background_transaction(record_header.type)) {
          has_tail_delta = true;
        }
      }
      if (has_tail_delta) {
        bool found_delta = false;
        auto maybe_record_deltas_list = try_decode_deltas(
          record_group_header, mdbuf, locator.record_block_base);
        if (!maybe_record_deltas_list) {
          ERROR("unable to decode deltas from {} at {}",
                record_group_header, locator);
          ceph_abort();
        }
        for (auto &record_deltas : *maybe_record_deltas_list) {
          for (auto &[ctime, delta] : record_deltas.deltas) {
            if (delta.type == extent_types_t::JOURNAL_TAIL) {
              found_delta = true;
              journal_tail_delta_t tail_delta;
              decode(tail_delta, delta.bl);
              auto start_seq = locator.write_result.start_seq;
              DEBUG("got {}, at {}", tail_delta, start_seq);
              ceph_assert(tail_delta.dirty_tail != JOURNAL_SEQ_NULL);
              ceph_assert(tail_delta.alloc_tail != JOURNAL_SEQ_NULL);
              trimmer.update_journal_tails(
                  tail_delta.dirty_tail, tail_delta.alloc_tail);
            }
          }
        }
        ceph_assert(found_delta);
      }
      return seastar::now();
    }),
    [this, nonce=segment_header.segment_nonce](auto &cursor, auto &handler)
  {
    return sm_group.scan_valid_records(
      cursor,
      nonce,
      std::numeric_limits<std::size_t>::max(),
      handler).discard_result();
  });
}

SegmentedJournal::replay_ertr::future<>
SegmentedJournal::replay_segment(
  journal_seq_t seq,
  segment_header_t header,
  delta_handler_t &handler,
  replay_stats_t &stats)
{
  LOG_PREFIX(Journal::replay_segment);
  INFO("starting at {} -- {}", seq, header);
  return seastar::do_with(
    scan_valid_records_cursor(seq),
    SegmentManagerGroup::found_record_handler_t(
      [&handler, this, &stats](
      record_locator_t locator,
      const record_group_header_t& header,
      const bufferlist& mdbuf)
      -> SegmentManagerGroup::scan_valid_records_ertr::future<>
    {
      LOG_PREFIX(Journal::replay_segment);
      ++stats.num_record_groups;
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
         &handler,
         &stats](auto& record_deltas_list)
      {
        return crimson::do_for_each(
          record_deltas_list,
          [write_result,
           this,
           FNAME,
           &handler,
           &stats](record_deltas_t& record_deltas)
        {
          ++stats.num_records;
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
             &handler,
             &stats](auto &p)
          {
	    auto& modify_time = p.first;
	    auto& delta = p.second;
	    return handler(
	      locator,
	      delta,
	      trimmer.get_dirty_tail(),
	      trimmer.get_alloc_tail(),
              modify_time
            ).safe_then([&stats, delta_type=delta.type](auto ret) {
	      auto [is_applied, ext] = ret;
              if (is_applied) {
                // see Cache::replay_delta()
                assert(delta_type != extent_types_t::JOURNAL_TAIL);
                if (delta_type == extent_types_t::ALLOC_INFO) {
                  ++stats.num_alloc_deltas;
                } else {
                  ++stats.num_dirty_deltas;
                }
              }
            });
          });
        });
      });
    }),
    [=, this](auto &cursor, auto &dhandler) {
      return sm_group.scan_valid_records(
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

SegmentedJournal::replay_ret SegmentedJournal::replay(
  delta_handler_t &&delta_handler)
{
  LOG_PREFIX(Journal::replay);
  return sm_group.find_journal_segment_headers(
  ).safe_then([this, FNAME, delta_handler=std::move(delta_handler)]
    (auto &&segment_headers) mutable -> replay_ret {
    INFO("got {} segments", segment_headers.size());
    return seastar::do_with(
      std::move(delta_handler),
      replay_segments_t(),
      replay_stats_t(),
      [this, segment_headers=std::move(segment_headers), FNAME]
      (auto &handler, auto &segments, auto &stats) mutable -> replay_ret {
	return prep_replay_segments(std::move(segment_headers)
	).safe_then([this, &handler, &segments, &stats](auto replay_segs) mutable {
	  segments = std::move(replay_segs);
	  return crimson::do_for_each(segments,[this, &handler, &stats](auto i) mutable {
	    return replay_segment(i.first, i.second, handler, stats);
	  });
        }).safe_then([&stats, FNAME] {
          INFO("replay done, record_groups={}, records={}, "
               "alloc_deltas={}, dirty_deltas={}",
               stats.num_record_groups,
               stats.num_records,
               stats.num_alloc_deltas,
               stats.num_dirty_deltas);
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
