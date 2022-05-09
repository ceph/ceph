// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <seastar/core/metrics.hh>

#include "crimson/common/log.h"
#include "crimson/os/seastore/logging.h"

#include "crimson/os/seastore/segment_cleaner.h"
#include "crimson/os/seastore/transaction_manager.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore_cleaner);
  }
}

SET_SUBSYS(seastore_cleaner);

namespace crimson::os::seastore {

void segment_info_t::set_open(
    segment_seq_t _seq, segment_type_t _type, std::size_t seg_size)
{
  ceph_assert(_seq != NULL_SEG_SEQ);
  ceph_assert(_type != segment_type_t::NULL_SEG);
  ceph_assert(seg_size > 0);
  state = Segment::segment_state_t::OPEN;
  seq = _seq;
  type = _type;
  open_avail_bytes = seg_size;
}

void segment_info_t::set_empty()
{
  state = Segment::segment_state_t::EMPTY;
  seq = NULL_SEG_SEQ;
  type = segment_type_t::NULL_SEG;
  last_modified = {};
  last_rewritten = {};
  open_avail_bytes = 0;
}

void segment_info_t::set_closed()
{
  state = Segment::segment_state_t::CLOSED;
  // the rest of information is unchanged
}

void segment_info_t::init_closed(
    segment_seq_t _seq, segment_type_t _type)
{
  ceph_assert(_seq != NULL_SEG_SEQ);
  ceph_assert(_type != segment_type_t::NULL_SEG);
  state = Segment::segment_state_t::CLOSED;
  seq = _seq;
  type = _type;
  open_avail_bytes = 0;
}

std::ostream& operator<<(std::ostream &out, const segment_info_t &info)
{
  out << "seg_info_t("
      << "state=" << info.state;
  if (info.is_empty()) {
    // pass
  } else { // open or closed
    out << ", seq=" << segment_seq_printer_t{info.seq}
        << ", type=" << info.type
        << ", last_modified=" << info.last_modified.time_since_epoch()
        << ", last_rewritten=" << info.last_rewritten.time_since_epoch()
        << ", open_avail_bytes=" << info.open_avail_bytes;
  }
  return out << ")";
}

void segments_info_t::reset()
{
  segments.clear();

  segment_size = 0;

  num_in_journal = 0;
  num_open = 0;
  num_empty = 0;
  num_closed = 0;

  count_open = 0;
  count_release = 0;
  count_close = 0;

  total_bytes = 0;
  avail_bytes = 0;
}

void segments_info_t::add_segment_manager(
    SegmentManager &segment_manager)
{
  LOG_PREFIX(segments_info_t::add_segment_manager);
  device_id_t d_id = segment_manager.get_device_id();
  auto ssize = segment_manager.get_segment_size();
  auto nsegments = segment_manager.get_num_segments();
  auto sm_size = segment_manager.get_size();
  INFO("adding segment manager {}, size={}, ssize={}, segments={}",
       device_id_printer_t{d_id}, sm_size, ssize, nsegments);
  ceph_assert(ssize > 0);
  ceph_assert(nsegments > 0);
  ceph_assert(sm_size > 0);

  // also validate if the device is duplicated
  segments.add_device(d_id, nsegments, segment_info_t{});

  // assume all the segment managers share the same settings as follows.
  if (segment_size == 0) {
    ceph_assert(ssize > 0);
    segment_size = ssize;
  } else {
    ceph_assert(segment_size == (std::size_t)ssize);
  }

  // NOTE: by default the segments are empty
  num_empty += nsegments;

  total_bytes += sm_size;
  avail_bytes += sm_size;
}

void segments_info_t::init_closed(
    segment_id_t segment, segment_seq_t seq, segment_type_t type)
{
  LOG_PREFIX(segments_info_t::init_closed);
  auto& segment_info = segments[segment];
  INFO("initiating {} {} {}, {}, num_segments(empty={}, opened={}, closed={})",
       segment, segment_seq_printer_t{seq}, type,
       segment_info, num_empty, num_open, num_closed);
  ceph_assert(segment_info.is_empty());
  segment_info.init_closed(seq, type);
  ceph_assert(num_empty > 0);
  --num_empty;
  ++num_closed;
  if (type == segment_type_t::JOURNAL) {
    ++num_in_journal;
  }
  ceph_assert(avail_bytes >= get_segment_size());
  avail_bytes -= get_segment_size();
  // do not increment count_close;
}

void segments_info_t::mark_open(
    segment_id_t segment, segment_seq_t seq, segment_type_t type)
{
  LOG_PREFIX(segments_info_t::mark_open);
  auto& segment_info = segments[segment];
  INFO("opening {} {} {}, {}, num_segments(empty={}, opened={}, closed={})",
       segment, segment_seq_printer_t{seq}, type,
       segment_info, num_empty, num_open, num_closed);
  ceph_assert(segment_info.is_empty());
  segment_info.set_open(seq, type, get_segment_size());
  ceph_assert(num_empty > 0);
  --num_empty;
  ++num_open;
  if (type == segment_type_t::JOURNAL) {
    ++num_in_journal;
  }
  ++count_open;
}

void segments_info_t::mark_empty(
    segment_id_t segment)
{
  LOG_PREFIX(segments_info_t::mark_empty);
  auto& segment_info = segments[segment];
  INFO("releasing {}, {}, num_segments(empty={}, opened={}, closed={})",
       segment, segment_info,
       num_empty, num_open, num_closed);
  ceph_assert(segment_info.is_closed());
  auto type = segment_info.type;
  assert(type != segment_type_t::NULL_SEG);
  segment_info.set_empty();
  ceph_assert(num_closed > 0);
  --num_closed;
  ++num_empty;
  if (type == segment_type_t::JOURNAL) {
    ceph_assert(num_in_journal > 0);
    --num_in_journal;
  }
  avail_bytes += get_segment_size();
  ++count_release;
}

void segments_info_t::mark_closed(
    segment_id_t segment)
{
  LOG_PREFIX(segments_info_t::mark_closed);
  auto& segment_info = segments[segment];
  INFO("closing {}, {}, num_segments(empty={}, opened={}, closed={})",
       segment, segment_info,
       num_empty, num_open, num_closed);
  ceph_assert(segment_info.is_open());
  segment_info.set_closed();
  ceph_assert(num_open > 0);
  --num_open;
  ++num_closed;
  ceph_assert(avail_bytes >= segment_info.open_avail_bytes);
  avail_bytes -= segment_info.open_avail_bytes;
  ++count_close;
}

void segments_info_t::update_written_to(
    paddr_t offset)
{
  LOG_PREFIX(segments_info_t::update_written_to);
  auto& saddr = offset.as_seg_paddr();
  auto& segment_info = segments[saddr.get_segment_id()];
  if (!segment_info.is_open()) {
    ERROR("segment is not open, not updating, offset={}, {}",
          offset, segment_info);
    // FIXME
    return;
  }

  auto new_avail = get_segment_size() - saddr.get_segment_off();
  if (segment_info.open_avail_bytes < new_avail) {
    ERROR("open_avail_bytes should not increase! offset={}, {}",
          offset, segment_info);
    ceph_abort();
  }

  DEBUG("offset={}, {}", offset, segment_info);
  auto avail_deduction = segment_info.open_avail_bytes - new_avail;
  ceph_assert(avail_bytes >= avail_deduction);
  avail_bytes -= avail_deduction;
  segment_info.open_avail_bytes = new_avail;
}

bool SpaceTrackerSimple::equals(const SpaceTrackerI &_other) const
{
  const auto &other = static_cast<const SpaceTrackerSimple&>(_other);

  if (other.live_bytes_by_segment.size() != live_bytes_by_segment.size()) {
    logger().error("{}: different segment counts, bug in test");
    assert(0 == "segment counts should match");
    return false;
  }

  bool all_match = true;
  for (auto i = live_bytes_by_segment.begin(), j = other.live_bytes_by_segment.begin();
       i != live_bytes_by_segment.end(); ++i, ++j) {
    if (i->second.live_bytes != j->second.live_bytes) {
      all_match = false;
      logger().debug(
	"{}: segment_id {} live bytes mismatch *this: {}, other: {}",
	__func__,
	i->first,
	i->second.live_bytes,
	j->second.live_bytes);
    }
  }
  return all_match;
}

int64_t SpaceTrackerDetailed::SegmentMap::allocate(
  device_segment_id_t segment,
  seastore_off_t offset,
  extent_len_t len,
  const extent_len_t block_size)
{
  assert(offset % block_size == 0);
  assert(len % block_size == 0);

  const auto b = (offset / block_size);
  const auto e = (offset + len) / block_size;

  bool error = false;
  for (auto i = b; i < e; ++i) {
    if (bitmap[i]) {
      if (!error) {
	logger().error(
	  "SegmentMap::allocate found allocated in {}, {} ~ {}",
	  segment,
	  offset,
	  len);
	error = true;
      }
      logger().debug(
	"SegmentMap::allocate block {} allocated",
	i * block_size);
    }
    bitmap[i] = true;
  }
  return update_usage(len);
}

int64_t SpaceTrackerDetailed::SegmentMap::release(
  device_segment_id_t segment,
  seastore_off_t offset,
  extent_len_t len,
  const extent_len_t block_size)
{
  assert(offset % block_size == 0);
  assert(len % block_size == 0);

  const auto b = (offset / block_size);
  const auto e = (offset + len) / block_size;

  bool error = false;
  for (auto i = b; i < e; ++i) {
    if (!bitmap[i]) {
      if (!error) {
	logger().error(
	  "SegmentMap::release found unallocated in {}, {} ~ {}",
	  segment,
	  offset,
	  len);
	error = true;
      }
      logger().debug(
	"SegmentMap::release block {} unallocated",
	i * block_size);
    }
    bitmap[i] = false;
  }
  return update_usage(-(int64_t)len);
}

bool SpaceTrackerDetailed::equals(const SpaceTrackerI &_other) const
{
  const auto &other = static_cast<const SpaceTrackerDetailed&>(_other);

  if (other.segment_usage.size() != segment_usage.size()) {
    logger().error("{}: different segment counts, bug in test");
    assert(0 == "segment counts should match");
    return false;
  }

  bool all_match = true;
  for (auto i = segment_usage.begin(), j = other.segment_usage.begin();
       i != segment_usage.end(); ++i, ++j) {
    if (i->second.get_usage() != j->second.get_usage()) {
      all_match = false;
      logger().error(
	"{}: segment_id {} live bytes mismatch *this: {}, other: {}",
	__func__,
	i->first,
	i->second.get_usage(),
	j->second.get_usage());
    }
  }
  return all_match;
}

void SpaceTrackerDetailed::SegmentMap::dump_usage(extent_len_t block_size) const
{
  for (unsigned i = 0; i < bitmap.size(); ++i) {
    if (bitmap[i]) {
      logger().debug("    {} still live", i * block_size);
    }
  }
}

void SpaceTrackerDetailed::dump_usage(segment_id_t id) const
{
  logger().debug("SpaceTrackerDetailed::dump_usage {}", id);
  segment_usage[id].dump_usage(
    block_size_by_segment_manager[id.device_id()]);
}

void SpaceTrackerSimple::dump_usage(segment_id_t id) const
{
  logger().info(
    "SpaceTrackerSimple::dump_usage id: {}, live_bytes: {}",
    id,
    live_bytes_by_segment[id].live_bytes);
}

SegmentCleaner::SegmentCleaner(
  config_t config,
  SegmentManagerGroupRef&& sm_group,
  bool detailed)
  : detailed(detailed),
    config(config),
    sm_group(std::move(sm_group)),
    ool_segment_seq_allocator(
      new SegmentSeqAllocator(segment_type_t::OOL)),
    gc_process(*this)
{}

void SegmentCleaner::register_metrics()
{
  namespace sm = seastar::metrics;
  stats.segment_util.buckets.resize(11);
  for (int i = 0; i < 11; i++) {
    stats.segment_util.buckets[i].upper_bound = ((double)(i + 1)) / 10;
    if (!i) {
      stats.segment_util.buckets[i].count = segments.get_num_segments();
    } else {
      stats.segment_util.buckets[i].count = 0;
    }
  }
  metrics.add_group("segment_cleaner", {
    sm::make_derive("segments_number",
		    [this] { return segments.get_num_segments(); },
		    sm::description("the number of segments")),
    sm::make_derive("segment_size",
		    [this] { return segments.get_segment_size(); },
		    sm::description("the bytes of a segment")),
    sm::make_derive("segments_in_journal",
		    [this] { return segments.get_num_in_journal(); },
		    sm::description("the number of segments in journal")),
    sm::make_derive("segments_open",
		    [this] { return segments.get_num_open(); },
		    sm::description("the number of open segments")),
    sm::make_derive("segments_empty",
		    [this] { return segments.get_num_empty(); },
		    sm::description("the number of empty segments")),
    sm::make_derive("segments_closed",
		    [this] { return segments.get_num_closed(); },
		    sm::description("the number of closed segments")),
    sm::make_derive("segments_count_open",
		    [this] { return segments.get_count_open(); },
		    sm::description("the count of open segment operations")),
    sm::make_derive("segments_count_release",
		    [this] { return segments.get_count_release(); },
		    sm::description("the count of release segment operations")),
    sm::make_derive("segments_count_close",
		    [this] { return segments.get_count_close(); },
		    sm::description("the count of close segment operations")),
    sm::make_derive("total_bytes",
		    [this] { return segments.get_total_bytes(); },
		    sm::description("the size of the space")),
    sm::make_derive("available_bytes",
		    [this] { return segments.get_available_bytes(); },
		    sm::description("the size of the space is available")),

    sm::make_counter("accumulated_blocked_ios", stats.accumulated_blocked_ios,
		     sm::description("accumulated total number of ios that were blocked by gc")),
    sm::make_counter("reclaim_rewrite_bytes", stats.reclaim_rewrite_bytes,
		     sm::description("rewritten bytes due to reclaim")),
    sm::make_counter("reclaiming_bytes", stats.reclaiming_bytes,
		     sm::description("bytes being reclaimed")),
    sm::make_derive("ios_blocking", stats.ios_blocking,
		    sm::description("IOs that are blocking on space usage")),
    sm::make_derive("used_bytes", stats.used_bytes,
		    sm::description("the size of the space occupied by live extents")),
    sm::make_derive("projected_used_bytes", stats.projected_used_bytes,
		    sm::description("the size of the space going to be occupied by new extents")),
    sm::make_histogram("segment_utilization_distribution",
		       [this]() -> seastar::metrics::histogram& {
		         return stats.segment_util;
		       },
		       sm::description("utilization distribution of all segments"))
  });
}

segment_id_t SegmentCleaner::allocate_segment(
    segment_seq_t seq,
    segment_type_t type)
{
  LOG_PREFIX(SegmentCleaner::allocate_segment);
  assert(seq != NULL_SEG_SEQ);
  for (auto it = segments.begin();
       it != segments.end();
       ++it) {
    auto seg_id = it->first;
    auto& segment_info = it->second;
    if (segment_info.is_empty()) {
      segments.mark_open(seg_id, seq, type);
      INFO("opened, should_block_on_gc {}, projected_avail_ratio {}, "
           "projected_reclaim_ratio {}",
           should_block_on_gc(),
           get_projected_available_ratio(),
           get_projected_reclaim_ratio());
      return seg_id;
    }
  }
  ERROR("out of space with segment_seq={}", segment_seq_printer_t{seq});
  ceph_abort();
  return NULL_SEG_ID;
}

void SegmentCleaner::update_journal_tail_target(journal_seq_t target)
{
  logger().debug(
    "{}: {}, current tail target {}",
    __func__,
    target,
    journal_tail_target);
  assert(journal_tail_target == JOURNAL_SEQ_NULL || target >= journal_tail_target);
  if (journal_tail_target == JOURNAL_SEQ_NULL || target > journal_tail_target) {
    journal_tail_target = target;
  }
  gc_process.maybe_wake_on_space_used();
  maybe_wake_gc_blocked_io();
}

void SegmentCleaner::update_journal_tail_committed(journal_seq_t committed)
{
  if (journal_tail_committed == JOURNAL_SEQ_NULL ||
      committed > journal_tail_committed) {
    logger().debug(
      "{}: update journal_tail_committed {}",
      __func__,
      committed);
    journal_tail_committed = committed;
  }
  if (journal_tail_target == JOURNAL_SEQ_NULL ||
      committed > journal_tail_target) {
    logger().debug(
      "{}: update journal_tail_target {}",
      __func__,
      committed);
    journal_tail_target = committed;
  }
}

void SegmentCleaner::close_segment(segment_id_t segment)
{
  LOG_PREFIX(SegmentCleaner::close_segment);
  ceph_assert(init_complete);
  segments.mark_closed(segment);
  INFO("closed, should_block_on_gc {}, projected_avail_ratio {}, "
       "projected_reclaim_ratio {}",
       should_block_on_gc(),
       get_projected_available_ratio(),
       get_projected_reclaim_ratio());
}

SegmentCleaner::rewrite_dirty_ret SegmentCleaner::rewrite_dirty(
  Transaction &t,
  journal_seq_t limit)
{
  LOG_PREFIX(SegmentCleaner::rewrite_dirty);
  return ecb->get_next_dirty_extents(
    t,
    limit,
    config.journal_rewrite_per_cycle
  ).si_then([=, &t](auto dirty_list) {
    DEBUGT("rewrite {} dirty extents", t, dirty_list.size());
    return seastar::do_with(
      std::move(dirty_list),
      [FNAME, this, &t](auto &dirty_list) {
	return trans_intr::do_for_each(
	  dirty_list,
	  [FNAME, this, &t](auto &e) {
	    DEBUGT("cleaning {}", t, *e);
	    return ecb->rewrite_extent(t, e);
	  });
      });
  });
}

SegmentCleaner::gc_cycle_ret SegmentCleaner::GCProcess::run()
{
  return seastar::do_until(
    [this] { return stopping; },
    [this] {
      return maybe_wait_should_run(
      ).then([this] {
	cleaner.log_gc_state("GCProcess::run");

	if (stopping) {
	  return seastar::now();
	} else {
	  return cleaner.do_gc_cycle();
	}
      });
    });
}

SegmentCleaner::gc_cycle_ret SegmentCleaner::do_gc_cycle()
{
  if (gc_should_trim_journal()) {
    return gc_trim_journal(
    ).handle_error(
      crimson::ct_error::assert_all{
	"GCProcess::run encountered invalid error in gc_trim_journal"
      }
    );
  } else if (gc_should_reclaim_space()) {
    return gc_reclaim_space(
    ).handle_error(
      crimson::ct_error::assert_all{
	"GCProcess::run encountered invalid error in gc_reclaim_space"
      }
    );
  } else {
    return seastar::now();
  }
}

SegmentCleaner::gc_trim_journal_ret SegmentCleaner::gc_trim_journal()
{
  return repeat_eagain([this] {
    return ecb->with_transaction_intr(
      Transaction::src_t::CLEANER_TRIM,
      "trim_journal",
      [this](auto& t)
    {
      return rewrite_dirty(t, get_dirty_tail()
      ).si_then([this, &t] {
        return ecb->submit_transaction_direct(t);
      });
    });
  });
}

SegmentCleaner::gc_reclaim_space_ret SegmentCleaner::gc_reclaim_space()
{
  if (!scan_cursor) {
    journal_seq_t next = get_next_gc_target();
    if (next == JOURNAL_SEQ_NULL) {
      logger().debug(
	"SegmentCleaner::do_gc: no segments to gc");
      return seastar::now();
    }
    scan_cursor =
      std::make_unique<SegmentManagerGroup::scan_extents_cursor>(
	next);
    logger().debug(
      "SegmentCleaner::do_gc: starting gc on segment {}",
      scan_cursor->seq);
  } else {
    ceph_assert(!scan_cursor->is_complete());
  }

  return sm_group->scan_extents(
    *scan_cursor,
    config.reclaim_bytes_stride
  ).safe_then([this](auto &&_extents) {
    return seastar::do_with(
        std::move(_extents),
	(size_t)0,
        [this](auto &extents, auto &reclaimed) {
      return repeat_eagain([this, &extents, &reclaimed]() mutable {
	reclaimed = 0;
        logger().debug(
          "SegmentCleaner::gc_reclaim_space: processing {} extents",
          extents.size());
        return ecb->with_transaction_intr(
          Transaction::src_t::CLEANER_RECLAIM,
          "reclaim_space",
          [this, &extents, &reclaimed](auto& t)
        {
          return trans_intr::do_for_each(
              extents,
              [this, &t, &reclaimed](auto &extent) {
	    auto &addr = extent.first;
	    auto commit_time = extent.second.first.commit_time;
	    auto commit_type = extent.second.first.commit_type;
	    auto &info = extent.second.second;
            logger().debug(
              "SegmentCleaner::gc_reclaim_space: checking extent {}",
              info);
            return ecb->get_extent_if_live(
              t,
              info.type,
              addr,
              info.addr,
              info.len
            ).si_then([&info, commit_type, commit_time, addr=addr, &t, this, &reclaimed]
	      (CachedExtentRef ext) {
              if (!ext) {
                logger().debug(
                  "SegmentCleaner::gc_reclaim_space: addr {} dead, skipping",
                  addr);
                return ExtentCallbackInterface::rewrite_extent_iertr::now();
              } else {
                logger().debug(
                  "SegmentCleaner::gc_reclaim_space: addr {} alive, gc'ing {}",
                  addr,
                  *ext);
		assert(commit_time);
		assert(info.last_modified);
		assert(commit_type == record_commit_type_t::MODIFY
		  || commit_type == record_commit_type_t::REWRITE);
		if (ext->get_last_modified() == time_point()) {
		  assert(ext->get_last_rewritten() == time_point());
		  ext->set_last_modified(duration(info.last_modified));
		}
		if (commit_type == record_commit_type_t::REWRITE
		    && ext->get_last_rewritten() == time_point()) {
		  ext->set_last_rewritten(duration(commit_time));
		}

		assert(
		  (commit_type == record_commit_type_t::MODIFY
		    && commit_time <=
		      ext->get_last_modified().time_since_epoch().count())
		  || (commit_type == record_commit_type_t::REWRITE
		      && commit_time ==
			ext->get_last_rewritten().time_since_epoch().count()));

		reclaimed += ext->get_length();
                return ecb->rewrite_extent(
                  t,
                  ext);
              }
            });
          }).si_then([this, &t] {
            if (scan_cursor->is_complete()) {
              t.mark_segment_to_release(scan_cursor->get_segment_id());
            }
            return ecb->submit_transaction_direct(t);
          });
        });
      }).safe_then([&reclaimed] {
	return seastar::make_ready_future<size_t>(reclaimed);
      });
    });
  }).safe_then([this](size_t reclaimed) {
    stats.reclaiming_bytes += reclaimed;
    if (scan_cursor->is_complete()) {
      stats.reclaim_rewrite_bytes += stats.reclaiming_bytes;
      stats.reclaiming_bytes = 0;
      scan_cursor.reset();
    }
  });
}

SegmentCleaner::mount_ret SegmentCleaner::mount()
{
  const auto& sms = sm_group->get_segment_managers();
  logger().info(
    "SegmentCleaner::mount: {} segment managers", sms.size());
  init_complete = false;
  stats = {};
  journal_tail_target = JOURNAL_SEQ_NULL;
  journal_tail_committed = JOURNAL_SEQ_NULL;
  journal_head = JOURNAL_SEQ_NULL;
  
  space_tracker.reset(
    detailed ?
    (SpaceTrackerI*)new SpaceTrackerDetailed(
      sms) :
    (SpaceTrackerI*)new SpaceTrackerSimple(
      sms));
  
  segments.reset();
  for (auto sm : sms) {
    segments.add_segment_manager(*sm);
  }
  metrics.clear();
  register_metrics();

  logger().info("SegmentCleaner::mount: {} segments", segments.get_num_segments());
  return seastar::do_with(
    std::vector<std::pair<segment_id_t, segment_header_t>>(),
    [this](auto& segment_set) {
    return crimson::do_for_each(
      segments.begin(),
      segments.end(),
      [this, &segment_set](auto& it) {
	auto segment_id = it.first;
	return sm_group->read_segment_header(
	  segment_id
	).safe_then([segment_id, this, &segment_set](auto header) {
	  logger().info(
	    "SegmentCleaner::mount: segment_id={} -- {}",
	    segment_id, header);
	  auto s_type = header.get_type();
	  if (s_type == segment_type_t::NULL_SEG) {
	    logger().error(
	      "SegmentCleaner::mount: got null segment, segment_id={} -- {}",
	      segment_id, header);
	    ceph_abort();
	  }
	  return sm_group->read_segment_tail(
	    segment_id
	  ).safe_then([this, segment_id, &segment_set, header](auto tail)
	    -> scan_extents_ertr::future<> {
	    if (tail.segment_nonce != header.segment_nonce) {
	      return scan_nonfull_segment(header, segment_set, segment_id);
	    }
	    time_point last_modified(duration(tail.last_modified));
	    time_point last_rewritten(duration(tail.last_rewritten));
	    segments.update_last_modified_rewritten(
                segment_id, last_modified, last_rewritten);
	    init_mark_segment_closed(
	      segment_id,
	      header.segment_seq,
	      header.type);
	    return seastar::now();
	  }).handle_error(
	    crimson::ct_error::enodata::handle(
	      [this, header, segment_id, &segment_set](auto) {
	      return scan_nonfull_segment(header, segment_set, segment_id);
	    }),
	    crimson::ct_error::pass_further_all{}
	  );
	}).handle_error(
	  crimson::ct_error::enoent::handle([](auto) {
	    return mount_ertr::now();
	  }),
	  crimson::ct_error::enodata::handle([](auto) {
	    return mount_ertr::now();
	  }),
	  crimson::ct_error::input_output_error::pass_further{},
	  crimson::ct_error::assert_all{"unexpected error"}
	);
      });
  });
}

SegmentCleaner::scan_extents_ret SegmentCleaner::scan_nonfull_segment(
  const segment_header_t& header,
  scan_extents_ret_bare& segment_set,
  segment_id_t segment_id)
{
  if (header.get_type() == segment_type_t::OOL) {
    logger().info(
      "SegmentCleaner::scan_nonfull_segment: out-of-line segment {}",
      segment_id);
    return seastar::do_with(
      scan_valid_records_cursor({
	segments[segment_id].seq,
	paddr_t::make_seg_paddr(segment_id, 0)}),
      [this, segment_id, header](auto& cursor) {
      return seastar::do_with(
	SegmentManagerGroup::found_record_handler_t([this, segment_id](
	    record_locator_t locator,
	    const record_group_header_t& header,
	    const bufferlist& mdbuf
	  ) mutable -> SegmentManagerGroup::scan_valid_records_ertr::future<> {
	  LOG_PREFIX(SegmentCleaner::scan_nonfull_segment);
	  DEBUG("decodeing {} records", header.records);
	  auto maybe_headers = try_decode_record_headers(header, mdbuf);
	  if (!maybe_headers) {
	    ERROR("unable to decode record headers for record group {}",
	      locator.record_block_base);
	    return crimson::ct_error::input_output_error::make();
	  }

	  for (auto& header : *maybe_headers) {
	    mod_time_point_t ctime = header.commit_time;
	    auto commit_type = header.commit_type;
	    if (!ctime) {
	      ERROR("SegmentCleaner::scan_nonfull_segment: extent {} 0 commit_time",
		ctime);
	      ceph_abort("0 commit_time");
	    }
	    time_point commit_time{duration(ctime)};
	    assert(commit_type == record_commit_type_t::MODIFY
	      || commit_type == record_commit_type_t::REWRITE);
	    if (commit_type == record_commit_type_t::MODIFY) {
              segments.update_last_modified_rewritten(segment_id, commit_time, {});
	    }
	    if (commit_type == record_commit_type_t::REWRITE) {
              segments.update_last_modified_rewritten(segment_id, {}, commit_time);
	    }
	  }
	  return seastar::now();
	}),
	[&cursor, header, this](auto& handler) {
	  return sm_group->scan_valid_records(
	    cursor,
	    header.segment_nonce,
	    segments.get_segment_size(),
	    handler);
	}
      );
    }).safe_then([this, segment_id, header](auto) {
      init_mark_segment_closed(
	segment_id,
	header.segment_seq,
	header.type);
      return seastar::now();
    });
  } else if (header.get_type() == segment_type_t::JOURNAL) {
    logger().info(
      "SegmentCleaner::scan_nonfull_segment: journal segment {}",
      segment_id);
    segment_set.emplace_back(std::make_pair(segment_id, std::move(header)));
  } else {
    ceph_abort("unexpected segment type");
  }
  init_mark_segment_closed(
    segment_id,
    header.segment_seq,
    header.type);
  return seastar::now();
}

SegmentCleaner::release_ertr::future<>
SegmentCleaner::maybe_release_segment(Transaction &t)
{
  auto to_release = t.get_segment_to_release();
  if (to_release != NULL_SEG_ID) {
    LOG_PREFIX(SegmentCleaner::maybe_release_segment);
    INFOT("releasing segment {}", t, to_release);
    return sm_group->release_segment(to_release
    ).safe_then([this, FNAME, &t, to_release] {
      segments.mark_empty(to_release);
      INFOT("released, should_block_on_gc {}, projected_avail_ratio {}, "
           "projected_reclaim_ratio {}",
           t,
           should_block_on_gc(),
           get_projected_available_ratio(),
           get_projected_reclaim_ratio());
      if (space_tracker->get_usage(to_release) != 0) {
        space_tracker->dump_usage(to_release);
        ceph_abort();
      }
      maybe_wake_gc_blocked_io();
    });
  } else {
    return SegmentManager::release_ertr::now();
  }
}

}
