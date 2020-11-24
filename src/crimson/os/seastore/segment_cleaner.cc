// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/common/log.h"

#include "crimson/os/seastore/transaction.h"
#include "crimson/os/seastore/segment_cleaner.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore {

bool SpaceTrackerSimple::equals(const SpaceTrackerI &_other) const
{
  const auto &other = static_cast<const SpaceTrackerSimple&>(_other);

  if (other.live_bytes_by_segment.size() != live_bytes_by_segment.size()) {
    logger().error("{}: different segment counts, bug in test");
    assert(0 == "segment counts should match");
    return false;
  }

  bool all_match = true;
  for (segment_id_t i = 0; i < live_bytes_by_segment.size(); ++i) {
    if (other.live_bytes_by_segment[i] != live_bytes_by_segment[i]) {
      all_match = false;
      logger().debug(
	"{}: segment_id {} live bytes mismatch *this: {}, other: {}",
	__func__,
	i,
	live_bytes_by_segment[i],
	other.live_bytes_by_segment[i]);
    }
  }
  return all_match;
}

int64_t SpaceTrackerDetailed::SegmentMap::allocate(
  segment_id_t segment,
  segment_off_t offset,
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
  return update_usage(block_size);
}

int64_t SpaceTrackerDetailed::SegmentMap::release(
  segment_id_t segment,
  segment_off_t offset,
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
  return update_usage(-(int64_t)block_size);
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
  for (segment_id_t i = 0; i < segment_usage.size(); ++i) {
    if (other.segment_usage[i].get_usage() != segment_usage[i].get_usage()) {
      all_match = false;
      logger().error(
	"{}: segment_id {} live bytes mismatch *this: {}, other: {}",
	__func__,
	i,
	segment_usage[i].get_usage(),
	other.segment_usage[i].get_usage());
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
  segment_usage[id].dump_usage(block_size);
}

SegmentCleaner::get_segment_ret SegmentCleaner::get_segment()
{
  for (size_t i = 0; i < segments.size(); ++i) {
    if (segments[i].is_empty()) {
      mark_open(i);
      logger().debug("{}: returning segment {}", __func__, i);
      return get_segment_ret(
	get_segment_ertr::ready_future_marker{},
	i);
    }
  }
  assert(0 == "out of space handling todo");
  return get_segment_ret(
    get_segment_ertr::ready_future_marker{},
    0);
}

void SegmentCleaner::update_journal_tail_target(journal_seq_t target)
{
  logger().debug(
    "{}: {}",
    __func__,
    target);
  assert(journal_tail_target == journal_seq_t() || target >= journal_tail_target);
  if (journal_tail_target == journal_seq_t() || target > journal_tail_target) {
    journal_tail_target = target;
  }
}

void SegmentCleaner::update_journal_tail_committed(journal_seq_t committed)
{
  if (journal_tail_committed == journal_seq_t() ||
      committed > journal_tail_committed) {
    logger().debug(
      "{}: update journal_tail_committed {}",
      __func__,
      committed);
    journal_tail_committed = committed;
  }
  if (journal_tail_target == journal_seq_t() ||
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
  mark_closed(segment);
}

SegmentCleaner::do_immediate_work_ret SegmentCleaner::do_immediate_work(
  Transaction &t)
{
  auto next_target = get_dirty_tail_limit();
  logger().debug(
    "{}: journal_tail_target={} get_dirty_tail_limit()={}",
    __func__,
    journal_tail_target,
    next_target);

  logger().debug(
    "SegmentCleaner::do_immediate_work gc total {}, available {}, unavailable {}, used {}  available_ratio {}, reclaim_ratio {}, bytes_to_gc_for_available {}, bytes_to_gc_for_reclaim {}",
    get_total_bytes(),
    get_available_bytes(),
    get_unavailable_bytes(),
    get_used_bytes(),
    get_available_ratio(),
    get_reclaim_ratio(),
    get_immediate_bytes_to_gc_for_available(),
    get_immediate_bytes_to_gc_for_reclaim());

  auto dirty_fut = do_immediate_work_ertr::now();
  if (journal_tail_target < next_target) {
    dirty_fut = rewrite_dirty(t, next_target);
  }
  return dirty_fut.safe_then([=, &t] {
    return do_gc(t, get_immediate_bytes_to_gc());
  }).handle_error(
    do_immediate_work_ertr::pass_further{},
    crimson::ct_error::assert_all{}
  );
}

SegmentCleaner::do_deferred_work_ret SegmentCleaner::do_deferred_work(
  Transaction &t)
{
  return do_deferred_work_ret(
    do_deferred_work_ertr::ready_future_marker{},
    ceph::timespan());
}

SegmentCleaner::rewrite_dirty_ret SegmentCleaner::rewrite_dirty(
  Transaction &t,
  journal_seq_t limit)
{
  return ecb->get_next_dirty_extents(
    limit
  ).then([=, &t](auto dirty_list) {
    if (dirty_list.empty()) {
      return do_immediate_work_ertr::now();
    } else {
      update_journal_tail_target(dirty_list.front()->get_dirty_from());
    }
    return seastar::do_with(
      std::move(dirty_list),
      [this, &t](auto &dirty_list) {
	return crimson::do_for_each(
	  dirty_list,
	  [this, &t](auto &e) {
	    logger().debug(
	      "SegmentCleaner::do_immediate_work cleaning {}",
	      *e);
	    return ecb->rewrite_extent(t, e);
	  });
      });
  });
}

SegmentCleaner::do_gc_ret SegmentCleaner::do_gc(
  Transaction &t,
  size_t bytes)
{
  if (bytes == 0) {
    return do_gc_ertr::now();
  }

  if (!scan_cursor) {
    paddr_t next = P_ADDR_NULL;
    next.segment = get_next_gc_target();
    if (next == P_ADDR_NULL) {
      logger().debug(
	"SegmentCleaner::do_gc: no segments to gc");
      return do_gc_ertr::now();
    }
    next.offset = 0;
    scan_cursor =
      std::make_unique<ExtentCallbackInterface::scan_extents_cursor>(
	next);
    logger().debug(
      "SegmentCleaner::do_gc: starting gc on segment {}",
      scan_cursor->get_offset().segment);
  }

  return ecb->scan_extents(
    *scan_cursor,
    bytes
  ).safe_then([=, &t](auto addrs) {
    return seastar::do_with(
      std::move(addrs),
      [=, &t](auto &addr_list) {
	return crimson::do_for_each(
	  addr_list,
	  [=, &t](auto &addr_pair) {
	    auto &[addr, info] = addr_pair;
	    logger().debug(
	      "SegmentCleaner::do_gc: checking addr {}",
	      addr);
	    return ecb->get_extent_if_live(
	      t,
	      info.type,
	      addr,
	      info.addr,
	      info.len
	    ).safe_then([addr=addr, &t, this](CachedExtentRef ext) {
	      if (!ext) {
		logger().debug(
		  "SegmentCleaner::do_gc: addr {} dead, skipping",
		  addr);
		return ExtentCallbackInterface::rewrite_extent_ertr::now();
	      } else {
		logger().debug(
		  "SegmentCleaner::do_gc: addr {} alive, gc'ing {}",
		  addr,
		  *ext);
	      }
	      return ecb->rewrite_extent(
		t,
		ext);
	    });
	  }).safe_then([&t, this] {
	    if (scan_cursor->is_complete()) {
	      t.mark_segment_to_release(scan_cursor->get_offset().segment);
	      scan_cursor.reset();
	    }
	    return ExtentCallbackInterface::release_segment_ertr::now();
	  });
      });
  });
}

}
