// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/common/log.h"

#include "crimson/os/seastore/segment_cleaner.h"
#include "crimson/os/seastore/transaction_manager.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore);
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
  return update_usage(len);
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

SegmentCleaner::SegmentCleaner(config_t config, bool detailed)
  : detailed(detailed),
    config(config),
    gc_process(*this)
{
  register_metrics();
}

void SegmentCleaner::register_metrics()
{
  namespace sm = seastar::metrics;
  metrics.add_group("segment_cleaner", {
    sm::make_counter("segments_released", stats.segments_released,
		     sm::description("total number of extents released by SegmentCleaner")),
  });
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
  gc_process.maybe_wake_on_space_used();
  maybe_wake_gc_blocked_io();
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

SegmentCleaner::rewrite_dirty_ret SegmentCleaner::rewrite_dirty(
  Transaction &t,
  journal_seq_t limit)
{
  return trans_intr::make_interruptible(
    ecb->get_next_dirty_extents(
      limit,
      config.journal_rewrite_per_cycle)
  ).then_interruptible([=, &t](auto dirty_list) {
    return seastar::do_with(
      std::move(dirty_list),
      [this, &t](auto &dirty_list) {
	return trans_intr::do_for_each(
	  dirty_list,
	  [this, &t](auto &e) {
	    logger().debug(
	      "SegmentCleaner::rewrite_dirty cleaning {}",
	      *e);
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
  return repeat_eagain(
    [this] {
      return seastar::do_with(
	ecb->create_transaction(),
	[this](auto &tref) {
	  return with_trans_intr(*tref, [this](auto &t) {
	    return rewrite_dirty(t, get_dirty_tail()
	    ).si_then([this, &t] {
	      return ecb->submit_transaction_direct(
		t);
	    });
	  });
	});
    });
}

SegmentCleaner::gc_reclaim_space_ret SegmentCleaner::gc_reclaim_space()
{
  if (!scan_cursor) {
    paddr_t next = P_ADDR_NULL;
    next.segment = get_next_gc_target();
    if (next == P_ADDR_NULL) {
      logger().debug(
	"SegmentCleaner::do_gc: no segments to gc");
      return seastar::now();
    }
    next.offset = 0;
    scan_cursor =
      std::make_unique<ExtentCallbackInterface::scan_extents_cursor>(
	next);
    logger().debug(
      "SegmentCleaner::do_gc: starting gc on segment {}",
      scan_cursor->get_offset().segment);
  } else {
    ceph_assert(!scan_cursor->is_complete());
  }

  return ecb->scan_extents(
    *scan_cursor,
    config.reclaim_bytes_stride
  ).safe_then([this](auto &&_extents) {
    return seastar::do_with(
      std::move(_extents),
      [this](auto &extents) {
	return repeat_eagain([this, &extents]() mutable {
	  logger().debug(
	    "SegmentCleaner::gc_reclaim_space: processing {} extents",
	    extents.size());
	  return seastar::do_with(
	    ecb->create_transaction(),
	    [this, &extents](auto &tref) mutable {
	      return with_trans_intr(*tref, [this, &extents](auto &t) {
		return trans_intr::do_for_each(
		  extents,
		  [this, &t](auto &extent) {
		    auto &[addr, info] = extent;
		    logger().debug(
		      "SegmentCleaner::gc_reclaim_space: checking extent {}",
		      info);
		    return ecb->get_extent_if_live(
		      t,
		      info.type,
		      addr,
		      info.addr,
		      info.len
		    ).si_then([addr=addr, &t, this](CachedExtentRef ext) {
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
			return ecb->rewrite_extent(
			  t,
			  ext);
		      }
		    });
		  }
		).si_then([this, &t] {
		  if (scan_cursor->is_complete()) {
		    t.mark_segment_to_release(scan_cursor->get_offset().segment);
		  }
		  return ecb->submit_transaction_direct(t);
		});
	      });
	    });
	});
      });
  }).safe_then([this] {
    if (scan_cursor->is_complete()) {
      scan_cursor.reset();
    }
  });
}

}
