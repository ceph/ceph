// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "crimson/os/seastore/extent_placement_manager.h"

SET_SUBSYS(seastore_tm);

namespace crimson::os::seastore {

SegmentedAllocator::SegmentedAllocator(
  SegmentProvider& sp,
  SegmentManager& sm)
  : rewriter(sp, sm)
{
  std::generate_n(
    std::back_inserter(writers),
    crimson::common::get_conf<uint64_t>(
      "seastore_init_write_segments_num_per_device"),
    [&] {
      return Writer{sp, sm};
    }
  );
}

SegmentedAllocator::Writer::write_iertr::future<>
SegmentedAllocator::Writer::_write(
  Transaction& t,
  ool_record_t& record)
{
  LOG_PREFIX(SegmentedAllocator::Writer::_write);
  record.set_base(segment_allocator.get_written_to());
  auto record_size = record.get_encoded_record_length();
  bufferlist bl = record.encode(
      segment_allocator.get_segment_id(),
      segment_allocator.get_nonce());
  assert(bl.length() == record_size.get_encoded_length());

  DEBUGT(
    "written {} extents, {} bytes to segment {} at {}",
    t,
    record.get_num_extents(),
    bl.length(),
    segment_allocator.get_segment_id(),
    record.get_base());

  // account transactional ool writes before write()
  auto& stats = t.get_ool_write_stats();
  stats.extents.num += record.get_num_extents();
  stats.extents.bytes += record_size.dlength;
  stats.header_raw_bytes += record_size.get_raw_mdlength();
  stats.header_bytes += record_size.get_mdlength();
  stats.data_bytes += record_size.dlength;
  stats.num_records += 1;

  for (auto& ool_extent : record.get_extents()) {
    auto& lextent = ool_extent.get_lextent();
    auto paddr = ool_extent.get_ool_paddr();
    TRACET("ool extent written at {} -- {}", t, *lextent, paddr);
    lextent->hint = placement_hint_t::NUM_HINTS; // invalidate hint
    t.mark_delayed_extent_ool(lextent, paddr);
  }

  return trans_intr::make_interruptible(
    segment_allocator.write(bl).discard_result()
  );
}

SegmentedAllocator::Writer::write_iertr::future<>
SegmentedAllocator::Writer::do_write(
  Transaction& t,
  std::list<LogicalCachedExtentRef>& extents)
{
  LOG_PREFIX(SegmentedAllocator::Writer::do_write);
  assert(!extents.empty());
  if (roll_promise.has_value()) {
    return trans_intr::make_interruptible(
      roll_promise->get_shared_future()
    ).then_interruptible([this, &t, &extents] {
      return do_write(t, extents);
    });
  }
  assert(segment_allocator.can_write());

  ool_record_t record(
    segment_allocator.get_block_size(),
    (t.get_src() == Transaction::src_t::MUTATE)
      ? record_commit_type_t::MODIFY
      : record_commit_type_t::REWRITE);
  for (auto it = extents.begin(); it != extents.end();) {
    auto& extent = *it;
    auto wouldbe_length = record.get_wouldbe_encoded_record_length(extent);
    if (segment_allocator.needs_roll(wouldbe_length)) {
      // reached the segment end, write and roll
      assert(!roll_promise.has_value());
      roll_promise = seastar::shared_promise<>();
      auto num_extents = record.get_num_extents();
      DEBUGT("end of segment, writing {} extents", t, num_extents);
      return (num_extents ?
              _write(t, record) :
              write_iertr::now()
      ).si_then([this] {
        return segment_allocator.roll();
      }).finally([this] {
        roll_promise->set_value();
        roll_promise.reset();
      }).si_then([this, &t, &extents] {
        if (!extents.empty()) {
          return do_write(t, extents);
        }
        return write_iertr::now();
      });
    }
    DEBUGT("add extent to record -- {}", t, *extent);
    extent->prepare_write();
    record.add_extent(extent);
    it = extents.erase(it);
  }

  DEBUGT("writing {} extents", t, record.get_num_extents());
  return _write(t, record);
}

SegmentedAllocator::Writer::write_iertr::future<>
SegmentedAllocator::Writer::write(
  Transaction& t,
  std::list<LogicalCachedExtentRef>& extents)
{
  if (extents.empty()) {
    return write_iertr::now();
  }
  return seastar::with_gate(write_guard, [this, &t, &extents] {
    if (!roll_promise.has_value() &&
        !segment_allocator.can_write()) {
      roll_promise = seastar::shared_promise<>();
      return trans_intr::make_interruptible(
        segment_allocator.open().discard_result()
      ).finally([this] {
        roll_promise->set_value();
        roll_promise.reset();
      }).si_then([this, &t, &extents] {
        return do_write(t, extents);
      });
    }
    return do_write(t, extents);
  });
}

}
