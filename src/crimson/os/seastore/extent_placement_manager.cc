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
  record_t&& record,
  std::list<LogicalCachedExtentRef>&& extents)
{
  LOG_PREFIX(SegmentedAllocator::Writer::_write);
  assert(extents.size());
  assert(extents.size() == record.extents.size());
  assert(!record.deltas.size());
  auto record_group = record_group_t(
      std::move(record), segment_allocator.get_block_size());
  auto record_size = record_group.size;
  ceph::bufferlist bl = encode_records(
      record_group,
      JOURNAL_SEQ_NULL,
      segment_allocator.get_nonce()); // 0
  assert(bl.length() == record_size.get_encoded_length());

  DEBUGT("writing {} bytes to segment {}",
         t, bl.length(), segment_allocator.get_segment_id());

  // account transactional ool writes before write()
  auto& stats = t.get_ool_write_stats();
  stats.extents.num += extents.size();
  stats.extents.bytes += record_size.dlength;
  stats.header_raw_bytes += record_size.get_raw_mdlength();
  stats.header_bytes += record_size.get_mdlength();
  stats.data_bytes += record_size.dlength;
  stats.num_records += 1;

  return trans_intr::make_interruptible(
    segment_allocator.write(bl)
  ).si_then([FNAME, record_size, &t,
             extents=std::move(extents)](write_result_t wr) mutable {
    assert(wr.start_seq.segment_seq == OOL_SEG_SEQ);
    paddr_t extent_addr = wr.start_seq.offset;
    extent_addr = extent_addr.as_seg_paddr().add_offset(
        record_size.get_mdlength());
    for (auto& extent : extents) {
      TRACET("ool extent written at {} -- {}", t, *extent, extent_addr);
      extent->hint = placement_hint_t::NUM_HINTS; // invalidate hint
      t.mark_delayed_extent_ool(extent, extent_addr);
      extent_addr = extent_addr.as_seg_paddr().add_offset(
          extent->get_length());
    }
    assert(extent_addr == wr.get_end_seq().offset);
  });
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

  record_t record;
  std::list<LogicalCachedExtentRef> pending_extents;

  auto commit_time = seastar::lowres_system_clock::now();
  record_commit_type_t commit_type;
  if (t.get_src() == Transaction::src_t::MUTATE) {
    commit_type = record_commit_type_t::MODIFY;
  } else {
    assert(t.get_src() == Transaction::src_t::CLEANER_TRIM ||
           t.get_src() == Transaction::src_t::CLEANER_RECLAIM);
    commit_type = record_commit_type_t::REWRITE;
  }
  record.commit_time = commit_time.time_since_epoch().count();
  record.commit_type = commit_type;

  for (auto it = extents.begin(); it != extents.end();) {
    auto& extent = *it;
    record_size_t wouldbe_rsize = record.size;
    wouldbe_rsize.account_extent(extent->get_bptr().length());
    auto wouldbe_length = record_group_size_t(
      wouldbe_rsize, segment_allocator.get_block_size()
    ).get_encoded_length();
    if (segment_allocator.needs_roll(wouldbe_length)) {
      // reached the segment end, write and roll
      assert(!roll_promise.has_value());
      roll_promise = seastar::shared_promise<>();
      auto num_extents = pending_extents.size();
      DEBUGT("end of segment, writing {} extents", t, num_extents);
      return (num_extents ?
              _write(t, std::move(record), std::move(pending_extents)) :
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
    if (commit_type == record_commit_type_t::MODIFY) {
      extent->set_last_modified(commit_time);
    } else {
      assert(commit_type == record_commit_type_t::REWRITE);
      extent->set_last_rewritten(commit_time);
    }
    ceph::bufferlist bl;
    extent->prepare_write();
    bl.append(extent->get_bptr());
    assert(bl.length() == extent->get_length());
    record.push_back(extent_t{
      extent->get_type(),
      extent->get_laddr(),
      std::move(bl),
      extent->get_last_modified().time_since_epoch().count()});
    pending_extents.push_back(extent);
    it = extents.erase(it);
  }

  DEBUGT("writing {} extents", t, pending_extents.size());
  return _write(t, std::move(record), std::move(pending_extents));
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
