// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "crimson/os/seastore/extent_placement_manager.h"

#include "crimson/os/seastore/segment_cleaner.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore_tm);
  }
}

SET_SUBSYS(seastore_tm);

namespace crimson::os::seastore {

SegmentedAllocator::SegmentedAllocator(
  SegmentProvider& sp,
  SegmentManager& sm)
  : segment_provider(sp),
    segment_manager(sm)
{
  std::generate_n(
    std::back_inserter(writers),
    crimson::common::get_conf<uint64_t>(
      "seastore_init_rewrite_segments_num_per_device"),
    [&] {
      return Writer{
	segment_provider,
	segment_manager};
      });
}

SegmentedAllocator::Writer::write_iertr::future<>
SegmentedAllocator::Writer::_write(
  Transaction& t,
  ool_record_t& record)
{
  LOG_PREFIX(SegmentedAllocator::Writer::_write);
  record.set_base(allocated_to);
  auto record_size = record.get_encoded_record_length();
  allocated_to += record_size.get_encoded_length();
  segment_provider.update_segment_avail_bytes(
    paddr_t::make_seg_paddr(
      current_segment->get_segment_id(),
      allocated_to));
  bufferlist bl = record.encode(
      current_segment->get_segment_id(),
      0);

  DEBUGT(
    "written {} extents, {} bytes to segment {} at {}",
    t,
    record.get_num_extents(),
    bl.length(),
    current_segment->get_segment_id(),
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
    current_segment->write(record.get_base(), bl
    ).safe_then([FNAME, &t, base=record.get_base(), cs=current_segment] {
      DEBUGT("written {} {}", t, cs->get_segment_id(), base);
    })
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
    return roll_promise->get_shared_future(
    ).then([this, &t, &extents] {
      return do_write(t, extents);
    });
  }
  assert(current_segment);

  ool_record_t record(segment_manager.get_block_size());
  for (auto it = extents.begin(); it != extents.end();) {
    auto& extent = *it;
    auto wouldbe_length = record.get_wouldbe_encoded_record_length(extent);
    if (_needs_roll(wouldbe_length)) {
      // reached the segment end, write and roll
      assert(!roll_promise.has_value());
      roll_promise = seastar::shared_promise<>();
      auto num_extents = record.get_num_extents();
      DEBUGT(
        "end of segment, writing {} extents to segment {} at {}",
        t,
        num_extents,
        current_segment->get_segment_id(),
        allocated_to);
      return (num_extents ?
              _write(t, record) :
              write_iertr::now()
      ).si_then([this] {
        return roll_segment();
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

  DEBUGT(
    "writing {} extents to segment {} at {}",
    t,
    record.get_num_extents(),
    current_segment->get_segment_id(),
    allocated_to);
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
    if (!roll_promise.has_value() && !current_segment) {
      roll_promise = seastar::shared_promise<>();
      return trans_intr::make_interruptible(
        roll_segment().finally([this] {
          roll_promise->set_value();
          roll_promise.reset();
        })
      ).si_then([this, &t, &extents] {
        return do_write(t, extents);
      });
    }
    return do_write(t, extents);
  });
}

bool SegmentedAllocator::Writer::_needs_roll(seastore_off_t length) const {
  return allocated_to + length > current_segment->get_write_capacity();
}

SegmentedAllocator::Writer::init_segment_ertr::future<>
SegmentedAllocator::Writer::init_segment(Segment& segment) {
  bufferptr bp(
    ceph::buffer::create_page_aligned(
      segment_manager.get_block_size()));
  bp.zero();
  auto header =segment_header_t{
    OOL_SEG_SEQ,
    segment.get_segment_id(),
    NO_DELTAS, 0};
  logger().debug("SegmentedAllocator::Writer::init_segment: initting {}, {}",
    segment.get_segment_id(),
    header);
  ceph::bufferlist bl;
  encode(header, bl);
  bl.cbegin().copy(bl.length(), bp.c_str());
  bl.clear();
  bl.append(bp);
  allocated_to = segment_manager.get_block_size();
  return segment.write(0, bl).handle_error(
    crimson::ct_error::input_output_error::pass_further{},
    crimson::ct_error::assert_all{
      "Invalid error when initing segment"}
  );
}

SegmentedAllocator::Writer::roll_segment_ertr::future<>
SegmentedAllocator::Writer::roll_segment() {
  LOG_PREFIX(SegmentedAllocator::Writer::roll_segment);
  DEBUG("start");
  assert(roll_promise.has_value());
  return [this, FNAME] {
    if (current_segment) {
      auto seg_to_close = std::move(current_segment);
      if (write_guard.is_closed()) {
        DEBUG("write_guard is closed, should be stopping");
        return seg_to_close->close(
        ).safe_then([seg_to_close=std::move(seg_to_close)] {});
      } else {
        DEBUG("rolling OOL segment, close {} ...", seg_to_close->get_segment_id());
        (void) seastar::with_gate(write_guard,
          [this, seg_to_close=std::move(seg_to_close)]() mutable
        {
          return seg_to_close->close(
          ).safe_then([this, seg_to_close=std::move(seg_to_close)] {
            segment_provider.close_segment(seg_to_close->get_segment_id());
          });
        });
        return Segment::close_ertr::now();
      }
    } else {
      DEBUG("rolling OOL segment, no current ...");
      return Segment::close_ertr::now();
    }
  }().safe_then([this] {
    auto new_segment_id = segment_provider.get_segment(
        segment_manager.get_device_id(), OOL_SEG_SEQ);
    return segment_manager.open(new_segment_id);
  }).safe_then([this, FNAME](auto segref) {
    DEBUG("opened new segment: {}", segref->get_segment_id());
    return init_segment(*segref
    ).safe_then([segref=std::move(segref), this, FNAME] {
      assert(!current_segment);
      current_segment = segref;
      DEBUG("inited new segment: {}", segref->get_segment_id());
    });
  }).handle_error(
    roll_segment_ertr::pass_further{},
    crimson::ct_error::all_same_way([] { ceph_assert(0 == "TODO"); })
  );
}

}
