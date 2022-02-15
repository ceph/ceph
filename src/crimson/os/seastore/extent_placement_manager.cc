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

  return trans_intr::make_interruptible(
    current_segment->write(record.get_base(), bl
    ).safe_then([FNAME, &t, &record, cs=current_segment] {
      DEBUGT("written {} {}",
             t, cs->get_segment_id(), record.get_base());
    })
  ).si_then([FNAME, &record, &t] {
    for (auto& ool_extent : record.get_extents()) {
      auto& lextent = ool_extent.get_lextent();
      auto paddr = ool_extent.get_ool_paddr();
      TRACET("ool extent written at {} -- {}", t, *lextent, paddr);
      lextent->hint = placement_hint_t::NUM_HINTS; // invalidate hint
      t.mark_delayed_extent_ool(lextent, paddr);
    }
    record.clear();
  });
}

void SegmentedAllocator::Writer::add_extent_to_write(
  ool_record_t& record,
  LogicalCachedExtentRef& extent) {
  logger().debug(
    "SegmentedAllocator::Writer::add_extent_to_write: "
    "add extent {} to record",
    extent);
  extent->prepare_write();
  record.add_extent(extent);
}

SegmentedAllocator::Writer::write_iertr::future<>
SegmentedAllocator::Writer::write(
  Transaction& t,
  std::list<LogicalCachedExtentRef>& extents)
{
  auto write_func = [this, &extents, &t] {
    return seastar::do_with(ool_record_t(segment_manager.get_block_size()),
      [this, &extents, &t](auto& record) {
      return trans_intr::repeat([this, &record, &t, &extents]()
        -> write_iertr::future<seastar::stop_iteration> {
        if (extents.empty()) {
          return seastar::make_ready_future<
            seastar::stop_iteration>(seastar::stop_iteration::yes);
        }

        return segment_rotation_guard.wait(
          [this] {
            return !rolling_segment;
          },
          [this, &record, &extents, &t]() -> write_iertr::future<> {
            LOG_PREFIX(SegmentedAllocator::Writer::write);
            record.set_base(allocated_to);
            for (auto it = extents.begin();
                 it != extents.end();) {
              auto& extent = *it;
              auto wouldbe_length =
                record.get_wouldbe_encoded_record_length(extent);
              if (_needs_roll(wouldbe_length)) {
                // reached the segment end, write and roll
                assert(!rolling_segment);
                rolling_segment = true;
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
                ).si_then([this]() mutable {
                  return roll_segment(false);
                }).finally([this] {
                  rolling_segment = false;
                  segment_rotation_guard.broadcast();
                });
              }
              add_extent_to_write(record, extent);
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
        ).si_then([]()
          -> write_iertr::future<seastar::stop_iteration> {
          return seastar::make_ready_future<
            seastar::stop_iteration>(seastar::stop_iteration::no);
        });
      });
    });
  };

  return seastar::with_gate(write_guard,
    [this, write_func=std::move(write_func)]() mutable
  {
    if (rolling_segment) {
      return segment_rotation_guard.wait([this] {
          return !rolling_segment;
        }, std::move(write_func));

    } else if (!current_segment) {
      return trans_intr::make_interruptible(roll_segment(true)).si_then(
        [write_func=std::move(write_func)] {
        return write_func();
      });
    }
    return write_func();
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
SegmentedAllocator::Writer::roll_segment(bool set_rolling) {
  LOG_PREFIX(SegmentedAllocator::Writer::roll_segment);
  DEBUG("set_rolling {}", set_rolling);
  if (set_rolling) {
    rolling_segment = true;
  }
  assert(rolling_segment);
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
      rolling_segment = false;
      segment_rotation_guard.broadcast();
      DEBUG("inited new segment: {}", segref->get_segment_id());
    });
  }).handle_error(
    roll_segment_ertr::pass_further{},
    crimson::ct_error::all_same_way([] { ceph_assert(0 == "TODO"); })
  );
}

}
