// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "crimson/os/seastore/journal.h"
#include "crimson/os/seastore/extent_placement_manager.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore_tm);
  }
}

SET_SUBSYS(seastore_tm);

namespace crimson::os::seastore {

SegmentedAllocator::SegmentedAllocator(
  SegmentProvider& sp,
  SegmentManager& sm,
  LBAManager& lba_manager,
  Journal& journal,
  Cache& cache)
  : segment_provider(sp),
    segment_manager(sm),
    lba_manager(lba_manager),
    journal(journal),
    cache(cache)
{
  std::generate_n(
    std::back_inserter(writers),
    crimson::common::get_conf<uint64_t>(
      "seastore_init_rewrite_segments_num_per_device"),
    [&] {
      return Writer{
	segment_provider,
	segment_manager,
	lba_manager,
	journal,
        cache};
      });
}

SegmentedAllocator::Writer::finish_record_ret
SegmentedAllocator::Writer::finish_write(
  Transaction& t,
  ool_record_t& record) {
  return trans_intr::do_for_each(record.get_extents(),
    [this, &t](auto& ool_extent) {
    LOG_PREFIX(SegmentedAllocator::Writer::finish_write);
    auto& lextent = ool_extent.get_lextent();
    DEBUGT("extent: {}, ool_paddr: {}",
      t,
      *lextent,
      ool_extent.get_ool_paddr());
    return lba_manager.update_mapping(
      t,
      lextent->get_laddr(),
      lextent->get_paddr(),
      ool_extent.get_ool_paddr()
    ).si_then([&ool_extent, &t, &lextent, this] {
      lextent->backend_type = device_type_t::NONE;
      lextent->hint = {};
      cache.mark_delayed_extent_ool(t, lextent, ool_extent.get_ool_paddr());
      return finish_record_iertr::now();
    });
  }).si_then([&record] {
    record.clear();
  });
}

SegmentedAllocator::Writer::write_iertr::future<>
SegmentedAllocator::Writer::_write(
  Transaction& t,
  ool_record_t& record)
{
  auto record_size = record.get_encoded_record_length();
  allocated_to += record_size.get_encoded_length();
  segment_provider.update_segment_avail_bytes(
    paddr_t::make_seg_paddr(
      current_segment->segment->get_segment_id(),
      allocated_to));
  bufferlist bl = record.encode(
      current_segment->segment->get_segment_id(),
      0);
  seastar::promise<> pr;
  current_segment->inflight_writes.emplace_back(pr.get_future());
  LOG_PREFIX(SegmentedAllocator::Writer::_write);

  DEBUGT(
    "written {} extents, {} bytes to segment {} at {}",
    t,
    record.get_num_extents(),
    bl.length(),
    current_segment->segment->get_segment_id(),
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
    current_segment->segment->write(record.get_base(), bl).safe_then(
      [this, pr=std::move(pr), &t,
      it=(--current_segment->inflight_writes.end()),
      cs=current_segment]() mutable {
        LOG_PREFIX(SegmentedAllocator::Writer::_write);
        if (cs->outdated) {
          DEBUGT("segment rolled", t);
          pr.set_value();
        } else{
          DEBUGT("segment not rolled", t);
          current_segment->inflight_writes.erase(it);
        }
        return seastar::now();
    })
  ).si_then([this, &record, &t]() mutable {
    return finish_write(t, record);
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
                  current_segment->segment->get_segment_id(),
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
              current_segment->segment->get_segment_id(),
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
}

bool SegmentedAllocator::Writer::_needs_roll(segment_off_t length) const {
  return allocated_to + length > current_segment->segment->get_write_capacity();
}

SegmentedAllocator::Writer::init_segment_ertr::future<>
SegmentedAllocator::Writer::init_segment(Segment& segment) {
  bufferptr bp(
    ceph::buffer::create_page_aligned(
      segment_manager.get_block_size()));
  bp.zero();
  auto header =segment_header_t{
    journal.get_segment_seq(),
    segment.get_segment_id(),
    NO_DELTAS, 0, true};
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
  if (current_segment) {
    (void) seastar::with_gate(writer_guard, [this] {
      auto fut = seastar::now();
      if (!current_segment->inflight_writes.empty()) {
        fut = seastar::when_all_succeed(
          current_segment->inflight_writes.begin(),
          current_segment->inflight_writes.end());
      }
      current_segment->outdated = true;
      return fut.then(
        [cs=std::move(current_segment), this, it=(--open_segments.end())] {
        return cs->segment->close().safe_then([this, cs, it] {
          LOG_PREFIX(SegmentedAllocator::Writer::roll_segment);
          assert((*it).get() == cs.get());
          segment_provider.close_segment(cs->segment->get_segment_id());
          open_segments.erase(it);
          DEBUG("closed segment: {}", cs->segment->get_segment_id());
        });
      });
    }).handle_exception_type([](seastar::gate_closed_exception e) {
      LOG_PREFIX(SegmentedAllocator::Writer::roll_segment);
      DEBUG(" writer_guard closed, should be stopping");
      return seastar::now();
    });
  }

  return segment_provider.get_segment(
    segment_manager.get_device_id()
  ).safe_then([this](auto segment) {
    return segment_manager.open(segment);
  }).safe_then([this](auto segref) {
    LOG_PREFIX(SegmentedAllocator::Writer::roll_segment);
    DEBUG("opened new segment: {}", segref->get_segment_id());
    return init_segment(*segref).safe_then([segref=std::move(segref), this] {
      LOG_PREFIX(SegmentedAllocator::Writer::roll_segment);
      assert(!current_segment.get());
      current_segment.reset(new open_segment_wrapper_t());
      current_segment->segment = segref;
      open_segments.emplace_back(current_segment);
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
