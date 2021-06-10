// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#include "crimson/os/seastore/extent_placement_manager.h"
#include "crimson/os/seastore/lba_manager/btree/lba_btree_node_impl.h"
#include "crimson/os/seastore/omap_manager/btree/omap_btree_node_impl.h"
#include "crimson/os/seastore/onode_manager/staged-fltree/node_extent_manager/seastore.h"
#include "crimson/os/seastore/collection_manager/collection_flat_node.h"
#include "crimson/os/seastore/object_data_handler.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore);
  }
}

namespace crimson::os::seastore {

void SegmentedRewriter::add_extent_to_write(
  bufferlist& bl,
  extents_to_write_t& extents_to_write,
  LogicalCachedExtentRef& extent) {
  extent->prepare_write();
  // all io_wait_promise set here will be completed when the corresponding
  // transaction "complete_commit"s, we can't do complete_io() here, otherwise,
  // there might be consistency problem.
  extent->set_io_wait();
  extent->set_rewriting_paddr(
    {current_segment->get_segment_id(),
    allocated_to + bl.length()});
  bl.append(extent->get_bptr());
  extents_to_write.emplace_back(extent);
}

SegmentedRewriter::write_iertr::future<>
SegmentedRewriter::write(std::list<LogicalCachedExtentRef>& extents) {
  logger().debug("{}", __func__);
  auto fut = roll_segment_ertr::now();
  if (!current_segment) {
    fut = roll_segment();
  }
  return fut.safe_then([this, &extents] {
    return seastar::do_with(bufferlist(), std::vector<LogicalCachedExtentRef>(),
      [this, &extents](auto& bl, auto& extents_to_write) {
      return crimson::do_for_each(
        extents,
        [this, &bl, &extents_to_write](auto& extent)
        -> Segment::write_ertr::future<> {
        if (_needs_roll(bl.length() + extent->get_bptr().length())) {
          return current_segment->write(allocated_to, bl).safe_then(
            [this, &bl, &extent, &extents_to_write] {
            bl.clear();
            extents_to_write.clear();
            return roll_segment().safe_then([this, &bl, &extent, &extents_to_write] {
              add_extent_to_write(bl, extents_to_write, extent);
              extent->extent_writer = nullptr;
              return seastar::now();
            });
          });
        }
        add_extent_to_write(bl, extents_to_write, extent);
        extent->extent_writer = nullptr;
        return seastar::now();
      }).safe_then([this, &bl, &extents_to_write]()
        -> write_iertr::future<> {
        if (!bl.length()) {
          assert(extents_to_write.empty());
          return seastar::now();
        }
        assert(!extents_to_write.empty());
        return current_segment->write(allocated_to, bl).safe_then(
          [this, len=bl.length(), &extents_to_write] {
          allocated_to += len;
          return seastar::now();
        });
      });
    });
  });
}

bool SegmentedRewriter::_needs_roll(segment_off_t length) const {
  return allocated_to + length > current_segment->get_write_capacity();
}

SegmentedRewriter::init_segment_ertr::future<>
SegmentedRewriter::init_segment(Segment& segment) {
  logger().debug("SegmentedRewriter::init_segment: initting {}", segment.get_segment_id());
  bufferptr bp(
    ceph::buffer::create_page_aligned(
      segment_manager.get_block_size()));
  bp.zero();
  auto header =
    crimson::os::seastore::extent_placement_manager::segment_header_t{
      REWRITE_SEGMENT_HEADER_MAGIC};
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

SegmentedRewriter::roll_segment_ertr::future<>
SegmentedRewriter::roll_segment() {
  return segment_provider.get_segment().safe_then([this](auto segment) {
    return segment_manager.open(segment);
  }).safe_then([this](auto segref) {
    return init_segment(*segref).safe_then([segref=std::move(segref), this] {
      current_segment = segref;
      open_segments.emplace_back(segref);
    });
  }).handle_error(
    roll_segment_ertr::pass_further{},
    crimson::ct_error::all_same_way([] { ceph_assert(0 == "TODO"); })
  );
}

SegmentedAllocator::scan_device_ertr::future<>
SegmentedAllocator::scan_device() {
  logger().debug("{}", __func__);
  return crimson::do_for_each(
    boost::make_counting_iterator(segment_id_t{0}),
    boost::make_counting_iterator(segment_manager.get_num_segments()),
    [this](auto segment_id) {
      logger().debug("to test segment {}", segment_id);
      return segment_manager.read(
        paddr_t{segment_id, 0},
        segment_manager.get_block_size()
      ).safe_then([this, segment_id](bufferptr ptr) {
        extent_placement_manager::segment_header_t header;
        ceph::bufferlist bl;
        bl.push_back(ptr);
        auto bp = bl.cbegin();
        try {
          decode(header, bp);
        } catch (ceph::buffer::error &e) {
          logger().debug("segment {}, not a rewrite segment", segment_id);
          return scan_device_ertr::now();
        }
        if (header.magic == REWRITE_SEGMENT_HEADER_MAGIC) {
          logger().debug("got one valid segment: {}", segment_id);
          segment_provider.init_mark_segment_closed(segment_id);
        }
        logger().debug("segment {} scanned, header magic size: {}",
                       segment_id,
                       header.magic.length());
        return scan_device_ertr::now();
      }).handle_error(
        crimson::ct_error::enoent::handle([](auto) {
          return scan_device_ertr::now();
        }),
        crimson::ct_error::input_output_error::pass_further{},
        crimson::ct_error::assert_all{"Invalid error when scanning segments"}
      );
  });
}

CachedExtentRef SegmentedAllocator::alloc_ool_extent(
  extent_types_t type,
  const CachedExtentRef& old_extent,
  segment_off_t length)
{
  switch (type) {
  case extent_types_t::ROOT:
    assert(0 == "ROOT is never directly alloc'd");
    return CachedExtentRef();
  case extent_types_t::ONODE_BLOCK_STAGED:
    return alloc_ool_extent<onode::SeastoreNodeExtent>(old_extent, length);
  case extent_types_t::OMAP_INNER:
    return alloc_ool_extent<omap_manager::OMapInnerNode>(old_extent, length);
  case extent_types_t::OMAP_LEAF:
    return alloc_ool_extent<omap_manager::OMapLeafNode>(old_extent, length);
  case extent_types_t::COLL_BLOCK:
    return alloc_ool_extent<collection_manager::CollectionNode>(old_extent, length);
  case extent_types_t::OBJECT_DATA_BLOCK:
    return alloc_ool_extent<ObjectDataBlock>(old_extent, length);
  case extent_types_t::RETIRED_PLACEHOLDER:
    ceph_assert(0 == "impossible");
    return CachedExtentRef();
  case extent_types_t::TEST_BLOCK:
    return alloc_ool_extent<TestBlock>(old_extent, length);
  case extent_types_t::NONE: {
    ceph_assert(0 == "NONE is an invalid extent type");
    return CachedExtentRef();
  }
  default:
    ceph_assert(0 == "impossible");
    return CachedExtentRef();
  }
}

}
