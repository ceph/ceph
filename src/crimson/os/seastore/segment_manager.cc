// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/segment_manager/block.h"
#include "crimson/os/seastore/logging.h"

#ifdef HAVE_ZNS
#include "crimson/os/seastore/segment_manager/zns.h"
SET_SUBSYS(seastore_device);
#endif


namespace crimson::os::seastore {

std::ostream& operator<<(std::ostream& out, const block_sm_superblock_t& sb)
{
  out << "superblock("
      << "size=" << sb.size
      << ", segment_size=" << sb.segment_size
      << ", block_size=" << sb.block_size
      << ", segments=" << sb.segments
      << ", tracker_offset=" << sb.tracker_offset
      << ", first_segment_offset=" << sb.first_segment_offset
      << ", config=" << sb.config
      << ")";
  return out;
}

std::ostream& operator<<(std::ostream &out, Segment::segment_state_t s)
{
  using state_t = Segment::segment_state_t;
  switch (s) {
  case state_t::EMPTY:
    return out << "EMPTY";
  case state_t::OPEN:
    return out << "OPEN";
  case state_t::CLOSED:
    return out << "CLOSED";
  default:
    return out << "INVALID_SEGMENT_STATE!";
  }
}

seastar::future<crimson::os::seastore::SegmentManagerRef>
SegmentManager::get_segment_manager(
  const std::string &device, device_type_t dtype)
{
#ifdef HAVE_ZNS
LOG_PREFIX(SegmentManager::get_segment_manager);
  return seastar::do_with(
    static_cast<size_t>(0),
    [&](auto &nr_zones) {
      return seastar::open_file_dma(
	device + "/block",
	seastar::open_flags::rw
      ).then([&](auto file) {
	return seastar::do_with(
	  file,
	  [=, &nr_zones](auto &f) -> seastar::future<int> {
	    ceph_assert(f);
	    return f.ioctl(BLKGETNRZONES, (void *)&nr_zones);
	  });
      }).then([&](auto ret) -> crimson::os::seastore::SegmentManagerRef {
	crimson::os::seastore::SegmentManagerRef sm;
	INFO("Found {} zones.", nr_zones);
	if (nr_zones != 0) {
	  return std::make_unique<
	    segment_manager::zns::ZNSSegmentManager
	    >(device + "/block");
	} else {
	  return std::make_unique<
	    segment_manager::block::BlockSegmentManager
	    >(device + "/block", dtype);
	}
      });
    });
#else
  return seastar::make_ready_future<crimson::os::seastore::SegmentManagerRef>(
    std::make_unique<
      segment_manager::block::BlockSegmentManager
    >(device + "/block", dtype));
#endif
}

}
