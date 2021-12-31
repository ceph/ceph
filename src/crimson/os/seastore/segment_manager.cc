// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/segment_manager/block.h"
#include "crimson/common/log.h"


#ifdef HAVE_ZNS
#include "crimson/os/seastore/segment_manager/zns.h"
#endif

namespace{
  seastar::logger &logger(){
    return crimson::get_logger(ceph_subsys_seastore_device);
  }
}

namespace crimson::os::seastore {

seastar::future<crimson::os::seastore::SegmentManagerRef>
SegmentManager::get_segment_manager(
  const std::string &device)
{
#ifdef HAVE_ZNS
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
	logger().error("NR_ZONES: {}", nr_zones);
	if (nr_zones != 0) {
	  return std::make_unique<
	    segment_manager::zns::ZNSSegmentManager
	    >(device + "/block");
	} else {
	  return std::make_unique<
	    segment_manager::block::BlockSegmentManager
	    >(device + "/block");
	}
      });
    });
#else
  return seastar::make_ready_future<crimson::os::seastore::SegmentManagerRef>(
    std::make_unique<
      segment_manager::block::BlockSegmentManager
    >(device + "/block"));
#endif
}

}
