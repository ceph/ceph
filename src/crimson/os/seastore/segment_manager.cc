// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/segment_manager/block.h"
#include "crimson/os/seastore/logging.h"

#ifdef HAVE_ZNS
#include "crimson/os/seastore/segment_manager/zbd.h"
#endif


namespace crimson::os::seastore {

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
    const std::string& device,
    device_type_t dtype)
{
  const std::string device_block = device + "/block";
#ifdef HAVE_ZNS
  if (dtype == device_type_t::ZBD) {
    co_return std::make_unique<segment_manager::zbd::ZBDSegmentManager>(
        device_block);
  }
#endif
  co_return std::make_unique<segment_manager::block::BlockSegmentManager>(
      device_block, dtype);
}
} // namespace crimson::os::seastore
