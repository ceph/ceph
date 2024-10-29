// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include "seastar/core/sleep.hh"

#include "crimson/common/log.h"

#include "include/buffer.h"
#include "crimson/os/seastore/segment_manager/ephemeral.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore_device);
  }
}

namespace crimson::os::seastore::segment_manager {

std::ostream &operator<<(std::ostream &lhs, const ephemeral_config_t &c) {
  return lhs << "ephemeral_config_t(size=" << c.size << ", block_size=" << c.block_size
	     << ", segment_size=" << c.segment_size << ")";
}

EphemeralSegmentManagerRef create_test_ephemeral() {
  return EphemeralSegmentManagerRef(
    new EphemeralSegmentManager(DEFAULT_TEST_EPHEMERAL));
}

device_config_t get_ephemeral_device_config(
    std::size_t index,
    std::size_t num_main_devices,
    std::size_t num_cold_devices)
{
  auto num_devices = num_main_devices + num_cold_devices;
  assert(num_devices > index);
  auto get_sec_dtype = [num_main_devices](std::size_t idx) {
    if (idx < num_main_devices) {
      return device_type_t::EPHEMERAL_MAIN;
    } else {
      return device_type_t::EPHEMERAL_COLD;
    }
  };

  magic_t magic = 0xabcd;
  bool is_major_device;
  secondary_device_set_t secondary_devices;
  if (index == 0) {
    is_major_device = true;
    for (std::size_t secondary_index = index + 1;
         secondary_index < num_devices;
         ++secondary_index) {
      device_id_t secondary_id = static_cast<device_id_t>(secondary_index);
      secondary_devices.insert({
        secondary_index,
	device_spec_t{
	  magic,
	  get_sec_dtype(secondary_index),
	  secondary_id
	}
      });
    }
  } else { // index > 0
    is_major_device = false;
  }

  device_id_t id = static_cast<device_id_t>(index);
  seastore_meta_t meta = {};
  return {is_major_device,
          device_spec_t{
            magic,
	    get_sec_dtype(index),
            id
          },
          meta,
          secondary_devices};
}

EphemeralSegment::EphemeralSegment(
  EphemeralSegmentManager &manager, segment_id_t id)
  : manager(manager), id(id) {}

segment_off_t EphemeralSegment::get_write_capacity() const
{
  return manager.get_segment_size();
}

Segment::close_ertr::future<> EphemeralSegment::close()
{
  return manager.segment_close(id).safe_then([] {
    return seastar::sleep(std::chrono::milliseconds(1));
  });
}

Segment::write_ertr::future<> EphemeralSegment::write(
  segment_off_t offset, ceph::bufferlist bl)
{
  if (offset < write_pointer || offset % manager.config.block_size != 0)
    return crimson::ct_error::invarg::make();

  if (offset + bl.length() > (size_t)manager.get_segment_size())
    return crimson::ct_error::enospc::make();

  return manager.segment_write(paddr_t::make_seg_paddr(id, offset), bl);
}

Segment::write_ertr::future<> EphemeralSegment::advance_wp(
  segment_off_t offset)
{
  return write_ertr::now();
}

Segment::close_ertr::future<> EphemeralSegmentManager::segment_close(segment_id_t id)
{
  auto s_id = id.device_segment_id();
  if (segment_state[s_id] != segment_state_t::OPEN)
    return crimson::ct_error::invarg::make();

  segment_state[s_id] = segment_state_t::CLOSED;
  return Segment::close_ertr::now().safe_then([] {
    return seastar::sleep(std::chrono::milliseconds(1));
  });
}

EphemeralSegmentManager::mkfs_ret
EphemeralSegmentManager::mkfs(device_config_t _config)
{
  logger().info(
    "Mkfs ephemeral segment manager with {}",
    _config);
  device_config = _config;
  return mkfs_ertr::now();
}

Segment::write_ertr::future<> EphemeralSegmentManager::segment_write(
  paddr_t addr,
  ceph::bufferlist bl,
  bool ignore_check)
{
  auto& seg_addr = addr.as_seg_paddr();
  logger().debug(
    "segment_write to segment {} at offset {}, physical offset {}, len {}, crc {}",
    seg_addr.get_segment_id(),
    seg_addr.get_segment_off(),
    get_offset(addr),
    bl.length(),
    bl.crc32c(1));
  if (!ignore_check && segment_state[seg_addr.get_segment_id().device_segment_id()] 
      != segment_state_t::OPEN)
    return crimson::ct_error::invarg::make();

  bl.begin().copy(bl.length(), buffer + get_offset(addr));
  return Segment::write_ertr::now().safe_then([] {
    return seastar::sleep(std::chrono::milliseconds(1));
  });
}

EphemeralSegmentManager::init_ertr::future<> EphemeralSegmentManager::init()
{
  logger().info(
    "Initing ephemeral segment manager with config {}",
    config);

  if (config.block_size % (4<<10) != 0) {
    return crimson::ct_error::invarg::make();
  }
  if (config.segment_size % config.block_size != 0) {
    return crimson::ct_error::invarg::make();
  }
  if (config.size % config.segment_size != 0) {
    return crimson::ct_error::invarg::make();
  }

  void* addr = ::mmap(
    nullptr,
    config.size,
    PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS,
    -1,
    0);

  segment_state.resize(config.size / config.segment_size, segment_state_t::EMPTY);

  if (addr == MAP_FAILED)
    return crimson::ct_error::enospc::make();

  buffer = (char*)addr;

  ::memset(buffer, 0, config.size);
  return init_ertr::now().safe_then([] {
    return seastar::sleep(std::chrono::milliseconds(1));
  });
}

EphemeralSegmentManager::~EphemeralSegmentManager()
{
  if (buffer) {
    ::munmap(buffer, config.size);
  }
}

void EphemeralSegmentManager::remount()
{
  for (auto &i : segment_state) {
    if (i == Segment::segment_state_t::OPEN)
      i = Segment::segment_state_t::CLOSED;
  }
}

SegmentManager::open_ertr::future<SegmentRef> EphemeralSegmentManager::open(
  segment_id_t id)
{
  auto s_id = id.device_segment_id();
  if (s_id >= get_num_segments()) {
    logger().error("EphemeralSegmentManager::open: invalid segment {}", id);
    return crimson::ct_error::invarg::make();
  }

  if (segment_state[s_id] != segment_state_t::EMPTY) {
    logger().error("EphemeralSegmentManager::open: segment {} not empty", id);
    return crimson::ct_error::invarg::make();
  }

  segment_state[s_id] = segment_state_t::OPEN;
  return open_ertr::make_ready_future<SegmentRef>(new EphemeralSegment(*this, id));
}

SegmentManager::release_ertr::future<> EphemeralSegmentManager::release(
  segment_id_t id)
{
  auto s_id = id.device_segment_id();
  logger().debug("EphemeralSegmentManager::release: {}", id);

  if (s_id >= get_num_segments()) {
    logger().error(
      "EphemeralSegmentManager::release: invalid segment {}",
      id);
    return crimson::ct_error::invarg::make();
  }

  if (segment_state[s_id] != segment_state_t::CLOSED) {
    logger().error(
      "EphemeralSegmentManager::release: segment id {} not closed",
      id);
    return crimson::ct_error::invarg::make();
  }

  ::memset(buffer + get_offset(paddr_t::make_seg_paddr(id, 0)), 0, config.segment_size);
  segment_state[s_id] = segment_state_t::EMPTY;
  return release_ertr::now().safe_then([] {
    return seastar::sleep(std::chrono::milliseconds(1));
  });
}

SegmentManager::read_ertr::future<> EphemeralSegmentManager::read(
  paddr_t addr,
  size_t len,
  ceph::bufferptr &out)
{
  auto& seg_addr = addr.as_seg_paddr();
  if (seg_addr.get_segment_id().device_segment_id() >= get_num_segments()) {
    logger().error(
      "EphemeralSegmentManager::read: invalid segment {}",
      addr);
    return crimson::ct_error::invarg::make();
  }

  if (seg_addr.get_segment_off() + len > config.segment_size) {
    logger().error(
      "EphemeralSegmentManager::read: invalid offset {}~{}!",
      addr,
      len);
    return crimson::ct_error::invarg::make();
  }

  out.copy_in(0, len, buffer + get_offset(addr));

  bufferlist bl;
  bl.push_back(out);
  logger().debug(
    "segment_read to segment {} at offset {}, physical offset {}, length {}, crc {}",
    seg_addr.get_segment_id().device_segment_id(),
    seg_addr.get_segment_off(),
    get_offset(addr),
    len,
    bl.begin().crc32c(len, 1));

  return read_ertr::now().safe_then([] {
    return seastar::sleep(std::chrono::milliseconds(1));
  });
}

}
