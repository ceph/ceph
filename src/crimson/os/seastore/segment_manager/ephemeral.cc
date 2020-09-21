// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include "crimson/common/log.h"

#include "include/buffer.h"
#include "crimson/os/seastore/segment_manager/ephemeral.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore::segment_manager {

EphemeralSegment::EphemeralSegment(
  EphemeralSegmentManager &manager, segment_id_t id)
  : manager(manager), id(id) {}

segment_off_t EphemeralSegment::get_write_capacity() const
{
  return manager.get_segment_size();
}

Segment::close_ertr::future<> EphemeralSegment::close()
{
  manager.segment_close(id);
  return close_ertr::now();
}

Segment::write_ertr::future<> EphemeralSegment::write(
  segment_off_t offset, ceph::bufferlist bl)
{
  if (offset < write_pointer || offset % manager.config.block_size != 0)
    return crimson::ct_error::invarg::make();

  if (offset + bl.length() >= manager.config.segment_size)
    return crimson::ct_error::enospc::make();

  return manager.segment_write({id, offset}, bl);
}

EphemeralSegmentManager::EphemeralSegmentManager(ephemeral_config_t config)
  : config(config) {}

Segment::close_ertr::future<> EphemeralSegmentManager::segment_close(segment_id_t id)
{
  if (segment_state[id] != segment_state_t::OPEN)
    return crimson::ct_error::invarg::make();

  segment_state[id] = segment_state_t::CLOSED;
  return Segment::close_ertr::now();
}

Segment::write_ertr::future<> EphemeralSegmentManager::segment_write(
  paddr_t addr,
  ceph::bufferlist bl,
  bool ignore_check)
{
  logger().debug(
    "segment_write to segment {} at offset {}, physical offset {}, len {}, crc {}",
    addr.segment,
    addr.offset,
    get_offset(addr),
    bl.length(),
    bl.crc32c(0));
  if (!ignore_check && segment_state[addr.segment] != segment_state_t::OPEN)
    return crimson::ct_error::invarg::make();

  bl.begin().copy(bl.length(), buffer + get_offset(addr));
  return Segment::write_ertr::now();
}

EphemeralSegmentManager::init_ertr::future<> EphemeralSegmentManager::init()
{
  logger().debug(
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

  auto addr = ::mmap(
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
  return init_ertr::now();
}

EphemeralSegmentManager::~EphemeralSegmentManager()
{
  if (buffer) {
    ::munmap(buffer, config.size);
  }
}

SegmentManager::open_ertr::future<SegmentRef> EphemeralSegmentManager::open(
  segment_id_t id)
{
  if (id >= get_num_segments())
    return crimson::ct_error::invarg::make();

  if (segment_state[id] != segment_state_t::EMPTY)
    return crimson::ct_error::invarg::make();

  segment_state[id] = segment_state_t::OPEN;
  return open_ertr::make_ready_future<SegmentRef>(new EphemeralSegment(*this, id));
}

SegmentManager::release_ertr::future<> EphemeralSegmentManager::release(
  segment_id_t id)
{
  if (id >= get_num_segments())
    return crimson::ct_error::invarg::make();

  if (segment_state[id] != segment_state_t::CLOSED)
    return crimson::ct_error::invarg::make();

  ::memset(buffer + get_offset({id, 0}), 0, config.segment_size);
  segment_state[id] = segment_state_t::EMPTY;
  return release_ertr::now();
}

SegmentManager::read_ertr::future<> EphemeralSegmentManager::read(
  paddr_t addr,
  size_t len,
  ceph::bufferptr &out)
{
  if (addr.segment >= get_num_segments())
    return crimson::ct_error::invarg::make();

  if (addr.offset + len >= config.segment_size)
    return crimson::ct_error::invarg::make();

  out.copy_in(0, len, buffer + get_offset(addr));

  bufferlist bl;
  bl.push_back(out);
  logger().debug(
    "segment_read to segment {} at offset {}, physical offset {}, length {}, crc {}",
    addr.segment,
    addr.offset,
    get_offset(addr),
    len,
    bl.begin().crc32c(config.block_size, 0));

  return read_ertr::now();
}

}
