// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include "crimson/common/log.h"

#include "include/buffer.h"
#include "crimson/os/poseidonstore/device_manager/memory.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::poseidonstore::device_manager {

MemoryDeviceManager::MemoryDeviceManager(memory_config_t config)
  : config(config) {}

MemoryDeviceManager::~MemoryDeviceManager()
{
  if (buffer) {
    ::munmap(buffer, config.size);
  }
}

MemoryDeviceManager::write_ertr::future<> MemoryDeviceManager::write(
  paddr_t addr,
  ceph::bufferlist bl)
{
  logger().debug(
    "memory_write to at offset {}, len {}, crc {}",
    addr,
    bl.length(),
    bl.crc32c(1));
  if (bl.length() == 0) 
    return crimson::ct_error::invarg::make();

  bl.begin().copy(bl.length(), buffer + addr);

  return DeviceManager::write_ertr::now();
}

MemoryDeviceManager::init_ertr::future<> MemoryDeviceManager::init()
{
  logger().debug(
    "Initing memory device manager with config {}",
    config);

  if (config.block_size % (4<<10) != 0) {
    return crimson::ct_error::invarg::make();
  }
  if (config.block_size % config.page_size != 0) {
    return crimson::ct_error::invarg::make();
  }

  auto addr = ::mmap(
    nullptr,
    config.size,
    PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS,
    -1,
    0);

  if (addr == MAP_FAILED)
    return crimson::ct_error::enospc::make();

  buffer = (char*)addr;

  ::memset(buffer, 0, config.size);
  return init_ertr::now();
}

MemoryDeviceManager::read_ertr::future<> MemoryDeviceManager::read(
  paddr_t addr,
  size_t len,
  ceph::bufferptr &out)
{

  if (addr + len > config.size)
    return crimson::ct_error::invarg::make();

  out.copy_in(0, len, buffer + addr);

  bufferlist bl;
  bl.push_back(out);

  logger().debug(
    "memory_read at offset {}, length {}, crc {}",
    addr,
    len,
    bl.begin().crc32c(len, 1));

  return read_ertr::now();
}

}
