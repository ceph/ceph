// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include "crimson/common/log.h"

#include "include/buffer.h"
#include "nvmedevice.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore::nvme_device {

open_ertr::future<>
TestMemory::open(const std::string &in_path, seastar::open_flags mode)
{
  if (buf) {
    return open_ertr::now();
  }

  logger().debug(
    "Initializing test memory divice {}",
    size);

  void* addr = ::mmap(
    nullptr,
    size,
    PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS,
    -1,
    0);

  buf = (char*)addr;

  ::memset(buf, 0, size);
  return open_ertr::now();
}

write_ertr::future<>
TestMemory::write(
  uint64_t offset,
  bufferptr &bptr,
  uint16_t stream)
{
  ceph_assert(buf);
  logger().debug(
    "TestMemory: write offset {} len {}",
    offset,
    bptr.length());

  ::memcpy(buf + offset, bptr.c_str(), bptr.length());

  return write_ertr::now();
}

read_ertr::future<>
TestMemory::read(
  uint64_t offset,
  bufferptr &bptr)
{
  ceph_assert(buf);
  logger().debug(
    "TestMemory: read offset {} len {}",
    offset,
    bptr.length());

  bptr.copy_in(0, bptr.length(), buf + offset);
  return read_ertr::now();
}

seastar::future<>
TestMemory::close()
{
  logger().debug(" close ");
  return seastar::now();
}

}
