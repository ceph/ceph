// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/common/errorator-utils.h"
#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/random_block_manager/hdd_device.h"

SET_SUBSYS(seastore_device);

namespace crimson::os::seastore::random_block_device {

seastar::future<> RotationalDevice::start(uint32_t shard_nums) {
  device_shard_nums = shard_nums;
  auto num_shard_services = (device_shard_nums + seastar::smp::count - 1 ) /
    seastar::smp::count;
  LOG_PREFIX(NVMeBlockDevice::start);
  DEBUG("device_shard_nums={} seastar::smp={}, num_shard_services={}",
    device_shard_nums, seastar::smp::count, num_shard_services);
  return shard_devices.start(num_shard_services, device_path);
}

RotationalDevice::mkfs_ret RotationalDevice::mkfs(device_config_t config) {
  LOG_PREFIX(RotationalDevice::mkfs);
  INFO("{}", config);
  return shard_devices.local().mshard_devices[0]->do_primary_mkfs(
    config, seastar::smp::count, 0);
}

RotationalDevice::mount_ret RotationalDevice::mount() {
  LOG_PREFIX(RotationalDevice::mount);
  DEBUG("mount");
  return shard_devices.invoke_on_all([](auto &local_device) {
    return seastar::do_for_each(
      local_device.mshard_devices,
      [](auto &mshard_device) {
      return mshard_device->do_shard_mount(
      ).handle_error(
	crimson::ct_error::assert_all(
	  "Invalid error in RBMDevice::do_mount"
	)
      );
    });
  });
}

read_ertr::future<> RotationalDevice::read(
  uint64_t offset,
  bufferptr &bptr)
{
  auto length = bptr.length();
  return device.dma_read(offset, bptr.c_str(), length
  ).handle_exception([](auto e) -> read_ertr::future<size_t> {
    return crimson::ct_error::input_output_error::make();
  }).then([length](auto result) -> read_ertr::future<> {
    if (result != length) {
      return crimson::ct_error::input_output_error::make();
    }
    return read_ertr::now();
  });
}

read_ertr::future<> RotationalDevice::_readv(
  uint64_t offset,
  std::vector<bufferptr> ptrs) {
  LOG_PREFIX(NVMeBlockDevice::_readv);
  DEBUG("block: read offset {}, {} buffers", offset, ptrs.size());
  if (ptrs.size() == 0) {
    return read_ertr::now();
  }

  std::vector<iovec> iov;
  size_t length = 0;
  for (auto &ptr : ptrs) {
    length += ptr.length();
    assert((ptr.length() % super.block_size) == 0);
    iov.emplace_back(ptr.c_str(), ptr.length());
  }
  return device.dma_read(offset, std::move(iov)
  ).handle_exception(
    [FNAME](auto e) -> read_ertr::future<size_t> {
    ERROR("read: dma_read got error{}", e);
    return crimson::ct_error::input_output_error::make();
  }).then([length, FNAME](auto result) -> read_ertr::future<> {
    if (result != length) {
      ERROR("read: dma_read got error with not proper length");
      return crimson::ct_error::input_output_error::make();
    }
    return read_ertr::now();
  });
}

write_ertr::future<> RotationalDevice::write(
  uint64_t offset,
  bufferptr bptr,
  uint16_t stream)
{
  auto length = bptr.length();
  return seastar::do_with(std::move(bptr), [this, offset, length](auto &bptr) {
    return device.dma_write(offset, bptr.c_str(), length
    ).handle_exception([](auto e) -> write_ertr::future<size_t> {
      return crimson::ct_error::input_output_error::make();
    }).then([length](auto result) -> write_ertr::future<> {
      if (result != length) {
        return crimson::ct_error::input_output_error::make();
      }
      return write_ertr::now();
    });
  });
}

open_ertr::future<> RotationalDevice::open(
  const std::string &path,
  seastar::open_flags mode)
{
  return seastar::open_file_dma(path, mode).then([this](auto file) {
    device = std::move(file);
  }).handle_exception([](auto e) -> open_ertr::future<> {
    return crimson::ct_error::input_output_error::make();
  });
}

write_ertr::future<> RotationalDevice::writev(
  uint64_t offset,
  ceph::bufferlist bl,
  uint16_t stream) {
  bl.rebuild_aligned(super.block_size);

  return seastar::do_with(
    bl.prepare_iovs(),
    std::move(bl),
    [this, offset](auto& iovs, auto& bl)
  {
    return write_ertr::parallel_for_each(
      iovs,
      [this, offset](auto& p) mutable
    {
      auto off = offset + p.offset;
      auto len = p.length;
      auto& iov = p.iov;
      return device.dma_write(off, std::move(iov)
      ).handle_exception(
        [](auto e) -> write_ertr::future<size_t>
      {
        return crimson::ct_error::input_output_error::make();
      }).then([len](size_t written) -> write_ertr::future<> {
        if (written != len) {
          return crimson::ct_error::input_output_error::make();
        }
        return write_ertr::now();
      });
    });
  });
}

}
