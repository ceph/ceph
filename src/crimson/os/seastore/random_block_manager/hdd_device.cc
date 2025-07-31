// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/common/errorator-loop.h"
#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/random_block_manager/hdd_device.h"

SET_SUBSYS(seastore_device);

namespace crimson::os::seastore::random_block_device {

RotationalDevice::mkfs_ret RotationalDevice::mkfs(device_config_t config) {
  LOG_PREFIX(RotationalDevice::mkfs);
  INFO("{}", config);
  return shard_devices.local().do_primary_mkfs(config, seastar::smp::count, 0);
}

RotationalDevice::mount_ret RotationalDevice::mount() {
  return shard_devices.invoke_on_all([](auto &local_device) {
    return local_device.do_shard_mount(
    ).handle_error(
      crimson::ct_error::assert_all{
        "Invalid error in RBMDevice::do_mount"
      }
    );
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
