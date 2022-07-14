// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/random_block_manager.h"
#include "crimson/os/seastore/random_block_manager/rbm_device.h"
#include "crimson/os/seastore/device.h"

namespace crimson::os::seastore::hdd_device {
using read_ertr = rbm_device::read_ertr;
using write_ertr = rbm_device::write_ertr;
using open_ertr = rbm_device::open_ertr;
using nvme_command_ertr = rbm_device::nvme_command_ertr;
using discard_ertr = rbm_device::discard_ertr;

class HDDBlockDevice : public rbm_device::RBMDevice {
public:
  device_type_t get_device_type() const final {
    return device_type_t::HARD_DISK;
  }

  open_ertr::future<> open(
    const std::string &in_path,
    seastar::open_flags mode) override {
    return seastar::file_stat(in_path).then([this, in_path, mode] (seastar::stat_data stat) {
      size = stat.size;
      block_size = stat.block_size;
      return seastar::open_file_dma(in_path, mode).then([this](auto file) {
	device = file;
      });
    });
  }

  write_ertr::future<> write(
    uint64_t offset,
    bufferptr &bptr,
    uint16_t stream = 0) override {
    bufferlist bl;
    bl.append(bptr);
    return device::do_write(device_id, device, offset, bptr);
  }

  using RBMDevice::read;
  read_ertr::future<> read(
    uint64_t offset,
    bufferptr &bptr) final {
    return device::do_read(device_id, device, offset, bptr.length(), bptr);
  }

  close_ertr::future<> close() override {
    return device.close();
  }

  discard_ertr::future<> discard(
    uint64_t offset,
    uint64_t len) override {
    return discard_ertr::now();
  }

  mkfs_ret mkfs(device_config_t) final {
    return mkfs_ertr::now();
  }

  mount_ret mount() final {
    return mount_ertr::now();
  }

  write_ertr::future<> writev(
    uint64_t offset,
    ceph::bufferlist bl,
    uint16_t stream = 0) final {
    return device::do_writev(device_id, device, offset, std::move(bl), block_size);
  }
private:
  seastar::file device;
};
}
