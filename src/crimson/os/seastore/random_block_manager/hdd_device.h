// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/random_block_manager/rbm_device.h"

namespace crimson::os::seastore::random_block_device {
class RotationalDevice : public RBMDevice {
public:
  RotationalDevice(std::string device_path) : device_path(device_path) {}
  ~RotationalDevice() = default;

  /// Device interface

  seastar::future<> start() final {
    return shard_devices.start(device_path);
  }

  seastar::future<> stop() final {
    return shard_devices.stop();
  }

  Device& get_sharded_device() final {
    return shard_devices.local();
  }

  mkfs_ret mkfs(device_config_t config) final;

  mount_ret mount() final;

  device_type_t get_device_type() const final {
    return device_type_t::HDD;
  }

  close_ertr::future<> close() final {
    return device.close();
  }

  /// RBMDevice interface

  read_ertr::future<> read(
    uint64_t offset,
    bufferptr &bptr) final;

  write_ertr::future<> write(
    uint64_t offset,
    bufferptr bptr,
    uint16_t stream = 0) final;

  open_ertr::future<> open(
    const std::string &path,
    seastar::open_flags mode) final;

  write_ertr::future<> writev(
    uint64_t offset,
    ceph::bufferlist bl,
    uint16_t stream = 0) final;

  stat_device_ret stat_device() final {
    return seastar::file_stat(device_path, seastar::follow_symlink::yes
    ).then([this](auto stat) {
      return seastar::open_file_dma(
        device_path,
	seastar::open_flags::rw | seastar::open_flags::dsync
      ).then([stat](auto file) mutable {
	return seastar::do_with(std::move(file), [stat](auto &file) mutable {
	  return file.size().then([stat](auto size) mutable {
	    stat.size = size;
	    return stat;
	  }).then([file](auto stat) mutable {
	    return file.close().then([stat] {
	      return stat;
	    });
	  });
	});
      });
    }).handle_exception([](auto e) -> stat_device_ret {
      return crimson::ct_error::input_output_error::make();
    });
  }

  std::string get_device_path() const final {
    return device_path;
  }

private:
  std::string device_path;
  seastar::file device;
  seastar::sharded<RotationalDevice> shard_devices;
};

}
