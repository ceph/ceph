// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "crimson/os/poseidonstore/device_manager.h"

#include "crimson/os/poseidonstore/device_manager/memory.h"

namespace crimson::os::poseidonstore::device_manager {

class MemoryDeviceManager;

class MemoryDeviceManager final : public DeviceManager {

  const memory_config_t config;

  char *buffer = nullptr;

public:
  MemoryDeviceManager(memory_config_t config);
  ~MemoryDeviceManager();

  init_ertr::future<> init() final;

  read_ertr::future<ceph::bufferptr> read(
    paddr_t addr,
    size_t len) {
    auto ptrref = std::make_unique<ceph::bufferptr>(len);
    return read(addr, len, *ptrref).safe_then(
      [ptrref=std::move(ptrref)]() mutable {
	return read_ertr::make_ready_future<bufferptr>(std::move(*ptrref));
      });
  }

  read_ertr::future<> read(
    paddr_t addr,
    size_t len,
    ceph::bufferptr &out) final;
  write_ertr::future<> write(
    paddr_t addr,
    ceph::bufferlist bl) final;

  size_t get_size() const final {
    return config.size;
  }
  uint64_t get_block_size() const {
    return config.block_size;
  }
};

}
