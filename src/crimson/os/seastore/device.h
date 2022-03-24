// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <memory>

#include "include/buffer_fwd.h"

#include "crimson/common/errorator.h"

namespace crimson::os::seastore {

/**
 * Device
 *
 * Represents a general device regardless of the underlying medium.
 */
class Device {
public:
  virtual device_id_t get_device_id() const = 0;

  virtual seastore_off_t get_block_size() const = 0;

  using read_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange>;
  virtual read_ertr::future<> read(
    paddr_t addr,
    size_t len,
    ceph::bufferptr &out) = 0;

  read_ertr::future<ceph::bufferptr> read(
    paddr_t addr,
    size_t len
  ) {
    auto ptrref = std::make_unique<ceph::bufferptr>(
      buffer::create_page_aligned(len));
    return read(addr, len, *ptrref
    ).safe_then([ptrref=std::move(ptrref)]() mutable {
      return read_ertr::make_ready_future<bufferptr>(std::move(*ptrref));
    });
  }
};

}
