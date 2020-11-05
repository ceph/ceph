// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <iosfwd>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <seastar/core/future.hh>

#include "include/ceph_assert.h"
#include "include/buffer_fwd.h"
#include "crimson/osd/exceptions.h"
#include "crimson/os/poseidonstore/poseidonstore_types.h"

namespace crimson::os::poseidonstore {

class DeviceManager {
public:
  using init_ertr = crimson::errorator<
    crimson::ct_error::enospc,
    crimson::ct_error::invarg,
    crimson::ct_error::erange>;
  virtual init_ertr::future<> init() = 0;

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
      size_t len) {
    auto ptrref = std::make_unique<ceph::bufferptr>(len);
    return read(addr, len, *ptrref).safe_then(
	[ptrref=std::move(ptrref)]() mutable {
	return read_ertr::make_ready_future<bufferptr>(std::move(*ptrref));
	});
  }

  using write_ertr = crimson::errorator<
    crimson::ct_error::input_output_error, 
    crimson::ct_error::invarg,             
    crimson::ct_error::ebadf,              
    crimson::ct_error::enospc              
    >;
  virtual write_ertr::future<> write(
    uint64_t offset, ceph::bufferlist bl) = 0;

  virtual size_t get_size() const = 0;
  virtual size_t get_block_size() const = 0;

  virtual ~DeviceManager() {}
};
using DeviceManagerRef = std::unique_ptr<DeviceManager>;

namespace device_manager {

struct memory_config_t {
  size_t size;
  size_t block_size;
  size_t page_size;
};
constexpr memory_config_t DEFAULT_TEST_MEMORY = {
  1 << 20,
  4 << 10,
  1 << 9 
};

std::ostream &operator<<(std::ostream &, const memory_config_t &);
DeviceManagerRef create_memory(memory_config_t config);

}

}
