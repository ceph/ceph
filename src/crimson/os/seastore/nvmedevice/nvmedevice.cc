// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include <fcntl.h>

#include "crimson/common/log.h"

#include "include/buffer.h"
#include "crimson/os/seastore/nvmedevice/nvmedevice.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_filestore);
  }
}

namespace crimson::os::seastore::nvme_device {

open_ertr::future<> PosixNVMeDevice::open(
  const std::string &in_path,
  seastar::open_flags mode) {
  return seastar::do_with(in_path, [this, mode](auto& in_path) {
    return seastar::file_stat(in_path).then([this, mode, in_path](auto stat) {
      this->block_size = stat.block_size;
      this->size = stat.size;
      return seastar::open_file_dma(in_path, mode).then([=](auto file) {
        this->device = file;
        logger().debug("open");
        return seastar::now();
      });
    }).handle_exception([](auto e) -> open_ertr::future<> {
      logger().error("open: got error{}", e);
      return crimson::ct_error::input_output_error::make();
    });
  });
}

write_ertr::future<> PosixNVMeDevice::write(
  uint64_t offset,
  bufferptr &bptr,
  uint16_t stream) {
  logger().debug(
      "block: write offset {} len {}",
      offset,
      bptr.length());
  auto length = bptr.length();

  assert((length % block_size) == 0);

  return device.dma_write(offset, bptr.c_str(), length).handle_exception(
    [length](auto e) -> write_ertr::future<size_t> {
      logger().error("write: dma_write got error{}", e);
      return crimson::ct_error::input_output_error::make();
    }).then([=](auto result) -> write_ertr::future<> {
      if (result != length) {
        logger().error("write: dma_write got error with not proper length");
        return crimson::ct_error::input_output_error::make();
      }
      return write_ertr::now();
    });
}

read_ertr::future<> PosixNVMeDevice::read(
  uint64_t offset,
  bufferptr &bptr) {
  logger().debug(
      "block: read offset {} len {}",
      offset,
      bptr.length());
  auto length = bptr.length();

  assert((length % block_size) == 0);

  return device.dma_read(offset, bptr.c_str(), length).handle_exception(
    [length](auto e) -> read_ertr::future<size_t> {
      logger().error("read: dma_read got error{}", e);
      return crimson::ct_error::input_output_error::make();
    }).then([=](auto result) -> read_ertr::future<> {
      if (result != length) {
        logger().error("read: dma_read got error with not proper length");
        return crimson::ct_error::input_output_error::make();
      }
      return read_ertr::now();
    });
}

seastar::future<> PosixNVMeDevice::close() {
  logger().debug(" close ");
  return device.close();
}
}
