// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include <fcntl.h>

#include "crimson/common/log.h"

#include "include/buffer.h"
#include "nvmedevice.h"

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
        // Get SSD's features from identify_controller and namespace command.
        // Do identify_controller first, and then identify_namespace.
        return identify_controller().safe_then([this](auto id_controller_data) {
          support_multistream = id_controller_data.oacs.support_directives;
          return identify_namespace().safe_then([this] (auto id_namespace_data) {
            block_size = (1 << id_namespace_data.lbaf0.lbads);
            data_protection_type = id_namespace_data.dps.protection_type;
            data_protection_enabled = (data_protection_type > 0);
            if (id_namespace_data.nsfeat.opterf == 1){
              // NPWG and NPWA is 0'based value
              write_granularity = block_size * (id_namespace_data.npwg + 1);
              write_alignment = block_size * (id_namespace_data.npwa + 1);
            }
            return seastar::now();
          });
        });
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
    [](auto e) -> write_ertr::future<size_t> {
      logger().error("write: dma_write got error{}", e);
        return crimson::ct_error::input_output_error::make();
    }).then([length](auto result) -> write_ertr::future<> {
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
    [](auto e) -> read_ertr::future<size_t> {
      logger().error("read: dma_read got error{}", e);
      return crimson::ct_error::input_output_error::make();
    }).then([length](auto result) -> read_ertr::future<> {
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

nvme_command_ertr::future<nvme_identify_controller_data_t>
PosixNVMeDevice::identify_controller() {
  return seastar::do_with(
    nvme_admin_command_t(),
    nvme_identify_controller_data_t(),
    [this](auto &admin_command, auto &data) {
    admin_command.common.opcode = nvme_admin_command_t::OPCODE_IDENTIFY;
    admin_command.common.addr = (uint64_t)&data;
    admin_command.common.data_len = sizeof(data);
    admin_command.identify.cns = nvme_identify_command_t::CNS_CONTROLLER;

    return pass_admin(admin_command).safe_then([&data](auto status) {
      return seastar::make_ready_future<nvme_identify_controller_data_t>(
        std::move(data));
      });
  });
}

discard_ertr::future<> PosixNVMeDevice::discard(uint64_t offset, uint64_t len) {
  return device.discard(offset, len);
}

nvme_command_ertr::future<nvme_identify_namespace_data_t>
PosixNVMeDevice::identify_namespace() {
  return get_nsid().safe_then([this](auto nsid) {
    return seastar::do_with(
      nvme_admin_command_t(),
      nvme_identify_namespace_data_t(),
      [this, nsid](auto &admin_command, auto &data) {
      admin_command.common.opcode = nvme_admin_command_t::OPCODE_IDENTIFY;
      admin_command.common.addr = (uint64_t)&data;
      admin_command.common.data_len = sizeof(data);
      admin_command.common.nsid = nsid;
      admin_command.identify.cns = nvme_identify_command_t::CNS_NAMESPACE;

      return pass_admin(admin_command).safe_then([&data](auto status){
        return seastar::make_ready_future<nvme_identify_namespace_data_t>(
          std::move(data));
      });
    });
  });
}

nvme_command_ertr::future<int> PosixNVMeDevice::get_nsid() {
  return device.ioctl(NVME_IOCTL_ID, nullptr);
}

nvme_command_ertr::future<int> PosixNVMeDevice::pass_admin(
  nvme_admin_command_t& admin_cmd) {
  return device.ioctl(NVME_IOCTL_ADMIN_CMD, &admin_cmd);
}

nvme_command_ertr::future<int> PosixNVMeDevice::pass_through_io(
  nvme_io_command_t& io_cmd) {
  return device.ioctl(NVME_IOCTL_IO_CMD, &io_cmd);
}

open_ertr::future<> TestMemory::open(
  const std::string &in_path,
   seastar::open_flags mode) {
  if (buf) {
    return open_ertr::now();
  }

  logger().debug(
    "Initializing test memory device {}",
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

write_ertr::future<> TestMemory::write(
  uint64_t offset,
  bufferptr &bptr,
  uint16_t stream) {
  ceph_assert(buf);
  logger().debug(
    "TestMemory: write offset {} len {}",
    offset,
    bptr.length());

  ::memcpy(buf + offset, bptr.c_str(), bptr.length());

  return write_ertr::now();
}

read_ertr::future<> TestMemory::read(
  uint64_t offset,
  bufferptr &bptr) {
  ceph_assert(buf);
  logger().debug(
    "TestMemory: read offset {} len {}",
    offset,
    bptr.length());

  bptr.copy_in(0, bptr.length(), buf + offset);
  return read_ertr::now();
}

seastar::future<> TestMemory::close() {
  logger().debug(" close ");
  return seastar::now();
}
}
