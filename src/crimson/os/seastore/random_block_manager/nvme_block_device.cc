// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <sys/mman.h>
#include <string.h>

#include <fcntl.h>

#include "crimson/common/log.h"
#include "crimson/common/errorator-loop.h"

#include "include/buffer.h"
#include "rbm_device.h"
#include "nvme_block_device.h"
#include "block_rb_manager.h"
#include "crimson/os/seastore/logging.h"
SET_SUBSYS(seastore_device);

namespace crimson::os::seastore::random_block_device::nvme {

NVMeBlockDevice::mkfs_ret NVMeBlockDevice::mkfs(device_config_t config) {
  using crimson::common::get_conf;
  co_await shard_devices.local().do_primary_mkfs(config,
    seastar::smp::count,
    get_conf<Option::size_t>("seastore_cbjournal_size") 
  );
  co_return;
}

open_ertr::future<> NVMeBlockDevice::open(
  const std::string &in_path,
  seastar::open_flags mode) {
  auto file = co_await seastar::open_file_dma(in_path, mode);
  device = std::move(file);
  // Get SSD's features from identify_controller and namespace command.
  // Do identify_controller first, and then identify_namespace.
  auto id_ctr_data = co_await identify_controller(device
  ).handle_error(crimson::ct_error::input_output_error::handle([] {
    LOG_PREFIX(NVMeBlockDevice::open);
    DEBUG("identify_controller failed.\
      Proceeding to open the device normally without adding device-specific information.");
    return std::nullopt;
  }));
  if (id_ctr_data) {
    // TODO: enable multi-stream if the nvme device supports
    auto id_controller_data = *id_ctr_data;
    awupf = id_controller_data.awupf + 1;
    auto id_ns_data = co_await identify_namespace(device
    ).handle_error(crimson::ct_error::input_output_error::handle([] {
      LOG_PREFIX(NVMeBlockDevice::open);
      DEBUG("identify_namespace failed.\
	Proceeding to open the device normally without adding device-specific information.");
      return std::nullopt;
    }));
    if (id_ns_data) {
      auto id_namespace_data = *id_ns_data;
      atomic_write_unit = awupf * super.block_size;
      if (id_namespace_data.nsfeat.opterf == 1){
	// NPWG and NPWA is 0'based value
	write_granularity = super.block_size * (id_namespace_data.npwg + 1);
	write_alignment = super.block_size * (id_namespace_data.npwa + 1);
      }
    }
  } 
  co_return co_await open_for_io(in_path, mode);
}

open_ertr::future<> NVMeBlockDevice::open_for_io(
  const std::string& in_path,
  seastar::open_flags mode) {
  io_device.resize(stream_id_count);
  for (uint32_t i = 0; i < stream_id_count; i++) {
    auto file = co_await seastar::open_file_dma(in_path, mode);
    assert(io_device.size() > stream_index_to_open);
    io_device[stream_index_to_open] = std::move(file);
  }
  co_return;
}

NVMeBlockDevice::mount_ret NVMeBlockDevice::mount()
{
  LOG_PREFIX(NVMeBlockDevice::mount);
  DEBUG("mount");
  co_await shard_devices.invoke_on_all([](auto &local_device) {
    return local_device.do_shard_mount(
    ).handle_error(
      crimson::ct_error::assert_all{
	"Invalid error in NVMeBlockDevice::do_shard_mount"
    });
  });

  if (is_end_to_end_data_protection()) {
    auto id_ns_data = co_await identify_namespace(device);
    assert(id_ns_data);
    auto id_namespace_data = *id_ns_data;
    if (id_namespace_data.dps.protection_type !=
	nvme_format_nvm_command_t::PROTECT_INFORMATION_TYPE_2) {
      ERROR("seastore was formated with end-to-end-data-protection \
	but the device being mounted to use seastore does not support \
	the functionality. Please check the device.");
      ceph_abort();
    }
    if (id_namespace_data.lbaf[id_namespace_data.flbas.lba_index].ms != 
	nvme_identify_namespace_data_t::METASIZE_FOR_CHECKSUM_OFFLOAD) {
      ERROR("seastore was formated with end-to-end-data-protection \
	but the formatted device meta size is wrong. Please check the device.");
      ceph_abort();
    }
  }
  co_return;
}

write_ertr::future<> NVMeBlockDevice::write(
  uint64_t offset,
  bufferptr bptr,
  uint16_t stream) {
  LOG_PREFIX(NVMeBlockDevice::write);
  DEBUG("block: write offset {} len {}",
      offset,
      bptr.length());
  auto length = bptr.length();

  assert((length % super.block_size) == 0);
  uint16_t supported_stream = stream;
  if (stream >= stream_id_count) {
    supported_stream = WRITE_LIFE_NOT_SET;
  }
  if (is_end_to_end_data_protection()) {
    co_await nvme_write(offset, bptr.length(), bptr.c_str());
    co_return;
  }
  auto ret = co_await io_device[supported_stream].dma_write(
    offset, bptr.c_str(), length).handle_exception(
    [FNAME](auto e) -> write_ertr::future<size_t> {
    ERROR("write: dma_write got error{}", e);
    return crimson::ct_error::input_output_error::make();
  });
  if (ret != length) {
    ERROR("write: dma_write got error with not proper length");
    co_return co_await write_ertr::future<>(
      crimson::ct_error::input_output_error::make());
  }
  co_return;
}

read_ertr::future<> NVMeBlockDevice::read(
  uint64_t offset,
  bufferptr &bptr) {
  LOG_PREFIX(NVMeBlockDevice::read);
  DEBUG("block: read offset {} len {}",
      offset,
      bptr.length());
  auto length = bptr.length();
  if (length == 0) {
    co_return;
  }
  assert((length % super.block_size) == 0);

  if (is_end_to_end_data_protection()) {
    co_await nvme_read(offset, length, bptr.c_str());
    co_return;
  }
  auto ret = co_await device.dma_read(offset, bptr.c_str(), length
  ).handle_exception(
    [FNAME](auto e) -> read_ertr::future<size_t> {
    ERROR("read: dma_read got error{}", e);
    return crimson::ct_error::input_output_error::make();
  });
  
  if (ret != length) {
    ERROR("read: dma_read got error with not proper length");
    co_return co_await read_ertr::future<>(
      crimson::ct_error::input_output_error::make());
  }
  co_return;
}

write_ertr::future<> NVMeBlockDevice::writev(
  uint64_t offset,
  ceph::bufferlist bl,
  uint16_t stream) {
  LOG_PREFIX(NVMeBlockDevice::writev);
  DEBUG("block: write offset {} len {}",
    offset,
    bl.length());

  uint16_t supported_stream = stream;
  if (stream >= stream_id_count) {
    supported_stream = WRITE_LIFE_NOT_SET;
  }
  if (is_end_to_end_data_protection()) {
    co_await nvme_write(offset, bl.length(), bl.c_str());
    co_return;
  }
  bl.rebuild_aligned(super.block_size);
  auto iovs = bl.prepare_iovs();
  std::vector<seastar::future<>> tasks;
  tasks.reserve(iovs.size());

  for (auto &p : iovs) {
    auto off = offset + p.offset;
    auto len = p.length;
    auto iov = std::move(p.iov);
    tasks.emplace_back(
      seastar::futurize_invoke([this, off, supported_stream, len,
      iov = std::move(iov), FNAME]() mutable -> seastar::future<> {
      try {
	size_t written = co_await io_device[supported_stream].dma_write(
	  off, std::move(iov));
	if (written != len) {
	  ERROR("{} poffset={}~{} dma_write len={} inconsistent",
	    device_id_printer_t{get_device_id()}, off, len, written);
	  throw std::system_error(EIO, std::system_category(),
	    "written len mismatch"); 
	}
      } catch (const std::exception& e) {
	ERROR("{} poffset={}~{} dma_write got error -- {}",
	  device_id_printer_t{get_device_id()}, off, len, e.what());
	throw; 
      }
      co_return;
    }));
  }

  bool err = false;
  try {
    co_await seastar::when_all_succeed(tasks.begin(), tasks.end());
  } catch (...) {
    err = true;
  }
  if (err) {
    co_return co_await write_ertr::future<>(
      crimson::ct_error::input_output_error::make());
  }
  co_return;
}

Device::close_ertr::future<> NVMeBlockDevice::close() {
  LOG_PREFIX(NVMeBlockDevice::close);
  DEBUG("close");
  stream_index_to_open = WRITE_LIFE_NOT_SET;
  co_await device.close();
  for (auto& target_device : io_device) {
    co_await target_device.close();
  }
  co_return;
}

nvme_command_ertr::future<std::optional<nvme_identify_controller_data_t>>
NVMeBlockDevice::identify_controller(seastar::file f) {

  nvme_admin_command_t admin_command;
  nvme_identify_controller_data_t data;
  admin_command.common.opcode = nvme_admin_command_t::OPCODE_IDENTIFY;
  admin_command.common.addr = (uint64_t)&data;
  admin_command.common.data_len = sizeof(data);
  admin_command.identify.cns = nvme_identify_command_t::CNS_CONTROLLER;
  co_await pass_admin(admin_command, f);
  co_return std::move(data);
}

discard_ertr::future<> NVMeBlockDevice::discard(uint64_t offset, uint64_t len) {
  co_return co_await device.discard(offset, len);
}

nvme_command_ertr::future<std::optional<nvme_identify_namespace_data_t>>
NVMeBlockDevice::identify_namespace(seastar::file f) {

  auto nsid = co_await get_nsid(f);
  namespace_id = nsid;
  nvme_admin_command_t admin_command;
  nvme_identify_namespace_data_t data;
  admin_command.common.opcode = nvme_admin_command_t::OPCODE_IDENTIFY;
  admin_command.common.addr = (uint64_t)&data;
  admin_command.common.data_len = sizeof(data);
  admin_command.common.nsid = nsid;
  admin_command.identify.cns = nvme_identify_command_t::CNS_NAMESPACE;

  co_await pass_admin(admin_command, f);
  co_return std::move(data);
}

nvme_command_ertr::future<int> NVMeBlockDevice::get_nsid(seastar::file f) {
  auto ret = co_await f.ioctl(NVME_IOCTL_ID, nullptr
  ).handle_exception(
    [](auto e)->nvme_command_ertr::future<int> {
    LOG_PREFIX(NVMeBlockDevice::get_nsid);
    ERROR("pass_admin: ioctl failed");
    return crimson::ct_error::input_output_error::make();
  });
  co_return ret;
}

nvme_command_ertr::future<int> NVMeBlockDevice::pass_admin(
  nvme_admin_command_t& admin_cmd, seastar::file f) {
  auto ret = co_await f.ioctl(NVME_IOCTL_ADMIN_CMD, nullptr
  ).handle_exception(
    [](auto e)->nvme_command_ertr::future<int> {
    LOG_PREFIX(NVMeBlockDevice::pass_admin);
    ERROR("pass_admin: ioctl failed {}", e);
    return crimson::ct_error::input_output_error::make();
  });
  co_return ret;
}

nvme_command_ertr::future<int> NVMeBlockDevice::pass_through_io(
  nvme_io_command_t& io_cmd) {
  auto ret = co_await device.ioctl(NVME_IOCTL_IO_CMD, &io_cmd
  ).handle_exception(
    [](auto e)->nvme_command_ertr::future<int> {
    LOG_PREFIX(NVMeBlockDevice::pass_through_io);
    ERROR("pass_through_io: ioctl failed {}", e);
    return crimson::ct_error::input_output_error::make();
  });
  co_return ret;
}

nvme_command_ertr::future<> NVMeBlockDevice::try_enable_end_to_end_protection() {
  LOG_PREFIX(NVMeBlockDevice::try_enable_end_to_end_protection);
  auto id_ns_data = co_await identify_namespace(device);
  auto id_namespace_data = *id_ns_data;
  if (!id_namespace_data.nlbaf) {
    INFO("the device does not support end to end data protection,\
      mkfs() will be done without this functionality.");
    co_return;
  }
  int lba_format_index = -1;
  for (int i = 0; i < id_namespace_data.nlbaf; i++) {
    // TODO: enable other types of end to end data protection 
    // Note that the nvme device will generate crc if the namespace
    // is formatted with meta size 8
    // The nvme device can provide other types of data protections.
    // But, for now, we only consider the checksum offload in the device side.
    if (id_namespace_data.lbaf[i].ms ==
	nvme_identify_namespace_data_t::METASIZE_FOR_CHECKSUM_OFFLOAD) {
      lba_format_index = i;
      super.nvme_block_size = (1 << id_namespace_data.lbaf[i].lbads);
      break;
    }
  }
  if (lba_format_index == -1) {
    INFO("the device does not support end to end data protection,\
      mkfs() will be done without this functionality.");
    co_return;
  }

  auto nsid = co_await get_nsid(device);
  nvme_admin_command_t cmd;
  cmd.common.opcode = nvme_admin_command_t::OPCODE_FORMAT_NVM;
  cmd.common.nsid = nsid;
  // TODO: configure other protect information types (2 or 3) see above
  cmd.format.pi = nvme_format_nvm_command_t::PROTECT_INFORMATION_TYPE_2;
  cmd.format.lbaf = lba_format_index;
  auto ret = co_await pass_admin(cmd, device);
  if (ret != 0) {
    ERROR(
      "formt nvm command to use end-to-end-protection fails : {}", ret);
    ceph_abort();
  }

  id_ns_data = co_await identify_namespace(device);
  id_namespace_data = *id_ns_data;
  ceph_assert(id_namespace_data.dps.protection_type ==
     nvme_format_nvm_command_t::PROTECT_INFORMATION_TYPE_2);
  super.set_end_to_end_data_protection();
  co_return;
}

nvme_command_ertr::future<> NVMeBlockDevice::initialize_nvme_features() {
  if (!crimson::common::get_conf<bool>("seastore_disable_end_to_end_data_protection")) {
    co_return co_await try_enable_end_to_end_protection();
  }
  co_return;
}

write_ertr::future<> NVMeBlockDevice::nvme_write(
  uint64_t offset, size_t len, void *buffer_ptr) {
  nvme_io_command_t cmd;
  cmd.common.opcode = nvme_io_command_t::OPCODE_WRITE;
  cmd.common.nsid = namespace_id;
  cmd.common.data_len = len;
  // To perform checksum offload, we need to set PRACT to 1 and PRCHK to 4
  // according to NVMe spec.
  cmd.rw.prinfo_pract = nvme_rw_command_t::PROTECT_INFORMATION_ACTION_ENABLE;
  cmd.rw.prinfo_prchk = nvme_rw_command_t::PROTECT_INFORMATION_CHECK_GUARD;
  cmd.common.addr = (__u64)(uintptr_t)buffer_ptr;
  ceph_assert(super.nvme_block_size > 0);
  auto lba_shift = ffsll(super.nvme_block_size) - 1;
  cmd.rw.s_lba = offset >> lba_shift;
  cmd.rw.nlb = (len >> lba_shift) - 1;
  auto ret = co_await pass_through_io(cmd);
  if (ret != 0) {
    LOG_PREFIX(NVMeBlockDevice::nvme_write);
    ERROR("write nvm command with checksum offload fails : {}", ret);
    ceph_abort();
  }
  co_return;
}

read_ertr::future<> NVMeBlockDevice::nvme_read(
  uint64_t offset, size_t len, void *buffer_ptr) {

  nvme_io_command_t cmd;
  cmd.common.opcode = nvme_io_command_t::OPCODE_READ;
  cmd.common.nsid = namespace_id;
  cmd.common.data_len = len;
  cmd.rw.prinfo_pract = nvme_rw_command_t::PROTECT_INFORMATION_ACTION_ENABLE;
  cmd.rw.prinfo_prchk = nvme_rw_command_t::PROTECT_INFORMATION_CHECK_GUARD;
  cmd.common.addr = (__u64)(uintptr_t)buffer_ptr;
  ceph_assert(super.nvme_block_size > 0);
  auto lba_shift = ffsll(super.nvme_block_size) - 1;
  cmd.rw.s_lba = offset >> lba_shift;
  cmd.rw.nlb = (len >> lba_shift) - 1;
  auto ret = co_await pass_through_io(cmd);
  if (ret != 0) {
    LOG_PREFIX(NVMeBlockDevice::nvme_read);
    ERROR("read nvm command with checksum offload fails : {}", ret);
    ceph_abort();
  }
  co_return;
}

}
