// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <sys/mman.h>
#include <string.h>

#include <fcntl.h>
#include <seastar/coroutine/parallel_for_each.hh>

#include "crimson/common/log.h"
#include "crimson/common/errorator-utils.h"

#include "include/buffer.h"
#include "rbm_device.h"
#include "nvme_block_device.h"
#include "block_rb_manager.h"
#include "crimson/os/seastore/logging.h"
SET_SUBSYS(seastore_device);

namespace crimson::os::seastore::random_block_device::nvme {

seastar::future<> NVMeBlockDevice::start(uint32_t shard_nums)
{
  device_shard_nums = shard_nums;
  auto num_shard_services = (device_shard_nums + seastar::smp::count - 1 ) / seastar::smp::count;
  LOG_PREFIX(NVMeBlockDevice::start);
  DEBUG("device_shard_nums={} seastar::smp={}, num_shard_services={}", device_shard_nums, seastar::smp::count, num_shard_services);
  return shard_devices.start(num_shard_services, device_path);

}

seastar::future<> NVMeBlockDevice::stop()
{
  return shard_devices.stop();
}

Device& NVMeBlockDevice::get_sharded_device(store_index_t store_index)
{
  assert(store_index < shard_devices.local().mshard_devices.size());
  return *shard_devices.local().mshard_devices[store_index];
}

NVMeBlockDevice::mkfs_ret NVMeBlockDevice::mkfs(device_config_t config) {
  using crimson::common::get_conf;
  co_await shard_devices.local().mshard_devices[0]->do_primary_mkfs(config,
    seastar::smp::count,
    get_conf<Option::size_t>("seastore_cbjournal_size") 
  );
}

open_ertr::future<> NVMeBlockDevice::open(
  const std::string &in_path,
  seastar::open_flags mode) {
  LOG_PREFIX(NVMeBlockDevice::open);
  driver = make_nvme_io_driver(in_path);
  co_await driver->open(in_path, mode
  ).handle_error(
    open_ertr::pass_further{},
    crimson::ct_error::assert_all("Invalid error in NVMeBlockDevice::open"));
  // Combine the raw identify-derived capabilities with this device's superblock
  // block size (the on-disk logical block size) to compute the write
  // granularity / alignment / atomic-write-unit, exactly as before. The math
  // stays here because it depends on super.block_size, which the driver does
  // not own.
  const auto &caps = driver->get_caps();
  if (caps.ctrl_identified) {
    awupf = caps.awupf;
    if (caps.ns_identified) {
      atomic_write_unit = awupf * super.block_size;
      if (caps.opterf) {
	// NPWG and NPWA is 0'based value
	write_granularity = super.block_size * (caps.npwg + 1);
	write_alignment = super.block_size * (caps.npwa + 1);
      }
    } else {
      DEBUG("identify_namespace failed.\
	Proceeding to open the device normally without adding device-specific information.");
    }
  } else {
    DEBUG("identify_controller failed.\
      Proceeding to open the device normally without adding device-specific information.");
  }
  // PI is activated on the driver once the superblock that records it is
  // available: in try_enable_end_to_end_protection() during mkfs, and in
  // mount() after read_rbm_superblock. super is not yet loaded here on the
  // mount path, so there is nothing to push at open() time.
}

NVMeBlockDevice::mount_ret NVMeBlockDevice::mount()
{
  LOG_PREFIX(NVMeBlockDevice::mount);
  DEBUG("mount");
  co_await shard_devices.invoke_on_all([](auto &local_device) {
    return seastar::do_for_each(local_device.mshard_devices, [](auto& mshard_device) {
      return mshard_device->do_shard_mount(
      ).handle_error(
        crimson::ct_error::assert_all(
          "Invalid error in NVMeBlockDevice::do_shard_mount"
      ));
    });
  });

  // Superblocks are loaded now: activate PI on each shard's driver if the
  // device was formatted with end-to-end data protection, so subsequent I/O
  // uses the PI command path.
  co_await shard_devices.invoke_on_all([](auto &local_device) {
    for (auto& mshard_device : local_device.mshard_devices) {
      if (mshard_device->is_end_to_end_data_protection()) {
        mshard_device->driver->set_protection_info(
          true, mshard_device->super.nvme_block_size);
      }
    }
    return seastar::now();
  });

  if (is_end_to_end_data_protection()) {
    // Validate the attached device actually supports the PI the superblock
    // expects. A throwaway driver performs the identify.
    auto io_driver = make_nvme_io_driver(device_path);
    co_await io_driver->open(device_path,
      seastar::open_flags::rw | seastar::open_flags::dsync
    ).handle_error(crimson::ct_error::assert_all(
      "Invalid error open in NVMeBlockDevice::mount PI-validate"));
    auto id_ns_data = co_await io_driver->identify_namespace();
    co_await io_driver->close().handle_error(crimson::ct_error::assert_all(
      "Invalid error close in NVMeBlockDevice::mount PI-validate"));
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
}

write_ertr::future<> NVMeBlockDevice::write(
  uint64_t offset,
  bufferptr bptr,
  uint16_t stream) {
  LOG_PREFIX(NVMeBlockDevice::write);
  DEBUG("block: write offset {} len {}", offset, bptr.length());
  assert((bptr.length() % super.block_size) == 0);
  // bptr is a by-value parameter: co_await (rather than returning the future)
  // keeps it alive in this coroutine frame for the duration of the driver call.
  co_await driver->write(get_device_id(), offset, bptr, stream);
}

read_ertr::future<> NVMeBlockDevice::read(
  uint64_t offset,
  bufferptr &bptr) {
  return driver->read(get_device_id(), offset, bptr.length(), bptr);
}

read_ertr::future<> NVMeBlockDevice::_readv(
  uint64_t offset,
  std::vector<bufferptr> ptrs) {
  return driver->readv(get_device_id(), offset, std::move(ptrs));
}

write_ertr::future<> NVMeBlockDevice::writev(
  uint64_t offset,
  ceph::bufferlist bl,
  uint16_t stream) {
  LOG_PREFIX(NVMeBlockDevice::writev);
  DEBUG("block: write offset {} len {}", offset, bl.length());
  // bl is a by-value parameter: co_await keeps it (and the iovecs the driver
  // builds from it) alive for the duration of the driver call.
  co_await driver->writev(
    get_device_id(), offset, std::move(bl), super.block_size, stream);
}

Device::close_ertr::future<> NVMeBlockDevice::close() {
  LOG_PREFIX(NVMeBlockDevice::close);
  DEBUG("close");
  if (!driver) {
    return close_ertr::now();
  }
  return driver->close();
}

discard_ertr::future<> NVMeBlockDevice::discard(uint64_t offset, uint64_t len) {
  return driver->discard(offset, len);
}

nvme_command_ertr::future<> NVMeBlockDevice::try_enable_end_to_end_protection() {
  LOG_PREFIX(NVMeBlockDevice::try_enable_end_to_end_protection);
  auto id_ns_data = co_await driver->identify_namespace();
  if (!id_ns_data) {
    INFO("the device does not support end to end data protection,\
      mkfs() will be done without this functionality.");
    co_return;
  }
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

  auto nsid = driver->get_namespace_id();
  nvme_admin_command_t cmd;
  cmd.common.opcode = nvme_admin_command_t::OPCODE_FORMAT_NVM;
  cmd.common.nsid = nsid;
  // TODO: configure other protect information types (2 or 3) see above
  cmd.format.pi = nvme_format_nvm_command_t::PROTECT_INFORMATION_TYPE_2;
  cmd.format.lbaf = lba_format_index;
  auto ret = co_await driver->pass_admin(cmd);
  if (ret != 0) {
    ERROR(
      "formt nvm command to use end-to-end-protection fails : {}", ret);
    ceph_abort();
  }

  id_ns_data = co_await driver->identify_namespace();
  assert(id_ns_data);
  id_namespace_data = *id_ns_data;
  ceph_assert(id_namespace_data.dps.protection_type ==
     nvme_format_nvm_command_t::PROTECT_INFORMATION_TYPE_2);
  super.set_end_to_end_data_protection();
  // Activate PI on the driver so the superblock write that follows in
  // do_primary_mkfs() uses the PI command path.
  driver->set_protection_info(true, super.nvme_block_size);
}

nvme_command_ertr::future<> NVMeBlockDevice::initialize_nvme_features() {
  if (!crimson::common::get_conf<bool>("seastore_disable_end_to_end_data_protection")) {
    co_await try_enable_end_to_end_protection();
  }
}

}
