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

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_seastore_tm);
  }
}

namespace crimson::os::seastore::random_block_device::nvme {

NVMeBlockDevice::mkfs_ret NVMeBlockDevice::mkfs(device_config_t config) {
  using crimson::common::get_conf;
  return shard_devices.local().do_primary_mkfs(config,
    seastar::smp::count,
    get_conf<Option::size_t>("seastore_cbjournal_size") 
  );
}

open_ertr::future<> NVMeBlockDevice::open(
  const std::string &in_path,
  seastar::open_flags mode) {
  return seastar::do_with(in_path, [this, mode](auto& in_path) {
    return seastar::file_stat(in_path).then([this, mode, in_path](auto stat) {
      return seastar::open_file_dma(in_path, mode).then([=, this](auto file) {
        device = std::move(file);
        logger().debug("open");
        // Get SSD's features from identify_controller and namespace command.
        // Do identify_controller first, and then identify_namespace.
        return identify_controller(device).safe_then([this, in_path, mode](
          auto id_controller_data) {
	  // TODO: enable multi-stream if the nvme device supports
          awupf = id_controller_data.awupf + 1;
          return identify_namespace(device).safe_then([this, in_path, mode] (
            auto id_namespace_data) {
            atomic_write_unit = awupf * super.block_size;
            if (id_namespace_data.nsfeat.opterf == 1){
              // NPWG and NPWA is 0'based value
              write_granularity = super.block_size * (id_namespace_data.npwg + 1);
              write_alignment = super.block_size * (id_namespace_data.npwa + 1);
            }
            return open_for_io(in_path, mode);
          });
        }).handle_error(crimson::ct_error::input_output_error::handle([this, in_path, mode]{
          logger().error("open: id ctrlr failed. open without ioctl");
          return open_for_io(in_path, mode);
        }), crimson::ct_error::pass_further_all{});
      });
    });
  });
}

open_ertr::future<> NVMeBlockDevice::open_for_io(
  const std::string& in_path,
  seastar::open_flags mode) {
  io_device.resize(stream_id_count);
  return seastar::do_for_each(io_device, [=, this](auto &target_device) {
    return seastar::open_file_dma(in_path, mode).then([this](
      auto file) {
      assert(io_device.size() > stream_index_to_open);
      io_device[stream_index_to_open] = std::move(file);
      return seastar::now();
    });
  });
}

NVMeBlockDevice::mount_ret NVMeBlockDevice::mount()
{
  logger().debug(" mount ");
  return shard_devices.invoke_on_all([](auto &local_device) {
    return local_device.do_shard_mount(
    ).handle_error(
      crimson::ct_error::assert_all{
	"Invalid error in NVMeBlockDevice::do_shard_mount"
    });
  }).then([this] () {
    if (is_end_to_end_data_protection()) {
      return identify_namespace(device
      ).safe_then([] (auto id_namespace_data) {
	if (id_namespace_data.dps.protection_type !=
	    nvme_format_nvm_command_t::PROTECT_INFORMATION_TYPE_2) {
	  logger().error("seastore was formated with end-to-end-data-protection \
	    but the device being mounted to use seastore does not support \
	    the functionality. Please check the device.");
	  ceph_abort();
	}
	if (id_namespace_data.lbaf[id_namespace_data.flbas.lba_index].ms != 
	    nvme_identify_namespace_data_t::METASIZE_FOR_CHECKSUM_OFFLOAD) {
	  logger().error("seastore was formated with end-to-end-data-protection \
	    but the formatted device meta size is wrong. Please check the device.");
	  ceph_abort();
	}
	return mount_ertr::now();
      });
    }
    return mount_ertr::now();
  });
}

write_ertr::future<> NVMeBlockDevice::write(
  uint64_t offset,
  bufferptr bptr,
  uint16_t stream) {
  logger().debug(
      "block: write offset {} len {}",
      offset,
      bptr.length());
  auto length = bptr.length();

  assert((length % super.block_size) == 0);
  uint16_t supported_stream = stream;
  if (stream >= stream_id_count) {
    supported_stream = WRITE_LIFE_NOT_SET;
  }
  if (is_end_to_end_data_protection()) {
    return seastar::do_with(
      bptr,
      [this, offset] (auto &bptr) {
      return nvme_write(offset, bptr.length(), bptr.c_str());
    });
  }
  return seastar::do_with(
    bptr,
    [this, offset, length, supported_stream] (auto& bptr) {
    return io_device[supported_stream].dma_write(
      offset, bptr.c_str(), length).handle_exception(
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
  });
}

read_ertr::future<> NVMeBlockDevice::read(
  uint64_t offset,
  bufferptr &bptr) {
  logger().debug(
      "block: read offset {} len {}",
      offset,
      bptr.length());
  auto length = bptr.length();
  if (length == 0) {
    return read_ertr::now();
  }
  assert((length % super.block_size) == 0);

  if (is_end_to_end_data_protection()) {
    return nvme_read(offset, length, bptr.c_str());
  }

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

write_ertr::future<> NVMeBlockDevice::writev(
  uint64_t offset,
  ceph::bufferlist bl,
  uint16_t stream) {
  logger().debug(
    "block: write offset {} len {}",
    offset,
    bl.length());

  uint16_t supported_stream = stream;
  if (stream >= stream_id_count) {
    supported_stream = WRITE_LIFE_NOT_SET;
  }
  if (is_end_to_end_data_protection()) {
    return seastar::do_with(
      std::move(bl),
      [this, offset] (auto &bl) {
      return nvme_write(offset, bl.length(), bl.c_str());
    });
  }
  bl.rebuild_aligned(super.block_size);

  return seastar::do_with(
    bl.prepare_iovs(),
    std::move(bl),
    [this, supported_stream, offset](auto& iovs, auto& bl)
  {
    return write_ertr::parallel_for_each(
      iovs,
      [this, supported_stream, offset](auto& p) mutable
    {
      auto off = offset + p.offset;
      auto len = p.length;
      auto& iov = p.iov;
      return io_device[supported_stream].dma_write(off, std::move(iov)
      ).handle_exception(
        [this, off, len](auto e) -> write_ertr::future<size_t>
      {
        logger().error("{} poffset={}~{} dma_write got error -- {}",
                       device_id_printer_t{get_device_id()}, off, len, e);
        return crimson::ct_error::input_output_error::make();
      }).then([this, off, len](size_t written) -> write_ertr::future<> {
        if (written != len) {
          logger().error("{} poffset={}~{} dma_write len={} inconsistent",
                         device_id_printer_t{get_device_id()}, off, len, written);
          return crimson::ct_error::input_output_error::make();
        }
        return write_ertr::now();
      });
    });
  });
}

Device::close_ertr::future<> NVMeBlockDevice::close() {
  logger().debug(" close ");
  stream_index_to_open = WRITE_LIFE_NOT_SET;
  return device.close().then([this]() {
    return seastar::do_for_each(io_device, [](auto target_device) {
      return target_device.close();
    });
  });
}

nvme_command_ertr::future<nvme_identify_controller_data_t>
NVMeBlockDevice::identify_controller(seastar::file f) {
  return seastar::do_with(
    nvme_admin_command_t(),
    nvme_identify_controller_data_t(),
    [this, f](auto &admin_command, auto &data) {
    admin_command.common.opcode = nvme_admin_command_t::OPCODE_IDENTIFY;
    admin_command.common.addr = (uint64_t)&data;
    admin_command.common.data_len = sizeof(data);
    admin_command.identify.cns = nvme_identify_command_t::CNS_CONTROLLER;

    return pass_admin(admin_command, f).safe_then([&data](auto status) {
      return seastar::make_ready_future<nvme_identify_controller_data_t>(
        std::move(data));
      });
  });
}

discard_ertr::future<> NVMeBlockDevice::discard(uint64_t offset, uint64_t len) {
  return device.discard(offset, len);
}

nvme_command_ertr::future<nvme_identify_namespace_data_t>
NVMeBlockDevice::identify_namespace(seastar::file f) {
  return get_nsid(f).safe_then([this, f](auto nsid) {
    namespace_id = nsid;
    return seastar::do_with(
      nvme_admin_command_t(),
      nvme_identify_namespace_data_t(),
      [this, nsid, f](auto &admin_command, auto &data) {
      admin_command.common.opcode = nvme_admin_command_t::OPCODE_IDENTIFY;
      admin_command.common.addr = (uint64_t)&data;
      admin_command.common.data_len = sizeof(data);
      admin_command.common.nsid = nsid;
      admin_command.identify.cns = nvme_identify_command_t::CNS_NAMESPACE;

      return pass_admin(admin_command, f).safe_then([&data](auto status){
        return seastar::make_ready_future<nvme_identify_namespace_data_t>(
          std::move(data));
      });
    });
  });
}

nvme_command_ertr::future<int> NVMeBlockDevice::get_nsid(seastar::file f) {
  return f.ioctl(NVME_IOCTL_ID, nullptr).handle_exception(
    [](auto e)->nvme_command_ertr::future<int> {
      logger().error("pass_admin: ioctl failed");
      return crimson::ct_error::input_output_error::make();
    });
}

nvme_command_ertr::future<int> NVMeBlockDevice::pass_admin(
  nvme_admin_command_t& admin_cmd, seastar::file f) {
  return f.ioctl(NVME_IOCTL_ADMIN_CMD, &admin_cmd).handle_exception(
    [](auto e)->nvme_command_ertr::future<int> {
      logger().error("pass_admin: ioctl failed {}", e);
      return crimson::ct_error::input_output_error::make();
    });
}

nvme_command_ertr::future<int> NVMeBlockDevice::pass_through_io(
  nvme_io_command_t& io_cmd) {
  return device.ioctl(NVME_IOCTL_IO_CMD, &io_cmd
  ).handle_exception([](auto e)->nvme_command_ertr::future<int> {
    logger().error("pass_through_io: ioctl failed {}", e);
    return crimson::ct_error::input_output_error::make();
  });
}

nvme_command_ertr::future<> NVMeBlockDevice::try_enable_end_to_end_protection() {
  return identify_namespace(device
  ).safe_then([this] (auto id_namespace_data) -> nvme_command_ertr::future<> {
    if (!id_namespace_data.nlbaf) {
      logger().info("the device does not support end to end data protection,\
	mkfs() will be done without this functionality.");
      return nvme_command_ertr::now();
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
      logger().info("the device does not support end to end data protection,\
	mkfs() will be done without this functionality.");
      return nvme_command_ertr::now();
    }
    return get_nsid(device
    ).safe_then([this, i=lba_format_index](auto nsid) {
      return seastar::do_with(
	nvme_admin_command_t(),
	[this, nsid=nsid, i=i] (auto &cmd) {
	cmd.common.opcode = nvme_admin_command_t::OPCODE_FORMAT_NVM;
	cmd.common.nsid = nsid;
	// TODO: configure other protect information types (2 or 3) see above
	cmd.format.pi = nvme_format_nvm_command_t::PROTECT_INFORMATION_TYPE_2;
	cmd.format.lbaf = i;
	return pass_admin(cmd, device
	).safe_then([this](auto ret) {
	  if (ret != 0) {
	    logger().error(
	      "formt nvm command to use end-to-end-protection fails : {}", ret);
	    ceph_abort();
	  }
	  return identify_namespace(device
	  ).safe_then([this] (auto id_namespace_data) -> nvme_command_ertr::future<> {
	    ceph_assert(id_namespace_data.dps.protection_type ==
	       nvme_format_nvm_command_t::PROTECT_INFORMATION_TYPE_2);
	    super.set_end_to_end_data_protection();
	    return nvme_command_ertr::now();
	  });
	});
      });
    });
  }).handle_error(crimson::ct_error::input_output_error::handle([]{
    logger().info("the device does not support identify namespace command");
    return nvme_command_ertr::now();
  }), crimson::ct_error::pass_further_all{});
}

nvme_command_ertr::future<> NVMeBlockDevice::initialize_nvme_features() {
  if (!crimson::common::get_conf<bool>("seastore_disable_end_to_end_data_protection")) {
    return try_enable_end_to_end_protection();
  }
  return nvme_command_ertr::now();
}

write_ertr::future<> NVMeBlockDevice::nvme_write(
  uint64_t offset, size_t len, void *buffer_ptr) {
  return seastar::do_with(
    nvme_io_command_t(),
    [this, offset, len, buffer_ptr] (auto &cmd) {
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
    return pass_through_io(cmd
    ).safe_then([] (auto ret) {
      if (ret != 0) {
	logger().error(
	  "write nvm command with checksum offload fails : {}", ret);
	ceph_abort();
      }
      return nvme_command_ertr::now();
    });
  });
}

read_ertr::future<> NVMeBlockDevice::nvme_read(
  uint64_t offset, size_t len, void *buffer_ptr) {
  return seastar::do_with(
    nvme_io_command_t(),
    [this, offset, len, buffer_ptr] (auto &cmd) {
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
    return pass_through_io(cmd
    ).safe_then([] (auto ret) {
      if (ret != 0) {
	logger().error(
	  "read nvm command with checksum offload fails : {}", ret);
	ceph_abort();
      }
      return nvme_command_ertr::now();
    });
  });
}

}
