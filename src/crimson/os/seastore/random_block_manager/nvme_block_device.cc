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

namespace crimson::os::seastore::random_block_device {
#include "crimson/os/seastore/logging.h"
SET_SUBSYS(seastore_device);

RBMDevice::mkfs_ret RBMDevice::mkfs(device_config_t config) {
  LOG_PREFIX(RBMDevice::mkfs);
  return stat_device(
  ).handle_error(
    mkfs_ertr::pass_further{},
    crimson::ct_error::assert_all{
    "Invalid error stat_device in RBMDevice::mkfs"}
  ).safe_then([this, FNAME, config=std::move(config)](auto st) {
    super.block_size = st.block_size;
    super.size = st.size;

    super.feature |= RBM_BITMAP_BLOCK_CRC;
    super.config = std::move(config);
    DEBUG("super {} ", super);
    // write super block
    return write_rbm_header(
    ).safe_then([] {
      return mkfs_ertr::now();
    }).handle_error(
      mkfs_ertr::pass_further{},
      crimson::ct_error::assert_all{
      "Invalid error write_rbm_header in RBMDevice::mkfs"
    });
  });
}

write_ertr::future<> RBMDevice::write_rbm_header()
{
  bufferlist meta_b_header;
  super.crc = 0;
  encode(super, meta_b_header);
  // If NVMeDevice supports data protection, CRC for checksum is not required
  // NVMeDevice is expected to generate and store checksum internally.
  // CPU overhead for CRC might be saved.
  if (is_data_protection_enabled()) {
    super.crc = -1;
  } else {
    super.crc = meta_b_header.crc32c(-1);
  }

  bufferlist bl;
  encode(super, bl);
  auto iter = bl.begin();
  auto bp = bufferptr(ceph::buffer::create_page_aligned(super.block_size));
  assert(bl.length() < super.block_size);
  iter.copy(bl.length(), bp.c_str());

  return write(RBM_START_ADDRESS, bp);
}

read_ertr::future<rbm_metadata_header_t> RBMDevice::read_rbm_header(
  rbm_abs_addr addr)
{
  LOG_PREFIX(RBMDevice::read_rbm_header);
  bufferptr bptr =
    bufferptr(ceph::buffer::create_page_aligned(RBM_SUPERBLOCK_SIZE));
  bptr.zero();
  return read(
    addr,
    bptr
  ).safe_then([length=bptr.length(), this, bptr, FNAME]()
    -> read_ertr::future<rbm_metadata_header_t> {
    bufferlist bl;
    bl.append(bptr);
    auto p = bl.cbegin();
    rbm_metadata_header_t super_block;
    try {
      decode(super_block, p);
    }
    catch (ceph::buffer::error& e) {
      DEBUG("read_rbm_header: unable to decode rbm super block {}",
	    e.what());
      return crimson::ct_error::enoent::make();
    }
    checksum_t crc = super_block.crc;
    bufferlist meta_b_header;
    super_block.crc = 0;
    encode(super_block, meta_b_header);
    assert(ceph::encoded_sizeof<rbm_metadata_header_t>(super_block) <
	super_block.block_size);

    // Do CRC verification only if data protection is not supported.
    if (is_data_protection_enabled() == false) {
      if (meta_b_header.crc32c(-1) != crc) {
	DEBUG("bad crc on super block, expected {} != actual {} ",
	      meta_b_header.crc32c(-1), crc);
	return crimson::ct_error::input_output_error::make();
      }
    } else {
      ceph_assert_always(crc == (checksum_t)-1);
    }
    super_block.crc = crc;
    super = super_block;
    DEBUG("got {} ", super);
    return read_ertr::future<rbm_metadata_header_t>(
      read_ertr::ready_future_marker{},
      super_block
    );
  });
}

}

namespace crimson::os::seastore::random_block_device::nvme {

open_ertr::future<> NVMeBlockDevice::open(
  const std::string &in_path,
  seastar::open_flags mode) {
  return seastar::do_with(in_path, [this, mode](auto& in_path) {
    return seastar::file_stat(in_path).then([this, mode, in_path](auto stat) {
      super.size = stat.size;
      super.block_size = stat.block_size;
      return seastar::open_file_dma(in_path, mode).then([=, this](auto file) {
        device = file;
        logger().debug("open");
        // Get SSD's features from identify_controller and namespace command.
        // Do identify_controller first, and then identify_namespace.
        return identify_controller().safe_then([this, in_path, mode](
          auto id_controller_data) {
          support_multistream = id_controller_data.oacs.support_directives;
          if (support_multistream) {
            stream_id_count = WRITE_LIFE_MAX;
          }
          awupf = id_controller_data.awupf + 1;
          return identify_namespace().safe_then([this, in_path, mode] (
            auto id_namespace_data) {
            // LBA format provides LBA size which is power of 2. LBA is the
            // minimum size of read and write.
            super.block_size = (1 << id_namespace_data.lbaf0.lbads);
            atomic_write_unit = awupf * super.block_size;
            data_protection_type = id_namespace_data.dps.protection_type;
            data_protection_enabled = (data_protection_type > 0);
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
      io_device[stream_index_to_open] = file;
      return io_device[stream_index_to_open].fcntl(
        F_SET_FILE_RW_HINT,
        (uintptr_t)&stream_index_to_open).then([this](auto ret) {
        stream_index_to_open++;
        return seastar::now();
      });
    });
  });
}

write_ertr::future<> NVMeBlockDevice::write(
  uint64_t offset,
  bufferptr &bptr,
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
}

read_ertr::future<> NVMeBlockDevice::read(
  uint64_t offset,
  bufferptr &bptr) {
  logger().debug(
      "block: read offset {} len {}",
      offset,
      bptr.length());
  auto length = bptr.length();

  assert((length % super.block_size) == 0);

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
  return device.close().then([this]() {
    return seastar::do_for_each(io_device, [](auto target_device) {
      return target_device.close();
    });
  });
}

nvme_command_ertr::future<nvme_identify_controller_data_t>
NVMeBlockDevice::identify_controller() {
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

discard_ertr::future<> NVMeBlockDevice::discard(uint64_t offset, uint64_t len) {
  return device.discard(offset, len);
}

nvme_command_ertr::future<nvme_identify_namespace_data_t>
NVMeBlockDevice::identify_namespace() {
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

nvme_command_ertr::future<int> NVMeBlockDevice::get_nsid() {
  return device.ioctl(NVME_IOCTL_ID, nullptr);
}

nvme_command_ertr::future<int> NVMeBlockDevice::pass_admin(
  nvme_admin_command_t& admin_cmd) {
  return device.ioctl(NVME_IOCTL_ADMIN_CMD, &admin_cmd).handle_exception(
    [](auto e)->nvme_command_ertr::future<int> {
      logger().error("pass_admin: ioctl failed");
      return crimson::ct_error::input_output_error::make();
    });
}

nvme_command_ertr::future<int> NVMeBlockDevice::pass_through_io(
  nvme_io_command_t& io_cmd) {
  return device.ioctl(NVME_IOCTL_IO_CMD, &io_cmd);
}

}

namespace crimson::os::seastore::random_block_device {

EphemeralRBMDeviceRef create_test_ephemeral(uint64_t journal_size, uint64_t data_size) {
  return EphemeralRBMDeviceRef(
    new EphemeralRBMDevice(journal_size + data_size + 
	random_block_device::RBMDevice::get_journal_start(),
	EphemeralRBMDevice::TEST_BLOCK_SIZE));
}

open_ertr::future<> EphemeralRBMDevice::open(
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

write_ertr::future<> EphemeralRBMDevice::write(
  uint64_t offset,
  bufferptr &bptr,
  uint16_t stream) {
  ceph_assert(buf);
  logger().debug(
    "EphemeralRBMDevice: write offset {} len {}",
    offset,
    bptr.length());

  ::memcpy(buf + offset, bptr.c_str(), bptr.length());

  return write_ertr::now();
}

read_ertr::future<> EphemeralRBMDevice::read(
  uint64_t offset,
  bufferptr &bptr) {
  ceph_assert(buf);
  logger().debug(
    "EphemeralRBMDevice: read offset {} len {}",
    offset,
    bptr.length());

  bptr.copy_in(0, bptr.length(), buf + offset);
  return read_ertr::now();
}

Device::close_ertr::future<> EphemeralRBMDevice::close() {
  logger().debug(" close ");
  return close_ertr::now();
}

write_ertr::future<> EphemeralRBMDevice::writev(
  uint64_t offset,
  ceph::bufferlist bl,
  uint16_t stream) {
  ceph_assert(buf);
  logger().debug(
    "EphemeralRBMDevice: write offset {} len {}",
    offset,
    bl.length());

  bl.begin().copy(bl.length(), buf + offset);
  return write_ertr::now();
}

}
