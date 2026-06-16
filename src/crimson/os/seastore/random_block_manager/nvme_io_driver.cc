//-*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <strings.h>
#include <stdexcept>

#include <fcntl.h>
#include <seastar/coroutine/parallel_for_each.hh>

#include "crimson/common/log.h"
#include "crimson/common/config_proxy.h"
#include "crimson/common/errorator-utils.h"

#include "include/buffer.h"
#include "nvme_io_driver.h"
#include "crimson/os/seastore/logging.h"
#ifdef HAVE_SPDK
#include "spdk_nvme_io_driver.h"
#endif

SET_SUBSYS(seastore_device);

namespace crimson::os::seastore::random_block_device::nvme {

/**
 * KernelNVMeIODriver
 *
 * Kernel/io_uring NVMe transport. The data path is plain dma_read/dma_write
 * against seastar::file handles (device for reads, io_device[stream] for
 * writes); the control plane uses NVME_IOCTL_* on those handles. The PI path
 * (nvme_read/nvme_write) submits passthrough commands carrying protection
 * information. This is the code that previously lived in NVMeBlockDevice,
 * decoupled from the RBM superblock: PI state and the formatted NVMe block size
 * are pushed in via set_protection_info().
 */
class KernelNVMeIODriver final : public NVMeIODriver {
  seastar::file device;
  std::vector<seastar::file> io_device;
  caps_t caps;

  bool pi_enabled = false;
  uint32_t nvme_block_size = 0;
  int namespace_id = 0;

  uint32_t stream_index_to_open = WRITE_LIFE_NOT_SET;
  uint32_t stream_id_count = 1; // stream is disabled, defaultly.

  open_ertr::future<> open_for_io(
    const std::string &in_path, seastar::open_flags mode)
  {
    io_device.resize(stream_id_count);
    for (auto &target_device : io_device) {
      auto file = co_await seastar::open_file_dma(in_path, mode);
      assert(io_device.size() > stream_index_to_open);
      target_device = std::move(file);
    }
  }

  nvme_command_ertr::future<int> get_nsid() {
    auto ret = co_await device.ioctl(NVME_IOCTL_ID, nullptr
    ).handle_exception(
      [](auto e)->nvme_command_ertr::future<int> {
      LOG_PREFIX(KernelNVMeIODriver::get_nsid);
      ERROR("get_nsid: ioctl failed {}", e);
      return crimson::ct_error::input_output_error::make();
    });
    co_return ret;
  }

public:
  KernelNVMeIODriver() = default;

  const caps_t &get_caps() const final {
    return caps;
  }

  int get_namespace_id() const final {
    return namespace_id;
  }

  void set_protection_info(bool enabled, uint32_t block_size) final {
    pi_enabled = enabled;
    nvme_block_size = block_size;
  }

  open_ertr::future<seastar::stat_data> open(
    const std::string &in_path,
    seastar::open_flags mode) final
  {
    LOG_PREFIX(KernelNVMeIODriver::open);
    auto file = co_await seastar::open_file_dma(in_path, mode);
    device = std::move(file);
    // Get SSD's features from identify_controller and namespace command.
    // Do identify_controller first, and then identify_namespace.
    auto id_ctr_data = co_await identify_controller();
    std::optional<nvme_identify_namespace_data_t> id_ns_data;
    if (id_ctr_data) {
      // TODO: enable multi-stream if the nvme device supports
      caps.ctrl_identified = true;
      caps.awupf = id_ctr_data->awupf + 1;
      id_ns_data = co_await identify_namespace();
      if (id_ns_data) {
        caps.ns_identified = true;
        if (id_ns_data->nsfeat.opterf == 1) {
          // NPWG and NPWA is 0'based value
          caps.opterf = true;
          caps.npwg = id_ns_data->npwg;
          caps.npwa = id_ns_data->npwa;
        }
      } else {
        DEBUG("identify_namespace failed.");
      }
    } else {
      DEBUG("identify_controller failed.");
    }

    seastar::stat_data stat = co_await seastar::file_stat(
      in_path, seastar::follow_symlink::yes);
    stat.size = co_await device.size();
    // LBA format provides the LBA size (power of 2); fall back to the RBM
    // superblock size when identify is unavailable.
    if (id_ns_data) {
      stat.block_size = (1 << (*id_ns_data).lbaf[0].lbads);
    }
    if (stat.block_size < RBM_SUPERBLOCK_SIZE) {
      stat.block_size = RBM_SUPERBLOCK_SIZE;
    }

    co_await open_for_io(in_path, mode);
    co_return stat;
  }

  close_ertr::future<> close() final {
    LOG_PREFIX(KernelNVMeIODriver::close);
    DEBUG("close");
    stream_index_to_open = WRITE_LIFE_NOT_SET;
    co_await device.close();
    for (auto &target_device : io_device) {
      co_await target_device.close();
    }
  }

  seastar::future<std::optional<nvme_identify_controller_data_t>>
  identify_controller() final {
    nvme_admin_command_t admin_command;
    nvme_identify_controller_data_t data;
    admin_command.common.opcode = nvme_admin_command_t::OPCODE_IDENTIFY;
    admin_command.common.addr = (uint64_t)&data;
    admin_command.common.data_len = sizeof(data);
    admin_command.identify.cns = nvme_identify_command_t::CNS_CONTROLLER;
    auto ret = co_await pass_admin(admin_command
    ).handle_error(crimson::ct_error::input_output_error::handle([] {
      return seastar::make_ready_future<int>(-1);
    }));
    if (ret < 0) {
      co_return std::nullopt;
    }
    co_return std::move(data);
  }

  seastar::future<std::optional<nvme_identify_namespace_data_t>>
  identify_namespace() final {
    auto nsid = co_await get_nsid(
    ).handle_error(crimson::ct_error::input_output_error::handle([] {
      return seastar::make_ready_future<int>(-1);
    }));
    if (nsid < 0) {
      co_return std::nullopt;
    }
    namespace_id = nsid;
    nvme_admin_command_t admin_command;
    nvme_identify_namespace_data_t data;
    admin_command.common.opcode = nvme_admin_command_t::OPCODE_IDENTIFY;
    admin_command.common.addr = (uint64_t)&data;
    admin_command.common.data_len = sizeof(data);
    admin_command.common.nsid = nsid;
    admin_command.identify.cns = nvme_identify_command_t::CNS_NAMESPACE;

    auto ret = co_await pass_admin(admin_command
    ).handle_error(crimson::ct_error::input_output_error::handle([] {
      return seastar::make_ready_future<int>(-1);
    }));
    if (ret < 0) {
      co_return std::nullopt;
    }
    co_return std::move(data);
  }

  nvme_command_ertr::future<int> pass_admin(
    nvme_admin_command_t &admin_cmd) final {
    auto ret = co_await device.ioctl(NVME_IOCTL_ADMIN_CMD, &admin_cmd
    ).handle_exception(
      [](auto e)->nvme_command_ertr::future<int> {
      LOG_PREFIX(KernelNVMeIODriver::pass_admin);
      ERROR("pass_admin: ioctl failed {}", e);
      return crimson::ct_error::input_output_error::make();
    });
    co_return ret;
  }

  nvme_command_ertr::future<int> pass_through_io(
    nvme_io_command_t &io_cmd) final {
    auto ret = co_await device.ioctl(NVME_IOCTL_IO_CMD, &io_cmd
    ).handle_exception(
      [](auto e)->nvme_command_ertr::future<int> {
      LOG_PREFIX(KernelNVMeIODriver::pass_through_io);
      ERROR("pass_through_io: ioctl failed {}", e);
      return crimson::ct_error::input_output_error::make();
    });
    co_return ret;
  }

  discard_ertr::future<> discard(uint64_t offset, uint64_t len) final {
    co_return co_await device.discard(offset, len);
  }

  // ---- data path -------------------------------------------------------

  write_ertr::future<> nvme_write(
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
    ceph_assert(nvme_block_size > 0);
    auto lba_shift = ffsll(nvme_block_size) - 1;
    cmd.rw.s_lba = offset >> lba_shift;
    cmd.rw.nlb = (len >> lba_shift) - 1;
    auto ret = co_await pass_through_io(cmd);
    if (ret != 0) {
      LOG_PREFIX(KernelNVMeIODriver::nvme_write);
      ERROR("write nvm command with checksum offload fails : {}", ret);
      ceph_abort();
    }
  }

  read_ertr::future<> nvme_read(
    uint64_t offset, size_t len, void *buffer_ptr) {
    nvme_io_command_t cmd;
    cmd.common.opcode = nvme_io_command_t::OPCODE_READ;
    cmd.common.nsid = namespace_id;
    cmd.common.data_len = len;
    cmd.rw.prinfo_pract = nvme_rw_command_t::PROTECT_INFORMATION_ACTION_ENABLE;
    cmd.rw.prinfo_prchk = nvme_rw_command_t::PROTECT_INFORMATION_CHECK_GUARD;
    cmd.common.addr = (__u64)(uintptr_t)buffer_ptr;
    ceph_assert(nvme_block_size > 0);
    auto lba_shift = ffsll(nvme_block_size) - 1;
    cmd.rw.s_lba = offset >> lba_shift;
    cmd.rw.nlb = (len >> lba_shift) - 1;
    auto ret = co_await pass_through_io(cmd);
    if (ret != 0) {
      LOG_PREFIX(KernelNVMeIODriver::nvme_read);
      ERROR("read nvm command with checksum offload fails : {}", ret);
      ceph_abort();
    }
  }

  read_ertr::future<> nvme_readv(
    uint64_t offset, std::vector<bufferptr> ptrs) {
    struct io_t {
      uint64_t offset = 0;
      bufferptr ptr;
    };
    std::vector<io_t> iov;
    size_t off = 0;
    for (auto &ptr : ptrs) {
      auto len = ptr.length();
      iov.emplace_back(offset + off, std::move(ptr));
      off += len;
    }
    return seastar::do_with(
      std::move(iov),
      [this](auto &iov) {
      return read_ertr::parallel_for_each(
        iov,
        [this](auto &io) {
        return nvme_read(io.offset, io.ptr.length(), io.ptr.c_str());
      });
    });
  }

  read_ertr::future<> read(
    device_id_t device_id,
    uint64_t offset,
    size_t len,
    bufferptr &bptr) final {
    LOG_PREFIX(KernelNVMeIODriver::read);
    DEBUG("block: read offset {} len {}", offset, len);
    if (len == 0) {
      co_return;
    }
    if (pi_enabled) {
      co_await nvme_read(offset, len, bptr.c_str());
      co_return;
    }
    auto ret = co_await device.dma_read(offset, bptr.c_str(), len
    ).handle_exception(
      [FNAME](auto e) -> read_ertr::future<size_t> {
      ERROR("read: dma_read got error{}", e);
      return crimson::ct_error::input_output_error::make();
    });
    if (ret != len) {
      ERROR("read: dma_read got error with not proper length");
      co_await read_ertr::future<>(
        crimson::ct_error::input_output_error::make());
    }
  }

  read_ertr::future<> readv(
    device_id_t device_id,
    uint64_t offset,
    std::vector<bufferptr> ptrs) final {
    LOG_PREFIX(KernelNVMeIODriver::readv);
    DEBUG("block: read offset {}, {} buffers", offset, ptrs.size());
    if (ptrs.size() == 0) {
      return read_ertr::now();
    }
    if (pi_enabled) {
      return nvme_readv(offset, std::move(ptrs));
    }
    std::vector<iovec> iov;
    size_t length = 0;
    for (auto &ptr : ptrs) {
      length += ptr.length();
      iov.emplace_back(ptr.c_str(), ptr.length());
    }
    return device.dma_read(offset, std::move(iov)
    ).handle_exception(
      [FNAME](auto e) -> read_ertr::future<size_t> {
        ERROR("read: dma_read got error{}", e);
        return crimson::ct_error::input_output_error::make();
      }).then([length, FNAME](auto result) -> read_ertr::future<> {
        if (result != length) {
          ERROR("read: dma_read got error with not proper length");
          return crimson::ct_error::input_output_error::make();
        }
        return read_ertr::now();
      });
  }

  using NVMeIODriver::write;
  using NVMeIODriver::writev;

  write_ertr::future<> write(
    device_id_t device_id,
    uint64_t offset,
    bufferptr &bptr,
    uint16_t stream) final {
    LOG_PREFIX(KernelNVMeIODriver::write);
    auto length = bptr.length();
    DEBUG("block: write offset {} len {}", offset, length);
    uint16_t supported_stream = stream;
    if (stream >= stream_id_count) {
      supported_stream = WRITE_LIFE_NOT_SET;
    }
    if (pi_enabled) {
      co_await nvme_write(offset, length, bptr.c_str());
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
      co_await write_ertr::future<>(
        crimson::ct_error::input_output_error::make());
    }
  }

  write_ertr::future<> writev(
    device_id_t device_id,
    uint64_t offset,
    bufferlist &&bl,
    size_t block_size,
    uint16_t stream) final {
    LOG_PREFIX(KernelNVMeIODriver::writev);
    DEBUG("block: write offset {} len {}", offset, bl.length());
    uint16_t supported_stream = stream;
    if (stream >= stream_id_count) {
      supported_stream = WRITE_LIFE_NOT_SET;
    }
    if (pi_enabled) {
      co_await nvme_write(offset, bl.length(), bl.c_str());
      co_return;
    }
    bl.rebuild_aligned(block_size);
    auto iovs = bl.prepare_iovs();
    auto has_error = seastar::make_lw_shared<bool>(false);
    co_await seastar::coroutine::parallel_for_each(
      iovs,
      [this, supported_stream, offset, has_error, device_id, FNAME](auto &p)
    {
      auto off = offset + p.offset;
      auto len = p.length;
      auto &iov = p.iov;
      return io_device[supported_stream].dma_write(off, std::move(iov)
      ).handle_exception(
        [this, off, len, has_error, device_id, FNAME](auto e) -> seastar::future<size_t>
      {
        ERROR("{} poffset={}~{} dma_write got error -- {}",
          device_id_printer_t{device_id}, off, len, e);
        *has_error = true;
        return seastar::make_ready_future<size_t>(0);
      }).then([this, off, len, has_error, device_id, FNAME](size_t written) {
        if (written != len) {
          ERROR("{} poffset={}~{} dma_write len={} inconsistent",
            device_id_printer_t{device_id}, off, len, written);
          *has_error = true;
        }
        return seastar::now();
      });
    });
    if (*has_error) {
      co_await write_ertr::future<>(
        crimson::ct_error::input_output_error::make());
    }
  }
};

NVMeIODriverRef make_nvme_io_driver(const std::string &path)
{
  auto trid = crimson::common::local_conf().get_val<std::string>(
    "seastore_spdk_transport_id");
#ifdef HAVE_SPDK
  if (!trid.empty()) {
    return std::make_unique<SPDKNVMeIODriver>(trid);
  }
#else
  if (!trid.empty()) {
    throw std::invalid_argument(
      "seastore_spdk_transport_id is set but this build has WITH_SPDK=OFF");
  }
#endif
  // RBM is never ZBD, so no zoned guard is needed here.
  return std::make_unique<KernelNVMeIODriver>();
}

}
