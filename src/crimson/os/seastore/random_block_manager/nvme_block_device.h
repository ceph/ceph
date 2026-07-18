//-*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <memory>
#include <vector>

#include <seastar/core/file.hh>

#include "crimson/osd/exceptions.h"
#include "crimson/common/layout.h"
#include "rbm_device.h"
#include "nvme_command.h"
#include "nvme_io_driver.h"

namespace ceph {
  namespace buffer {
    class bufferptr;
  }
}

namespace crimson::os::seastore::random_block_device::nvme {

/*
 * Implementation of NVMeBlockDevice with POSIX APIs
 *
 * NVMeBlockDevice provides NVMe SSD interfaces through POSIX APIs which is
 * generally available at most operating environment.
 */
class NVMeBlockDevice : public RBMDevice {
public:

  /*
   * Service NVMe device relative size
   *
   * size : total size of device in byte.
   *
   * block_size : IO unit size in byte. Caller should follow every IO command
   * aligned with block size.
   *
   * preffered_write_granularity(PWG), preffered_write_alignment(PWA) : IO unit
   * size for write in byte. Caller should request every write IO sized multiple
   * times of PWG and aligned starting address by PWA. Available only if NVMe
   * Device supports NVMe protocol 1.4 or later versions.
   * atomic_write_unit : The maximum size of write whose atomicity is guranteed
   * by SSD even on power failure. The write equal to or smaller than 
   * atomic_write_unit does not require fsync().
   */

  NVMeBlockDevice(std::string device_path, store_index_t store_index = 0)
    : RBMDevice(store_index),
      device_path(device_path) {}
  ~NVMeBlockDevice() = default;

  ceph::unique_leakable_ptr<ceph::buffer::raw> alloc_io_buffer(
    size_t len) final {
    return driver ? driver->alloc_io_buffer(len) : ceph::buffer::create_page_aligned(len);
  }

  ceph::unique_leakable_ptr<ceph::buffer::raw> alloc_journal_md_buffer(
    size_t len) final {
    return (driver && driver->dma_passthrough())
      ? driver->alloc_io_buffer(len)
      : nullptr;
  }

  open_ertr::future<> open(
    const std::string &in_path,
    seastar::open_flags mode) override;

  write_ertr::future<> write(
    uint64_t offset,
    bufferptr bptr,
    uint16_t stream = 0) override;

  using RBMDevice::read;
  read_ertr::future<> read(
    uint64_t offset,
    bufferptr &bptr) final;
  read_ertr::future<> _readv(
    uint64_t offset,
    std::vector<bufferptr> ptrs) final;

  close_ertr::future<> close() override;

  discard_ertr::future<> discard(
    uint64_t offset,
    uint64_t len) override;

  mount_ret mount() final;

  nvme_command_ertr::future<> initialize_nvme_features() final;

  mkfs_ret mkfs(device_config_t config) final;

  write_ertr::future<> writev(
    uint64_t offset,
    ceph::bufferlist bl,
    uint16_t stream = 0) final;

  stat_device_ret stat_device() final {
    // sizing only: a throwaway driver does the open + identify and reports
    // {size, block_size}, then is dropped.
    auto io_driver = make_nvme_io_driver(device_path);
    auto stat = co_await io_driver->open(
      device_path, seastar::open_flags::rw | seastar::open_flags::dsync
    ).handle_error(crimson::ct_error::all_same_way([]() -> stat_device_ret {
      return crimson::ct_error::input_output_error::make();
    }));
    co_await io_driver->close().handle_error(
      crimson::ct_error::assert_all("Invalid error in stat_device close"));
    // driver->open() already derives block_size from the LBA format and clamps
    // it to RBM_SUPERBLOCK_SIZE.
    co_return std::move(stat);
  }

  std::string get_device_path() const final {
    return device_path;
  }

  seastar::future<> start(uint32_t shard_nums) final;

  seastar::future<> stop() final;

  Device& get_sharded_device(store_index_t store_index = 0) final;

  uint64_t get_preffered_write_granularity() const { return write_granularity; }
  uint64_t get_preffered_write_alignment() const { return write_alignment; }
  uint64_t get_atomic_write_unit() const { return atomic_write_unit; }
  /*
   * End-to-End Data Protection
   *
   * NVMe device keeps track of data integrity similar with checksum. Client can
   * offload checksuming to NVMe device to reduce its CPU utilization. If data
   * protection is enabled, checksum is calculated on every write and used to
   * verify data on every read.
   */
  nvme_command_ertr::future<> try_enable_end_to_end_protection();

  /*
   * Data Health
   *
   * Returns list of LBAs which have almost corrupted data. Data of the LBAs
   * will be corrupted very soon. Caller can overwrite, unmap or refresh data to
   * protect data
   */
   virtual nvme_command_ertr::future<std::list<uint64_t>> get_data_health() {
     std::list<uint64_t> fragile_lbas;
     return nvme_command_ertr::future<std::list<uint64_t>>(
	nvme_command_ertr::ready_future_marker{},
	fragile_lbas
     );
   }

  /*
   * Recovery Level
   *
   * Regulate magnitude of SSD-internal data recovery. Caller can get good read
   * latency with lower magnitude.
   */
   virtual nvme_command_ertr::future<> set_data_recovery_level(
     uint32_t level) { return nvme_command_ertr::now(); }

  bool support_multistream = false;

  /*
   * Predictable Latency
   *
   * NVMe device can guarantee IO latency within pre-defined time window. This
   * functionality will be analyzed soon.
   */

private:
  // The NVMe transport (kernel today, SPDK later). Owns the device handles and
  // the NVMe control plane (identify, PI-aware I/O, streams, passthrough).
  NVMeIODriverRef driver;

  uint32_t awupf = 0;

  uint64_t write_granularity = 4096;
  uint64_t write_alignment = 4096;
  uint32_t atomic_write_unit = 4096;

  std::string device_path;

  class MultiShardDevices {
    public:
      std::vector<std::unique_ptr<NVMeBlockDevice>> mshard_devices;

    public:
    MultiShardDevices(size_t count,
                      const std::string path)
    : mshard_devices() {
      mshard_devices.reserve(count);
      for (size_t store_index = 0; store_index < count; ++store_index) {
        mshard_devices.emplace_back(std::make_unique<NVMeBlockDevice>(
          path, store_index));
      }
    }
    ~MultiShardDevices() {
     mshard_devices.clear();
    }
  };
  seastar::sharded<MultiShardDevices> shard_devices;
};

}
