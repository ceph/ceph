//-*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/device.h"
#include "crimson/os/seastore/random_block_manager.h"

namespace crimson::os::seastore::rbm_device {
using read_ertr = crimson::errorator<
  crimson::ct_error::input_output_error,
  crimson::ct_error::invarg,
  crimson::ct_error::enoent,
  crimson::ct_error::erange>;

using write_ertr = crimson::errorator<
  crimson::ct_error::input_output_error,
  crimson::ct_error::invarg,
  crimson::ct_error::ebadf,
  crimson::ct_error::enospc>;

using open_ertr = crimson::errorator<
  crimson::ct_error::input_output_error,
  crimson::ct_error::invarg,
  crimson::ct_error::enoent>;

using nvme_command_ertr = crimson::errorator<
  crimson::ct_error::input_output_error>;

using discard_ertr = crimson::errorator<
  crimson::ct_error::input_output_error>;

/*
 * Interface between NVMe SSD and its user.
 *
 * RBMDevice provides not only the basic APIs for IO, but also helper APIs
 * to accelerate SSD IO performance and reduce system overhead. By aggresively
 * utilizing and abstract useful features of latest NVMe SSD, it helps user ease
 * to get high performance of NVMe SSD and low system overhead.
 *
 * Various implementations with different interfaces such as POSIX APIs, Seastar,
 * and SPDK, are available.
 */
class RBMDevice : public Device {
public:
  using Device::read;
  read_ertr::future<> read(
    paddr_t addr,
    size_t len,
    ceph::bufferptr &out) final {
    uint64_t rbm_addr = convert_paddr_to_abs_addr(addr);
    return read(rbm_addr, out);
  }
protected:
  uint64_t size = 0;

  // LBA Size
  uint64_t block_size = 4096;

  uint64_t write_granularity = 4096;
  uint64_t write_alignment = 4096;
  uint32_t atomic_write_unit = 4096;

  bool data_protection_enabled = false;
  device_id_t device_id;
  seastore_meta_t meta;
  secondary_device_set_t devices;
public:
  RBMDevice() {}
  virtual ~RBMDevice() = default;

  template <typename T>
  static std::unique_ptr<T> create() {
    return std::make_unique<T>();
  }

  device_id_t get_device_id() const {
    return device_id;
  }
  void set_device_id(device_id_t id) {
    device_id = id;
  }

  magic_t get_magic() const final {
    return magic_t();
  }

  virtual device_type_t get_device_type() const {
    return device_type_t::RANDOM_BLOCK;
  }

  const seastore_meta_t &get_meta() const final {
    return meta;
  }

  secondary_device_set_t& get_secondary_devices() final {
    return devices;
  }

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

  std::size_t get_size() const { return size; }
  seastore_off_t get_block_size() const { return block_size; }

  uint64_t get_preffered_write_granularity() const { return write_granularity; }
  uint64_t get_preffered_write_alignment() const { return write_alignment; }

  uint64_t get_atomic_write_unit() const { return atomic_write_unit; }

  virtual read_ertr::future<> read(
    uint64_t offset,
    bufferptr &bptr) = 0;

  /*
   * Multi-stream write
   *
   * Give hint to device about classification of data whose life time is similar
   * with each other. Data with same stream value will be managed together in
   * SSD for better write performance.
   */
  virtual write_ertr::future<> write(
    uint64_t offset,
    bufferptr &bptr,
    uint16_t stream = 0) = 0;

  virtual discard_ertr::future<> discard(
    uint64_t offset,
    uint64_t len) { return seastar::now(); }

  virtual open_ertr::future<> open(
      const std::string& path,
      seastar::open_flags mode) = 0;

  virtual write_ertr::future<> writev(
    uint64_t offset,
    ceph::bufferlist bl,
    uint16_t stream = 0) = 0;

  /*
   * End-to-End Data Protection
   *
   * NVMe device keeps track of data integrity similar with checksum. Client can
   * offload checksuming to NVMe device to reduce its CPU utilization. If data
   * protection is enabled, checksum is calculated on every write and used to
   * verify data on every read.
   */
   bool is_data_protection_enabled() const { return data_protection_enabled; }

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

  /*
   * Predictable Latency
   *
   * NVMe device can guarantee IO latency within pre-defined time window. This
   * functionality will be analyzed soon.
   */
};
}
