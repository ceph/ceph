//-*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <list>
#include <optional>
#include <string>
#include <vector>

#include <seastar/core/file.hh>

#include "crimson/os/seastore/block_io_driver.h"
#include "rbm_device.h"        // RBM ertr aliases (nvme_command_ertr, ...)
#include "nvme_command.h"

namespace crimson::os::seastore::random_block_device::nvme {

/**
 * NVMeIODriver
 *
 * Tier-2 transport for the random-block (RBM) backend. It is a BlockIODriver
 * (so the bulk read/write/writev/readv data path is shared with the segmented
 * backend) plus the NVMe control plane that an NVMe SSD exposes:
 *
 *   - identify-derived capabilities (caps_t),
 *   - end-to-end data protection (PI) aware I/O,
 *   - write streams,
 *   - admin / IO command passthrough.
 *
 * Ownership boundary: the *driver* owns all device interaction (data and admin)
 * and the transient PI state used to choose plain DMA vs. PI passthrough. The
 * *device* (NVMeBlockDevice/RBMDevice) still owns the on-disk superblock, which
 * persists whether PI is enabled and the formatted NVMe block size; it pushes
 * that state into the driver via set_protection_info(). Nothing in the driver
 * reaches into RBMDevice::super, which is what lets a kernel and an SPDK
 * implementation share this interface.
 */
class NVMeIODriver : public crimson::os::seastore::BlockIODriver {
public:
  /// Raw identify-derived controller/namespace capabilities, valid after open().
  /// The device combines these with its own superblock block_size to compute
  /// write_granularity / write_alignment / atomic_write_unit (kept on the device
  /// so that math stays tied to the on-disk block size, unchanged from today).
  struct caps_t {
    bool ctrl_identified = false;        ///< identify_controller succeeded
    bool ns_identified = false;          ///< identify_namespace succeeded
    uint32_t awupf = 0;                  ///< AWUPF + 1 (i.e. already 1's based)
    bool opterf = false;                 ///< namespace supports NPWG/NPWA
    uint16_t npwg = 0;                   ///< preferred write granularity (0's based)
    uint16_t npwa = 0;                   ///< preferred write alignment (0's based)
    bool supports_multistream = false;
    uint32_t stream_id_count = 1;        ///< 1 == multistream disabled
  };
  virtual const caps_t &get_caps() const = 0;

  /// NVMe namespace id, cached by identify_namespace()/open().
  virtual int get_namespace_id() const = 0;

  /**
   * Configure PI for subsequent I/O. PI is a device-wide mode: the device reads
   * it from the superblock (set at mkfs, validated at mount) and pushes it here.
   * When enabled, read()/write() submit PI-carrying NVMe commands using
   * nvme_block_size as the LBA unit; otherwise they use plain DMA.
   */
  virtual void set_protection_info(bool enabled, uint32_t nvme_block_size) = 0;

  /// Stream-aware write. stream is ignored when stream_id_count == 1. PI is
  /// applied internally per set_protection_info().
  virtual write_ertr::future<> write(
    device_id_t device_id,
    uint64_t offset,
    bufferptr &bptr,
    uint16_t stream) = 0;
  virtual write_ertr::future<> writev(
    device_id_t device_id,
    uint64_t offset,
    bufferlist &&bl,
    size_t block_size,
    uint16_t stream) = 0;

  // The BlockIODriver base write/writev (no stream) forward to the stream-aware
  // overloads with the default stream.
  write_ertr::future<> write(
    device_id_t device_id, uint64_t offset, bufferptr &bptr) final {
    return write(device_id, offset, bptr, /*stream=*/0);
  }
  write_ertr::future<> writev(
    device_id_t device_id, uint64_t offset,
    bufferlist &&bl, size_t block_size) final {
    return writev(device_id, offset, std::move(bl), block_size, /*stream=*/0);
  }

  /// NVMe identify, exposed so device-level logic (mkfs PI formatting, mount
  /// validation) can run without owning the transport.
  virtual seastar::future<std::optional<nvme_identify_controller_data_t>>
    identify_controller() = 0;
  virtual seastar::future<std::optional<nvme_identify_namespace_data_t>>
    identify_namespace() = 0;

  /// Raw NVMe passthrough for advanced features and PI formatting.
  virtual nvme_command_ertr::future<int> pass_admin(
    nvme_admin_command_t &admin_cmd) = 0;
  virtual nvme_command_ertr::future<int> pass_through_io(
    nvme_io_command_t &io_cmd) = 0;

  /// Data-health / recovery-level hooks (default no-ops on devices that lack
  /// the corresponding NVMe features).
  virtual nvme_command_ertr::future<std::list<uint64_t>> get_data_health() {
    return nvme_command_ertr::make_ready_future<std::list<uint64_t>>();
  }
  virtual nvme_command_ertr::future<> set_data_recovery_level(uint32_t level) {
    return nvme_command_ertr::now();
  }

  /// discard/trim the given device range.
  virtual discard_ertr::future<> discard(uint64_t offset, uint64_t len) = 0;
};
using NVMeIODriverRef = std::unique_ptr<NVMeIODriver>;

/**
 * make_nvme_io_driver
 *
 * RBM transport factory. Returns the kernel NVMe driver today; Phase 6 adds the
 * SPDK branch keyed on seastore_spdk_transport_id. Synchronous, non-blocking
 * construction (env init / probe is deferred to open()).
 */
NVMeIODriverRef make_nvme_io_driver(const std::string &path);

}
