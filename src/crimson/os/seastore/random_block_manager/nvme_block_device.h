//-*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <memory>
#include <vector>

#include <seastar/core/file.hh>
#include <linux/nvme_ioctl.h>

#include "crimson/osd/exceptions.h"
#include "crimson/common/layout.h"
#include "rbm_device.h"

namespace ceph {
  namespace buffer {
    class bufferptr;
  }
}

namespace crimson::os::seastore::random_block_device::nvme {
/*
 * NVMe protocol structures (nvme_XX, identify_XX)
 *
 * All structures relative to NVMe protocol are following NVMe protocol v1.4
 * (latest). NVMe is protocol for fast interfacing between user and SSD device.
 * We selectively adopted features among various NVMe features to ease
 * implementation. And also, NVMeBlockDevice provides generic command submission
 * APIs for IO and Admin commands. Please use pass_through_io() and pass_admin()
 * to do it.
 *
 * For more information about NVMe protocol, refer https://nvmexpress.org/
 */
struct nvme_identify_command_t {
  uint32_t common_dw[10];

  uint32_t cns : 8;
  uint32_t reserved : 8;
  uint32_t cnt_id : 16;

  static const uint8_t CNS_NAMESPACE = 0x00;
  static const uint8_t CNS_CONTROLLER = 0x01;
};

struct nvme_admin_command_t {
  union {
    nvme_passthru_cmd common;
    nvme_identify_command_t identify;
  };

  static const uint8_t OPCODE_IDENTIFY = 0x06;
};

// Optional Admin Command Support (OACS)
// Indicates optional commands are supported by SSD or not 
struct oacs_t {
  uint16_t unused : 5;
  uint16_t support_directives : 1; // Support multi-stream
  uint16_t unused2 : 10;
};

struct nvme_identify_controller_data_t {
  union {
    struct {
      uint8_t unused[256];  // [255:0]
      oacs_t oacs;          // [257:256]
      uint8_t unused2[270]; // [527:258]
      uint16_t awupf;       // [529:528]
    };
    uint8_t raw[4096];
  };
};

// End-to-end Data Protection Capabilities (DPC)
// Indicates type of E2E data protection supported by SSD
struct dpc_t {
  uint8_t support_type1 : 1;
  uint8_t support_type2 : 1;
  uint8_t support_type3 : 1;
  uint8_t support_first_meta : 1;
  uint8_t support_last_meta : 1;
  uint8_t reserved : 3;
};

// End-to-end Data Protection Type Settings (DPS)
// Indicates enabled type of E2E data protection
struct dps_t {
  uint8_t protection_type : 3;
  uint8_t protection_info : 1;
  uint8_t reserved : 4;
};

// Namespace Features (NSFEAT)
// Indicates features of namespace
struct nsfeat_t {
  uint8_t thinp : 1;
  uint8_t nsabp : 1;
  uint8_t dae : 1;
  uint8_t uid_reuse : 1;
  uint8_t opterf : 1; // Support NPWG, NPWA
  uint8_t reserved : 3;
};

// LBA Format (LBAF)
// Indicates LBA format (metadata size, data size, performance)
struct lbaf_t {
  uint32_t ms : 16;
  uint32_t lbads : 8;
  uint32_t rp : 2;
  uint32_t reserved : 6;
};

struct nvme_identify_namespace_data_t {
  union {
    struct {
      uint8_t unused[24];   // [23:0]
      nsfeat_t nsfeat;      // [24]
      uint8_t unused2[3];   // [27:25]
      dpc_t dpc;            // [28]
      dps_t dps;            // [29]
      uint8_t unused3[34];  // [63:30]
      uint16_t npwg;        // [65:64]
      uint16_t npwa;        // [67:66]
      uint8_t unused4[60];  // [127:68]
      lbaf_t lbaf0;         // [131:128]
    };
    uint8_t raw[4096];
  };
};

struct nvme_rw_command_t {
  uint32_t common_dw[10];

  uint64_t s_lba;

  uint32_t nlb : 16; // 0's based value
  uint32_t reserved : 4;
  uint32_t d_type : 4;
  uint32_t reserved2 : 2;
  uint32_t prinfo_prchk : 3;
  uint32_t prinfo_pract : 1;
  uint32_t fua : 1;
  uint32_t lr : 1;

  uint32_t reserved3 : 16;
  uint32_t dspec : 16;

  static const uint32_t DTYPE_STREAM = 1;
};

struct nvme_io_command_t {
  union {
    nvme_passthru_cmd common;
    nvme_rw_command_t rw;
  };
  static const uint8_t OPCODE_WRITE = 0x01;
  static const uint8_t OPCODE_READ = 0x01;
};

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

  NVMeBlockDevice(std::string device_path) : device_path(device_path) {}
  ~NVMeBlockDevice() = default;

  open_ertr::future<> open(
    const std::string &in_path,
    seastar::open_flags mode) override;

  write_ertr::future<> write(
    uint64_t offset,
    bufferptr &&bptr,
    uint16_t stream = 0) override;

  using RBMDevice::read;
  read_ertr::future<> read(
    uint64_t offset,
    bufferptr &bptr) final;

  close_ertr::future<> close() override;

  discard_ertr::future<> discard(
    uint64_t offset,
    uint64_t len) override;

  mount_ret mount() final;

  mkfs_ret mkfs(device_config_t config) final;

  write_ertr::future<> writev(
    uint64_t offset,
    ceph::bufferlist bl,
    uint16_t stream = 0) final;

  stat_device_ret stat_device() final {
    return seastar::file_stat(device_path, seastar::follow_symlink::yes
    ).handle_exception([](auto e) -> stat_device_ret {
      return crimson::ct_error::input_output_error::make();
    }).then([this](auto stat) {
      return seastar::open_file_dma(
	device_path,
	seastar::open_flags::rw | seastar::open_flags::dsync
      ).then([this, stat](auto file) mutable {
	return file.size().then([this, stat, file](auto size) mutable {
	  stat.size = size;
	  return identify_namespace(file
	  ).safe_then([stat] (auto id_namespace_data) mutable {
	    // LBA format provides LBA size which is power of 2. LBA is the
	    // minimum size of read and write.
	    stat.block_size = (1 << id_namespace_data.lbaf0.lbads);
	    if (stat.block_size < RBM_SUPERBLOCK_SIZE) {
	      stat.block_size = RBM_SUPERBLOCK_SIZE;
	    } 
	    return stat_device_ret(
	      read_ertr::ready_future_marker{},
	      stat
	    );
	  }).handle_error(crimson::ct_error::input_output_error::handle(
	    [stat]{
	    return stat_device_ret(
	      read_ertr::ready_future_marker{},
	      stat
	    );
	  }), crimson::ct_error::pass_further_all{});
	}).safe_then([file](auto st) mutable {
	  return file.close(
	  ).then([st] {
	    return stat_device_ret(
	      read_ertr::ready_future_marker{},
	      st
	    );
	  });
	});
      });
    });
  }

  std::string get_device_path() const final {
    return device_path;
  }

  seastar::future<> start() final {
    return shard_devices.start(device_path);
  }

  seastar::future<> stop() final {
    return shard_devices.stop();
  }

  Device& get_sharded_device() final {
    return shard_devices.local();
  }

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
   * For passsing through nvme IO or Admin command to SSD
   * Caller can construct and execute its own nvme command
   */
  nvme_command_ertr::future<int> pass_admin(
    nvme_admin_command_t& admin_cmd, seastar::file f);
  nvme_command_ertr::future<int> pass_through_io(
    nvme_io_command_t& io_cmd);

  bool support_multistream = false;
  uint8_t data_protection_type = 0;

  /*
   * Predictable Latency
   *
   * NVMe device can guarantee IO latency within pre-defined time window. This
   * functionality will be analyzed soon.
   */

private:
  // identify_controller/namespace are used to get SSD internal information such
  // as supported features, NPWG and NPWA
  nvme_command_ertr::future<nvme_identify_controller_data_t> 
    identify_controller(seastar::file f);
  nvme_command_ertr::future<nvme_identify_namespace_data_t>
    identify_namespace(seastar::file f);
  nvme_command_ertr::future<int> get_nsid(seastar::file f);
  open_ertr::future<> open_for_io(
    const std::string& in_path,
    seastar::open_flags mode);

  seastar::file device;
  std::vector<seastar::file> io_device;
  uint32_t stream_index_to_open = WRITE_LIFE_NOT_SET;
  uint32_t stream_id_count = 1; // stream is disabled, defaultly.
  uint32_t awupf = 0;

  uint64_t write_granularity = 4096;
  uint64_t write_alignment = 4096;
  uint32_t atomic_write_unit = 4096;

  bool data_protection_enabled = false;
  std::string device_path;
  seastar::sharded<NVMeBlockDevice> shard_devices;
};

}
