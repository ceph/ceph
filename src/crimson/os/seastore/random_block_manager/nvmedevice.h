//-*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <memory>
#include <vector>
#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>
#include <linux/nvme_ioctl.h>
#include <libaio.h>

#include "crimson/osd/exceptions.h"
#include "crimson/common/layout.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/random_block_manager.h"
#include "crimson/os/seastore/device.h"

namespace ceph {
  namespace buffer {
    class bufferptr;
  }
}

namespace crimson::os::seastore::nvme_device {

// from blk/BlockDevice.h
#if defined(__linux__)
#if !defined(F_SET_FILE_RW_HINT)
#define F_LINUX_SPECIFIC_BASE 1024
#define F_SET_FILE_RW_HINT         (F_LINUX_SPECIFIC_BASE + 14)
#endif
// These values match Linux definition
// https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/tree/include/uapi/linux/fcntl.h#n56
#define  WRITE_LIFE_NOT_SET  	0 	// No hint information set
#define  WRITE_LIFE_NONE  	1       // No hints about write life time
#define  WRITE_LIFE_SHORT  	2       // Data written has a short life time
#define  WRITE_LIFE_MEDIUM  	3    	// Data written has a medium life time
#define  WRITE_LIFE_LONG  	4       // Data written has a long life time
#define  WRITE_LIFE_EXTREME  	5     	// Data written has an extremely long life time
#define  WRITE_LIFE_MAX  	6
#else
// On systems don't have WRITE_LIFE_* only use one FD
// And all files are created equal
#define  WRITE_LIFE_NOT_SET  	0 	// No hint information set
#define  WRITE_LIFE_NONE  	0       // No hints about write life time
#define  WRITE_LIFE_SHORT  	0       // Data written has a short life time
#define  WRITE_LIFE_MEDIUM  	0    	// Data written has a medium life time
#define  WRITE_LIFE_LONG  	0       // Data written has a long life time
#define  WRITE_LIFE_EXTREME  	0    	// Data written has an extremely long life time
#define  WRITE_LIFE_MAX  	1
#endif

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

struct io_context_t {
  iocb cb;
  bool done = false;
};

/*
 * Interface between NVMe SSD and its user.
 *
 * NVMeBlockDevice provides not only the basic APIs for IO, but also helper APIs
 * to accelerate SSD IO performance and reduce system overhead. By aggresively
 * utilizing and abstract useful features of latest NVMe SSD, it helps user ease
 * to get high performance of NVMe SSD and low system overhead.
 *
 * Various implementations with different interfaces such as POSIX APIs, Seastar,
 * and SPDK, are available.
 */
class NVMeBlockDevice : public Device {
public:
  using Device::read;
  read_ertr::future<> read (
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
  NVMeBlockDevice() {}
  virtual ~NVMeBlockDevice() = default;

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

  device_type_t get_device_type() const final {
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
   * For passsing through nvme IO or Admin command to SSD
   * Caller can construct and execute its own nvme command
   */
  virtual nvme_command_ertr::future<int> pass_through_io(
    nvme_io_command_t& command) { return seastar::make_ready_future<int>(0); }
  virtual nvme_command_ertr::future<int> pass_admin(
    nvme_admin_command_t& command) { return seastar::make_ready_future<int>(0); }

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

/*
 * Implementation of NVMeBlockDevice with POSIX APIs
 *
 * PosixNVMeDevice provides NVMe SSD interfaces through POSIX APIs which is
 * generally available at most operating environment.
 */
class PosixNVMeDevice : public NVMeBlockDevice {
public:
  PosixNVMeDevice() {}
  ~PosixNVMeDevice() = default;

  open_ertr::future<> open(
    const std::string &in_path,
    seastar::open_flags mode) override;

  write_ertr::future<> write(
    uint64_t offset,
    bufferptr &bptr,
    uint16_t stream = 0) override;

  using NVMeBlockDevice::read;
  read_ertr::future<> read(
    uint64_t offset,
    bufferptr &bptr) final;

  close_ertr::future<> close() override;

  discard_ertr::future<> discard(
    uint64_t offset,
    uint64_t len) override;

  mkfs_ret mkfs(device_config_t) final {
    return mkfs_ertr::now();
  }

  mount_ret mount() final {
    return mount_ertr::now();
  }

  write_ertr::future<> writev(
    uint64_t offset,
    ceph::bufferlist bl,
    uint16_t stream = 0) final;

  nvme_command_ertr::future<int> pass_admin(
    nvme_admin_command_t& admin_cmd) override;
  nvme_command_ertr::future<int> pass_through_io(
    nvme_io_command_t& io_cmd) override;

  bool support_multistream = false;
  uint8_t data_protection_type = 0;

private:
  // identify_controller/namespace are used to get SSD internal information such
  // as supported features, NPWG and NPWA
  nvme_command_ertr::future<nvme_identify_controller_data_t> identify_controller();
  nvme_command_ertr::future<nvme_identify_namespace_data_t> identify_namespace();
  nvme_command_ertr::future<int> get_nsid();
  open_ertr::future<> open_for_io(
    const std::string& in_path,
    seastar::open_flags mode);

  seastar::file device;
  std::vector<seastar::file> io_device;
  uint32_t stream_index_to_open = WRITE_LIFE_NOT_SET;
  uint32_t stream_id_count = 1; // stream is disabled, defaultly.
  uint32_t awupf = 0;
};


class TestMemory : public NVMeBlockDevice {
public:

  TestMemory(size_t size) : buf(nullptr), size(size) {}
  ~TestMemory() {
    if (buf) {
      ::munmap(buf, size);
      buf = nullptr;
    }
  }

  mkfs_ret mkfs(device_config_t) final {
    return mkfs_ertr::now();
  }

  mount_ret mount() final {
    return mount_ertr::now();
  }

  open_ertr::future<> open(
    const std::string &in_path,
    seastar::open_flags mode) override;

  write_ertr::future<> write(
    uint64_t offset,
    bufferptr &bptr,
    uint16_t stream = 0) override;

  using NVMeBlockDevice::read;
  read_ertr::future<> read(
    uint64_t offset,
    bufferptr &bptr) override;

  close_ertr::future<> close() override;

  write_ertr::future<> writev(
    uint64_t offset,
    ceph::bufferlist bl,
    uint16_t stream = 0) final;

  char *buf;
  size_t size;
};
}
