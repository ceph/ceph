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

namespace ceph {
  namespace buffer {
    class bufferptr;
  }
}

namespace crimson::os::seastore::nvme_device {

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
  uint32_t cntroller_id : 16;

  static const uint8_t CNS_NAMESPACE = 0x00;
  static const uint8_t CNS_CONTROLLER = 0x01;
};

struct nvme_admin_command_t {
  union
  {
    nvme_passthru_cmd common_cmd;
    nvme_identify_command_t identify_cmd;
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
      uint8_t unused[256];
      oacs_t oacs;
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
      uint8_t unused[28];   // [27:0]
      dpc_t dpc;            // [28]
      dps_t dps;            // [29]
      uint8_t unused2[98];  // [127:30]
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
class NVMeBlockDevice {
protected:
  uint64_t size = 0;
  uint64_t block_size = 4096;

  uint64_t write_granularity = 4096;
  uint64_t write_alignment = 4096;

  bool data_protection_enabled = false;

public:
  NVMeBlockDevice() {}
  virtual ~NVMeBlockDevice() = default;

  template <typename T>
  static std::unique_ptr<T> create() {
    return std::make_unique<T>();
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
   */
  uint64_t get_size() const { return size; }
  uint64_t get_block_size() const { return block_size; }

  uint64_t get_preffered_write_granularity() const { return write_granularity; }
  uint64_t get_preffered_write_alignment() const { return write_alignment; }

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

  // TODO
  virtual discard_ertr::future<> discard(
    uint64_t offset,
    uint64_t len) { return seastar::now(); }

  virtual open_ertr::future<> open(
      const std::string& path,
      seastar::open_flags mode) = 0;
  virtual seastar::future<> close() = 0;

  /*
   * For passsing through nvme IO or Admin command to SSD
   * Caller can construct and execute its own nvme command
   */
  virtual nvme_command_ertr::future<> pass_through_io(
    const NVMePassThroughCommand& command) { return nvme_command_ertr::now(); }
  virtual nvme_command_ertr::future<> pass_admin(
    const nvme_admin_command_t& command) { return nvme_command_ertr::now(); }

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

  read_ertr::future<> read(
    uint64_t offset,
    bufferptr &bptr) override;

  seastar::future<> close() override;

private:
  // identify_controller/namespace are used to get SSD internal information such
  // as supported features
  uint8_t data_protection_type = 0;

  nvme_command_ertr::future<nvme_identify_controller_data_t> identify_controller();
  nvme_command_ertr::future<nvme_identify_namespace_data_t> identify_namespace();
  nvme_command_ertr::future<int> get_nsid();
  seastar::file device;
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

  open_ertr::future<> open(
    const std::string &in_path,
    seastar::open_flags mode) override;

  write_ertr::future<> write(
    uint64_t offset,
    bufferptr &bptr,
    uint16_t stream = 0) override;

  read_ertr::future<> read(
    uint64_t offset,
    bufferptr &bptr) override;

  seastar::future<> close() override;

  char *buf;
  size_t size;
};

}
