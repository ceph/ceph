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

using NVMePassThroughCommand = nvme_passthru_cmd;

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
   * offload checksuming to NVMe device to reduce its CPU utilization
   */
   virtual write_ertr::future<> protected_write(
    uint64_t offset,
    bufferptr &bptr,
    uint16_t stream = 0) { return write_ertr::now(); }

   virtual read_ertr::future<> protected_read(
    uint64_t offset,
    bufferptr &bptr) { return read_ertr::now(); }

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

  // TODO Servicing NVMe features (multi-stream, protected write etc..) should
  // be followed by upstreaming ioctl to seastar.

private:
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
