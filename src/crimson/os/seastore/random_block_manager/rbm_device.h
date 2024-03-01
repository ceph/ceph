//-*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/random_block_manager.h"
#include "crimson/os/seastore/device.h"

namespace ceph {
  namespace buffer {
    class bufferptr;
  }
}

namespace crimson::os::seastore::random_block_device {

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

constexpr uint32_t RBM_SUPERBLOCK_SIZE = 4096;
enum {
  // TODO: This allows the device to manage crc on a block by itself
  RBM_NVME_END_TO_END_PROTECTION = 1,
  RBM_BITMAP_BLOCK_CRC = 2,
};

class RBMDevice : public Device {
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
  rbm_metadata_header_t super;
  rbm_shard_info_t shard_info;
public:
  RBMDevice() {}
  virtual ~RBMDevice() = default;

  template <typename T>
  static std::unique_ptr<T> create() {
    return std::make_unique<T>();
  }

  device_id_t get_device_id() const {
    return super.config.spec.id;
  }

  magic_t get_magic() const final {
    return super.config.spec.magic;
  }

  device_type_t get_device_type() const final {
    return device_type_t::RANDOM_BLOCK_SSD;
  }

  backend_type_t get_backend_type() const final {
    return backend_type_t::RANDOM_BLOCK;
  }

  const seastore_meta_t &get_meta() const final {
    return super.config.meta;
  }

  secondary_device_set_t& get_secondary_devices() final {
    return super.config.secondary_devices;
  }
  std::size_t get_available_size() const { return super.size; }
  extent_len_t get_block_size() const { return super.block_size; }

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
    bufferptr &&bptr,
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

  bool is_data_protection_enabled() const { return false; }

  mkfs_ret do_mkfs(device_config_t);

  // shard 0 mkfs
  mkfs_ret do_primary_mkfs(device_config_t, int shard_num, size_t journal_size);

  mount_ret do_mount();

  mount_ret do_shard_mount();

  write_ertr::future<> write_rbm_header();

  read_ertr::future<rbm_metadata_header_t> read_rbm_header(rbm_abs_addr addr);

  using stat_device_ret =
    read_ertr::future<seastar::stat_data>;
  virtual stat_device_ret stat_device() = 0;

  virtual std::string get_device_path() const = 0;

  uint64_t get_journal_size() const {
    return super.journal_size;
  }

  static rbm_abs_addr get_shard_reserved_size() {
    return RBM_SUPERBLOCK_SIZE;
  }

  rbm_abs_addr get_shard_journal_start() {
    return shard_info.start_offset + get_shard_reserved_size();
  }

  uint64_t get_shard_start() const {
    return shard_info.start_offset;
  }

  uint64_t get_shard_end() const {
    return shard_info.start_offset + shard_info.size;
  }
};
using RBMDeviceRef = std::unique_ptr<RBMDevice>;

constexpr uint64_t DEFAULT_TEST_CBJOURNAL_SIZE = 1 << 26;

class EphemeralRBMDevice : public RBMDevice {
public:
  uint64_t size = 0;
  uint64_t block_size = 0;
  constexpr static uint32_t TEST_BLOCK_SIZE = 4096;

  EphemeralRBMDevice(size_t size, uint64_t block_size) :
    size(size), block_size(block_size), buf(nullptr) {
  }
  ~EphemeralRBMDevice() {
    if (buf) {
      ::munmap(buf, size);
      buf = nullptr;
    }
  }

  std::size_t get_available_size() const final { return size; }
  extent_len_t get_block_size() const final { return block_size; }

  mount_ret mount() final;
  mkfs_ret mkfs(device_config_t config) final;

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
    bufferptr &bptr) override;

  close_ertr::future<> close() override;

  write_ertr::future<> writev(
    uint64_t offset,
    ceph::bufferlist bl,
    uint16_t stream = 0) final;

  stat_device_ret stat_device() final {
    seastar::stat_data stat;
    stat.block_size = block_size;
    stat.size = size;
    return stat_device_ret(
      read_ertr::ready_future_marker{},
      stat
    );
  }

  std::string get_device_path() const final {
    return "";
  }

  char *buf;
};
using EphemeralRBMDeviceRef = std::unique_ptr<EphemeralRBMDevice>;
EphemeralRBMDeviceRef create_test_ephemeral(
  uint64_t journal_size = DEFAULT_TEST_CBJOURNAL_SIZE,
  uint64_t data_size = DEFAULT_TEST_CBJOURNAL_SIZE);

}
