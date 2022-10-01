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
  uint64_t size = 0;

  // LBA Size
  uint64_t block_size = 4096;

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

  device_type_t get_device_type() const final {
    return device_type_t::RANDOM_BLOCK;
  }

  const seastore_meta_t &get_meta() const final {
    return meta;
  }

  secondary_device_set_t& get_secondary_devices() final {
    return devices;
  }
  std::size_t get_size() const { return size; }
  seastore_off_t get_block_size() const { return block_size; }

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

  bool is_data_protection_enabled() const { return false; }
};


class TestMemory : public RBMDevice {
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
    return open("", seastar::open_flags::rw
    ).safe_then([]() {
      return mount_ertr::now();
    }).handle_error(
      mount_ertr::pass_further{},
      crimson::ct_error::assert_all{
	"Invalid error mount"
      }
    );
  }

  open_ertr::future<> open(
    const std::string &in_path,
    seastar::open_flags mode) override;

  write_ertr::future<> write(
    uint64_t offset,
    bufferptr &bptr,
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

  char *buf;
  size_t size;
};
}
