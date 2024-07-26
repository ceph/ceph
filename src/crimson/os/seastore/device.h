// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <memory>

#include "include/buffer_fwd.h"

#include "crimson/common/errorator.h"
#include "crimson/os/seastore/seastore_types.h"

namespace crimson::os::seastore {

using magic_t = uint64_t;

struct device_spec_t {
  magic_t magic = 0;
  device_type_t dtype = device_type_t::NONE;
  device_id_t id = DEVICE_ID_NULL;
  DENC(device_spec_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.magic, p);
    denc(v.dtype, p);
    denc(v.id, p);
    DENC_FINISH(p);
  }
};

std::ostream& operator<<(std::ostream&, const device_spec_t&);

using secondary_device_set_t =
  std::map<device_id_t, device_spec_t>;

struct device_config_t {
  bool major_dev = false;
  device_spec_t spec;
  seastore_meta_t meta;
  secondary_device_set_t secondary_devices;
  DENC(device_config_t, v, p) {
    DENC_START(1, 1, p);
    denc(v.major_dev, p);
    denc(v.spec, p);
    denc(v.meta, p);
    denc(v.secondary_devices, p);
    DENC_FINISH(p);
  }
  static device_config_t create_primary(
    uuid_d new_osd_fsid,
    device_id_t id,
    device_type_t d_type,
    secondary_device_set_t sds) {
    return device_config_t{
             true,
             device_spec_t{
               (magic_t)std::rand(),
		d_type,
		id},
             seastore_meta_t{new_osd_fsid},
             sds};
   }
  static device_config_t create_secondary(
    uuid_d new_osd_fsid,
    device_id_t id,
    device_type_t d_type,
    magic_t magic) {
    return device_config_t{
             false,
             device_spec_t{
               magic,
               d_type,
               id},
             seastore_meta_t{new_osd_fsid},
             secondary_device_set_t()};
  }
};

std::ostream& operator<<(std::ostream&, const device_config_t&);

class Device;
using DeviceRef = std::unique_ptr<Device>;

/**
 * Device
 *
 * Represents a general device regardless of the underlying medium.
 */
class Device {
// interfaces used by device
public:
  virtual ~Device() {}

  virtual seastar::future<> start() {
    return seastar::now();
  }

  virtual seastar::future<> stop() {
    return seastar::now();
  }
  // called on the shard to get this shard device;
  virtual Device& get_sharded_device() {
    return *this;
  }

  using access_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::permission_denied,
    crimson::ct_error::enoent>;

  using mkfs_ertr = access_ertr;
  using mkfs_ret = mkfs_ertr::future<>;
  virtual mkfs_ret mkfs(device_config_t) = 0;

  using mount_ertr = access_ertr;
  using mount_ret = access_ertr::future<>;
  virtual mount_ret mount() = 0;

  static seastar::future<DeviceRef> make_device(
    const std::string &device,
    device_type_t dtype);

// interfaces used by each device shard
public:
  virtual device_id_t get_device_id() const = 0;

  virtual magic_t get_magic() const = 0;

  virtual device_type_t get_device_type() const = 0;

  virtual backend_type_t get_backend_type() const = 0;

  virtual const seastore_meta_t &get_meta() const = 0;

  virtual extent_len_t get_block_size() const = 0;

  virtual std::size_t get_available_size() const = 0;

  virtual secondary_device_set_t& get_secondary_devices() = 0;

  using close_ertr = crimson::errorator<
    crimson::ct_error::input_output_error>;
  virtual close_ertr::future<> close() = 0;

  using read_ertr = crimson::errorator<
    crimson::ct_error::input_output_error,
    crimson::ct_error::invarg,
    crimson::ct_error::enoent,
    crimson::ct_error::erange>;
  virtual read_ertr::future<> read(
    paddr_t addr,
    size_t len,
    ceph::bufferptr &out) = 0;

  read_ertr::future<ceph::bufferptr> read(
    paddr_t addr,
    size_t len
  ) {
    auto ptrref = std::make_unique<ceph::bufferptr>(
      buffer::create_page_aligned(len));
    return read(addr, len, *ptrref
    ).safe_then([ptrref=std::move(ptrref)]() mutable {
      return read_ertr::make_ready_future<bufferptr>(std::move(*ptrref));
    });
  }
};

}

WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::device_spec_t)
WRITE_CLASS_DENC(crimson::os::seastore::device_config_t)

#if FMT_VERSION >= 90000
template <> struct fmt::formatter<crimson::os::seastore::device_config_t> : fmt::ostream_formatter {};
template <> struct fmt::formatter<crimson::os::seastore::device_spec_t> : fmt::ostream_formatter {};
#endif
