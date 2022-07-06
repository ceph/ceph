// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <memory>

#include <seastar/core/file.hh>
#include <seastar/core/future.hh>
#include <seastar/core/reactor.hh>

#include "include/buffer_fwd.h"

#include "crimson/common/errorator.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/logging.h"

namespace crimson::os::seastore {

using magic_t = uint64_t;

struct device_spec_t{
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
public:
  virtual ~Device() {}

  virtual device_id_t get_device_id() const = 0;

  virtual magic_t get_magic() const = 0;

  virtual device_type_t get_device_type() const = 0;

  virtual const seastore_meta_t &get_meta() const = 0;

  virtual seastore_off_t get_block_size() const = 0;

  virtual std::size_t get_size() const = 0;

  virtual secondary_device_set_t& get_secondary_devices() = 0;

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

  static seastar::future<DeviceRef> make_device(const std::string &device);
};

namespace device {

using write_ertr = crimson::errorator<
  crimson::ct_error::input_output_error>;
using read_ertr = crimson::errorator<
  crimson::ct_error::input_output_error>;

write_ertr::future<> do_write(
  device_id_t device_id,
  seastar::file &device,
  uint64_t offset,
  bufferptr &bptr);

write_ertr::future<> do_writev(
  device_id_t device_id,
  seastar::file &device,
  uint64_t offset,
  bufferlist&& bl,
  size_t block_size);

read_ertr::future<> do_read(
  device_id_t device_id,
  seastar::file &device,
  uint64_t offset,
  size_t len,
  bufferptr &bptr);

using access_ertr = crimson::errorator<
  crimson::ct_error::input_output_error,
  crimson::ct_error::permission_denied,
  crimson::ct_error::enoent>;
using check_create_device_ertr = access_ertr;
using check_create_device_ret = check_create_device_ertr::future<>;
check_create_device_ret check_create_device(
  const std::string &path,
  size_t size);

using open_device_ret = access_ertr::future<
  std::pair<seastar::file, seastar::stat_data>>;
open_device_ret open_device(const std::string& path);

template <typename T>
access_ertr::future<> write_superblock(
    device_id_t device_id,
    seastar::file &device,
    T sb)
{
  auto &logger = get_logger(ceph_subsys_seastore_device);
  logger.debug("block_write_superblock: D{} write {}",
	       device_id_printer_t{device_id}, sb);
  sb.validate();
  assert(ceph::encoded_sizeof<T>(sb) <
	 sb.block_size);
  return seastar::do_with(
    bufferptr(ceph::buffer::create_page_aligned(sb.block_size)),
    [=, &device](auto &bp)
  {
    bufferlist bl;
    encode(sb, bl);
    auto iter = bl.begin();
    assert(bl.length() < sb.block_size);
    iter.copy(bl.length(), bp.c_str());
    return do_write(device_id, device, 0, bp);
  });
}

template <typename T>
access_ertr::future<T> read_superblock(seastar::file &device, seastar::stat_data sd)
{
  auto &logger = get_logger(ceph_subsys_seastore_device);
  logger.debug("reading superblock ...");
  return seastar::do_with(
    bufferptr(ceph::buffer::create_page_aligned(sd.block_size)),
    [=, &device, &logger](auto &bp)
  {
    return do_read(
      DEVICE_ID_NULL, // unknown
      device,
      0,
      bp.length(),
      bp
    ).safe_then([=, &bp, &logger] {
      bufferlist bl;
      bl.push_back(bp);
      T ret;
      auto bliter = bl.cbegin();
      try {
        decode(ret, bliter);
      } catch (...) {
        logger.error("got decode error!");
        ceph_assert(0 == "invalid superblock");
      }
      assert(ceph::encoded_sizeof<T>(ret) <
             sd.block_size);
      return access_ertr::future<T>(
        access_ertr::ready_future_marker{},
        ret);
    });
  });
}
}

}

WRITE_CLASS_DENC_BOUNDED(crimson::os::seastore::device_spec_t)
WRITE_CLASS_DENC(crimson::os::seastore::device_config_t)
