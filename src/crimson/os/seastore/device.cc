// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "device.h"

#include <seastar/core/smp.hh>

#include "segment_manager.h"
#include "random_block_manager.h"
#include "random_block_manager/rbm_device.h"

SET_SUBSYS(seastore);

namespace crimson::os::seastore {

std::ostream& operator<<(std::ostream& out, const device_spec_t& ds)
{
  return out << "device_spec("
             << "magic=0x" << std::hex << ds.magic << std::dec
             << ", dtype=" << ds.dtype
             << ", " << device_id_printer_t{ds.id}
             << ")";
}

std::ostream& operator<<(std::ostream& out, const device_config_t& conf)
{
  out << "device_config_t("
      << "major_dev=" << conf.major_dev
      << ", spec=" << conf.spec
      << ", meta=" << conf.meta
      << ", secondary(";
  for (const auto& [k, v] : conf.secondary_devices) {
    out << device_id_printer_t{k}
        << ": " << v << ", ";
  }
  return out << "))";
}

std::ostream& operator<<(std::ostream& out, const device_shard_info_t& si)
{
  fmt::print(out, "{}", si);
  return out;
}

std::ostream& operator<<(std::ostream& out, const device_superblock_t& sb)
{
  fmt::print(out, "{}", sb);
  return out;
}

void device_superblock_t::validate() const
{
  // NOTE: magic is validated at the read site, outside this struct.
  ceph_assert(version == CRIMSON_DEVICE_SUPERBLOCK_VERSION);
  if (crimson::common::get_conf<bool>(
          "seastore_require_partition_count_match_reactor_count")) {
    ceph_assert(shard_num == seastar::smp::count);
  }
  ceph_assert(block_size > 0);
  ceph_assert(config.spec.magic != 0);
  ceph_assert(config.spec.id <= DEVICE_ID_MAX_VALID);
  if (!config.major_dev) {
    ceph_assert(config.secondary_devices.empty());
  }
  for (const auto& [k, v] : config.secondary_devices) {
    ceph_assert(k != config.spec.id);
    ceph_assert(k <= DEVICE_ID_MAX_VALID);
    ceph_assert(k == v.id);
    ceph_assert(v.magic != 0);
    ceph_assert(v.dtype > device_type_t::NONE);
    ceph_assert(v.dtype < device_type_t::NUM_TYPES);
  }
  if (config.spec.dtype == device_type_t::ZBD) {
    // ZBD: check zone/segment geometry
    ceph_assert(segment_capacity > 0);
    ceph_assert_always(segment_capacity <= SEGMENT_OFF_MAX);
  }
  auto backend = get_default_backend_of_device(config.spec.dtype);
  if (backend == backend_type_t::SEGMENTED) {
    ceph_assert(segment_size > 0 && segment_size % block_size == 0);
    ceph_assert_always(segment_size <= SEGMENT_OFF_MAX);
    ceph_assert(shard_infos.size() >= shard_num);
    for (unsigned int i = 0; i < shard_num; i++) {
      ceph_assert(shard_infos[i].size > 0);
      ceph_assert_always(shard_infos[i].size <= DEVICE_OFF_MAX);
      ceph_assert(shard_infos[i].segments > 0);
      ceph_assert_always(shard_infos[i].segments <= DEVICE_SEGMENT_ID_MAX);
      if (config.spec.dtype != device_type_t::ZBD) {
        // HDD: check tracker and first-segment offsets
        ceph_assert(shard_infos[i].size > segment_size &&
                    shard_infos[i].size % block_size == 0);
        ceph_assert(shard_infos[i].tracker_offset > 0 &&
                    shard_infos[i].tracker_offset % block_size == 0);
        ceph_assert(shard_infos[i].first_segment_offset >
                      shard_infos[i].tracker_offset &&
                    shard_infos[i].first_segment_offset % block_size == 0);
      }
    }
  } else {
    // RBM
    ceph_assert(total_size > 0);
    ceph_assert(get_default_backend_of_device(config.spec.dtype) ==
                backend_type_t::RANDOM_BLOCK);
    ceph_assert(shard_infos.size() >= shard_num);
    for (unsigned int i = 0; i < shard_num; i++) {
      ceph_assert(shard_infos[i].size > block_size &&
                  shard_infos[i].size % block_size == 0);
      ceph_assert_always(shard_infos[i].size <= DEVICE_OFF_MAX);
      ceph_assert(journal_size > 0 && journal_size % block_size == 0);
      ceph_assert(shard_infos[i].start_offset < total_size &&
                  shard_infos[i].start_offset % block_size == 0);
    }
  }
}

seastar::future<DeviceRef>
Device::make_device(const std::string& device, device_type_t dtype)
{
  if (get_default_backend_of_device(dtype) == backend_type_t::SEGMENTED) {
    return SegmentManager::get_segment_manager(device, dtype
    ).then([](DeviceRef ret) {
      return ret;
    });
  } 
  assert(get_default_backend_of_device(dtype) == backend_type_t::RANDOM_BLOCK);
  return get_rb_device(device
  ).then([](DeviceRef ret) {
    return ret;
  });
}

check_create_device_ret check_create_device(
  const std::string path,
  size_t size)
{
  LOG_PREFIX(block_check_create_device);
  INFO("path={}, size=0x{:x}", path, size);
  return seastar::open_file_dma(
    path,
    seastar::open_flags::exclusive |
    seastar::open_flags::rw |
    seastar::open_flags::create
  ).then([size, FNAME, path](auto file) {
    return seastar::do_with(
      file,
      [size, FNAME, path](auto &f) -> seastar::future<>
    {
      DEBUG("path={} created, truncating to 0x{:x}", path, size);
      ceph_assert(f);
      return f.truncate(
        size
      ).then([&f, size] {
        return f.allocate(0, size);
      }).finally([&f] {
        return f.close();
      });
    });
  }).then_wrapped([path, FNAME](auto f) -> check_create_device_ret {
    if (f.failed()) {
      try {
	f.get();
	return seastar::now();
      } catch (const std::system_error &e) {
	if (e.code().value() == EEXIST) {
          DEBUG("path={} exists", path);
	  return seastar::now();
	} else {
          ERROR("path={} creation error -- {}", path, e);
	  return crimson::ct_error::input_output_error::make();
	}
      } catch (...) {
        ERROR("path={} creation error", path);
	return crimson::ct_error::input_output_error::make();
      }
    }

    DEBUG("path={} complete", path);
    std::ignore = f.discard_result();
    return seastar::now();
  });
}
}
