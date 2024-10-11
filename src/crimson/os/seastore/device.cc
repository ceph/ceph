// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "device.h"

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
          ERROR("path={} exists", path);
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
