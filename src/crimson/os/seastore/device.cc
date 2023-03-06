// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab

#include "device.h"

#include "segment_manager.h"
#include "random_block_manager.h"
#include "random_block_manager/rbm_device.h"

namespace crimson::os::seastore {

std::ostream& operator<<(std::ostream& out, const device_spec_t& ds)
{
  return out << "device_spec("
             << "magic=" << ds.magic
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

}
