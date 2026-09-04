// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/random_block_manager.h"
#include "crimson/os/seastore/random_block_manager/nvme_block_device.h"
#include "crimson/os/seastore/random_block_manager/rbm_device.h"
#include "crimson/os/seastore/random_block_manager/hdd_device.h"

namespace crimson::os::seastore {

seastar::future<random_block_device::RBMDeviceRef>
get_rb_device(
  const std::string &device, device_type_t dtype)
{
  std::string device_path = normalize_device_path(device);
  if (dtype == device_type_t::RANDOM_BLOCK_HDD) {
    return seastar::make_ready_future<random_block_device::RBMDeviceRef>(
      std::make_unique<
        random_block_device::RotationalDevice
      >(std::move(device_path)));
  } else {
    return seastar::make_ready_future<random_block_device::RBMDeviceRef>(
      std::make_unique<
        random_block_device::nvme::NVMeBlockDevice
      >(std::move(device_path)));
  }
}

}
