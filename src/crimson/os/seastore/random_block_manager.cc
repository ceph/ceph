// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/random_block_manager.h"
#include "crimson/os/seastore/random_block_manager/nvme_block_device.h"
#include "crimson/os/seastore/random_block_manager/rbm_device.h"

namespace crimson::os::seastore {

seastar::future<random_block_device::RBMDeviceRef>
get_rb_device(
  const std::string &device)
{
  std::string dev_path = crimson::common::local_conf().get_val<std::string>("seastore_device_path");
  if (dev_path.empty()) {
    dev_path = device + "/block";
  }
  return seastar::make_ready_future<random_block_device::RBMDeviceRef>(
    std::make_unique<
      random_block_device::nvme::NVMeBlockDevice
    >(dev_path));
}

}
