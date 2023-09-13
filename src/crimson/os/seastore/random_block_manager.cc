// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/os/seastore/logging.h"
#include "crimson/os/seastore/random_block_manager.h"
#include "crimson/os/seastore/random_block_manager/nvme_block_device.h"
#include "crimson/os/seastore/random_block_manager/rbm_device.h"

namespace crimson::os::seastore {

seastar::future<random_block_device::RBMDeviceRef>
get_rb_device(
  const std::string &device)
{
  return seastar::make_ready_future<random_block_device::RBMDeviceRef>(
    std::make_unique<
      random_block_device::nvme::NVMeBlockDevice
    >(device + "/block"));
}

}
