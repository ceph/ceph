// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "fs_driver.h"
#include "block_driver.h"

#include "tm_driver.h"

BlockDriverRef get_backend(BlockDriver::config_t config,
			   seastar::alien::instance& alien)
{
  if (config.type == "transaction_manager") {
    return std::make_unique<TMDriver>(config);
  } else if (config.is_futurized_store()) {
    return std::make_unique<FSDriver>(config, alien);
  } else {
    ceph_assert(0 == "invalid option");
    return BlockDriverRef();
  }
}
