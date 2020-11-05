// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include "crimson/os/poseidonstore/device_manager/memory.h"

namespace crimson::os::poseidonstore::device_manager {

DeviceManagerRef create_memory(memory_config_t config) {
  return DeviceManagerRef{new MemoryDeviceManager(config)};
}

std::ostream &operator<<(std::ostream &lhs, const memory_config_t &c) {
  return lhs << "memory_config_t(size=" << c.size << ", block_size=" << c.block_size
	     << ", page_size=" << c.page_size << ")";
}

}

