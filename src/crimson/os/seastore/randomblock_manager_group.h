// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab expandtab

#pragma once

#include <set>

#include "crimson/common/errorator.h"
#include "crimson/os/seastore/seastore_types.h"
#include "crimson/os/seastore/random_block_manager.h"
#include "crimson/os/seastore/random_block_manager/block_rb_manager.h"

namespace crimson::os::seastore {

class RBMDeviceGroup {
public:
  RBMDeviceGroup() {
    rb_devices.resize(DEVICE_ID_MAX);
  }

  const std::set<device_id_t>& get_device_ids() const {
    return device_ids;
  }

  std::vector<RandomBlockManager*> get_rb_managers() const {
    assert(device_ids.size());
    std::vector<RandomBlockManager*> ret;
    for (auto& device_id : device_ids) {
      auto rb_device = rb_devices[device_id].get();
      assert(rb_device->get_device_id() == device_id);
      ret.emplace_back(rb_device);
    }
    return ret;
  }

  void add_rb_manager(RandomBlockManagerRef rbm) {
    auto device_id = rbm->get_device_id();
    ceph_assert(!has_device(device_id));
    rb_devices[device_id] = std::move(rbm);
    device_ids.insert(device_id);
  }

  void reset() {
    rb_devices.clear();
    rb_devices.resize(DEVICE_ID_MAX);
    device_ids.clear();
  }

  auto get_block_size() const {
    assert(device_ids.size());
    return rb_devices[*device_ids.begin()]->get_block_size();
  }

  const seastore_meta_t &get_meta() const {
    assert(device_ids.size());
    return rb_devices[*device_ids.begin()]->get_meta();
  }

private:
  bool has_device(device_id_t id) const {
    assert(id <= DEVICE_ID_MAX_VALID);
    return device_ids.count(id) >= 1;
  }

  std::vector<RandomBlockManagerRef> rb_devices;
  std::set<device_id_t> device_ids;
};

using RBMDeviceGroupRef = std::unique_ptr<RBMDeviceGroup>;

}
