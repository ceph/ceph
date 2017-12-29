// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_MAP_SIMPLE_POLICY_H
#define CEPH_RBD_MIRROR_IMAGE_MAP_SIMPLE_POLICY_H

#include "Policy.h"

namespace rbd {
namespace mirror {
namespace image_map {

class SimplePolicy : public Policy {
public:
  static SimplePolicy *create(librados::IoCtx &ioctx) {
    return new SimplePolicy(ioctx);
  }

protected:
  using Policy::m_map_lock;
  using Policy::m_map;

  SimplePolicy(librados::IoCtx &ioctx);

  std::string do_map(const std::string &global_image_id) override;

  void do_shuffle_add_instances(const std::vector<std::string> &instance_ids,
                                std::set<std::string> *remap_global_image_ids) override;

private:
  uint64_t calc_images_per_instance(int nr_instances);
};

} // namespace image_map
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_MAP_SIMPLE_POLICY_H
