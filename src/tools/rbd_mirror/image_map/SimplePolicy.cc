// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"

#include "SimplePolicy.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_map::SimplePolicy: " << this \
                           << " " << __func__ << ": "
namespace rbd {
namespace mirror {
namespace image_map {

SimplePolicy::SimplePolicy(librados::IoCtx &ioctx)
  : Policy(ioctx) {
}

size_t SimplePolicy::calc_images_per_instance(const InstanceToImageMap& map,
                                              size_t image_count) {
  size_t nr_instances = 0;
  for (auto const &it : map) {
    if (!Policy::is_dead_instance(it.first)) {
      ++nr_instances;
    }
  }
  ceph_assert(nr_instances > 0);

  size_t images_per_instance = image_count / nr_instances;
  if (images_per_instance == 0) {
    ++images_per_instance;
  }

  return images_per_instance;
}

void SimplePolicy::do_shuffle_add_instances(
    const InstanceToImageMap& map, size_t image_count,
    GlobalIds *remap_global_ids) {
  uint64_t images_per_instance = calc_images_per_instance(map, image_count);
  dout(5) << "images per instance=" << images_per_instance << dendl;

  for (auto const &instance : map) {
    uint64_t instance_image_count =
      std::accumulate(instance.second.begin(), instance.second.end(), 0,
                      [this](uint64_t count, const GlobalId &global_id) {
                        return count + get_weight(global_id);
                      });

    if (instance_image_count <= images_per_instance) {
      continue;
    }

    auto it = instance.second.begin();

    uint64_t cut_off = instance_image_count - images_per_instance;

    // TODO: improve for weight > 1: find the best entity(ies) to cut off
    while (it != instance.second.end() && cut_off > 0) {
      auto weight = get_weight(*it);
      if (weight <= cut_off) {
        if (Policy::is_entity_shuffling(*it)) {
          cut_off -= weight;
        } else if (Policy::can_shuffle_entity(*it)) {
          cut_off -= weight;
          remap_global_ids->emplace(*it);
        }
      }
      ++it;
    }
  }
}

std::string SimplePolicy::do_map(const InstanceToImageMap& map,
                                 const GlobalId &global_id) {
  auto min_it = map.end();
  uint64_t min_image_count = UINT64_MAX;
  for (auto it = map.begin(); it != map.end(); ++it) {
    ceph_assert(it->second.find(global_id) == it->second.end());
    if (Policy::is_dead_instance(it->first)) {
      continue;
    }
    uint64_t image_count =
      std::accumulate(it->second.begin(), it->second.end(), 0,
                      [this](uint64_t count, const GlobalId &global_id) {
                        return count + get_weight(global_id);
                      });
    if (image_count < min_image_count) {
      min_it = it;
      min_image_count = image_count;
    }
  }

  ceph_assert(min_it != map.end());
  dout(20) << "global_id=" << global_id << " maps to instance_id="
           << min_it->first << dendl;
  return min_it->first;
}

} // namespace image_map
} // namespace mirror
} // namespace rbd
