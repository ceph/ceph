// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/debug.h"
#include "common/errno.h"

#include "SimplePolicy.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rbd_mirror
#undef dout_prefix
#define dout_prefix *_dout << "rbd::mirror::image_map::SimplePolicy: " << this \
                           << " " << __func__
namespace rbd {
namespace mirror {
namespace image_map {

SimplePolicy::SimplePolicy(librados::IoCtx &ioctx)
  : Policy(ioctx) {
}

uint64_t SimplePolicy::calc_images_per_instance(int nr_instances) {
  assert(nr_instances > 0);

  uint64_t nr_images = 0;
  for (auto const &it : m_map) {
    if (!Policy::is_dead_instance(it.first)) {
      nr_images += it.second.size();
    }
  }

  uint64_t images_per_instance = nr_images / nr_instances;
  if (images_per_instance == 0) {
    ++images_per_instance;
  }

  return images_per_instance;
}

void SimplePolicy::do_shuffle_add_instances(const std::vector<std::string> &instance_ids,
                                            std::set<std::string> *remap_global_image_ids) {
  assert(m_map_lock.is_wlocked());

  uint64_t images_per_instance = calc_images_per_instance(m_map.size());
  dout(5) << ": images per instance=" << images_per_instance << dendl;

  for (auto const &instance : m_map) {
    if (instance.second.size() <= images_per_instance) {
      continue;
    }

    auto it = instance.second.begin();
    uint64_t cut_off = instance.second.size() - images_per_instance;

    while (it != instance.second.end() && cut_off > 0) {
      if (Policy::can_shuffle_image(*it)) {
        --cut_off;
        remap_global_image_ids->emplace(*it);
      }

      ++it;
    }
  }
}

std::string SimplePolicy::do_map(const std::string &global_image_id) {
  assert(m_map_lock.is_wlocked());

  auto min_it = m_map.begin();

  for (auto it = min_it; it != m_map.end(); ++it) {
    assert(it->second.find(global_image_id) == it->second.end());
    if (it->second.size() < min_it->second.size() && !Policy::is_dead_instance(it->first)) {
      min_it = it;
    }
  }

  dout(20) << ": global_image_id=" << global_image_id << " maps to instance_id="
           << min_it->first << dendl;
  return min_it->first;
}

} // namespace image_map
} // namespace mirror
} // namespace rbd
