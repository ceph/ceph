// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_MAP_POLICY_H
#define CEPH_RBD_MIRROR_IMAGE_MAP_POLICY_H

#include <map>
#include <tuple>
#include <boost/optional.hpp>

#include "cls/rbd/cls_rbd_types.h"
#include "include/rados/librados.hpp"
#include "tools/rbd_mirror/image_map/StateTransition.h"
#include "tools/rbd_mirror/image_map/Types.h"

class Context;

namespace rbd {
namespace mirror {
namespace image_map {

class Policy {
public:
  Policy(librados::IoCtx &ioctx);

  virtual ~Policy() {
  }

  // init -- called during initialization
  void init(const std::map<GlobalId, cls::rbd::MirrorImageMap> &image_mapping);

  // lookup an image from the map
  LookupInfo lookup(const GlobalId &global_id);

  // add, remove
  bool add_entity(const GlobalId &global_id, uint64_t weight);
  bool remove_entity(const GlobalId &global_id);

  // shuffle images when instances are added/removed
  void add_instances(const InstanceIds &instance_ids, GlobalIds* global_ids);
  void remove_instances(const InstanceIds &instance_ids, GlobalIds* global_ids);

  ActionType start_action(const GlobalId &global_id);
  bool finish_action(const GlobalId &global_id, int r);

protected:
  typedef std::map<std::string, GlobalIds> InstanceToImageMap;

  bool is_dead_instance(const std::string instance_id) const {
    ceph_assert(ceph_mutex_is_locked(m_map_lock));
    return m_dead_instances.find(instance_id) != m_dead_instances.end();
  }

  bool is_entity_shuffling(const GlobalId &global_id) const;
  bool can_shuffle_entity(const GlobalId &global_id) const;
  uint64_t get_weight(const GlobalId &global_id) const;

  // map an entity (global id) to an instance
  virtual std::string do_map(const InstanceToImageMap& map,
                             const GlobalId &global_id) = 0;

  // shuffle images when instances are added/removed
  virtual void do_shuffle_add_instances(
      const InstanceToImageMap& map, size_t image_count,
      GlobalIds *remap_global_ids) = 0;

private:
  struct ImageState {
    uint64_t weight = 1;
    std::string instance_id = UNMAPPED_INSTANCE_ID;
    utime_t mapped_time;

    ImageState() {}
    ImageState(uint64_t weight, const std::string& instance_id,
               const utime_t& mapped_time)
      : weight(weight), instance_id(instance_id), mapped_time(mapped_time) {
    }

    // active state and action
    StateTransition::State state = StateTransition::STATE_UNASSOCIATED;
    StateTransition::Transition transition;

    // next scheduled state
    boost::optional<StateTransition::State> next_state = boost::none;
  };

  typedef std::map<GlobalId, ImageState> ImageStates;

  librados::IoCtx &m_ioctx;

  ceph::shared_mutex m_map_lock;        // protects m_map
  InstanceToImageMap m_map; // instance_id -> global_id map

  ImageStates m_image_states;
  std::set<std::string> m_dead_instances;

  bool m_initial_update = true;

  void remove_instances(const ceph::shared_mutex& lock,
			const InstanceIds &instance_ids,
                        GlobalIds* global_ids);

  bool set_state(ImageState* image_state, StateTransition::State state,
                 bool ignore_current_state);

  void execute_policy_action(const GlobalId& global_id, ImageState* image_state,
                             StateTransition::PolicyAction policy_action);

  void map(const GlobalId &global_id, ImageState* image_state);
  void unmap(const GlobalId &global_id, ImageState* image_state);

  bool is_state_scheduled(const ImageState& image_state,
                          StateTransition::State state) const;

};

} // namespace image_map
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_IMAGE_MAP_POLICY_H
