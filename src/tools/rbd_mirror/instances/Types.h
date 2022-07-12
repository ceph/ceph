// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_INSTANCES_TYPES_H
#define CEPH_RBD_MIRROR_INSTANCES_TYPES_H

#include <string>
#include <vector>

namespace rbd {
namespace mirror {
namespace instances {

struct Listener {
  typedef std::vector<std::string> InstanceIds;

  virtual ~Listener() {
  }

  virtual void handle_added(const InstanceIds& instance_ids) = 0;
  virtual void handle_removed(const InstanceIds& instance_ids) = 0;
};

} // namespace instances
} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_INSTANCES_TYPES_H
