// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_CLUSTER_WATCHER_H
#define CEPH_RBD_MIRROR_CLUSTER_WATCHER_H

#include <map>
#include <memory>
#include <set>

#include "common/ceph_context.h"
#include "common/Mutex.h"
#include "common/Timer.h"
#include "include/rados/librados.hpp"
#include "types.h"

namespace rbd {
namespace mirror {

/**
 * Tracks mirroring configuration for pools in a single
 * cluster.
 */
class ClusterWatcher {
public:
  ClusterWatcher(RadosRef cluster, Mutex &lock);
  ~ClusterWatcher() = default;
  ClusterWatcher(const ClusterWatcher&) = delete;
  ClusterWatcher& operator=(const ClusterWatcher&) = delete;
  // Caller controls frequency of calls
  void refresh_pools();
  const std::map<peer_t, std::set<int64_t> >& get_peer_configs() const;
  const std::set<std::string>& get_pool_names() const;

private:
  void read_configs(std::map<peer_t, std::set<int64_t> > *peer_configs,
		    std::set<std::string> *pool_names);

  Mutex &m_lock;
  RadosRef m_cluster;
  std::map<peer_t, std::set<int64_t> > m_peer_configs;
  std::set<std::string> m_pool_names;
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_CLUSTER_WATCHER_H
