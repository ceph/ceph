// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_H
#define CEPH_RBD_MIRROR_H

#include <map>
#include <memory>
#include <set>

#include "common/ceph_context.h"
#include "common/Mutex.h"
#include "include/atomic.h"
#include "include/rados/librados.hpp"
#include "ClusterWatcher.h"
#include "Replayer.h"
#include "types.h"

namespace rbd {
namespace mirror {

struct Threads;

/**
 * Contains the main loop and overall state for rbd-mirror.
 *
 * Sets up mirroring, and coordinates between noticing config
 * changes and applying them.
 */
class Mirror {
public:
  Mirror(CephContext *cct);
  Mirror(const Mirror&) = delete;
  Mirror& operator=(const Mirror&) = delete;

  int init();
  void run();
  void handle_signal(int signum);

private:
  void refresh_peers(const set<peer_t> &peers);
  void update_replayers(const map<peer_t, set<int64_t> > &peer_configs);

  CephContext *m_cct;
  Threads *m_threads = nullptr;
  Mutex m_lock;
  Cond m_cond;
  RadosRef m_local;

  // monitor local cluster for config changes in peers
  std::unique_ptr<ClusterWatcher> m_local_cluster_watcher;
  Mutex m_replayers_lock;
  std::map<peer_t, std::unique_ptr<Replayer> > m_replayers;
  atomic_t m_stopping;
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_H
