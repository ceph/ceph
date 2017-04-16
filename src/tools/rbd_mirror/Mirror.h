// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_H
#define CEPH_RBD_MIRROR_H

#include "common/ceph_context.h"
#include "common/Mutex.h"
#include "include/rados/librados.hpp"
#include "ClusterWatcher.h"
#include "PoolReplayer.h"
#include "ImageDeleter.h"
#include "types.h"

#include <set>
#include <map>
#include <memory>
#include <atomic>

namespace librbd { struct ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> struct Threads;
class MirrorAdminSocketHook;

/**
 * Contains the main loop and overall state for rbd-mirror.
 *
 * Sets up mirroring, and coordinates between noticing config
 * changes and applying them.
 */
class Mirror {
public:
  Mirror(CephContext *cct, const std::vector<const char*> &args);
  Mirror(const Mirror&) = delete;
  Mirror& operator=(const Mirror&) = delete;
  ~Mirror();

  int init();
  void run();
  void handle_signal(int signum);

  void print_status(Formatter *f, stringstream *ss);
  void start();
  void stop();
  void restart();
  void flush();
  void release_leader();

private:
  typedef ClusterWatcher::PoolPeers PoolPeers;
  typedef std::pair<int64_t, peer_t> PoolPeer;

  void update_pool_replayers(const PoolPeers &pool_peers);

  CephContext *m_cct;
  std::vector<const char*> m_args;
  Threads<librbd::ImageCtx> *m_threads = nullptr;
  Mutex m_lock;
  Cond m_cond;
  RadosRef m_local;

  // monitor local cluster for config changes in peers
  std::unique_ptr<ClusterWatcher> m_local_cluster_watcher;
  std::shared_ptr<ImageDeleter> m_image_deleter;
  std::map<PoolPeer, std::unique_ptr<PoolReplayer> > m_pool_replayers;
  std::atomic<bool> m_stopping = { false };
  bool m_manual_stop = false;
  MirrorAdminSocketHook *m_asok_hook;
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_H
