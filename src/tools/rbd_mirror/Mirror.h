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
#include "ImageDeleter.h"
#include "types.h"

namespace rbd {
namespace mirror {

struct Threads;
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

private:
  typedef ClusterWatcher::PoolPeers PoolPeers;
  typedef std::pair<int64_t, peer_t> PoolPeer;

  void update_replayers(const PoolPeers &pool_peers);

  CephContext *m_cct;
  std::vector<const char*> m_args;
  Threads *m_threads = nullptr;
  Mutex m_lock;
  Cond m_cond;
  RadosRef m_local;

  // monitor local cluster for config changes in peers
  std::unique_ptr<ClusterWatcher> m_local_cluster_watcher;
  std::shared_ptr<ImageDeleter> m_image_deleter;
  ImageSyncThrottlerRef<> m_image_sync_throttler;
  std::map<PoolPeer, std::unique_ptr<Replayer> > m_replayers;
  atomic_t m_stopping;
  bool m_manual_stop = false;
  MirrorAdminSocketHook *m_asok_hook;
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_H
