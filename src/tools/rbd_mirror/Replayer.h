// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_REPLAYER_H
#define CEPH_RBD_MIRROR_REPLAYER_H

#include <map>
#include <memory>
#include <set>
#include <string>

#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/WorkQueue.h"
#include "include/atomic.h"
#include "include/rados/librados.hpp"

#include "ClusterWatcher.h"
#include "ImageReplayer.h"
#include "PoolWatcher.h"
#include "types.h"

namespace rbd {
namespace mirror {

struct Threads;
class ReplayerAdminSocketHook;
class MirrorStatusWatchCtx;

/**
 * Controls mirroring for a single remote cluster.
 */
class Replayer {
public:
  Replayer(Threads *threads, RadosRef local_cluster, const peer_t &peer,
	   const std::vector<const char*> &args);
  ~Replayer();
  Replayer(const Replayer&) = delete;
  Replayer& operator=(const Replayer&) = delete;

  int init();
  void run();

  void print_status(Formatter *f, stringstream *ss);
  void start();
  void stop();
  void restart();
  void flush();

private:
  typedef PoolWatcher::ImageIds ImageIds;
  typedef PoolWatcher::PoolImageIds PoolImageIds;

  void set_sources(const PoolImageIds &pool_image_ids);

  void start_image_replayer(unique_ptr<ImageReplayer<> > &image_replayer);
  bool stop_image_replayer(unique_ptr<ImageReplayer<> > &image_replayer);

  int mirror_image_status_init(int64_t pool_id, librados::IoCtx& ioctx);
  void mirror_image_status_shut_down(int64_t pool_id);

  Threads *m_threads;
  Mutex m_lock;
  Cond m_cond;
  atomic_t m_stopping;
  bool m_manual_stop = false;

  peer_t m_peer;
  std::vector<const char*> m_args;
  RadosRef m_local, m_remote;
  std::unique_ptr<PoolWatcher> m_pool_watcher;
  // index by pool so it's easy to tell what is affected
  // when a pool's configuration changes
  std::map<int64_t, std::map<std::string,
			     std::unique_ptr<ImageReplayer<> > > > m_images;
  std::map<int64_t, std::unique_ptr<MirrorStatusWatchCtx> > m_status_watchers;
  ReplayerAdminSocketHook *m_asok_hook;

  class ReplayerThread : public Thread {
    Replayer *m_replayer;
  public:
    ReplayerThread(Replayer *replayer) : m_replayer(replayer) {}
    void *entry() {
      m_replayer->run();
      return 0;
    }
  } m_replayer_thread;
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_REPLAYER_H
