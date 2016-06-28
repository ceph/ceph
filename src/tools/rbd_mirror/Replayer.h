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
#include "ImageDeleter.h"
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
  Replayer(Threads *threads, std::shared_ptr<ImageDeleter> image_deleter,
           ImageSyncThrottlerRef<> image_sync_throttler,
           RadosRef local_cluster, int64_t local_pool_id, const peer_t &peer,
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
  typedef PoolWatcher::ImageId ImageId;
  typedef PoolWatcher::ImageIds ImageIds;

  void init_local_mirroring_images();
  void set_sources(const ImageIds &image_ids);

  void start_image_replayer(unique_ptr<ImageReplayer<> > &image_replayer,
                            const std::string &image_id,
                            const boost::optional<std::string>& image_name);
  bool stop_image_replayer(unique_ptr<ImageReplayer<> > &image_replayer);

  int mirror_image_status_init();
  void mirror_image_status_shut_down();

  Threads *m_threads;
  std::shared_ptr<ImageDeleter> m_image_deleter;
  ImageSyncThrottlerRef<> m_image_sync_throttler;
  Mutex m_lock;
  Cond m_cond;
  atomic_t m_stopping;
  bool m_manual_stop = false;

  peer_t m_peer;
  std::vector<const char*> m_args;
  RadosRef m_local;
  RadosRef m_remote;

  librados::IoCtx m_local_io_ctx;
  librados::IoCtx m_remote_io_ctx;

  int64_t m_local_pool_id = -1;
  int64_t m_remote_pool_id = -1;

  std::unique_ptr<PoolWatcher> m_pool_watcher;
  std::map<std::string, std::unique_ptr<ImageReplayer<> > > m_image_replayers;
  std::unique_ptr<MirrorStatusWatchCtx> m_status_watcher;

  std::string m_asok_hook_name;
  ReplayerAdminSocketHook *m_asok_hook;

  struct InitImageInfo {
    std::string global_id;
    std::string id;
    std::string name;

    InitImageInfo(const std::string& global_id, const std::string &id = "",
                  const std::string &name = "")
      : global_id(global_id), id(id), name(name) {
    }

    inline bool operator==(const InitImageInfo &rhs) const {
      return (global_id == rhs.global_id && id == rhs.id && name == rhs.name);
    }
    inline bool operator<(const InitImageInfo &rhs) const {
      return global_id < rhs.global_id;
    }
  };

  std::set<InitImageInfo> m_init_images;

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
