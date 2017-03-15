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
#include "LeaderWatcher.h"
#include "PoolWatcher.h"
#include "ImageDeleter.h"
#include "types.h"

class AdminSocketHook;

namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> struct Threads;
template <typename> class InstanceWatcher;

/**
 * Controls mirroring for a single remote cluster.
 */
class Replayer {
public:
  Replayer(Threads<librbd::ImageCtx> *threads,
           std::shared_ptr<ImageDeleter> image_deleter,
           ImageSyncThrottlerRef<> image_sync_throttler,
           int64_t local_pool_id, const peer_t &peer,
           const std::vector<const char*> &args);
  ~Replayer();
  Replayer(const Replayer&) = delete;
  Replayer& operator=(const Replayer&) = delete;

  bool is_blacklisted() const;
  bool is_leader() const;

  int init();
  void run();

  void print_status(Formatter *f, stringstream *ss);
  void start();
  void stop(bool manual);
  void restart();
  void flush();
  void release_leader();

private:
  struct PoolWatcherListener : public PoolWatcher<>::Listener {
    Replayer *replayer;

    PoolWatcherListener(Replayer *replayer) : replayer(replayer) {
    }

    void handle_update(const ImageIds &added_image_ids,
                       const ImageIds &removed_image_ids) override {
      replayer->handle_update(added_image_ids, removed_image_ids);
    }
  };

  struct C_RefreshLocalImages;

  void handle_update(const ImageIds &added_image_ids,
                     const ImageIds &removed_image_ids);

  void start_image_replayer(unique_ptr<ImageReplayer<> > &image_replayer);
  bool stop_image_replayer(unique_ptr<ImageReplayer<> > &image_replayer);
  void stop_image_replayers();
  void stop_image_replayers(Context *on_finish);

  int init_rados(const std::string &cluster_name,
                 const std::string &client_name,
                 const std::string &description, RadosRef *rados_ref);

  void handle_post_acquire_leader(Context *on_finish);
  void handle_pre_release_leader(Context *on_finish);

  void refresh_local_images(Context *on_finish);
  void handle_refresh_local_images(int r, ImageIds &&image_ids,
                                   Context *on_finish);

  void init_pool_watcher(Context *on_finish);
  void shut_down_pool_watcher(Context *on_finish);
  void handle_shut_down_pool_watcher(int r, Context *on_finish);

  Threads<librbd::ImageCtx> *m_threads;
  std::shared_ptr<ImageDeleter> m_image_deleter;
  ImageSyncThrottlerRef<> m_image_sync_throttler;
  mutable Mutex m_lock;
  Cond m_cond;
  atomic_t m_stopping;
  bool m_manual_stop = false;
  bool m_blacklisted = false;

  peer_t m_peer;
  std::vector<const char*> m_args;
  RadosRef m_local_rados;
  RadosRef m_remote_rados;

  librados::IoCtx m_local_io_ctx;
  librados::IoCtx m_remote_io_ctx;

  int64_t m_local_pool_id = -1;
  int64_t m_remote_pool_id = -1;

  std::string m_remote_mirror_uuid;

  PoolWatcherListener m_pool_watcher_listener;
  std::unique_ptr<PoolWatcher<> > m_pool_watcher;

  std::map<std::string, std::unique_ptr<ImageReplayer<> > > m_image_replayers;

  std::string m_asok_hook_name;
  AdminSocketHook *m_asok_hook;

  std::set<ImageId> m_init_image_ids;

  class ReplayerThread : public Thread {
    Replayer *m_replayer;
  public:
    ReplayerThread(Replayer *replayer) : m_replayer(replayer) {}
    void *entry() override {
      m_replayer->run();
      return 0;
    }
  } m_replayer_thread;

  class LeaderListener : public LeaderWatcher<>::Listener {
  public:
    LeaderListener(Replayer *replayer) : m_replayer(replayer) {
    }

  protected:
    void post_acquire_handler(Context *on_finish) override {
      m_replayer->handle_post_acquire_leader(on_finish);
    }

    void pre_release_handler(Context *on_finish) override {
      m_replayer->handle_pre_release_leader(on_finish);
    }

  private:
    Replayer *m_replayer;
  } m_leader_listener;

  std::unique_ptr<LeaderWatcher<> > m_leader_watcher;
  std::unique_ptr<InstanceWatcher<librbd::ImageCtx> > m_instance_watcher;
};

} // namespace mirror
} // namespace rbd

#endif // CEPH_RBD_MIRROR_REPLAYER_H
