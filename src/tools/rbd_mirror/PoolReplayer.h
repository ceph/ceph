// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_POOL_REPLAYER_H
#define CEPH_RBD_MIRROR_POOL_REPLAYER_H

#include "common/AsyncOpTracker.h"
#include "common/Cond.h"
#include "common/Mutex.h"
#include "common/WorkQueue.h"
#include "include/rados/librados.hpp"

#include "ClusterWatcher.h"
#include "LeaderWatcher.h"
#include "PoolWatcher.h"
#include "ImageDeleter.h"
#include "types.h"
#include "tools/rbd_mirror/image_map/Types.h"
#include "tools/rbd_mirror/leader_watcher/Types.h"
#include "tools/rbd_mirror/pool_watcher/Types.h"
#include "tools/rbd_mirror/service_daemon/Types.h"

#include <set>
#include <map>
#include <memory>
#include <atomic>
#include <string>
#include <vector>

class AdminSocketHook;

namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> class ImageMap;
template <typename> class InstanceReplayer;
template <typename> class InstanceWatcher;
template <typename> class ServiceDaemon;
template <typename> struct Threads;

/**
 * Controls mirroring for a single remote cluster.
 */
template <typename ImageCtxT = librbd::ImageCtx>
class PoolReplayer {
public:
  PoolReplayer(Threads<ImageCtxT> *threads,
               ServiceDaemon<ImageCtxT>* service_daemon,
	       int64_t local_pool_id, const peer_t &peer,
	       const std::vector<const char*> &args);
  ~PoolReplayer();
  PoolReplayer(const PoolReplayer&) = delete;
  PoolReplayer& operator=(const PoolReplayer&) = delete;

  bool is_blacklisted() const;
  bool is_leader() const;
  bool is_running() const;

  void init();
  void shut_down();

  void run();

  void print_status(Formatter *f, stringstream *ss);
  void start();
  void stop(bool manual);
  void restart();
  void flush();
  void release_leader();
  void reopen_logs();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   *  INIT
   *    |
   *    v
   * <follower> <-------------------------\
   *    .                                 |
   *    .                                 |
   *    v (leader acquired)               |
   * INIT_IMAGE_MAP             SHUT_DOWN_IMAGE_MAP
   *    |                                 ^
   *    v                                 |
   * INIT_LOCAL_POOL_WATCHER    WAIT_FOR_NOTIFICATIONS
   *    |                                 ^
   *    v                                 |
   * INIT_REMOTE_POOL_WATCHER   SHUT_DOWN_POOL_WATCHERS
   *    |                                 ^
   *    v                                 |
   * INIT_IMAGE_DELETER         SHUT_DOWN_IMAGE_DELETER
   *    |                                 ^
   *    v                                 .
   * <leader> <-----------\               .
   *    .                 |               .
   *    . (image update)  |               .
   *    . . > NOTIFY_INSTANCE_WATCHER     .
   *    .                                 .
   *    . (leader lost / shut down)       .
   *    . . . . . . . . . . . . . . . . . .
   *
   * @endverbatim
   */

  typedef std::vector<std::string> InstanceIds;

  struct PoolWatcherListener : public pool_watcher::Listener {
    PoolReplayer *pool_replayer;
    bool local;

    PoolWatcherListener(PoolReplayer *pool_replayer, bool local)
      : pool_replayer(pool_replayer), local(local) {
    }

    void handle_update(const std::string &mirror_uuid,
                       ImageIds &&added_image_ids,
                       ImageIds &&removed_image_ids) override {
      pool_replayer->handle_update((local ? "" : mirror_uuid),
				   std::move(added_image_ids),
                                   std::move(removed_image_ids));
    }
  };

  struct ImageMapListener : public image_map::Listener {
    PoolReplayer *pool_replayer;

    ImageMapListener(PoolReplayer *pool_replayer)
      : pool_replayer(pool_replayer) {
    }

    void acquire_image(const std::string &global_image_id,
                       const std::string &instance_id,
                       Context* on_finish) override {
      pool_replayer->handle_acquire_image(global_image_id, instance_id,
                                          on_finish);
    }

    void release_image(const std::string &global_image_id,
                       const std::string &instance_id,
                       Context* on_finish) override {
      pool_replayer->handle_release_image(global_image_id, instance_id,
                                          on_finish);
    }

    void remove_image(const std::string &mirror_uuid,
                      const std::string &global_image_id,
                      const std::string &instance_id,
                      Context* on_finish) override {
      pool_replayer->handle_remove_image(mirror_uuid, global_image_id,
                                         instance_id, on_finish);
    }
  };

  void handle_update(const std::string &mirror_uuid,
                     ImageIds &&added_image_ids,
                     ImageIds &&removed_image_ids);

  int init_rados(const std::string &cluster_name,
                 const std::string &client_name,
                 const std::string &description, RadosRef *rados_ref,
                 bool strip_cluster_overrides);

  void handle_post_acquire_leader(Context *on_finish);
  void handle_pre_release_leader(Context *on_finish);

  void init_image_map(Context *on_finish);
  void handle_init_image_map(int r, Context *on_finish);

  void init_local_pool_watcher(Context *on_finish);
  void handle_init_local_pool_watcher(int r, Context *on_finish);

  void init_remote_pool_watcher(Context *on_finish);
  void handle_init_remote_pool_watcher(int r, Context *on_finish);

  void init_image_deleter(Context* on_finish);
  void handle_init_image_deleter(int r, Context* on_finish);

  void shut_down_image_deleter(Context* on_finish);
  void handle_shut_down_image_deleter(int r, Context* on_finish);

  void shut_down_pool_watchers(Context *on_finish);
  void handle_shut_down_pool_watchers(int r, Context *on_finish);

  void wait_for_update_ops(Context *on_finish);
  void handle_wait_for_update_ops(int r, Context *on_finish);

  void shut_down_image_map(Context *on_finish);
  void handle_shut_down_image_map(int r, Context *on_finish);

  void handle_update_leader(const std::string &leader_instance_id);

  void handle_acquire_image(const std::string &global_image_id,
                            const std::string &instance_id,
                            Context* on_finish);
  void handle_release_image(const std::string &global_image_id,
                            const std::string &instance_id,
                            Context* on_finish);
  void handle_remove_image(const std::string &mirror_uuid,
                           const std::string &global_image_id,
                           const std::string &instance_id,
                           Context* on_finish);

  void handle_instances_added(const InstanceIds &instance_ids);
  void handle_instances_removed(const InstanceIds &instance_ids);

  Threads<ImageCtxT> *m_threads;
  ServiceDaemon<ImageCtxT>* m_service_daemon;
  int64_t m_local_pool_id = -1;
  peer_t m_peer;
  std::vector<const char*> m_args;

  mutable Mutex m_lock;
  Cond m_cond;
  std::atomic<bool> m_stopping = { false };
  bool m_manual_stop = false;
  bool m_blacklisted = false;

  RadosRef m_local_rados;
  RadosRef m_remote_rados;

  librados::IoCtx m_local_io_ctx;
  librados::IoCtx m_remote_io_ctx;

  PoolWatcherListener m_local_pool_watcher_listener;
  std::unique_ptr<PoolWatcher<ImageCtxT>> m_local_pool_watcher;

  PoolWatcherListener m_remote_pool_watcher_listener;
  std::unique_ptr<PoolWatcher<ImageCtxT>> m_remote_pool_watcher;

  std::unique_ptr<InstanceReplayer<ImageCtxT>> m_instance_replayer;
  std::unique_ptr<ImageDeleter<ImageCtxT>> m_image_deleter;

  ImageMapListener m_image_map_listener;
  std::unique_ptr<ImageMap<librbd::ImageCtx>> m_image_map;

  std::string m_asok_hook_name;
  AdminSocketHook *m_asok_hook = nullptr;

  service_daemon::CalloutId m_callout_id = service_daemon::CALLOUT_ID_NONE;

  class PoolReplayerThread : public Thread {
    PoolReplayer *m_pool_replayer;
  public:
    PoolReplayerThread(PoolReplayer *pool_replayer)
      : m_pool_replayer(pool_replayer) {
    }
    void *entry() override {
      m_pool_replayer->run();
      return 0;
    }
  } m_pool_replayer_thread;

  class LeaderListener : public leader_watcher::Listener {
  public:
    LeaderListener(PoolReplayer *pool_replayer)
      : m_pool_replayer(pool_replayer) {
    }

  protected:
    void post_acquire_handler(Context *on_finish) override {
      m_pool_replayer->handle_post_acquire_leader(on_finish);
    }

    void pre_release_handler(Context *on_finish) override {
      m_pool_replayer->handle_pre_release_leader(on_finish);
    }

    void update_leader_handler(
      const std::string &leader_instance_id) override {
      m_pool_replayer->handle_update_leader(leader_instance_id);
    }

    void handle_instances_added(const InstanceIds& instance_ids) override {
      m_pool_replayer->handle_instances_added(instance_ids);
    }

    void handle_instances_removed(const InstanceIds& instance_ids) override {
      m_pool_replayer->handle_instances_removed(instance_ids);
    }

  private:
    PoolReplayer *m_pool_replayer;
  } m_leader_listener;

  std::unique_ptr<LeaderWatcher<ImageCtxT>> m_leader_watcher;
  std::unique_ptr<InstanceWatcher<ImageCtxT>> m_instance_watcher;
  AsyncOpTracker m_update_op_tracker;
};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::PoolReplayer<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_POOL_REPLAYER_H
