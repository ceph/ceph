// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_NAMESPACE_REPLAYER_H
#define CEPH_RBD_MIRROR_NAMESPACE_REPLAYER_H

#include "common/AsyncOpTracker.h"
#include "common/ceph_mutex.h"
#include "include/rados/librados.hpp"

#include "tools/rbd_mirror/ImageDeleter.h"
#include "tools/rbd_mirror/ImageMap.h"
#include "tools/rbd_mirror/InstanceReplayer.h"
#include "tools/rbd_mirror/InstanceWatcher.h"
#include "tools/rbd_mirror/MirrorStatusUpdater.h"
#include "tools/rbd_mirror/PoolWatcher.h"
#include "tools/rbd_mirror/Types.h"
#include "tools/rbd_mirror/image_map/Types.h"
#include "tools/rbd_mirror/pool_watcher/Types.h"

#include <memory>
#include <string>
#include <vector>

class AdminSocketHook;

namespace journal { struct CacheManagerHandler; }

namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

struct PoolMetaCache;
template <typename> class ServiceDaemon;
template <typename> class Throttler;
template <typename> struct Threads;

/**
 * Controls mirroring for a single remote cluster.
 */
template <typename ImageCtxT = librbd::ImageCtx>
class NamespaceReplayer {
public:
  static NamespaceReplayer *create(
      const std::string &name,
      librados::IoCtx &local_ioctx,
      librados::IoCtx &remote_ioctx,
      const std::string &local_mirror_uuid,
      const std::string &local_mirror_peer_uuid,
      const RemotePoolMeta& remote_pool_meta,
      Threads<ImageCtxT> *threads,
      Throttler<ImageCtxT> *image_sync_throttler,
      Throttler<ImageCtxT> *image_deletion_throttler,
      ServiceDaemon<ImageCtxT> *service_daemon,
      journal::CacheManagerHandler *cache_manager_handler,
      PoolMetaCache* pool_meta_cache) {
    return new NamespaceReplayer(name, local_ioctx, remote_ioctx,
                                 local_mirror_uuid, local_mirror_peer_uuid,
                                 remote_pool_meta, threads,
                                 image_sync_throttler, image_deletion_throttler,
                                 service_daemon, cache_manager_handler,
                                 pool_meta_cache);
  }

  NamespaceReplayer(const std::string &name,
                    librados::IoCtx &local_ioctx,
                    librados::IoCtx &remote_ioctx,
                    const std::string &local_mirror_uuid,
                    const std::string& local_mirror_peer_uuid,
                    const RemotePoolMeta& remote_pool_meta,
                    Threads<ImageCtxT> *threads,
                    Throttler<ImageCtxT> *image_sync_throttler,
                    Throttler<ImageCtxT> *image_deletion_throttler,
                    ServiceDaemon<ImageCtxT> *service_daemon,
                    journal::CacheManagerHandler *cache_manager_handler,
                    PoolMetaCache* pool_meta_cache);
  NamespaceReplayer(const NamespaceReplayer&) = delete;
  NamespaceReplayer& operator=(const NamespaceReplayer&) = delete;

  bool is_blacklisted() const;

  void init(Context *on_finish);
  void shut_down(Context *on_finish);

  void handle_acquire_leader(Context *on_finish);
  void handle_release_leader(Context *on_finish);
  void handle_update_leader(const std::string &leader_instance_id);
  void handle_instances_added(const std::vector<std::string> &instance_ids);
  void handle_instances_removed(const std::vector<std::string> &instance_ids);

  void print_status(Formatter *f);
  void start();
  void stop();
  void restart();
  void flush();

private:
  /**
   * @verbatim
   *
   * <uninitialized> <------------------------------------\
   *    | (init)                 ^ (error)                |
   *    v                        *                        |
   * INIT_LOCAL_STATUS_UPDATER * *   * * * * * * > SHUT_DOWN_LOCAL_STATUS_UPDATER
   *    |                            * (error)            ^
   *    v                            *                    |
   * INIT_REMOTE_STATUS_UPDATER  * * *   * * * * > SHUT_DOWN_REMOTE_STATUS_UPDATER
   *    |                                * (error)        ^
   *    v                                *                |
   * INIT_INSTANCE_REPLAYER  * * * * * * *   * * > SHUT_DOWN_INSTANCE_REPLAYER
   *    |                                    *            ^
   *    v                                    *            |
   * INIT_INSTANCE_WATCHER * * * * * * * * * *     SHUT_DOWN_INSTANCE_WATCHER
   *    |                       (error)                   ^
   *    |                                                 |
   *    v                                          STOP_INSTANCE_REPLAYER
   *    |                                                 ^
   *    |    (shut down)                                  |
   *    |  /----------------------------------------------/
   *    v  |
   * <follower> <---------------------------\
   *    .                                   |
   *    .                                   |
   *    v (leader acquired)                 |
   * INIT_IMAGE_MAP                         |
   *    |                                   |
   *    v                                   |
   * INIT_LOCAL_POOL_WATCHER             SHUT_DOWN_IMAGE_MAP
   *    |                                   ^
   *    v                                   |
   * INIT_REMOTE_POOL_WATCHER            SHUT_DOWN_POOL_WATCHERS
   *    |                                   ^
   *    v                                   |
   * INIT_IMAGE_DELETER                  SHUT_DOWN_IMAGE_DELETER
   *    |                                   ^
   *    v                                   .
   * <leader> <-----------\                 .
   *    .                 |                 .
   *    . (image update)  |                 .
   *    . . > NOTIFY_INSTANCE_WATCHER       .
   *    .                                   .
   *    . (leader lost / shut down)         .
   *    . . . . . . . . . . . . . . . . . . .
   *
   * @endverbatim
   */

  struct PoolWatcherListener : public pool_watcher::Listener {
    NamespaceReplayer *namespace_replayer;
    bool local;

    PoolWatcherListener(NamespaceReplayer *namespace_replayer, bool local)
      : namespace_replayer(namespace_replayer), local(local) {
    }

    void handle_update(const std::string &mirror_uuid,
                       ImageIds &&added_image_ids,
                       ImageIds &&removed_image_ids) override {
      namespace_replayer->handle_update((local ? "" : mirror_uuid),
				   std::move(added_image_ids),
                                   std::move(removed_image_ids));
    }
  };

  struct ImageMapListener : public image_map::Listener {
    NamespaceReplayer *namespace_replayer;

    ImageMapListener(NamespaceReplayer *namespace_replayer)
      : namespace_replayer(namespace_replayer) {
    }

    void acquire_image(const std::string &global_image_id,
                       const std::string &instance_id,
                       Context* on_finish) override {
      namespace_replayer->handle_acquire_image(global_image_id, instance_id,
                                          on_finish);
    }

    void release_image(const std::string &global_image_id,
                       const std::string &instance_id,
                       Context* on_finish) override {
      namespace_replayer->handle_release_image(global_image_id, instance_id,
                                          on_finish);
    }

    void remove_image(const std::string &mirror_uuid,
                      const std::string &global_image_id,
                      const std::string &instance_id,
                      Context* on_finish) override {
      namespace_replayer->handle_remove_image(mirror_uuid, global_image_id,
                                         instance_id, on_finish);
    }
  };

  void handle_update(const std::string &mirror_uuid,
                     ImageIds &&added_image_ids,
                     ImageIds &&removed_image_ids);

  int init_rados(const std::string &cluster_name,
                 const std::string &client_name,
                 const std::string &mon_host,
                 const std::string &key,
                 const std::string &description, RadosRef *rados_ref,
                 bool strip_cluster_overrides);

  void init_local_status_updater();
  void handle_init_local_status_updater(int r);

  void init_remote_status_updater();
  void handle_init_remote_status_updater(int r);

  void init_instance_replayer();
  void handle_init_instance_replayer(int r);

  void init_instance_watcher();
  void handle_init_instance_watcher(int r);

  void stop_instance_replayer();
  void handle_stop_instance_replayer(int r);

  void shut_down_instance_watcher();
  void handle_shut_down_instance_watcher(int r);

  void shut_down_instance_replayer();
  void handle_shut_down_instance_replayer(int r);

  void shut_down_remote_status_updater();
  void handle_shut_down_remote_status_updater(int r);

  void shut_down_local_status_updater();
  void handle_shut_down_local_status_updater(int r);

  void init_image_map(Context *on_finish);
  void handle_init_image_map(int r, ImageMap<ImageCtxT> *image_map,
                             Context *on_finish);

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

  void shut_down_image_map(Context *on_finish);
  void handle_shut_down_image_map(int r, Context *on_finish);

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

  std::string m_namespace_name;
  librados::IoCtx m_local_io_ctx;
  librados::IoCtx m_remote_io_ctx;
  std::string m_local_mirror_uuid;
  std::string m_local_mirror_peer_uuid;
  RemotePoolMeta m_remote_pool_meta;
  Threads<ImageCtxT> *m_threads;
  Throttler<ImageCtxT> *m_image_sync_throttler;
  Throttler<ImageCtxT> *m_image_deletion_throttler;
  ServiceDaemon<ImageCtxT> *m_service_daemon;
  journal::CacheManagerHandler *m_cache_manager_handler;
  PoolMetaCache* m_pool_meta_cache;

  mutable ceph::mutex m_lock;

  int m_ret_val = 0;
  Context *m_on_finish = nullptr;

  std::unique_ptr<MirrorStatusUpdater<ImageCtxT>> m_local_status_updater;
  std::unique_ptr<MirrorStatusUpdater<ImageCtxT>> m_remote_status_updater;

  PoolWatcherListener m_local_pool_watcher_listener;
  std::unique_ptr<PoolWatcher<ImageCtxT>> m_local_pool_watcher;

  PoolWatcherListener m_remote_pool_watcher_listener;
  std::unique_ptr<PoolWatcher<ImageCtxT>> m_remote_pool_watcher;

  std::unique_ptr<InstanceReplayer<ImageCtxT>> m_instance_replayer;
  std::unique_ptr<ImageDeleter<ImageCtxT>> m_image_deleter;

  ImageMapListener m_image_map_listener;
  std::unique_ptr<ImageMap<ImageCtxT>> m_image_map;

  std::unique_ptr<InstanceWatcher<ImageCtxT>> m_instance_watcher;
};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::NamespaceReplayer<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_NAMESPACE_REPLAYER_H
