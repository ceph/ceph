// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_INSTANCE_REPLAYER_H
#define RBD_MIRROR_INSTANCE_REPLAYER_H

#include <map>
#include <sstream>

#include "common/AsyncOpTracker.h"
#include "common/Formatter.h"
#include "common/ceph_mutex.h"
#include "tools/rbd_mirror/Types.h"

namespace journal { struct CacheManagerHandler; }

namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

template <typename> class ImageReplayer;
template <typename> class InstanceWatcher;
template <typename> class MirrorStatusUpdater;
template <typename> struct PoolMetaCache;
template <typename> class ServiceDaemon;
template <typename> struct Threads;

template <typename ImageCtxT = librbd::ImageCtx>
class InstanceReplayer {
public:
  typedef std::set<Peer<ImageCtxT>> Peers;
  static InstanceReplayer* create(
      librados::IoCtx &local_io_ctx, const std::string &local_mirror_uuid,
      Threads<ImageCtxT> *threads, ServiceDaemon<ImageCtxT> *service_daemon,
      MirrorStatusUpdater<ImageCtxT>* local_status_updater,
      journal::CacheManagerHandler *cache_manager_handler,
      PoolMetaCache<ImageCtxT>* pool_meta_cache) {
    return new InstanceReplayer(local_io_ctx, local_mirror_uuid, threads,
                                service_daemon, local_status_updater,
                                cache_manager_handler, pool_meta_cache);
  }
  void destroy() {
    delete this;
  }

  InstanceReplayer(librados::IoCtx &local_io_ctx,
                   const std::string &local_mirror_uuid,
                   Threads<ImageCtxT> *threads,
                   ServiceDaemon<ImageCtxT> *service_daemon,
                   MirrorStatusUpdater<ImageCtxT>* local_status_updater,
                   journal::CacheManagerHandler *cache_manager_handler,
                   PoolMetaCache<ImageCtxT>* pool_meta_cache);
  ~InstanceReplayer();

  bool is_blocklisted() const;

  int init();
  void shut_down();

  void init(Context *on_finish);
  void shut_down(Context *on_finish);

  void add_peer(const Peer<ImageCtxT>& peer);
  void remove_peer(const Peer<ImageCtxT>& peer, Context *on_finish);

  void acquire_image(InstanceWatcher<ImageCtxT> *instance_watcher,
                     const std::string &global_image_id, Context *on_finish);
  void release_image(const std::string &global_image_id, Context *on_finish);
  void remove_peer_image(const std::string &global_image_id,
                         const std::string &peer_mirror_uuid,
                         Context *on_finish);

  void release_all(Context *on_finish);

  void print_status(Formatter *f);
  void start();
  void stop();
  void restart();
  void flush();

  void stop(Context *on_finish);

private:
  /**
   * @verbatim
   *
   * <uninitialized> <-------------------\
   *    | (init)                         |                    (repeat for each
   *    v                             STOP_IMAGE_REPLAYER ---\ image replayer)
   * SCHEDULE_IMAGE_STATE_CHECK_TASK     ^         ^         |
   *    |                                |         |         |
   *    v          (shut_down)           |         \---------/
   * <initialized> -----------------> WAIT_FOR_OPS
   *
   * @endverbatim
   */
  librados::IoCtx &m_local_io_ctx;
  std::string m_local_mirror_uuid;
  Threads<ImageCtxT> *m_threads;
  ServiceDaemon<ImageCtxT> *m_service_daemon;
  MirrorStatusUpdater<ImageCtxT>* m_local_status_updater;
  journal::CacheManagerHandler *m_cache_manager_handler;
  PoolMetaCache<ImageCtxT>* m_pool_meta_cache;

  mutable ceph::mutex m_lock;
  AsyncOpTracker m_async_op_tracker;
  std::map<std::string, ImageReplayer<ImageCtxT> *> m_image_replayers;
  Peers m_peers;
  Context *m_image_state_check_task = nullptr;
  Context *m_on_shut_down = nullptr;
  bool m_manual_stop = false;
  bool m_blocklisted = false;

  void wait_for_ops();
  void handle_wait_for_ops(int r);

  void start_image_replayer(ImageReplayer<ImageCtxT> *image_replayer,
                            Context *on_finish);
  void queue_start_image_replayers();
  void start_image_replayers(int r);

  void stop_image_replayer(ImageReplayer<ImageCtxT> *image_replayer,
                           Context *on_finish);

  void stop_image_replayers();
  void handle_stop_image_replayers(int r);

  void schedule_image_state_check_task();
  void cancel_image_state_check_task();
};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::InstanceReplayer<librbd::ImageCtx>;

#endif // RBD_MIRROR_INSTANCE_REPLAYER_H
