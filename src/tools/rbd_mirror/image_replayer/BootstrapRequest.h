// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_BOOTSTRAP_REQUEST_H
#define RBD_MIRROR_IMAGE_REPLAYER_BOOTSTRAP_REQUEST_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "common/ceph_mutex.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/mirror/Types.h"
#include "tools/rbd_mirror/CancelableRequest.h"
#include "tools/rbd_mirror/Types.h"
#include <string>

class Context;
class SafeTimer;

namespace journal { class CacheManagerHandler; }
namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

class ProgressContext;

template <typename> class ImageSync;
template <typename> class InstanceWatcher;
struct PoolMetaCache;
template <typename> struct Threads;

namespace image_replayer {

template <typename> class StateBuilder;

template <typename ImageCtxT = librbd::ImageCtx>
class BootstrapRequest : public CancelableRequest {
public:
  typedef rbd::mirror::ProgressContext ProgressContext;

  static BootstrapRequest* create(
      Threads<ImageCtxT>* threads,
      librados::IoCtx& local_io_ctx,
      librados::IoCtx& remote_io_ctx,
      InstanceWatcher<ImageCtxT>* instance_watcher,
      const std::string& global_image_id,
      const std::string& local_mirror_uuid,
      const RemotePoolMeta& remote_pool_meta,
      ::journal::CacheManagerHandler* cache_manager_handler,
      PoolMetaCache* pool_meta_cache,
      ProgressContext* progress_ctx,
      StateBuilder<ImageCtxT>** state_builder,
      bool* do_resync,
      Context* on_finish) {
    return new BootstrapRequest(
      threads, local_io_ctx, remote_io_ctx, instance_watcher, global_image_id,
      local_mirror_uuid, remote_pool_meta, cache_manager_handler,
      pool_meta_cache, progress_ctx, state_builder, do_resync, on_finish);
  }

  BootstrapRequest(
      Threads<ImageCtxT>* threads,
      librados::IoCtx& local_io_ctx,
      librados::IoCtx& remote_io_ctx,
      InstanceWatcher<ImageCtxT>* instance_watcher,
      const std::string& global_image_id,
      const std::string& local_mirror_uuid,
      const RemotePoolMeta& remote_pool_meta,
      ::journal::CacheManagerHandler* cache_manager_handler,
      PoolMetaCache* pool_meta_cache,
      ProgressContext* progress_ctx,
      StateBuilder<ImageCtxT>** state_builder,
      bool* do_resync,
      Context* on_finish);

  bool is_syncing() const;

  void send() override;
  void cancel() override;

  std::string get_local_image_name() const;

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v                                           (error)
   * PREPARE_LOCAL_IMAGE  * * * * * * * * * * * * * * * * * *
   *    |                                                   *
   *    v                                           (error) *
   * PREPARE_REMOTE_IMAGE * * * * * * * * * * * * * * * * * *
   *    |                                                   *
   *    v                                           (error) *
   * OPEN_REMOTE_IMAGE  * * * * * * * * * * * * * * * * * * *
   *    |                                                   *
   *    |                                                   *
   *    \----> CREATE_LOCAL_IMAGE * * * * * * * * * * * *   *
   *    |         |       ^                             *   *
   *    |         |       .                             *   *
   *    |         v       . (image DNE)                 *   *
   *    \----> OPEN_LOCAL_IMAGE * * * * * * * * * * * * *   *
   *              |                                     *   *
   *              |                                     *   *
   *              v                                     *   *
   *           PREPARE_REPLAY * * * * * * * * * * * * * *   *
   *              |                                     *   *
   *              |                                     *   *
   *              v (skip if not needed)                *   *
   *           IMAGE_SYNC * * * * * * * * * * * * * * * *   *
   *              |                                     *   *
   *              |                                     *   *
   *    /---------/                                     *   *
   *    |                                               *   *
   *    v                                               *   *
   * CLOSE_REMOTE_IMAGE < * * * * * * * * * * * * * * * *   *
   *    |                                                   *
   *    v                                                   *
   * <finish> < * * * * * * * * * * * * * * * * * * * * * * *
   *
   * @endverbatim
   */
  Threads<ImageCtxT>* m_threads;
  librados::IoCtx &m_local_io_ctx;
  librados::IoCtx &m_remote_io_ctx;
  InstanceWatcher<ImageCtxT> *m_instance_watcher;
  std::string m_global_image_id;
  std::string m_local_mirror_uuid;
  RemotePoolMeta m_remote_pool_meta;
  ::journal::CacheManagerHandler *m_cache_manager_handler;
  PoolMetaCache* m_pool_meta_cache;
  ProgressContext *m_progress_ctx;
  StateBuilder<ImageCtxT>** m_state_builder;
  bool *m_do_resync;

  mutable ceph::mutex m_lock;
  bool m_canceled = false;

  int m_ret_val = 0;

  std::string m_local_image_name;
  std::string m_prepare_local_image_name;

  bool m_syncing = false;
  ImageSync<ImageCtxT> *m_image_sync = nullptr;

  void prepare_local_image();
  void handle_prepare_local_image(int r);

  void prepare_remote_image();
  void handle_prepare_remote_image(int r);

  void open_remote_image();
  void handle_open_remote_image(int r);

  void open_local_image();
  void handle_open_local_image(int r);

  void create_local_image();
  void handle_create_local_image(int r);

  void prepare_replay();
  void handle_prepare_replay(int r);

  void image_sync();
  void handle_image_sync(int r);

  void close_remote_image();
  void handle_close_remote_image(int r);

  void update_progress(const std::string &description);
};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::BootstrapRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_BOOTSTRAP_REQUEST_H
