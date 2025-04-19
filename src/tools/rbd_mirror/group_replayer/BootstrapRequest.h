// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_GROUP_REPLAYER_BOOTSTRAP_REQUEST_H
#define RBD_MIRROR_GROUP_REPLAYER_BOOTSTRAP_REQUEST_H

#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include "tools/rbd_mirror/CancelableRequest.h"
#include "tools/rbd_mirror/group_replayer/Types.h"

#include <atomic>
#include <list>
#include <set>
#include <string>


class Context;

namespace journal { struct CacheManagerHandler; }
namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {

struct GroupCtx;
template <typename> struct ImageReplayer;
template <typename> struct InstanceWatcher;
template <typename> struct MirrorStatusUpdater;
struct PoolMetaCache;
template <typename> struct Threads;

namespace group_replayer {

template <typename> class GroupStateBuilder;

template <typename ImageCtxT = librbd::ImageCtx>
class BootstrapRequest : public CancelableRequest {
public:
  static BootstrapRequest *create(
      Threads<ImageCtxT> *threads,
      librados::IoCtx &local_io_ctx,
      librados::IoCtx &remote_io_ctx,
      const std::string &global_group_id,
      const std::string &local_mirror_uuid,
      InstanceWatcher<ImageCtxT> *instance_watcher,
      MirrorStatusUpdater<ImageCtxT> *local_status_updater,
      MirrorStatusUpdater<ImageCtxT> *remote_status_updater,
      journal::CacheManagerHandler *cache_manager_handler,
      PoolMetaCache *pool_meta_cache,
      bool *resync_requested,
      GroupCtx *local_group_ctx,
      std::list<std::pair<librados::IoCtx, ImageReplayer<ImageCtxT> *>> *image_replayers,
      GroupStateBuilder<ImageCtxT> **state_builder,
      Context *on_finish) {
    return new BootstrapRequest(
      threads, local_io_ctx, remote_io_ctx, global_group_id, local_mirror_uuid,
      instance_watcher, local_status_updater, remote_status_updater,
      cache_manager_handler, pool_meta_cache, resync_requested,
      local_group_ctx, image_replayers, state_builder, on_finish);
  }

  BootstrapRequest(
      Threads<ImageCtxT> *threads,
      librados::IoCtx &local_io_ctx,
      librados::IoCtx &remote_io_ctx,
      const std::string &global_group_id,
      const std::string &local_mirror_uuid,
      InstanceWatcher<ImageCtxT> *instance_watcher,
      MirrorStatusUpdater<ImageCtxT> *local_status_updater,
      MirrorStatusUpdater<ImageCtxT> *remote_status_updater,
      journal::CacheManagerHandler *cache_manager_handler,
      PoolMetaCache *pool_meta_cache,
      bool *resync_requested,
      GroupCtx *local_group_ctx,
      std::list<std::pair<librados::IoCtx, ImageReplayer<ImageCtxT> *>> *image_replayers,
      GroupStateBuilder<ImageCtxT> **state_builder,
      Context* on_finish);

  void send() override;
  void cancel() override;

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v                           (error)
   * PREPARE_LOCAL_GROUP  * * * * * * * * * * *
   *    |                                     *
   *    v                            (error)  *
   * PREPARE_REMOTE_GROUP_NAME  * * * * * * * *
   *    |                                     *
   *    | (remote dne)                        *
   *    \------------> REMOVE_LOCAL_GROUP * * *
   *    |             (if local non-primary)  *
   *    |                                     *
   *    | (local dne)                         *
   *    \------------> CREATE_LOCAL_GROUP * * *
   *    |              (if remote primary)    *
   *    v                       |             *
   * CREATE_IMAGE_REPLAYERS <---/             *
   *    |                                     *
   *    v                                     *
   * <finish> < * * * * * * * * * * * * * * * *
   *
   * @endverbatim
   */

  Threads<ImageCtxT>* m_threads;
  librados::IoCtx &m_local_io_ctx;
  librados::IoCtx &m_remote_io_ctx;
  std::string m_global_group_id;
  std::string m_local_mirror_uuid;
  InstanceWatcher<ImageCtxT> *m_instance_watcher;
  MirrorStatusUpdater<ImageCtxT> *m_local_status_updater;
  MirrorStatusUpdater<ImageCtxT> *m_remote_status_updater;
  journal::CacheManagerHandler *m_cache_manager_handler;
  PoolMetaCache *m_pool_meta_cache;
  bool *m_resync_requested;
  GroupCtx *m_local_group_ctx;
  std::list<std::pair<librados::IoCtx, ImageReplayer<ImageCtxT> *>> *m_image_replayers;
  GroupStateBuilder<ImageCtxT> **m_state_builder = nullptr;
  Context *m_on_finish;

  mutable ceph::mutex m_lock;
  std::atomic<bool> m_canceled = false;

  std::string m_local_group_name;
  std::string m_prepare_local_group_name;
  std::string m_prepare_remote_group_name;
  bool m_local_group_removed = false;

  bufferlist m_out_bl;

  void prepare_local_group();
  void handle_prepare_local_group(int r);

  void prepare_remote_group();
  void handle_prepare_remote_group(int r);

  void get_local_group_meta();
  void handle_get_local_group_meta(int r);

  void create_local_group();
  void handle_create_local_group(int r);

  void remove_local_group();
  void handle_remove_local_group(int r);

  int create_replayers();

  void finish(int r);
};

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::group_replayer::BootstrapRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_GROUP_REPLAYER_BOOTSTRAP_REQUEST_H
