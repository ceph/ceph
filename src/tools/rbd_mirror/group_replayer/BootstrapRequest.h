// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_GROUP_REPLAYER_BOOTSTRAP_REQUEST_H
#define RBD_MIRROR_GROUP_REPLAYER_BOOTSTRAP_REQUEST_H

#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include "tools/rbd_mirror/CancelableRequest.h"

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
      std::string *local_group_id,
      std::string *remote_group_id,
      std::map<std::string, cls::rbd::GroupSnapshot> *local_group_snaps,
      GroupCtx *local_group_ctx,
      std::list<std::pair<librados::IoCtx, ImageReplayer<ImageCtxT> *>> *image_replayers,
      std::map<std::pair<int64_t, std::string>, ImageReplayer<ImageCtxT> *> *image_replayer_index,
      Context *on_finish) {
    return new BootstrapRequest(
      threads, local_io_ctx, remote_io_ctx, global_group_id, local_mirror_uuid,
      instance_watcher, local_status_updater, remote_status_updater,
      cache_manager_handler, pool_meta_cache, resync_requested, local_group_id,
      remote_group_id, local_group_snaps, local_group_ctx, image_replayers,
      image_replayer_index, on_finish);
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
      std::string *local_group_id,
      std::string *remote_group_id,
      std::map<std::string, cls::rbd::GroupSnapshot> *local_group_snaps,
      GroupCtx *local_group_ctx,
      std::list<std::pair<librados::IoCtx, ImageReplayer<ImageCtxT> *>> *image_replayers,
      std::map<std::pair<int64_t, std::string>, ImageReplayer<ImageCtxT> *> *image_replayer_index,
      Context* on_finish);

  void send() override;
  void cancel() override;
  std::string prepare_non_primary_mirror_snap_name(
      const std::string &global_group_id, const std::string &snap_id);

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_REMOTE_GROUP_ID  * * * * * * * * * * *
   *    |                 (noent)             *
   *    v                                     v
   * GET_REMOTE_GROUP_NAME  * * * * * * * * * *
   *    |                 (noent)             *
   *    v                                     v
   * GET_REMOTE_MIRROR_GROUP  * * * * * * * * *
   *    |           (noent or not primary)    *
   *    v                                     v
   * LIST_REMOTE_GROUP_SNAPSHOTS  * * * * * * *
   *    |                             (noent) *
   *    v                                     v
   * LIST_REMOTE_GROUP  * * * * * * * * * * * *
   *    |  (repeat if neeeded)        (noent) *
   *    v                                     v
   * GET_REMOTE_MIRROR_IMAGE  * * * * * * * * *
   *    |  (repeat for every image)   (noent) *
   *    |                                     v
   *    |/< * * * * * * * * * * * * * * * * * *
   *    v
   * GET_LOCAL_GROUP_ID * * * * * * * * * * * *
   *    |               (noent)               *
   *    v                                     *
   * GET_LOCAL_GROUP_NAME                     *
   *    |                                     *
   *    v                                     v
   * GET_LOCAL_MIRROR_GROUP <------------- GET_LOCAL_GROUP_ID_BY_NAME
   *    |                                     * (noent)
   *    v               (noent)               *
   * LIST_LOCAL_GROUP_SNAPSHOTS  * * *        *
   *    |                            *        *
   *    v               (noent)      *        *
   * LIST_LOCAL_GROUP  * * * * * * * *        *
   *    |  (repeat if neeeded)       *        *
   *    v                            *        *
   * GET_LOCAL_MIRROR_IMAGE          *        *
   *    |  (repeat for every image)  *        *
   *    v                            *        *
   * REMOVE_LOCAL_IMAGE_FROM_GROUP   *        *
   *    |                       ^    *        *
   *    v                       |    *        v
   * MOVE_LOCAL_IMAGE_TO_TRASH -/    *     CREATE_LOCAL_GROUP_ID
   *    |         (repeat for every  *        |
   *    |              stale image)  *        v
   *    |\----\                      * * > CREATE_LOCAL_GROUP
   *    |     | (if stale                     |
   *    |     v  or removing)                 |
   *    |  DISABLE_LOCAL_MIRROR_GROUP         |
   *    |     |                               |
   *    |     v                               v
   *    |  REMOVE_LOCAL_MIRROR_GROUP ----> CREATE_LOCAL_MIRROR_GROUP
   *    |     |                   (if stale)  |
   *    |     v                               v
   *    |  REMOVE_LOCAL_GROUP              CREATE_LOCAL_NON_PRIMARY_GROUP_SNAPSHOT
   *    |     |        (if removing)          |
   *    |     v                               |
   *    |  REMOVE_LOCAL_GROUP_ID              |
   *    |     |        (if removing)          |
   *    v     v                               |
   * <finish> <-------------------------------/
   *
   * @endverbatim
   */

  typedef std::pair<int64_t /*pool_id*/, std::string /*global_image_id*/> GlobalImageId;

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
  std::string *m_local_group_id;
  std::string *m_remote_group_id;
  std::map<std::string, cls::rbd::GroupSnapshot> *m_local_group_snaps;
  GroupCtx *m_local_group_ctx;
  std::list<std::pair<librados::IoCtx, ImageReplayer<ImageCtxT> *>> *m_image_replayers;
  std::map<std::pair<int64_t, std::string>, ImageReplayer<ImageCtxT> *> *m_image_replayer_index;
  Context *m_on_finish;

  std::atomic<bool> m_canceled = false;

  std::string m_group_name;
  bool m_local_group_id_by_name = false;
  cls::rbd::MirrorGroup m_remote_mirror_group;
  cls::rbd::MirrorGroup m_local_mirror_group;
  std::vector<cls::rbd::GroupSnapshot> remote_group_snaps;
  std::vector<cls::rbd::GroupSnapshot> local_group_snaps;
  bool m_remote_mirror_group_primary = false;
  bool m_local_mirror_group_primary = false;
  std::list<cls::rbd::GroupImageStatus> m_images;
  librados::IoCtx m_image_io_ctx;

  std::map<GlobalImageId, std::string> m_remote_images;
  std::set<GlobalImageId> m_local_images;
  std::map<GlobalImageId, std::string> m_local_trash_images;

  bufferlist m_out_bl;

  bool has_remote_image(int64_t local_pool_id,
                        const std::string &global_image_id) const;

  void get_remote_group_id();
  void handle_get_remote_group_id(int r);

  void get_remote_group_name();
  void handle_get_remote_group_name(int r);

  void get_remote_mirror_group();
  void handle_get_remote_mirror_group(int r);

  void get_remote_mirror_image();
  void handle_get_remote_mirror_image(int r);

  void list_remote_group_snapshots();
  void handle_list_remote_group_snapshots(int r);

  void list_remote_group();
  void handle_list_remote_group(int r);

  void get_local_group_id();
  void handle_get_local_group_id(int r);

  void get_local_group_id_by_name();
  void handle_get_local_group_id_by_name(int r);

  void get_local_group_name();
  void handle_get_local_group_name(int r);

  void get_local_mirror_group();
  void handle_get_local_mirror_group(int r);

  void list_local_group_snapshots();
  void handle_list_local_group_snapshots(int r);

  void list_local_group();
  void handle_list_local_group(int r);

  void get_local_mirror_image();
  void handle_get_local_mirror_image(int r);

  void remove_local_image_from_group();
  void handle_remove_local_image_from_group(int r);

  void move_local_image_to_trash();
  void handle_move_local_image_to_trash(int r);

  void remove_local_mirror_image();
  void handle_remove_local_mirror_image(int r);

  void disable_local_mirror_group();
  void handle_disable_local_mirror_group(int r);

  void remove_local_mirror_group();
  void handle_remove_local_mirror_group(int r);

  void remove_local_group();
  void handle_remove_local_group(int r);

  void remove_local_group_id();
  void handle_remove_local_group_id(int r);

  void create_local_group_id();
  void handle_create_local_group_id(int r);

  void create_local_group();
  void handle_create_local_group(int r);

  void create_local_mirror_group();
  void handle_create_local_mirror_group(int r);

  void create_local_non_primary_group_snapshot();
  void handle_create_local_non_primary_group_snapshot(int r);

  void finish(int r);

  int create_replayers();
};

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::group_replayer::BootstrapRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_GROUP_REPLAYER_BOOTSTRAP_REQUEST_H
