// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef RBD_MIRROR_GROUP_REPLAYER_BOOTSTRAP_REQUEST_H
#define RBD_MIRROR_GROUP_REPLAYER_BOOTSTRAP_REQUEST_H

#include "include/rados/librados.hpp"
#include "common/snap_types.h"
#include "cls/rbd/cls_rbd_types.h"
#include "tools/rbd_mirror/CancelableRequest.h"
#include "tools/rbd_mirror/group_replayer/Types.h"

#include <atomic>
#include <list>
#include <map>
#include <set>
#include <string>
#include <vector>


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
      instance_watcher, local_status_updater,
      remote_status_updater, cache_manager_handler, pool_meta_cache,
      resync_requested, local_group_ctx, image_replayers, state_builder,
      on_finish);
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

  int get_local_image_id(const std::string &global_image_id,
    std::string *image_id);
  void send() override;
  void cancel() override;

private:
  /**
   * @verbatim
   *
   *  PREPARATION PATH
   *  ================
   *
   *                         <start>
   *                            |
   *                            v
   *                  PREPARE_LOCAL_GROUP
   *                            | error, except -ENOENT
   *                            + -----------------------------> FINISH(error)
   *                            |
   *                            v
   *                  PREPARE_REMOTE_GROUP
   *                            |
   *                            v
   *                 HANDLE_REMOTE_GROUP_RESULT
   *                            |
   *                            | remove local group
   *                            + -------------------> REMOVE_LOCAL_GROUP -----> FINISH(r)
   *                            |
   *                            | create local group
   *                            + -------------------> CREATE_LOCAL_GROUP -----> FINISH(r)
   *                            |
   *                            | existing local secondary
   *                            + -------------------> GET_LOCAL_GROUP_META
   *                            |                              | resync requested?
   *                            |                false         v         true
   *                            |        + <------------------ + ---------------> +
   *                            |        |                                        |
   *                            |        v                                        v
   *                            |     FINISH(0)                         REMOVE_LOCAL_GROUP
   *                            |                                                 |
   *                            |                                                 v
   *                            + ------------------------------------------> FINISH(r)
   *
   *  HANDLE_REMOTE_GROUP_RESULT also calls FINISH directly when the local
   *  group is primary, both groups are missing, the peers are not linked, or
   *  an error prevents bootstrap from continuing.
   *
   *
   *  FINISH PATH
   *  ===========
   *
   *                           FINISH(r)
   *                              |
   *                              + -- error or canceled ---> COMPLETE(r)
   *                              |
   *                              + -- local group removed --> COMPLETE(-ENOENT)
   *                              |
   *                              + -- local primary
   *                              |              |
   *                              |              v
   *                              |       CREATE_REPLAYERS
   *                              |              |
   *                              |              v
   *                              |         COMPLETE(r)
   *                              |
   *                              + -- existing local secondary
   *                                             |
   *                                             v
   *                                 LOAD_LOCAL_GROUP_SNAPSHOTS
   *                                             |
   *                                             v
   *                                 LOAD_REMOTE_GROUP_SNAPSHOTS
   *                                             |
   *                                             v
   *                               SELECT_TARGET_GROUP_MEMBERSHIP
   *                                             |
   *                                             v
   *                               RECONCILE_LOCAL_GROUP_MEMBERS
   *                                             |
   *                                             v
   *                                      CREATE_REPLAYERS
   *                                             |
   *                                             v
   *                                        COMPLETE(r)
   *                                             |
   *                                             v
   *                                          <finish>
   *
   *
   *  TARGET MEMBERSHIP ERROR PATHS
   *  =============================
   *
   *      SELECT_TARGET_GROUP_MEMBERSHIP
   *                    |
   *                    | latest local snapshot missing from remote history
   *                    + ---------------> CREATE_REPLAYERS (status only)
   *                    |                             |
   *                    |                             v
   *                    |                      COMPLETE(-EEXIST)
   *                    |
   *                    | other error
   *                    + ---------------> COMPLETE(error)
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
  std::deque<cls::rbd::GroupImageSpec> m_remove_group_images;
  std::deque<cls::rbd::GroupImageSpec> m_add_group_images;
  std::vector<cls::rbd::GroupSnapshot> m_local_group_snaps;
  std::vector<cls::rbd::GroupSnapshot> m_remote_group_snaps;
  std::map<std::string, std::pair<int64_t, std::string>> m_target_remote_images;
  const cls::rbd::GroupSnapshot *m_target_remote_snap = nullptr;
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

  void load_local_group_snapshots();
  void handle_load_local_group_snapshots(int r);
  void load_remote_group_snapshots();
  void handle_load_remote_group_snapshots(int r);
  int select_target_group_membership();

  void reconcile_local_group_members();
  void remove_next_group_member();
  void handle_remove_group_member(int r);
  void add_next_group_member();
  void handle_add_group_member(int r);

  void complete_reconcile();
  int update_local_mirror_images(const std::set<std::string>& group_images,
    std::vector<std::pair<std::string, std::string>>* attached_images,
    std::vector<std::pair<std::string, std::string>>* detached_images);
  int create_replayers();

  void finish(int r);
};

} // namespace group_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::group_replayer::BootstrapRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_GROUP_REPLAYER_BOOTSTRAP_REQUEST_H
