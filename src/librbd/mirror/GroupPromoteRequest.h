// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_GROUP_PROMOTE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_GROUP_PROMOTE_REQUEST_H

#include "include/buffer.h"
#include "include/rbd/librbd.hpp"
#include "common/ceph_mutex.h"
#include "common/Timer.h"
#include "librbd/internal.h"
#include "librbd/Utils.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/mirror/Types.h"
#include <string>
#include <set>
#include <vector>
#include <list>

struct Context;
struct obj_watch_t;

namespace librbd {
struct ImageCtx;

namespace mirror {

template <typename ImageCtxT = librbd::ImageCtx>
class GroupPromoteRequest {
public:
  static GroupPromoteRequest *create(librados::IoCtx &io_ctx,
                                     const std::string &group_id,
                                     const std::string &group_name,
                                     bool force,
                                     Context *on_finish) {
    return new GroupPromoteRequest(io_ctx, group_id, group_name, force,
                                   on_finish);
  }

  GroupPromoteRequest(librados::IoCtx &io_ctx,
                      const std::string &group_id,
                      const std::string &group_name,
                      bool force,
                      Context *on_finish)
    : m_group_ioctx(io_ctx), m_group_id(group_id), m_group_name(group_name), m_force(force),
      m_on_finish(on_finish), m_cct(reinterpret_cast<CephContext*>(io_ctx.cct())) {
}

  void send();

private:

  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_MIRROR_GROUP
   *    |
   *    v
   * GET_LAST_MIRROR_SNAPSHOT_STATE
   *    |
   *    v
   * GET_LOCAl_GROUP_META
   *    |
   *    v                  (if group is empty)
   * PREPARE_GROUP_IMAGES------------------------------->
   *    |                                               |
   *    v         (if force promote else skip)          |
   * CHECK_ROLL_BACK_NEEDED                             |
   *    |                                               |
   *    v          (regular group promote)              |
   * PROMOTE_GROUP ------------------------------------>|
   *    |                                               |
   *    v (incomplete), (skip if not needed)            |
   * CREATE_GROUP_ORPHAN_SNAPSHOT                       |
   *    |                                               |
   *    v                                               |
   * CREATE_IMAGES_ORPHAN_SNAPSHOTS                     |
   *    |                                               |
   *    v   (skip if not needed)                        |
   * MARK_GROUP_ORPHAN_SNAPSHOT_COMPLETE                |
   *    |                                               |
   *    v                                               |
   * UNREGISTER_IMAGES_WATCHERS                         |
   *    |                                               |
   *    v                                               |
   * ACQUIRE_EXCLUSIVE_LOCKS                            |
   *    |                                               |
   *    v  (skip if not needed)                         |
   * ROLLBACK                                           |
   *    |                                               |
   *    v  (incomplete)                                 |
   * CREATE_PRIMARY_GROUP_SNAPSHOT <--------------------|
   *    |
   *    v (skip if not needed)
   * CREATE_IMAGES_PRIMARY_SNAPSHOTS
   *    |
   *    v
   * DISABLE_NON_PRIMARY_FEATURES
   *    |
   *    v  (complete)
   * UPDATE_PRIMARY_GROUP_SNAPSHOT
   *    |
   *    v
   * GROUP_UNLINK_PEER
   *    |
   *    v   (skip if not needed)
   * RELEASE_EXCLUSIVE_LOCKS
   *    |
   *    v
   * CLOSE_IMAGES
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  librados::IoCtx &m_group_ioctx;
  const std::string m_group_id;
  const std::string m_group_name;
  bool m_force;
  Context *m_on_finish;
  CephContext *m_cct;

  std::vector<cls::rbd::GroupSnapshot> m_snaps;

  cls::rbd::MirrorGroup m_mirror_group;
  std::set<std::string> m_mirror_peer_uuids;
  std::vector<cls::rbd::GroupImageStatus> m_images;
  std::vector<ImageCtxT *> m_image_ctxs;
  ceph::bufferlist m_out_bl;
  std::vector<bool> m_locks_acquired;
  bool m_excl_locks_acquired = false;

  int m_ret_val = 0;
  bool m_need_rollback = false;
  std::vector<uint64_t> m_rollback_snap_ids;
  cls::rbd::GroupSnapshot m_rollback_group_snap;
  cls::rbd::GroupSnapshot m_orphan_group_snap;
  cls::rbd::GroupSnapshot m_group_snap;

  std::vector<cls::rbd::MirrorImage> m_mirror_images;
  std::vector<uint64_t> m_snap_ids;
  std::vector<uint64_t> m_orphan_snap_ids;
  std::vector<std::string> m_global_image_ids;

  NoOpProgressContext m_progress_ctx;

  void get_mirror_group();
  void handle_get_mirror_group(int r);

  void get_last_mirror_snapshot_state();
  void handle_get_last_mirror_snapshot_state(int r);

  void get_local_group_meta();
  void handle_get_local_group_meta(int r);

  void prepare_group_images();
  void handle_prepare_group_images(int r);

  void check_rollback_needed();

  void promote_group();

  void create_group_orphan_snapshot();
  void handle_create_group_orphan_snapshot(int r);

  void create_images_orphan_snapshots();
  void handle_create_images_orphan_snapshots(int r);

  void mark_group_orphan_snapshot_complete();
  void handle_mark_group_orphan_snapshot_complete(int r);

  void unregister_images_watchers();
  void handle_unregister_images_watchers(int r);

  void acquire_exclusive_locks();
  void handle_acquire_exclusive_locks(int r);

  void rollback();
  void handle_rollback(int r);

  void create_primary_group_snapshot();
  void handle_create_primary_group_snapshot(int r);

  void create_images_primary_snapshots();
  void handle_create_images_primary_snapshots(int r);

  void disable_non_primary_features();
  void handle_disable_non_primary_features(int r);

  void update_primary_group_snapshot();
  void handle_update_primary_group_snapshot(int r);

  void release_exclusive_locks();
  void handle_release_exclusive_locks(int r);

  void close_images();
  void handle_close_images(int r);

  void group_unlink_peer();
  void handle_group_unlink_peer(int r);

  void remove_primary_group_snapshot();
  void handle_remove_primary_group_snapshot(int r);

  void finish(int r);
};


} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::GroupPromoteRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_GROUP_PROMOTE_REQUEST_H
