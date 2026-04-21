// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_GROUP_PROMOTE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_GROUP_PROMOTE_REQUEST_H

#include "include/buffer.h"
#include "include/rbd/librbd.hpp"
#include "librbd/internal.h"
#include "cls/rbd/cls_rbd_types.h"
#include <string>
#include <set>
#include <vector>

struct Context;

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
   * CHECK_GROUP_PRIMARY_STATE
   *    |
   *    v
   * REMOVE_RESYNC_KEY
   *    |
   *    v
   * PREPARE_GROUP_IMAGES
   *    |
   *    v         (if force promote else skip)
   * CHECK_ROLL_BACK_NEEDED
   *    |
   *    v          (regular group promote)
   * PREPARE_GROUP_PROMOTION--------------------------->|
   *    |                                               |
   *    v (incomplete)                                  |
   * CREATE_GROUP_ORPHAN_SNAPSHOT                       |
   *    |                                               |
   *    v                                               |
   * CREATE_IMAGES_ORPHAN_SNAPSHOTS                     |
   *    |                                               |
   *    v                                               |
   * MARK_GROUP_ORPHAN_SNAPSHOT_COMPLETE                |
   *    |                                               |
   *    v                                               |
   * DRAIN_IMAGES_WATCHERS                              |
   *    |                                               |
   *    v                       (no rollback needed)    |
   * ACQUIRE_EXCLUSIVE_LOCKS ---------------------------|
   *    |                                               |
   *    v  (skip if not needed)                         |
   * REMOVE_IMAGES_FROM_GROUP                           |
   *    |                                               |
   *    v                                               |
   * ROLLBACK                                           |
   *    |                                               |
   *    v  (incomplete)                                 |
   * CREATE_PRIMARY_GROUP_SNAPSHOT <--------------------|
   *    |
   *    v  (skip if group is empty)    (on error)
   * CREATE_IMAGES_PRIMARY_SNAPSHOTS -----------------------------\
   *    |                                                         |
   *    v  (skip if group is empty)    (on error)                 |
   * DISABLE_NON_PRIMARY_FEATURES -----------------------\        |
   *    |                                                |        |
   *    v  (complete)                  (on error)        |        |
   * UPDATE_PRIMARY_GROUP_SNAPSHOT ----------------------|        |
   *    |                                                |        |
   *    v                                                v        |
   * GROUP_UNLINK_PEER               ENABLE_NON_PRIMARY_FEATURES  |
   *    |                                                |        |
   *    v  (skip if not required)                        |        |
   * DISABLE_REMOVED_IMAGES                              |        |
   *    |                                                |        |
   *    v  (skip if not required)                        |        |
   * REMOVE_NON_MEMBER_IMAGES                            |        |
   *    |                                                |        |
   *    v  (skip if not needed)                          v        v
   * RELEASE_EXCLUSIVE_LOCKS <-------------REMOVE_PRIMARY_GROUP_SNAPSHOT
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
  bool m_orphan_required = false;
  Context *m_on_finish;
  CephContext *m_cct;

  std::vector<cls::rbd::GroupSnapshot> m_snaps;
  std::vector<cls::rbd::GroupImageSpec> m_to_add;
  std::vector<cls::rbd::GroupImageSpec> m_to_remove;

  cls::rbd::MirrorGroup m_mirror_group;
  std::set<std::string> m_mirror_peer_uuids;
  std::vector<cls::rbd::GroupImageStatus> m_images;
  std::vector<ImageCtxT *> m_image_ctxs;
  std::vector<ImageCtxT *> m_image_ctxs_old_membership;
  std::vector<librados::IoCtx> m_remove_ioctxs;
  ceph::bufferlist m_out_bl;
  cls::rbd::GroupImageSpec m_start_after;
  bool m_excl_locks_acquire = false;

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

  void check_group_primary_state();
  void handle_check_group_primary_state(int r);

  void remove_resync_key();
  void handle_remove_resync_key(int r);

  void prepare_group_images();
  void handle_prepare_group_images(int r);

  void check_rollback_needed();

  void prepare_group_promotion();

  bool build_group_snapshot_metadata(
    bool is_primary, cls::rbd::GroupSnapshot* group_snap);

  void create_group_orphan_snapshot();
  void handle_create_group_orphan_snapshot(int r);

  void create_images_orphan_snapshots();
  void handle_create_images_orphan_snapshots(int r);

  void mark_group_orphan_snapshot_complete();
  void handle_mark_group_orphan_snapshot_complete(int r);

  void drain_image_watchers();
  void handle_drain_image_watchers(int r);

  void acquire_exclusive_locks();
  void handle_acquire_exclusive_locks(int r);

  void remove_images_from_group();
  void handle_remove_images_from_group(int r);

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

  void group_unlink_peer();
  void handle_group_unlink_peer(int r);

  void disable_removed_images();
  void handle_disable_removed_images(int r);

  void remove_non_member_images();
  void handle_remove_non_member_images(int r);

  void release_exclusive_locks();
  void handle_release_exclusive_locks(int r);

  void close_images();
  void handle_close_images(int r);

  void enable_non_primary_features();
  void handle_enable_non_primary_features(int r);

  void remove_primary_group_snapshot();
  void handle_remove_primary_group_snapshot(int r);

  void finish(int r);
};


} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::GroupPromoteRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_GROUP_PROMOTE_REQUEST_H
