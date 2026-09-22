// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_MIRROR_GROUP_DETACH_IMAGE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_GROUP_DETACH_IMAGE_REQUEST_H

#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_types.h"

#include <string>
#include <vector>
#include <set>

class CephContext;

namespace librbd {

class ImageCtx;

namespace mirror {

template <typename ImageCtxT = librbd::ImageCtx>
class GroupDetachImageRequest {
public:
  static GroupDetachImageRequest<ImageCtxT>* create(librados::IoCtx& io_ctx,
    const std::string& group_id, librados::IoCtx& image_ioctx,
    const std::string& image_id, uint64_t group_snap_create_flags,
    const cls::rbd::MirrorGroup& mirror_group, Context* on_finish) {
    return new GroupDetachImageRequest<ImageCtxT>(io_ctx, group_id, image_ioctx,
      image_id, group_snap_create_flags, mirror_group, on_finish);
  }

  GroupDetachImageRequest(librados::IoCtx& io_ctx, const std::string& group_id,
    librados::IoCtx& image_ioctx, const std::string& image_id,
    uint64_t group_snap_create_flags, const cls::rbd::MirrorGroup& mirror_group,
    Context* on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * PREPARE_GROUP_IMAGES
   *    |
   *    v
   * VALIDATE_IMAGE
   *    |
   *    | group mirrored
   *    |--------------------> DETACH_MIRROR_IMAGE
   *    |                             |
   *    |                             v
   *    |                  DETACHED_SNAPSHOT_PATH
   *    | resumed standalone
   *    |--------------------> DETACHED_SNAPSHOT_PATH
   *
   * DETACHED_SNAPSHOT_PATH
   *    |
   *    v
   * CREATE_DETACHED_IMAGE_SNAPSHOT
   *    |
   *    v
   * CREATE_PRIMARY_GROUP_SNAPSHOT
   *    |
   *    | group empty
   *    |-----------------> UPDATE_PRIMARY_GROUP_SNAPSHOT
   *    |                              |
   *    |                              v
   *    |                       MEMBERSHIP_PATH
   *    | members remain
   *    |-----------------> CREATE_PRIMARY_IMAGE_SNAPSHOTS
   *                                  |
   *                                  v
   *                         UPDATE_PRIMARY_GROUP_SNAPSHOT
   *                                  |
   *                                  v
   *                           MEMBERSHIP_PATH
   *
   * MEMBERSHIP_PATH
   *    |
   *    v
   * REMOVE_GROUP_MEMBERSHIP
   *    |
   *    v
   * NOTIFY_MIRRORING_WATCHER
   *    |
   *    v
   * CLOSE_IMAGES
   *    |
   *    v
   * <finish>
   *
   * Errors before mirror metadata changes go directly to CLOSE_IMAGES.
   * Cleanup then depends on the last durable step:
   *
   * Mirror type update failed:
   * RESTORE_MIRROR_IMAGE -> CLOSE_IMAGES -> <finish>
   *
   * Detached image or group snapshot creation failed:
   * REMOVE_DETACHED_IMAGE_SNAPSHOT -> RESTORE_MIRROR_IMAGE
   *    -> CLOSE_IMAGES -> <finish>
   *
   * Failure after the group snapshot was created:
   * REMOVE_PRIMARY_GROUP_SNAPSHOT -> REMOVE_DETACHED_IMAGE_SNAPSHOT
   *    -> RESTORE_MIRROR_IMAGE -> CLOSE_IMAGES -> <finish>
   *
   * Watcher notification errors are logged after the membership change is
   * durable and then continue to CLOSE_IMAGES.
   *
   * @endverbatim
   */

  void prepare_group_images();
  void handle_prepare_group_images(int r);

  void validate_image();

  void detach_mirror_image();
  void handle_detach_mirror_image(int r);

  void create_detached_image_snapshot();
  void handle_create_detached_image_snapshot(int r);

  void create_primary_group_snapshot();
  void handle_create_primary_group_snapshot(int r);

  void create_primary_image_snapshots();
  void handle_create_primary_image_snapshots(int r);

  void update_primary_group_snapshot();
  void handle_update_primary_group_snapshot(int r);

  void remove_group_membership();
  void handle_remove_group_membership(int r);

  void notify_mirroring_watcher();
  void handle_notify_mirroring_watcher(int r);

  void remove_primary_group_snapshot();
  void handle_remove_primary_group_snapshot(int r);

  void remove_detached_image_snapshot();
  void handle_remove_detached_image_snapshot(int r);

  void restore_mirror_image();
  void handle_restore_mirror_image(int r);

  void close_images();
  void handle_close_images(int r);

  void finish(int r);

private:
  librados::IoCtx& m_group_ioctx;
  librados::IoCtx& m_image_ioctx;

  std::string m_group_id;
  std::string m_image_id;

  uint64_t m_group_snap_create_flags;

  cls::rbd::MirrorGroup m_mirror_group;

  // All images originally belonging to the group
  std::vector<ImageCtxT*> m_image_ctxs;
  std::vector<cls::rbd::GroupImageStatus> m_images;
  std::vector<cls::rbd::MirrorImage> m_mirror_images;

  // Remaining images after the detach operation.
  // These are used for creating the final group snapshot.
  std::vector<ImageCtxT*> m_group_image_ctxs;
  std::vector<cls::rbd::MirrorImage> m_group_mirror_images;

  std::set<std::string> m_mirror_peer_uuids;

  // Image being detached from the mirror group
  ImageCtxT* m_detach_image_ctx = nullptr;
  int m_detach_image_index = -1;
  cls::rbd::MirrorImage m_original_mirror_image;

  // Final primary group snapshot containing the
  // post-detach group membership.
  cls::rbd::GroupSnapshot m_group_snap;

  // Snapshot IDs returned by GroupImageCreatePrimaryRequest
  std::vector<uint64_t> m_snap_ids;

  uint64_t m_detached_snap_id = CEPH_NOSNAP;

  int m_ret_val = 0;

  Context* m_on_finish;

  CephContext* m_cct;
};

} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::GroupDetachImageRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_GROUP_DETACH_IMAGE_REQUEST_H
