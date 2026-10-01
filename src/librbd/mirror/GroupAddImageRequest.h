// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_MIRROR_GROUP_ADD_IMAGE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_GROUP_ADD_IMAGE_REQUEST_H

#include "librbd/ImageCtx.h"
#include "librbd/mirror/ImageStateUpdateRequest.h"
#include "librbd/mirror/snapshot/GroupPrepareImagesRequest.h"
#include "librbd/mirror/snapshot/GroupImageCreatePrimaryRequest.h"
#include "librbd/mirror/snapshot/RemoveGroupSnapshotRequest.h"
#include "cls/rbd/cls_rbd_client.h"
#include "include/rbd/librbd.hpp"
#include <string>
#include <vector>

namespace librbd {
namespace mirror {

template <typename I = librbd::ImageCtx>
class GroupAddImageRequest {
public:
  static GroupAddImageRequest *create(librados::IoCtx &group_io_ctx,
                                      const std::string &group_id,
                                      const std::string &image_id,
                                      uint64_t group_snap_create_flags,
                                      const cls::rbd::MirrorGroup &mirror_group,
                                      const cls::rbd::MirrorImage &existing_mirror_image,
                                      Context *on_finish) {
    return new GroupAddImageRequest(group_io_ctx, group_id, image_id,
          group_snap_create_flags, mirror_group, existing_mirror_image,
          on_finish);
  }

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
   *    | already attached
   *    |-----------------------> NOTIFY_GROUP_MEMBERSHIP_UPDATED
   *    |                                  |
   *    |                                  v
   *    |                            FINALIZE_PATH
   *    | new or standalone
   *    |-----------------------> CREATE_PRIMARY_GROUP_SNAPSHOT
   *                                      |
   *                                      v
   *                             CREATE_PRIMARY_IMAGE_SNAPSHOTS
   *                                      |
   *                                      v
   *                             UPDATE_PRIMARY_GROUP_SNAPSHOT
   *                                      |
   *                                      |
   *                                      v
   *                         ATTACH_EXISTING_MIRROR_IMAGE
   *                                      |
   *                                      v
   *                       NOTIFY_GROUP_MEMBERSHIP_UPDATED
   *                                      |
   *                                      v
   *                                FINALIZE_PATH
   *
   * FINALIZE_PATH
   *    |
   *    v
   * NOTIFY_GROUP_UPDATED
   *    |
   *    v
   * CLOSE_IMAGES
   *    |
   *    v
   * <finish>
   *
   * Errors before a group snapshot exists go directly to CLOSE_IMAGES.
   * Errors after snapshot creation follow CLEANUP_PATH:
   *
   * CLEANUP_PATH
   *    |
   *    v
   * REMOVE_PRIMARY_GROUP_SNAPSHOT
   *    |
   *    |
   *    v
   * RESTORE_MIRROR_IMAGE
   *    |
   *    v
   * CLEANUP_CLOSE_PATH
   *
   * CLEANUP_CLOSE_PATH
   *    |
   *    v
   * CLOSE_IMAGES
   *    |
   *    v
   * <finish>
   *
   * Watcher notification errors are logged after the membership change is
   * durable and then continue along FINALIZE_PATH.
   *
   * @endverbatim
   */

  GroupAddImageRequest(librados::IoCtx &group_io_ctx,
      const std::string &group_id, const std::string &image_id,
      uint64_t group_snap_create_flags,
      const cls::rbd::MirrorGroup &mirror_group,
      const cls::rbd::MirrorImage &existing_mirror_image,
      Context *on_finish);

  librados::IoCtx &m_group_ioctx;
  std::string m_group_id;
  std::string m_image_id;
  uint64_t m_group_snap_create_flags;
  cls::rbd::MirrorGroup m_mirror_group;
  cls::rbd::MirrorImage m_existing_mirror_image;
  Context *m_on_finish;

  CephContext *m_cct;
  std::vector<I*> m_image_ctxs;
  std::vector<cls::rbd::GroupImageStatus> m_images;
  std::vector<cls::rbd::MirrorImage> m_mirror_images;
   std::set<std::string> m_mirror_peer_uuids;
  std::vector<uint64_t> m_snap_ids;
  std::vector<std::string> m_global_image_ids;

  // Newly added image
  I *m_add_image_ctx = nullptr;
  int m_add_image_index = -1;

  cls::rbd::GroupSnapshot m_group_snap;

  int m_ret_val = 0;

  void prepare_group_images();
  void handle_prepare_group_images(int r);
  void validate_image();
  void create_primary_group_snapshot();
  void handle_create_primary_group_snapshot(int r);
  void create_primary_image_snapshots();
  void handle_create_primary_image_snapshots(int r);
  void update_primary_group_snapshot();
  void handle_update_primary_group_snapshot(int r);
  void attach_existing_mirror_image();
  void handle_attach_existing_mirror_image(int r);

  void notify_group_membership_updated();
  void handle_notify_group_membership_updated(int r);

  void notify_group_updated();
  void handle_notify_group_updated(int r);

  void restore_mirror_image();
  void handle_restore_mirror_image(int r);

  // Cleanup
  void close_images();
  void handle_close_images(int r);
  void remove_primary_group_snapshot();
  void handle_remove_primary_group_snapshot(int r);
  void finish(int r);

};

} // namespace mirror
} // namespace librbd

#endif // CEPH_LIBRBD_MIRROR_GROUP_ADD_IMAGE_REQUEST_H
