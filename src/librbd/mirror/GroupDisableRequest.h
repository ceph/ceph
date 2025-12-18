// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_GROUP_DISABLE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_GROUP_DISABLE_REQUEST_H

#include "include/buffer.h"
#include "include/rbd/librbd.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/mirror/Types.h"
#include <string>
#include <vector>

struct Context;

namespace librbd {
struct ImageCtx;

namespace mirror {

template <typename ImageCtxT = librbd::ImageCtx>
class GroupDisableRequest {
public:
  static GroupDisableRequest *create(librados::IoCtx &io_ctx,
                                     const std::string &group_id,
                                     const std::string &group_name,
                                     bool force,
                                     Context *on_finish) {
    return new GroupDisableRequest(io_ctx, group_id, group_name, force,
                                   on_finish);
  }

  GroupDisableRequest(librados::IoCtx &io_ctx,
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
   * GET_MIRROR_GROUP_INFO
   *    |
   *    v
   * REMOVE_RESYNC_KEY
   *    |
   *    v
   * PREPARE_GROUP_IMAGES
   *    |
   *    v
   * GET_IMAGES_MIRROR_UUID
   *    |
   *    v
   * REMOVE_SNAPSHOT_KEYS
   *    |
   *    v (skip if group is primary)
   * FORCE_PROMOTE_GROUP
   *    |
   *    v (skip if not needed)
   * REFRESH_IMAGES
   *    |
   *    v
   * SET_MIRROR_GROUP_DISABLING
   *    |
   *    v
   * SET_MIRROR_IMAGES_DISABLING
   *    |
   *    v
   * LIST_GROUP_SNAPSHOTS
   *    |
   *    v
   * REMOVE_GROUP_MIRROR_SNAPSHOTS
   *    |
   *    |        /-------------<--------------------\
   *    v        v                                  |
   * REMOVE_GROUP_MIRROR_SNAPSHOT--->HANDLE_REMOVE_GROUP_MIRROR_SNAPSHOT
   *    |
   *    | (all group mirror snapshots removed)
   *    v
   * REMOVE_MIRROR_IMAGES
   *    |
   *    v
   * REMOVE_MIRROR_GROUP
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
   * @endverbatim
   */

  librados::IoCtx &m_group_ioctx;
  const std::string m_group_id;
  const std::string m_group_name;
  bool m_force;
  Context *m_on_finish;
  CephContext *m_cct;

  std::vector<cls::rbd::GroupSnapshot> m_group_snaps;
  size_t m_group_snap_index = 0;

  cls::rbd::MirrorGroup m_mirror_group;
  std::vector<cls::rbd::GroupImageStatus> m_images;
  std::vector<ImageCtxT *> m_image_ctxs;
  std::vector<std::string> m_image_mirror_uuid;

  int m_ret_val = 0;
  PromotionState m_promotion_state;

  std::vector<cls::rbd::MirrorImage> m_mirror_images;
  bool m_is_group_primary = false;

  void get_mirror_group_info();
  void handle_get_mirror_group_info(int r);

  void remove_resync_key();
  void handle_remove_resync_key(int r);

  void prepare_group_images();
  void handle_prepare_group_images(int r);

  void get_images_mirror_uuid();
  void handle_get_images_mirror_uuid(int r);

  void remove_snapshot_keys();

  void set_mirror_group_disabling();
  void handle_set_mirror_group_disabling(int r);

  void set_mirror_images_disabling();
  void handle_set_mirror_images_disabling(int r);

  void force_promote_group();
  void handle_force_promote_group(int r);

  void refresh_images();
  void handle_refresh_images(int r);

  void list_group_snapshots();
  void handle_list_group_snapshots(int r);

  void remove_group_mirror_snapshots();
  void remove_group_mirror_snapshot();
  void handle_remove_group_mirror_snapshot(int r);

  void remove_mirror_images();
  void handle_remove_mirror_images(int r);

  void remove_mirror_group();
  void handle_remove_mirror_group(int r);

  void notify_mirroring_watcher();
  void handle_notify_mirroring_watcher(int r);

  void close_images();
  void handle_close_images(int r);

  void finish(int r);
};


} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::GroupDisableRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_GROUP_DISABLE_REQUEST_H
