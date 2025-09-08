// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_GROUP_ENABLE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_GROUP_ENABLE_REQUEST_H

#include "include/buffer_fwd.h"
#include "include/rados/librados_fwd.hpp"
#include "include/rbd/librbd.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/mirror/Types.h"
#include <map>
#include <string>

class Context;

namespace librbd {

namespace asio { struct ContextWQ; }

namespace mirror {

template <typename ImageCtxT = ImageCtx>
class GroupEnableRequest {
public:
  static GroupEnableRequest *create(librados::IoCtx &group_io_ctx,
                                    const std::string &group_id,
                                    uint64_t group_snap_create_flags,
                                    cls::rbd::MirrorImageMode mode,
                                    Context *on_finish) {
    return new GroupEnableRequest(group_io_ctx, group_id,
                                  group_snap_create_flags, mode, on_finish);
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v                         (on error)
   * GET_MIRROR_GROUP  * * * * * * * * * * *
   *    |                                  *
   *    v                                  *
   * GET_MIRROR_PEERS  * * * * * * * * * * *
   *    |                                  *
   *    v                                  *
   * LIST_GROUP_IMAGES * * * * * * * * * * *
   *    |                                  *
   *    v  (skip if not needed)            *
   * CHECK_MIRROR_IMAGES_DISABLED  * * * * *
   *    |                                  *
   *    v  (skip if not needed)            *
   * OPEN_IMAGES   * * * * * * * * * * * * *
   *    |                                  *
   *    v  (skip if not needed)            *
   * VALIDATE_IMAGES   * * * * * * * * * * *
   *    |                                  *
   *    v                                  *
   * SET_MIRROR_GROUP_ENABLING * * * * * * *
   *    |                                  *
   *    v (incomplete)                     *
   * CREATE_PRIMARY_GROUP_SNAP * * * * * * *
   *    |                                  *
   *    v (skip if not needed)             *
   * CREATE_PRIMARY_IMAGE_SNAPS            *
   *    |                                  *
   *    v (complete)                       *
   * UPDATE_PRIMARY_GROUP_SNAP * * * * * * *
   *    |                                  *
   *    v (skip if not needed)             *
   * SET_MIRROR_IMAGES_ENABLED * * * * * * *
   *    |                                  *
   *    v                                  *
   * SET_MIRROR_GROUP_ENABLED  * * * * * * *
   *    |                                  *
   *    v                            (if required)
   * NOTIFY_MIRRORING_WATCHER           cleanup
   *    |                                  *
   *    v (skip if not needed)             *
   * CLOSE_IMAGE < * * * * * * * * * * * * *
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  GroupEnableRequest(librados::IoCtx &io_ctx, const std::string &group_id,
                     uint64_t group_snap_create_flags,
                     cls::rbd::MirrorImageMode mode, Context *on_finish);

  librados::IoCtx &m_group_ioctx;
  const std::string m_group_id;
  uint64_t m_group_snap_create_flags;
  const cls::rbd::MirrorImageMode m_mode;
  Context *m_on_finish;

  CephContext *m_cct = nullptr;
  std::vector<bufferlist> m_out_bls;
  cls::rbd::MirrorGroup m_mirror_group;

  int m_ret_val = 0;
  librados::IoCtx m_default_ns_ioctx;

  std::vector<ImageCtxT *> m_image_ctxs;
  std::vector<cls::rbd::MirrorImage> m_mirror_images;

  std::set<std::string> m_mirror_peer_uuids;
  cls::rbd::GroupImageSpec m_start_after;
  std::vector<cls::rbd::GroupImageStatus> m_images;

  cls::rbd::GroupSnapshot m_group_snap;
  std::vector<uint64_t> m_snap_ids;
  std::vector<std::string> m_global_image_ids;

  bool m_need_to_cleanup_group_snapshot = false;
  bool m_need_to_cleanup_mirror_images = false;

  void get_mirror_group();
  void handle_get_mirror_group(int r);

  void get_mirror_peer_list();
  void handle_get_mirror_peer_list(int r);

  void list_group_images();
  void handle_list_group_images(int r);

  void check_mirror_images_disabled();
  void handle_check_mirror_images_disabled(int r);

  void open_images();
  void handle_open_images(int r);

  void validate_images();

  void create_primary_group_snapshot();
  void handle_create_primary_group_snapshot(int r);

  void set_mirror_group_enabling();
  void handle_set_mirror_group_enabling(int r);

  void create_primary_image_snapshots();
  void handle_create_primary_image_snapshots(int r);

  void update_primary_group_snapshot();
  void handle_update_primary_group_snapshot(int r);

  void set_mirror_images_enabled();
  void handle_set_mirror_images_enabled(int r);

  void set_mirror_group_enabled();
  void handle_set_mirror_group_enabled(int r);

  void notify_mirroring_watcher();
  void handle_notify_mirroring_watcher(int r);

  void close_images();
  void handle_close_images(int r);

  // Cleanup
  void disable_mirror_group();
  void handle_disable_mirror_group(int r);

  void get_mirror_images_for_cleanup();
  void handle_get_mirror_images_for_cleanup(int r);

  void disable_mirror_images();
  void handle_disable_mirror_images(int r);

  void remove_primary_group_snapshot();
  void handle_remove_primary_group_snapshot(int r);

  void remove_mirror_images();
  void handle_remove_mirror_images(int r);

  void remove_mirror_group();
  void handle_remove_mirror_group(int r);

  void finish(int r);
};

} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::GroupEnableRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_GROUP_ENABLE_REQUEST_H
