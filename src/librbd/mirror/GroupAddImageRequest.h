// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_GROUP_ADD_IMAGE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_GROUP_ADD_IMAGE_REQUEST_H

#include "include/buffer.h"
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
class GroupAddImageRequest {
public:
  static GroupAddImageRequest *create(librados::IoCtx &group_io_ctx,
                                    const std::string &group_id,
                                    const std::string &image_id,
                                    uint64_t group_snap_create_flags,
                                    cls::rbd::MirrorImageMode mode,
                                    Context *on_finish) {
    return new GroupAddImageRequest(group_io_ctx, group_id, image_id,
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
   * LIST_GROUP_IMAGES * * * * * * * * * * *
   *    |                                  *
   *    v  (skip if not needed)            *
   * CHECK_MIRROR_IMAGE_DISABLED * * * * * *
   *    |                                  *
   *    v  (skip if not needed)            *
   * OPEN_IMAGES   * * * * * * * * * * * * *
   *    |                                  *
   *    v  (skip if not needed)            *
   * VALIDATE_IMAGES   * * * * * * * * * * *
   *    |                                  *
   *    v                                  *
   * GET_MIRROR_IMAGES   * * * * * * * * * *
   *    |                                  *
   *    v                                  *
   * GET_MIRROR_PEERS  * * * * * * * * * * *
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
   * SET_MIRROR_IMAGE_ENABLED  * * * * * * *
   *    |                                  *
   *    v                            (if required)
   * NOTIFY_MIRRORING_WATCHER           cleanup
   *    |                                  *
   *    v (skip if not needed)             *
   * CLOSE_IMAGES  * * * * * * * * * * * * *
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  GroupAddImageRequest(librados::IoCtx &io_ctx, const std::string &group_id,
                     const std::string &image_id,
                     uint64_t group_snap_create_flags,
                     cls::rbd::MirrorImageMode mode, Context *on_finish);

  typedef std::pair<int64_t /*pool_id*/, std::string /*global_image_id*/> GlobalImageId;

  librados::IoCtx &m_group_ioctx;
  const std::string m_group_id;
  const std::string m_image_id;
  uint64_t m_group_snap_create_flags;
  const cls::rbd::MirrorImageMode m_mode;
  Context *m_on_finish;

  CephContext *m_cct = nullptr;
  std::vector<bufferlist> m_out_bls;
  cls::rbd::MirrorGroup m_mirror_group;

  int m_ret_val = 0;
  librados::IoCtx m_default_ns_ioctx;

  std::vector<ImageCtxT *> m_image_ctxs;
  cls::rbd::MirrorImage m_mirror_images;

  std::set<std::string> m_mirror_peer_uuids;
  cls::rbd::GroupImageSpec m_start_after;
  std::vector<cls::rbd::GroupImageStatus> m_images;
  std::vector<GlobalImageId> m_local_images;

  cls::rbd::GroupSnapshot m_group_snap;
  std::vector<uint64_t> m_snap_ids;

  std::vector<std::string> m_global_image_ids;

  bool m_need_to_cleanup_mirror_image = false;

  void get_mirror_group();
  void handle_get_mirror_group(int r);

  void list_group_images();
  void handle_list_group_images(int r);

  void check_mirror_image_disabled();
  void handle_check_mirror_image_disabled(int r);

  void open_images();
  void handle_open_images(int r);

  void validate_images();

  void get_mirror_images();
  void handle_get_mirror_images(int r);

  void get_mirror_peer_list();
  void handle_get_mirror_peer_list(int r);

  void create_primary_group_snapshot();
  void handle_create_primary_group_snapshot(int r);

  void create_primary_image_snapshots();
  void handle_create_primary_image_snapshots(int r);

  void update_primary_group_snapshot();
  void handle_update_primary_group_snapshot(int r);

  void set_mirror_image_enabled();
  void handle_set_mirror_image_enabled(int r);

  void notify_mirroring_watcher();
  void handle_notify_mirroring_watcher(int r);

  void close_images();
  void handle_close_images(int r);

  // cleanup
  void disable_mirror_image();
  void handle_disable_mirror_image(int r);

  void remove_primary_group_snapshot();
  void handle_remove_primary_group_snapshot(int r);

  void remove_mirror_image();
  void handle_remove_mirror_image(int r);

  void finish(int r);
};

} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::GroupAddImageRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_GROUP_ADD_IMAGE_REQUEST_H
