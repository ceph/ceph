// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_GROUP_ADD_IMAGE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_GROUP_ADD_IMAGE_REQUEST_H

#include "librbd/mirror/snapshot/GroupPrepareImagesRequest.h"
#include "include/rados/librados_fwd.hpp"
#include "cls/rbd/cls_rbd_types.h"

#include <string>
#include <vector>
#include <set>

class Context;

namespace librbd {

class ImageCtx;
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
                                      const cls::rbd::MirrorGroup &mirror_group,
                                      Context *on_finish) {
    return new GroupAddImageRequest(group_io_ctx, group_id, image_id,
                                    group_snap_create_flags, mode,
                                    mirror_group, on_finish);
  }

  void send();

private:
  /**
   * Adds an image to an enabled mirror group.
   *
   * ImageCtx ownership is transferred to
   * GroupMirrorEnableUpdateRequest once started.
   * This request only closes images if preparation fails.
   *
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * PREPARE_GROUP_IMAGES
   *    |       |  (on error)
   *    |       v
   *    |   CLOSE_IMAGES
   *    |       |
   *    |       v
   *    |    <finish>
   *    v
   * GROUP_MIRROR_ADD_IMAGE_UPDATE
   *    |
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  GroupAddImageRequest(librados::IoCtx &io_ctx, const std::string &group_id,
                       const std::string &image_id,
                       uint64_t group_snap_create_flags,
                       cls::rbd::MirrorImageMode mode,
                       const cls::rbd::MirrorGroup &mirror_group,
                       Context *on_finish);

  librados::IoCtx &m_group_ioctx;
  const std::string m_group_id;
  const std::string m_image_id;
  uint64_t m_group_snap_create_flags;
  const cls::rbd::MirrorImageMode m_mode;
  cls::rbd::MirrorGroup m_mirror_group;
  Context *m_on_finish;

  CephContext *m_cct = nullptr;

  int m_ret_val = 0;

  std::vector<ImageCtxT *> m_image_ctxs;
  std::vector<cls::rbd::MirrorImage> m_mirror_images;
  std::set<std::string> m_mirror_peer_uuids;
  std::vector<cls::rbd::GroupImageStatus> m_images;

  snapshot::GroupPrepareImagesRequest<ImageCtxT>* m_prepare_req = nullptr;

  void prepare_group_images();
  void handle_prepare_group_images(int r);

  void group_mirror_add_image_update();
  void handle_group_mirror_add_image_update(int r);

  void close_images();
  void handle_close_images(int r);

  void finish(int r);
};

} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::GroupAddImageRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_GROUP_ADD_IMAGE_REQUEST_H
