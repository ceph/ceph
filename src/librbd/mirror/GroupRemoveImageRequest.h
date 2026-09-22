// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_GROUP_REMOVE_IMAGE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_GROUP_REMOVE_IMAGE_REQUEST_H

#include <set>
#include "include/buffer.h"
#include "include/Context.h"
#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/mirror/Types.h"

namespace librbd {

struct ImageCtx;

namespace mirror {

template <typename ImageCtxT = ImageCtx>
class GroupRemoveImageRequest {
public:
  static GroupRemoveImageRequest* create(ImageCtxT* image_ctx,
                                         const std::string& group_id,
                                         librados::IoCtx& group_io_ctx,
                                         Context* on_finish) {
    return new GroupRemoveImageRequest(image_ctx, group_id, group_io_ctx,
                                       on_finish);
  }

  GroupRemoveImageRequest(ImageCtxT* image_ctx, const std::string& group_id,
                          librados::IoCtx& group_io_ctx, Context* on_finish);

  void send();

private:

/**
 * @verbatim
 *
 *            <start>
 *               |
 *               v
 *         GET_MIRROR_INFO
 *               |
 *      +--------+-----------------------+
 *      |                                |
 *  not mirrored                       mirrored
 *      |                                |
 *      |                                v
 *      |                    SET_MIRROR_IMAGE_DISABLING
 *      |                                |
 *      |                                v
 *      |                      GET_MIRROR_PEER_LIST
 *      |                                |
 *      |                                v
 *      |                    UNLINK_GROUP_SNAPSHOTS
 *      |                                |
 *      +----------------<---------------+
 *               |
 *               v
 *  REMOVE_GROUP_REF_FROM_IMAGE
 *               |
 *               v
 *  REMOVE_GLOBAL_MIRROR_IMAGE_ENTRY
 *               |
 *               v
 *           <finish>
 *
 *  Fatal operation errors terminate at <finish>. The caller owns the image
 *  context, so this request deliberately leaves it open.
 *
 * @endverbatim
 */

  ImageCtxT* m_image_ctx;
  std::string m_group_id;
  librados::IoCtx& m_group_io_ctx;
  Context* m_on_finish;

  // mirror peer handling
  std::set<std::string> m_mirror_peer_uuids;

  // rados read buffer
  bufferlist m_out_bl;

  // ioctx for default namespace (peer listing requires this)
  librados::IoCtx m_default_ns_ioctx;

  std::vector<ImageCtx*> m_image_ctxs_single;

  CephContext* m_cct;

  // mirror state
  cls::rbd::MirrorImage m_mirror_image;
  PromotionState m_promotion_state = PROMOTION_STATE_UNKNOWN;
  std::string m_mirror_uuid;

  void get_mirror_info();
  void handle_get_mirror_info(int r);

  void set_mirror_image_disabling();
  void handle_set_mirror_image_disabling(int r);

  void get_mirror_peer_list();
  void handle_get_mirror_peer_list(int r);

  void unlink_group_snapshots();
  void handle_unlink_group_snapshots(int r);

  void promote_image();
  void handle_promote_image(int r);

  void remove_group_ref_from_image();
  void handle_remove_group_ref_from_image(int r);

  void remove_global_mirror_image_entry();
  void handle_remove_global_mirror_image_entry(int r);

  void close_image();
  void handle_close(int r);

  void finish(int r);
};

} // namespace mirror
} // namespace librbd

extern template class
librbd::mirror::GroupRemoveImageRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_GROUP_REMOVE_IMAGE_REQUEST_H
