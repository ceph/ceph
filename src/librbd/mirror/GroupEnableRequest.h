// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#ifndef CEPH_LIBRBD_MIRROR_GROUP_ENABLE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_GROUP_ENABLE_REQUEST_H

#include "librbd/mirror/snapshot/GroupPrepareImagesRequest.h"
#include "include/rados/librados_fwd.hpp"
#include "cls/rbd/cls_rbd_types.h"
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
   * PREPARE_GROUP_IMAGES  * * * * * * * * *
   *    |                                  *
   *    v                                  *
   * GROUP_MIRROR_ENABLE_UPDATE  * * * * * *
   *    |                                  *
   *    v                                  *
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

  std::vector<ImageCtxT *> m_image_ctxs;

  std::set<std::string> m_mirror_peer_uuids;
  std::vector<cls::rbd::GroupImageStatus> m_images;

  snapshot::GroupPrepareImagesRequest<ImageCtxT>* m_prepare_req = nullptr;

  void get_mirror_group();
  void handle_get_mirror_group(int r);

  void prepare_group_images();
  void handle_prepare_group_images(int r);

  void group_mirror_enable_update();
  void handle_group_mirror_enable_update(int r);

  void close_images();
  void handle_close_images(int r);

  void finish(int r);
};

} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::GroupEnableRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_GROUP_ENABLE_REQUEST_H
