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
                                    cls::rbd::MirrorImageMode mode,
                                    Context *on_finish) {
    return new GroupEnableRequest(group_io_ctx, group_id,mode,
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
   * GET_MIRROR_IMAGE * * * * * * *
   *    |                         * (on error)
   *    v (skip if not needed)    *
   * GET_TAG_OWNER  * * * * * * * *
   *    |                         *
   *    v (skip if not needed)    *
   * OPEN_IMAGE                   *
   *    |                         *
   *    v (skip if not needed)    *
   * CREATE_PRIMARY_SNAPSHOT  * * *
   *    |                         *
   *    v (skip of not opened)    *
   * CLOSE_IMAGE                  *
   *    |                         *
   *    v (skip if not needed)    *
   * ENABLE_NON_PRIMARY_FEATURE   *
   *    |                         *
   *    v (skip if not needed)    *
   * IMAGE_STATE_UPDATE * * * * * *
   *    |                         *
   *    v                         *
   * <finish>   < * * * * * * * * *
   *
   * @endverbatim
   */

  GroupEnableRequest(librados::IoCtx &io_ctx, const std::string &group_id,
                     cls::rbd::MirrorImageMode mode, Context *on_finish);

  librados::IoCtx &m_io_ctx;
  const std::string m_group_id;
  const cls::rbd::MirrorImageMode m_mode;
  Context *m_on_finish;

  CephContext *m_cct = nullptr;
  bufferlist m_out_bl;
  cls::rbd::MirrorGroup m_mirror_group;

  int m_ret_val;
  librados::IoCtx m_default_ns_ioctx;

  std::vector<uint64_t> m_quiesce_requests;
  std::vector<ImageCtxT *> m_image_ctxs;
  std::vector<cls::rbd::MirrorImage> m_mirror_images;

  std::set<std::string> m_mirror_peer_uuids;
  bool m_is_primary = false;
  cls::rbd::GroupImageSpec m_start_after;
  std::vector<cls::rbd::GroupImageStatus> m_images;


  void get_mirror_group();
  void handle_get_mirror_group(int r);

  void get_mirror_peer_list();
  void handle_get_mirror_peer_list(int r);

  void list_group_images();
  void handle_list_group_images(int r);

  void get_mirror_images();
  void handle_get_mirror_images(int r);

  void open_images();
  void handle_open_images(int r);

  void validate_images();

  void close_images();
  void handle_close_images(int r);

  void finish(int r);
};

} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::GroupEnableRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_GROUP_ENABLE_REQUEST_H
