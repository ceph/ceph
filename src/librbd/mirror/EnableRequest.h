// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_ENABLE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_ENABLE_REQUEST_H

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
class EnableRequest {
public:
  static EnableRequest *create(ImageCtxT *image_ctx,
                               cls::rbd::MirrorImageMode mode,
                               const std::string &non_primary_global_image_id,
                               bool image_clean, Context *on_finish) {
    return new EnableRequest(image_ctx->md_ctx, image_ctx->id, image_ctx, mode,
                             non_primary_global_image_id, image_clean,
                             image_ctx->op_work_queue, on_finish);
  }
  static EnableRequest *create(librados::IoCtx &io_ctx,
                               const std::string &image_id,
                               cls::rbd::MirrorImageMode mode,
                               const std::string &non_primary_global_image_id,
                               bool image_clean, asio::ContextWQ *op_work_queue,
                               Context *on_finish) {
    return new EnableRequest(io_ctx, image_id, nullptr, mode,
                             non_primary_global_image_id, image_clean,
                             op_work_queue, on_finish);
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
   *    v                         *
   * IMAGE_STATE_UPDATE * * * * * *
   *    |                         *
   *    v                         *
   * <finish>   < * * * * * * * * *
   *
   * @endverbatim
   */

  EnableRequest(librados::IoCtx &io_ctx, const std::string &image_id,
                ImageCtxT* image_ctx, cls::rbd::MirrorImageMode mode,
                const std::string &non_primary_global_image_id,
                bool image_clean, asio::ContextWQ *op_work_queue,
                Context *on_finish);

  librados::IoCtx &m_io_ctx;
  std::string m_image_id;
  ImageCtxT* m_image_ctx;
  cls::rbd::MirrorImageMode m_mode;
  std::string m_non_primary_global_image_id;
  bool m_image_clean;
  asio::ContextWQ *m_op_work_queue;
  Context *m_on_finish;

  CephContext *m_cct = nullptr;
  bufferlist m_out_bl;
  cls::rbd::MirrorImage m_mirror_image;

  int m_ret_val = 0;
  bool m_close_image = false;

  bool m_is_primary = false;
  uint64_t m_snap_id = CEPH_NOSNAP;

  void get_mirror_image();
  void handle_get_mirror_image(int r);

  void get_tag_owner();
  void handle_get_tag_owner(int r);

  void open_image();
  void handle_open_image(int r);

  void create_primary_snapshot();
  void handle_create_primary_snapshot(int r);

  void close_image();
  void handle_close_image(int r);

  void enable_non_primary_feature();
  void handle_enable_non_primary_feature(int r);

  void image_state_update();
  void handle_image_state_update(int r);

  void finish(int r);
};

} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::EnableRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_ENABLE_REQUEST_H
