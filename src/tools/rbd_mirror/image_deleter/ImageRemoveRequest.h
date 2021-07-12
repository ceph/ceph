// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_IMAGE_DELETE_IMAGE_REMOVE_REQUEST_H
#define CEPH_RBD_MIRROR_IMAGE_DELETE_IMAGE_REMOVE_REQUEST_H

#include "include/buffer.h"
#include "include/rados/librados.hpp"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/mirror/Types.h"
#include <string>

struct Context;
namespace librbd {
struct ImageCtx;
namespace asio { struct ContextWQ; }
} // namespace librbd

namespace rbd {
namespace mirror {
namespace image_deleter {

template <typename ImageCtxT = librbd::ImageCtx>
class ImageRemoveRequest {
public:
  static ImageRemoveRequest* create(librados::IoCtx& io_ctx,
                                    const std::string& global_image_id,
                                    const std::string& image_id,
                                    librbd::asio::ContextWQ* op_work_queue,
                                    Context* on_finish) {
    return new ImageRemoveRequest(io_ctx, global_image_id, image_id,
                                  op_work_queue, on_finish);
  }

  ImageRemoveRequest(librados::IoCtx& io_ctx,
                     const std::string& global_image_id,
                     const std::string& image_id,
                     librbd::asio::ContextWQ* op_work_queue, Context* on_finish)
    : m_io_ctx(io_ctx), m_global_image_id(global_image_id),
      m_image_id(image_id), m_op_work_queue(op_work_queue),
      m_on_finish(on_finish) {
  }

  void send();

private:
  /*
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_MIRROR_IMAGE_ID
   *    |
   *    v
   * GET_MIRROR_INFO
   *    |
   *    v
   * DISABLE_MIRROR_IMAGE
   *    |
   *    v
   * REMOVE_MIRROR_IMAGE
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  librados::IoCtx &m_io_ctx;
  std::string m_global_image_id;
  std::string m_image_id;
  librbd::asio::ContextWQ *m_op_work_queue;
  Context *m_on_finish;

  cls::rbd::MirrorImage m_mirror_image;
  librbd::mirror::PromotionState m_promotion_state;
  std::string m_primary_mirror_uuid;

  void get_mirror_info();
  void handle_get_mirror_info(int r);

  void disable_mirror_image();
  void handle_disable_mirror_image(int r);

  void remove_mirror_image();
  void handle_remove_mirror_image(int r);

  void finish(int r);

};

} // namespace image_deleter
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_deleter::ImageRemoveRequest<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_IMAGE_DELETE_IMAGE_REMOVE_REQUEST_H
