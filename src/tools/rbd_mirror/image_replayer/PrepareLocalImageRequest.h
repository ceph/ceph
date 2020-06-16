// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_PREPARE_LOCAL_IMAGE_REQUEST_H
#define RBD_MIRROR_IMAGE_REPLAYER_PREPARE_LOCAL_IMAGE_REQUEST_H

#include "include/buffer.h"
#include "include/rados/librados_fwd.hpp"
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
namespace image_replayer {

template <typename> class StateBuilder;

template <typename ImageCtxT = librbd::ImageCtx>
class PrepareLocalImageRequest {
public:
  static PrepareLocalImageRequest *create(
      librados::IoCtx &io_ctx,
      const std::string &global_image_id,
      std::string *local_image_name,
      StateBuilder<ImageCtxT>** state_builder,
      librbd::asio::ContextWQ *work_queue,
      Context *on_finish) {
    return new PrepareLocalImageRequest(io_ctx, global_image_id,
                                        local_image_name, state_builder,
                                        work_queue, on_finish);
  }

  PrepareLocalImageRequest(
      librados::IoCtx &io_ctx,
      const std::string &global_image_id,
      std::string *local_image_name,
      StateBuilder<ImageCtxT>** state_builder,
      librbd::asio::ContextWQ *work_queue,
      Context *on_finish)
    : m_io_ctx(io_ctx), m_global_image_id(global_image_id),
      m_local_image_name(local_image_name), m_state_builder(state_builder),
      m_work_queue(work_queue), m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_LOCAL_IMAGE_ID
   *    |
   *    v
   * GET_LOCAL_IMAGE_NAME
   *    |
   *    v
   * GET_MIRROR_INFO
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  librados::IoCtx &m_io_ctx;
  std::string m_global_image_id;
  std::string *m_local_image_name;
  StateBuilder<ImageCtxT>** m_state_builder;
  librbd::asio::ContextWQ *m_work_queue;
  Context *m_on_finish;

  bufferlist m_out_bl;
  std::string m_local_image_id;
  cls::rbd::MirrorImage m_mirror_image;
  librbd::mirror::PromotionState m_promotion_state;
  std::string m_primary_mirror_uuid;

  void get_local_image_id();
  void handle_get_local_image_id(int r);

  void get_local_image_name();
  void handle_get_local_image_name(int r);

  void get_mirror_info();
  void handle_get_mirror_info(int r);

  void finish(int r);

};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::PrepareLocalImageRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_PREPARE_LOCAL_IMAGE_REQUEST_H
