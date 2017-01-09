// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_IS_PRIMARY_REQUEST_H
#define RBD_MIRROR_IMAGE_REPLAYER_IS_PRIMARY_REQUEST_H

#include "include/buffer.h"

class Context;
class ContextWQ;
namespace librbd { class ImageCtx; }

namespace rbd {
namespace mirror {
namespace image_replayer {

template <typename ImageCtxT = librbd::ImageCtx>
class IsPrimaryRequest {
public:
  static IsPrimaryRequest* create(ImageCtxT *image_ctx, bool *primary,
                                  Context *on_finish) {
    return new IsPrimaryRequest(image_ctx, primary, on_finish);
  }

  IsPrimaryRequest(ImageCtxT *image_ctx, bool *primary, Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_MIRROR_STATE * * * * *
   *    |                     *
   *    v                     *
   * IS_TAG_OWNER * * * * * * * (error)
   *    |                     *
   *    v                     *
   * <finish> < * * * * * * * *
   *
   * @endverbatim
   */
  ImageCtxT *m_image_ctx;
  bool *m_primary;
  Context *m_on_finish;

  bufferlist m_out_bl;

  void send_get_mirror_state();
  void handle_get_mirror_state(int r);

  void send_is_tag_owner();
  void handle_is_tag_owner(int r);

  void finish(int r);
};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::IsPrimaryRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_IS_PRIMARY_REQUEST_H
