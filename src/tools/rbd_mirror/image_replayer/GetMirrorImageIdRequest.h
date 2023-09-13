// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_GET_MIRROR_IMAGE_ID_REQUEST_H
#define RBD_MIRROR_IMAGE_REPLAYER_GET_MIRROR_IMAGE_ID_REQUEST_H

#include "include/buffer.h"
#include "include/rados/librados_fwd.hpp"
#include <string>

namespace librbd { struct ImageCtx; }

struct Context;

namespace rbd {
namespace mirror {
namespace image_replayer {

template <typename ImageCtxT = librbd::ImageCtx>
class GetMirrorImageIdRequest {
public:
  static GetMirrorImageIdRequest *create(librados::IoCtx &io_ctx,
                                         const std::string &global_image_id,
                                         std::string *image_id,
                                         Context *on_finish) {
    return new GetMirrorImageIdRequest(io_ctx, global_image_id, image_id,
                                       on_finish);
  }

  GetMirrorImageIdRequest(librados::IoCtx &io_ctx,
                           const std::string &global_image_id,
                           std::string *image_id,
                           Context *on_finish)
    : m_io_ctx(io_ctx), m_global_image_id(global_image_id),
      m_image_id(image_id), m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_IMAGE_ID
   *    |
   *    v
   * <finish>

   * @endverbatim
   */

  librados::IoCtx &m_io_ctx;
  std::string m_global_image_id;
  std::string *m_image_id;
  Context *m_on_finish;

  bufferlist m_out_bl;

  void get_image_id();
  void handle_get_image_id(int r);

  void finish(int r);

};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::GetMirrorImageIdRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_GET_MIRROR_IMAGE_ID_REQUEST_H
