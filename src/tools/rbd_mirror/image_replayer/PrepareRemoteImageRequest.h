// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_REPLAYER_PREPARE_REMOTE_IMAGE_REQUEST_H
#define RBD_MIRROR_IMAGE_REPLAYER_PREPARE_REMOTE_IMAGE_REQUEST_H

#include "include/buffer.h"
#include <string>

namespace librados { struct IoCtx; }
namespace librbd { struct ImageCtx; }

struct Context;
struct ContextWQ;

namespace rbd {
namespace mirror {
namespace image_replayer {

template <typename ImageCtxT = librbd::ImageCtx>
class PrepareRemoteImageRequest {
public:
  static PrepareRemoteImageRequest *create(librados::IoCtx &io_ctx,
                                           const std::string &global_image_id,
                                           std::string *remote_mirror_uuid,
                                           std::string *remote_image_id,
                                           Context *on_finish) {
    return new PrepareRemoteImageRequest(io_ctx, global_image_id,
                                         remote_mirror_uuid, remote_image_id,
                                         on_finish);
  }

  PrepareRemoteImageRequest(librados::IoCtx &io_ctx,
                           const std::string &global_image_id,
                           std::string *remote_mirror_uuid,
                           std::string *remote_image_id,
                           Context *on_finish)
    : m_io_ctx(io_ctx), m_global_image_id(global_image_id),
      m_remote_mirror_uuid(remote_mirror_uuid),
      m_remote_image_id(remote_image_id),
      m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * GET_REMOTE_MIRROR_UUID
   *    |
   *    v
   * GET_REMOTE_IMAGE_ID
   *    |
   *    v
   * <finish>

   * @endverbatim
   */

  librados::IoCtx &m_io_ctx;
  std::string m_global_image_id;
  std::string *m_remote_mirror_uuid;
  std::string *m_remote_image_id;
  Context *m_on_finish;

  bufferlist m_out_bl;

  void get_remote_mirror_uuid();
  void handle_get_remote_mirror_uuid(int r);

  void get_remote_image_id();
  void handle_get_remote_image_id(int r);

  void finish(int r);

};

} // namespace image_replayer
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_replayer::PrepareRemoteImageRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_REPLAYER_PREPARE_REMOTE_IMAGE_REQUEST_H
