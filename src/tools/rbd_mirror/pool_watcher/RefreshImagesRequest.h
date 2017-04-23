// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_RBD_MIRROR_POOL_WATCHER_REFRESH_IMAGES_REQUEST_H
#define CEPH_RBD_MIRROR_POOL_WATCHER_REFRESH_IMAGES_REQUEST_H

#include "include/buffer.h"
#include "include/rados/librados.hpp"
#include "tools/rbd_mirror/types.h"
#include <string>

struct Context;

namespace librbd { struct ImageCtx; }

namespace rbd {
namespace mirror {
namespace pool_watcher {

template <typename ImageCtxT = librbd::ImageCtx>
class RefreshImagesRequest {
public:
  static RefreshImagesRequest *create(librados::IoCtx &remote_io_ctx,
                                      ImageIds *image_ids, Context *on_finish) {
    return new RefreshImagesRequest(remote_io_ctx, image_ids, on_finish);
  }

  RefreshImagesRequest(librados::IoCtx &remote_io_ctx, ImageIds *image_ids,
                       Context *on_finish)
    : m_remote_io_ctx(remote_io_ctx), m_image_ids(image_ids),
      m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    |   /-------------\
   *    |   |             |
   *    v   v             | (more images)
   * MIRROR_IMAGE_LIST ---/
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  librados::IoCtx &m_remote_io_ctx;
  ImageIds *m_image_ids;
  Context *m_on_finish;

  bufferlist m_out_bl;
  std::string m_start_after;

  void mirror_image_list();
  void handle_mirror_image_list(int r);

  void finish(int r);

};

} // namespace pool_watcher
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::pool_watcher::RefreshImagesRequest<librbd::ImageCtx>;

#endif // CEPH_RBD_MIRROR_POOL_WATCHER_REFRESH_IMAGES_REQUEST_H
