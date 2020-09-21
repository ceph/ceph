// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_SYNC_SYNC_POINT_CREATE_REQUEST_H
#define RBD_MIRROR_IMAGE_SYNC_SYNC_POINT_CREATE_REQUEST_H

#include "librbd/internal.h"
#include "Types.h"
#include <string>

class Context;
namespace journal { class Journaler; }
namespace librbd { class ImageCtx; }
namespace librbd { namespace journal { struct MirrorPeerClientMeta; } }

namespace rbd {
namespace mirror {
namespace image_sync {

template <typename ImageCtxT = librbd::ImageCtx>
class SyncPointCreateRequest {
public:
  static SyncPointCreateRequest* create(
      ImageCtxT *remote_image_ctx,
      const std::string &local_mirror_uuid,
      SyncPointHandler* sync_point_handler,
      Context *on_finish) {
    return new SyncPointCreateRequest(remote_image_ctx, local_mirror_uuid,
                                      sync_point_handler, on_finish);
  }

  SyncPointCreateRequest(
      ImageCtxT *remote_image_ctx,
      const std::string &local_mirror_uuid,
      SyncPointHandler* sync_point_handler,
      Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * UPDATE_SYNC_POINTS < . .
   *    |                   .
   *    v                   .
   * REFRESH_IMAGE          .
   *    |                   . (repeat on EEXIST)
   *    v                   .
   * CREATE_SNAP  . . . . . .
   *    |
   *    v
   * REFRESH_IMAGE
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT *m_remote_image_ctx;
  std::string m_local_mirror_uuid;
  SyncPointHandler* m_sync_point_handler;
  Context *m_on_finish;

  SyncPoints m_sync_points_copy;
  librbd::NoOpProgressContext m_prog_ctx;

  void send_update_sync_points();
  void handle_update_sync_points(int r);

  void send_refresh_image();
  void handle_refresh_image(int r);

  void send_create_snap();
  void handle_create_snap(int r);

  void send_final_refresh_image();
  void handle_final_refresh_image(int r);

  void finish(int r);
};

} // namespace image_sync
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_sync::SyncPointCreateRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_SYNC_SYNC_POINT_CREATE_REQUEST_H
