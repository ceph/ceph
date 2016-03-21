// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_SYNC_SYNC_POINT_PRUNE_REQUEST_H
#define RBD_MIRROR_IMAGE_SYNC_SYNC_POINT_PRUNE_REQUEST_H

#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"
#include <list>
#include <string>

class Context;
namespace journal { class Journaler; }
namespace librbd { class ImageCtx; }
namespace librbd { namespace journal { struct MirrorPeerClientMeta; } }

namespace rbd {
namespace mirror {
namespace image_sync {

template <typename ImageCtxT = librbd::ImageCtx>
class SyncPointPruneRequest {
public:
  typedef librbd::journal::TypeTraits<ImageCtxT> TypeTraits;
  typedef typename TypeTraits::Journaler Journaler;
  typedef librbd::journal::MirrorPeerClientMeta MirrorPeerClientMeta;
  typedef librbd::journal::MirrorPeerSyncPoint MirrorPeerSyncPoint;

  static SyncPointPruneRequest* create(ImageCtxT *remote_image_ctx,
                                       bool sync_complete,
                                       Journaler *journaler,
                                       MirrorPeerClientMeta *client_meta,
                                       Context *on_finish) {
    return new SyncPointPruneRequest(remote_image_ctx, sync_complete, journaler,
                                      client_meta, on_finish);
  }

  SyncPointPruneRequest(ImageCtxT *remote_image_ctx, bool sync_complete,
                        Journaler *journaler, MirrorPeerClientMeta *client_meta,
                        Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    |    . . . . .
   *    |    .       .
   *    v    v       . (repeat if from snap
   * REMOVE_SNAP . . .  unused by other sync)
   *    |
   *    v
   * REFRESH_IMAGE
   *    |
   *    v
   * UPDATE_CLIENT
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT *m_remote_image_ctx;
  bool m_sync_complete;
  Journaler *m_journaler;
  MirrorPeerClientMeta *m_client_meta;
  Context *m_on_finish;

  MirrorPeerClientMeta m_client_meta_copy;
  std::list<std::string> m_snap_names;

  void send_remove_snap();
  void handle_remove_snap(int r);

  void send_refresh_image();
  void handle_refresh_image(int r);

  void send_update_client();
  void handle_update_client(int r);

  void finish(int r);
};

} // namespace image_sync
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_sync::SyncPointPruneRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_SYNC_SYNC_POINT_PRUNE_REQUEST_H
