// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_SYNC_H
#define RBD_MIRROR_IMAGE_SYNC_H

#include "include/int_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/journal/TypeTraits.h"
#include "common/Mutex.h"
#include "tools/rbd_mirror/BaseRequest.h"
#include <map>
#include <vector>

class Context;
class ContextWQ;
class Mutex;
class SafeTimer;
namespace journal { class Journaler; }
namespace librbd { namespace journal { struct MirrorPeerClientMeta; } }

namespace rbd {
namespace mirror {

class ProgressContext;

namespace image_sync { template <typename> class ImageCopyRequest; }
namespace image_sync { template <typename> class SnapshotCopyRequest; }

template <typename ImageCtxT = librbd::ImageCtx>
class ImageSync : public BaseRequest {
public:
  typedef librbd::journal::TypeTraits<ImageCtxT> TypeTraits;
  typedef typename TypeTraits::Journaler Journaler;
  typedef librbd::journal::MirrorPeerClientMeta MirrorPeerClientMeta;

  static ImageSync* create(ImageCtxT *local_image_ctx,
                           ImageCtxT *remote_image_ctx, SafeTimer *timer,
                           Mutex *timer_lock, const std::string &mirror_uuid,
                           Journaler *journaler,
                           MirrorPeerClientMeta *client_meta,
                           ContextWQ *work_queue, Context *on_finish,
			   ProgressContext *progress_ctx = nullptr) {
    return new ImageSync(local_image_ctx, remote_image_ctx, timer, timer_lock,
                         mirror_uuid, journaler, client_meta, work_queue,
                         on_finish, progress_ctx);
  }

  ImageSync(ImageCtxT *local_image_ctx, ImageCtxT *remote_image_ctx,
            SafeTimer *timer, Mutex *timer_lock, const std::string &mirror_uuid,
            Journaler *journaler, MirrorPeerClientMeta *client_meta,
            ContextWQ *work_queue, Context *on_finish,
            ProgressContext *progress_ctx = nullptr);
  ~ImageSync();

  void send();
  void cancel();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * PRUNE_CATCH_UP_SYNC_POINT
   *    |
   *    v
   * CREATE_SYNC_POINT (skip if already exists and
   *    |               not disconnected)
   *    v
   * COPY_SNAPSHOTS
   *    |
   *    v
   * COPY_IMAGE . . . . . . . . . . . . . .
   *    |                                 .
   *    v                                 .
   * COPY_OBJECT_MAP (skip if object      .
   *    |             map disabled)       .
   *    v                                 .
   * REFRESH_OBJECT_MAP (skip if object   .
   *    |                map disabled)    .
   *    v
   * PRUNE_SYNC_POINTS                    . (image sync canceled)
   *    |                                 .
   *    v                                 .
   * <finish> < . . . . . . . . . . . . . .
   *
   * @endverbatim
   */

  typedef std::vector<librados::snap_t> SnapIds;
  typedef std::map<librados::snap_t, SnapIds> SnapMap;

  ImageCtxT *m_local_image_ctx;
  ImageCtxT *m_remote_image_ctx;
  SafeTimer *m_timer;
  Mutex *m_timer_lock;
  std::string m_mirror_uuid;
  Journaler *m_journaler;
  MirrorPeerClientMeta *m_client_meta;
  ContextWQ *m_work_queue;
  ProgressContext *m_progress_ctx;

  SnapMap m_snap_map;

  Mutex m_lock;
  bool m_canceled = false;

  image_sync::SnapshotCopyRequest<ImageCtxT> *m_snapshot_copy_request = nullptr;
  image_sync::ImageCopyRequest<ImageCtxT> *m_image_copy_request = nullptr;
  decltype(ImageCtxT::object_map) m_object_map = nullptr;

  void send_prune_catch_up_sync_point();
  void handle_prune_catch_up_sync_point(int r);

  void send_create_sync_point();
  void handle_create_sync_point(int r);

  void send_copy_snapshots();
  void handle_copy_snapshots(int r);

  void send_copy_image();
  void handle_copy_image(int r);

  void send_copy_object_map();
  void handle_copy_object_map(int r);

  void send_refresh_object_map();
  void handle_refresh_object_map(int r);

  void send_prune_sync_points();
  void handle_prune_sync_points(int r);

  void update_progress(const std::string &description);
};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::ImageSync<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_SYNC_H
