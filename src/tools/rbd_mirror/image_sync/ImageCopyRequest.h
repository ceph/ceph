// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_SYNC_IMAGE_COPY_REQUEST_H
#define RBD_MIRROR_IMAGE_SYNC_IMAGE_COPY_REQUEST_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"
#include "common/Mutex.h"
#include "librbd/journal/Types.h"
#include "librbd/journal/TypeTraits.h"
#include "tools/rbd_mirror/BaseRequest.h"
#include <map>
#include <vector>

class Context;
class SafeTimer;
namespace journal { class Journaler; }
namespace librbd { struct ImageCtx; }

namespace rbd {
namespace mirror {

class ProgressContext;

namespace image_sync {

template <typename ImageCtxT = librbd::ImageCtx>
class ImageCopyRequest : public BaseRequest {
public:
  typedef std::vector<librados::snap_t> SnapIds;
  typedef std::map<librados::snap_t, SnapIds> SnapMap;
  typedef librbd::journal::TypeTraits<ImageCtxT> TypeTraits;
  typedef typename TypeTraits::Journaler Journaler;
  typedef librbd::journal::MirrorPeerSyncPoint MirrorPeerSyncPoint;
  typedef librbd::journal::MirrorPeerClientMeta MirrorPeerClientMeta;
  typedef rbd::mirror::ProgressContext ProgressContext;

  static ImageCopyRequest* create(ImageCtxT *local_image_ctx,
                                  ImageCtxT *remote_image_ctx,
                                  SafeTimer *timer, Mutex *timer_lock,
                                  Journaler *journaler,
                                  MirrorPeerClientMeta *client_meta,
                                  MirrorPeerSyncPoint *sync_point,
                                  Context *on_finish,
				  ProgressContext *progress_ctx = nullptr) {
    return new ImageCopyRequest(local_image_ctx, remote_image_ctx, timer,
                                timer_lock, journaler, client_meta, sync_point,
                                on_finish, progress_ctx);
  }

  ImageCopyRequest(ImageCtxT *local_image_ctx, ImageCtxT *remote_image_ctx,
                   SafeTimer *timer, Mutex *timer_lock, Journaler *journaler,
                   MirrorPeerClientMeta *client_meta,
                   MirrorPeerSyncPoint *sync_point, Context *on_finish,
		   ProgressContext *progress_ctx = nullptr);

  void send();
  void cancel();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * UPDATE_MAX_OBJECT_COUNT
   *    |
   *    |   . . . . .
   *    |   .       .  (parallel execution of
   *    v   v       .   multiple objects at once)
   * COPY_OBJECT  . .
   *    |
   *    v
   * FLUSH_SYNC_POINT
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT *m_local_image_ctx;
  ImageCtxT *m_remote_image_ctx;
  SafeTimer *m_timer;
  Mutex *m_timer_lock;
  Journaler *m_journaler;
  MirrorPeerClientMeta *m_client_meta;
  MirrorPeerSyncPoint *m_sync_point;
  ProgressContext *m_progress_ctx;

  SnapMap m_snap_map;

  Mutex m_lock;
  bool m_canceled = false;

  uint64_t m_object_no = 0;
  uint64_t m_end_object_no;
  uint64_t m_current_ops = 0;
  int m_ret_val = 0;

  bool m_updating_sync_point;
  Context *m_update_sync_ctx;
  double m_update_sync_point_interval;

  MirrorPeerClientMeta m_client_meta_copy;

  void send_update_max_object_count();
  void handle_update_max_object_count(int r);

  void send_object_copies();
  void send_next_object_copy();
  void handle_object_copy(int r);

  void send_update_sync_point();
  void handle_update_sync_point(int r);

  void send_flush_sync_point();
  void handle_flush_sync_point(int r);

  int compute_snap_map();

  void update_progress(const std::string &description, bool flush = true);
};

} // namespace image_sync
} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::image_sync::ImageCopyRequest<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_SYNC_IMAGE_COPY_REQUEST_H
