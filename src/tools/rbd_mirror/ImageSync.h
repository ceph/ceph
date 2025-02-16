// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef RBD_MIRROR_IMAGE_SYNC_H
#define RBD_MIRROR_IMAGE_SYNC_H

#include "include/int_types.h"
#include "librbd/ImageCtx.h"
#include "librbd/Types.h"
#include "common/ceph_mutex.h"
#include "tools/rbd_mirror/CancelableRequest.h"
#include "tools/rbd_mirror/image_sync/Types.h"

class Context;
namespace journal { class Journaler; }
namespace librbd { template <typename> class DeepCopyRequest; }

namespace rbd {
namespace mirror {

class ProgressContext;
template <typename> class InstanceWatcher;
template <typename> class Threads;

namespace image_sync { struct SyncPointHandler; }

template <typename ImageCtxT = librbd::ImageCtx>
class ImageSync : public CancelableRequest {
public:
  static ImageSync* create(
      Threads<ImageCtxT>* threads,
      ImageCtxT *local_image_ctx,
      ImageCtxT *remote_image_ctx,
      const std::string &local_mirror_uuid,
      image_sync::SyncPointHandler* sync_point_handler,
      InstanceWatcher<ImageCtxT> *instance_watcher,
      ProgressContext *progress_ctx,
      Context *on_finish) {
    return new ImageSync(threads, local_image_ctx, remote_image_ctx,
                         local_mirror_uuid, sync_point_handler,
                         instance_watcher, progress_ctx, on_finish);
  }

  ImageSync(
      Threads<ImageCtxT>* threads,
      ImageCtxT *local_image_ctx,
      ImageCtxT *remote_image_ctx,
      const std::string &local_mirror_uuid,
      image_sync::SyncPointHandler* sync_point_handler,
      InstanceWatcher<ImageCtxT> *instance_watcher,
      ProgressContext *progress_ctx,
      Context *on_finish);
  ~ImageSync() override;

  void send() override;
  void cancel() override;

protected:
  void finish(int r) override;

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * NOTIFY_SYNC_REQUEST
   *    |
   *    v
   * PRUNE_CATCH_UP_SYNC_POINT
   *    |
   *    v
   * CREATE_SYNC_POINT (skip if already exists and
   *    |               not disconnected)
   *    v
   * COPY_IMAGE . . . . . . . . . . . . . .
   *    |                                 .
   *    v                                 .
   * FLUSH_SYNC_POINT                     .
   *    |                                 . (image sync canceled)
   *    v                                 .
   * PRUNE_SYNC_POINTS                    .
   *    |                                 .
   *    v                                 .
   * <finish> < . . . . . . . . . . . . . .
   *
   * @endverbatim
   */

  class ImageCopyProgressHandler;

  Threads<ImageCtxT>* m_threads;
  ImageCtxT *m_local_image_ctx;
  ImageCtxT *m_remote_image_ctx;
  std::string m_local_mirror_uuid;
  image_sync::SyncPointHandler* m_sync_point_handler;
  InstanceWatcher<ImageCtxT> *m_instance_watcher;
  ProgressContext *m_progress_ctx;

  ceph::mutex m_lock;
  bool m_canceled = false;

  librbd::DeepCopyRequest<ImageCtxT> *m_image_copy_request = nullptr;
  ImageCopyProgressHandler *m_image_copy_prog_handler = nullptr;

  bool m_updating_sync_point = false;
  Context *m_update_sync_ctx = nullptr;
  double m_update_sync_point_interval;
  uint64_t m_image_copy_object_no = 0;
  uint64_t m_image_copy_object_count = 0;

  librbd::SnapSeqs m_snap_seqs_copy;
  image_sync::SyncPoints m_sync_points_copy;

  int m_ret_val = 0;

  void send_notify_sync_request();
  void handle_notify_sync_request(int r);

  void send_prune_catch_up_sync_point();
  void handle_prune_catch_up_sync_point(int r);

  void send_create_sync_point();
  void handle_create_sync_point(int r);

  void send_update_max_object_count();
  void handle_update_max_object_count(int r);

  void send_copy_image();
  void handle_copy_image(int r);
  void handle_copy_image_update_progress(uint64_t object_no,
                                         uint64_t object_count);
  void send_update_sync_point();
  void handle_update_sync_point(int r);

  void send_flush_sync_point();
  void handle_flush_sync_point(int r);

  void send_prune_sync_points();
  void handle_prune_sync_points(int r);

  void update_progress(const std::string &description);
};

} // namespace mirror
} // namespace rbd

extern template class rbd::mirror::ImageSync<librbd::ImageCtx>;

#endif // RBD_MIRROR_IMAGE_SYNC_H
