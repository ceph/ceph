// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_IMAGE_WATCHER_REWATCH_REQUEST_H
#define CEPH_LIBRBD_IMAGE_WATCHER_REWATCH_REQUEST_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"

struct Context;
struct RWLock;

namespace librbd {

class ImageCtx;

namespace image_watcher {

template <typename ImageCtxT = librbd::ImageCtx>
class RewatchRequest {
public:

  static RewatchRequest *create(ImageCtxT &image_ctx, RWLock &watch_lock,
                                librados::WatchCtx2 *watch_ctx,
                                uint64_t *watch_handle, Context *on_finish) {
    return new RewatchRequest(image_ctx, watch_lock, watch_ctx, watch_handle,
                              on_finish);
  }

  RewatchRequest(ImageCtxT &image_ctx, RWLock &watch_lock,
                 librados::WatchCtx2 *watch_ctx, uint64_t *watch_handle,
                 Context *on_finish);

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    v
   * UNWATCH
   *    |
   *    |  . . . .
   *    |  .     . (recoverable error)
   *    v  v     .
   * REWATCH . . .
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT &m_image_ctx;
  RWLock &m_watch_lock;
  librados::WatchCtx2 *m_watch_ctx;
  uint64_t *m_watch_handle;
  Context *m_on_finish;

  uint64_t m_rewatch_handle = 0;

  void unwatch();
  void handle_unwatch(int r);

  void rewatch();
  void handle_rewatch(int r);

  void finish(int r);
};

} // namespace image_watcher
} // namespace librbd

extern template class librbd::image_watcher::RewatchRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_IMAGE_WATCHER_REWATCH_REQUEST_H
