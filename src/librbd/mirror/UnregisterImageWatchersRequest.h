// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_UNREGISTER_IMAGE_WATCHERS_REQUEST_H
#define CEPH_LIBRBD_MIRROR_UNREGISTER_IMAGE_WATCHERS_REQUEST_H

#include "common/ceph_mutex.h"
#include "include/Context.h"
#include "librbd/ImageCtx.h"

struct Context;
struct obj_watch_t;

namespace librbd {
struct ImageCtx;

namespace mirror {

template <typename ImageCtxT = librbd::ImageCtx>
class UnregisterImageWatchersRequest {
public:
  static UnregisterImageWatchersRequest* create(ImageCtxT* image_ctx,
                                                Context* on_finish) {
    return new UnregisterImageWatchersRequest(image_ctx, on_finish);
  }

   UnregisterImageWatchersRequest(ImageCtxT* image_ctx,
                                  Context* on_finish)
    : m_image_ctx(image_ctx), m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    |     /-- UNREGISTER_UPDATE_WATCHER <-\
   *    v     v                               |
   * LIST_WATCHERS ----> WAIT_UPDATE_NOTIFY --/
   *    |
   *    | (no watchers)
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT* m_image_ctx;
  Context* m_on_finish;

  class UpdateWatchCtx : public librbd::UpdateWatchCtx {
  public:
    UpdateWatchCtx(UnregisterImageWatchersRequest *request)
      : request(request) {
    }

    void handle_notify() {
      request->handle_update_notify();
    }

  private:
    UnregisterImageWatchersRequest *request;

  } m_update_watch_ctx = {this};

  std::list<obj_watch_t> m_watchers;
  uint64_t m_update_watcher_handle = 0;
  uint64_t m_scheduler_ticks = 0;
  SafeTimer *m_timer = nullptr;
  ceph::mutex *m_timer_lock = nullptr;

  void list_watchers();
  void handle_list_watchers(int r);

  void wait_update_notify();
  void handle_update_notify();

  void scheduler_unregister_update_watcher();
  void unregister_update_watcher();
  void handle_unregister_update_watcher(int r);

  void finish(int r);
};

} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::UnregisterImageWatchersRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_UNREGISTER_IMAGE_WATCHERS_REQUEST_H

