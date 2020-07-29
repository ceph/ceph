// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_MIRROR_SNAPSHOT_PROMOTE_REQUEST_H
#define CEPH_LIBRBD_MIRROR_SNAPSHOT_PROMOTE_REQUEST_H

#include "include/buffer.h"
#include "include/rbd/librbd.hpp"
#include "common/ceph_mutex.h"
#include "librbd/internal.h"

#include <string>
#include <set>

class SafeTimer;
struct Context;

namespace librbd {

struct ImageCtx;

namespace mirror {
namespace snapshot {

template <typename ImageCtxT = librbd::ImageCtx>
class PromoteRequest {
public:
  static PromoteRequest *create(ImageCtxT *image_ctx,
                                const std::string& global_image_id,
                                Context *on_finish) {
    return new PromoteRequest(image_ctx, global_image_id, on_finish);
  }

  PromoteRequest(ImageCtxT *image_ctx, const std::string& global_image_id,
                 Context *on_finish)
    : m_image_ctx(image_ctx), m_global_image_id(global_image_id),
      m_on_finish(on_finish) {
  }

  void send();

private:
  /**
   * @verbatim
   *
   * <start>
   *    |
   *    |          (can promote)
   *    |\----------------------------------------\
   *    |                                         |
   *    |                                         |
   *    v (skip if not needed)                    |
   * CREATE_ORPHAN_SNAPSHOT                       |
   *    |                                         |
   *    |     /-- UNREGISTER_UPDATE_WATCHER <-\   |
   *    v     v                               |   |
   * LIST_WATCHERS ----> WAIT_UPDATE_NOTIFY --/   |
   *    |                                         |
   *    | (no watchers)                           |
   *    v                                         |
   * ACQUIRE_EXCLUSIVE_LOCK                       |
   *    |  (skip if not needed)                   |
   *    v                                         |
   * ROLLBACK                                     |
   *    |                                         |
   *    v                                         |
   * CREATE_PROMOTE_SNAPSHOT <--------------------/
   *    |
   *    v
   * DISABLE_NON_PRIMARY_FEATURE
   *    |
   *    v
   * RELEASE_EXCLUSIVE_LOCK (skip if not needed)
   *    |
   *    v
   * <finish>
   *
   * @endverbatim
   */

  ImageCtxT *m_image_ctx;
  std::string m_global_image_id;
  Context *m_on_finish;

  uint64_t m_rollback_snap_id = CEPH_NOSNAP;
  bool m_lock_acquired = false;
  NoOpProgressContext m_progress_ctx;

  class UpdateWatchCtx : public librbd::UpdateWatchCtx {
  public:
    UpdateWatchCtx(PromoteRequest *promote_request)
      : promote_request(promote_request) {
    }

    void handle_notify() {
      promote_request->handle_update_notify();
    }

  private:
    PromoteRequest *promote_request;

  } m_update_watch_ctx = {this};

  std::list<obj_watch_t> m_watchers;
  uint64_t m_update_watcher_handle = 0;
  uint64_t m_scheduler_ticks = 0;
  SafeTimer *m_timer = nullptr;
  ceph::mutex *m_timer_lock = nullptr;

  void refresh_image();
  void handle_refresh_image(int r);

  void create_orphan_snapshot();
  void handle_create_orphan_snapshot(int r);

  void list_watchers();
  void handle_list_watchers(int r);

  void wait_update_notify();
  void handle_update_notify();
  void scheduler_unregister_update_watcher();

  void unregister_update_watcher();
  void handle_unregister_update_watcher(int r);

  void acquire_exclusive_lock();
  void handle_acquire_exclusive_lock(int r);

  void rollback();
  void handle_rollback(int r);

  void create_promote_snapshot();
  void handle_create_promote_snapshot(int r);

  void disable_non_primary_feature();
  void handle_disable_non_primary_feature(int r);

  void release_exclusive_lock();
  void handle_release_exclusive_lock(int r);

  void finish(int r);

};

} // namespace snapshot
} // namespace mirror
} // namespace librbd

extern template class librbd::mirror::snapshot::PromoteRequest<librbd::ImageCtx>;

#endif // CEPH_LIBRBD_MIRROR_SNAPSHOT_PROMOTE_REQUEST_H
