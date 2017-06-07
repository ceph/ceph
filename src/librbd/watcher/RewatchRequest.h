// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPH_LIBRBD_WATCHER_REWATCH_REQUEST_H
#define CEPH_LIBRBD_WATCHER_REWATCH_REQUEST_H

#include "include/int_types.h"
#include "include/rados/librados.hpp"

struct Context;
struct RWLock;

namespace librbd {

namespace watcher {

class RewatchRequest {
public:

  static RewatchRequest *create(librados::IoCtx& ioctx, const std::string& oid,
                                RWLock &watch_lock,
                                librados::WatchCtx2 *watch_ctx,
                                uint64_t *watch_handle, Context *on_finish) {
    return new RewatchRequest(ioctx, oid, watch_lock, watch_ctx, watch_handle,
                              on_finish);
  }

  RewatchRequest(librados::IoCtx& ioctx, const std::string& oid,
                 RWLock &watch_lock, librados::WatchCtx2 *watch_ctx,
                 uint64_t *watch_handle, Context *on_finish);

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

  librados::IoCtx& m_ioctx;
  std::string m_oid;
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

} // namespace watcher
} // namespace librbd

#endif // CEPH_LIBRBD_WATCHER_REWATCH_REQUEST_H
