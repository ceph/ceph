// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#ifndef CEPHFS_MIRROR_WATCHER_REWATCH_REQUEST_H
#define CEPHFS_MIRROR_WATCHER_REWATCH_REQUEST_H

#include "common/ceph_mutex.h"
#include "include/int_types.h"
#include "include/rados/librados.hpp"

struct Context;

namespace cephfs {
namespace mirror {
namespace watcher {

// Rewatch an existing watch -- the watch can be in an operational
// or error state.

class RewatchRequest {
public:

  static RewatchRequest *create(librados::IoCtx &ioctx, const std::string &oid,
                                ceph::shared_mutex &watch_lock,
                                librados::WatchCtx2 *watch_ctx,
                                uint64_t *watch_handle, Context *on_finish) {
    return new RewatchRequest(ioctx, oid, watch_lock, watch_ctx, watch_handle,
                              on_finish);
  }

  RewatchRequest(librados::IoCtx &ioctx, const std::string &oid,
                 ceph::shared_mutex &watch_lock, librados::WatchCtx2 *watch_ctx,
                 uint64_t *watch_handle, Context *on_finish);

  void send();

private:
  librados::IoCtx& m_ioctx;
  std::string m_oid;
  ceph::shared_mutex &m_lock;
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
} // namespace mirror
} // namespace cephfs

#endif // CEPHFS_MIRROR_WATCHER_REWATCH_REQUEST_H
