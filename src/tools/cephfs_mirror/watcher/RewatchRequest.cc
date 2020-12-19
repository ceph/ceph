// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_mutex.h"
#include "common/debug.h"
#include "common/errno.h"
#include "include/Context.h"
#include "tools/cephfs_mirror/aio_utils.h"
#include "RewatchRequest.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_cephfs_mirror
#undef dout_prefix
#define dout_prefix *_dout << "cephfs::mirror::watcher:RewatchRequest " << __func__

namespace cephfs {
namespace mirror {
namespace watcher {

RewatchRequest::RewatchRequest(librados::IoCtx &ioctx, const std::string &oid,
                               ceph::shared_mutex &watch_lock,
                               librados::WatchCtx2 *watch_ctx,
                               uint64_t *watch_handle, Context *on_finish)
  : m_ioctx(ioctx), m_oid(oid), m_lock(watch_lock),
    m_watch_ctx(watch_ctx), m_watch_handle(watch_handle),
    m_on_finish(on_finish) {
}

void RewatchRequest::send() {
  unwatch();
}

void RewatchRequest::unwatch() {
  ceph_assert(ceph_mutex_is_wlocked(m_lock));
  if (*m_watch_handle == 0) {
    rewatch();
    return;
  }

  dout(10) << dendl;

  uint64_t watch_handle = 0;
  std::swap(*m_watch_handle, watch_handle);

  librados::AioCompletion *aio_comp =
    librados::Rados::aio_create_completion(
      this, &rados_callback<RewatchRequest, &RewatchRequest::handle_unwatch>);
  int r = m_ioctx.aio_unwatch(watch_handle, aio_comp);
  ceph_assert(r == 0);
  aio_comp->release();
}

void RewatchRequest::handle_unwatch(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r == -EBLOCKLISTED) {
    derr << ": client blocklisted" << dendl;
    finish(r);
    return;
  } else if (r < 0) {
    derr << ": failed to unwatch: " << cpp_strerror(r) << dendl;
  }

  rewatch();
}

void RewatchRequest::rewatch() {
  dout(20) << dendl;

  librados::AioCompletion *aio_comp =
    librados::Rados::aio_create_completion(
      this, &rados_callback<RewatchRequest, &RewatchRequest::handle_rewatch>);
  int r = m_ioctx.aio_watch(m_oid, aio_comp, &m_rewatch_handle, m_watch_ctx);
  ceph_assert(r == 0);
  aio_comp->release();
}

void RewatchRequest::handle_rewatch(int r) {
  dout(20) << ": r=" << r << dendl;

  if (r < 0) {
    derr << ": failed to watch object: " << cpp_strerror(r) << dendl;
    m_rewatch_handle = 0;
  }

  {
    std::unique_lock locker(m_lock);
    *m_watch_handle = m_rewatch_handle;
  }

  finish(r);
}

void RewatchRequest::finish(int r) {
  dout(20) << ": r=" << r << dendl;
  m_on_finish->complete(r);
  delete this;
}

} // namespace watcher
} // namespace mirror
} // namespace cephfs
