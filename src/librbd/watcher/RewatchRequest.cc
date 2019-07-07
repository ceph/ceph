// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/watcher/RewatchRequest.h"
#include "common/ceph_mutex.h"
#include "common/errno.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::watcher::RewatchRequest: " \
                           << this << " " << __func__ << " "

namespace librbd {

using util::create_context_callback;
using util::create_rados_callback;

namespace watcher {

using std::string;

RewatchRequest::RewatchRequest(librados::IoCtx& ioctx, const string& oid,
                               ceph::shared_mutex &watch_lock,
                               librados::WatchCtx2 *watch_ctx,
                               uint64_t *watch_handle, Context *on_finish)
  : m_ioctx(ioctx), m_oid(oid), m_watch_lock(watch_lock),
    m_watch_ctx(watch_ctx), m_watch_handle(watch_handle),
    m_on_finish(on_finish) {
}

void RewatchRequest::send() {
  unwatch();
}

void RewatchRequest::unwatch() {
  ceph_assert(ceph_mutex_is_wlocked(m_watch_lock));
  if (*m_watch_handle == 0) {
    rewatch();
    return;
  }

  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  ldout(cct, 10) << dendl;

  uint64_t watch_handle = 0;
  std::swap(*m_watch_handle, watch_handle);

  librados::AioCompletion *aio_comp = create_rados_callback<
                        RewatchRequest, &RewatchRequest::handle_unwatch>(this);
  int r = m_ioctx.aio_unwatch(watch_handle, aio_comp);
  ceph_assert(r == 0);
  aio_comp->release();
}

void RewatchRequest::handle_unwatch(int r) {
  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  ldout(cct, 10) << "r=" << r << dendl;

  if (r == -EBLACKLISTED) {
    lderr(cct) << "client blacklisted" << dendl;
    finish(r);
    return;
  } else if (r < 0) {
    lderr(cct) << "failed to unwatch: " << cpp_strerror(r) << dendl;
  }
  rewatch();
}

void RewatchRequest::rewatch() {
  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  ldout(cct, 10) << dendl;

  librados::AioCompletion *aio_comp = create_rados_callback<
                        RewatchRequest, &RewatchRequest::handle_rewatch>(this);
  int r = m_ioctx.aio_watch(m_oid, aio_comp, &m_rewatch_handle, m_watch_ctx);
  ceph_assert(r == 0);
  aio_comp->release();
}

void RewatchRequest::handle_rewatch(int r) {
  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  ldout(cct, 10) << "r=" << r << dendl;
  if (r < 0) {
    lderr(cct) << "failed to watch object: " << cpp_strerror(r)
               << dendl;
    m_rewatch_handle = 0;
  }

  {
    std::unique_lock watch_locker{m_watch_lock};
    *m_watch_handle = m_rewatch_handle;
  }

  finish(r);
}

void RewatchRequest::finish(int r) {
  CephContext *cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
  ldout(cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace watcher
} // namespace librbd

