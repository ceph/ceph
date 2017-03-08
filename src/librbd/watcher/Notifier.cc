// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/watcher/Notifier.h"
#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::object_watcher::Notifier: "

namespace librbd {
namespace watcher {

const uint64_t Notifier::NOTIFY_TIMEOUT = 5000;

Notifier::Notifier(ContextWQ *work_queue, IoCtx &ioctx, const std::string &oid)
  : m_work_queue(work_queue), m_oid(oid),
    m_aio_notify_lock(util::unique_lock_name(
      "librbd::object_watcher::Notifier::m_aio_notify_lock", this)) {
  m_ioctx.dup(ioctx);
  m_cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
}

Notifier::~Notifier() {
  Mutex::Locker aio_notify_locker(m_aio_notify_lock);
  assert(m_pending_aio_notifies == 0);
}

void Notifier::flush(Context *on_finish) {
  Mutex::Locker aio_notify_locker(m_aio_notify_lock);
  if (m_pending_aio_notifies == 0) {
    m_work_queue->queue(on_finish, 0);
    return;
  }

  m_aio_notify_flush_ctxs.push_back(on_finish);
}

void Notifier::notify(bufferlist &bl, bufferlist *out_bl, Context *on_finish) {
  {
    Mutex::Locker aio_notify_locker(m_aio_notify_lock);
    ++m_pending_aio_notifies;

    ldout(m_cct, 20) << __func__ << ": pending=" << m_pending_aio_notifies
                     << dendl;
  }

  C_AioNotify *ctx = new C_AioNotify(this, on_finish);
  librados::AioCompletion *comp = util::create_rados_callback(ctx);
  int r = m_ioctx.aio_notify(m_oid, comp, bl, NOTIFY_TIMEOUT, out_bl);
  assert(r == 0);
  comp->release();
}

void Notifier::handle_notify(int r, Context *on_finish) {
  if (on_finish != nullptr) {
    m_work_queue->queue(on_finish, r);
  }

  Mutex::Locker aio_notify_locker(m_aio_notify_lock);
  assert(m_pending_aio_notifies > 0);
  --m_pending_aio_notifies;

  ldout(m_cct, 20) << __func__ << ": pending=" << m_pending_aio_notifies
                   << dendl;
  if (m_pending_aio_notifies == 0) {
    for (auto ctx : m_aio_notify_flush_ctxs) {
      m_work_queue->queue(ctx, 0);
    }
    m_aio_notify_flush_ctxs.clear();
  }
}

} // namespace watcher
} // namespace librbd
