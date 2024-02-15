// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/watcher/Notifier.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/watcher/Types.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::watcher::Notifier: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace watcher {

const uint64_t Notifier::NOTIFY_TIMEOUT = 5000;

Notifier::C_AioNotify::C_AioNotify(Notifier *notifier, NotifyResponse *response,
                                   Context *on_finish)
  : notifier(notifier), response(response), on_finish(on_finish) {
}

void Notifier::C_AioNotify::finish(int r) {
  if (response != nullptr) {
    if (r == 0 || r == -ETIMEDOUT) {
      try {
        auto it = out_bl.cbegin();
        decode(*response, it);
      } catch (const buffer::error &err) {
        r = -EBADMSG;
      }
    }
  }
  notifier->handle_notify(r, on_finish);
}

Notifier::Notifier(asio::ContextWQ *work_queue, IoCtx &ioctx,
                   const std::string &oid)
  : m_work_queue(work_queue), m_ioctx(ioctx), m_oid(oid),
    m_aio_notify_lock(ceph::make_mutex(util::unique_lock_name(
      "librbd::object_watcher::Notifier::m_aio_notify_lock", this))) {
  m_cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
}

Notifier::~Notifier() {
  std::lock_guard aio_notify_locker{m_aio_notify_lock};
  ceph_assert(m_pending_aio_notifies == 0);
}

void Notifier::flush(Context *on_finish) {
  std::lock_guard aio_notify_locker{m_aio_notify_lock};
  if (m_pending_aio_notifies == 0) {
    m_work_queue->queue(on_finish, 0);
    return;
  }

  m_aio_notify_flush_ctxs.push_back(on_finish);
}

void Notifier::notify(bufferlist &bl, NotifyResponse *response,
                      Context *on_finish) {
  {
    std::lock_guard aio_notify_locker{m_aio_notify_lock};
    ++m_pending_aio_notifies;

    ldout(m_cct, 20) << "pending=" << m_pending_aio_notifies << dendl;
  }

  C_AioNotify *ctx = new C_AioNotify(this, response, on_finish);
  librados::AioCompletion *comp = util::create_rados_callback(ctx);
  int r = m_ioctx.aio_notify(m_oid, comp, bl, NOTIFY_TIMEOUT, &ctx->out_bl);
  ceph_assert(r == 0);
  comp->release();
}

void Notifier::handle_notify(int r, Context *on_finish) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  std::lock_guard aio_notify_locker{m_aio_notify_lock};
  ceph_assert(m_pending_aio_notifies > 0);
  --m_pending_aio_notifies;

  ldout(m_cct, 20) << "pending=" << m_pending_aio_notifies << dendl;
  if (m_pending_aio_notifies == 0) {
    for (auto ctx : m_aio_notify_flush_ctxs) {
      m_work_queue->queue(ctx, 0);
    }
    m_aio_notify_flush_ctxs.clear();
  }

  if (on_finish != nullptr) {
    m_work_queue->queue(on_finish, r);
  }
}

} // namespace watcher
} // namespace librbd
