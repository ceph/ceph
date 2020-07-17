// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image_watcher/NotifyLockOwner.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/WatchNotifyTypes.h"
#include "librbd/watcher/Notifier.h"
#include <map>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image_watcher::NotifyLockOwner: " \
                           << this << " " << __func__

namespace librbd {

namespace image_watcher {

using namespace watch_notify;
using util::create_context_callback;

NotifyLockOwner::NotifyLockOwner(ImageCtx &image_ctx,
                                 watcher::Notifier &notifier,
                                 bufferlist &&bl, Context *on_finish)
  : m_image_ctx(image_ctx), m_notifier(notifier), m_bl(std::move(bl)),
    m_on_finish(on_finish) {
}

void NotifyLockOwner::send() {
  send_notify();
}

void NotifyLockOwner::send_notify() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << dendl;

  ceph_assert(ceph_mutex_is_locked(m_image_ctx.owner_lock));
  m_notifier.notify(m_bl, &m_notify_response, create_context_callback<
    NotifyLockOwner, &NotifyLockOwner::handle_notify>(this));
}

void NotifyLockOwner::handle_notify(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 20) << ": r=" << r << dendl;

  if (r < 0 && r != -ETIMEDOUT) {
    lderr(cct) << ": lock owner notification failed: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  bufferlist response;
  bool lock_owner_responded = false;
  for (auto &it : m_notify_response.acks) {
    if (it.second.length() > 0) {
      if (lock_owner_responded) {
        lderr(cct) << ": duplicate lock owners detected" << dendl;
        finish(-EINVAL);
        return;
      }
      lock_owner_responded = true;
      response = std::move(it.second);
    }
  }

  if (!lock_owner_responded) {
    ldout(cct, 1) << ": no lock owners detected" << dendl;
    finish(-ETIMEDOUT);
    return;
  }

  try {
    auto iter = response.cbegin();

    ResponseMessage response_message;
    using ceph::decode;
    decode(response_message, iter);

    r = response_message.result;
  } catch (const buffer::error &err) {
    r = -EINVAL;
  }
  finish(r);
}

void NotifyLockOwner::finish(int r) {
  m_on_finish->complete(r);
  delete this;
}

} // namespace image_watcher
} // namespace librbd
