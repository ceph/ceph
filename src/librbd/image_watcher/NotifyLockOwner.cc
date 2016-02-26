// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image_watcher/NotifyLockOwner.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/WatchNotifyTypes.h"
#include "librbd/image_watcher/Notifier.h"
#include <map>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image_watcher::NotifyLockOwner: " \
                           << this << " " << __func__

namespace librbd {
namespace image_watcher {

using namespace watch_notify;
using util::create_context_callback;

NotifyLockOwner::NotifyLockOwner(ImageCtx &image_ctx, Notifier &notifier,
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

  assert(m_image_ctx.owner_lock.is_locked());
  m_notifier.notify(m_bl, &m_out_bl, create_context_callback<
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

  typedef std::map<std::pair<uint64_t, uint64_t>, bufferlist> responses_t;
  responses_t responses;
  if (m_out_bl.length() > 0) {
    try {
      bufferlist::iterator iter = m_out_bl.begin();
      ::decode(responses, iter);
    } catch (const buffer::error &err) {
      lderr(cct) << ": failed to decode response" << dendl;
      finish(-EINVAL);
      return;
    }
  }

  bufferlist response;
  bool lock_owner_responded = false;
  for (responses_t::iterator i = responses.begin(); i != responses.end(); ++i) {
    if (i->second.length() > 0) {
      if (lock_owner_responded) {
        lderr(cct) << ": duplicate lock owners detected" << dendl;
        finish(-EINVAL);
        return;
      }
      lock_owner_responded = true;
      response.claim(i->second);
    }
  }

  if (!lock_owner_responded) {
    lderr(cct) << ": no lock owners detected" << dendl;
    finish(-ETIMEDOUT);
    return;
  }

  try {
    bufferlist::iterator iter = response.begin();

    ResponseMessage response_message;
    ::decode(response_message, iter);

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
