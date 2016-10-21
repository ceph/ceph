// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/managed_lock/NotifyLockOwner.h"
#include "common/errno.h"
#include "librbd/Utils.h"
#include "librbd/object_watcher/Notifier.h"
#include "librbd/managed_lock/LockWatcherTypes.h"
#include "librbd/watcher/WatcherTypes.h"
#include <map>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::managed_lock::NotifyLockOwner: " \
                           << this << " " << __func__

namespace librbd {

using watcher::ResponseMessage;

namespace managed_lock {

using util::create_context_callback;

NotifyLockOwner::NotifyLockOwner(CephContext *cct,
                                 object_watcher::Notifier &notifier,
                                 bufferlist &&bl, Context *on_finish)
  : m_cct(cct), m_notifier(notifier), m_bl(std::move(bl)),
    m_on_finish(on_finish) {
}

void NotifyLockOwner::send() {
  send_notify();
}

void NotifyLockOwner::send_notify() {
  ldout(m_cct, 20) << dendl;

  m_notifier.notify(m_bl, &m_out_bl, create_context_callback<
    NotifyLockOwner, &NotifyLockOwner::handle_notify>(this));
}

void NotifyLockOwner::handle_notify(int r) {
  ldout(m_cct, 20) << ": r=" << r << dendl;

  if (r < 0 && r != -ETIMEDOUT) {
    lderr(m_cct) << ": lock owner notification failed: " << cpp_strerror(r)
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
      lderr(m_cct) << ": failed to decode response" << dendl;
      finish(-EINVAL);
      return;
    }
  }

  bufferlist response;
  bool lock_owner_responded = false;
  for (responses_t::iterator i = responses.begin(); i != responses.end(); ++i) {
    if (i->second.length() > 0) {
      if (lock_owner_responded) {
        lderr(m_cct) << ": duplicate lock owners detected" << dendl;
        finish(-EINVAL);
        return;
      }
      lock_owner_responded = true;
      response.claim(i->second);
    }
  }

  if (!lock_owner_responded) {
    ldout(m_cct, 1) << ": no lock owners detected" << dendl;
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

} // namespace managed_lock
} // namespace librbd
