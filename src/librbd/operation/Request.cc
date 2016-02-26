// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/Request.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include "librbd/Journal.h"
#include "librbd/Utils.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::Request: "

namespace librbd {
namespace operation {

template <typename I>
Request<I>::Request(I &image_ctx, Context *on_finish, uint64_t journal_op_tid)
  : AsyncRequest<I>(image_ctx, on_finish), m_op_tid(journal_op_tid) {
}

template <typename I>
void Request<I>::send() {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());

  // automatically create the event if we don't need to worry
  // about affecting concurrent IO ops
  if (can_affect_io() || !append_op_event()) {
    send_op();
  }
}

template <typename I>
void Request<I>::finish(int r) {
  // automatically commit the event if we don't need to worry
  // about affecting concurrent IO ops
  if (r < 0 || !can_affect_io()) {
    commit_op_event(r);
  }

  assert(!m_appended_op_event || m_committed_op_event);
  AsyncRequest<I>::finish(r);
}

template <typename I>
bool Request<I>::append_op_event() {
  I &image_ctx = this->m_image_ctx;

  assert(image_ctx.owner_lock.is_locked());
  RWLock::RLocker snap_locker(image_ctx.snap_lock);
  if (image_ctx.journal != NULL &&
      !image_ctx.journal->is_journal_replaying()) {
    append_op_event(util::create_context_callback<
      Request<I>, &Request<I>::handle_op_event_safe>(this));
    return true;
  }
  return false;
}

template <typename I>
void Request<I>::commit_op_event(int r) {
  I &image_ctx = this->m_image_ctx;
  RWLock::RLocker snap_locker(image_ctx.snap_lock);

  if (!m_appended_op_event) {
    return;
  }

  assert(m_op_tid != 0);
  assert(!m_committed_op_event);
  m_committed_op_event = true;

  if (image_ctx.journal != NULL &&
      !image_ctx.journal->is_journal_replaying()) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

    // ops will be canceled / completed before closing journal
    assert(image_ctx.journal->is_journal_ready());
    image_ctx.journal->commit_op_event(m_op_tid, r);
  }
}

template <typename I>
void Request<I>::replay_op_ready(Context *on_safe) {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());
  assert(image_ctx.snap_lock.is_locked());
  assert(m_op_tid != 0);

  m_appended_op_event = true;
  image_ctx.journal->replay_op_ready(
    m_op_tid, util::create_async_context_callback(image_ctx, on_safe));
}

template <typename I>
void Request<I>::append_op_event(Context *on_safe) {
  I &image_ctx = this->m_image_ctx;
  assert(image_ctx.owner_lock.is_locked());
  assert(image_ctx.snap_lock.is_locked());

  CephContext *cct = image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_op_tid = image_ctx.journal->allocate_op_tid();
  image_ctx.journal->append_op_event(
    m_op_tid, journal::EventEntry{create_event(m_op_tid)},
    new C_OpEventSafe(this, on_safe));
}

template <typename I>
void Request<I>::handle_op_event_safe(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to commit op event to journal: " << cpp_strerror(r)
               << dendl;
    this->finish(r);
    delete this;
  } else {
    assert(!can_affect_io());

    // haven't started the request state machine yet
    RWLock::RLocker owner_locker(image_ctx.owner_lock);
    send_op();
  }
}

} // namespace operation
} // namespace librbd

template class librbd::operation::Request<librbd::ImageCtx>;
