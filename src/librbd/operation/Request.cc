// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/operation/Request.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/asio/ContextWQ.h"

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
  [[maybe_unused]] I &image_ctx = this->m_image_ctx;
  ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));

  // automatically create the event if we don't need to worry
  // about affecting concurrent IO ops
  if (can_affect_io() || !append_op_event()) {
    send_op();
  }
}

template <typename I>
Context *Request<I>::create_context_finisher(int r) {
  // automatically commit the event if required (delete after commit)
  if (m_appended_op_event && !m_committed_op_event &&
      commit_op_event(r)) {
    return nullptr;
  }

  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;
  return util::create_context_callback<Request<I>, &Request<I>::finish>(this);
}

template <typename I>
void Request<I>::finish_and_destroy(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  // automatically commit the event if required (delete after commit)
  if (m_appended_op_event && !m_committed_op_event &&
      commit_op_event(r)) {
    return;
  }

  AsyncRequest<I>::finish_and_destroy(r);
}

template <typename I>
void Request<I>::finish(int r) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  ceph_assert(!m_appended_op_event || m_committed_op_event);
  AsyncRequest<I>::finish(r);
}

template <typename I>
bool Request<I>::append_op_event() {
  I &image_ctx = this->m_image_ctx;

  ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));
  std::shared_lock image_locker{image_ctx.image_lock};
  if (image_ctx.journal != nullptr &&
      image_ctx.journal->is_journal_appending()) {
    append_op_event(util::create_context_callback<
      Request<I>, &Request<I>::handle_op_event_safe>(this));
    return true;
  }
  return false;
}

template <typename I>
bool Request<I>::commit_op_event(int r) {
  I &image_ctx = this->m_image_ctx;
  std::shared_lock image_locker{image_ctx.image_lock};

  if (!m_appended_op_event) {
    return false;
  }

  ceph_assert(m_op_tid != 0);
  ceph_assert(!m_committed_op_event);
  m_committed_op_event = true;

  if (image_ctx.journal != nullptr &&
      image_ctx.journal->is_journal_appending()) {
    CephContext *cct = image_ctx.cct;
    ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

    // ops will be canceled / completed before closing journal
    ceph_assert(image_ctx.journal->is_journal_ready());
    image_ctx.journal->commit_op_event(m_op_tid, r,
                                       new C_CommitOpEvent(this, r));
    return true;
  }
  return false;
}

template <typename I>
void Request<I>::handle_commit_op_event(int r, int original_ret_val) {
  I &image_ctx = this->m_image_ctx;
  CephContext *cct = image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to commit op event to journal: " << cpp_strerror(r)
               << dendl;
  }
  if (original_ret_val < 0) {
    r = original_ret_val;
  }
  finish(r);
}

template <typename I>
void Request<I>::replay_op_ready(Context *on_safe) {
  I &image_ctx = this->m_image_ctx;
  ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));
  ceph_assert(ceph_mutex_is_locked(image_ctx.image_lock));
  ceph_assert(m_op_tid != 0);

  m_appended_op_event = true;
  image_ctx.journal->replay_op_ready(
    m_op_tid, util::create_async_context_callback(image_ctx, on_safe));
}

template <typename I>
void Request<I>::append_op_event(Context *on_safe) {
  I &image_ctx = this->m_image_ctx;
  ceph_assert(ceph_mutex_is_locked(image_ctx.owner_lock));
  ceph_assert(ceph_mutex_is_locked(image_ctx.image_lock));

  CephContext *cct = image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_op_tid = image_ctx.journal->allocate_op_tid();
  image_ctx.journal->append_op_event(
    m_op_tid, journal::EventEntry{create_event(m_op_tid)},
    new C_AppendOpEvent(this, on_safe));
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
    ceph_assert(!can_affect_io());

    // haven't started the request state machine yet
    std::shared_lock owner_locker{image_ctx.owner_lock};
    send_op();
  }
}

} // namespace operation
} // namespace librbd

#ifndef TEST_F
template class librbd::operation::Request<librbd::ImageCtx>;
#endif
