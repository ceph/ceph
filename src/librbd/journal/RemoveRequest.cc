// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/dout.h"
#include "common/errno.h"
#include "common/Timer.h"
#include "journal/Settings.h"
#include "include/ceph_assert.h"
#include "librbd/Utils.h"
#include "librbd/journal/RemoveRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::Journal::RemoveRequest: "

namespace librbd {

using util::create_context_callback;

namespace journal {

template<typename I>
RemoveRequest<I>::RemoveRequest(IoCtx &ioctx, const std::string &image_id,
                                const std::string &client_id,
                                ContextWQ *op_work_queue,
                                Context *on_finish)
  : m_ioctx(ioctx), m_image_id(image_id), m_image_client_id(client_id),
    m_op_work_queue(op_work_queue), m_on_finish(on_finish) {
  m_cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
}

template<typename I>
void RemoveRequest<I>::send() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  stat_journal();
}

template<typename I>
void RemoveRequest<I>::stat_journal() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  ImageCtx::get_timer_instance(m_cct, &m_timer, &m_timer_lock);
  m_journaler = new Journaler(m_op_work_queue, m_timer, m_timer_lock, m_ioctx,
                              m_image_id, m_image_client_id, {}, nullptr);

  using klass = RemoveRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_stat_journal>(this);

  m_journaler->exists(ctx);
}

template<typename I>
Context *RemoveRequest<I>::handle_stat_journal(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if ((*result < 0) && (*result != -ENOENT)) {
    lderr(m_cct) << "failed to stat journal header: " << cpp_strerror(*result) << dendl;
    shut_down_journaler(*result);
    return nullptr;
  }

  if (*result == -ENOENT) {
    shut_down_journaler(0);
    return nullptr;
  }

  init_journaler();
  return nullptr;
}

template<typename I>
void RemoveRequest<I>::init_journaler() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  using klass = RemoveRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_init_journaler>(this);

  m_journaler->init(ctx);
}

template<typename I>
Context *RemoveRequest<I>::handle_init_journaler(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if ((*result < 0) && (*result != -ENOENT)) {
    lderr(m_cct) << "failed to init journaler: " << cpp_strerror(*result) << dendl;
    shut_down_journaler(*result);
    return nullptr;
  }

  remove_journal();
  return nullptr;
}

template<typename I>
void RemoveRequest<I>::remove_journal() {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  using klass = RemoveRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_remove_journal>(this);

  m_journaler->remove(true, ctx);
}

template<typename I>
Context *RemoveRequest<I>::handle_remove_journal(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "failed to remove journal: " << cpp_strerror(*result) << dendl;
  }

  shut_down_journaler(*result);
  return nullptr;
}

template<typename I>
void RemoveRequest<I>::shut_down_journaler(int r) {
  ldout(m_cct, 20) << this << " " << __func__ << dendl;

  m_r_saved = r;

  using klass = RemoveRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_journaler_shutdown>(this);

  m_journaler->shut_down(ctx);
}

template<typename I>
Context *RemoveRequest<I>::handle_journaler_shutdown(int *result) {
  ldout(m_cct, 20) << __func__ << ": r=" << *result << dendl;

  if (*result < 0) {
    lderr(m_cct) << "failed to shut down journaler: " << cpp_strerror(*result) << dendl;
  }

  delete m_journaler;

  if (m_r_saved == 0) {
    ldout(m_cct, 20) << "done." << dendl;
  }

  m_on_finish->complete(m_r_saved);
  delete this;

  return nullptr;
}

} // namespace journal
} // namespace librbd

template class librbd::journal::RemoveRequest<librbd::ImageCtx>;
