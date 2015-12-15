// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/ImageState.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/Cond.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/image/CloseRequest.h"
#include "librbd/image/OpenRequest.h"
#include "librbd/image/RefreshRequest.h"
#include "librbd/image/SetSnapRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::ImageState: "

namespace librbd {

using util::create_context_callback;

template <typename I>
ImageState<I>::ImageState(I *image_ctx)
  : m_image_ctx(image_ctx), m_state(STATE_UNINITIALIZED),
    m_lock(util::unique_lock_name("librbd::ImageState::m_lock", this)),
    m_last_refresh(0), m_refresh_seq(0) {
}

template <typename I>
ImageState<I>::~ImageState() {
  assert(m_state == STATE_UNINITIALIZED || m_state == STATE_CLOSED);
}

template <typename I>
int ImageState<I>::open() {
  C_SaferCond ctx;
  open(&ctx);
  return ctx.wait();
}

template <typename I>
void ImageState<I>::open(Context *on_finish) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << dendl;

  Mutex::Locker locker(m_lock);
  assert(m_state == STATE_UNINITIALIZED);

  Action action(ACTION_TYPE_OPEN);
  action.refresh_seq = m_refresh_seq;
  execute_action(action, on_finish);
}

template <typename I>
int ImageState<I>::close() {
  C_SaferCond ctx;
  close(&ctx);

  int r = ctx.wait();
  delete m_image_ctx;
  return r;
}

template <typename I>
void ImageState<I>::close(Context *on_finish) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << dendl;

  Mutex::Locker locker(m_lock);
  assert(!is_closed());

  Action action(ACTION_TYPE_CLOSE);
  action.refresh_seq = m_refresh_seq;
  execute_action(action, on_finish);
}

template <typename I>
void ImageState<I>::handle_update_notification() {
  Mutex::Locker locker(m_lock);
  ++m_refresh_seq;

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << "refresh_seq = " << m_refresh_seq << ", "
		 << "last_refresh = " << m_last_refresh << dendl;
}

template <typename I>
bool ImageState<I>::is_refresh_required() const {
  Mutex::Locker locker(m_lock);
  return (m_last_refresh != m_refresh_seq);
}

template <typename I>
int ImageState<I>::refresh() {
  C_SaferCond refresh_ctx;
  refresh(&refresh_ctx);
  return refresh_ctx.wait();
}

template <typename I>
void ImageState<I>::refresh(Context *on_finish) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << dendl;

  m_lock.Lock();
  if (is_closed()) {
    m_lock.Unlock();
    on_finish->complete(0);
    return;
  }

  Action action(ACTION_TYPE_REFRESH);
  action.refresh_seq = m_refresh_seq;
  execute_action(action, on_finish);
  m_lock.Unlock();
}

template <typename I>
int ImageState<I>::refresh_if_required() {
  RWLock::RLocker owner_locker(m_image_ctx->owner_lock);
  return refresh_if_required(m_image_ctx->owner_lock);
}

template <typename I>
int ImageState<I>::refresh_if_required(const RWLock &) {
  assert(m_image_ctx->owner_lock.is_locked());

  C_SaferCond ctx;
  {
    Mutex::Locker locker(m_lock);
    if (m_last_refresh == m_refresh_seq || is_closed()) {
      return 0;
    }

    Action action(ACTION_TYPE_REFRESH);
    action.refresh_seq = m_refresh_seq;
    execute_action(action, &ctx);
  }

  return ctx.wait();
}

template <typename I>
void ImageState<I>::snap_set(const std::string &snap_name, Context *on_finish) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 20) << __func__ << ": snap_name=" << snap_name << dendl;

  Mutex::Locker locker(m_lock);
  Action action(ACTION_TYPE_SET_SNAP);
  action.snap_name = snap_name;
  execute_action(action, on_finish);
}

template <typename I>
bool ImageState<I>::is_transition_state() const {
  switch (m_state) {
  case STATE_UNINITIALIZED:
  case STATE_OPEN:
  case STATE_CLOSED:
    return false;
  case STATE_OPENING:
  case STATE_CLOSING:
  case STATE_REFRESHING:
  case STATE_SETTING_SNAP:
    break;
  }
  return true;
}

template <typename I>
bool ImageState<I>::is_closed() const {
  assert(m_lock.is_locked());

  return ((m_state == STATE_CLOSED) ||
          (!m_actions_contexts.empty() &&
           m_actions_contexts.back().first.action_type == ACTION_TYPE_CLOSE));
}

template <typename I>
void ImageState<I>::append_context(const Action &action, Context *context) {
  assert(m_lock.is_locked());

  ActionContexts *action_contexts = nullptr;
  for (auto &action_ctxs : m_actions_contexts) {
    if (action == action_ctxs.first) {
      action_contexts = &action_ctxs;
      break;
    }
  }

  if (action_contexts == nullptr) {
    m_actions_contexts.push_back({action, {}});
    action_contexts = &m_actions_contexts.back();
  }

  if (context != nullptr) {
    action_contexts->second.push_back(context);
  }
}

template <typename I>
void ImageState<I>::execute_next_action() {
  assert(m_lock.is_locked());
  assert(!m_actions_contexts.empty());
  switch (m_actions_contexts.front().first.action_type) {
  case ACTION_TYPE_OPEN:
    send_open();
    return;
  case ACTION_TYPE_CLOSE:
    send_close();
    return;
  case ACTION_TYPE_REFRESH:
    send_refresh();
    return;
  case ACTION_TYPE_SET_SNAP:
    send_set_snap();
    return;
  }
  assert(false);
}

template <typename I>
void ImageState<I>::execute_action(const Action &action, Context *on_finish) {
  assert(m_lock.is_locked());

  append_context(action, on_finish);
  if (!is_transition_state()) {
    execute_next_action();
  }
}

template <typename I>
void ImageState<I>::complete_action(State next_state, int r) {
  assert(m_lock.is_locked());
  assert(!m_actions_contexts.empty());

  ActionContexts action_contexts(std::move(m_actions_contexts.front()));
  m_actions_contexts.pop_front();
  m_state = next_state;

  m_lock.Unlock();
  for (auto ctx : action_contexts.second) {
    ctx->complete(r);
  }
  m_lock.Lock();

  if (!is_transition_state() && !m_actions_contexts.empty()) {
    execute_next_action();
  }
}

template <typename I>
void ImageState<I>::send_open() {
  assert(m_lock.is_locked());
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_state = STATE_OPENING;

  Context *ctx = create_context_callback<
    ImageState<I>, &ImageState<I>::handle_open>(this);
  image::OpenRequest<I> *req = image::OpenRequest<I>::create(
    m_image_ctx, ctx);

  m_lock.Unlock();
  req->send();
  m_lock.Lock();
}

template <typename I>
void ImageState<I>::handle_open(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to open image: " << cpp_strerror(r) << dendl;
  }

  Mutex::Locker locker(m_lock);
  complete_action(r < 0 ? STATE_UNINITIALIZED : STATE_OPEN, r);
}

template <typename I>
void ImageState<I>::send_close() {
  assert(m_lock.is_locked());
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_state = STATE_CLOSING;

  Context *ctx = create_context_callback<
    ImageState<I>, &ImageState<I>::handle_close>(this);
  image::CloseRequest<I> *req = image::CloseRequest<I>::create(
    m_image_ctx, ctx);

  m_lock.Unlock();
  req->send();
  m_lock.Lock();
}

template <typename I>
void ImageState<I>::handle_close(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "error occurred while closing image: " << cpp_strerror(r)
               << dendl;
  }

  Mutex::Locker locker(m_lock);
  complete_action(STATE_CLOSED, r);
}

template <typename I>
void ImageState<I>::send_refresh() {
  assert(m_lock.is_locked());
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_state = STATE_REFRESHING;

  Context *ctx = create_context_callback<
    ImageState<I>, &ImageState<I>::handle_refresh>(this);
  image::RefreshRequest<I> *req = image::RefreshRequest<I>::create(
    *m_image_ctx, ctx);

  m_lock.Unlock();
  req->send();
  m_lock.Lock();
}

template <typename I>
void ImageState<I>::handle_refresh(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": r=" << r << dendl;

  Mutex::Locker locker(m_lock);
  assert(!m_actions_contexts.empty());

  ActionContexts &action_contexts(m_actions_contexts.front());
  assert(action_contexts.first.action_type == ACTION_TYPE_REFRESH);
  assert(m_last_refresh <= action_contexts.first.refresh_seq);
  m_last_refresh = action_contexts.first.refresh_seq;

  complete_action(STATE_OPEN, r);
}

template <typename I>
void ImageState<I>::send_set_snap() {
  assert(m_lock.is_locked());

  m_state = STATE_SETTING_SNAP;

  assert(!m_actions_contexts.empty());
  ActionContexts &action_contexts(m_actions_contexts.front());
  assert(action_contexts.first.action_type == ACTION_TYPE_SET_SNAP);

  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << ": "
                 << "snap_name=" << action_contexts.first.snap_name << dendl;

  Context *ctx = create_context_callback<
    ImageState<I>, &ImageState<I>::handle_set_snap>(this);
  image::SetSnapRequest<I> *req = image::SetSnapRequest<I>::create(
    *m_image_ctx, action_contexts.first.snap_name, ctx);

  m_lock.Unlock();
  req->send();
  m_lock.Lock();
}

template <typename I>
void ImageState<I>::handle_set_snap(int r) {
  CephContext *cct = m_image_ctx->cct;
  ldout(cct, 10) << this << " " << __func__ << " r=" << r << dendl;

  if (r < 0) {
    lderr(cct) << "failed to set snapshot: " << cpp_strerror(r) << dendl;
  }

  Mutex::Locker locker(m_lock);
  complete_action(STATE_OPEN, r);
}

} // namespace librbd

template class librbd::ImageState<librbd::ImageCtx>;
