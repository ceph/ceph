// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/exclusive_lock/PreAcquireRequest.h"
#include "librbd/Utils.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageWatcher.h"
#include "librbd/ImageState.h"
#include "librbd/asio/ContextWQ.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::exclusive_lock::PreAcquireRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace exclusive_lock {

using util::create_async_context_callback;
using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
PreAcquireRequest<I>* PreAcquireRequest<I>::create(I &image_ctx,
                                                   Context *on_finish) {
  return new PreAcquireRequest(image_ctx, on_finish);
}

template <typename I>
PreAcquireRequest<I>::PreAcquireRequest(I &image_ctx, Context *on_finish)
  : m_image_ctx(image_ctx),
    m_on_finish(create_async_context_callback(image_ctx, on_finish)),
    m_error_result(0) {
}

template <typename I>
PreAcquireRequest<I>::~PreAcquireRequest() {
}

template <typename I>
void PreAcquireRequest<I>::send() {
  send_prepare_lock();
}

template <typename I>
void PreAcquireRequest<I>::send_prepare_lock() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  // acquire the lock if the image is not busy performing other actions
  Context *ctx = create_context_callback<
    PreAcquireRequest<I>, &PreAcquireRequest<I>::handle_prepare_lock>(this);
  m_image_ctx.state->prepare_lock(ctx);
}

template <typename I>
void PreAcquireRequest<I>::handle_prepare_lock(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << "r=" << r << dendl;

  send_flush_notifies();
}

template <typename I>
void PreAcquireRequest<I>::send_flush_notifies() {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  using klass = PreAcquireRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_flush_notifies>(
    this);
  m_image_ctx.image_watcher->flush(ctx);
}

template <typename I>
void PreAcquireRequest<I>::handle_flush_notifies(int r) {
  CephContext *cct = m_image_ctx.cct;
  ldout(cct, 10) << dendl;

  ceph_assert(r == 0);
  finish();
}

template <typename I>
void PreAcquireRequest<I>::finish() {
  m_on_finish->complete(m_error_result);
  delete this;
}

} // namespace exclusive_lock
} // namespace librbd

template class librbd::exclusive_lock::PreAcquireRequest<librbd::ImageCtx>;
