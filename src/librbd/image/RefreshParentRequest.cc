// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/RefreshParentRequest.h"
#include "include/rados/librados.hpp"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/image/CloseRequest.h"
#include "librbd/image/OpenRequest.h"
#include "librbd/image/SetSnapRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::RefreshParentRequest: "

namespace librbd {
namespace image {

using util::create_async_context_callback;
using util::create_context_callback;

template <typename I>
RefreshParentRequest<I>::RefreshParentRequest(I &child_image_ctx,
                                              const parent_info &parent_md,
                                              Context *on_finish)
  : m_child_image_ctx(child_image_ctx), m_parent_md(parent_md),
    m_on_finish(on_finish), m_parent_image_ctx(nullptr),
    m_parent_snap_id(CEPH_NOSNAP), m_error_result(0) {
}

template <typename I>
bool RefreshParentRequest<I>::is_refresh_required(I &child_image_ctx,
                                                  const parent_info &parent_md) {
  assert(child_image_ctx.snap_lock.is_locked());
  assert(child_image_ctx.parent_lock.is_locked());
  return (is_open_required(child_image_ctx, parent_md) ||
          is_close_required(child_image_ctx, parent_md));
}

template <typename I>
bool RefreshParentRequest<I>::is_close_required(I &child_image_ctx,
                                                const parent_info &parent_md) {
  return (child_image_ctx.parent != nullptr &&
          (parent_md.spec.pool_id == -1 || parent_md.overlap == 0));
}

template <typename I>
bool RefreshParentRequest<I>::is_open_required(I &child_image_ctx,
                                               const parent_info &parent_md) {
  return (parent_md.spec.pool_id > -1 && parent_md.overlap > 0 &&
          (child_image_ctx.parent == nullptr ||
           child_image_ctx.parent->md_ctx.get_id() != parent_md.spec.pool_id ||
           child_image_ctx.parent->id != parent_md.spec.image_id ||
           child_image_ctx.parent->snap_id != parent_md.spec.snap_id));
}

template <typename I>
void RefreshParentRequest<I>::send() {
  if (is_open_required(m_child_image_ctx, m_parent_md)) {
    send_open_parent();
  } else {
    // parent will be closed (if necessary) during finalize
    send_complete(0);
  }
}

template <typename I>
void RefreshParentRequest<I>::apply() {
  if (m_child_image_ctx.parent != nullptr) {
    // closing parent image
    m_child_image_ctx.clear_nonexistence_cache();
  }
  assert(m_child_image_ctx.snap_lock.is_wlocked());
  assert(m_child_image_ctx.parent_lock.is_wlocked());
  std::swap(m_child_image_ctx.parent, m_parent_image_ctx);
}

template <typename I>
void RefreshParentRequest<I>::finalize(Context *on_finish) {
  CephContext *cct = m_child_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_on_finish = on_finish;
  if (m_parent_image_ctx != nullptr) {
    send_close_parent();
  } else {
    send_complete(0);
  }
}

template <typename I>
void RefreshParentRequest<I>::send_open_parent() {
  assert(m_parent_md.spec.pool_id >= 0);

  CephContext *cct = m_child_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::Rados rados(m_child_image_ctx.md_ctx);

  librados::IoCtx parent_io_ctx;
  int r = rados.ioctx_create2(m_parent_md.spec.pool_id, parent_io_ctx);
  assert(r == 0);

  // since we don't know the image and snapshot name, set their ids and
  // reset the snap_name and snap_exists fields after we read the header
  m_parent_image_ctx = new I("", m_parent_md.spec.image_id, NULL, parent_io_ctx,
                             true);

  // set rados flags for reading the parent image
  if (m_child_image_ctx.balance_parent_reads) {
    m_parent_image_ctx->set_read_flag(librados::OPERATION_BALANCE_READS);
  } else if (m_child_image_ctx.localize_parent_reads) {
    m_parent_image_ctx->set_read_flag(librados::OPERATION_LOCALIZE_READS);
  }

  using klass = RefreshParentRequest<I>;
  Context *ctx = create_async_context_callback(
    m_child_image_ctx, create_context_callback<
      klass, &klass::handle_open_parent, false>(this));
  OpenRequest<I> *req = OpenRequest<I>::create(m_parent_image_ctx, ctx);
  req->send();
}

template <typename I>
Context *RefreshParentRequest<I>::handle_open_parent(int *result) {
  CephContext *cct = m_child_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << " r=" << *result << dendl;

  save_result(result);
  if (*result < 0) {
    lderr(cct) << "failed to open parent image: " << cpp_strerror(*result)
               << dendl;

    // image already closed by open state machine
    delete m_parent_image_ctx;
    m_parent_image_ctx = nullptr;

    return m_on_finish;
  }

  send_set_parent_snap();
  return nullptr;
}

template <typename I>
void RefreshParentRequest<I>::send_set_parent_snap() {
  assert(m_parent_md.spec.snap_id != CEPH_NOSNAP);

  CephContext *cct = m_child_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  int r;
  std::string snap_name;
  {
    RWLock::RLocker snap_locker(m_parent_image_ctx->snap_lock);
    r = m_parent_image_ctx->get_snap_name(m_parent_md.spec.snap_id, &snap_name);
  }

  if (r < 0) {
    lderr(cct) << "failed to located snapshot: " << cpp_strerror(r) << dendl;
    send_complete(r);
    return;
  }

  using klass = RefreshParentRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_set_parent_snap, false>(this);
  SetSnapRequest<I> *req = SetSnapRequest<I>::create(
    *m_parent_image_ctx, snap_name, ctx);
  req->send();
}

template <typename I>
Context *RefreshParentRequest<I>::handle_set_parent_snap(int *result) {
  CephContext *cct = m_child_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << " r=" << *result << dendl;

  save_result(result);
  if (*result < 0) {
    lderr(cct) << "failed to set parent snapshot: " << cpp_strerror(*result)
               << dendl;
    send_close_parent();
    return nullptr;
  }

  return m_on_finish;
}

template <typename I>
void RefreshParentRequest<I>::send_close_parent() {
  assert(m_parent_image_ctx != nullptr);

  CephContext *cct = m_child_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  using klass = RefreshParentRequest<I>;
  Context *ctx = create_async_context_callback(
    m_child_image_ctx, create_context_callback<
      klass, &klass::handle_close_parent, false>(this));
  CloseRequest<I> *req = CloseRequest<I>::create(m_parent_image_ctx, ctx);
  req->send();
}

template <typename I>
Context *RefreshParentRequest<I>::handle_close_parent(int *result) {
  CephContext *cct = m_child_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << " r=" << *result << dendl;

  delete m_parent_image_ctx;
  if (*result < 0) {
    lderr(cct) << "failed to close parent image: " << cpp_strerror(*result)
               << dendl;
  }

  if (m_error_result < 0) {
    // propagate errors from opening the image
    *result = m_error_result;
  } else {
    // ignore errors from closing the image
    *result = 0;
  }

  return m_on_finish;
}

template <typename I>
void RefreshParentRequest<I>::send_complete(int r) {
  CephContext *cct = m_child_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_on_finish->complete(r);
}

} // namespace image
} // namespace librbd

template class librbd::image::RefreshParentRequest<librbd::ImageCtx>;
