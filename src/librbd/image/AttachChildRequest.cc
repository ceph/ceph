// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/AttachChildRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/image/RefreshRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::AttachChildRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace image {

using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
AttachChildRequest<I>::AttachChildRequest(
    I *image_ctx, I *parent_image_ctx, const librados::snap_t &parent_snap_id,
    I *old_parent_image_ctx, const librados::snap_t &old_parent_snap_id,
    uint32_t clone_format, Context* on_finish)
    : m_image_ctx(image_ctx), m_parent_image_ctx(parent_image_ctx),
      m_parent_snap_id(parent_snap_id),
      m_old_parent_image_ctx(old_parent_image_ctx),
      m_old_parent_snap_id(old_parent_snap_id), m_clone_format(clone_format),
      m_on_finish(on_finish), m_cct(m_image_ctx->cct) {
}

template <typename I>
void AttachChildRequest<I>::send() {
  if (m_clone_format == 1) {
    v1_add_child();
  } else {
    v2_set_op_feature();
  }
}

template <typename I>
void AttachChildRequest<I>::v1_add_child() {
  ldout(m_cct, 15) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::add_child(&op, {m_parent_image_ctx->md_ctx.get_id(),
                              m_parent_image_ctx->md_ctx.get_namespace(),
                              m_parent_image_ctx->id,
                              m_parent_snap_id}, m_image_ctx->id);

  using klass = AttachChildRequest<I>;
  librados::AioCompletion *comp =
    create_rados_callback<klass, &klass::handle_v1_add_child>(this);
  int r = m_image_ctx->md_ctx.aio_operate(RBD_CHILDREN, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void AttachChildRequest<I>::handle_v1_add_child(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    if (r == -EEXIST && m_old_parent_image_ctx != nullptr) {
      ldout(m_cct, 5) << "child already exists" << dendl;
    } else {
      lderr(m_cct) << "couldn't add child: " << cpp_strerror(r) << dendl;
      finish(r);
      return;
    }
  }

  v1_refresh();
}

template <typename I>
void AttachChildRequest<I>::v1_refresh() {
  ldout(m_cct, 15) << dendl;

  using klass = AttachChildRequest<I>;
  RefreshRequest<I> *req = RefreshRequest<I>::create(
      *m_parent_image_ctx, false, false,
      create_context_callback<klass, &klass::handle_v1_refresh>(this));
  req->send();
}

template <typename I>
void AttachChildRequest<I>::handle_v1_refresh(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  bool snap_protected = false;
  if (r == 0) {
    RWLock::RLocker snap_locker(m_parent_image_ctx->snap_lock);
    r = m_parent_image_ctx->is_snap_protected(m_parent_snap_id,
                                              &snap_protected);
  }

  if (r < 0 || !snap_protected) {
    lderr(m_cct) << "validate protected failed" << dendl;
    finish(-EINVAL);
    return;
  }

  v1_remove_child_from_old_parent();
}

template <typename I>
void AttachChildRequest<I>::v1_remove_child_from_old_parent() {
  if (m_old_parent_image_ctx == nullptr) {
    finish(0);
    return;
  }

  ldout(m_cct, 15) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::remove_child(&op, {m_old_parent_image_ctx->md_ctx.get_id(),
                                 m_old_parent_image_ctx->md_ctx.get_namespace(),
                                 m_old_parent_image_ctx->id,
                                 m_old_parent_snap_id}, m_image_ctx->id);

  using klass = AttachChildRequest<I>;
  librados::AioCompletion *comp = create_rados_callback<
      klass, &klass::handle_v1_remove_child_from_old_parent>(this);
  int r = m_image_ctx->md_ctx.aio_operate(RBD_CHILDREN, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void AttachChildRequest<I>::handle_v1_remove_child_from_old_parent(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "couldn't remove child: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void AttachChildRequest<I>::v2_set_op_feature() {
  ldout(m_cct, 15) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::op_features_set(&op, RBD_OPERATION_FEATURE_CLONE_CHILD,
                              RBD_OPERATION_FEATURE_CLONE_CHILD);

  using klass = AttachChildRequest<I>;
  auto aio_comp = create_rados_callback<
      klass, &klass::handle_v2_set_op_feature>(this);
  int r = m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, aio_comp,
                                          &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void AttachChildRequest<I>::handle_v2_set_op_feature(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to enable clone v2: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  v2_child_attach();
}

template <typename I>
void AttachChildRequest<I>::v2_child_attach() {
  ldout(m_cct, 15) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::child_attach(&op, m_parent_snap_id,
                           {m_image_ctx->md_ctx.get_id(),
                            m_image_ctx->md_ctx.get_namespace(),
                            m_image_ctx->id});

  using klass = AttachChildRequest<I>;
  auto aio_comp = create_rados_callback<
      klass, &klass::handle_v2_child_attach>(this);
  int r = m_parent_image_ctx->md_ctx.aio_operate(m_parent_image_ctx->header_oid,
                                                 aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void AttachChildRequest<I>::handle_v2_child_attach(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0) {
    if (r == -EEXIST && m_old_parent_image_ctx != nullptr) {
      ldout(m_cct, 5) << "child already exists" << dendl;
    } else {
      lderr(m_cct) << "failed to attach child image: " << cpp_strerror(r)
                   << dendl;
      finish(r);
      return;
    }
  }

  v2_child_detach_from_old_parent();
}

template <typename I>
void AttachChildRequest<I>::v2_child_detach_from_old_parent() {
  if (m_old_parent_image_ctx == nullptr) {
    finish(0);
    return;
  }

  ldout(m_cct, 15) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::child_detach(&op, m_old_parent_snap_id,
                           {m_image_ctx->md_ctx.get_id(),
                            m_image_ctx->md_ctx.get_namespace(),
                            m_image_ctx->id});

  using klass = AttachChildRequest<I>;
  auto aio_comp = create_rados_callback<
      klass, &klass::handle_v2_child_detach_from_old_parent>(this);
  int r = m_old_parent_image_ctx->md_ctx.aio_operate(
      m_old_parent_image_ctx->header_oid, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void AttachChildRequest<I>::handle_v2_child_detach_from_old_parent(int r) {
  ldout(m_cct, 15) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed to detach child image: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void AttachChildRequest<I>::finish(int r) {
  ldout(m_cct, 5) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image
} // namespace librbd

template class librbd::image::AttachChildRequest<librbd::ImageCtx>;
