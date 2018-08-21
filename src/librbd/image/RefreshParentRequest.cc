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
#include "librbd/io/ObjectDispatcher.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::RefreshParentRequest: "

namespace librbd {
namespace image {

using util::create_async_context_callback;
using util::create_context_callback;

template <typename I>
RefreshParentRequest<I>::RefreshParentRequest(
    I &child_image_ctx, const ParentInfo &parent_md,
    const MigrationInfo &migration_info, Context *on_finish)
  : m_child_image_ctx(child_image_ctx), m_parent_md(parent_md),
    m_migration_info(migration_info), m_on_finish(on_finish),
    m_parent_image_ctx(nullptr), m_parent_snap_id(CEPH_NOSNAP),
    m_error_result(0) {
}

template <typename I>
bool RefreshParentRequest<I>::is_refresh_required(
    I &child_image_ctx, const ParentInfo &parent_md,
    const MigrationInfo &migration_info) {
  assert(child_image_ctx.snap_lock.is_locked());
  assert(child_image_ctx.parent_lock.is_locked());
  return (is_open_required(child_image_ctx, parent_md, migration_info) ||
          is_close_required(child_image_ctx, parent_md, migration_info));
}

template <typename I>
bool RefreshParentRequest<I>::is_close_required(
    I &child_image_ctx, const ParentInfo &parent_md,
    const MigrationInfo &migration_info) {
  return (child_image_ctx.parent != nullptr &&
          !does_parent_exist(child_image_ctx, parent_md, migration_info));
}

template <typename I>
bool RefreshParentRequest<I>::is_open_required(
    I &child_image_ctx, const ParentInfo &parent_md,
    const MigrationInfo &migration_info) {
  return (does_parent_exist(child_image_ctx, parent_md, migration_info) &&
          (child_image_ctx.parent == nullptr ||
           child_image_ctx.parent->md_ctx.get_id() != parent_md.spec.pool_id ||
           child_image_ctx.parent->id != parent_md.spec.image_id ||
           child_image_ctx.parent->snap_id != parent_md.spec.snap_id));
}

template <typename I>
bool RefreshParentRequest<I>::does_parent_exist(
    I &child_image_ctx, const ParentInfo &parent_md,
    const MigrationInfo &migration_info) {
  return (parent_md.spec.pool_id > -1 && parent_md.overlap > 0) ||
      !migration_info.empty();
}

template <typename I>
void RefreshParentRequest<I>::send() {
  if (is_open_required(m_child_image_ctx, m_parent_md, m_migration_info)) {
    send_open_parent();
  } else {
    // parent will be closed (if necessary) during finalize
    send_complete(0);
  }
}

template <typename I>
void RefreshParentRequest<I>::apply() {
  assert(m_child_image_ctx.snap_lock.is_wlocked());
  assert(m_child_image_ctx.parent_lock.is_wlocked());
  std::swap(m_child_image_ctx.parent, m_parent_image_ctx);
  std::swap(m_child_image_ctx.migration_parent, m_migration_parent_image_ctx);
}

template <typename I>
void RefreshParentRequest<I>::finalize(Context *on_finish) {
  CephContext *cct = m_child_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  m_on_finish = on_finish;
  if (m_parent_image_ctx != nullptr) {
    send_close_migration_parent();
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
  if (r < 0) {
    lderr(cct) << "failed to create IoCtx: " << cpp_strerror(r) << dendl;
    send_complete(r);
    return;
  }

  // TODO support clone v2 parent namespaces
  parent_io_ctx.set_namespace(m_child_image_ctx.md_ctx.get_namespace());

  std::string image_name;
  uint64_t flags = 0;
  if (!m_migration_info.empty() && !m_migration_info.image_name.empty()) {
    image_name = m_migration_info.image_name;
    flags |= OPEN_FLAG_OLD_FORMAT;
  }

  m_parent_image_ctx = new I(image_name, m_parent_md.spec.image_id,
                             m_parent_md.spec.snap_id, parent_io_ctx, true);
  m_parent_image_ctx->child = &m_child_image_ctx;

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
  OpenRequest<I> *req = OpenRequest<I>::create(m_parent_image_ctx, flags, ctx);
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

  if (m_migration_info.empty()) {
    return m_on_finish;
  }

  send_open_migration_parent();
  return nullptr;
}

template <typename I>
void RefreshParentRequest<I>::send_open_migration_parent() {
  assert(m_parent_image_ctx != nullptr);
  assert(!m_migration_info.empty());

  CephContext *cct = m_child_image_ctx.cct;
  ParentSpec parent_spec;
  {
    RWLock::RLocker snap_locker(m_parent_image_ctx->snap_lock);
    RWLock::RLocker parent_locker(m_parent_image_ctx->parent_lock);

    auto snap_id = m_migration_info.snap_map.begin()->first;
    auto parent_info = m_parent_image_ctx->get_parent_info(snap_id);
    if (parent_info == nullptr) {
      lderr(cct) << "could not find parent info for snap id " << snap_id
                 << dendl;
    } else {
      parent_spec = parent_info->spec;
    }
  }

  if (parent_spec.pool_id == -1) {
    send_complete(0);
    return;
  }

  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::Rados rados(m_child_image_ctx.md_ctx);

  librados::IoCtx parent_io_ctx;
  int r = rados.ioctx_create2(parent_spec.pool_id, parent_io_ctx);
  if (r < 0) {
    lderr(cct) << "failed to access parent pool (id=" << parent_spec.pool_id
                 << "): " << cpp_strerror(r) << dendl;
    save_result(&r);
    send_close_parent();
    return;
  }

  m_migration_parent_image_ctx = new I("", parent_spec.image_id,
                                       parent_spec.snap_id, parent_io_ctx,
                                       true);

  using klass = RefreshParentRequest<I>;
  Context *ctx = create_async_context_callback(
    m_child_image_ctx, create_context_callback<
      klass, &klass::handle_open_migration_parent, false>(this));
  OpenRequest<I> *req = OpenRequest<I>::create(m_migration_parent_image_ctx, 0,
                                               ctx);
  req->send();
}

template <typename I>
Context *RefreshParentRequest<I>::handle_open_migration_parent(int *result) {
  CephContext *cct = m_child_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << " r=" << *result << dendl;

  save_result(result);
  if (*result < 0) {
    lderr(cct) << "failed to open migration parent image: "
               << cpp_strerror(*result) << dendl;

    // image already closed by open state machine
    delete m_migration_parent_image_ctx;
    m_migration_parent_image_ctx = nullptr;
  }

  return m_on_finish;
}

template <typename I>
void RefreshParentRequest<I>::send_close_migration_parent() {
  if (m_migration_parent_image_ctx == nullptr) {
    send_close_parent();
    return;
  }

  CephContext *cct = m_child_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  using klass = RefreshParentRequest<I>;
  Context *ctx = create_async_context_callback(
    m_child_image_ctx, create_context_callback<
      klass, &klass::handle_close_migration_parent, false>(this));
  CloseRequest<I> *req = CloseRequest<I>::create(m_migration_parent_image_ctx,
                                                 ctx);
  req->send();
}

template <typename I>
Context *RefreshParentRequest<I>::handle_close_migration_parent(int *result) {
  CephContext *cct = m_child_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << " r=" << *result << dendl;

  delete m_migration_parent_image_ctx;
  m_migration_parent_image_ctx = nullptr;

  if (*result < 0) {
    lderr(cct) << "failed to close migration parent image: "
               << cpp_strerror(*result) << dendl;
  }

  send_close_parent();
  return nullptr;
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
  m_parent_image_ctx = nullptr;

  if (*result < 0) {
    lderr(cct) << "failed to close parent image: " << cpp_strerror(*result)
               << dendl;
  }

  send_reset_existence_cache();
  return nullptr;
}

template <typename I>
void RefreshParentRequest<I>::send_reset_existence_cache() {
  CephContext *cct = m_child_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  Context *ctx = create_async_context_callback(
    m_child_image_ctx, create_context_callback<
      RefreshParentRequest<I>,
      &RefreshParentRequest<I>::handle_reset_existence_cache, false>(this));
  m_child_image_ctx.io_object_dispatcher->reset_existence_cache(ctx);
}

template <typename I>
Context *RefreshParentRequest<I>::handle_reset_existence_cache(int *result) {
  CephContext *cct = m_child_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << " r=" << *result << dendl;

  if (*result < 0) {
    lderr(cct) << "failed to reset object existence cache: "
               << cpp_strerror(*result) << dendl;
  }

  if (m_error_result < 0) {
    // propagate errors from opening the image
    *result = m_error_result;
  } else {
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
