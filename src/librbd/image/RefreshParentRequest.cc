// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/RefreshParentRequest.h"
#include "include/rados/librados.hpp"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/ImageCtx.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/image/CloseRequest.h"
#include "librbd/image/OpenRequest.h"
#include "librbd/io/ObjectDispatcherInterface.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::RefreshParentRequest: "

namespace librbd {
namespace image {

using util::create_async_context_callback;
using util::create_context_callback;

template <typename I>
RefreshParentRequest<I>::RefreshParentRequest(
    I &child_image_ctx, const ParentImageInfo &parent_md,
    const MigrationInfo &migration_info, Context *on_finish)
  : m_child_image_ctx(child_image_ctx), m_parent_md(parent_md),
    m_migration_info(migration_info), m_on_finish(on_finish),
    m_parent_image_ctx(nullptr), m_parent_snap_id(CEPH_NOSNAP),
    m_error_result(0) {
}

template <typename I>
bool RefreshParentRequest<I>::is_refresh_required(
    I &child_image_ctx, const ParentImageInfo &parent_md,
    const MigrationInfo &migration_info) {
  ceph_assert(ceph_mutex_is_locked(child_image_ctx.image_lock));
  return (is_open_required(child_image_ctx, parent_md, migration_info) ||
          is_close_required(child_image_ctx, parent_md, migration_info));
}

template <typename I>
bool RefreshParentRequest<I>::is_close_required(
    I &child_image_ctx, const ParentImageInfo &parent_md,
    const MigrationInfo &migration_info) {
  return (child_image_ctx.parent != nullptr &&
          !does_parent_exist(child_image_ctx, parent_md, migration_info));
}

template <typename I>
bool RefreshParentRequest<I>::is_open_required(
    I &child_image_ctx, const ParentImageInfo &parent_md,
    const MigrationInfo &migration_info) {
  return (does_parent_exist(child_image_ctx, parent_md, migration_info) &&
          (child_image_ctx.parent == nullptr ||
           child_image_ctx.parent->md_ctx.get_id() != parent_md.spec.pool_id ||
           child_image_ctx.parent->md_ctx.get_namespace() !=
             parent_md.spec.pool_namespace ||
           child_image_ctx.parent->id != parent_md.spec.image_id ||
           child_image_ctx.parent->snap_id != parent_md.spec.snap_id));
}

template <typename I>
bool RefreshParentRequest<I>::does_parent_exist(
    I &child_image_ctx, const ParentImageInfo &parent_md,
    const MigrationInfo &migration_info) {
  if (child_image_ctx.child != nullptr &&
      child_image_ctx.child->migration_info.empty() && parent_md.overlap == 0) {
    // intermediate, non-migrating images should only open their parent if they
    // overlap
    return false;
  }

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
  ceph_assert(ceph_mutex_is_wlocked(m_child_image_ctx.image_lock));
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
  ceph_assert(m_parent_md.spec.pool_id >= 0);

  CephContext *cct = m_child_image_ctx.cct;
  ldout(cct, 10) << this << " " << __func__ << dendl;

  librados::IoCtx parent_io_ctx;
  int r = util::create_ioctx(m_child_image_ctx.md_ctx, "parent image",
                             m_parent_md.spec.pool_id,
                             m_parent_md.spec.pool_namespace, &parent_io_ctx);
  if (r < 0) {
    send_complete(r);
    return;
  }

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
  if (m_child_image_ctx.config.template get_val<bool>("rbd_balance_parent_reads")) {
    m_parent_image_ctx->set_read_flag(librados::OPERATION_BALANCE_READS);
  } else if (m_child_image_ctx.config.template get_val<bool>("rbd_localize_parent_reads")) {
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
  }

  return m_on_finish;
}

template <typename I>
void RefreshParentRequest<I>::send_close_parent() {
  ceph_assert(m_parent_image_ctx != nullptr);

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
