// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/DetachChildRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Operations.h"
#include "librbd/Utils.h"
#include <string>

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::DetachChildRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace image {

using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
DetachChildRequest<I>::~DetachChildRequest() {
  ceph_assert(m_parent_image_ctx == nullptr);
}

template <typename I>
void DetachChildRequest<I>::send() {
  {
    RWLock::RLocker snap_locker(m_image_ctx.snap_lock);
    RWLock::RLocker parent_locker(m_image_ctx.parent_lock);

    // use oldest snapshot or HEAD for parent spec
    if (!m_image_ctx.snap_info.empty()) {
      m_parent_spec = m_image_ctx.snap_info.begin()->second.parent.spec;
    } else {
      m_parent_spec = m_image_ctx.parent_md.spec;
    }
  }

  if (m_parent_spec.pool_id == -1) {
    // ignore potential race with parent disappearing
    m_image_ctx.op_work_queue->queue(create_context_callback<
      DetachChildRequest<I>,
      &DetachChildRequest<I>::finish>(this), 0);
    return;
  } else if (!m_image_ctx.test_op_features(RBD_OPERATION_FEATURE_CLONE_CHILD)) {
    clone_v1_remove_child();
    return;
  }

  clone_v2_child_detach();
}

template <typename I>
void DetachChildRequest<I>::clone_v2_child_detach() {
  auto cct = m_image_ctx.cct;
  ldout(cct, 5) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::child_detach(&op, m_parent_spec.snap_id,
                           {m_image_ctx.md_ctx.get_id(),
                            m_image_ctx.md_ctx.get_namespace(),
                            m_image_ctx.id});

  int r = util::create_ioctx(m_image_ctx.md_ctx, "parent image",
                             m_parent_spec.pool_id,
                             m_parent_spec.pool_namespace, &m_parent_io_ctx);
  if (r < 0) {
    finish(r);
    return;
  }

  m_parent_header_name = util::header_name(m_parent_spec.image_id);

  auto aio_comp = create_rados_callback<
    DetachChildRequest<I>,
    &DetachChildRequest<I>::handle_clone_v2_child_detach>(this);
  r = m_parent_io_ctx.aio_operate(m_parent_header_name, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void DetachChildRequest<I>::handle_clone_v2_child_detach(int r) {
  auto cct = m_image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(cct) << "error detaching child from parent: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

  clone_v2_get_snapshot();
}

template <typename I>
void DetachChildRequest<I>::clone_v2_get_snapshot() {
  auto cct = m_image_ctx.cct;
  ldout(cct, 5) << dendl;

  librados::ObjectReadOperation op;
  cls_client::snapshot_get_start(&op, m_parent_spec.snap_id);

  m_out_bl.clear();
  auto aio_comp = create_rados_callback<
    DetachChildRequest<I>,
    &DetachChildRequest<I>::handle_clone_v2_get_snapshot>(this);
  int r = m_parent_io_ctx.aio_operate(m_parent_header_name, aio_comp, &op,
                                      &m_out_bl);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void DetachChildRequest<I>::handle_clone_v2_get_snapshot(int r) {
  auto cct = m_image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  bool remove_snapshot = false;
  if (r == 0) {
    cls::rbd::SnapshotInfo snap_info;
    auto it = m_out_bl.cbegin();
    r = cls_client::snapshot_get_finish(&it, &snap_info);
    if (r == 0) {
      m_parent_snap_namespace = snap_info.snapshot_namespace;
      m_parent_snap_name = snap_info.name;

      if (cls::rbd::get_snap_namespace_type(m_parent_snap_namespace) ==
            cls::rbd::SNAPSHOT_NAMESPACE_TYPE_TRASH &&
          snap_info.child_count == 0) {
        // snapshot is in trash w/ zero children, so remove it
        remove_snapshot = true;
      }
    }
  }

  if (r < 0) {
    ldout(cct, 5) << "failed to retrieve snapshot: " << cpp_strerror(r)
                  << dendl;
  }

  if (!remove_snapshot) {
    finish(0);
    return;
  }

  clone_v2_open_parent();
}

template<typename I>
void DetachChildRequest<I>::clone_v2_open_parent() {
  auto cct = m_image_ctx.cct;
  ldout(cct, 5) << dendl;

  m_parent_image_ctx = I::create("", m_parent_spec.image_id, nullptr,
                                 m_parent_io_ctx, false);

  auto ctx = create_context_callback<
    DetachChildRequest<I>,
    &DetachChildRequest<I>::handle_clone_v2_open_parent>(this);
  m_parent_image_ctx->state->open(OPEN_FLAG_SKIP_OPEN_PARENT, ctx);
}

template<typename I>
void DetachChildRequest<I>::handle_clone_v2_open_parent(int r) {
  auto cct = m_image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 5) << "failed to open parent for read/write: "
                  << cpp_strerror(r) << dendl;
    m_parent_image_ctx->destroy();
    m_parent_image_ctx = nullptr;
    finish(0);
    return;
  }

  clone_v2_remove_snapshot();
}

template<typename I>
void DetachChildRequest<I>::clone_v2_remove_snapshot() {
  auto cct = m_image_ctx.cct;
  ldout(cct, 5) << dendl;

  auto ctx = create_context_callback<
    DetachChildRequest<I>,
    &DetachChildRequest<I>::handle_clone_v2_remove_snapshot>(this);
  m_parent_image_ctx->operations->snap_remove(m_parent_snap_namespace,
                                              m_parent_snap_name, ctx);
}

template<typename I>
void DetachChildRequest<I>::handle_clone_v2_remove_snapshot(int r) {
  auto cct = m_image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    ldout(cct, 5) << "failed to remove trashed clone snapshot: "
                  << cpp_strerror(r) << dendl;
  }

  clone_v2_close_parent();
}

template<typename I>
void DetachChildRequest<I>::clone_v2_close_parent() {
  auto cct = m_image_ctx.cct;
  ldout(cct, 5) << dendl;

  auto ctx = create_context_callback<
    DetachChildRequest<I>,
    &DetachChildRequest<I>::handle_clone_v2_close_parent>(this);
  m_parent_image_ctx->state->close(ctx);
}

template<typename I>
void DetachChildRequest<I>::handle_clone_v2_close_parent(int r) {
  auto cct = m_image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (r < 0) {
    ldout(cct, 5) << "failed to close parent image:" << cpp_strerror(r)
                  << dendl;
  }

  m_parent_image_ctx->destroy();
  m_parent_image_ctx = nullptr;
  finish(0);
}

template<typename I>
void DetachChildRequest<I>::clone_v1_remove_child() {
  auto cct = m_image_ctx.cct;
  ldout(cct, 5) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::remove_child(&op, m_parent_spec, m_image_ctx.id);

  auto aio_comp = create_rados_callback<
    DetachChildRequest<I>,
    &DetachChildRequest<I>::handle_clone_v1_remove_child>(this);
  int r = m_image_ctx.md_ctx.aio_operate(RBD_CHILDREN, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template<typename I>
void DetachChildRequest<I>::handle_clone_v1_remove_child(int r) {
  auto cct = m_image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (r == -ENOENT) {
    r = 0;
  } else if (r < 0) {
    lderr(cct) << "failed to remove child from children list: "
               << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  finish(0);
}

template <typename I>
void DetachChildRequest<I>::finish(int r) {
  auto cct = m_image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image
} // namespace librbd

template class librbd::image::DetachChildRequest<librbd::ImageCtx>;
