// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/DetachChildRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "common/WorkQueue.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ImageCtx.h"
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
                           {m_image_ctx.md_ctx.get_id(), m_image_ctx.id});

  librados::Rados rados(m_image_ctx.md_ctx);
  int r = rados.ioctx_create2(m_parent_spec.pool_id, m_parent_io_ctx);
  assert(r == 0);

  auto aio_comp = create_rados_callback<
    DetachChildRequest<I>,
    &DetachChildRequest<I>::handle_clone_v2_child_detach>(this);
  r = m_parent_io_ctx.aio_operate(util::header_name(m_parent_spec.image_id),
                                  aio_comp, &op);
  assert(r == 0);
  aio_comp->release();
}

template <typename I>
void DetachChildRequest<I>::handle_clone_v2_child_detach(int r) {
  auto cct = m_image_ctx.cct;
  ldout(cct, 5) << "r=" << r << dendl;

  if (r == -ENOENT) {
    r = 0;
  } else if (r < 0) {
    lderr(cct) << "error detaching child from parent: " << cpp_strerror(r)
               << dendl;
    finish(r);
    return;
  }

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
  assert(r == 0);
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
