// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "SetHeadRequest.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "cls/rbd/cls_rbd_types.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/Utils.h"
#include "librbd/image/AttachParentRequest.h"
#include "librbd/image/DetachParentRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::deep_copy::SetHeadRequest: " \
                           << this << " " << __func__ << ": "

namespace librbd {
namespace deep_copy {

using librbd::util::create_context_callback;
using librbd::util::create_rados_callback;

template <typename I>
SetHeadRequest<I>::SetHeadRequest(I *image_ctx, uint64_t size,
                                  const cls::rbd::ParentImageSpec &spec,
                                  uint64_t parent_overlap,
                                  Context *on_finish)
  : m_image_ctx(image_ctx), m_size(size), m_parent_spec(spec),
    m_parent_overlap(parent_overlap), m_on_finish(on_finish),
    m_cct(image_ctx->cct) {
  ceph_assert(m_parent_overlap <= m_size);
}

template <typename I>
void SetHeadRequest<I>::send() {
  send_set_size();
}

template <typename I>
void SetHeadRequest<I>::send_set_size() {
  m_image_ctx->snap_lock.get_read();
  if (m_image_ctx->size == m_size) {
    m_image_ctx->snap_lock.put_read();
    send_detach_parent();
    return;
  }
  m_image_ctx->snap_lock.put_read();

  ldout(m_cct, 20) << dendl;

  // Change the image size on disk so that the snapshot picks up
  // the expected size.  We can do this because the last snapshot
  // we process is the sync snapshot which was created to match the
  // image size. We also don't need to worry about trimming because
  // we track the highest possible object number within the sync record
  librados::ObjectWriteOperation op;
  librbd::cls_client::set_size(&op, m_size);

  int r;
  auto finish_op_ctx = start_lock_op(&r);
  if (finish_op_ctx == nullptr) {
    lderr(m_cct) << "lost exclusive lock" << dendl;
    finish(r);
    return;
  }

  auto ctx = new FunctionContext([this, finish_op_ctx](int r) {
      handle_set_size(r);
      finish_op_ctx->complete(0);
    });
  librados::AioCompletion *comp = create_rados_callback(ctx);
  r = m_image_ctx->md_ctx.aio_operate(m_image_ctx->header_oid, comp, &op);
  ceph_assert(r == 0);
  comp->release();
}

template <typename I>
void SetHeadRequest<I>::handle_set_size(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to update image size: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  {
    // adjust in-memory image size now that it's updated on disk
    RWLock::WLocker snap_locker(m_image_ctx->snap_lock);
    if (m_image_ctx->size > m_size) {
      RWLock::WLocker parent_locker(m_image_ctx->parent_lock);
      if (m_image_ctx->parent_md.spec.pool_id != -1 &&
          m_image_ctx->parent_md.overlap > m_size) {
        m_image_ctx->parent_md.overlap = m_size;
      }
    }
    m_image_ctx->size = m_size;
  }

  send_detach_parent();
}

template <typename I>
void SetHeadRequest<I>::send_detach_parent() {
  m_image_ctx->parent_lock.get_read();
  if (m_image_ctx->parent_md.spec.pool_id == -1 ||
      (m_image_ctx->parent_md.spec == m_parent_spec &&
       m_image_ctx->parent_md.overlap == m_parent_overlap)) {
    m_image_ctx->parent_lock.put_read();
    send_attach_parent();
    return;
  }
  m_image_ctx->parent_lock.put_read();

  ldout(m_cct, 20) << dendl;

  int r;
  auto finish_op_ctx = start_lock_op(&r);
  if (finish_op_ctx == nullptr) {
    lderr(m_cct) << "lost exclusive lock" << dendl;
    finish(r);
    return;
  }

  auto ctx = new FunctionContext([this, finish_op_ctx](int r) {
      handle_detach_parent(r);
      finish_op_ctx->complete(0);
    });
  auto req = image::DetachParentRequest<I>::create(*m_image_ctx, ctx);
  req->send();
}

template <typename I>
void SetHeadRequest<I>::handle_detach_parent(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove parent: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  {
    // adjust in-memory parent now that it's updated on disk
    RWLock::WLocker parent_locker(m_image_ctx->parent_lock);
    m_image_ctx->parent_md.spec = {};
    m_image_ctx->parent_md.overlap = 0;
  }

  send_attach_parent();
}

template <typename I>
void SetHeadRequest<I>::send_attach_parent() {
  m_image_ctx->parent_lock.get_read();
  if (m_image_ctx->parent_md.spec == m_parent_spec &&
      m_image_ctx->parent_md.overlap == m_parent_overlap) {
    m_image_ctx->parent_lock.put_read();
    finish(0);
    return;
  }
  m_image_ctx->parent_lock.put_read();

  ldout(m_cct, 20) << dendl;

  int r;
  auto finish_op_ctx = start_lock_op(&r);
  if (finish_op_ctx == nullptr) {
    lderr(m_cct) << "lost exclusive lock" << dendl;
    finish(r);
    return;
  }

  auto ctx = new FunctionContext([this, finish_op_ctx](int r) {
      handle_attach_parent(r);
      finish_op_ctx->complete(0);
    });
  auto req = image::AttachParentRequest<I>::create(
    *m_image_ctx, m_parent_spec, m_parent_overlap, false, ctx);
  req->send();
}

template <typename I>
void SetHeadRequest<I>::handle_attach_parent(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to attach parent: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  {
    // adjust in-memory parent now that it's updated on disk
    RWLock::WLocker parent_locker(m_image_ctx->parent_lock);
    m_image_ctx->parent_md.spec = m_parent_spec;
    m_image_ctx->parent_md.overlap = m_parent_overlap;
  }

  finish(0);
}

template <typename I>
Context *SetHeadRequest<I>::start_lock_op(int* r) {
  RWLock::RLocker owner_locker(m_image_ctx->owner_lock);
  if (m_image_ctx->exclusive_lock == nullptr) {
    return new FunctionContext([](int r) {});
  }
  return m_image_ctx->exclusive_lock->start_op(r);
}

template <typename I>
void SetHeadRequest<I>::finish(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace deep_copy
} // namespace librbd

template class librbd::deep_copy::SetHeadRequest<librbd::ImageCtx>;
