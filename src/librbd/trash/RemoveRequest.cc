// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/trash/RemoveRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "cls/rbd/cls_rbd_client.h"
#include "librbd/ExclusiveLock.h"
#include "librbd/ImageCtx.h"
#include "librbd/ImageState.h"
#include "librbd/Utils.h"
#include "librbd/asio/ContextWQ.h"
#include "librbd/image/RemoveRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::trash::RemoveRequest: " << this \
                           << " " << __func__ << ": "

namespace librbd {
namespace trash {

using util::create_context_callback;
using util::create_rados_callback;

template <typename I>
void RemoveRequest<I>::send() {
  set_state();
}

template <typename I>
void RemoveRequest<I>::set_state() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::trash_state_set(&op, m_image_id, m_trash_set_state,
                              m_trash_expect_state);

  auto aio_comp = create_rados_callback<
      RemoveRequest<I>, &RemoveRequest<I>::handle_set_state>(this);
  int r = m_io_ctx.aio_operate(RBD_TRASH, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void RemoveRequest<I>::handle_set_state(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0 && r != -EOPNOTSUPP) {
    lderr(m_cct) << "error setting trash image state: " << cpp_strerror(r)
                 << dendl;
    if (m_ret_val == 0) {
      m_ret_val = r;
    }
    if (m_trash_set_state == cls::rbd::TRASH_IMAGE_STATE_REMOVING) {
      close_image();
    } else {
      finish(m_ret_val);
    }
    return;
  }

  if (m_trash_set_state == cls::rbd::TRASH_IMAGE_STATE_REMOVING) {
    remove_image();
  } else {
    ceph_assert(m_trash_set_state == cls::rbd::TRASH_IMAGE_STATE_NORMAL);
    finish(m_ret_val < 0 ? m_ret_val : r);
  };
}

template <typename I>
void RemoveRequest<I>::close_image() {
  if (m_image_ctx == nullptr) {
    finish(m_ret_val);
    return;
  }

  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
      RemoveRequest<I>, &RemoveRequest<I>::handle_close_image>(this);
  m_image_ctx->state->close(ctx);
}

template <typename I>
void RemoveRequest<I>::handle_close_image(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    ldout(m_cct, 5) << "failed to close image:" << cpp_strerror(r) << dendl;
  }

  m_image_ctx->destroy();
  m_image_ctx = nullptr;
  finish(m_ret_val);
}

template <typename I>
void RemoveRequest<I>::remove_image() {
  ldout(m_cct, 10) << dendl;

  auto ctx = create_context_callback<
      RemoveRequest<I>, &RemoveRequest<I>::handle_remove_image>(this);
  if (m_image_ctx != nullptr) {
    auto req = librbd::image::RemoveRequest<I>::create(
        m_io_ctx, m_image_ctx, m_force, true, m_prog_ctx, m_op_work_queue, ctx);
    req->send();
  } else {
    auto req = librbd::image::RemoveRequest<I>::create(
        m_io_ctx, "", m_image_id, m_force, true, m_prog_ctx, m_op_work_queue,
        ctx);
    req->send();
  }
}

template <typename I>
void RemoveRequest<I>::handle_remove_image(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0) {
    ldout(m_cct, 5) << "failed to remove image:" << cpp_strerror(r) << dendl;

    m_ret_val = r;
    m_trash_set_state = cls::rbd::TRASH_IMAGE_STATE_NORMAL;
    m_trash_expect_state = cls::rbd::TRASH_IMAGE_STATE_REMOVING;
    set_state();
    return;
  }

  m_image_ctx = nullptr;
  remove_trash_entry();
}

template <typename I>
void RemoveRequest<I>::remove_trash_entry() {
  ldout(m_cct, 10) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::trash_remove(&op, m_image_id);

  auto aio_comp = create_rados_callback<
      RemoveRequest<I>, &RemoveRequest<I>::handle_remove_trash_entry>(this);
  int r = m_io_ctx.aio_operate(RBD_TRASH, aio_comp, &op);
  ceph_assert(r == 0);
  aio_comp->release();
}

template <typename I>
void RemoveRequest<I>::handle_remove_trash_entry(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "error removing trash entry: " << cpp_strerror(r) << dendl;
  }

  finish(0);
}

template <typename I>
void RemoveRequest<I>::finish(int r) {
  ldout(m_cct, 10) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace trash
} // namespace librbd

template class librbd::trash::RemoveRequest<librbd::ImageCtx>;
