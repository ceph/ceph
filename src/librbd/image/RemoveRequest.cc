// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "librbd/image/RemoveRequest.h"
#include "common/dout.h"
#include "common/errno.h"
#include "librbd/internal.h"
#include "librbd/ImageState.h"
#include "librbd/Journal.h"
#include "librbd/ObjectMap.h"
#include "librbd/image/DetachChildRequest.h"
#include "librbd/image/PreRemoveRequest.h"
#include "librbd/journal/RemoveRequest.h"
#include "librbd/journal/TypeTraits.h"
#include "librbd/mirror/DisableRequest.h"
#include "librbd/operation/TrimRequest.h"

#define dout_subsys ceph_subsys_rbd
#undef dout_prefix
#define dout_prefix *_dout << "librbd::image::RemoveRequest: " << this << " " \
                           << __func__ << ": "

namespace librbd {
namespace image {

using librados::IoCtx;
using util::create_context_callback;
using util::create_async_context_callback;
using util::create_rados_callback;

template<typename I>
RemoveRequest<I>::RemoveRequest(IoCtx &ioctx, const std::string &image_name,
                                const std::string &image_id, bool force,
                                bool from_trash_remove,
                                ProgressContext &prog_ctx,
                                ContextWQ *op_work_queue, Context *on_finish)
  : m_ioctx(ioctx), m_image_name(image_name), m_image_id(image_id),
    m_force(force), m_from_trash_remove(from_trash_remove),
    m_prog_ctx(prog_ctx), m_op_work_queue(op_work_queue),
    m_on_finish(on_finish) {
  m_cct = reinterpret_cast<CephContext *>(m_ioctx.cct());
}

template<typename I>
RemoveRequest<I>::RemoveRequest(IoCtx &ioctx, I *image_ctx, bool force,
                                bool from_trash_remove,
                                ProgressContext &prog_ctx,
                                ContextWQ *op_work_queue, Context *on_finish)
  : m_ioctx(ioctx), m_image_name(image_ctx->name), m_image_id(image_ctx->id),
    m_image_ctx(image_ctx), m_force(force),
    m_from_trash_remove(from_trash_remove), m_prog_ctx(prog_ctx),
    m_op_work_queue(op_work_queue), m_on_finish(on_finish),
    m_cct(image_ctx->cct), m_header_oid(image_ctx->header_oid),
    m_old_format(image_ctx->old_format), m_unknown_format(false) {
}

template<typename I>
void RemoveRequest<I>::send() {
  ldout(m_cct, 20) << dendl;

  open_image();
}

template<typename I>
void RemoveRequest<I>::open_image() {
  if (m_image_ctx != nullptr) {
    pre_remove_image();
    return;
  }

  m_image_ctx = I::create(m_image_id.empty() ? m_image_name : "", m_image_id,
                          nullptr, m_ioctx, false);

  ldout(m_cct, 20) << dendl;

  using klass = RemoveRequest<I>;
  Context *ctx = create_context_callback<klass, &klass::handle_open_image>(
    this);

  m_image_ctx->state->open(OPEN_FLAG_SKIP_OPEN_PARENT, ctx);
}

template<typename I>
void RemoveRequest<I>::handle_open_image(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    m_image_ctx = nullptr;

    if (r != -ENOENT) {
      lderr(m_cct) << "error opening image: " << cpp_strerror(r) << dendl;
      finish(r);
      return;
    }

    remove_image();
    return;
  }

  m_image_id = m_image_ctx->id;
  m_image_name = m_image_ctx->name;
  m_header_oid = m_image_ctx->header_oid;
  m_old_format = m_image_ctx->old_format;
  m_unknown_format = false;

  pre_remove_image();
}

template<typename I>
void RemoveRequest<I>::pre_remove_image() {
  ldout(m_cct, 5) << dendl;

  auto ctx = create_context_callback<
    RemoveRequest<I>, &RemoveRequest<I>::handle_pre_remove_image>(this);
  auto req = PreRemoveRequest<I>::create(m_image_ctx, m_force, ctx);
  req->send();
}

template<typename I>
void RemoveRequest<I>::handle_pre_remove_image(int r) {
  ldout(m_cct, 5) << "r=" << r << dendl;

  if (r < 0) {
    if (r == -ECHILD) {
      r = -ENOTEMPTY;
    }
    send_close_image(r);
    return;
  }

  if (!m_image_ctx->data_ctx.is_valid()) {
    detach_child();
    return;
  }

  trim_image();
}

template<typename I>
void RemoveRequest<I>::trim_image() {
  ldout(m_cct, 20) << dendl;

  using klass = RemoveRequest<I>;
  Context *ctx = create_async_context_callback(
    *m_image_ctx, create_context_callback<
      klass, &klass::handle_trim_image>(this));

  std::shared_lock owner_lock{m_image_ctx->owner_lock};
  auto req = librbd::operation::TrimRequest<I>::create(
    *m_image_ctx, ctx, m_image_ctx->size, 0, m_prog_ctx);
  req->send();
}

template<typename I>
void RemoveRequest<I>::handle_trim_image(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to remove some object(s): "
                 << cpp_strerror(r) << dendl;
    send_close_image(r);
    return;
  }

  if (m_old_format) {
    send_close_image(r);
    return;
  }

  detach_child();
}

template<typename I>
void RemoveRequest<I>::detach_child() {
  ldout(m_cct, 20) << dendl;

  auto ctx = create_context_callback<
    RemoveRequest<I>, &RemoveRequest<I>::handle_detach_child>(this);
  auto req = DetachChildRequest<I>::create(*m_image_ctx, ctx);
  req->send();
}

template<typename I>
void RemoveRequest<I>::handle_detach_child(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "failed to detach child from parent: "
                 << cpp_strerror(r) << dendl;
    send_close_image(r);
    return;
  }

  send_disable_mirror();
}

template<typename I>
void RemoveRequest<I>::send_disable_mirror() {
  ldout(m_cct, 20) << dendl;

  using klass = RemoveRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_disable_mirror>(this);

  mirror::DisableRequest<I> *req =
    mirror::DisableRequest<I>::create(m_image_ctx, m_force, !m_force, ctx);
  req->send();
}

template<typename I>
void RemoveRequest<I>::handle_disable_mirror(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r == -EOPNOTSUPP) {
    r = 0;
  } else if (r < 0) {
    lderr(m_cct) << "error disabling image mirroring: "
                 << cpp_strerror(r) << dendl;
  }

  // one last chance to ensure all snapshots have been deleted
  m_image_ctx->image_lock.lock_shared();
  if (!m_image_ctx->snap_info.empty()) {
    ldout(m_cct, 5) << "image has snapshots - not removing" << dendl;
    m_ret_val = -ENOTEMPTY;
  }
  m_image_ctx->image_lock.unlock_shared();

  send_close_image(r);
}

template<typename I>
void RemoveRequest<I>::send_close_image(int r) {
  ldout(m_cct, 20) << dendl;

  m_ret_val = r;
  using klass = RemoveRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_send_close_image>(this);

  m_image_ctx->state->close(ctx);
}

template<typename I>
void RemoveRequest<I>::handle_send_close_image(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0) {
    lderr(m_cct) << "error encountered while closing image: "
                 << cpp_strerror(r) << dendl;
  }

  m_image_ctx = nullptr;
  if (m_ret_val < 0) {
    r = m_ret_val;
    finish(r);
    return;
  }

  remove_header();
}

template<typename I>
void RemoveRequest<I>::remove_header() {
  ldout(m_cct, 20) << dendl;

  using klass = RemoveRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_callback<klass, &klass::handle_remove_header>(this);
  int r = m_ioctx.aio_remove(m_header_oid, rados_completion);
  ceph_assert(r == 0);
  rados_completion->release();
}

template<typename I>
void RemoveRequest<I>::handle_remove_header(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "error removing header: " << cpp_strerror(r) << dendl;
    m_ret_val = r;
  }

  remove_image();
}

template<typename I>
void RemoveRequest<I>::remove_header_v2() {
  ldout(m_cct, 20) << dendl;

  if (m_header_oid.empty()) {
    m_header_oid = util::header_name(m_image_id);
  }

  using klass = RemoveRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_callback<klass, &klass::handle_remove_header_v2>(this);
  int r = m_ioctx.aio_remove(m_header_oid, rados_completion);
  ceph_assert(r == 0);
  rados_completion->release();
}

template<typename I>
void RemoveRequest<I>::handle_remove_header_v2(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "error removing header: " << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  send_journal_remove();
}

template<typename I>
void RemoveRequest<I>::send_journal_remove() {
  ldout(m_cct, 20) << dendl;

  using klass = RemoveRequest<I>;
  Context *ctx = create_context_callback<
    klass, &klass::handle_journal_remove>(this);

  typename journal::TypeTraits<I>::ContextWQ* context_wq;
  Journal<I>::get_work_queue(m_cct, &context_wq);

  journal::RemoveRequest<I> *req = journal::RemoveRequest<I>::create(
    m_ioctx, m_image_id, Journal<>::IMAGE_CLIENT_ID, context_wq, ctx);
  req->send();
}

template<typename I>
void RemoveRequest<I>::handle_journal_remove(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed to remove image journal: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  } else {
    r = 0;
  }

  send_object_map_remove();
}

template<typename I>
void RemoveRequest<I>::send_object_map_remove() {
  ldout(m_cct, 20) << dendl;

  using klass = RemoveRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_callback<klass, &klass::handle_object_map_remove>(this);

  int r = ObjectMap<>::aio_remove(m_ioctx,
				  m_image_id,
                                  rados_completion);
  ceph_assert(r == 0);
  rados_completion->release();
}

template<typename I>
void RemoveRequest<I>::handle_object_map_remove(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "failed to remove image journal: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  } else {
    r = 0;
  }

  mirror_image_remove();
}

template<typename I>
void RemoveRequest<I>::mirror_image_remove() {
  ldout(m_cct, 20) << dendl;

  librados::ObjectWriteOperation op;
  cls_client::mirror_image_remove(&op, m_image_id);

  using klass = RemoveRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_callback<klass, &klass::handle_mirror_image_remove>(this);
  int r = m_ioctx.aio_operate(RBD_MIRRORING, rados_completion, &op);
  ceph_assert(r == 0);
  rados_completion->release();
}

template<typename I>
void RemoveRequest<I>::handle_mirror_image_remove(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT && r != -EOPNOTSUPP) {
    lderr(m_cct) << "failed to remove mirror image state: "
                 << cpp_strerror(r) << dendl;
    finish(r);
    return;
  }

  if (m_from_trash_remove) {
    // both the id object and the directory entry have been removed in
    // a previous call to trash_move.
    finish(0);
    return;
  }

  remove_id_object();
}

template<typename I>
void RemoveRequest<I>::remove_image() {
  ldout(m_cct, 20) << dendl;

  if (m_old_format || m_unknown_format) {
    remove_v1_image();
  } else {
    remove_v2_image();
  }
}

template<typename I>
void RemoveRequest<I>::remove_v1_image() {
  ldout(m_cct, 20) << dendl;

  Context *ctx = new LambdaContext([this] (int r) {
      r = tmap_rm(m_ioctx, m_image_name);
      handle_remove_v1_image(r);
    });

  m_op_work_queue->queue(ctx, 0);
}

template<typename I>
void RemoveRequest<I>::handle_remove_v1_image(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  m_old_format = (r == 0);
  if (r == 0 || (r < 0 && !m_unknown_format)) {
    if (r < 0 && r != -ENOENT) {
      lderr(m_cct) << "error removing image from v1 directory: "
                   << cpp_strerror(r) << dendl;
    }

    m_on_finish->complete(r);
    delete this;
    return;
  }

  if (!m_old_format) {
    remove_v2_image();
  }
}

template<typename I>
void RemoveRequest<I>::remove_v2_image() {
  ldout(m_cct, 20) << dendl;

  if (m_image_id.empty()) {
    dir_get_image_id();
    return;
  } else if (m_image_name.empty()) {
    dir_get_image_name();
    return;
  }

  remove_header_v2();
  return;
}

template<typename I>
void RemoveRequest<I>::dir_get_image_id() {
  ldout(m_cct, 20) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::dir_get_id_start(&op, m_image_name);

  using klass = RemoveRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_callback<klass, &klass::handle_dir_get_image_id>(this);
  m_out_bl.clear();
  int r = m_ioctx.aio_operate(RBD_DIRECTORY, rados_completion, &op, &m_out_bl);
  ceph_assert(r == 0);
  rados_completion->release();
}

template<typename I>
void RemoveRequest<I>::handle_dir_get_image_id(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "error fetching image id: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::dir_get_id_finish(&iter, &m_image_id);
    if (r < 0) {
      finish(r);
      return;
    }
  }

  remove_header_v2();
}

template<typename I>
void RemoveRequest<I>::dir_get_image_name() {
  ldout(m_cct, 20) << dendl;

  librados::ObjectReadOperation op;
  librbd::cls_client::dir_get_name_start(&op, m_image_id);

  using klass = RemoveRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_callback<klass, &klass::handle_dir_get_image_name>(this);
  m_out_bl.clear();
  int r = m_ioctx.aio_operate(RBD_DIRECTORY, rados_completion, &op, &m_out_bl);
  ceph_assert(r == 0);
  rados_completion->release();
}

template<typename I>
void RemoveRequest<I>::handle_dir_get_image_name(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "error fetching image name: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  if (r == 0) {
    auto iter = m_out_bl.cbegin();
    r = librbd::cls_client::dir_get_name_finish(&iter, &m_image_name);
    if (r < 0) {
      finish(r);
      return;
    }
  }

  remove_header_v2();
}

template<typename I>
void RemoveRequest<I>::remove_id_object() {
  ldout(m_cct, 20) << dendl;

  using klass = RemoveRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_callback<klass, &klass::handle_remove_id_object>(this);
  int r = m_ioctx.aio_remove(util::id_obj_name(m_image_name), rados_completion);
  ceph_assert(r == 0);
  rados_completion->release();
}

template<typename I>
void RemoveRequest<I>::handle_remove_id_object(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "error removing id object: " << cpp_strerror(r)
                 << dendl;
    finish(r);
    return;
  }

  dir_remove_image();
}

template<typename I>
void RemoveRequest<I>::dir_remove_image() {
  ldout(m_cct, 20) << dendl;

  librados::ObjectWriteOperation op;
  librbd::cls_client::dir_remove_image(&op, m_image_name, m_image_id);

  using klass = RemoveRequest<I>;
  librados::AioCompletion *rados_completion =
    create_rados_callback<klass, &klass::handle_dir_remove_image>(this);
  int r = m_ioctx.aio_operate(RBD_DIRECTORY, rados_completion, &op);
  ceph_assert(r == 0);
  rados_completion->release();
}

template<typename I>
void RemoveRequest<I>::handle_dir_remove_image(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  if (r < 0 && r != -ENOENT) {
    lderr(m_cct) << "error removing image from v2 directory: "
                 << cpp_strerror(r) << dendl;
  }

  finish(r);
}

template<typename I>
void RemoveRequest<I>::finish(int r) {
  ldout(m_cct, 20) << "r=" << r << dendl;

  m_on_finish->complete(r);
  delete this;
}

} // namespace image
} // namespace librbd

template class librbd::image::RemoveRequest<librbd::ImageCtx>;
